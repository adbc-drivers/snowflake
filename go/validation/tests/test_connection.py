# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import secrets

import adbc_driver_manager
import adbc_drivers_validation.tests.connection
import pytest

from . import snowflake


def pytest_generate_tests(metafunc) -> None:
    quirks = [snowflake.get_quirks(metafunc.config.getoption("vendor_version"))]
    return adbc_drivers_validation.tests.connection.generate_tests(quirks, metafunc)


class TestConnection(adbc_drivers_validation.tests.connection.TestConnection):
    def test_unknown_option(self, subtests, driver, conn) -> None:
        # database impl here accepts all options as extra connection options, so override this test
        with conn.cursor() as cursor:
            for handle in [
                conn.adbc_database,
                conn.adbc_connection,
                cursor.adbc_statement,
            ]:
                with subtests.test(name=handle.__class__.__name__):
                    for getter in (
                        "get_option",
                        "get_option_int",
                        "get_option_float",
                        "get_option_bytes",
                    ):
                        with pytest.raises(conn.ProgrammingError) as excinfo:
                            getattr(handle, getter)("this_option_does_not_exist")
                        assert (
                            excinfo.value.status_code
                            == adbc_driver_manager.AdbcStatusCode.NOT_FOUND
                        )

                    if handle is conn.adbc_database:
                        continue

                    for v in [
                        "value",
                        4,
                        4.0,
                        b"value",
                    ]:
                        with pytest.raises(conn.NotSupportedError) as excinfo:
                            handle.set_options(this_option_does_not_exist=v)
                        assert (
                            excinfo.value.status_code
                            == adbc_driver_manager.AdbcStatusCode.NOT_IMPLEMENTED
                        )

    @pytest.fixture(scope="class")
    @classmethod
    def literal_conn(cls, conn_factory):
        with conn_factory() as conn:
            conn.adbc_connection.set_options(
                **{"adbc.connection.get_objects.disable_wildcards": True}
            )
            yield conn

    @pytest.fixture(scope="class")
    @classmethod
    def literal_tables(cls, driver, conn):
        base = f"getobjects{secrets.token_hex(8)}"
        literal = f"{base}_%!"
        names = (literal, literal.upper(), f"{base}xother!")
        catalog = driver.features.current_catalog
        schema = driver.features.current_schema
        created = []
        try:
            with conn.cursor() as cursor:
                for name in names:
                    driver.try_drop_table(
                        cursor,
                        catalog_name=catalog,
                        schema_name=schema,
                        table_name=name,
                    )
                    quoted = driver.quote_identifier(catalog, schema, name)
                    cursor.execute(
                        f"CREATE TABLE {quoted} "
                        '("id" INT PRIMARY KEY, "col_%!" INT, "colXother!" INT, '
                        '"bytes" BINARY(16))'
                    )
                    created.append(name)
            yield catalog, schema, names
        finally:
            with conn.cursor() as cursor:
                for name in reversed(created):
                    driver.try_drop_table(
                        cursor,
                        catalog_name=catalog,
                        schema_name=schema,
                        table_name=name,
                    )

    @staticmethod
    def literal_objects(conn, depth, catalog, schema=None, table=None, column=None):
        return (
            conn.adbc_get_objects(
                depth=depth,
                catalog_filter=catalog,
                db_schema_filter=schema,
                table_name_filter=table,
                column_name_filter=column,
            )
            .read_all()
            .to_pylist()
        )

    @staticmethod
    def literal_object_tables(objects):
        return {
            table["table_name"]: table
            for obj in objects
            for schema in obj["catalog_db_schemas"] or []
            for table in schema["db_schema_tables"] or []
        }

    @pytest.mark.parametrize("depth", ["tables", "columns", "all"])
    def test_get_objects_literal_exact_table(
        self, driver, literal_conn, get_objects_constraints, depth
    ):
        tables = self.literal_object_tables(
            self.literal_objects(
                literal_conn,
                depth,
                driver.features.current_catalog,
                driver.features.current_schema,
                "constraint_primary",
            )
        )
        assert set(tables) == {"constraint_primary"}

    @pytest.mark.parametrize("depth", ["tables", "columns", "all"])
    @pytest.mark.parametrize("table_filter", ["", "%", "_"])
    def test_get_objects_literal_no_table_matches(
        self, literal_conn, get_objects_table, depth, table_filter
    ):
        catalog, schema, _ = get_objects_table
        objects = self.literal_objects(
            literal_conn, depth, catalog, schema, table_filter
        )
        assert self.literal_object_tables(objects) == {}
        assert [obj["catalog_name"] for obj in objects] == [catalog]
        assert [
            sch["db_schema_name"]
            for obj in objects
            for sch in obj["catalog_db_schemas"]
        ] == [schema]

    def test_get_objects_literal_parent_prefixes(self, driver, literal_conn):
        catalog = driver.features.current_catalog
        schema = driver.features.current_schema
        objects = self.literal_objects(literal_conn, "catalogs", catalog[:-1])
        assert all(
            obj["catalog_name"].casefold() == catalog[:-1].casefold() for obj in objects
        )
        objects = self.literal_objects(literal_conn, "db_schemas", catalog, schema[:-1])
        assert all(
            sch["db_schema_name"].casefold() == schema[:-1].casefold()
            for obj in objects
            for sch in obj["catalog_db_schemas"]
        )

    @pytest.mark.parametrize(
        "depth", ["catalogs", "db_schemas", "tables", "columns", "all"]
    )
    def test_get_objects_literal_case(self, literal_conn, get_objects_table, depth):
        catalog, schema, table = get_objects_table
        expected = self.literal_objects(literal_conn, depth, catalog, schema, table)
        actual = self.literal_objects(
            literal_conn, depth, catalog.swapcase(), schema.swapcase(), table.swapcase()
        )
        assert actual == expected
        assert [obj["catalog_name"] for obj in actual] == [catalog]
        if depth != "catalogs":
            assert [
                sch["db_schema_name"]
                for obj in actual
                for sch in obj["catalog_db_schemas"]
            ] == [schema]
        if depth in ("tables", "columns", "all"):
            assert set(self.literal_object_tables(actual)) == {table}

    @pytest.mark.parametrize("depth", ["tables", "columns", "all"])
    def test_get_objects_literal_special_names(
        self, literal_conn, literal_tables, depth
    ):
        catalog, schema, names = literal_tables
        tables = self.literal_object_tables(
            self.literal_objects(literal_conn, depth, catalog, schema, names[0])
        )
        assert set(tables) == set(names[:2])
        if depth != "tables":
            for table in tables.values():
                assert len(table["table_constraints"]) == 1
                constraint = table["table_constraints"][0]
                assert constraint["constraint_type"] == "PRIMARY KEY"
                assert constraint["constraint_column_names"] == ["id"]
                columns = {col["column_name"]: col for col in table["table_columns"]}
                assert columns["bytes"]["xdbc_column_size"] == 16

    @pytest.mark.parametrize("column_filter", ["COL_%!", "", "%", "_"])
    def test_get_objects_literal_column_filter(
        self, literal_conn, literal_tables, column_filter
    ):
        catalog, schema, names = literal_tables
        tables = self.literal_object_tables(
            self.literal_objects(
                literal_conn, "columns", catalog, schema, names[0], column_filter
            )
        )
        assert set(tables) == set(names[:2])
        expected = ["col_%!"] if column_filter == "COL_%!" else []
        for table in tables.values():
            assert [col["column_name"] for col in table["table_columns"]] == expected

    @pytest.mark.parametrize("depth", ["columns", "all"])
    @pytest.mark.parametrize(
        "table,kind,columns,usage",
        [
            ("constraint_primary", "PRIMARY KEY", ["a"], None),
            ("constraint_unique", "UNIQUE", ["a"], None),
            ("constraint_foreign", "FOREIGN KEY", ["b"], ["a"]),
        ],
    )
    def test_get_objects_literal_constraints_case(
        self,
        driver,
        literal_conn,
        get_objects_constraints,
        depth,
        table,
        kind,
        columns,
        usage,
    ):
        catalog = driver.features.current_catalog
        schema = driver.features.current_schema
        tables = self.literal_object_tables(
            self.literal_objects(
                literal_conn,
                depth,
                catalog.swapcase(),
                schema.swapcase(),
                table.upper(),
            )
        )
        assert set(tables) == {table}
        constraints = tables[table]["table_constraints"]
        if kind == "UNIQUE":
            assert len(constraints) == 2
            assert {tuple(c["constraint_column_names"]) for c in constraints} == {
                ("a",),
                ("c", "b"),
            }
        else:
            assert len(constraints) == 1
        assert any(
            constraint["constraint_type"] == kind
            and constraint["constraint_column_names"] == columns
            and (
                constraint["constraint_column_usage"] is None
                if usage is None
                else [
                    entry["fk_column_name"]
                    for entry in constraint["constraint_column_usage"]
                ]
                == usage
            )
            for constraint in constraints
        )
        if usage is not None:
            assert constraints[0]["constraint_column_usage"] == [
                {
                    "fk_catalog": catalog,
                    "fk_db_schema": schema,
                    "fk_table": "constraint_primary",
                    "fk_column_name": "a",
                }
            ]

    def test_get_objects_literal_nil_filters(self, literal_conn, get_objects_table):
        catalog, schema, table = get_objects_table
        tables = self.literal_object_tables(
            self.literal_objects(literal_conn, "tables", catalog, schema)
        )
        assert table in tables

    def test_get_objects_disable_wildcards(self, driver, conn):
        objects = self.literal_objects(conn, "catalogs", "%")
        assert driver.features.current_catalog in {
            obj["catalog_name"] for obj in objects
        }
        option = "adbc.connection.get_objects.disable_wildcards"
        previous = conn.adbc_connection.get_option(option)
        conn.adbc_connection.set_options(**{option: True})
        try:
            objects = self.literal_objects(conn, "catalogs", "%")
            assert objects == []
        finally:
            conn.adbc_connection.set_options(**{option: previous})
