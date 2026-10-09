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

    @pytest.fixture
    def literal_conn(self, conn):
        option = "adbc.connection.get_objects.disable_wildcards"
        previous = conn.adbc_connection.get_option(option)
        conn.adbc_connection.set_options(**{option: True})
        try:
            yield conn
        finally:
            conn.adbc_connection.set_options(**{option: previous})

    @pytest.mark.parametrize("depth", ["tables", "columns", "all"])
    def test_get_objects_literal_table(self, literal_conn, get_objects_table, depth):
        catalog, schema, table = get_objects_table
        objects = (
            literal_conn.adbc_get_objects(
                depth=depth,
                catalog_filter=catalog,
                db_schema_filter=schema,
                table_name_filter=table,
            )
            .read_all()
            .to_pylist()
        )
        assert set(
            {
                table["table_name"]: table
                for obj in objects
                for schema in obj["catalog_db_schemas"] or []
                for table in schema["db_schema_tables"] or []
            }
        ) == {table}
        for name in (table[:-1], table.swapcase(), "", "%", "_"):
            objects = (
                literal_conn.adbc_get_objects(
                    depth=depth,
                    catalog_filter=catalog,
                    db_schema_filter=schema,
                    table_name_filter=name,
                )
                .read_all()
                .to_pylist()
            )
            assert {
                table["table_name"]: table
                for obj in objects
                for schema in obj["catalog_db_schemas"] or []
                for table in schema["db_schema_tables"] or []
            } == {}
        objects = (
            literal_conn.adbc_get_objects(
                depth=depth,
                catalog_filter=catalog,
                db_schema_filter=schema,
            )
            .read_all()
            .to_pylist()
        )
        assert table in {
            table["table_name"]: table
            for obj in objects
            for schema in obj["catalog_db_schemas"] or []
            for table in schema["db_schema_tables"] or []
        }

    @pytest.mark.parametrize("depth", ["catalogs", "db_schemas", "tables"])
    def test_get_objects_literal_parents(self, literal_conn, get_objects_table, depth):
        catalog, schema, table = get_objects_table
        objects = (
            literal_conn.adbc_get_objects(
                depth=depth,
                catalog_filter=catalog,
                db_schema_filter=schema,
                table_name_filter=table,
            )
            .read_all()
            .to_pylist()
        )
        assert [obj["catalog_name"] for obj in objects] == [catalog]
        if depth == "catalogs":
            for name in (catalog[:-1], catalog.swapcase(), "", "%", "_"):
                assert (
                    literal_conn.adbc_get_objects(
                        depth=depth,
                        catalog_filter=name,
                    )
                    .read_all()
                    .to_pylist()
                    == []
                )
        else:
            for name in (schema[:-1], schema.swapcase(), "", "%", "_"):
                objects = (
                    literal_conn.adbc_get_objects(
                        depth=depth,
                        catalog_filter=catalog,
                        db_schema_filter=name,
                    )
                    .read_all()
                    .to_pylist()
                )
                schemas = [
                    sch for obj in objects for sch in obj["catalog_db_schemas"] or []
                ]
                if depth == "db_schemas":
                    assert schemas == []
                else:
                    assert {
                        table["table_name"]: table
                        for obj in objects
                        for schema in obj["catalog_db_schemas"] or []
                        for table in schema["db_schema_tables"] or []
                    } == {}

    @pytest.mark.parametrize("depth", ["columns", "all"])
    def test_get_objects_literal_constraints(
        self, driver, literal_conn, get_objects_constraints, depth
    ):
        catalog = driver.features.current_catalog
        schema = driver.features.current_schema
        for name, kind in (
            ("constraint_primary", "PRIMARY KEY"),
            ("constraint_unique", "UNIQUE"),
            ("constraint_foreign", "FOREIGN KEY"),
        ):
            tables = {
                table["table_name"]: table
                for obj in literal_conn.adbc_get_objects(
                    depth=depth,
                    catalog_filter=catalog,
                    db_schema_filter=schema,
                    table_name_filter=name,
                )
                .read_all()
                .to_pylist()
                for schema in obj["catalog_db_schemas"] or []
                for table in schema["db_schema_tables"] or []
            }
            assert set(tables) == {name}
            assert any(
                c["constraint_type"] == kind for c in tables[name]["table_constraints"]
            )
            empty_columns = {
                table["table_name"]: table
                for obj in literal_conn.adbc_get_objects(
                    depth=depth,
                    catalog_filter=catalog,
                    db_schema_filter=schema,
                    table_name_filter=name,
                    column_name_filter="",
                )
                .read_all()
                .to_pylist()
                for schema in obj["catalog_db_schemas"] or []
                for table in schema["db_schema_tables"] or []
            }
            assert empty_columns[name]["table_columns"] == []
            assert sorted(
                empty_columns[name]["table_constraints"],
                key=lambda constraint: constraint["constraint_name"],
            ) == sorted(
                tables[name]["table_constraints"],
                key=lambda constraint: constraint["constraint_name"],
            )
            assert {
                table["table_name"]: table
                for obj in literal_conn.adbc_get_objects(
                    depth=depth,
                    catalog_filter=catalog,
                    db_schema_filter=schema,
                    table_name_filter=name.upper(),
                )
                .read_all()
                .to_pylist()
                for schema in obj["catalog_db_schemas"] or []
                for table in schema["db_schema_tables"] or []
            } == {}

    @pytest.mark.parametrize(
        "depth",
        [
            # TODO(lidavidm): identify why this fails
            pytest.param("tables", marks=[pytest.mark.xfail()]),
            "columns",
            "all",
        ],
    )
    @pytest.mark.parametrize(
        "name",
        [
            r"literal_%!\n'quoted",
            "literal_%!'quoted",
            "literal_%!",
            "literal_under_score",
        ],
    )
    def test_get_objects_literal_special_names(self, driver, literal_conn, depth, name):
        catalog = driver.features.current_catalog
        schema = driver.features.current_schema
        names = (name, name.upper(), name + "suffix")
        created = []
        try:
            with literal_conn.cursor() as cursor:
                for table in names:
                    cursor.execute(
                        f"CREATE TEMPORARY TABLE {driver.quote_identifier(catalog, schema, table)} "
                        '("id" INT PRIMARY KEY, "col_%!" INT, "bytes" BINARY(16))'
                    )
                    created.append(table)
            tables = {
                table["table_name"]: table
                for obj in literal_conn.adbc_get_objects(
                    depth=depth,
                    catalog_filter=catalog,
                    db_schema_filter=schema,
                    table_name_filter=name,
                )
                .read_all()
                .to_pylist()
                for schema in obj["catalog_db_schemas"] or []
                for table in schema["db_schema_tables"] or []
            }
            assert set(tables) == {name}
            if depth != "tables":
                assert len(tables[name]["table_constraints"]) == 1
                columns = {c["column_name"]: c for c in tables[name]["table_columns"]}
                assert columns["bytes"]["xdbc_column_size"] == 16
                for column, expected in (
                    ("col_%!", ["col_%!"]),
                    ("COL_%!", []),
                    ("", []),
                ):
                    tables = {
                        table["table_name"]: table
                        for obj in literal_conn.adbc_get_objects(
                            depth=depth,
                            catalog_filter=catalog,
                            db_schema_filter=schema,
                            table_name_filter=name,
                            column_name_filter=column,
                        )
                        .read_all()
                        .to_pylist()
                        for schema in obj["catalog_db_schemas"] or []
                        for table in schema["db_schema_tables"] or []
                    }
                    assert [
                        c["column_name"] for c in tables[name]["table_columns"]
                    ] == expected
        finally:
            with literal_conn.cursor() as cursor:
                for table in reversed(created):
                    driver.try_drop_table(
                        cursor,
                        catalog_name=catalog,
                        schema_name=schema,
                        table_name=table,
                    )
