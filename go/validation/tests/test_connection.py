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

import contextlib
import uuid

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

    @pytest.fixture
    def metadata_queries(self, literal_conn):
        @contextlib.contextmanager
        def capture():
            queries = []
            tag = "get_objects_" + uuid.uuid4().hex
            with literal_conn.cursor() as cursor:
                cursor.execute("SHOW PARAMETERS LIKE 'QUERY_TAG' IN SESSION")
                previous = cursor.fetchall()[0][1]
                cursor.execute(f"ALTER SESSION SET QUERY_TAG = '{tag}'")
            try:
                yield queries
            finally:
                with literal_conn.cursor() as cursor:
                    escaped = previous.replace("\\", r"\\").replace("'", "''")
                    cursor.execute(f"ALTER SESSION SET QUERY_TAG = '{escaped}'")
                    cursor.execute(
                        "SELECT query_text FROM TABLE(information_schema.query_history_by_session()) "
                        f"WHERE query_tag = '{tag}' ORDER BY start_time"
                    )
                    queries.extend(row[0] for row in cursor.fetchall())

        return capture

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
    def object_tables(objects):
        return {
            table["table_name"]: table
            for obj in objects
            for schema in obj["catalog_db_schemas"] or []
            for table in schema["db_schema_tables"] or []
        }

    @pytest.mark.parametrize("depth", ["tables", "columns", "all"])
    def test_get_objects_literal_table(self, literal_conn, get_objects_table, depth):
        catalog, schema, table = get_objects_table
        objects = self.literal_objects(literal_conn, depth, catalog, schema, table)
        assert set(self.object_tables(objects)) == {table}
        for name in (table[:-1], table.swapcase(), "", "%", "_"):
            objects = self.literal_objects(literal_conn, depth, catalog, schema, name)
            assert self.object_tables(objects) == {}
        objects = self.literal_objects(literal_conn, depth, catalog, schema)
        assert table in self.object_tables(objects)

    @pytest.mark.parametrize("depth", ["catalogs", "db_schemas", "tables"])
    def test_get_objects_literal_parents(self, literal_conn, get_objects_table, depth):
        catalog, schema, table = get_objects_table
        objects = self.literal_objects(literal_conn, depth, catalog, schema, table)
        assert [obj["catalog_name"] for obj in objects] == [catalog]
        if depth == "catalogs":
            for name in (catalog[:-1], catalog.swapcase(), "", "%", "_"):
                assert self.literal_objects(literal_conn, depth, name) == []
        else:
            for name in (schema[:-1], schema.swapcase(), "", "%", "_"):
                objects = self.literal_objects(literal_conn, depth, catalog, name)
                schemas = [
                    sch for obj in objects for sch in obj["catalog_db_schemas"] or []
                ]
                if depth == "db_schemas":
                    assert schemas == []
                else:
                    assert self.object_tables(objects) == {}

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
            tables = self.object_tables(
                self.literal_objects(literal_conn, depth, catalog, schema, name)
            )
            assert set(tables) == {name}
            assert any(
                c["constraint_type"] == kind for c in tables[name]["table_constraints"]
            )
            empty_columns = self.object_tables(
                self.literal_objects(literal_conn, depth, catalog, schema, name, "")
            )
            assert empty_columns[name]["table_columns"] == []
            assert sorted(
                empty_columns[name]["table_constraints"],
                key=lambda constraint: constraint["constraint_name"],
            ) == sorted(
                tables[name]["table_constraints"],
                key=lambda constraint: constraint["constraint_name"],
            )
            assert (
                self.object_tables(
                    self.literal_objects(
                        literal_conn, depth, catalog, schema, name.upper()
                    )
                )
                == {}
            )

    @pytest.mark.parametrize("depth", ["tables", "columns", "all"])
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
            tables = self.object_tables(
                self.literal_objects(literal_conn, depth, catalog, schema, name)
            )
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
                    tables = self.object_tables(
                        self.literal_objects(
                            literal_conn, depth, catalog, schema, name, column
                        )
                    )
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

    @pytest.mark.parametrize(
        "path", ["schemas", "tables", "tables_database", "columns"]
    )
    def test_get_objects_literal_query_scopes(
        self, literal_conn, get_objects_table, metadata_queries, driver, path
    ):
        catalog, schema, table = get_objects_table
        depth = (
            "db_schemas"
            if path == "schemas"
            else "columns"
            if path == "columns"
            else "tables"
        )
        schema_filter = None if path == "tables_database" else schema
        with metadata_queries() as queries:
            objects = self.literal_objects(
                literal_conn, depth, catalog, schema_filter, table
            )
        if path != "schemas":
            assert table in self.object_tables(objects)
        shows = [q for q in queries if q.startswith("SHOW ")]
        assert shows
        for query in shows:
            if " DATABASES " in query:
                assert " IN ACCOUNT" in query
                assert " STARTS WITH '" in query
                continue
            assert " IN ACCOUNT" not in query
            if query.startswith("SHOW TERSE"):
                if " SCHEMAS " in query:
                    if schema_filter is not None:
                        assert " STARTS WITH '" in query
                else:
                    assert " STARTS WITH '" in query
            else:
                assert (
                    " IN TABLE " + driver.quote_identifier(catalog, schema, table)
                    in query
                )
        if path == "schemas" or path == "tables":
            assert len(shows) == 1
        elif path == "tables_database":
            assert len(shows) == 2

    @pytest.mark.parametrize(
        "depth", ["catalogs", "db_schemas", "tables", "columns", "all"]
    )
    @pytest.mark.parametrize("empty_filter", ["catalog", "schema", "table", "column"])
    def test_get_objects_literal_empty_queries(
        self, literal_conn, get_objects_table, metadata_queries, depth, empty_filter
    ):
        catalog, schema, table = get_objects_table
        filters = {"catalog": catalog, "schema": schema, "table": table, "column": None}
        filters[empty_filter] = ""
        with metadata_queries() as queries:
            objects = self.literal_objects(literal_conn, depth, **filters)
        shows = [q for q in queries if q.startswith("SHOW ")]
        if empty_filter == "catalog":
            assert shows == []
            if depth in ("catalogs", "columns", "all"):
                assert objects == []
            else:
                assert [obj["catalog_name"] for obj in objects] == [""]
        elif empty_filter == "schema" and depth != "catalogs":
            assert all(" DATABASES " in q and " IN ACCOUNT" in q for q in shows)
            assert [obj["catalog_name"] for obj in objects] == [catalog]
            assert all(obj["catalog_db_schemas"] == [] for obj in objects)
        elif empty_filter == "table" and depth in ("tables", "columns", "all"):
            assert all(" DATABASES " in q or " SCHEMAS " in q for q in shows)
            assert self.object_tables(objects) == {}
            assert [obj["catalog_name"] for obj in objects] == [catalog]
            assert [
                sch["db_schema_name"]
                for obj in objects
                for sch in obj["catalog_db_schemas"]
            ] == [schema]
        elif empty_filter == "column" and depth in ("columns", "all"):
            assert all(not q.startswith("SHOW COLUMNS") for q in shows)
            tables = self.object_tables(objects)
            assert table in tables
            assert tables[table]["table_columns"] == []

    @pytest.mark.parametrize("depth", ["db_schemas", "tables", "columns", "all"])
    @pytest.mark.parametrize("missing_filter", ["catalog", "schema", "table"])
    def test_get_objects_literal_missing_queries(
        self, literal_conn, get_objects_table, metadata_queries, depth, missing_filter
    ):
        catalog, schema, table = get_objects_table
        filters = {"catalog": catalog, "schema": schema, "table": table}
        filters[missing_filter] = "get_objects_nonexistent_" + uuid.uuid4().hex
        with metadata_queries() as queries:
            objects = self.literal_objects(literal_conn, depth, **filters)
        if depth != "db_schemas":
            assert self.object_tables(objects) == {}
        elif missing_filter != "table":
            assert all(obj["catalog_db_schemas"] == [] for obj in objects)
        assert not any(q.startswith("SHOW TERSE") and " LIKE ''" in q for q in queries)
        if depth in ("columns", "all") and missing_filter in ("catalog", "schema"):
            sources = [q for q in queries if q.startswith("SELECT NULL::VARCHAR")]
            assert any('AS "name"' in q and 'AS "database_name"' in q for q in sources)
            assert any('AS "kind"' in q for q in sources)
            assert all(q.endswith("WHERE FALSE") for q in sources)
