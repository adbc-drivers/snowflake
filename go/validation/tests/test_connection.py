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

    def test_get_objects_disable_wildcards(
        self, driver, conn, get_objects_table
    ) -> None:
        # if we use %, this should return all objects normally and no objects otherwise
        objects = (
            conn.adbc_get_objects(
                depth="all",
                catalog_filter="%",
                db_schema_filter="%",
                table_name_filter="%",
            )
            .read_all()
            .to_pylist()
        )
        tables = [
            (obj["catalog_name"], schema["db_schema_name"], table["table_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
        ]
        assert get_objects_table in tables

        conn.adbc_connection.set_options(
            **{"adbc.connection.get_objects.disable_wildcards": True}
        )
        try:
            objects = (
                conn.adbc_get_objects(
                    depth="all",
                    catalog_filter="%",
                    db_schema_filter="%",
                    table_name_filter="%",
                )
                .read_all()
                .to_pylist()
            )
            tables = [
                (obj["catalog_name"], schema["db_schema_name"], table["table_name"])
                for obj in objects
                for schema in obj["catalog_db_schemas"]
                for table in schema["db_schema_tables"]
            ]
            assert tables == []
        finally:
            conn.adbc_connection.set_options(
                **{"adbc.connection.get_objects.disable_wildcards": False}
            )
