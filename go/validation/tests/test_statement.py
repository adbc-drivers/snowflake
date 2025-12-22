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


import adbc_drivers_validation.tests.statement as statement_tests

from . import snowflake


def pytest_generate_tests(metafunc) -> None:
    return statement_tests.generate_tests(snowflake.QUIRKS, metafunc)


class TestStatement(statement_tests.TestStatement):
    def test_rows_affected(
        self,
        driver,
        conn,
    ) -> None:
        """
        Override to handle Snowflake's mixed row count behavior.

        Snowflake returns -1 for DDL operations (CREATE TABLE) but actual
        row counts for DML operations (INSERT/UPDATE/DELETE).
        """
        table_name = "test_rows_affected"
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query(
                driver.drop_table(table_name="test_rows_affected")
            )
            try:
                cursor.adbc_statement.execute_update()
            except Exception as e:
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise

            cursor.adbc_statement.set_sql_query(
                f"CREATE TABLE {driver.quote_identifier(table_name)} (id INT)"
            )
            rows_affected = cursor.adbc_statement.execute_update()
            # Snowflake returns -1 for DDL operations like CREATE TABLE
            assert rows_affected == -1

            cursor.adbc_statement.set_sql_query(
                f"INSERT INTO {driver.quote_identifier(table_name)} (id) VALUES (1)"
            )
            rows_affected = cursor.adbc_statement.execute_update()
            # Snowflake returns actual row counts for DML operations
            assert rows_affected == 1

            cursor.adbc_statement.set_sql_query(
                f"UPDATE {driver.quote_identifier(table_name)} SET id = id + 1"
            )
            rows_affected = cursor.adbc_statement.execute_update()
            assert rows_affected == 1

            cursor.adbc_statement.set_sql_query(
                f"DELETE FROM {driver.quote_identifier(table_name)}"
            )
            rows_affected = cursor.adbc_statement.execute_update()
            assert rows_affected == 1

            cursor.adbc_statement.set_sql_query(
                driver.drop_table(table_name="test_rows_affected")
            )

            rows_affected = cursor.adbc_statement.execute_update()
            # DROP TABLE return -1 (DDL)
            assert rows_affected == -1
