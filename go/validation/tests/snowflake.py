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

from pathlib import Path

from adbc_drivers_validation import model, quirks


class SnowflakeQuirks(model.DriverQuirks):
    name = "snowflake"
    driver = "adbc_driver_snowflake"
    driver_name = "ADBC Snowflake Driver - Go"
    vendor_name = "Snowflake"
    vendor_version = "unknown"
    short_version = "snowflake"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_set_current_catalog=True,
        connection_set_current_schema=True,
        connection_transactions=True,
        get_objects_constraints_foreign=True,
        get_objects_constraints_primary=True,
        statement_bulk_ingest=True,
        statement_bulk_ingest_schema=True,
        statement_bulk_ingest_temporary=False,
        statement_execute_schema=True,
        current_catalog=model.FromEnv("SNOWFLAKE_DATABASE"),
        current_schema=model.FromEnv("SNOWFLAKE_SCHEMA"),
        secondary_schema=model.FromEnv("SNOWFLAKE_SECONDARY_SCHEMA"),
        supported_xdbc_fields=[
            "xdbc_data_type",
            "xdbc_type_name",
            "xdbc_nullable",
            "xdbc_sql_data_type",
            "xdbc_decimal_digits",
            "xdbc_column_size",
            "xdbc_char_octet_length",
            "xdbc_is_nullable",
            "xdbc_num_prec_radix",
            "xdbc_datetime_sub",
        ],
    )
    setup = model.DriverSetup(
        database={
            "username": model.FromEnv("SNOWFLAKE_USERNAME"),
            "password": model.FromEnv("SNOWFLAKE_PASSWORD"),
            "adbc.snowflake.sql.auth_type": model.FromEnv("SNOWFLAKE_AUTH_TYPE"),
            "adbc.snowflake.sql.account": model.FromEnv("SNOWFLAKE_ACCOUNT"),
            "adbc.snowflake.sql.db": model.FromEnv("SNOWFLAKE_DATABASE"),
            "adbc.snowflake.sql.schema": model.FromEnv("SNOWFLAKE_SCHEMA"),
            "adbc.snowflake.sql.warehouse": model.FromEnv("SNOWFLAKE_WAREHOUSE"),
            "adbc.snowflake.sql.role": model.FromEnv("SNOWFLAKE_ROLE"),
        },
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries",)

    def is_table_not_found(self, table_name: str, error: Exception) -> bool:
        return "does not exist" in str(error).lower() and table_name.lower() in str(error).lower()

    def quote_one_identifier(self, identifier: str) -> str:
        # Return unquoted identifier to match drop_table behavior
        # This ensures all operations use Snowflake's uppercase normalization
        return identifier

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement)

    def drop_table(
        self,
        *,
        table_name: str,
        schema_name: str | None = None,
        catalog_name: str | None = None,
        if_exists: bool = True,
        temporary: bool = False,
    ) -> str:
        """Drop table using unquoted identifiers to match setup SQL behavior."""
        if temporary:
            raise NotImplementedError

        # Use unquoted identifiers to match how setup SQL creates tables
        # This ensures DROP and CREATE operations use the same identifier style
        parts = [part for part in (catalog_name, schema_name, table_name) if part]
        unquoted_name = ".".join(parts)

        if if_exists:
            return f"DROP TABLE IF EXISTS {unquoted_name}"
        else:
            return f"DROP TABLE {unquoted_name}"


    @property
    def sample_ddl_constraints(self) -> list[str]:
        return [
            "CREATE TABLE constraint_primary (z INT, a INT, b STRING, PRIMARY KEY (a))",
            "CREATE TABLE constraint_primary_multi (z INT, a INT, b STRING, PRIMARY KEY (b, a))",
            "CREATE TABLE constraint_primary_multi2 (z INT, a STRING, b INT, PRIMARY KEY (a, b))",
            "CREATE TABLE constraint_foreign (z INT, a INT, b INT, FOREIGN KEY (b) REFERENCES constraint_primary(a))",
            "CREATE TABLE constraint_foreign_multi (z INT, a INT, b INT, c STRING, FOREIGN KEY (c, b) REFERENCES constraint_primary_multi2(a, b))",
            # Clean up extra columns
            "ALTER TABLE constraint_primary DROP COLUMN z",
            "ALTER TABLE constraint_primary_multi DROP COLUMN z",
            "ALTER TABLE constraint_primary_multi2 DROP COLUMN z",
            "ALTER TABLE constraint_foreign DROP COLUMN z",
            "ALTER TABLE constraint_foreign_multi DROP COLUMN z",
        ]


QUIRKS = [SnowflakeQuirks()]