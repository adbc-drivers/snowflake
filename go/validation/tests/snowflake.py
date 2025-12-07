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

from adbc_drivers_validation import model


class SnowflakeQuirks(model.DriverQuirks):
    name = "snowflake"
    driver = "adbc_driver_snowflake"
    driver_name = "ADBC Snowflake Driver - Go"
    vendor_name = "Snowflake"
    vendor_version = "v1.18.0"
    short_version = "snowflake"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_transactions=True,
        get_objects_constraints_foreign=False,
        get_objects_constraints_primary=False,
        get_objects_constraints_unique=False,
        statement_bulk_ingest=True,
        statement_bulk_ingest_catalog=False,
        statement_bulk_ingest_schema=False,
        statement_bulk_ingest_temporary=False,
        statement_execute_schema=True,
        statement_get_parameter_schema=False,
        statement_rows_affected=True,
        current_catalog=model.FromEnv("SNOWFLAKE_DATABASE"),
        current_schema=model.FromEnv("SNOWFLAKE_SCHEMA"),
        supported_xdbc_fields=[],
    )
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("SNOWFLAKE_URI"),
            "adbc.snowflake.sql.client_option.max_timestamp_precision": "microseconds",
            "adbc.snowflake.sql.client_option.use_high_precision": "false",
        },
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries",)

    def is_table_not_found(self, table_name: str | None, error: Exception) -> bool:
        error_msg = str(error).lower()

        # Snowflake returns "Object does not exist, or operation cannot be performed."
        # Error codes 002043 or 002003 for table/object not found errors
        # Snowflake doesn't include the table name in the error message
        return (
            "002043" in error_msg
            or "002003" in error_msg
            or "object does not exist" in error_msg
            or "does not exist or not authorized" in error_msg
        )

    def quote_one_identifier(self, identifier: str) -> str:
        """Quote an identifier to preserve case and ensure consistency."""
        identifier = identifier.replace('"', '""')
        return f'"{identifier}"'

    def split_statement(self, statement: str) -> list[str]:
        # Custom split logic to handle comments after semicolons
        statements = []
        current = []
        for line in statement.split("\n"):
            current.append(line)
            # Check if line contains a semicolon (ignoring comments)
            if ";" in line:
                statements.append("\n".join(current))
                current = []
        if current:
            statements.append("\n".join(current))

        return statements


QUIRKS = [SnowflakeQuirks()]
