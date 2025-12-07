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


import functools
import re
from pathlib import Path

import adbc_drivers_validation.model as model
import adbc_drivers_validation.tests.query as query_tests

from . import snowflake

# Store the original query function
_original_query = model.query


@functools.cache
def _snowflake_query(path: Path) -> str:
    """
    Snowflake-specific query function that quotes table names and column names in SQL.

    This is required because snowflake turns the table and column names uppercase if not quoted.
    """
    sql = _original_query(path)

    # Pattern to match table names like test_boolean, test_int32, etc.
    table_pattern = r"\b(test_\w+)\b"

    def quote_identifier(match):
        identifier = match.group(1)
        return f'"{identifier}"'

    # Replace unquoted table references with quoted ones
    quoted_sql = re.sub(table_pattern, quote_identifier, sql)

    # Quote column names in CREATE TABLE statements
    # Pattern to find CREATE TABLE (...) and quote unquoted column names inside
    def quote_create_table_columns(match):
        before_paren = match.group(1)  # "CREATE TABLE table_name ("
        columns_part = match.group(2)  # column definitions
        after_paren = match.group(3)  # ");"

        # Quote common column names like idx, res, etc.
        column_names = r"\b(idx|res|value|name)\b(?=\s)"
        quoted_columns = re.sub(column_names, lambda m: f'"{m.group(1)}"', columns_part)

        return before_paren + quoted_columns + after_paren

    # Apply column quoting to CREATE TABLE statements
    create_table_pattern = r"(CREATE\s+TABLE\s+[^(]+\s*\()(.*?)(\);?)"
    quoted_sql = re.sub(
        create_table_pattern,
        quote_create_table_columns,
        quoted_sql,
        flags=re.DOTALL | re.IGNORECASE,
    )

    # Quote column names in SELECT statements
    column_pattern = r"\b(res\w*|idx\w*)\b(?=\s|$|,)"
    quoted_sql = re.sub(column_pattern, quote_identifier, quoted_sql)

    # Quote column names in INSERT statements
    # Pattern to match INSERT INTO table_name (columns) VALUES
    def quote_insert_columns(match):
        before_cols = match.group(1)  # "INSERT INTO table_name ("
        columns_list = match.group(2)  # column list
        after_cols = match.group(3)  # ") VALUES"

        # Split by comma and quote each unquoted column name
        columns = [col.strip() for col in columns_list.split(",")]
        quoted_columns = []

        for col in columns:
            # If already quoted, keep as is; otherwise quote it
            if col.startswith('"') and col.endswith('"'):
                quoted_columns.append(col)
            else:
                quoted_columns.append(f'"{col}"')

        return before_cols + ", ".join(quoted_columns) + after_cols

    # Apply column quoting to INSERT statements
    insert_pattern = r"(INSERT\s+INTO\s+[^(]+\s*\()([^)]+)(\)\s+VALUES)"
    quoted_sql = re.sub(
        insert_pattern, quote_insert_columns, quoted_sql, flags=re.IGNORECASE
    )

    return quoted_sql


# Monkey patch the query function for Snowflake tests
model.query = _snowflake_query


def pytest_generate_tests(metafunc) -> None:
    return query_tests.generate_tests(snowflake.QUIRKS, metafunc)


class TestQuery(query_tests.TestQuery):
    pass
