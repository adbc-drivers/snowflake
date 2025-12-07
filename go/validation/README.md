<!--
  Copyright (c) 2025 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Validation Suite Setup

The following must be set up.

- Snowflake Account and credentials
- A database, stored in `SNOWFLAKE_DATABASE`
- A schema (dataset), stored in `SNOWFLAKE_SCHEMA`

## Authentication

You must provide Snowflake credentials by setting the following environment variables:

```bash
export SNOWFLAKE_URI="snowflake://user:password@account.snowflakecomputing.com/database/schema?warehouse=warehouse"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_SCHEMA="your_schema"
```

Example:
```bash
export SNOWFLAKE_URI="snowflake://testuser:mypassword@myorg-account1/testdb/public?warehouse=compute_wh&role=myrole"
export SNOWFLAKE_DATABASE="testdb"
export SNOWFLAKE_SCHEMA="public"
```

## Running Tests

Once configured, run the validation suite with:

```bash
cd go
pixi run validate
```
