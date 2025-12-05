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
- A secondary schema for cross-schema tests, stored in `SNOWFLAKE_SECONDARY_SCHEMA`
- A warehouse, stored in `SNOWFLAKE_WAREHOUSE`

## Authentication

You must provide Snowflake credentials via one of the following methods:

### Option 1: Environment Variables
Set the following environment variables:
```bash
export SNOWFLAKE_URI="snowflake://user:password@account.snowflakecomputing.com/database/schema?warehouse=warehouse"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_SCHEMA="your_schema"
export SNOWFLAKE_SECONDARY_SCHEMA="your_secondary_schema"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
```

### Option 2: Snowflake URI Format
The `SNOWFLAKE_URI` should follow the format:
```
snowflake://user:password@account/database/schema?warehouse=warehouse&other_params=value
```

Example:
```
snowflake://testuser:mypassword@myorg-account1/testdb/public?warehouse=compute_wh&role=myrole
```

## Running Tests

Once configured, run the validation suite with:

```bash
pytest
```

Or run specific test modules:
```bash
pytest tests/test_connection.py
pytest tests/test_query.py
pytest tests/test_ingest.py
```

## Test Environment Requirements

- Snowflake account with appropriate permissions
- Database and schema with CREATE, INSERT, SELECT, DROP permissions
- Warehouse with USAGE privileges
- Network access to Snowflake endpoints