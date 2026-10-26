---
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
---

{{ cross_reference|safe }}

# Snowflake Driver {{ version }}

{{ heading|safe }}

This driver provides access to [Snowflake][snowflake], a cloud-based data warehouse platform.

## Installation & Quickstart

The driver can be installed with [dbc](https://docs.columnar.tech/dbc):

```bash
dbc install snowflake
```

## Pre-requisites

Using the Snowflake driver requires a Snowflake account and authentication. See [Getting Started With Snowflake](https://docs.snowflake.com/en/user-guide-getting-started) for instructions.

## Connecting

To connect, replace the Snowflake options below with the appropriate values for your situation and run the following:

```python
from adbc_driver_manager import dbapi

conn = dbapi.connect(
    driver="snowflake",
    db_kwargs={
        "username": "USER",
        ### for username/password authentication: ###
        "adbc.snowflake.sql.auth_type": "auth_snowflake",
        "password": "PASS",
        ### for JWT authentication: ###
        # "adbc.snowflake.sql.auth_type": "auth_jwt",
        # "adbc.snowflake.sql.client_option.jwt_private_key": "/path/to/rsa_key.p8",
        "adbc.snowflake.sql.account": "ACCOUNT-IDENT",
        "adbc.snowflake.sql.db": "SNOWFLAKE_SAMPLE_DATA",
        "adbc.snowflake.sql.schema": "TPCH_SF1",
        "adbc.snowflake.sql.warehouse": "MY_WAREHOUSE",
        "adbc.snowflake.sql.role": "MY_ROLE",
    },
)
```

Note: The example above is for Python using the [adbc-driver-manager](https://pypi.org/project/adbc-driver-manager) package but the process will be similar for other driver managers.  See [adbc-quickstarts](https://github.com/columnar-tech/adbc-quickstarts).

The driver supports connecting with individual options or connection strings.

### Connection String Format

Snowflake URI syntax:

```
snowflake://user[:password]@host[:port]/database[/schema][?param1=value1&param2=value2]
```

This follows the [Go Snowflake Driver Connection String](https://pkg.go.dev/github.com/snowflakedb/gosnowflake#hdr-Connection_String) format with the addition of the `snowflake://` scheme.

Components:

- `scheme`: `snowflake://` (required)
- `user/password`: (optional) For username/password authentication
- `host`: (required) The Snowflake account identifier string (e.g., myorg-account1) OR the full hostname (e.g., private.network.com). If a full hostname is used, the actual Snowflake account identifier must be provided separately via the account query parameter (see example 3).
- `port`: The port is optional and defaults to 443.
- `database`: Database name (required)
- `schema`: Schema name (optional)
- `Query Parameters`: Additional configuration options. For a complete list of parameters, see the [Go Snowflake Driver Connection Parameters](https://pkg.go.dev/github.com/snowflakedb/gosnowflake#hdr-Connection_Parameters)

:::{note}
Reserved characters in URI elements must be URI-encoded. For example, `@` becomes `%40`.
:::

Examples:

- `snowflake://jane.doe:MyS3cr3t!@myorg-account1/ANALYTICS_DB/SALES_DATA?warehouse=WH_XL&role=ANALYST`
- `snowflake://service_user@myorg-account2/RAW_DATA_LAKE?authenticator=oauth&application=ADBC_APP`
- `snowflake://sys_admin@private.network.com:443/OPS_MONITOR/DBA?account=vpc-id-1234&insecureMode=true&client_session_keep_alive=true` (Uses full hostname, requires explicit account parameter)

## Options

Database options use the same keys and values as the Go Snowflake driver, including the `adbc.snowflake.sql.auth_type` values (`auth_snowflake`, `auth_oauth`, `auth_ext_browser`, `auth_okta`, `auth_jwt`, `auth_mfa`, `auth_pat`, `auth_wif`). Durations such as `login_timeout` accept Go duration strings (for example `30s` or `1m30s`) and are rounded up to whole seconds.

Setting an option the driver does not recognize, or one it cannot honor, fails with `NOT_IMPLEMENTED` instead of being ignored. The following Go driver options are rejected for this reason: `adbc.snowflake.sql.region`, `adbc.snowflake.sql.client_option.ocsp_fail_open_mode`, `adbc.snowflake.sql.client_option.jwt_expire_timeout`, `adbc.snowflake.sql.client_option.tracing`, `adbc.snowflake.sql.client_option.config_file`, `adbc.snowflake.sql.client_option.use_high_precision`, `adbc.snowflake.sql.client_option.stream_retry_enabled`, `adbc.rpc.result_queue_size`, and `adbc.snowflake.rpc.prefetch_concurrency`. Invalid values for recognized options fail with `INVALID_ARGUMENT`.

### Database and Connection Options

- `adbc.snowflake.sql.client_option.max_timestamp_precision`: `nanoseconds` (default), `nanoseconds_error_on_overflow`, or `microseconds`. Also settable per statement.
- `adbc.snowflake.sql.client_option.geography_output_format` / `adbc.snowflake.sql.client_option.geometry_output_format`: `GeoJSON` (default) or `EWKB`, case-insensitive. Setting either on a connection applies it to the current session with `ALTER SESSION`. With `EWKB`, GEOGRAPHY and GEOMETRY columns are returned as binary `geoarrow.wkb` extension arrays; with `GeoJSON` they are strings.
- `adbc.connection.get_objects.match_exactly`: connection-only option, `false` (default) or `true`. When true, GetObjects catalog, schema, table, and column filters match literal names case-sensitively, including `%` and `_`; invalid values fail with `INVALID_ARGUMENT`. Without it, filters retain case-insensitive LIKE wildcard semantics.

GetObjects at `columns` and `all` depths includes PRIMARY KEY, FOREIGN KEY, and UNIQUE constraints, with ordered column names and referenced catalog/schema/table/column information for foreign keys. At `tables` depth, the constraint list remains null.

### Statement Options

- `adbc.snowflake.statement.query_timeout`: per-statement query timeout in seconds. Accepts a non-negative integer or a string such as `"30"` or `"30s"`. Set it to `0` to clear the override and use the Snowflake session/default timeout.

### Bulk Ingestion Options

Bulk ingestion writes the bound data to Parquet files, uploads them concurrently to a temporary stage, and loads them with `COPY INTO ... MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`. When appending (`adbc.ingest.mode.append` or `adbc.ingest.mode.create_append` with an existing table), the target table must have the same number of columns with matching names (case-insensitive); otherwise ingestion fails with `ALREADY_EXISTS`.

`adbc.ingest.mode.replace` loads into a staging table in the target schema and swaps it with the target (`ALTER TABLE ... SWAP WITH`) only after every file has been copied. If ingestion fails or is cancelled, the existing table and its data are left unchanged. As with any Snowflake table swap, grants on the replaced table are not carried over to the new data.

- `adbc.snowflake.statement.ingest_writer_concurrency`: number of Parquet writers. Defaults to the number of CPUs; `0` resets to the default.
- `adbc.snowflake.statement.ingest_upload_concurrency`: number of concurrent file uploads. Defaults to `8`; `0` resets to the default.
- `adbc.snowflake.statement.ingest_copy_concurrency`: number of concurrent background `COPY INTO` operations. Defaults to `4`; `0` removes the limit.
- `adbc.snowflake.statement.ingest_target_file_size`: approximate Parquet file size in bytes. Defaults to 10 MiB.
- `adbc.snowflake.statement.ingest_compression_codec`: `snappy` (default), `uncompressed`, `gzip`, `brotli`, `zstd`, or `lz4_raw`.
- `adbc.snowflake.statement.ingest_compression_level`: codec compression level; `-1` (default) uses the codec default.
- `adbc.snowflake.statement.ingest_use_vectorized_scanner`: use Snowflake's vectorized Parquet scanner. Defaults to `true`.
- `adbc.snowflake.statement.ingest_geo_type`: `geography` or `geometry` for GeoArrow input columns. When unset, the type is chosen from the column's CRS metadata.

### Result Types

- ARRAY columns carry the `arrow.json` extension type; UUID columns are returned as `arrow.uuid` (16-byte fixed-size binary).
- TIMESTAMP_LTZ columns use the session `TIMEZONE`; TIMESTAMP_TZ columns use UTC; TIMESTAMP_NTZ columns have no time zone.
- GetTableSchema reads `DESC TABLE` for `DATA_TYPE` and `PRIMARY_KEY` field metadata (plus `COMMENT` when set), while keeping Snowflake's Arrow type adjustments and session time zone. ARRAY and EWKB GeoArrow fields carry their extension metadata instead of these DESC keys, as in the Go driver.

### Cancellation

`AdbcStatementCancel` cancels the statement's in-progress query or ingestion. `AdbcConnectionCancel` cancels the connection's in-progress metadata or transaction operation (for example GetObjects, GetTableSchema, GetStatistics, commit, or rollback); it does not cancel statements, and the connection remains usable.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

{{ footnotes|safe }}

## Previous Versions

There are no previous published versions of this driver yet.

[snowflake]: https://www.snowflake.com/
