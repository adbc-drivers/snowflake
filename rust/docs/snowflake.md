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

Unrecognized database option names outside the `adbc.` namespace are string-valued Snowflake session parameters: they are returned by `GetOption` and sent at login with names uppercased (as in Go). URI query keys not recognized as gosnowflake configuration are also session parameters; recognized configuration keys the Rust driver cannot honor fail with `NOT_IMPLEMENTED` rather than being sent to Snowflake. The driver always overrides user-supplied `PYTHON_CONNECTOR_QUERY_RESULT_FORMAT` with `ARROW_FORCE` and `GEOGRAPHY_OUTPUT_FORMAT` / `GEOMETRY_OUTPUT_FORMAT` with their corresponding explicit format options (default `GEOJSON`). Unknown `adbc.` options still fail with `NOT_IMPLEMENTED`, as do unsupported Go driver options including `adbc.snowflake.sql.region`, `adbc.snowflake.sql.client_option.ocsp_fail_open_mode`, `adbc.snowflake.sql.client_option.tracing`, `adbc.snowflake.sql.client_option.config_file`, `adbc.snowflake.sql.client_option.use_high_precision`, and `adbc.snowflake.sql.client_option.stream_retry_enabled`. Invalid values for recognized options and non-string session parameter values fail with `INVALID_ARGUMENT`.

### Database and Connection Options

- `adbc.snowflake.sql.client_option.jwt_expire_timeout`: lifetime of the key-pair (`auth_jwt`) JWT, as a Go duration or seconds. `0` selects the default of 120 seconds (Go's default is 60 seconds).
- `adbc.snowflake.sql.client_option.max_timestamp_precision`: `nanoseconds` (default), `nanoseconds_error_on_overflow`, or `microseconds`. Also settable per statement.
- `adbc.snowflake.sql.client_option.geography_output_format` / `adbc.snowflake.sql.client_option.geometry_output_format`: `GeoJSON` (default) or `EWKB`, case-insensitive. Setting either on a connection applies it to the current session with `ALTER SESSION`. With `EWKB`, GEOGRAPHY and GEOMETRY columns are returned as binary `geoarrow.wkb` extension arrays; with `GeoJSON` they are strings.
- `adbc.connection.get_objects.match_exactly`: connection-only option, `false` (default) or `true`. When true, GetObjects catalog, schema, table, and column filters match literal names case-sensitively, including `%` and `_`; invalid values fail with `INVALID_ARGUMENT`. Without it, filters retain case-insensitive LIKE wildcard semantics.

GetObjects fetches catalog, schema, table, and column metadata by resolved scope, concurrently within each level, and filters the resulting names before narrowing subsequent scopes. Column-name filters are applied locally after fetching the original column positions: pushing those filters into Snowflake's `SHOW COLUMNS` would renumber the results. Constraints are fetched for the selected tables before the prefetched tree is handed to the `driverbase` GetObjects adapter; reading its Arrow batches does not issue more Snowflake requests.

At `columns` and `all` depths, the adapter retains original column ordinal positions and available remarks/XDBC metadata and includes PRIMARY KEY, FOREIGN KEY, and UNIQUE constraints with ordered column names and referenced catalog/schema/table/column information for foreign keys. At shallower depths, child lists remain null (`catalog_db_schemas` at `catalogs`, `db_schema_tables` at `schemas`, and table columns and constraints at `tables`).

### Statement Options

- `adbc.snowflake.statement.query_timeout`: per-statement query timeout in seconds. Accepts a non-negative integer or a string such as `"30"` or `"30s"`. Set it to `0` to clear the override and use the Snowflake session/default timeout.
- `adbc.snowflake.rpc.prefetch_concurrency`: number of result chunks downloaded and decoded concurrently (positive integer). When unset, the session's `CLIENT_PREFETCH_THREADS` applies (default 4), which `GetOption` reports.
- `adbc.rpc.result_queue_size`: maximum number of decoded record batches buffered ahead of the reader (positive integer). Unlike Go, which bounds batches per result stream, this bounds the total across chunks; a chunk with more batches than the limit is still admitted on its own. When unset there is no batch limit beyond the prefetch window, and `GetOption` fails with `NOT_FOUND`.

### Bulk Ingestion Options

Bulk ingestion writes the bound data to Parquet files, uploads them concurrently to a temporary stage, and loads them with `COPY INTO ... MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`. When appending (`adbc.ingest.mode.append` or `adbc.ingest.mode.create_append` with an existing table), the target table must have the same number of columns with matching names (case-insensitive); otherwise ingestion fails with `ALREADY_EXISTS`.

The standard target table, catalog, schema, and mode options use `driverbase::bulk_ingest::BulkIngestState`. Invalid modes are rejected with `INVALID_ARGUMENT` when the option is set, rather than when ingestion starts. Setting a SQL query clears the target table but retains the catalog, schema, and mode.

Arrow `Time32(Second)` input is converted to millisecond units in the Parquet files before `COPY INTO`; the Snowflake target remains `TIME(0)` and values retain their second precision.

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

- NUMBER columns with a nonzero scale are returned as exact `Decimal128(precision, scale)`. Scale-0 NUMBER result batches keep the signed integer width Snowflake sends (Int8, Int16, Int32, or Int64, depending on the values), while schema-only inspection (ExecuteSchema, GetTableSchema, and describe) reports Int64. A schema-only result can therefore differ from the subsequent result batch.
- ARRAY columns carry the `arrow.json` extension type; UUID columns are returned as `arrow.uuid` (16-byte fixed-size binary).
- TIMESTAMP_LTZ columns use the session `TIMEZONE`; TIMESTAMP_TZ columns use UTC; TIMESTAMP_NTZ columns have no time zone.
- GetTableSchema reads `DESC TABLE` for `DATA_TYPE` and `PRIMARY_KEY` field metadata (plus `COMMENT` when set), while keeping Snowflake's Arrow type adjustments and session time zone. ARRAY and EWKB GeoArrow fields carry their extension metadata instead of these DESC keys, as in the Go driver.

### Cancellation

`AdbcStatementCancel` cancels the statement's in-progress query or ingestion. `AdbcConnectionCancel` cancels the connection's in-progress metadata or transaction operation (for example GetObjects, GetTableSchema, GetStatistics, commit, or rollback); it does not cancel statements, and the connection remains usable.

## Telemetry

The cross-driver exporter, traceparent validation, and span lifecycle are provided by `driverbase-rs`'s `telemetry` feature; Snowflake-specific integration attaches its `sf_core` child spans to the same traces.

Set `adbc.telemetry.traces_exporter` on the database to select an OpenTelemetry trace exporter: `none`, `otlp`, `console` (one JSON span per stdout line), or `adbcfile`. This option takes precedence over `OTEL_TRACES_EXPORTER`; when neither is set, no ADBC-local traces are exported. An unsupported value fails with `INVALID_ARGUMENT`. `otlp` sends batched HTTP/protobuf spans (not gRPC) and reads the `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_EXPORTER_OTLP_TRACES_HEADERS`, `OTEL_EXPORTER_OTLP_TIMEOUT`, and `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT` environment variables. The OpenTelemetry Rust 0.30 HTTP transport does not implement the compression environment variables.

The exporter is shared by every database in the process: the exporter chosen when a connection opens applies to all databases' spans until another database that sets these options opens a connection. A database that sets neither option keeps the active exporter instead of disabling it.

With `adbcfile`, `adbc.telemetry.traces_folder_path` selects the output directory (created if needed). The default is `~/Library/Application Support/ADBC/Traces` on macOS, `%LOCALAPPDATA%/ADBC/Traces` on Windows, and `$XDG_STATE_HOME/adbc/traces` on Linux and other platforms (or `~/.local/state/adbc/traces` if `XDG_STATE_HOME` is unset). JSONL trace files named `apache.arrow.adbc.snowflake-<UTC timestamp>.jsonl` rotate at 1024 KiB, retaining at most 100 files. The folder option has no effect with other exporters.

Set `adbc.telemetry.trace_parent` to a W3C `traceparent` value on a database, connection, or statement to parent operation spans. Connections inherit the database value when created; statements inherit their connection value when created. A value on a more specific object overrides its inherited value. Set the option to an empty string to clear it on that object; an empty statement value falls back to its inherited connection value.

The tracer is `apache.arrow.adbc.snowflake` (version: driver crate version) with resource `service.name=apache.arrow.adbc`. ADBC operation spans are `databaseImpl.Open`, `connectionImpl.Close`, `connectionImpl.Release`, `connectionImpl.GetObjects`, `connectionImpl.GetTableSchema`, `connectionImpl.GetInfo`, `connectionImpl.GetTableTypes`, `connectionImpl.GetStatistics`, `connectionImpl.Commit`, `connectionImpl.Rollback`, `statement.ExecuteQuery`, `statement.ExecuteUpdate`, `statement.ExecuteSchema`, `statement.Ingest`, and `statement.Close`. Local traces also include `sf_core` child spans for connection, query, and result work. Every span includes `db.system.name=snowflake` and `db.operation.name=<span name>`; failures record an error event, status, and `error.type`. Execution spans include `snowflake.query.id` when available and `db.response.returned_rows` (or `-1` when the row count is unknown). Query text is not recorded in ADBC spans or mirrored `sf_core` spans.

These ADBC exporters are separate from `sf_core`'s in-band Snowflake telemetry. `adbc.snowflake.sql.client_option.disable_telemetry` disables the in-band telemetry upload, not local ADBC trace export; `sf_core` spans may still appear in an `adbcfile` trace even when in-band telemetry is disabled.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

{{ footnotes|safe }}

## Previous Versions

There are no previous published versions of this driver yet.

[snowflake]: https://www.snowflake.com/
