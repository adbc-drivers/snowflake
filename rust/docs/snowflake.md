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
- `snowflake://sys_admin@private.network.com:443/OPS_MONITOR/DBA?account=vpc-id-1234&client_session_keep_alive=true` (Uses full hostname, requires explicit account parameter)

`insecureMode` is not a gosnowflake v2 configuration key, so both drivers send it to Snowflake as a session parameter; it does not change TLS verification. `client_session_keep_alive=true` is a login session parameter that starts background heartbeats, as in the Go driver; it differs from `serverSessionKeepAlive`, which keeps the server session alive after the connection closes.

## Options

Database options use the same keys and values as the Go Snowflake driver, including the `adbc.snowflake.sql.auth_type` values (`auth_snowflake`, `auth_oauth`, `auth_ext_browser`, `auth_okta`, `auth_jwt`, `auth_mfa`, `auth_pat`, `auth_wif`). Durations such as `login_timeout` accept Go duration strings (for example `30s` or `1m30s`). Snowflake's client settings are whole seconds, so durations are rounded up; rounding down could turn a sub-second timeout into `0`, which means no timeout. As in Go, `0` selects the default for `login_timeout` (120 seconds; Go: 300), `client_timeout` (300 seconds; Go: 900) and `jwt_expire_timeout` (120 seconds; Go: 60), while `request_timeout` `0` means no timeout.

Unrecognized database option names outside the `adbc.` namespace are string-valued Snowflake session parameters: they are returned by `GetOption` and sent at login with names uppercased (as in Go). URI query keys not recognized as gosnowflake configuration are also session parameters; recognized configuration keys the Rust driver cannot honor fail with `NOT_IMPLEMENTED` rather than being sent to Snowflake. The driver always overrides user-supplied `PYTHON_CONNECTOR_QUERY_RESULT_FORMAT` with `ARROW_FORCE` and `GEOGRAPHY_OUTPUT_FORMAT` / `GEOMETRY_OUTPUT_FORMAT` with their corresponding explicit format options (default `GEOJSON`). Unknown `adbc.` options still fail with `NOT_IMPLEMENTED`, as does setting an unsupported Go driver option: `adbc.snowflake.sql.region`, `adbc.snowflake.sql.client_option.ocsp_fail_open_mode`, `adbc.snowflake.sql.client_option.use_high_precision`, or `adbc.snowflake.sql.client_option.stream_retry_enabled`. Invalid values for recognized options and non-string session parameter values fail with `INVALID_ARGUMENT`.

Unset database options return their defaults rather than an unknown-option error. Boolean options read back as `true` or `false`, as in Go; setters also accept `enabled`/`disabled`. The default authentication type is `auth_snowflake`, the retry count is `7`, and the application name is `ADBC Snowflake Driver (Rust)`. Timeout defaults are `120s` for login, `300s` for client requests, `120s` for JWT expiration, and `0s` (no limit) for requests. Explicitly set authentication types and durations read back as they were set; the Go driver instead returns native authenticator names (for example `SNOWFLAKE`) and durations as whole seconds.

Private-key options use the prefix `adbc.snowflake.sql.client_option.` and are parsed when set, as in the Go driver; invalid files, keys, or passwords fail immediately with `INVALID_ARGUMENT`. `jwt_private_key` reads an unencrypted RSA PKCS#1 or PKCS#8 file (DER or PEM) once, when set. `jwt_private_key_pkcs8_value` takes PKCS#8 PEM, unencrypted or PBES2-encrypted with AES-128/192/256-CBC or 3DES-CBC and PBKDF2-HMAC-SHA1, PBKDF2-HMAC-SHA256, or scrypt; Go's mislabeled "GCM" encodings are read as the CBC data its implementation produces, and genuine AES-GCM envelopes are not supported. RSA keys must be 2048–8192 bits (an AWS-LC limit). The last successfully set file or value replaces the previous key; a failed replacement keeps the previous one. When options are supplied at database creation, `jwt_private_key_pkcs8_password` may come before or after the encrypted value; afterwards, set the password before the encrypted value (changing the password later does not reparse the key). Unlike the Go driver, which rejects an encrypted value set after initialization, this works once the password is set.

All three private-key options, MFA passcodes, and OAuth client secrets are write-only: reading them fails with `NOT_FOUND`. Password, authentication-token, and proxy-password options remain readable, as in Go.

### Database and Connection Options

- `adbc.snowflake.sql.client_option.tls_skip_verify` (URI `disableOCSPChecks`): `true` or `false` (default). In the Go driver this only disables OCSP checks. The Rust driver performs no OCSP checks, so it validates and stores the value but neither value changes certificate-chain, hostname, or CRL verification. Configure CRL checking separately with `crl_check_mode` (URI `certRevocationCheckMode`), which is independent of this option, as in gosnowflake.
- `adbc.snowflake.sql.client_option.cache_mfa_token` and `adbc.snowflake.sql.client_option.store_temp_creds`: independent token-cache controls. `cache_mfa_token` applies to `auth_mfa`; `store_temp_creds` applies to the other authenticators (for example external browser and OAuth). When unset, caching is enabled on Windows and macOS and disabled elsewhere, as in Go; their getters return `false` until set.
- `adbc.snowflake.sql.client_option.max_retry_count`: retries per request. Unset or `0` selects 7 retries (8 attempts in total), as in Go. When the connection comes from a `connections.toml` profile, the profile's settings (or `sf_core`'s defaults) apply to the retry count, request timeout, and token caching unless set explicitly.
- `adbc.snowflake.sql.client_option.jwt_expire_timeout`: lifetime of the key-pair (`auth_jwt`) JWT, as a Go duration or seconds. `0` selects the default of 120 seconds (Go's default is 60 seconds).
- `adbc.snowflake.sql.client_option.max_timestamp_precision`: `nanoseconds` (default), `nanoseconds_error_on_overflow`, or `microseconds`. Also settable per statement.
- `adbc.snowflake.sql.client_option.geography_output_format` / `adbc.snowflake.sql.client_option.geometry_output_format`: `GeoJSON` (default) or `EWKB`, case-insensitive. Setting either on a connection applies it to the current session with `ALTER SESSION`. With `EWKB`, GEOGRAPHY and GEOMETRY columns are returned as binary `geoarrow.wkb` extension arrays; with `GeoJSON` they are strings.
- `adbc.connection.get_objects.match_exactly`: connection-only option, `false` (default) or `true`. When true, GetObjects catalog, schema, table, and column filters match literal names case-sensitively, including `%` and `_`; invalid values fail with `INVALID_ARGUMENT`. Without it, filters retain case-insensitive LIKE wildcard semantics.

GetObjects fetches catalog, schema, table, and column metadata by resolved scope, concurrently within each level, and filters the resulting names before narrowing subsequent scopes. Column-name filters are applied locally after fetching the original column positions: pushing those filters into Snowflake's `SHOW COLUMNS` would renumber the results. Constraints are fetched for the selected tables before the prefetched tree is handed to the `driverbase` GetObjects adapter; reading its Arrow batches does not issue more Snowflake requests.

At `columns` and `all` depths, the adapter retains original column ordinal positions and available remarks/XDBC metadata and includes PRIMARY KEY, FOREIGN KEY, and UNIQUE constraints with ordered column names and referenced catalog/schema/table/column information for foreign keys. At shallower depths, child lists remain null (`catalog_db_schemas` at `catalogs`, `db_schema_tables` at `schemas`, and table columns and constraints at `tables`).

As in the Go driver, a catalog, schema, or table scope is treated as empty when Snowflake reports vendor error 2003 or 2043 (object does not exist or is not authorized). A database or schema the role can list but not use (SQLSTATE `42501` on the scoped `SHOW`) is also treated as empty, which matches the Go driver's account-wide `SHOW` results. Any other metadata error, including other SQLSTATE `42000` failures, is returned to the caller.

GetObjects reports Snowflake's `INFORMATION_SCHEMA` type names in `xdbc_type_name` (for example `TEXT`, `NUMBER`, `FLOAT`, `BINARY`, `TIMESTAMP_NTZ`), as the Go driver does. `xdbc_column_size` is the character limit for TEXT, the numeric precision for NUMBER, and the byte limit for BINARY; `xdbc_char_octet_length` is set only for TEXT (four bytes per character, capped at Snowflake's 128 MiB limit). FLOAT precision, scale, and radix are null. TIME and TIMESTAMP columns report their fractional-second precision in `xdbc_datetime_sub`. `xdbc_data_type` holds the Arrow type ID and `xdbc_sql_data_type` the corresponding SQL type code, following this driver's schema mapping: scale-0 NUMBER is Int64 and reports `(9, -5)`, where the Go driver's default high-precision mode reports `(23, 3)`; TIME uses Time32 or Time64 according to its precision. As in Go, `xdbc_column_def` is null.

### Statement Options

- `snowflake.query.last_query_id`: read-only ID of the statement's latest executed query, or an empty string before the first. A failed query reports its ID when Snowflake assigned one; ExecuteSchema does not change it. Result streams also carry the ID in their schema metadata (and each batch's) under `SNOWFLAKE:query_id`; with bound parameters that is the first execution's ID. PUT/GET and bulk ingestion do not report a query ID.
- `adbc.snowflake.statement.query_tag`: string, unset by default. Snowflake `QUERY_TAG` applied to the statement's queries for monitoring, including every SQL statement and file upload issued by bulk ingestion.
- `adbc.snowflake.statement.query_timeout`: per-statement query timeout in seconds. Accepts a non-negative integer or a string such as `"30"` or `"30s"`. Set it to `0` to clear the override and use the Snowflake session/default timeout.
- `adbc.snowflake.rpc.prefetch_concurrency`: number of result chunks downloaded and decoded concurrently (positive integer). When unset, the session's `CLIENT_PREFETCH_THREADS` applies (default 4), which `GetOption` reports.
- `adbc.rpc.result_queue_size`: maximum number of decoded record batches buffered ahead of the reader (positive integer). Unlike Go, which bounds batches per result stream, this bounds the total across chunks; a chunk with more batches than the limit is still admitted on its own. When unset there is no batch limit beyond the prefetch window, and `GetOption` fails with `NOT_FOUND`.

Bound Float16, Float32, and Float64 parameters are sent as Snowflake `REAL` values, and only Arrow nulls become SQL `NULL`. NaN, infinities, and `-0` are preserved: infinities are sent as `Infinity`/`-Infinity`, because Snowflake rejects the `+Inf`/`-Inf` spelling that gosnowflake v2.4 sends (so the Go driver fails to bind infinities). Float64 values keep their full precision (gosnowflake v2.4 formats scalar float binds with 32-bit precision).

### Bulk Ingestion Options

Bulk ingestion writes the bound data to Parquet files, uploads them concurrently to a temporary stage, and loads them with `COPY INTO ... MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`. When appending (`adbc.ingest.mode.append` or `adbc.ingest.mode.create_append` with an existing table), the target table must have the same number of columns with matching names (case-insensitive); otherwise ingestion fails with `ALREADY_EXISTS`.

The standard target table, catalog, schema, and mode options use `driverbase::bulk_ingest::BulkIngestState`. Invalid modes are rejected with `INVALID_ARGUMENT` when the option is set, rather than when ingestion starts. Setting a SQL query clears the target table but retains the catalog, schema, and mode.

Arrow `Time32(Second)` input is converted to millisecond units in the Parquet files before `COPY INTO`; the Snowflake target remains `TIME(0)` and values retain their second precision.

`adbc.ingest.mode.replace` loads into a staging table in the target schema and swaps it with the target (`ALTER TABLE ... SWAP WITH`) only after every file has been copied. If ingestion fails or is cancelled, the existing table and its data are left unchanged. As with any Snowflake table swap, grants on the replaced table are not carried over to the new data.

An empty input stream succeeds with zero affected rows, as in the Go driver. Table creation still runs when the mode requires it, and replace mode replaces the target with an empty table.

`adbc.ingest.temporary`: `true` restricts ingestion to session-local Snowflake temporary tables; `false` is the default. The Go driver does not support it. As the ADBC specification allows, it cannot be combined with `adbc.ingest.target_catalog` or `adbc.ingest.target_db_schema` (`INVALID_ARGUMENT`); temporary tables are created in the session's current schema. `append` requires an existing temporary table (`NOT_FOUND` otherwise); `create` fails with `ALREADY_EXISTS` only when a temporary table already exists; `create_append` appends to an existing temporary table or creates one; `replace` replaces only the temporary table, loading a temporary staging table and swapping it in, so a failed replace leaves the existing temporary table unchanged. A permanent table of the same name is shadowed and never modified, in every mode.

When ingestion fails or is cancelled after loading started, the driver counts the target table again (on a fresh, non-cancelled request carrying the statement's query tag), as the Go driver does, and reports the rows loaded before the failure in the error message and in the error detail `adbc.snowflake.ingest.rows_loaded`. A failed replace reports 0 because the target was not replaced. If the recount itself fails, the original error is returned unchanged. (The Go driver's C API drops this count on error.)

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
- Field metadata follows the Go driver on each path:
  - Arrow result sets keep the metadata Snowflake sends (for example `logicalType`, `precision`, `scale`, `charLength`, `byteLength`); no `SNOWFLAKE_TYPE` is added.
  - JSON-format results, schema-only results, and ExecuteSchema carry `SNOWFLAKE_TYPE` set to Snowflake's column type.
  - ARRAY columns carry only the `arrow.json` extension; UUID columns add the `arrow.uuid` extension. To stay readable by arrow-rs, `arrow.json` includes an empty `ARROW:extension:metadata` and `arrow.uuid` has none; the Go driver omits the key for `arrow.json` and writes an empty one for `arrow.uuid`.
  - GetTableSchema reads `DESC TABLE` for `DATA_TYPE` and `PRIMARY_KEY` (plus `COMMENT` when set), keeping Snowflake's Arrow type adjustments and session time zone. ARRAY and EWKB GeoArrow fields carry their extension metadata instead of these keys, and GEOGRAPHY uses `EPSG:4326` with spherical edges.
- With `EWKB` output, GEOGRAPHY and GEOMETRY columns are `geoarrow.wkb`. As in the Go driver, the column CRS comes from the first batch only: one consistent nonzero SRID becomes `EPSG:<srid>`; mixed SRIDs, no SRIDs, or an empty first batch leave the CRS unset. Every value's EWKB SRID prefix is stripped to plain WKB, including values whose SRID differs from the column CRS.
- Results Snowflake returns as JSON chunks (for example from some `SHOW` commands) are validated as in the Go driver: truncated rows, unexpected content between rows, wrong column counts, and chunk row-count mismatches are errors, and invalid UTF-8 in string values is replaced with U+FFFD.

### Cancellation

`AdbcStatementCancel` cancels the statement's in-progress query or ingestion, and also a result stream from that statement that is still being read (the ADBC specification allows cancelling while the stream is consumed; the Go driver stops tracking the query once `ExecuteQuery` returns). When nothing is running and no result stream is open, it fails with `INVALID_STATE` and the message `AdbcStatementCancel: no active query to cancel`, as in the Go driver. `AdbcConnectionCancel` always succeeds, as in Go, and cancels the connection's in-progress metadata operation (for example GetObjects, GetTableSchema, or GetStatistics); it does not cancel statements, and the connection remains usable. As in the Go driver, commit and rollback are never cancelled, even when cancellation is requested before or during them, so a cancel cannot leave the transaction outcome unknown. A cancel requested during commit or rollback does not carry over to later operations.

### Error Messages

As in the Go driver, errors that carry a Snowflake server diagnostic (query failures and login rejections) are prefixed `[snowflake]` and end with `(query ID: <id>)` when Snowflake assigned one; errors raised by the driver itself are prefixed `[Snowflake]`.

## Telemetry

The cross-driver exporter, traceparent validation, and span lifecycle are provided by `driverbase-rs`'s `telemetry` feature; Snowflake-specific integration attaches its `sf_core` child spans to the same traces.

Set `adbc.telemetry.traces_exporter` on the database to select an OpenTelemetry trace exporter: `none`, `otlp`, `console` (one JSON span per stdout line), or `adbcfile`. This option takes precedence over `OTEL_TRACES_EXPORTER`; when neither is set, no ADBC-local traces are exported. An unsupported value fails with `INVALID_ARGUMENT`. An empty value unsets the option, so `OTEL_TRACES_EXPORTER` applies. `otlp` sends batched HTTP/protobuf spans (not gRPC) and reads the `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_EXPORTER_OTLP_TRACES_HEADERS`, `OTEL_EXPORTER_OTLP_TIMEOUT`, and `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT` environment variables. The OpenTelemetry Rust 0.30 HTTP transport does not implement the compression environment variables.

The exporter is shared by every database in the process: the exporter chosen when a connection opens applies to all databases' spans until another database that sets these options opens a connection. A database that sets neither option keeps the active exporter instead of disabling it.

With `adbcfile`, `adbc.telemetry.traces_folder_path` selects the output directory (created if needed). The default is `~/Library/Application Support/ADBC/Traces` on macOS, `%LOCALAPPDATA%/ADBC/Traces` on Windows, and `$XDG_STATE_HOME/adbc/traces` on Linux and other platforms (or `~/.local/state/adbc/traces` if `XDG_STATE_HOME` is unset). JSONL trace files named `apache.arrow.adbc.snowflake-<UTC timestamp>.jsonl` rotate at 1024 KiB, retaining at most 100 files. The folder option has no effect with other exporters.

Set `adbc.telemetry.trace_parent` to a W3C `traceparent` value on a database, connection, or statement to parent operation spans. Connections inherit the database value when created; statements inherit their connection value when created. A value on a more specific object overrides its inherited value. Set the option to an empty string to clear it on that object; an empty statement value falls back to its inherited connection value.

The tracer is `apache.arrow.adbc.snowflake` (version: driver crate version) with resource `service.name=apache.arrow.adbc`. ADBC operation spans are `databaseImpl.Open`, `connectionImpl.Close`, `connectionImpl.Release`, `connectionImpl.GetObjects`, `connectionImpl.GetTableSchema`, `connectionImpl.GetInfo`, `connectionImpl.GetTableTypes`, `connectionImpl.GetStatistics`, `connectionImpl.Commit`, `connectionImpl.Rollback`, `statement.ExecuteQuery`, `statement.ExecuteUpdate`, `statement.ExecuteSchema`, `statement.Ingest`, and `statement.Close`. Local traces also include `sf_core` child spans for connection, query, and result work. Every span includes `db.system.name=snowflake` and `db.operation.name=<span name>`; failures record an error event, status, and `error.type`. Execution spans include `snowflake.query.id` when available and `db.response.returned_rows` (or `-1` when the row count is unknown). Query text is not recorded in ADBC spans or mirrored `sf_core` spans.

These ADBC exporters are separate from `sf_core`'s in-band Snowflake telemetry. `adbc.snowflake.sql.client_option.disable_telemetry` disables the in-band telemetry upload, not local ADBC trace export; `sf_core` spans may still appear in an `adbcfile` trace even when in-band telemetry is disabled.

### Logging

The database option `adbc.snowflake.sql.client_option.tracing` (URI key `tracing`) sets the process-wide log level for `sf_core` and this driver's own messages (including option warnings), as the Go driver's option sets gosnowflake's global logger. It does not affect driverbase logging or OpenTelemetry exporters. Accepted levels are `trace`, `debug`, `info`, `warn`, `error`, `fatal`, and `off`, case-insensitive. Setting the option only stores it; each connection attempt reapplies its database's level, as in Go. An unset option resets logging to `fatal`; an explicitly empty string leaves the active level unchanged; an invalid level fails connection creation with `IO`. During database initialization, URI `tracing` is reset to `fatal` unless the tracing option is also supplied. Before the first connection, the logger uses `warn`, as the Go driver sets at load time.

Without easy logging, output goes to stderr in Go's slog text form: `time=<RFC3339> level=<LEVEL> source=<file>:<line> msg="..."` followed by the event's fields; TRACE is written as `DEBUG-4`. Messages, source locations, and field names come from `sf_core` and this driver, not gosnowflake. `sf_core` has no fatal-level events, so `fatal` and `off` produce no output.

Easy logging is configured with `adbc.snowflake.sql.client_option.config_file` (URI `clientConfigFile` or `client_config_file`) and read when connecting. The configuration file is the explicit path, else `SF_CLIENT_CONFIG_FILE`, else `sf_client_config.json` beside the executable, else in the home directory. The JSON needs a `common` object; its `log_level` accepts the levels above except `fatal` (omitted means `off`), and output is appended to `<log_path>/go/snowflake.log` (default `<home>/go/snowflake.log`; `STDOUT` is treated as a directory name, as in gosnowflake). On Unix the file must belong to the current user, must not be group- or other-writable, and must not be a symlink. As in gosnowflake, easy logging is attempted once per process, except that an initial attempt without an explicit path allows one later explicit configuration; parse or directory errors do not use up the attempt, but a failure to open the log file does. On the configuring connection `common.log_level` overrides `tracing`; later connections reapply their own `tracing` level (including the default `fatal`) and keep writing to the file.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

{{ footnotes|safe }}

## Previous Versions

There are no previous published versions of this driver yet.

[snowflake]: https://www.snowflake.com/
