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
{}
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
        #"adbc.snowflake.sql.auth_type": "auth_jwt",
        #"adbc.snowflake.sql.client_option.jwt_private_key": "/path/to/rsa_key.p8",

        "adbc.snowflake.sql.account": "ACCOUNT-IDENT",
        "adbc.snowflake.sql.db": "SNOWFLAKE_SAMPLE_DATA",
        "adbc.snowflake.sql.schema": "TPCH_SF1",
        "adbc.snowflake.sql.warehouse": "MY_WAREHOUSE",
        "adbc.snowflake.sql.role": "MY_ROLE"
    }
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

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

{{ footnotes|safe }}

## Options

### Connection Options

`adbc.snowflake.sql.account`
: **Type:** string

  The Snowflake account name.

`adbc.snowflake.sql.auth_type`
: **Values:** (see table below). **Default:** `auth_snowflake`

  How to authenticate to Snowflake.

  | Auth Method        | Description                                                                                         |
  |--------------------|-----------------------------------------------------------------------------------------------------|
  | `auth_snowflake`   | username/password                                                                                   |
  | `auth_oauth`       | OAuth                                                                                               |
  | `auth_ext_browser` | use an external browser to access a FED and perform SSO auth                                        |
  | `auth_okta`        | use a native OKTA URL to perform SSO authentication on Okta                                         |
  | `auth_jwt`         | use a JWT                                                                                           |
  | `auth_mfa`         | username/password with MFA                                                                          |
  | `auth_pat`         | use a programmatic access token                                                                     |
  | `auth_wif`         | use Workload Identity Federation; must specify `adbc.snowflake.sql.client_option.identity_provider` |

`adbc.snowflake.sql.client_option.app_name`
: **Type:** string

  The application name to report to Snowflake.

`adbc.snowflake.sql.client_option.auth_token`
: **Type:** string

  The auth token to use.

`adbc.snowflake.sql.client_option.cache_mfa_token`
: **Type:** boolean

  Whether to cache the MFA token in the OS credential manager.

`adbc.snowflake.sql.client_option.client_timeout`
: **Type:** duration string (e.g. `300ms`, `1.5s`, or `1m30s`)

  Timeout for a network round trip plus reading the HTTP response. Uses Go's
  [`time.ParseDuration`](https://pkg.go.dev/time#ParseDuration) format; negative
  values are treated as their absolute value.

`adbc.snowflake.sql.client_option.config_file`
: **Type:** string (file path)

  Path to the gosnowflake client configuration file used for "easy logging"
  (controls the driver's log level and log output path).

`adbc.snowflake.sql.client_option.disable_telemetry`
: **Type:** boolean. **Default:** false

  When enabled, disables the driver's usage telemetry by setting the
  `CLIENT_TELEMETRY_ENABLED` session parameter to `false`.

`adbc.snowflake.sql.client_option.identity_provider`
: **Values:** `AWS`, `AZURE`, `GCP`, or `OIDC`

  The Workload Identity Federation provider used to generate the identity
  attestation. Must be set when `adbc.snowflake.sql.auth_type` is `auth_wif`.

`adbc.snowflake.sql.client_option.jwt_expire_timeout`
: **Type:** duration string (e.g. `300ms`, `1.5s`, or `1m30s`)

  How long a generated key-pair-authentication JWT remains valid. Uses Go's
  [`time.ParseDuration`](https://pkg.go.dev/time#ParseDuration) format; negative
  values are treated as their absolute value.

`adbc.snowflake.sql.client_option.jwt_private_key`
: **Type:** string (file path)

  Path to a file containing the RSA private key (PKCS#1 or PKCS#8, PEM or DER)
  used to sign the JWT for key-pair authentication (`auth_jwt`).

`adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_password`
: **Type:** string

  Passphrase used to decrypt an encrypted PKCS#8 private key supplied via
  `adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value`.

`adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value`
: **Type:** string (PEM)

  The RSA private key for key-pair authentication supplied inline as a PKCS#8
  PEM value, instead of as a file path. If the PEM is an encrypted private key,
  also set `adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_password`.

`adbc.snowflake.sql.client_option.keep_session_alive`
: **Type:** boolean. **Default:** false

  When enabled, keeps the Snowflake session alive (does not log it out) after the
  connection is closed.

`adbc.snowflake.sql.client_option.login_timeout`
: **Type:** duration string (e.g. `300ms`, `1.5s`, or `1m30s`)

  Login retry timeout, excluding network round trip and reading the HTTP
  response. Uses Go's [`time.ParseDuration`](https://pkg.go.dev/time#ParseDuration)
  format; negative values are treated as their absolute value.

`adbc.snowflake.sql.client_option.ocsp_fail_open_mode`
: **Type:** boolean. **Default:** true (fail-open)

  Controls OCSP certificate-revocation behavior. When enabled (fail-open), a
  revocation check that cannot complete (for example, an unreachable OCSP
  responder) is treated as a soft failure and the connection proceeds; when
  disabled (fail-closed), such a failure rejects the connection.

`adbc.snowflake.sql.client_option.okta_url`
: **Type:** string (URL)

  The native Okta URL (for example, `https://<org>.okta.com`) used for SSO when
  `adbc.snowflake.sql.auth_type` is `auth_okta`.

`adbc.snowflake.sql.client_option.request_timeout`
: **Type:** duration string (e.g. `300ms`, `1.5s`, or `1m30s`)

  Request retry timeout for non-login requests, excluding network round trip and
  reading the HTTP response. Uses Go's
  [`time.ParseDuration`](https://pkg.go.dev/time#ParseDuration) format; negative
  values are treated as their absolute value.

`adbc.snowflake.sql.client_option.store_temp_creds`
: **Type:** string

  Whether to cache the ID token in the OS credential manager.

`adbc.snowflake.sql.client_option.tls_skip_verify`
: **Type:** string

  (INSECURE) do not validate the server's TLS certificate.

`adbc.snowflake.sql.client_option.tracing`
: **Type:** string

  (Deprecated) set the log level.

`adbc.snowflake.sql.db`
: **Type:** string

  The database name to connect to.

`adbc.snowflake.sql.region`
: **Type:** string

  The warehouse region.

`adbc.snowflake.sql.role`
: **Type:** string

  The role to use.

`adbc.snowflake.sql.schema`
: **Type:** string

  The schema to connect to.

`adbc.snowflake.sql.uri.host`
: **Type:** string

  The Snowflake host to connect to. Normally derived from the account
  identifier; set this only to override the host (for example, a private-link or
  proxy hostname).

`adbc.snowflake.sql.uri.port`
: **Type:** int. **Default:** 443

  The port to connect to.

`adbc.snowflake.sql.uri.protocol`
: **Values:** `http` or `https`. **Default:** `https`

  The protocol scheme used for connections.

`adbc.snowflake.sql.warehouse`
: **Type:** string

  The warehouse to connect to.

### Options Affecting Queries

`adbc.rpc.result_queue_size`
: **Type:** int. **Default:** 100

  The max number of batches to buffer for each result stream. Batches are
  prefetched in parallel up to `adbc.snowflake.rpc.prefetch_concurrency`
  streams at a time.

  Can be set on the statement. For example:

  ```python
  with conn.cursor() as cur:
      cur.adbc_statement.set_options(**{"adbc.rpc.result_queue_size": "200"})
      cur.execute("SELECT * FROM my_table")
  ```

`adbc.snowflake.rpc.prefetch_concurrency`
: **Type:** int. **Default:** 5

  The max number of result streams to fetch in parallel. Each stream buffers up
  to `adbc.rpc.result_queue_size` batches.

  Can be set on the statement.

`adbc.snowflake.sql.client_option.max_timestamp_precision`
: **Values:** `nanoseconds`, `nanoseconds_error_on_overflow`, `microseconds`. **Default:** `nanoseconds`

  The Snowflake TIMESTAMP_LTZ, TIMESTAMP_NTZ, and TIMESTAMP_TZ types with nanosecond precision have a range greater than is possible to represent with Arrow nanosecond timestamps. This option controls what to do: `nanoseconds` will simply let the value overflow, `nanoseconds_error_on_overflow` will validate values and raise an error if they would overflow (at a performance cost), and `microseconds` will truncate to microseconds, which can represent the full range.

  Can be set on the database.

`adbc.snowflake.sql.client_option.stream_retry_enabled`
: **Type:** boolean. **Default:** false

  Whether to buffer data read, retrying on failure, or directly yield the underlying result stream. If enabled, transient network failures will be retried up to a fixed number of attempts.

  Can be set on the database, connection, and statement.

`adbc.snowflake.sql.client_option.use_high_precision`
: **Type:** boolean. **Default:** true

  For [NUMBER columns](https://docs.snowflake.com/en/sql-reference/data-types-numeric#number), whether to read as Arrow decimals, or as Arrow int64/float64. Note that when disabled, there is risk of data being truncated.

  Can be set on the database, connection, and statement.

`adbc.snowflake.statement.ingest_compression_codec`
: **Values:** `uncompressed`, `snappy`, `gzip`, `brotli`, `zstd`, or `lz4_raw` (case-insensitive). **Default:** `snappy`

  When ingesting, the compression codec to use for the Parquet files that are created and uploaded.

  Can be set on the statement.

`adbc.snowflake.statement.ingest_compression_level`
: **Type:** int. **Default:** the default level for the selected codec

  When ingesting, the codec-specific compression level for the Parquet files that are created. The valid range depends on the codec; some codecs (such as `snappy`) ignore it.

  Can be set on the statement.

`adbc.snowflake.statement.ingest_copy_concurrency`
: **Type:** int. **Default:** 4

  When ingesting, the max number of Parquet files to `COPY INTO` in parallel.

  Can be set on the statement.

`adbc.snowflake.statement.ingest_geo_type`
: **Values:** `geography` or `geometry`. **Default:** `geography`

  Which Snowflake data type to use when ingesting columns of GeoArrow extension types (`geoarrow.wkb`, `geoarrow.wkt`).

  Can be set on the statement.

`adbc.snowflake.statement.ingest_target_file_size`
: **Type:** int. **Default:** 10 MiB

  When ingesting, the approximate target size of Parquet files to create. The actual size will tend to be slightly larger; if set to 0 there is no limit.

  Can be set on the statement.

`adbc.snowflake.statement.ingest_upload_concurrency`
: **Type:** int. **Default:** 8

  When ingesting, the max number of Parquet files to upload in parallel.

  Can be set on the statement.

`adbc.snowflake.statement.ingest_use_vectorized_scanner`
: **Type:** boolean. **Default:** true

  Whether to pass [`USE_VECTORIZED_SCANNER=TRUE`](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table#label-use-vectorized-scanner) when ingesting data via `COPY INTO`.

`adbc.snowflake.statement.ingest_writer_concurrency`
: **Type:** int. **Default:** number of vCPUs detected

  When ingesting, the max number of Parquet files to write in parallel.

  Can be set on the statement.

`adbc.snowflake.statement.query_tag`
: **Type:** string. **Default:** (unset)

  A query tag to apply to queries, which can be used for monitoring.

  Can be set on the statement.

## Previous Versions

To see documentation for previous versions of this driver, see the following:

- [v1.10.3](./v1.10.3.md)
- [v1.10.1](./v1.10.1.md)
- [v1.10.0](./v1.10.0.md)

[snowflake]: https://www.snowflake.com/
