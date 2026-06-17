# Native C# Snowflake Driver for Apache Arrow ADBC

A from-scratch C# implementation of an [ADBC](https://arrow.apache.org/adbc/) driver for
Snowflake. Unlike the Interop package (which loads the Go driver through a native library), this
driver talks to Snowflake's REST API directly from managed code and returns results as Apache
Arrow record batches.

- **Target framework:** .NET 8.0
- **Assembly / package:** `AdbcDrivers.Snowflake.Native`
- **Result format:** Arrow end-to-end (Snowflake's Arrow wire format, streamed in chunks with
  bounded parallel prefetch)

## Quick start (ADBC API)

Connections are configured with an ADBC parameter dictionary:

```csharp
using AdbcDrivers.Snowflake.Native;

var parameters = new Dictionary<string, string>
{
    ["adbc.snowflake.sql.account"] = "myorg-myaccount",
    ["username"] = "MYUSER",
    ["password"] = "...",
    ["adbc.snowflake.sql.warehouse"] = "COMPUTE_WH",
    ["adbc.snowflake.sql.db"] = "MYDB",
    ["adbc.snowflake.sql.schema"] = "PUBLIC",
};

var driver = new SnowflakeDriver();
using var database = driver.Open(parameters);
using var connection = database.Connect(new Dictionary<string, string>());
using var statement = connection.CreateStatement();

statement.SqlQuery = "SELECT N_NATIONKEY, N_NAME FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION";
var result = statement.ExecuteQuery();          // or await statement.ExecuteQueryAsync()

using var stream = result.Stream!;              // IArrowArrayStream
while (await stream.ReadNextRecordBatchAsync() is { } batch)
{
    using (batch)
    {
        // process the Arrow RecordBatch
    }
}
```

DML goes through `ExecuteUpdate()` (returns the affected-row count); a long-running query can be
aborted from another thread with `statement.Cancel()`.

### Bind parameters

`?` placeholders bind positionally from an Arrow batch. A single-row batch binds scalar values:

```csharp
statement.SqlQuery = "SELECT * FROM ORDERS WHERE O_ORDERKEY = ? AND O_ORDERDATE > ?";
var schema = new Schema(
    [new Field("k", Int64Type.Default, true), new Field("d", Date32Type.Default, true)], null);
using var batch = new RecordBatch(schema,
    [
        new Int64Array.Builder().Append(42).Build(),
        new Date32Array.Builder().Append(new DateTime(2024, 1, 1)).Build(),
    ], 1);
statement.Bind(batch, schema);
```

A **multi-row** batch uses Snowflake's array binding (`executemany`): each parameter is sent as
one value array and the server executes the statement once per row — a whole batch in a single
round trip:

```csharp
statement.SqlQuery = "INSERT INTO ORDERS (O_ORDERKEY, O_ORDERDATE) VALUES (?, ?)";
using var batch = new RecordBatch(schema,
    [
        new Int64Array.Builder().Append(1).Append(2).Append(3).Build(),
        new Date32Array.Builder()
            .Append(new DateTime(2024, 1, 1)).Append(new DateTime(2024, 1, 2)).AppendNull().Build(),
    ], 3);
statement.Bind(batch, schema);
var result = statement.ExecuteUpdate();   // result.AffectedRows == 3
```

Supported bind types (identical for scalar and array binds, nulls preserved per row): Boolean,
Int8–64 / UInt8–64, Float/Double, Decimal128/256, String, Binary, Date32/64, Time32/64,
Timestamp. A bound batch stays attached to the statement across executions until replaced.

### Authentication

Select with `adbc.snowflake.sql.auth_type`. Canonical values follow the
[ADBC Snowflake driver reference](https://arrow.apache.org/adbc/current/driver/snowflake.html);
the connector-net-style spellings in parentheses are accepted as aliases:

| `auth_type` | Method | Additional keys |
|---|---|---|
| `auth_snowflake` (`snowflake`) — default | Username/password | `username`, `password` |
| `auth_jwt` (`snowflake_jwt`, `jwt`) | RSA key pair | `…client_option.jwt_private_key` (path to PEM file) or `…client_option.jwt_private_key_pkcs8_value` (inline PEM), + `…_pkcs8_password` for encrypted keys |
| `auth_oauth` (`oauth`) | OAuth 2.0 access token | `…client_option.auth_token` |
| `auth_pat` (`programmatic_access_token`, `pat`) | Programmatic access token (requires the user to be under a network policy) | `…client_option.auth_token` |
| `auth_ext_browser` (`externalbrowser`) | Browser-based SSO | — |

`auth_okta`, `auth_mfa`, and `auth_wif` are recognized as canonical ADBC values but not yet
supported.

### ADO.NET client

The driver also works behind the `Apache.Arrow.Adbc.Client` `DbConnection` layer:

```csharp
using AdbcClient = Apache.Arrow.Adbc.Client;

using var connection = new AdbcClient.AdbcConnection(
    new SnowflakeDriver(), parameters, new Dictionary<string, string>());
connection.Open();
using var command = connection.CreateCommand();
command.CommandText = "SELECT 1";
using var reader = command.ExecuteReader();
```

Note: `NUMBER(38,0)` surfaces through the client as `System.Data.SqlTypes.SqlDecimal`
(a CLR `decimal` cannot hold 38 digits); narrower precisions surface as `int`/`long`.

### Custom HTTP handling

Consumers who need control over the HTTP stack (corporate proxies, custom TLS, resilience
handlers, DNS rotation) construct `SnowflakeDatabase` directly and pass an
`HttpMessageHandler`. The driver always owns the `HttpClient` it builds on top; the caller
keeps ownership of the handler, which must outlive the database:

```csharp
// DNS-rotation-friendly without any DI infrastructure:
using var handler = new SocketsHttpHandler { PooledConnectionLifetime = TimeSpan.FromMinutes(5) };
using var database = new SnowflakeDatabase(parameters, handler);

// Or, in a DI app, hand the pooling problem to the factory:
var handler = httpMessageHandlerFactory.CreateHandler();   // IHttpMessageHandlerFactory
using var database = new SnowflakeDatabase(parameters, handler);
```

When a custom handler is supplied, the network options (`…client_option.tls_skip_verify`,
`…client_option.no_proxy`) are **not** applied — the handler is the caller's configuration
(a warning is logged if both are present and a logger factory was provided).

### Logging

Pass an `ILoggerFactory` to the `SnowflakeDatabase` constructor. Like the handler, it can't ride the ADBC string-dictionary `Open` — logging
requires constructing the database directly. The driver references
`Microsoft.Extensions.Logging.Abstractions` only and never builds a provider; without a factory
it logs nothing. Connection lifecycle and query execution log at Debug/Information; otherwise
best-effort failures — transport retries on transient errors, keep-alive heartbeat failures,
pool-maintenance errors — surface at Warning.

## Connection options

Keys follow the [ADBC Snowflake driver reference](https://arrow.apache.org/adbc/current/driver/snowflake.html)
where an official key exists; pool keys are this driver's own (`adbc.snowflake.pool.*`).

| Key | Meaning | Default |
|-----|---------|---------|
| `adbc.snowflake.sql.account` | Account identifier (**required**) | — |
| `username` / `password` | Credentials for password auth | — |
| `adbc.snowflake.sql.db` / `.schema` / `.warehouse` / `.role` | Session context | — |
| `adbc.snowflake.statement.query_tag` | Query tag shown in the Snowsight query history; connection-level default, overridable per statement via `SetOption` | — |
| `adbc.connection.catalog` / `adbc.connection.db_schema` | Canonical ADBC current catalog/schema (take precedence over the `sql.db`/`sql.schema` aliases) | — |
| `adbc.snowflake.sql.auth_type` | See Authentication above | `snowflake` |
| `adbc.snowflake.sql.client_option.jwt_private_key` | Key-pair auth: path to the private-key PEM file | — |
| `adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value` / `_password` | Key-pair auth: inline PEM / passphrase for encrypted keys | — |
| `adbc.snowflake.sql.client_option.auth_token` | Access token (OAuth or PAT, per `auth_type`) | — |
| `adbc.snowflake.sql.uri.host` / `.port` / `.protocol` | Endpoint override (PrivateLink etc.) | account URL |
| `adbc.snowflake.sql.client_option.tls_skip_verify` | Skip TLS certificate validation (**test only**) | `false` |
| `adbc.snowflake.sql.client_option.no_proxy` | Bypass the system proxy | `false` |
| `adbc.snowflake.sql.client_option.request_timeout` | Per-statement timeout, seconds (`STATEMENT_TIMEOUT_IN_SECONDS`) | 300 |
| `adbc.snowflake.sql.client_option.login_timeout` | Login/auth round-trip timeout, seconds | 60 |
| `adbc.snowflake.sql.client_option.enable_compression` | gzip/deflate response compression | `true` |
| `adbc.snowflake.sql.client_option.keep_session_alive` | Heartbeat idle pooled sessions so they never lapse to master-token expiry | `false` |
| `adbc.snowflake.sql.client_option.keep_session_alive_heartbeat_frequency` | Heartbeat interval, seconds (clamped 900–3600) | 3600 |
| `adbc.snowflake.rpc.prefetch_concurrency` | Parallel result-chunk downloads | 10 |
| `adbc.snowflake.pool.max_size` | Max pooled connections per distinct config | 10 |
| `adbc.snowflake.pool.idle_timeout` | Idle eviction (seconds or `30s`/`10m`/`1h`) | 10m |
| `adbc.snowflake.pool.acquire_timeout` | Max wait for a free connection when the pool is full | 120s |
| `adbc.snowflake.pool.max_lifetime` | Max connection lifetime | 1h |

## Sessions and pooling

Connections are pooled per distinct configuration (account, user, credential fingerprint,
database/schema/warehouse/role, endpoint). Session tokens (~1 h) are renewed transparently from
the master token when a query hits expiry; with `keep_session_alive` enabled, idle pooled
connections are heartbeated in the background so the ~4 h master window rolls forward
indefinitely. Server-side sessions are closed when the pool discards a connection.

## Testing

The suite is split by xUnit trait so the offline half runs anywhere (including CI) with no
Snowflake account:

| Category | What it covers | Requires |
|---|---|---|
| `Unit` (~150 tests) | Offline: type mapping and bind wire formats, option parsing, pool scheduling (deterministic via an injected `TimeProvider`/fake clock), chunk-prefetch back-pressure (fake HTTP client serving in-memory Arrow), result-decode fixups, request-body construction | Nothing |
| `Integration` (~77 tests) | Live against a real account: connect/lifecycle, statements + binds + cancellation, the wire type-decode matrix (`SELECT <literal>` per type), metadata/`GetObjects` content checks against `SNOWFLAKE_SAMPLE_DATA` (TPC-H), the ADO.NET client layer, session renewal + heartbeat | `SNOWFLAKE_TEST_CONFIG_FILE` → JSON config (account, credentials, warehouse; a **writable** database/schema for the DML and client tests) |

```bash
dotnet test csharp/test/Native --filter "Category=Unit"
SNOWFLAKE_TEST_CONFIG_FILE=/path/to/config.json dotnet test csharp/test/Native --filter "Category=Integration"
```

Per-file breakdown: [test suite readme](../../test/Native/readme.md). Session-lifecycle
behaviour (token renewal and keep-alive heartbeats) was validated with a long-running console
harness over 6–13 h live runs; the harness has since been removed.

## Performance

Benchmarked head-to-head against the Go driver (loaded via the Interop package) using an
**identical harness** — same query, same row limits, measuring execute + drain of every Arrow
batch (`BenchmarkTests` in both test suites; `csharp/run_benchmark.ps1` automates the
comparison).

Representative results (multi-run means — native n=5, interop n=10 — 2026-07-14, Release/net8.0):

| Rows fetched | Native C# | Interop (Go) | Native / Interop |
|---|---|---|---|
| 100 | 143 ms | 116 ms | 1.23× |
| 1,000 | 244 ms | 306 ms | 0.80× |
| 1,000,000 | 2,813 ms | 3,699 ms | **0.76×** |

At scale the native driver runs at parity or ahead of the Go driver (bounded parallel chunk
prefetch, pre-sized buffers, per-column decode loops); the small-query gap is
connection-establishment overhead, not the data path.

**Environment:** Intel Core Ultra 9 285H (16 cores / 16 threads), 32 GB RAM, Windows 11,
.NET 8 Release; default warehouse. Wall-clock over a live network varies heavily between runs —
in this session the *unchanged* Go driver's 1M-row runs ranged 2.2–9.8 s (sd ≈ 2.5 s) — so all
comparisons use interleaved native/interop runs and multi-run means, never cross-session
absolutes.

## Known Limitations

- **Live catalog/schema switching**: catalog/schema are honored at `Connect`; `SetOption` after
  connect supports only `adbc.connection.autocommit`.
- **Not implemented**: PUT/GET stage file transfer
- Semi-structured types (VARIANT/OBJECT/ARRAY) are returned as JSON strings; GEOGRAPHY/GEOMETRY as GeoJSON strings.
- Very slow consumption of very large results can outlive the chunk URLs' presigned validity.
- OTEL Tracing to be added
- Additional Auth methods

## License

Licensed under the Apache License, Version 2.0.
