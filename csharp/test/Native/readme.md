# Native Snowflake ADBC Driver — Tests

Tests for the native C# Snowflake ADBC driver (`AdbcDrivers.Snowflake.Native`). The
suite splits into two tiers, separated both by **folder** and by an xUnit **trait**:

- **Unit tests** — no network, no credentials; run everywhere (CI included). They live in
  the project root (plus `Configuration/`) and are tagged `[Trait("Category", "Unit")]`.
- **Integration tests** — exercise a *live* Snowflake account; they live under
  `Integration/` and are tagged `[Trait("Category", "Integration")]`. They are also written
  as `[SkippableFact]`/`[SkippableTheory]`, so they **skip automatically** when no
  configuration is present instead of failing.

```
dotnet test --filter "Category=Unit"          # offline only — no account needed
dotnet test --filter "Category=Integration"   # live — needs SNOWFLAKE_TEST_CONFIG_FILE
dotnet test                                   # everything (integration skips if unconfigured)
```

**Adding a test:** put offline tests in the root and tag them `Category=Unit`; put tests
that need a live account under `Integration/` and tag them `Category=Integration`.

---

## What is tested

### Offline unit tests (no Snowflake connection)

| File | Tests | What it covers |
|------|-------|----------------|
| `TypeConverterTests.cs` | ~41 | Snowflake ⇄ Arrow type mapping (describe path): NUMBER sized by precision (scale 0 → Int32 ≤9 / Int64 ≤18 / else Decimal128; scale>0 → Decimal128) to match the result decoder, BOOLEAN/VARCHAR/BINARY/DATE/TIME, TIMESTAMP NTZ/LTZ/TZ (TZ tagged UTC), and unsupported-type → `NotSupportedException`. |
| `RequestBuilderTests.cs` | ~8 | REST request-body construction — `BuildQueryRequest` (sqlText, ARROW result format, session parameters, bindings, multi-statement, `describeOnly`), `BuildCancelRequest`, and argument validation. |
| `SnowflakeAccountUrlTests.cs` | ~8 | Account → base-URL building: plain account vs. full hostname (no double-append, case-insensitive), and `NetworkConfig` host/port/protocol overrides. |
| `QueryExecutorTests.cs` | ~5 | DML affected-row detection (`TryGetDmlAffectedRows`): INSERT count, MERGE summing across count columns, reads the row-count summary (not the payload `Returned`), non-DML / empty → false. |
| `Configuration/ConnectionStringParserTests.cs` | ~12 | Connection-string / parameter parsing and required-parameter / invalid-authenticator validation. |
| `SnowflakeDriverTests.cs` | ~7 | `Open` parameter validation (missing/invalid/null → `ArgumentException`/`ArgumentNullException`) and idempotent `Dispose` — offline; the *live* connect path is `ConnectionTests`. |

These assert pure logic against in-memory inputs, so they're the fast feedback loop and
the regression net for the protocol/encoding code.

### Integration tests (require a live account)

Each integration file owns **one concern** of one of the driver's two surfaces — the Arrow-native
ADBC API (`Driver`/`Database`/`Connection`/`Statement` returning Arrow) or the ADO.NET client layer.

| File | Surface | What it covers |
|------|---------|----------------|
| `ConnectionTests.cs` | Arrow-native | **Live connectivity / lifecycle smoke**: the `Driver → Database → Connection` open path works against a real account. The fast first-line diagnostic that isolates "cannot connect" from "a query failed"; every other suite relies on this path as setup but does not assert it directly. |
| `StatementTests.cs` | Arrow-native | The `AdbcStatement` surface: execute query, `ExecuteUpdate` (SELECT → -1; DML → affected-row count), `Prepare` then execute, parameter binding (`CanBindParameter` theory drives every type in `BindCases.cs` by position), `Cancel` of a running query (`SYSTEM$WAIT` aborted via `/queries/v1/abort-request`), and `GetParameterSchema` → `NotImplementedException`. |
| `QueryAndMetadataTests.cs` | Arrow-native | **Content-asserting** query execution + the metadata methods, against the shared read-only `SNOWFLAKE_SAMPLE_DATA` (TPC-H SF1). Because that data has fixed cardinalities/contents in every account, these verify *real values*: REGION's 5 names, NATION = 25 rows/4 cols, CUSTOMER = full 150,000-row chunk streaming, invalid-SQL → `AdbcException`, deterministic `GetTableSchema` types, `GetTableTypes` = TABLE/VIEW, `GetInfo` vendor = "Snowflake", and `GetObjects` navigated through the nested Arrow down to NATION's columns. |
| `TypeDecodingTests.cs` | Arrow-native | The over-the-wire **result type-decode matrix**: for each Snowflake type, `SELECT <literal>` and assert the Arrow type the result stream produces — plus exact values for NUMBER precision sizing (Int32/Int64/Decimal128), TIME, and TIMESTAMP UTC instants. |
| `ClientTests.cs` | ADO.NET client | The driver used as a `System.Data.Common` provider (`Apache.Arrow.Adbc.Client`): `DbDataReader` row iteration, column metadata, connection-string parsing, `ExecuteNonQuery` DML, and the end-to-end Arrow→CLR type contract. |
| `BenchmarkTests.cs` | Arrow-native | **Performance**: result-fetch throughput across row-count sizes against the sample data. |

> **Note on result-set types.** Query result-set Arrow types come from Snowflake's
> native Arrow IPC (encoding-dependent), so the integration tests assert on **content,
> row counts, and column names** there. Deterministic *type* assertions are done via
> `GetTableSchema` (the describe path, which maps through `TypeConverter`).

---

## Requirements

### Offline tests
- .NET 8 SDK. Nothing else — `dotnet test` with the filters below runs them with no
  account.

### Integration tests
- A reachable **Snowflake account** and login (username/password, key-pair/JWT, or
  OAuth — see config below).
- A **running warehouse**.
- The **`SNOWFLAKE_SAMPLE_DATA`** share — mounted by default, **no setup required** — for the
  suites that read it (the query/metadata/benchmark tests).
- A **writable database + schema** for the write-path tests (anything that creates a
  `TEMPORARY` table — e.g. `StatementTests.ExecuteUpdateOnDmlReturnsAffectedRowCount` and the
  `ClientTests` write tests).
  This is the only manual prerequisite: create a database your role can write to (e.g.
  `CREATE DATABASE ADBC_TEST;` — a `PUBLIC` schema is created automatically) and point
  `metadata.catalog` / `metadata.schema` at it. The tests create their own **temporary**
  tables, so nothing needs to be pre-created or seeded; if no writable schema is configured,
  those tests `Skip`.

---

## Configuring integration tests

Configuration is loaded from a **JSON file** pointed to by the
`SNOWFLAKE_TEST_CONFIG_FILE` environment variable. (The per-variable
`SNOWFLAKE_*` env-var loader exists in `IntegrationTestingUtils` but is currently
disabled because it does not populate the `metadata.*` block the metadata tests need.)

**1. Create a config file** (keep it outside the repo — it holds secrets):

```json
{
  "account": "your-account",
  "user": "your-username",
  "password": "your-password",
  "warehouse": "your-warehouse",
  "database": "your-database",
  "schema": "your-schema",
  "query": "SELECT * FROM your-database.your-schema.your-table",
  "expectedResults": 2,
  "authentication": {
    "auth_snowflake": {
      "user": "your-username",
      "password": "your-password"
    },
    "auth_jwt": {
      "user": "your-service-user",
      "private_key_file": "C:\\path\\to\\rsa_key.p8"
    },
    "auth_pat": {
      "user": "your-service-user",
      "token": "your-programmatic-access-token"
    }
  },
  "metadata": {
    "catalog": "your-database",
    "schema": "your-schema",
    "table": "your-table",
    "expectedColumnCount": 30
  }
}
```

The native tests need only valid **credentials + a warehouse**, plus a **writable**
`metadata.catalog`/`metadata.schema` for the write-path tests (`ClientTests`,
`StatementTests.ExecuteUpdateOnDmlReturnsAffectedRowCount`). `QueryAndMetadataTests` and
`TypeDecodingTests` target `SNOWFLAKE_SAMPLE_DATA` / SQL literals directly and ignore
`metadata.*`. The `query` / `expectedResults` / `expectedColumnCount` fields are **not used** by
the native tests — they remain only for Interop-config compatibility.

When multiple `authentication` blocks are present, the main suite uses `auth_snowflake`
first; the `auth_jwt` and `auth_pat` blocks are exercised only by their dedicated
connection tests (`ConnectionTests.OpenAndConnect_WithKeyPair_Succeeds` /
`OpenAndConnect_WithPat_Succeeds`), which skip when the block is absent.

PAT setup — the details matter (the server reports every misconfiguration as the same
"Programmatic access token is invalid" error):
- The `auth_pat` block's `user` must be the user the token was **created for** (PATs are
  user-bound).
- The user must be subject to a **network policy** (`CREATE NETWORK POLICY …` +
  `ALTER USER … SET NETWORK_POLICY = …`). Snowsight's "bypass requirement for network
  policy" option exists for **human users only** and is temporary.
- For `TYPE = SERVICE` users, `ROLE_RESTRICTION` is **mandatory**:
  `ALTER USER <service-user> ADD PROGRAMMATIC ACCESS TOKEN <name>
  ROLE_RESTRICTION = '<role>' DAYS_TO_EXPIRY = 30;` — the printed `token_secret`
  (shown once) is what goes in the config, not the token's name. Key-pair setup — generate a PKCS#8 key pair, then register the public key on a
dedicated service user (its `DEFAULT_ROLE` only needs warehouse usage; the top-level
`role` in the config is not applied to this test):

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -nocrypt -out rsa_key.p8
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

```sql
CREATE ROLE IF NOT EXISTS ADBC_TEST_ROLE;
GRANT USAGE ON WAREHOUSE <warehouse> TO ROLE ADBC_TEST_ROLE;
CREATE USER <service-user> TYPE = SERVICE
  DEFAULT_ROLE = ADBC_TEST_ROLE DEFAULT_WAREHOUSE = <warehouse>;
GRANT ROLE ADBC_TEST_ROLE TO USER <service-user>;
-- public key with the BEGIN/END lines and newlines stripped
ALTER USER <service-user> SET RSA_PUBLIC_KEY='MIIB...';
-- verify: RSA_PUBLIC_KEY_FP must equal SHA256:<base64(SHA-256(SubjectPublicKeyInfo))>
DESC USER <service-user>;
```

**2. Point the environment variable at it:**

```powershell
# PowerShell
$env:SNOWFLAKE_TEST_CONFIG_FILE = "C:\path\to\snowflakeconfig.local.json"
```
```bash
# Linux/macOS
export SNOWFLAKE_TEST_CONFIG_FILE=/path/to/snowflakeconfig.local.json
```

The JSON format matches the Interop Snowflake tests, so the same config file works for
both drivers (the native driver just ignores `driverPath`/`driverEntryPoint`).

---

## Running

```bash
# Unit tests only (no account needed)
dotnet test --filter "Category=Unit"

# All integration tests (set SNOWFLAKE_TEST_CONFIG_FILE first)
dotnet test --filter "Category=Integration"

# A single integration suite
dotnet test --filter "Category=Integration & FullyQualifiedName~QueryAndMetadataTests"

# Everything — integration tests skip automatically if SNOWFLAKE_TEST_CONFIG_FILE is unset
dotnet test

# Deliberately slow tests (Category=Slow, e.g. the ~50s long-running-query polling test)
# skip by default; enable them explicitly:
$env:SNOWFLAKE_RUN_SLOW_TESTS = "1"   # PowerShell (bash: export SNOWFLAKE_RUN_SLOW_TESTS=1)
dotnet test --filter "Category=Slow"
```

---

## Security

- **Never commit credentials.** Keep the config file outside the repository.
- Prefer key-pair (JWT) or OAuth over passwords where possible.
- Rotate credentials regularly.
