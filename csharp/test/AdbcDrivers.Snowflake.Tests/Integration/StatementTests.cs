/*
* Copyright (c) 2026 ADBC Drivers Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*         http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

using Apache.Arrow;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Snowflake.Tests.Integration;

/// <summary>
/// Statement-level baseline tests for the C# Snowflake driver, mirroring the
/// Interop <c>StatementTests</c>. Covers the implemented statement surface:
/// execute query, execute update, prepare, and parameter-schema guarding.
///
/// Requires a live Snowflake instance; set SNOWFLAKE_TEST_CONFIG_FILE.
/// </summary>
[Trait("Category", "Integration")]
public class StatementTests
{
    private readonly ITestOutputHelper _output;
    private readonly IntegrationTestConfiguration _testConfiguration;

    public StatementTests(ITestOutputHelper output)
    {
        _output = output;
        _testConfiguration = IntegrationTestingUtils.TestConfiguration;

        Skip.If(string.IsNullOrEmpty(_testConfiguration.Account),
            $"Cannot execute test configuration from environment variable `{IntegrationTestingUtils.SnowflakeTestConfigVariable}`");
    }

    [SkippableFact]
    public async Task CanExecuteQuery()
    {
        // Given a simple two-column query
        await using var database = await IntegrationTestingUtils.OpenDatabaseAsync(_testConfiguration);
        await using var connection = await IntegrationTestingUtils.ConnectAsync(database);
        await using var statement = connection.CreateStatement();
        statement.SqlQuery = "SELECT 1 AS X, 'two' AS Y";

        // When it is executed and the first batch is read
        var result = await statement.ExecuteQueryAsync();
        Assert.NotNull(result.Stream);
        using var stream = result.Stream;
        var batch = await stream.ReadNextRecordBatchAsync();

        // Then the batch has the expected shape
        Assert.NotNull(batch);
        Assert.Equal(2, batch.ColumnCount);
        Assert.Equal(1, batch.Length);
    }

    [SkippableFact]
    public async Task ExecuteUpdateOnSelectReturnsNoRowCount()
    {
        // Given a statement holding a SELECT (which affects no rows)
        await using var database = await IntegrationTestingUtils.OpenDatabaseAsync(_testConfiguration);
        await using var connection = await IntegrationTestingUtils.ConnectAsync(database);
        await using var statement = connection.CreateStatement();
        statement.SqlQuery = "SELECT 1";

        // When it is run as an update
        var result = await statement.ExecuteUpdateAsync();

        // Then the driver reports -1 (unknown) per the ADBC contract
        _output.WriteLine($"ExecuteUpdate on SELECT reported {result.AffectedRows} affected rows");
        Assert.Equal(-1, result.AffectedRows);
    }

    [SkippableFact]
    public async Task GetParameterSchema_IsNotSupported()
    {
        // Given a fresh statement
        await using var database = await IntegrationTestingUtils.OpenDatabaseAsync(_testConfiguration);
        await using var connection = await IntegrationTestingUtils.ConnectAsync(database);
        await using var statement = connection.CreateStatement();

        // When / Then GetParameterSchema throws AdbcException(NotImplemented): Snowflake's protocol
        // does not report bind-parameter types, so it is unsupported regardless of whether Prepare ran.
        var ex = await Assert.ThrowsAsync<AdbcException>(() => statement.GetParameterSchemaAsync());
        Assert.Equal(AdbcStatusCode.NotImplemented, ex.Status);
    }

    [SkippableFact]
    public async Task CanPrepareAndExecute()
    {
        // Given a prepared statement
        await using var database = await IntegrationTestingUtils.OpenDatabaseAsync(_testConfiguration);
        await using var connection = await IntegrationTestingUtils.ConnectAsync(database);
        await using var statement = connection.CreateStatement();
        statement.SqlQuery = "SELECT 1 AS X";
        await statement.PrepareAsync();

        // When it is executed and the first batch is read
        var result = await statement.ExecuteQueryAsync();
        Assert.NotNull(result.Stream);
        using var stream = result.Stream;
        var batch = await stream.ReadNextRecordBatchAsync();

        // Then it returns the single row
        Assert.NotNull(batch);
        Assert.Equal(1, batch.Length);
    }

    [SkippableTheory]
    [MemberData(nameof(BindCases.LiveNames), MemberType = typeof(BindCases))]
    public Task CanBindParameter(string caseName)
    {
        // Each bindable Arrow type from the shared BindCases table, bound and compared by the
        // real server. The row returns only if Snowflake accepted the wire format and the value
        // matched the predicate, so this confirms the encoding end to end.
        var bindCase = BindCases.Get(caseName);
        return AssertBoundValueMatches(bindCase.LivePredicate!, bindCase.BuildArray());
    }

    /// <summary>
    /// Binds one parameter and runs <c>SELECT 'ok' WHERE &lt;predicate&gt;</c>: the row only returns
    /// if Snowflake accepted the bind wire format and the bound value compared equal. The bind field
    /// is derived from the array's own type, so a case only has to supply the array and predicate.
    /// </summary>
    private async Task AssertBoundValueMatches(string predicate, IArrowArray value)
    {
        await using var database = await IntegrationTestingUtils.OpenDatabaseAsync(_testConfiguration);
        await using var connection = await IntegrationTestingUtils.ConnectAsync(database);
        await using var statement = connection.CreateStatement();
        statement.SqlQuery = $"SELECT 'ok' AS V WHERE {predicate}";
        var schema = new Schema([new Field("p", value.Data.DataType, true)], null);
        using var batch = new RecordBatch(schema, [value], value.Length);

        statement.Bind(batch, schema);
        var result = await statement.ExecuteQueryAsync();
        Assert.NotNull(result.Stream);
        using var stream = result.Stream!;
        var read = await stream.ReadNextRecordBatchAsync();

        Assert.NotNull(read);
        Assert.Equal(1, read!.Length);
        Assert.Equal("ok", ((StringArray)read.Column(0)).GetString(0));
    }

    [SkippableFact]
    public async Task CanCancelRunningQuery()
    {
        // Given a query that runs long enough to be cancelled mid-flight
        await using var database = await IntegrationTestingUtils.OpenDatabaseAsync(_testConfiguration);
        await using var connection = await IntegrationTestingUtils.ConnectAsync(database);
        await using var statement = connection.CreateStatement();
        statement.SqlQuery = "SELECT SYSTEM$WAIT(10)";

        // When it is started and then cancelled through the explicit ADBC Cancel after it has
        // begun running (token-driven cancellation is covered by StatementCancellationTests)
        var queryTask = statement.ExecuteQueryAsync().AsTask();
        await Task.Delay(1000);
        statement.Cancel();

        // Then the running query terminates with a Snowflake cancellation error instead of completing
        var ex = await Assert.ThrowsAsync<AdbcException>(async () => await queryTask);
        _output.WriteLine($"Cancelled query failed with: {ex.Message}");
        Assert.Contains("cancel", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [SkippableFact]
    public async Task ExecuteUpdateOnDmlReturnsAffectedRowCount()
    {
        // Given a temporary table (requires a writable metadata.catalog / metadata.schema)
        await using var database = await IntegrationTestingUtils.OpenDatabaseAsync(_testConfiguration);
        await using var connection = await IntegrationTestingUtils.ConnectAsync(database);
        await using var statement = connection.CreateStatement();
        string table = string.Format(
            "{0}.{1}.CSHARP_UPD_{2}",
            _testConfiguration.Metadata.Catalog,
            _testConfiguration.Metadata.Schema,
            Guid.NewGuid().ToString("N"));
        statement.SqlQuery = $"CREATE TEMPORARY TABLE {table} (id INT)";
        await statement.ExecuteUpdateAsync();

        // When two rows are inserted and then deleted
        statement.SqlQuery = $"INSERT INTO {table} (id) VALUES (1), (2)";
        var result = await statement.ExecuteUpdateAsync();
        statement.SqlQuery = $"DELETE FROM {table} WHERE id IN (1,2)";
        var result2 = await statement.ExecuteUpdateAsync();

        // Then each reports its affected-row count, parsed from the JSON RowSet (or -1 if a
        // driver/server version cannot determine it). Both affect 2 rows while the payload's
        // Returned count is 1.
        _output.WriteLine($"Insert reported {result.AffectedRows}, delete reported {result2.AffectedRows} affected rows");
        Assert.True(result.AffectedRows == 2 || result.AffectedRows == -1);
        Assert.True(result2.AffectedRows == 2 || result2.AffectedRows == -1);
    }

    [SkippableFact]
    public async Task ExecuteUpdate_MultiRowBind_InsertsEveryRow()
    {
        // Array binding (executemany): one INSERT with a 3-row bound batch inserts 3 rows,
        // including a null cell.
        await using var database = await IntegrationTestingUtils.OpenDatabaseAsync(_testConfiguration);
        await using var connection = await IntegrationTestingUtils.ConnectAsync(database);
        await using var statement = connection.CreateStatement();
        string table = string.Format(
            "{0}.{1}.CSHARP_ARR_{2}",
            _testConfiguration.Metadata.Catalog,
            _testConfiguration.Metadata.Schema,
            Guid.NewGuid().ToString("N"));
        statement.SqlQuery = $"CREATE TEMPORARY TABLE {table} (id INT, name VARCHAR)";
        await statement.ExecuteUpdateAsync();

        var schema = new Schema(
        [
            new Field("id", Apache.Arrow.Types.Int64Type.Default, true),
            new Field("name", Apache.Arrow.Types.StringType.Default, true),
        ], null);
        var ids = new Int64Array.Builder().Append(1).Append(2).Append(3).Build();
        var names = new StringArray.Builder().Append("alpha").AppendNull().Append("gamma").Build();
        using var batch = new RecordBatch(schema, [ids, names], 3);

        statement.SqlQuery = $"INSERT INTO {table} (id, name) VALUES (?, ?)";
        statement.Bind(batch, schema);
        var result = await statement.ExecuteUpdateAsync();

        _output.WriteLine($"Array-bind insert reported {result.AffectedRows} affected rows");
        Assert.Equal(3, result.AffectedRows);

        // A fresh statement: the original still carries the bound batch, which must not ride
        // along with the verification query.
        await using var countStatement = connection.CreateStatement();
        Assert.Equal(3, await CountRowsAsync(countStatement, table));

        // Bindings persist across executions (ADBC semantics, matching gosnowflake): executing
        // the same statement again re-binds the same batch and inserts three more rows.
        var again = await statement.ExecuteUpdateAsync();
        Assert.Equal(3, again.AffectedRows);
        Assert.Equal(6, await CountRowsAsync(countStatement, table));
    }

    [SkippableFact]
    public async Task ExecuteUpdate_MultiRowBind_EncodesTypedValuesPerRow()
    {
        // Array binds reuse the scalar per-value wire formats (DATE = ms since epoch,
        // BINARY = hex, DECIMAL = plain string, ...). This proves the server decodes them in
        // array form too — including a null cell per column — by reading the values back.
        await using var database = await IntegrationTestingUtils.OpenDatabaseAsync(_testConfiguration);
        await using var connection = await IntegrationTestingUtils.ConnectAsync(database);
        await using var statement = connection.CreateStatement();
        string table = string.Format(
            "{0}.{1}.CSHARP_ARRT_{2}",
            _testConfiguration.Metadata.Catalog,
            _testConfiguration.Metadata.Schema,
            Guid.NewGuid().ToString("N"));
        statement.SqlQuery = $"CREATE TEMPORARY TABLE {table} (d DATE, num NUMBER(10,2), flag BOOLEAN, bin BINARY, s VARCHAR)";
        await statement.ExecuteUpdateAsync();

        var schema = new Schema(
        [
            new Field("d", Apache.Arrow.Types.Date32Type.Default, true),
            new Field("num", new Apache.Arrow.Types.Decimal128Type(10, 2), true),
            new Field("flag", Apache.Arrow.Types.BooleanType.Default, true),
            new Field("bin", Apache.Arrow.Types.BinaryType.Default, true),
            new Field("s", Apache.Arrow.Types.StringType.Default, true),
        ], null);
        var dates = new Date32Array.Builder().Append(new DateTime(2024, 1, 15)).AppendNull().Build();
        var nums = new Decimal128Array.Builder(new Apache.Arrow.Types.Decimal128Type(10, 2)).Append(12.34m).AppendNull().Build();
        var flags = new BooleanArray.Builder().Append(true).AppendNull().Build();
        var binBuilder = new BinaryArray.Builder();
        binBuilder.Append("\u07ad"u8.ToArray().AsSpan());
        binBuilder.AppendNull();
        var bins = binBuilder.Build();
        var strings = new StringArray.Builder().Append("row1").Append("row2").Build();
        using var batch = new RecordBatch(schema, [dates, nums, flags, bins, strings], 2);

        statement.SqlQuery = $"INSERT INTO {table} (d, num, flag, bin, s) VALUES (?, ?, ?, ?, ?)";
        statement.Bind(batch, schema);
        var result = await statement.ExecuteUpdateAsync();
        Assert.Equal(2, result.AffectedRows);

        // Row 1 must match on every typed value; row 2 must be all-null except the string.
        await using var verify = connection.CreateStatement();
        Assert.Equal(1, await IntegrationTestingUtils.ExecuteScalarInt64Async(verify,
            $"SELECT COUNT(*) FROM {table} WHERE d = DATE '2024-01-15' AND num = 12.34 AND flag AND bin = TO_BINARY('DEAD', 'HEX') AND s = 'row1'"));
        Assert.Equal(1, await IntegrationTestingUtils.ExecuteScalarInt64Async(verify,
            $"SELECT COUNT(*) FROM {table} WHERE d IS NULL AND num IS NULL AND flag IS NULL AND bin IS NULL AND s = 'row2'"));
    }


    [SkippableFact]
    public async Task Transactions_RollbackDiscardsAndCommitPersists()
    {
        await using var database = await IntegrationTestingUtils.OpenDatabaseAsync(_testConfiguration);
        await using var connection = await IntegrationTestingUtils.ConnectAsync(database);
        await using var statement = connection.CreateStatement();

        // DDL implicitly commits in Snowflake, so create the table BEFORE opening the scope.
        string table = string.Format(
            "{0}.{1}.CSHARP_TXN_{2}",
            _testConfiguration.Metadata.Catalog,
            _testConfiguration.Metadata.Schema,
            Guid.NewGuid().ToString("N"));
        statement.SqlQuery = $"CREATE TABLE {table} (id INT)";
        await statement.ExecuteUpdateAsync();

        await connection.SetOptionAsync(AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled);

        // An insert that is rolled back leaves no rows...
        statement.SqlQuery = $"INSERT INTO {table} (id) VALUES (1)";
        await statement.ExecuteUpdateAsync();
        await connection.RollbackAsync();
        Assert.Equal(0, await CountRowsAsync(statement, table));

        // ...and one that is committed persists.
        statement.SqlQuery = $"INSERT INTO {table} (id) VALUES (2)";
        await statement.ExecuteUpdateAsync();
        await connection.CommitAsync();
        Assert.Equal(1, await CountRowsAsync(statement, table));

        await connection.SetOptionAsync(AdbcOptions.Connection.Autocommit, AdbcOptions.Enabled);
    }

    private static Task<long> CountRowsAsync(AdbcStatement statement, string table) =>
        IntegrationTestingUtils.ExecuteScalarInt64Async(statement, $"SELECT COUNT(*) FROM {table}");


    [SkippableFact]
    [Trait("Category", "Slow")]
    public async Task LongRunningQuery_OutlivesSyncWindow_ReturnsResultViaPolling()
    {
        // A query that exceeds Snowflake's synchronous response window (~45s) returns a
        // query-in-progress response with a getResultUrl; the driver must poll it to the
        // final result instead of failing. SYSTEM$WAIT(50) makes that deterministic — and
        // makes the test itself take ~50s, so it only runs when explicitly enabled.
        Skip.IfNot(IntegrationTestingUtils.RunSlowTests,
            $"Slow test (~50s wall clock); set {IntegrationTestingUtils.RunSlowTestsVariable}=1 to run.");
        await using var database = await IntegrationTestingUtils.OpenDatabaseAsync(_testConfiguration);
        await using var connection = await IntegrationTestingUtils.ConnectAsync(database);
        await using var statement = connection.CreateStatement();
        statement.SqlQuery = "SELECT SYSTEM$WAIT(50)";

        var result = await statement.ExecuteQueryAsync();

        Assert.NotNull(result.Stream);
        using var stream = result.Stream;
        var batch = await stream.ReadNextRecordBatchAsync();
        Assert.NotNull(batch);
        using (batch)
        {
            string? value = ((StringArray)batch.Column(0)).GetString(0);
            _output.WriteLine($"Long-running query returned: {value}");
            Assert.Contains("waited 50 seconds", value);
        }
    }

    [SkippableFact]
    public async Task ExecuteQueryOnDmlAndDdlReturnsSummaryRows()
    {
        // Non-SELECT statements run through ExecuteQuery must return their JSON summary as a
        // result set (parity with the Go driver), not fail for lack of an Arrow stream.
        await using var database = await IntegrationTestingUtils.OpenDatabaseAsync(_testConfiguration);
        await using var connection = await IntegrationTestingUtils.ConnectAsync(database);
        await using var statement = connection.CreateStatement();
        string table = string.Format(
            "{0}.{1}.CSHARP_QRY_{2}",
            _testConfiguration.Metadata.Catalog,
            _testConfiguration.Metadata.Schema,
            Guid.NewGuid().ToString("N"));

        // DDL via ExecuteQuery: one string status row
        statement.SqlQuery = $"CREATE TEMPORARY TABLE {table} (id INT)";
        var ddlResult = await statement.ExecuteQueryAsync();
        Assert.NotNull(ddlResult.Stream);
        using (var stream = ddlResult.Stream)
        {
            var batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            using (batch)
            {
                Assert.Equal(1, batch.Length);
                string? status = ((StringArray)batch.Column(0)).GetString(0);
                _output.WriteLine($"DDL status row: {status}");
                Assert.Contains("successfully created", status);
            }
        }

        // DML via ExecuteQuery: the affected-count summary row
        statement.SqlQuery = $"INSERT INTO {table} (id) VALUES (1), (2)";
        var dmlResult = await statement.ExecuteQueryAsync();
        Assert.Equal(2, dmlResult.RowCount);
        Assert.NotNull(dmlResult.Stream);
        using (var stream = dmlResult.Stream)
        {
            Assert.Equal("number of rows inserted", stream.Schema.FieldsList[0].Name);
            var batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            using (batch)
            {
                Assert.Equal(1, batch.Length);
                Assert.Equal(2L, ((Int64Array)batch.Column(0)).GetValue(0));
            }
        }
    }
}
