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
using Apache.Arrow.Ipc;

namespace AdbcDrivers.Snowflake.Tests.Integration;

/// <summary>
/// Smoke tests for the driver's <b>synchronous</b> ADBC surface. The driver is async-first: each
/// synchronous method that does I/O blocks on its asynchronous counterpart, so the functional
/// suites exercise behavior through the async API and these tests only prove, once per entry point,
/// that the synchronous wrappers reach a live server and surface results and errors the same way
/// (in particular, that a failure arrives as the original <see cref="AdbcException"/> rather than
/// an <see cref="AggregateException"/>).
///
/// Every test here uses only SQL literals and each database's own INFORMATION_SCHEMA, so the suite
/// needs no sample data and no writable schema. Requires a live Snowflake instance; set
/// SNOWFLAKE_TEST_CONFIG_FILE.
/// </summary>
[Trait("Category", "Integration")]
public class SyncApiSmokeTests
{
    private readonly ITestOutputHelper _output;
    private readonly IntegrationTestConfiguration _testConfiguration;

    public SyncApiSmokeTests(ITestOutputHelper output)
    {
        _output = output;
        _testConfiguration = IntegrationTestingUtils.TestConfiguration;

        Skip.If(string.IsNullOrEmpty(_testConfiguration.Account),
            $"Cannot execute test configuration from environment variable `{IntegrationTestingUtils.SnowflakeTestConfigVariable}`");
    }

    [SkippableFact]
    public void Lifecycle_OpenConnectExecuteDispose()
    {
        var driver = IntegrationTestingUtils.GetSnowflakeAdbcDriver(_testConfiguration, out var parameters);
        var database = driver.Open(parameters);
        var connection = database.Connect(new Dictionary<string, string>());
        var statement = connection.CreateStatement();

        statement.SqlQuery = "SELECT 1 AS X, 'two' AS Y";
        statement.Prepare();
        var query = statement.ExecuteQuery();
        Assert.NotNull(query.Stream);
        using (query.Stream)
        {
            using RecordBatch? batch = ReadNext(query.Stream);
            Assert.NotNull(batch);
            Assert.Equal(2, batch.ColumnCount);
            Assert.Equal(1, batch.Length);
        }

        statement.SqlQuery = "SELECT 1";
        Assert.Equal(-1, statement.ExecuteUpdate().AffectedRows);

        // Disposing the database closes the pooled session; none of it may throw.
        statement.Dispose();
        connection.Dispose();
        Assert.Null(Record.Exception(database.Dispose));
    }

    [SkippableFact]
    public void ExecuteQuery_InvalidSql_ThrowsAdbcException()
    {
        using var database = OpenDatabase();
        using var connection = database.Connect(new Dictionary<string, string>());
        using var statement = connection.CreateStatement();
        statement.SqlQuery = "SELECT * FROM NO_SUCH_DATABASE_XYZ.NO_SUCH_SCHEMA.NO_SUCH_TABLE";

        Assert.Throws<AdbcException>(statement.ExecuteQuery);
        Assert.Throws<AdbcException>(statement.ExecuteUpdate);
        Assert.Equal(AdbcStatusCode.NotImplemented, Assert.Throws<AdbcException>(statement.GetParameterSchema).Status);
    }

    [SkippableFact]
    public void Metadata_GetInfoTableTypesObjectsAndTableSchema()
    {
        using var database = OpenDatabase();
        using var connection = database.Connect(new Dictionary<string, string>());

        using (var info = connection.GetInfo([AdbcInfoCode.VendorName]))
        {
            using RecordBatch? batch = ReadNext(info);
            Assert.NotNull(batch);
            var stringValues = (StringArray)((DenseUnionArray)batch.Column(1)).Fields[0];
            Assert.Equal("Snowflake", stringValues.GetString(0));
        }

        using (var tableTypes = connection.GetTableTypes())
        {
            using RecordBatch? batch = ReadNext(tableTypes);
            Assert.NotNull(batch);
            Assert.Equal(2, batch.Length);
        }

        // Any visible database will do: each one carries an INFORMATION_SCHEMA to describe.
        string catalog;
        using (var objects = connection.GetObjects(AdbcConnection.GetObjectsDepth.Catalogs, null, null, null, null, null))
        {
            using RecordBatch? batch = ReadNext(objects);
            Assert.NotNull(batch);
            var catalogNames = (StringArray)batch.Column(0);
            Assert.True(catalogNames.Length > 0, "GetObjects returned no catalogs");
            catalog = catalogNames.GetString(0)!;
        }
        _output.WriteLine($"Describing {catalog}.INFORMATION_SCHEMA.TABLES");

        Schema schema = connection.GetTableSchema(catalog, "INFORMATION_SCHEMA", "TABLES");
        Assert.Contains(schema.FieldsList, f => f.Name == "TABLE_NAME");
    }

    [SkippableFact]
    public void Transactions_SetOptionCommitRollback()
    {
        using var database = OpenDatabase();
        using var connection = database.Connect(new Dictionary<string, string>());

        // The data semantics of commit and rollback are covered by StatementTests; this only proves
        // the synchronous wrappers send the session statements and track the autocommit state.
        connection.SetOption(AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled);
        connection.Rollback();
        connection.Commit();
        connection.SetOption(AdbcOptions.Connection.Autocommit, AdbcOptions.Enabled);

        Assert.Equal(AdbcStatusCode.InvalidState, Assert.Throws<AdbcException>(connection.Commit).Status);
    }

    [SkippableFact]
    public async Task Cancel_AbortsQueryRunningOnAnotherThread()
    {
        using var database = OpenDatabase();
        using var connection = database.Connect(new Dictionary<string, string>());
        using var statement = connection.CreateStatement();
        statement.SqlQuery = "SELECT SYSTEM$WAIT(10)";

        // The classic synchronous pattern: a blocking ExecuteQuery on one thread, Cancel from another.
        var queryTask = Task.Run(statement.ExecuteQuery);
        await Task.Delay(1000);
        statement.Cancel();

        var ex = await Assert.ThrowsAsync<AdbcException>(async () => await queryTask);
        _output.WriteLine($"Cancelled query failed with: {ex.Message}");
        Assert.Contains("cancel", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private AdbcDatabase OpenDatabase()
    {
        var driver = IntegrationTestingUtils.GetSnowflakeAdbcDriver(_testConfiguration, out var parameters);
        return driver.Open(parameters);
    }

    // Arrow streams expose only an asynchronous read; block on it, as a synchronous caller would.
    private static RecordBatch? ReadNext(IArrowArrayStream stream) =>
        stream.ReadNextRecordBatchAsync().AsTask().GetAwaiter().GetResult();
}
