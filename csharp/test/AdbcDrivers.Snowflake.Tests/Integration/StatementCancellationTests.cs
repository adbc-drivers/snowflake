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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Snowflake.Tests.Integration;

/// <summary>
/// Live checks of the <see cref="CancellationToken"/> overloads of
/// <see cref="AdbcStatement.ExecuteQueryAsync(CancellationToken)"/>: that the token cancels the
/// query on the server (not just the client's wait), and that it covers execution only, not
/// reading the returned multi-chunk stream. Neither needs sample data or a writable schema.
///
/// Requires a live Snowflake instance; set SNOWFLAKE_TEST_CONFIG_FILE.
/// </summary>
[Trait("Category", "Integration")]
public class StatementCancellationTests
{
    private readonly ITestOutputHelper _output;
    private readonly IntegrationTestConfiguration _testConfiguration;

    public StatementCancellationTests(ITestOutputHelper output)
    {
        _output = output;
        _testConfiguration = IntegrationTestingUtils.TestConfiguration;

        Skip.If(string.IsNullOrEmpty(_testConfiguration.Account),
            $"Cannot execute test configuration from environment variable `{IntegrationTestingUtils.SnowflakeTestConfigVariable}`");
    }

    [SkippableFact]
    public async Task ExecuteQueryAsync_TokenCancelled_CancelsTheQueryOnTheServer()
    {
        // Query history is read through the configured database's INFORMATION_SCHEMA.
        Skip.If(string.IsNullOrEmpty(_testConfiguration.Database), "No database configured");

        // Given a query that would run for 30s, tagged so it can be found in the query history
        var driver = IntegrationTestingUtils.GetSnowflakeAdbcDriver(_testConfiguration, out var parameters);
        using var database = driver.Open(parameters);
        using var connection = database.Connect(new Dictionary<string, string>());
        using var statement = connection.CreateStatement();
        string marker = Guid.NewGuid().ToString("N");
        statement.SqlQuery = $"SELECT SYSTEM$WAIT(30), '{marker}'";

        // When its token is cancelled once it is running
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var elapsed = Stopwatch.StartNew();
        var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => statement.ExecuteQueryAsync(cts.Token).AsTask());
        elapsed.Stop();

        // Then the caller is released promptly, as OperationCanceledException for its token
        _output.WriteLine($"Cancelled after {elapsed.Elapsed.TotalSeconds:F1}s: {ex.Message}");
        Assert.Equal(cts.Token, ex.CancellationToken);
        Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(10), $"Cancellation took {elapsed.Elapsed}.");

        // And the server cancelled the query rather than running it to completion; this query on
        // the same connection also shows the connection is still usable.
        string outcome = await WaitForQueryOutcomeAsync(statement, marker, TimeSpan.FromSeconds(20));
        _output.WriteLine($"Server-side outcome: {outcome}");
        Assert.StartsWith("FAILED", outcome, StringComparison.Ordinal);
        Assert.Contains("cancel", outcome, StringComparison.OrdinalIgnoreCase);
    }

    [SkippableFact]
    public async Task ExecuteQueryAsync_TokenCancelledAfterReturn_StillReadsEveryChunk()
    {
        // Given a result large enough to be served as many externally downloaded chunks
        const int rowCount = 500_000;
        var driver = IntegrationTestingUtils.GetSnowflakeAdbcDriver(_testConfiguration, out var parameters);
        using var database = driver.Open(parameters);
        using var connection = database.Connect(new Dictionary<string, string>());
        using var statement = connection.CreateStatement();
        statement.SqlQuery =
            $"SELECT SEQ8() AS ID, RANDSTR(64, RANDOM()) AS S FROM TABLE(GENERATOR(ROWCOUNT => {rowCount}))";

        // When the execution token is cancelled right after execute returns, while chunk
        // downloads are still in flight
        using var cts = new CancellationTokenSource();
        var result = await statement.ExecuteQueryAsync(cts.Token);
        cts.Cancel();
        await Task.Delay(200);

        // Then the token doesn't reach the stream: every row of every chunk arrives
        Assert.NotNull(result.Stream);
        using var stream = result.Stream;
        long rows = 0;
        int batches = 0;
        while (await stream.ReadNextRecordBatchAsync() is { } batch)
        {
            using (batch)
            {
                rows += batch.Length;
                batches++;
            }
        }

        _output.WriteLine($"Read {rows} rows in {batches} batches.");
        Assert.Equal(rowCount, rows);
        Assert.True(batches > 1, "The result arrived as a single batch, so chunk prefetch wasn't exercised.");
    }

    /// <summary>
    /// Polls the session's query history for the query carrying <paramref name="marker"/> until
    /// it has finished, returning "EXECUTION_STATUS:ERROR_CODE:ERROR_MESSAGE".
    /// </summary>
    private async Task<string> WaitForQueryOutcomeAsync(AdbcStatement statement, string marker, TimeSpan timeout)
    {
        statement.SqlQuery =
            "SELECT EXECUTION_STATUS || ':' || COALESCE(TO_VARCHAR(ERROR_CODE), '') || ':' || COALESCE(ERROR_MESSAGE, '') " +
            $"FROM TABLE({_testConfiguration.Database}.INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 100)) " +
            $"WHERE STARTSWITH(QUERY_TEXT, 'SELECT SYSTEM$WAIT') AND CONTAINS(QUERY_TEXT, '{marker}')";

        var deadline = DateTime.UtcNow + timeout;
        string? outcome = null;
        while (DateTime.UtcNow < deadline)
        {
            var result = await statement.ExecuteQueryAsync();
            Assert.NotNull(result.Stream);
            using (var stream = result.Stream)
            {
                using var batch = await stream.ReadNextRecordBatchAsync();
                outcome = batch is { Length: > 0 } ? ((StringArray)batch.Column(0)).GetString(0) : null;
            }

            if (outcome != null && !outcome.StartsWith("RUNNING", StringComparison.Ordinal)
                && !outcome.StartsWith("QUEUED", StringComparison.Ordinal))
                return outcome;

            await Task.Delay(1000);
        }

        throw new Xunit.Sdk.XunitException(
            $"The query did not finish within {timeout} (last status: {outcome ?? "not in history"}); the server-side abort didn't take effect.");
    }
}
