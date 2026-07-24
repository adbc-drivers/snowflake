/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* This file has been modified from its original version, which is
* under the Apache License:
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Snowflake.Native.Tests.Integration;

/// <summary>
/// Query throughput benchmark for the native C# Snowflake driver. Mirrors the Interop
/// <c>BenchmarkTests</c> (which exercises the Go driver) so the two can be compared
/// apples-to-apples: same configured query, same row limits, same end-to-end measurement
/// (execute + stream all batches). The only intentional differences are the driver under
/// test and the <c>[NATIVE]</c>/<c>[INTEROP]</c> log label.
///
/// Requires a live Snowflake instance; set SNOWFLAKE_TEST_CONFIG_FILE with a <c>query</c>
/// against a table large enough to satisfy the largest limit.
/// </summary>
[Trait("Category", "Integration")]
public class BenchmarkTests
{
    private readonly ITestOutputHelper _output;
    private readonly IntegrationTestConfiguration _testConfig;

    public BenchmarkTests(ITestOutputHelper output)
    {
        _output = output;
        _testConfig = IntegrationTestingUtils.TestConfiguration;
    }

    [SkippableTheory]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(1000000)]
    public async Task BaselineQueryPerformance(int limit)
    {
        Skip.If(string.IsNullOrEmpty(_testConfig.Account), "Account not configured");
        Skip.If(string.IsNullOrWhiteSpace(_testConfig.Query), "No query configured");
        var driver = IntegrationTestingUtils.GetSnowflakeAdbcDriver(_testConfig, out var parameters);
        using var database = driver.Open(parameters);
        using var connection = database.Connect(new Dictionary<string, string>());
        using var statement = connection.CreateStatement();
        statement.SqlQuery = $"{_testConfig.Query} LIMIT {limit}";
        var stopwatch = Stopwatch.StartNew();
        var result = await statement.ExecuteQueryAsync();
        using var stream = result.Stream!;
        long totalRows = 0;
        int batchCount = 0;
        while (await stream.ReadNextRecordBatchAsync() is { } batch)
        {
            using (batch) { batchCount++; totalRows += batch.Length; }
        }
        stopwatch.Stop();
        _output.WriteLine($"[NATIVE] Fetched {totalRows} rows in {batchCount} batches with {stream.Schema.FieldsList.Count} columns for limit {limit} in {stopwatch.Elapsed.TotalMilliseconds:F0} ms.");
        Assert.Equal(limit, totalRows);
    }
}
