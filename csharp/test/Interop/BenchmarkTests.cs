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

namespace AdbcDrivers.Snowflake.Interop.Tests;

public class BenchmarkTests
{
    private readonly ITestOutputHelper _output;
    private readonly SnowflakeTestConfiguration _testConfig;

    public BenchmarkTests(ITestOutputHelper output)
    {
        _output = output;
        _testConfig = SnowflakeTestingUtils.TestConfiguration;
    }

    [SkippableTheory]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(1000000)]
    public async Task BaselineQueryPerformance(int limit)
    {
        Skip.If(string.IsNullOrEmpty(_testConfig.DriverPath), "Driver path not configured");
        Skip.If(string.IsNullOrWhiteSpace(_testConfig.Query), "No query configured");
        var driver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(_testConfig, out var parameters);
        parameters["adbc.snowflake.sql.client_option.tls_skip_verify"] = "true";
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
        _output.WriteLine($"[INTEROP] Fetched {totalRows} rows in {batchCount} batches with {stream.Schema.FieldsList.Count} columns for limit {limit} in {stopwatch.Elapsed.TotalMilliseconds:F0} ms.");
        Assert.Equal(limit, totalRows);
    }
}
