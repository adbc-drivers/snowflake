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
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Snowflake.Native.Tests.Integration;

/// <summary>
/// Live tests for session-token renewal against the real <c>/session/token-request</c> endpoint.
/// Snowflake issues a short-lived session token (~1h) backed by a longer master token (~4h); the
/// driver renews the session token with the master token, reactively when a query is rejected with
/// the session-expired code (390112). Forcing a natural expiry would take ~1h, so these drive the
/// renewal directly. Requires a live account; set SNOWFLAKE_TEST_CONFIG_FILE.
/// </summary>
[Trait("Category", "Integration")]
public class SessionRenewalTests
{
    private readonly ITestOutputHelper _output;
    private readonly IntegrationTestConfiguration _testConfiguration;

    public SessionRenewalTests(ITestOutputHelper output)
    {
        _output = output;
        _testConfiguration = IntegrationTestingUtils.TestConfiguration;

        Skip.If(string.IsNullOrEmpty(_testConfiguration.Account),
            $"Cannot execute test configuration from environment variable `{IntegrationTestingUtils.SnowflakeTestConfigVariable}`");
    }

    [SkippableFact]
    public async Task RenewSession_IssuesADifferentWorkingSessionToken()
    {
        // Given an open connection with a live session token
        using var connection = Connect();
        string? before = connection.AuthToken?.SessionToken;
        Assert.False(string.IsNullOrEmpty(before), "expected a session token after connect");

        // When the session is renewed via the master token (the real /session/token-request call)
        await connection.RenewSessionAsync();

        // Then a different session token is issued and it still works for a query
        string? after = connection.AuthToken?.SessionToken;
        _output.WriteLine($"session token changed on renewal: {before != after}");
        Assert.False(string.IsNullOrEmpty(after));
        Assert.NotEqual(before, after);

        using var statement = connection.CreateStatement();
        statement.SqlQuery = "SELECT 1 AS X";
        var result = await statement.ExecuteQueryAsync();
        using var stream = result.Stream!;
        var batch = await stream.ReadNextRecordBatchAsync();
        Assert.NotNull(batch);
        Assert.Equal(1, batch!.Length);
    }

    [SkippableFact]
    public async Task RenewedToken_IsUsedBySubsequentQueries()
    {
        // Given an open connection
        using var connection = Connect();
        var token = connection.AuthToken!;

        // When the session is renewed and several queries run afterwards
        await connection.RenewSessionAsync();
        string? renewed = token.SessionToken;

        // Then the connection keeps using the renewed token and queries keep working (the renewed
        // token is shared in place, so every later statement picks it up)
        for (int i = 0; i < 3; i++)
        {
            using var statement = connection.CreateStatement();
            statement.SqlQuery = $"SELECT {i} AS X";
            var result = await statement.ExecuteQueryAsync();
            using var stream = result.Stream!;
            var batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
        }

        Assert.Equal(renewed, token.SessionToken);
    }

    [SkippableFact]
    public async Task Heartbeat_KeepsTheSessionUsable()
    {
        // Given an open connection
        using var connection = Connect();

        // When the session is heartbeated (the real /session/heartbeat call)
        await connection.HeartbeatAsync();

        // Then the session is still usable for a query
        using var statement = connection.CreateStatement();
        statement.SqlQuery = "SELECT 1 AS X";
        var result = await statement.ExecuteQueryAsync();
        using var stream = result.Stream!;
        var batch = await stream.ReadNextRecordBatchAsync();
        Assert.NotNull(batch);
        Assert.Equal(1, batch!.Length);
    }

    private SnowflakeConnection Connect()
    {
        var driver = IntegrationTestingUtils.GetSnowflakeAdbcDriver(_testConfiguration, out var parameters);
        var database = driver.Open(parameters);
        var connection = database.Connect(new Dictionary<string, string>());
        return (SnowflakeConnection)connection;
    }
}
