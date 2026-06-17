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

using System;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native.Services.Authentication;
using AdbcDrivers.Snowflake.Native.Services.Query;
using AdbcDrivers.Snowflake.Native.Services.Transport;
using AdbcDrivers.Snowflake.Native.Services.TypeConversion;
using Apache.Arrow.Adbc;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace AdbcDrivers.Snowflake.Native.Tests;

/// <summary>
/// Unit tests for <see cref="QueryExecutor.HeartbeatAsync"/>.
/// </summary>
[Trait("Category", "Unit")]
public class QueryExecutorHeartbeatTests
{
    private readonly IRestApiClient _apiClient = Substitute.For<IRestApiClient>();
    private readonly ITypeConverter _typeConverter = Substitute.For<ITypeConverter>();
    private readonly QueryExecutor _sut;

    public QueryExecutorHeartbeatTests()
    {
        _sut = new QueryExecutor(
            _apiClient,
            _typeConverter,
            "testaccount",
            network: null,
            NullLogger<QueryExecutor>.Instance,
            onConnectionFault: static () => { });
    }

    private static AuthenticationToken CreateToken(string? masterToken = "master-token-123") =>
        new()
        {
            SessionToken = "session-token-abc",
            MasterToken = masterToken,
            ExpiresAt = DateTimeOffset.UtcNow.AddHours(1),
        };

    [Fact]
    public async Task HeartbeatAsync_WhenSuccessful_DoesNotThrow()
    {
        var authToken = CreateToken();

        _apiClient
            .PostAsync<EmptyRequestBody, SnowflakeQueryResponse>(
                Arg.Is<string>(e => e.Contains("/session/heartbeat")),
                Arg.Any<EmptyRequestBody>(),
                Arg.Is(authToken),
                Arg.Any<CancellationToken>())
            .Returns(new ApiResponse<SnowflakeQueryResponse> { Success = true });

        await _sut.HeartbeatAsync(authToken, CancellationToken.None);

        // No exception means success; verify the heartbeat endpoint was called exactly once.
        await _apiClient.Received(1).PostAsync<EmptyRequestBody, SnowflakeQueryResponse>(
            Arg.Is<string>(e => e.Contains("/session/heartbeat")),
            Arg.Any<EmptyRequestBody>(),
            Arg.Any<AuthenticationToken>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HeartbeatAsync_WhenSessionExpired_RenewsWithMasterToken()
    {
        var authToken = CreateToken(masterToken: "my-master-token");

        // First call: heartbeat returns session-expired
        _apiClient
            .PostAsync<EmptyRequestBody, SnowflakeQueryResponse>(
                Arg.Is<string>(e => e.Contains("/session/heartbeat")),
                Arg.Any<EmptyRequestBody>(),
                Arg.Any<AuthenticationToken>(),
                Arg.Any<CancellationToken>())
            .Returns(new ApiResponse<SnowflakeQueryResponse> { Success = false, Code = "390112" });

        // Second call: token-request renewal succeeds
        _apiClient
            .PostAsync<SnowflakeRenewSessionBody, SnowflakeRenewSessionData>(
                Arg.Is<string>(e => e.Contains("/session/token-request")),
                Arg.Any<SnowflakeRenewSessionBody>(),
                Arg.Is<AuthenticationToken>(t => t.SessionToken == "my-master-token"),
                Arg.Any<CancellationToken>())
            .Returns(new ApiResponse<SnowflakeRenewSessionData>
            {
                Success = true,
                Data = new SnowflakeRenewSessionData
                {
                    SessionToken = "renewed-session-token",
                    MasterToken = "renewed-master-token",
                    ValidityInSeconds = 3600,
                },
            });

        await _sut.HeartbeatAsync(authToken, CancellationToken.None);

        // Verify the renewal was called with the master token
        await _apiClient.Received(1).PostAsync<SnowflakeRenewSessionBody, SnowflakeRenewSessionData>(
            Arg.Is<string>(e => e.Contains("/session/token-request")),
            Arg.Any<SnowflakeRenewSessionBody>(),
            Arg.Is<AuthenticationToken>(t => t.SessionToken == "my-master-token"),
            Arg.Any<CancellationToken>());

        // Verify the auth token was updated
        Assert.Equal("renewed-session-token", authToken.SessionToken);
        Assert.Equal("renewed-master-token", authToken.MasterToken);
    }

    [Fact]
    public async Task HeartbeatAsync_WhenSessionExpiredButNoMasterToken_DoesNotThrow()
    {
        var authToken = CreateToken(masterToken: null);

        _apiClient
            .PostAsync<EmptyRequestBody, SnowflakeQueryResponse>(
                Arg.Is<string>(e => e.Contains("/session/heartbeat")),
                Arg.Any<EmptyRequestBody>(),
                Arg.Any<AuthenticationToken>(),
                Arg.Any<CancellationToken>())
            .Returns(new ApiResponse<SnowflakeQueryResponse> { Success = false, Code = "390112" });

        // Should not throw — just returns without renewal
        await _sut.HeartbeatAsync(authToken, CancellationToken.None);

        // Verify no renewal call was attempted
        await _apiClient.DidNotReceive().PostAsync<SnowflakeRenewSessionBody, SnowflakeRenewSessionData>(
            Arg.Any<string>(),
            Arg.Any<SnowflakeRenewSessionBody>(),
            Arg.Any<AuthenticationToken>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HeartbeatAsync_WhenOtherFailure_ThrowsAdbcException()
    {
        var authToken = CreateToken();

        _apiClient
            .PostAsync<EmptyRequestBody, SnowflakeQueryResponse>(
                Arg.Is<string>(e => e.Contains("/session/heartbeat")),
                Arg.Any<EmptyRequestBody>(),
                Arg.Any<AuthenticationToken>(),
                Arg.Any<CancellationToken>())
            .Returns(new ApiResponse<SnowflakeQueryResponse> { Success = false, Code = "999999" });

        var ex = await Assert.ThrowsAsync<AdbcException>(
            () => _sut.HeartbeatAsync(authToken, CancellationToken.None));

        Assert.Contains("heartbeat failed", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task HeartbeatAsync_WhenNullAuthToken_ThrowsArgumentNullException()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => _sut.HeartbeatAsync(null!, CancellationToken.None));
    }
}
