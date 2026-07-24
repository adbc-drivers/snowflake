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
using System.Net.Http;
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
/// Unit tests for the connection-fault callback: the executor must flag the pooled connection
/// when a failure leaves the session unusable or in an unknown state (transport error mid-query,
/// failed renewal, session-fatal GS code) — and must NOT flag it for ordinary SQL errors or a
/// caller-initiated cancellation, where the session remains healthy.
/// </summary>
[Trait("Category", "Unit")]
public class QueryExecutorFaultTests
{
    private readonly IRestApiClient _apiClient = Substitute.For<IRestApiClient>();
    private readonly QueryExecutor _sut;
    private int _faultCount;

    public QueryExecutorFaultTests()
    {
        _sut = new QueryExecutor(
            _apiClient,
            Substitute.For<ITypeConverter>(),
            "testaccount",
            network: null,
            NullLogger<QueryExecutor>.Instance,
            onConnectionFault: () => _faultCount++);
    }

    private static AuthenticationToken CreateToken(string? masterToken = "master-token-123") =>
        new()
        {
            SessionToken = "session-token-abc",
            MasterToken = masterToken,
            ExpiresAt = DateTimeOffset.UtcNow.AddHours(1),
        };

    private static QueryRequest Request(AuthenticationToken token) => new()
    {
        Statement = "SELECT 1",
        AuthToken = token,
    };

    private void SetupQueryResponses(params ApiResponse<SnowflakeQueryResponse>[] responses) =>
        _apiClient
            .PostAsync<SnowflakeQueryRequestBody, SnowflakeQueryResponse>(
                Arg.Is<string>(e => e.Contains("/queries/v1/query-request")),
                Arg.Any<SnowflakeQueryRequestBody>(),
                Arg.Any<AuthenticationToken>(),
                Arg.Any<CancellationToken>())
            .Returns(responses[0], responses[1..]);

    private void SetupQueryThrows(Exception exception) =>
        _apiClient
            .PostAsync<SnowflakeQueryRequestBody, SnowflakeQueryResponse>(
                Arg.Any<string>(),
                Arg.Any<SnowflakeQueryRequestBody>(),
                Arg.Any<AuthenticationToken>(),
                Arg.Any<CancellationToken>())
            .Returns<Task<ApiResponse<SnowflakeQueryResponse>>>(
                _ => Task.FromException<ApiResponse<SnowflakeQueryResponse>>(exception));

    private void SetupRenewalResponse(ApiResponse<SnowflakeRenewSessionData> response) =>
        _apiClient
            .PostAsync<SnowflakeRenewSessionBody, SnowflakeRenewSessionData>(
                Arg.Is<string>(e => e.Contains("/session/token-request")),
                Arg.Any<SnowflakeRenewSessionBody>(),
                Arg.Any<AuthenticationToken>(),
                Arg.Any<CancellationToken>())
            .Returns(response);

    [Fact]
    public async Task ExecuteQueryAsync_TransportFailure_FaultsConnection()
    {
        SetupQueryThrows(new HttpRequestException("connection reset"));

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()));

        Assert.Equal(QueryStatus.Failed, result.Status);
        Assert.Equal(1, _faultCount);
    }

    [Fact]
    public async Task ExecuteQueryAsync_SqlError_DoesNotFaultConnection()
    {
        // A compilation/execution error is a statement problem; the session is still healthy.
        SetupQueryResponses(new ApiResponse<SnowflakeQueryResponse>
        {
            Success = false,
            Code = "001003",
            Message = "SQL compilation error",
        });

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()));

        Assert.Equal(QueryStatus.Failed, result.Status);
        Assert.Equal(0, _faultCount);
    }

    [Fact]
    public async Task ExecuteQueryAsync_UnsupportedResultShape_DoesNotFaultConnection()
    {
        // A successful response the driver cannot represent (no Arrow data, no rowset) fails
        // the statement, but the session itself is still healthy.
        SetupQueryResponses(new ApiResponse<SnowflakeQueryResponse>
        {
            Success = true,
            Data = new SnowflakeQueryResponse(),
        });

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()));

        Assert.Equal(QueryStatus.Failed, result.Status);
        Assert.Equal(0, _faultCount);
    }

    [Fact]
    public async Task ExecuteQueryAsync_SessionExpiredWithoutMasterToken_FaultsConnection()
    {
        // 390112 with no master token to renew from: the session cannot be recovered.
        SetupQueryResponses(new ApiResponse<SnowflakeQueryResponse> { Success = false, Code = "390112" });

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken(masterToken: null)));

        Assert.Equal(QueryStatus.Failed, result.Status);
        Assert.Equal(1, _faultCount);
    }

    [Fact]
    public async Task ExecuteQueryAsync_MasterTokenExpired_FaultsConnection()
    {
        SetupQueryResponses(new ApiResponse<SnowflakeQueryResponse> { Success = false, Code = "390114" });

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()));

        Assert.Equal(QueryStatus.Failed, result.Status);
        Assert.Equal(1, _faultCount);
    }

    [Fact]
    public async Task ExecuteQueryAsync_RenewalRejected_FaultsConnection()
    {
        SetupQueryResponses(new ApiResponse<SnowflakeQueryResponse> { Success = false, Code = "390112" });
        SetupRenewalResponse(new ApiResponse<SnowflakeRenewSessionData> { Success = false, Code = "390114" });

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()));

        Assert.Equal(QueryStatus.Failed, result.Status);
        Assert.True(_faultCount >= 1);
    }

    [Fact]
    public async Task ExecuteQueryAsync_SuccessfulRenewalAndRetry_DoesNotFaultConnection()
    {
        SetupQueryResponses(
            new ApiResponse<SnowflakeQueryResponse> { Success = false, Code = "390112" },
            new ApiResponse<SnowflakeQueryResponse>
            {
                Success = true,
                // A representable result shape (a command status rowset), so the retry
                // classifies as a success rather than an unsupported response.
                Data = new SnowflakeQueryResponse
                {
                    QueryResultFormat = "json",
                    RowType = [new RowType { Name = "status", Type = "text" }],
                    RowSet = [["Statement executed successfully."]],
                },
            });
        SetupRenewalResponse(new ApiResponse<SnowflakeRenewSessionData>
        {
            Success = true,
            Data = new SnowflakeRenewSessionData
            {
                SessionToken = "renewed-session-token",
                ValidityInSeconds = 3600,
            },
        });

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()));

        Assert.Equal(QueryStatus.Success, result.Status);
        Assert.Equal(0, _faultCount);
    }

    [Fact]
    public async Task ExecuteQueryAsync_CallerCancellation_DoesNotFaultConnection()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        SetupQueryThrows(new OperationCanceledException(cts.Token));

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()), cts.Token);

        Assert.Equal(QueryStatus.Cancelled, result.Status);
        Assert.Equal(0, _faultCount);
    }

    [Fact]
    public async Task RenewSessionAsync_Rejected_FaultsConnection()
    {
        // Proactive renewal (not via a query) whose rejection also means the session is unusable.
        SetupRenewalResponse(new ApiResponse<SnowflakeRenewSessionData> { Success = false, Code = "390114" });

        await Assert.ThrowsAsync<AdbcException>(() => _sut.RenewSessionAsync(CreateToken()));

        Assert.Equal(1, _faultCount);
    }
}
