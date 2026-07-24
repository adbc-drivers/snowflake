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
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace AdbcDrivers.Snowflake.Native.Tests;

/// <summary>
/// Tests for long-running query completion: when a query outlives Snowflake's synchronous
/// response window the server answers with a query-in-progress GS code (333333/333334) and a
/// getResultUrl; the executor must poll that URL (repeatedly, if the server keeps handing out
/// new URLs) until the final result arrives, renewing the session token if it expires mid-poll.
/// </summary>
[Trait("Category", "Unit")]
public class QueryExecutorInProgressTests
{
    private readonly IRestApiClient _apiClient = Substitute.For<IRestApiClient>();
    private readonly QueryExecutor _sut;
    private int _faultCount;

    public QueryExecutorInProgressTests()
    {
        _sut = new QueryExecutor(
            _apiClient,
            TypeConverter.Shared,
            "testaccount",
            network: null,
            NullLogger<QueryExecutor>.Instance,
            onConnectionFault: () => _faultCount++);
    }

    private static AuthenticationToken CreateToken() => new()
    {
        SessionToken = "session-token-abc",
        MasterToken = "master-token-123",
        ExpiresAt = DateTimeOffset.UtcNow.AddHours(1),
    };

    private static QueryRequest Request(AuthenticationToken token) => new()
    {
        Statement = "SELECT SYSTEM$WAIT(60)",
        AuthToken = token,
    };

    private static ApiResponse<SnowflakeQueryResponse> InProgress(string resultUrl, string code = "333333") => new()
    {
        Success = true,
        Code = code,
        Data = new SnowflakeQueryResponse { GetResultUrl = resultUrl },
    };

    private static ApiResponse<SnowflakeQueryResponse> CompletedStatus(string status) => new()
    {
        Success = true,
        Data = new SnowflakeQueryResponse
        {
            QueryResultFormat = "json",
            Returned = 1,
            RowType = [new RowType { Name = "status", Type = "text" }],
            RowSet = [[status]],
        },
    };

    private void SetupPost(ApiResponse<SnowflakeQueryResponse> response) =>
        _apiClient
            .PostAsync<SnowflakeQueryRequestBody, SnowflakeQueryResponse>(
                Arg.Is<string>(e => e.Contains("/queries/v1/query-request")),
                Arg.Any<SnowflakeQueryRequestBody>(),
                Arg.Any<AuthenticationToken>(),
                Arg.Any<CancellationToken>())
            .Returns(response);

    private void SetupGet(string urlFragment, params ApiResponse<SnowflakeQueryResponse>[] responses) =>
        _apiClient
            .GetAsync<SnowflakeQueryResponse>(
                Arg.Is<string>(e => e.Contains(urlFragment)),
                Arg.Any<AuthenticationToken>(),
                Arg.Any<CancellationToken>())
            .Returns(responses[0], responses[1..]);

    [Fact]
    public async Task ExecuteQueryAsync_InProgress_PollsResultUrlToCompletion()
    {
        SetupPost(InProgress("/queries/qid-1/result"));
        SetupGet("/queries/qid-1/result", CompletedStatus("done"));

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()));

        Assert.Equal(QueryStatus.Success, result.Status);
        Assert.NotNull(result.ResultStream);
        result.ResultStream.Dispose();
        Assert.Equal(0, _faultCount);
    }

    [Fact]
    public async Task ExecuteQueryAsync_InProgressTwice_FollowsEachResultUrl()
    {
        // The server can answer a poll with another in-progress response carrying the next URL.
        SetupPost(InProgress("/queries/qid-1/result"));
        SetupGet("/queries/qid-1/result", InProgress("/queries/qid-1/result?disableOfflineChunks=true", code: "333334"));
        SetupGet("disableOfflineChunks", CompletedStatus("done"));

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()));

        Assert.Equal(QueryStatus.Success, result.Status);
        Assert.NotNull(result.ResultStream);
        result.ResultStream.Dispose();
    }

    [Fact]
    public async Task ExecuteQueryAsync_SessionExpiresMidPoll_RenewsAndRepolls()
    {
        SetupPost(InProgress("/queries/qid-1/result"));
        SetupGet(
            "/queries/qid-1/result",
            new ApiResponse<SnowflakeQueryResponse> { Success = false, Code = "390112" },
            CompletedStatus("done"));
        _apiClient
            .PostAsync<SnowflakeRenewSessionBody, SnowflakeRenewSessionData>(
                Arg.Is<string>(e => e.Contains("/session/token-request")),
                Arg.Any<SnowflakeRenewSessionBody>(),
                Arg.Any<AuthenticationToken>(),
                Arg.Any<CancellationToken>())
            .Returns(new ApiResponse<SnowflakeRenewSessionData>
            {
                Success = true,
                Data = new SnowflakeRenewSessionData { SessionToken = "renewed-token", ValidityInSeconds = 3600 },
            });

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()));

        Assert.Equal(QueryStatus.Success, result.Status);
        Assert.NotNull(result.ResultStream);
        result.ResultStream.Dispose();
        Assert.Equal(0, _faultCount);
    }

    [Fact]
    public async Task ExecuteQueryAsync_InProgressWithoutResultUrl_FailsAndFaultsConnection()
    {
        // Protocol anomaly: nothing to poll, and the query's server-side state is unknown.
        SetupPost(new ApiResponse<SnowflakeQueryResponse>
        {
            Success = true,
            Code = "333333",
            Data = new SnowflakeQueryResponse(),
        });

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()));

        Assert.Equal(QueryStatus.Failed, result.Status);
        Assert.Contains("no result URL", result.Errors[0].Message);
        Assert.Equal(1, _faultCount);
    }
}
