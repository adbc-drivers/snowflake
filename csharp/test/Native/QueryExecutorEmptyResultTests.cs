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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native.Configuration;
using AdbcDrivers.Snowflake.Native.Services.Authentication;
using AdbcDrivers.Snowflake.Native.Services.ConnectionPool;
using AdbcDrivers.Snowflake.Native.Services.Query;
using AdbcDrivers.Snowflake.Native.Services.Transport;
using AdbcDrivers.Snowflake.Native.Services.TypeConversion;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace AdbcDrivers.Snowflake.Native.Tests;

/// <summary>
/// Tests for zero-row result handling. A SELECT that matches no rows still reports
/// queryResultFormat=arrow, but Snowflake sends neither rowsetBase64 nor chunks — only the
/// rowtype metadata. The executor must surface an empty stream carrying the rowtype schema
/// (not a null stream, which the statement treats as an error).
/// </summary>
[Trait("Category", "Unit")]
public class QueryExecutorEmptyResultTests
{
    private readonly IRestApiClient _apiClient = Substitute.For<IRestApiClient>();
    private readonly QueryExecutor _sut;

    public QueryExecutorEmptyResultTests()
    {
        _sut = new QueryExecutor(
            _apiClient,
            TypeConverter.Shared,
            "testaccount",
            network: null,
            NullLogger<QueryExecutor>.Instance,
            onConnectionFault: () => { });
    }

    private static AuthenticationToken CreateToken() => new()
    {
        SessionToken = "session-token-abc",
        MasterToken = "master-token-123",
        ExpiresAt = DateTimeOffset.UtcNow.AddHours(1),
    };

    private static QueryRequest Request(AuthenticationToken token) => new()
    {
        Statement = "SELECT * FROM T WHERE 1 = 0",
        AuthToken = token,
    };

    private void SetupQueryResponse(SnowflakeQueryResponse data) =>
        _apiClient
            .PostAsync<SnowflakeQueryRequestBody, SnowflakeQueryResponse>(
                Arg.Is<string>(e => e.Contains("/queries/v1/query-request")),
                Arg.Any<SnowflakeQueryRequestBody>(),
                Arg.Any<AuthenticationToken>(),
                Arg.Any<CancellationToken>())
            .Returns(new ApiResponse<SnowflakeQueryResponse> { Success = true, Data = data });

    private static SnowflakeQueryResponse ZeroRowArrowResponse(string format = "arrow") => new()
    {
        QueryResultFormat = format,
        RowSetBase64 = "",
        Returned = 0,
        RowType =
        [
            new RowType { Name = "ID", Type = "fixed", Precision = 38, Scale = 0, Nullable = false },
            new RowType { Name = "NAME", Type = "text", Length = 100, Nullable = true },
        ],
    };

    [Theory]
    [InlineData("arrow")]
    [InlineData("ARROW")] // format comparison must be case-insensitive
    public async Task ExecuteQueryAsync_ArrowFormatZeroRows_ReturnsEmptyStreamWithRowTypeSchema(string format)
    {
        SetupQueryResponse(ZeroRowArrowResponse(format));

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()));

        Assert.Equal(QueryStatus.Success, result.Status);
        Assert.Equal(0, result.RowCount);
        Assert.NotNull(result.ResultStream);

        using var stream = result.ResultStream;
        Assert.Equal(2, stream.Schema.FieldsList.Count);
        // NUMBER(38,0) exceeds Int64, so it maps to Decimal128 — the same rule the result
        // decoder applies to non-empty results, keeping the schema stable either way.
        Assert.Equal("ID", stream.Schema.FieldsList[0].Name);
        Assert.IsType<Decimal128Type>(stream.Schema.FieldsList[0].DataType);
        Assert.Equal("NAME", stream.Schema.FieldsList[1].Name);
        Assert.IsType<StringType>(stream.Schema.FieldsList[1].DataType);

        Assert.Null(await stream.ReadNextRecordBatchAsync());
    }

    [Fact]
    public async Task ExecuteQueryAsync_ZeroRowsWithUnmappedColumnType_FailsWithSchemaError()
    {
        // A rowtype column type the converter cannot map (e.g. VECTOR): with zero rows there
        // is no Arrow wire type to pass through, so the driver fails naming the real reason.
        var data = ZeroRowArrowResponse();
        data.RowType = [new RowType { Name = "V", Type = "vector", Nullable = true }];
        SetupQueryResponse(data);

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()));

        Assert.Equal(QueryStatus.Failed, result.Status);
        QueryError error = Assert.Single(result.Errors);
        Assert.Equal("UNSUPPORTED_RESULT_SCHEMA", error.ErrorCode);
        Assert.Contains("zero rows", error.Message);
    }

    [Fact]
    public async Task ExecuteQueryAsync_ArrowFormatZeroRowsWithoutRowType_FailsAsUnsupportedShape()
    {
        // Without rowtype there is no schema to build and no rowset to surface: the driver
        // cannot represent the response, so it must fail explicitly rather than return a
        // success with nothing in it.
        var data = ZeroRowArrowResponse();
        data.RowType = null;
        SetupQueryResponse(data);

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()));

        Assert.Equal(QueryStatus.Failed, result.Status);
        Assert.Null(result.ResultStream);
        QueryError error = Assert.Single(result.Errors);
        Assert.Equal("UNSUPPORTED_RESULT_SHAPE", error.ErrorCode);
    }

    [Fact]
    public async Task SnowflakeStatement_EmptyResult_IsReadableWithoutError()
    {
        var schema = new Schema([new Field("ID", Int32Type.Default, nullable: true)], null);
        var executor = Substitute.For<IQueryExecutor>();
        executor.ExecuteQueryAsync(Arg.Any<QueryRequest>(), Arg.Any<CancellationToken>())
            .Returns(new Services.Query.QueryResult
            {
                Status = QueryStatus.Success,
                ResultStream = new EmptyArrowArrayStream(schema),
                RowCount = 0,
            });

        var pooledConnection = Substitute.For<IPooledConnection>();
        pooledConnection.AuthToken.Returns(CreateToken());

        using var statement = new SnowflakeStatement(new ConnectionConfig(), pooledConnection, executor);
        statement.SqlQuery = "SELECT ID FROM T WHERE 1 = 0";

        Apache.Arrow.Adbc.QueryResult result = await statement.ExecuteQueryAsync();

        Assert.NotNull(result.Stream);
        using var stream = result.Stream;
        Assert.Single(stream.Schema.FieldsList);
        Assert.Null(await stream.ReadNextRecordBatchAsync());
    }
}
