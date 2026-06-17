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

using Ipc = Apache.Arrow.Ipc;

namespace AdbcDrivers.Snowflake.Native.Tests;

/// <summary>
/// Tests for how non-Arrow (JSON) command results are surfaced, in parity with the Go driver:
/// DML statements expose their affected-count summary row as a result set (with the summed
/// count in <see cref="Services.Query.QueryResult.AffectedRows"/> for ExecuteUpdate), and other
/// JSON rowsets (DDL status messages etc.) come back as string columns instead of an error.
/// </summary>
[Trait("Category", "Unit")]
public class QueryExecutorCommandResultTests
{
    private readonly IRestApiClient _apiClient = Substitute.For<IRestApiClient>();
    private readonly QueryExecutor _sut;

    public QueryExecutorCommandResultTests()
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
        Statement = "INSERT INTO T VALUES (1), (2)",
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

    [Fact]
    public async Task ExecuteQueryAsync_DmlInsert_ReturnsSummaryRowAndAffectedCount()
    {
        SetupQueryResponse(new SnowflakeQueryResponse
        {
            QueryResultFormat = "json",
            Returned = 1,
            RowType = [new RowType { Name = "number of rows inserted", Type = "fixed", Precision = 19, Scale = 0 }],
            RowSet = [["2"]],
        });

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()));

        Assert.Equal(QueryStatus.Success, result.Status);
        Assert.Equal(2, result.AffectedRows);
        Assert.Equal(2, result.RowCount);
        Assert.NotNull(result.ResultStream);

        using var stream = result.ResultStream;
        Field field = Assert.Single(stream.Schema.FieldsList);
        Assert.Equal("number of rows inserted", field.Name);
        Assert.IsType<Int64Type>(field.DataType);

        using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
        Assert.NotNull(batch);
        Assert.Equal(1, batch.Length);
        Assert.Equal(2L, ((Int64Array)batch.Column(0)).GetValue(0));
        Assert.Null(await stream.ReadNextRecordBatchAsync());
    }

    [Fact]
    public async Task ExecuteQueryAsync_DmlMerge_ReturnsAllCountColumns()
    {
        SetupQueryResponse(new SnowflakeQueryResponse
        {
            QueryResultFormat = "json",
            Returned = 1,
            RowType =
            [
                new RowType { Name = "number of rows inserted", Type = "fixed" },
                new RowType { Name = "number of rows updated", Type = "fixed" },
            ],
            RowSet = [["3", "2"]],
        });

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()));

        Assert.Equal(5, result.AffectedRows);
        Assert.NotNull(result.ResultStream);

        using var stream = result.ResultStream;
        using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
        Assert.NotNull(batch);
        Assert.Equal(3L, ((Int64Array)batch.Column(0)).GetValue(0));
        Assert.Equal(2L, ((Int64Array)batch.Column(1)).GetValue(0));
    }

    [Fact]
    public async Task ExecuteQueryAsync_MultiRowDmlSummary_SumsAndSurfacesAllRows()
    {
        // DML summaries are normally a single row, but the driver must not silently drop
        // extra rows if the server ever sends them.
        SetupQueryResponse(new SnowflakeQueryResponse
        {
            QueryResultFormat = "json",
            Returned = 2,
            RowType = [new RowType { Name = "number of rows inserted", Type = "fixed" }],
            RowSet = [["2"], ["3"]],
        });

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()));

        Assert.Equal(5, result.AffectedRows);
        Assert.NotNull(result.ResultStream);

        using var stream = result.ResultStream;
        using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
        Assert.NotNull(batch);
        Assert.Equal(2, batch.Length);
        var column = (Int64Array)batch.Column(0);
        Assert.Equal(2L, column.GetValue(0));
        Assert.Equal(3L, column.GetValue(1));
    }

    [Fact]
    public async Task ExecuteQueryAsync_JsonDdlStatus_ReturnsStatusRowAsStrings()
    {
        // DDL status results (e.g. CREATE TABLE) arrive as a JSON rowset; parity with the Go
        // driver means surfacing the status row, not failing for lack of an Arrow stream.
        SetupQueryResponse(new SnowflakeQueryResponse
        {
            QueryResultFormat = "json",
            Returned = 1,
            RowType = [new RowType { Name = "status", Type = "text" }],
            RowSet = [["Table T successfully created."]],
        });

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()));

        Assert.Equal(QueryStatus.Success, result.Status);
        Assert.Null(result.AffectedRows); // not DML: ExecuteUpdate must report -1, not a count
        Assert.NotNull(result.ResultStream);

        using var stream = result.ResultStream;
        Field field = Assert.Single(stream.Schema.FieldsList);
        Assert.Equal("status", field.Name);
        Assert.IsType<StringType>(field.DataType);

        using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
        Assert.NotNull(batch);
        Assert.Equal(1, batch.Length);
        Assert.Equal("Table T successfully created.", ((StringArray)batch.Column(0)).GetString(0));
    }

    [Fact]
    public async Task ExecuteQueryAsync_JsonRowSetWithNullRowAndRaggedRow_SurfacesNulls()
    {
        // Defensive: a null row entry or a row with fewer cells than columns must become null
        // values, not a crash.
        SetupQueryResponse(new SnowflakeQueryResponse
        {
            QueryResultFormat = "json",
            Returned = 3,
            RowType =
            [
                new RowType { Name = "A", Type = "text" },
                new RowType { Name = "B", Type = "text" },
            ],
            RowSet = [["a1", "b1"], null!, ["a3"]],
        });

        Services.Query.QueryResult result = await _sut.ExecuteQueryAsync(Request(CreateToken()));

        Assert.Equal(QueryStatus.Success, result.Status);
        Assert.NotNull(result.ResultStream);

        using var stream = result.ResultStream;
        using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
        Assert.NotNull(batch);
        Assert.Equal(3, batch.Length);
        var columnB = (StringArray)batch.Column(1);
        Assert.Equal("b1", columnB.GetString(0));
        Assert.True(columnB.IsNull(1)); // null row
        Assert.True(columnB.IsNull(2)); // ragged row
    }

    // ---- Statement-level: how ExecuteQuery/ExecuteUpdate consume the executor result ----

    private static SnowflakeStatement CreateStatement(Services.Query.QueryResult executorResult)
    {
        var executor = Substitute.For<IQueryExecutor>();
        executor.ExecuteQueryAsync(Arg.Any<QueryRequest>(), Arg.Any<CancellationToken>())
            .Returns(executorResult);

        var pooledConnection = Substitute.For<IPooledConnection>();
        pooledConnection.AuthToken.Returns(CreateToken());

        var statement = new SnowflakeStatement(new ConnectionConfig(), pooledConnection, executor);
        statement.SqlQuery = "INSERT INTO T VALUES (1), (2)";
        return statement;
    }

    [Fact]
    public async Task ExecuteUpdate_DmlResult_ReportsAffectedRowsAndDisposesStream()
    {
        var stream = Substitute.For<Ipc.IArrowArrayStream>();
        using var statement = CreateStatement(new Services.Query.QueryResult
        {
            Status = QueryStatus.Success,
            ResultStream = stream,
            RowCount = 2,
            AffectedRows = 2,
        });

        Apache.Arrow.Adbc.UpdateResult result = await statement.ExecuteUpdateAsync();

        Assert.Equal(2, result.AffectedRows);
        stream.Received(1).Dispose();
    }

    [Fact]
    public async Task ExecuteUpdate_ResultSetWithoutAffectedRows_ReportsUnknown()
    {
        // A SELECT (or DDL status row) run through ExecuteUpdate: a result set with no DML
        // count still reports -1 per the ADBC contract.
        using var statement = CreateStatement(new Services.Query.QueryResult
        {
            Status = QueryStatus.Success,
            ResultStream = new EmptyArrowArrayStream(new Schema([new Field("ID", Int32Type.Default, nullable: true)], null)),
            RowCount = 0,
        });

        Apache.Arrow.Adbc.UpdateResult result = await statement.ExecuteUpdateAsync();

        Assert.Equal(-1, result.AffectedRows);
    }

    [Fact]
    public async Task ExecuteQuery_CancelledResult_ThrowsCancelledError()
    {
        // A cancelled result is neither Success nor Failed and carries no stream; it must
        // surface as a cancellation error, not a missing-stream error or a bogus result.
        using var statement = CreateStatement(new Services.Query.QueryResult
        {
            Status = QueryStatus.Cancelled,
        });

        var ex = await Assert.ThrowsAsync<Apache.Arrow.Adbc.AdbcException>(
            async () => await statement.ExecuteQueryAsync());
        Assert.Contains("cancelled", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ExecuteUpdate_CancelledResult_ThrowsCancelledError()
    {
        // Previously a cancelled update surfaced as a successful UpdateResult(0).
        using var statement = CreateStatement(new Services.Query.QueryResult
        {
            Status = QueryStatus.Cancelled,
        });

        var ex = await Assert.ThrowsAsync<Apache.Arrow.Adbc.AdbcException>(
            statement.ExecuteUpdateAsync);
        Assert.Contains("cancelled", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ExecuteQuery_DmlResult_SummaryRowIsReadable()
    {
        var schema = new Schema([new Field("number of rows inserted", Int64Type.Default, nullable: true)], null);
        var builder = new Int64Array.Builder();
        builder.Append(2);
        using var statement = CreateStatement(new Services.Query.QueryResult
        {
            Status = QueryStatus.Success,
            ResultStream = new InMemoryArrowStream(schema, [builder.Build()]),
            RowCount = 2,
            AffectedRows = 2,
        });

        Apache.Arrow.Adbc.QueryResult result = await statement.ExecuteQueryAsync();

        Assert.Equal(2, result.RowCount);
        Assert.NotNull(result.Stream);
        using var stream = result.Stream;
        using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
        Assert.NotNull(batch);
        Assert.Equal(2L, ((Int64Array)batch.Column(0)).GetValue(0));
    }
}
