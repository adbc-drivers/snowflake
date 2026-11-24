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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Configuration;
using AdbcDrivers.Snowflake.Services.Authentication;
using AdbcDrivers.Snowflake.Services.ConnectionPool;
using AdbcDrivers.Snowflake.Services.Query;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace AdbcDrivers.Snowflake.Tests;

/// <summary>
/// Offline tests for the async overrides of the connection's metadata methods and the statement's
/// local-only methods: that they pass the caller's token through, surface cancellation as
/// <see cref="OperationCanceledException"/>, and that the operations needing no I/O complete
/// without leaving the calling thread. Each has a sync smoke test proving the blocking wrapper
/// reaches the same implementation and rethrows the original exception.
/// </summary>
[Trait("Category", "Unit")]
public class SnowflakeAsyncSurfaceTests
{
    private readonly IQueryExecutor _executor = Substitute.For<IQueryExecutor>();
    private readonly IPooledConnection _pooled = Substitute.For<IPooledConnection>();
    private readonly SnowflakeConnection _connection;

    public SnowflakeAsyncSurfaceTests()
    {
        _pooled.AuthToken.Returns(new AuthenticationToken
        {
            SessionToken = "session-token",
            MasterToken = "master-token",
            ExpiresAt = DateTimeOffset.UtcNow.AddHours(1),
        });
        _connection = new SnowflakeConnection(new ConnectionConfig(), Substitute.For<IConnectionPoolManager>(),
            _pooled, _executor, NullLogger<SnowflakeConnection>.Instance);
    }

    private static readonly Schema DescribedSchema = new([new Field("ID", Apache.Arrow.Types.Int64Type.Default, true)], null);

    // ---- GetTableSchema ----

    [Fact]
    public async Task GetTableSchemaAsync_PassesTheTokenAndReturnsTheDescribedSchema()
    {
        using var cts = new CancellationTokenSource();
        _executor.DescribeAsync(Arg.Any<QueryRequest>(), Arg.Any<CancellationToken>())
            .Returns(new PreparedStatement { ResultSchema = DescribedSchema });

        Schema schema = await _connection.GetTableSchemaAsync("DB", "SCH", "T", cts.Token);

        Assert.Same(DescribedSchema, schema);
        await _executor.Received(1).DescribeAsync(
            Arg.Is<QueryRequest>(r => r.Statement == "SELECT * FROM \"DB\".\"SCH\".\"T\""), cts.Token);
    }

    [Fact]
    public async Task GetTableSchemaAsync_TokenCancelled_Throws()
    {
        _executor.DescribeAsync(Arg.Any<QueryRequest>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromCanceled<PreparedStatement>(call.Arg<CancellationToken>()));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => _connection.GetTableSchemaAsync(null, null, "T", new CancellationToken(canceled: true)));
    }

    [Fact]
    public void Sync_GetTableSchema_RethrowsTheOriginalException()
    {
        _executor.DescribeAsync(Arg.Any<QueryRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<PreparedStatement>(new AdbcException("no such table", AdbcStatusCode.InvalidArgument)));

        var ex = Assert.Throws<AdbcException>(() => _connection.GetTableSchema(null, null, "T"));
        Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
    }

    // ---- GetObjects ----

    [Fact]
    public async Task GetObjectsAsync_PassesTheTokenToEveryMetadataQuery()
    {
        using var cts = new CancellationTokenSource();
        _executor.ExecuteQueryAsync(Arg.Any<QueryRequest>(), Arg.Any<CancellationToken>())
            .Returns(new Services.Query.QueryResult { Status = QueryStatus.Success });

        using var stream = await _connection.GetObjectsAsync(
            AdbcConnection.GetObjectsDepth.Catalogs, "DB%", null, null, null, null, cts.Token);

        Assert.NotNull(stream);
        await _executor.Received(1).ExecuteQueryAsync(Arg.Any<QueryRequest>(), cts.Token);
    }

    [Fact]
    public async Task GetObjectsAsync_QueryCancelled_ThrowsOperationCanceled()
    {
        // The executor reports a cancelled token as a Cancelled result rather than throwing.
        using var cts = new CancellationTokenSource();
        _executor.ExecuteQueryAsync(Arg.Any<QueryRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                cts.Cancel();
                return Services.Query.QueryResult.Cancelled();
            });

        var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => _connection.GetObjectsAsync(
            AdbcConnection.GetObjectsDepth.Catalogs, null, null, null, null, null, cts.Token));
        Assert.Equal(cts.Token, ex.CancellationToken);
    }

    [Fact]
    public void Sync_GetObjects_QueryFails_ThrowsAdbcException()
    {
        _executor.ExecuteQueryAsync(Arg.Any<QueryRequest>(), Arg.Any<CancellationToken>())
            .Returns(Services.Query.QueryResult.Failed("002003", "Database does not exist"));

        var ex = Assert.Throws<AdbcException>(() => _connection.GetObjects(
            AdbcConnection.GetObjectsDepth.Catalogs, null, null, null, null, null));
        Assert.Contains("Database does not exist", ex.Message);
    }

    // ---- GetInfo / GetTableTypes: in-memory, so they complete synchronously ----

    [Fact]
    public async Task GetInfoAsyncAndGetTableTypesAsync_CompleteSynchronously()
    {
        Task<Apache.Arrow.Ipc.IArrowArrayStream> info = _connection.GetInfoAsync([AdbcInfoCode.VendorName]);
        Task<Apache.Arrow.Ipc.IArrowArrayStream> types = _connection.GetTableTypesAsync();

        Assert.True(info.IsCompletedSuccessfully);
        Assert.True(types.IsCompletedSuccessfully);
        using (var stream = await info)
        using (RecordBatch? batch = await stream.ReadNextRecordBatchAsync())
            Assert.Equal(1, batch!.Length);
        using (var stream = await types)
        using (RecordBatch? batch = await stream.ReadNextRecordBatchAsync())
            Assert.Equal(2, batch!.Length);
    }

    [Fact]
    public async Task GetInfoAsync_NullCodes_FaultsTheTaskInsteadOfThrowing()
    {
        Task<Apache.Arrow.Ipc.IArrowArrayStream> info = _connection.GetInfoAsync(null!);

        Assert.True(info.IsFaulted);
        await Assert.ThrowsAsync<ArgumentNullException>(() => info);
    }

    [Fact]
    public async Task GetTableTypesAsync_TokenAlreadyCancelled_IsCancelled()
    {
        Task<Apache.Arrow.Ipc.IArrowArrayStream> types = _connection.GetTableTypesAsync(new CancellationToken(canceled: true));

        Assert.True(types.IsCanceled);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => types);
    }

    [Fact]
    public void Sync_GetInfoAndGetTableTypes_ReturnStreams()
    {
        using var info = _connection.GetInfo(new List<AdbcInfoCode>());
        using var types = _connection.GetTableTypes();

        Assert.Equal(2, info.Schema.FieldsList.Count);
        Assert.Single(types.Schema.FieldsList);
    }

    // ---- Statement: Prepare / GetParameterSchema validate locally ----

    private SnowflakeStatement CreateStatement(string? sql) =>
        new(new ConnectionConfig(), _pooled, _executor) { SqlQuery = sql };

    [Fact]
    public async Task PrepareAsync_WithSql_CompletesSynchronouslyWithoutARoundTrip()
    {
        using var statement = CreateStatement("SELECT 1");

        Task prepare = statement.PrepareAsync();

        Assert.True(prepare.IsCompletedSuccessfully);
        await prepare;
        await _executor.DidNotReceiveWithAnyArgs().ExecuteQueryAsync(default!, default);
        await _executor.DidNotReceiveWithAnyArgs().DescribeAsync(default!, default);
    }

    [Fact]
    public async Task PrepareAsync_WithoutSql_FaultsWithInvalidOperation()
    {
        using var statement = CreateStatement(null);

        await Assert.ThrowsAsync<InvalidOperationException>(() => statement.PrepareAsync());
    }

    [Fact]
    public async Task GetParameterSchemaAsync_IsNotImplemented()
    {
        using var statement = CreateStatement("SELECT ?");

        var ex = await Assert.ThrowsAsync<AdbcException>(() => statement.GetParameterSchemaAsync());
        Assert.Equal(AdbcStatusCode.NotImplemented, ex.Status);
    }

    [Fact]
    public void Sync_PrepareAndGetParameterSchema_ThrowTheSameErrors()
    {
        using var statement = CreateStatement(null);

        Assert.Throws<InvalidOperationException>(statement.Prepare);
        statement.SqlQuery = "SELECT 1";
        statement.Prepare();
        Assert.Equal(AdbcStatusCode.NotImplemented, Assert.Throws<AdbcException>(statement.GetParameterSchema).Status);
    }
}
