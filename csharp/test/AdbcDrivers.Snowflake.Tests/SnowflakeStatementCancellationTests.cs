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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Configuration;
using AdbcDrivers.Snowflake.Services.Authentication;
using AdbcDrivers.Snowflake.Services.ConnectionPool;
using AdbcDrivers.Snowflake.Services.Query;
using NSubstitute;
using Xunit;

using Ipc = Apache.Arrow.Ipc;

namespace AdbcDrivers.Snowflake.Tests;

/// <summary>
/// Tests for the <see cref="CancellationToken"/> overloads of ExecuteQueryAsync/ExecuteUpdateAsync
/// that Apache.Arrow.Adbc 1.0 added to <see cref="Apache.Arrow.Adbc.AdbcStatement"/>.
/// </summary>
[Trait("Category", "Unit")]
public class SnowflakeStatementCancellationTests
{
    private readonly IQueryExecutor _executor = Substitute.For<IQueryExecutor>();
    private readonly AuthenticationToken _authToken = new()
    {
        SessionToken = "session-token-abc",
        MasterToken = "master-token-123",
        ExpiresAt = DateTimeOffset.UtcNow.AddHours(1),
    };

    private SnowflakeStatement CreateStatement()
    {
        var pooledConnection = Substitute.For<IPooledConnection>();
        pooledConnection.AuthToken.Returns(_authToken);
        return new SnowflakeStatement(new ConnectionConfig(), pooledConnection, _executor)
        {
            SqlQuery = "SELECT 1",
        };
    }

    // Mimics QueryExecutor: waits on the token and reports a cancelled token as a Cancelled result.
    private void ExecutorBlocksUntilCancelled()
    {
        _executor.ExecuteQueryAsync(Arg.Any<QueryRequest>(), Arg.Any<CancellationToken>())
            .Returns(async call =>
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, call.Arg<CancellationToken>());
                }
                catch (OperationCanceledException)
                {
                }
                return Services.Query.QueryResult.Cancelled();
            });
    }

    private static Services.Query.QueryResult Success() => new()
    {
        Status = QueryStatus.Success,
        ResultStream = Substitute.For<Ipc.IArrowArrayStream>(),
        AffectedRows = 1,
    };

    [Fact]
    public async Task ExecuteQueryAsync_TokenCancelledInFlight_ThrowsOperationCanceledAndAbortsOnServer()
    {
        ExecutorBlocksUntilCancelled();
        using var statement = CreateStatement();
        using var cts = new CancellationTokenSource();

        var execute = statement.ExecuteQueryAsync(cts.Token).AsTask();
        var request = (QueryRequest)_executor.ReceivedCalls().Single().GetArguments()[0]!;
        cts.Cancel();

        var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => execute);
        Assert.Equal(cts.Token, ex.CancellationToken);
        // The fire-and-forget abort gets its own bounded token, not the (cancelled) caller's.
        await _executor.Received(1).CancelQueryAsync(request.RequestId!, _authToken,
            Arg.Is<CancellationToken>(t => t.CanBeCanceled && !t.IsCancellationRequested));
    }

    [Fact]
    public async Task ExecuteQueryAsync_TokenCancelledAfterResultBuilt_DiscardsResultAndThrows()
    {
        // The executor returns Success, but the caller cancels after the result stream was built
        // (and linked to the execution token) and before ExecuteQueryAsync returns. That stream's
        // prefetch is already cancelled, so handing it out would truncate the result.
        var stream = Substitute.For<Ipc.IArrowArrayStream>();
        using var cts = new CancellationTokenSource();
        _executor.ExecuteQueryAsync(Arg.Any<QueryRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                cts.Cancel();
                return Task.FromResult(new Services.Query.QueryResult { Status = QueryStatus.Success, ResultStream = stream });
            });
        using var statement = CreateStatement();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => statement.ExecuteQueryAsync(cts.Token).AsTask());

        stream.Received(1).Dispose();
    }

    [Fact]
    public async Task ExecuteUpdateAsync_TokenCancelledInFlight_ThrowsOperationCanceledAndAbortsOnServer()
    {
        ExecutorBlocksUntilCancelled();
        using var statement = CreateStatement();
        using var cts = new CancellationTokenSource();

        var execute = statement.ExecuteUpdateAsync(cts.Token);
        var request = (QueryRequest)_executor.ReceivedCalls().Single().GetArguments()[0]!;
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => execute);
        await _executor.Received(1).CancelQueryAsync(request.RequestId!, _authToken, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ExecuteQueryAsync_TokenAlreadyCancelled_DoesNotSendQuery()
    {
        using var statement = CreateStatement();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => statement.ExecuteQueryAsync(new CancellationToken(canceled: true)).AsTask());

        Assert.Empty(_executor.ReceivedCalls());
    }

    [Fact]
    public async Task ExecuteQueryAsync_TokenCancelledAfterReturn_DoesNotReachResultStreamOrServer()
    {
        CancellationToken executorToken = default;
        _executor.ExecuteQueryAsync(Arg.Any<QueryRequest>(), Arg.Do<CancellationToken>(t => executorToken = t))
            .Returns(Success());
        using var statement = CreateStatement();
        using var cts = new CancellationTokenSource();

        await statement.ExecuteQueryAsync(cts.Token);
        cts.Cancel();

        // The result stream links the executor's token into its chunk prefetch; it must be scoped
        // to execution rather than be the caller's token.
        Assert.True(executorToken.CanBeCanceled);
        Assert.False(executorToken.IsCancellationRequested);
        await _executor.DidNotReceiveWithAnyArgs().CancelQueryAsync(default!, default!, default);
    }

    [Fact]
    public async Task ExecuteQueryAsync_NoToken_ForwardsWithUncancellableToken()
    {
        CancellationToken executorToken = new CancellationToken(canceled: true);
        _executor.ExecuteQueryAsync(Arg.Any<QueryRequest>(), Arg.Do<CancellationToken>(t => executorToken = t))
            .Returns(Success());
        using var statement = CreateStatement();

        await statement.ExecuteQueryAsync();

        Assert.False(executorToken.CanBeCanceled);
    }
}
