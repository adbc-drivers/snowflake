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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Configuration;
using AdbcDrivers.Snowflake.Services.Authentication;
using AdbcDrivers.Snowflake.Services.ConnectionPool;
using AdbcDrivers.Snowflake.Services.Query;
using Apache.Arrow.Adbc;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace AdbcDrivers.Snowflake.Tests;

/// <summary>
/// Offline tests for connection transaction control: the autocommit option toggles the
/// session's AUTOCOMMIT setting, Commit/Rollback run only inside a transaction scope, and a
/// connection released mid-transaction is cleaned up (or discarded) so pooled reuse is safe.
/// </summary>
[Trait("Category", "Unit")]
public class SnowflakeConnectionTransactionTests
{
    private readonly IQueryExecutor _executor = Substitute.For<IQueryExecutor>();
    private readonly IConnectionPoolManager _pool = Substitute.For<IConnectionPoolManager>();
    private readonly IPooledConnection _pooled = Substitute.For<IPooledConnection>();
    private readonly SnowflakeConnection _sut;

    public SnowflakeConnectionTransactionTests()
    {
        _pooled.AuthToken.Returns(new AuthenticationToken
        {
            SessionToken = "session-token",
            MasterToken = "master-token",
            ExpiresAt = DateTimeOffset.UtcNow.AddHours(1),
        });
        _executor.ExecuteQueryAsync(Arg.Any<QueryRequest>(), Arg.Any<CancellationToken>())
            .Returns(new Services.Query.QueryResult { Status = QueryStatus.Success });

        _sut = new SnowflakeConnection(
            new ConnectionConfig(), _pool, _pooled, _executor, NullLogger<SnowflakeConnection>.Instance);
    }

    private Task<Services.Query.QueryResult> Executed(string statement) =>
        _executor.Received(1).ExecuteQueryAsync(
            Arg.Is<QueryRequest>(r => r.Statement == statement), Arg.Any<CancellationToken>());

    // ---- Async surface (the implementation; the sync methods block on it) ----

    [Fact]
    public async Task CommitAsync_WithAutocommitEnabled_Throws()
    {
        var ex = await Assert.ThrowsAsync<AdbcException>(() => _sut.CommitAsync());
        Assert.Contains("autocommit", ex.Message);
        Assert.Equal(AdbcStatusCode.InvalidState, ex.Status);
        await _executor.DidNotReceiveWithAnyArgs().ExecuteQueryAsync(default!, default);
    }

    [Fact]
    public async Task RollbackAsync_WithAutocommitEnabled_Throws()
    {
        await Assert.ThrowsAsync<AdbcException>(() => _sut.RollbackAsync());
        await _executor.DidNotReceiveWithAnyArgs().ExecuteQueryAsync(default!, default);
    }

    [Fact]
    public async Task SetOptionAsync_DisableAutocommit_AltersSession()
    {
        await _sut.SetOptionAsync(AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled);

        await Executed("ALTER SESSION SET AUTOCOMMIT = FALSE");
    }

    [Fact]
    public async Task SetOptionAsync_SameValue_IsNoOp()
    {
        await _sut.SetOptionAsync(AdbcOptions.Connection.Autocommit, AdbcOptions.Enabled);

        await _executor.DidNotReceiveWithAnyArgs().ExecuteQueryAsync(default!, default);
    }

    [Fact]
    public async Task CommitAsyncAndRollbackAsync_InTransactionScope_RunTheStatements()
    {
        await _sut.SetOptionAsync(AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled);

        await _sut.CommitAsync();
        await Executed("COMMIT");

        await _sut.RollbackAsync();
        await Executed("ROLLBACK");
    }

    [Fact]
    public async Task SetOptionAsync_ReenableAutocommit_CommitsPendingWorkFirst()
    {
        await _sut.SetOptionAsync(AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled);

        await _sut.SetOptionAsync(AdbcOptions.Connection.Autocommit, AdbcOptions.Enabled);

        // The ADBC contract: turning autocommit back on commits the pending transaction.
        Received.InOrder(() =>
        {
            _executor.ExecuteQueryAsync(Arg.Is<QueryRequest>(r => r.Statement == "COMMIT"), Arg.Any<CancellationToken>());
            _executor.ExecuteQueryAsync(Arg.Is<QueryRequest>(r => r.Statement == "ALTER SESSION SET AUTOCOMMIT = TRUE"), Arg.Any<CancellationToken>());
        });
    }

    [Fact]
    public async Task SetOptionAsync_UnknownKey_ThrowsNotImplemented()
    {
        var ex = await Assert.ThrowsAsync<AdbcException>(() => _sut.SetOptionAsync("adbc.connection.readonly", "true"));
        Assert.Equal(AdbcStatusCode.NotImplemented, ex.Status);
        Assert.Contains("not supported", ex.Message);
    }

    [Fact]
    public async Task TransactionMethods_TokenAlreadyCancelled_SendNothing()
    {
        // The token is honored only before a session statement is sent: abandoning a COMMIT or an
        // autocommit change mid-flight would leave the session's transaction state unknown.
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => _sut.SetOptionAsync(AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled, cts.Token));
        await _sut.SetOptionAsync(AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled);
        _executor.ClearReceivedCalls();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => _sut.CommitAsync(cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => _sut.RollbackAsync(cts.Token));
        await _executor.DidNotReceiveWithAnyArgs().ExecuteQueryAsync(default!, default);
    }

    [Fact]
    public async Task SessionStatements_AreSentWithoutTheCallersToken()
    {
        using var cts = new CancellationTokenSource();
        await _sut.SetOptionAsync(AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled, cts.Token);
        await _sut.CommitAsync(cts.Token);

        await _executor.DidNotReceive().ExecuteQueryAsync(Arg.Any<QueryRequest>(), cts.Token);
    }

    [Fact]
    public async Task DisposeAsync_MidTransaction_RollsBackAndRestoresAutocommitBeforeRelease()
    {
        await _sut.SetOptionAsync(AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled);

        await _sut.DisposeAsync();

        Received.InOrder(() =>
        {
            _executor.ExecuteQueryAsync(Arg.Is<QueryRequest>(r => r.Statement == "ROLLBACK"), Arg.Any<CancellationToken>());
            _executor.ExecuteQueryAsync(Arg.Is<QueryRequest>(r => r.Statement == "ALTER SESSION SET AUTOCOMMIT = TRUE"), Arg.Any<CancellationToken>());
            _pool.ReleaseConnectionAsync(_pooled);
        });
        _pool.DidNotReceive().ReleaseConnection(Arg.Any<IPooledConnection>());
        _pooled.DidNotReceive().IsFaulted = true;
    }

    [Fact]
    public async Task DisposeAsync_TransactionResetFails_FaultsTheConnection()
    {
        await _sut.SetOptionAsync(AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled);
        _executor.ExecuteQueryAsync(Arg.Any<QueryRequest>(), Arg.Any<CancellationToken>())
            .Returns(Services.Query.QueryResult.Failed("EXECUTION_ERROR", "connection reset"));

        await _sut.DisposeAsync();

        // The session's transaction state is unknown; it must not be reused.
        _pooled.Received().IsFaulted = true;
        await _pool.Received(1).ReleaseConnectionAsync(_pooled);
    }

    [Fact]
    public async Task DisposeAsync_NoTransaction_DoesNotRunSessionStatements()
    {
        await _sut.DisposeAsync();
        await _sut.DisposeAsync();

        await _executor.DidNotReceiveWithAnyArgs().ExecuteQueryAsync(default!, default);
        await _pool.Received(1).ReleaseConnectionAsync(_pooled);
    }

    [Fact]
    public async Task DisposeAsync_WhileResetInFlight_RejectsNewWorkAndIgnoresASecondDispose()
    {
        await _sut.SetOptionAsync(AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled);
        var rollback = new TaskCompletionSource<Services.Query.QueryResult>();
        _executor.ExecuteQueryAsync(Arg.Is<QueryRequest>(r => r.Statement == "ROLLBACK"), Arg.Any<CancellationToken>())
            .Returns(rollback.Task);

        Task first = _sut.DisposeAsync().AsTask();
        Assert.False(first.IsCompleted);

        // Disposal is claimed before the reset is awaited: new work is rejected, and a concurrent
        // second dispose neither resets nor releases the session again.
        await Assert.ThrowsAsync<ObjectDisposedException>(() => _sut.CommitAsync());
        await _sut.DisposeAsync();
        await _pool.DidNotReceive().ReleaseConnectionAsync(Arg.Any<IPooledConnection>());

        rollback.SetResult(new Services.Query.QueryResult { Status = QueryStatus.Success });
        await first;
        await _executor.Received(1).ExecuteQueryAsync(Arg.Is<QueryRequest>(r => r.Statement == "ROLLBACK"), Arg.Any<CancellationToken>());
        await _pool.Received(1).ReleaseConnectionAsync(_pooled);
    }

    // ---- Sync surface smoke: each wrapper reaches the async implementation ----

    [Fact]
    public async Task Sync_SetOptionCommitRollback_RunTheStatements()
    {
        Assert.Throws<AdbcException>(_sut.Commit);

        _sut.SetOption(AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled);
        _sut.Commit();
        _sut.Rollback();

        await Executed("ALTER SESSION SET AUTOCOMMIT = FALSE");
        await Executed("COMMIT");
        await Executed("ROLLBACK");
    }

    [Fact]
    public async Task Sync_Dispose_MidTransaction_ResetsAndReleases()
    {
        _sut.SetOption(AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled);

        _sut.Dispose();

        await Executed("ROLLBACK");
        await Executed("ALTER SESSION SET AUTOCOMMIT = TRUE");
        _pool.Received(1).ReleaseConnection(_pooled);
        await _pool.DidNotReceive().ReleaseConnectionAsync(Arg.Any<IPooledConnection>());
    }
}
