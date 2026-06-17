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
using AdbcDrivers.Snowflake.Native.Configuration;
using AdbcDrivers.Snowflake.Native.Services.Authentication;
using AdbcDrivers.Snowflake.Native.Services.ConnectionPool;
using AdbcDrivers.Snowflake.Native.Services.Query;
using Apache.Arrow.Adbc;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace AdbcDrivers.Snowflake.Native.Tests;

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

    [Fact]
    public void Commit_WithAutocommitEnabled_Throws()
    {
        var ex = Assert.Throws<AdbcException>(_sut.Commit);
        Assert.Contains("autocommit", ex.Message);
        _executor.DidNotReceiveWithAnyArgs().ExecuteQueryAsync(default!, default);
    }

    [Fact]
    public void Rollback_WithAutocommitEnabled_Throws()
    {
        Assert.Throws<AdbcException>(_sut.Rollback);
        _executor.DidNotReceiveWithAnyArgs().ExecuteQueryAsync(default!, default);
    }

    [Fact]
    public async Task SetOption_DisableAutocommit_AltersSession()
    {
        _sut.SetOption(AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled);

        await Executed("ALTER SESSION SET AUTOCOMMIT = FALSE");
    }

    [Fact]
    public void SetOption_SameValue_IsNoOp()
    {
        _sut.SetOption(AdbcOptions.Connection.Autocommit, AdbcOptions.Enabled);

        _executor.DidNotReceiveWithAnyArgs().ExecuteQueryAsync(default!, default);
    }

    [Fact]
    public async Task CommitAndRollback_InTransactionScope_RunTheStatements()
    {
        _sut.SetOption(AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled);

        _sut.Commit();
        await Executed("COMMIT");

        _sut.Rollback();
        await Executed("ROLLBACK");
    }

    [Fact]
    public async Task SetOption_ReenableAutocommit_CommitsPendingWorkFirst()
    {
        _sut.SetOption(AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled);

        _sut.SetOption(AdbcOptions.Connection.Autocommit, AdbcOptions.Enabled);

        // The ADBC contract: turning autocommit back on commits the pending transaction.
        Received.InOrder(() =>
        {
            _executor.ExecuteQueryAsync(Arg.Is<QueryRequest>(r => r.Statement == "COMMIT"), Arg.Any<CancellationToken>());
            _executor.ExecuteQueryAsync(Arg.Is<QueryRequest>(r => r.Statement == "ALTER SESSION SET AUTOCOMMIT = TRUE"), Arg.Any<CancellationToken>());
        });
        await Task.CompletedTask;
    }

    [Fact]
    public void SetOption_UnknownKey_ThrowsNotImplemented()
    {
        var ex = Assert.Throws<AdbcException>(() => _sut.SetOption("adbc.connection.readonly", "true"));
        Assert.Contains("not supported", ex.Message);
    }

    [Fact]
    public async Task Dispose_MidTransaction_RollsBackAndRestoresAutocommitBeforeRelease()
    {
        _sut.SetOption(AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled);

        _sut.Dispose();

        await Executed("ROLLBACK");
        await Executed("ALTER SESSION SET AUTOCOMMIT = TRUE");
        _pool.Received(1).ReleaseConnection(_pooled);
        _pooled.DidNotReceive().IsFaulted = true;
    }

    [Fact]
    public void Dispose_TransactionResetFails_FaultsTheConnection()
    {
        _sut.SetOption(AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled);
        _executor.ExecuteQueryAsync(Arg.Any<QueryRequest>(), Arg.Any<CancellationToken>())
            .Returns(Services.Query.QueryResult.Failed("EXECUTION_ERROR", "connection reset"));

        _sut.Dispose();

        // The session's transaction state is unknown; it must not be reused.
        _pooled.Received().IsFaulted = true;
        _pool.Received(1).ReleaseConnection(_pooled);
    }

    [Fact]
    public void Dispose_NoTransaction_DoesNotRunSessionStatements()
    {
        _sut.Dispose();

        _executor.DidNotReceiveWithAnyArgs().ExecuteQueryAsync(default!, default);
        _pool.Received(1).ReleaseConnection(_pooled);
    }
}
