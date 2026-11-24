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
using Microsoft.Extensions.Time.Testing;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace AdbcDrivers.Snowflake.Tests.ConnectionPool;

[Trait("Category", "Unit")]
public class PooledConnectionTests
{
    [Fact]
    public void IsTokenExpired_IsKeyedOnMasterExpiry_AgainstTheInjectedClock()
    {
        // Given a connection whose session token expires in 1h but the master (recoverability
        // ceiling) expires in 4h — eviction should track the master, since a session-expired
        // connection is still usable via renewal until the master lapses.
        var fakeTime = new FakeTimeProvider();
        var token = new AuthenticationToken
        {
            SessionToken = "session",
            ExpiresAt = fakeTime.GetUtcNow().AddHours(1),
            MasterExpiresAt = fakeTime.GetUtcNow().AddHours(4),
        };
        var connection = new PooledConnection(
            "id",
            "pool-key",
            token,
            new ConnectionConfig { Account = "test" },
            sessionLifecycle: null,
            timeProvider: fakeTime);

        // Then it stays valid past the session expiry, and only flips once the master lapses
        Assert.False(connection.IsTokenExpired);

        fakeTime.Advance(TimeSpan.FromHours(2)); // session (1h) is long gone, master (4h) still alive
        Assert.False(connection.IsTokenExpired);

        fakeTime.Advance(TimeSpan.FromHours(2) + TimeSpan.FromMinutes(1)); // now past the 4h master
        Assert.True(connection.IsTokenExpired);
    }

    private static PooledConnection CreateConnection(ISessionLifecycle sessions) =>
        new("id", "pool-key", new AuthenticationToken { SessionToken = "session" },
            new ConnectionConfig { Account = "test" }, sessions);

    [Fact]
    public async Task DisposeAsync_ClosesTheSessionOnce_WithABoundedToken()
    {
        var sessions = Substitute.For<ISessionLifecycle>();
        var connection = CreateConnection(sessions);

        await connection.DisposeAsync();
        await connection.DisposeAsync();
        connection.Dispose();

        Assert.True(connection.IsDisposed);
        await sessions.Received(1).CloseAsync(connection.AuthToken, connection.Config,
            Arg.Is<CancellationToken>(t => t.CanBeCanceled));
    }

    [Fact]
    public async Task DisposeAsync_CloseFails_IsSwallowed()
    {
        var sessions = Substitute.For<ISessionLifecycle>();
        sessions.CloseAsync(default!, default!, default).ReturnsForAnyArgs(Task.FromException(new InvalidOperationException("network down")));
        var connection = CreateConnection(sessions);

        await connection.DisposeAsync();

        Assert.True(connection.IsDisposed);
    }

    [Fact]
    public async Task DisposeAsync_CloseIgnoresItsToken_StillCompletesAfterTheTimeout()
    {
        // A close that never completes and never observes cancellation must not hang disposal.
        var fakeTime = new FakeTimeProvider();
        var sessions = Substitute.For<ISessionLifecycle>();
        sessions.CloseAsync(default!, default!, default).ReturnsForAnyArgs(new TaskCompletionSource().Task);
        var connection = new PooledConnection("id", "pool-key", new AuthenticationToken { SessionToken = "session" },
            new ConnectionConfig { Account = "test" }, sessions, fakeTime);

        Task dispose = connection.DisposeAsync().AsTask();
        Assert.False(dispose.IsCompleted);

        fakeTime.Advance(PooledConnection.SessionCloseTimeout);
        await dispose.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(connection.IsDisposed);
    }

    [Fact]
    public async Task Sync_Dispose_ClosesTheSessionOnce()
    {
        var sessions = Substitute.For<ISessionLifecycle>();
        sessions.CloseAsync(default!, default!, default).ThrowsAsyncForAnyArgs(new InvalidOperationException("network down"));
        var connection = CreateConnection(sessions);

        connection.Dispose();
        await connection.DisposeAsync();

        Assert.True(connection.IsDisposed);
        await sessions.Received(1).CloseAsync(connection.AuthToken, connection.Config, Arg.Any<CancellationToken>());
    }
}
