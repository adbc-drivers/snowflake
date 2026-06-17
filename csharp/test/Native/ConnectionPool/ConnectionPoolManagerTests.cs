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
using Apache.Arrow.Adbc;
using Microsoft.Extensions.Time.Testing;
using NSubstitute;
using Xunit;

namespace AdbcDrivers.Snowflake.Native.Tests.ConnectionPool;

[Trait("Category", "Unit")]
public class ConnectionPoolManagerTests
{
    /// <summary>Minimal in-memory <see cref="IPooledConnection"/> for exercising the due-logic.</summary>
    private sealed class FakePooledConnection : IPooledConnection
    {
        public ConnectionConfig Config { get; init; } = new();
        public DateTimeOffset LastUsedAt { get; init; }
        public DateTimeOffset LastHeartbeatAt { get; set; }
        public int RecordHeartbeatCount { get; private set; }

        public string ConnectionId => "fake";
        public string PoolKey => "fake-pool-key";
        public AuthenticationToken AuthToken { get; } = new();
        public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
        public bool IsDisposed { get; init; }
        public bool IsTokenExpired => false;
        public bool IsFaulted { get; set; }
        void IPooledConnection.UpdateLastUsedAt() { }
        void IPooledConnection.RecordHeartbeat()
        {
            RecordHeartbeatCount++;
            LastHeartbeatAt = DateTimeOffset.UtcNow;
        }
        public void Dispose() { }
    }

    /// <summary>A keep-alive connection that has been idle long enough to be due at <paramref name="now"/>.</summary>
    private static FakePooledConnection DueConnection(DateTimeOffset now) =>
        new()
        {
            Config = new ConnectionConfig { ClientSessionKeepAlive = true, HeartbeatFrequency = TimeSpan.FromMinutes(15) },
            LastUsedAt = now - TimeSpan.FromMinutes(20),
            LastHeartbeatAt = now - TimeSpan.FromMinutes(20),
        };

    [Fact]
    public void IsHeartbeatDue_WhenKeepAliveDisabled_IsNeverDue()
    {
        // Given keep-alive off, even a long-idle connection is never due
        var now = DateTimeOffset.UtcNow;
        var connection = new FakePooledConnection
        {
            Config = new ConnectionConfig { ClientSessionKeepAlive = false, HeartbeatFrequency = TimeSpan.FromMinutes(15) },
            LastUsedAt = now - TimeSpan.FromHours(2),
            LastHeartbeatAt = now - TimeSpan.FromHours(2),
        };

        Assert.False(ConnectionPoolManager.IsHeartbeatDue(connection, now));
    }

    [Fact]
    public void IsHeartbeatDue_WhenIdleBeyondFrequency_IsDue()
    {
        // Given keep-alive on and no activity for longer than the frequency
        var now = DateTimeOffset.UtcNow;
        var connection = new FakePooledConnection
        {
            Config = new ConnectionConfig { ClientSessionKeepAlive = true, HeartbeatFrequency = TimeSpan.FromMinutes(15) },
            LastUsedAt = now - TimeSpan.FromMinutes(20),
            LastHeartbeatAt = now - TimeSpan.FromMinutes(20),
        };

        Assert.True(ConnectionPoolManager.IsHeartbeatDue(connection, now));
    }

    [Fact]
    public void IsHeartbeatDue_WithRecentActivity_IsNotDue()
    {
        // Given a recent query (LastUsedAt) even though the last heartbeat is old, the session is
        // already warm — activity counts as a heartbeat, so none is needed
        var now = DateTimeOffset.UtcNow;
        var connection = new FakePooledConnection
        {
            Config = new ConnectionConfig { ClientSessionKeepAlive = true, HeartbeatFrequency = TimeSpan.FromMinutes(15) },
            LastUsedAt = now - TimeSpan.FromMinutes(1),
            LastHeartbeatAt = now - TimeSpan.FromHours(1),
        };

        Assert.False(ConnectionPoolManager.IsHeartbeatDue(connection, now));
    }

    [Fact]
    public void IsHeartbeatDue_ExactlyAtBoundary_IsDue()
    {
        // When elapsed time equals the heartbeat frequency exactly, should be due (uses >=)
        var now = DateTimeOffset.UtcNow;
        var frequency = TimeSpan.FromMinutes(15);
        var connection = new FakePooledConnection
        {
            Config = new ConnectionConfig { ClientSessionKeepAlive = true, HeartbeatFrequency = frequency },
            LastUsedAt = now - frequency,
            LastHeartbeatAt = now - frequency,
        };

        Assert.True(ConnectionPoolManager.IsHeartbeatDue(connection, now));
    }

    [Fact]
    public void IsHeartbeatDue_WithRecentHeartbeat_IsNotDue()
    {
        // LastHeartbeatAt is recent even though LastUsedAt is old — not due
        var now = DateTimeOffset.UtcNow;
        var connection = new FakePooledConnection
        {
            Config = new ConnectionConfig { ClientSessionKeepAlive = true, HeartbeatFrequency = TimeSpan.FromMinutes(15) },
            LastUsedAt = now - TimeSpan.FromHours(2),
            LastHeartbeatAt = now - TimeSpan.FromMinutes(5),
        };

        Assert.False(ConnectionPoolManager.IsHeartbeatDue(connection, now));
    }

    [Fact]
    public async Task Pool_WithNoHeartbeatDelegate_DisposesCleanly()
    {
        // Create pool with no session-lifecycle collaborator, acquire+release, dispose — no crash
        var authService = Substitute.For<IAuthenticationService>();
        authService.AuthenticateAsync(Arg.Any<ConnectionConfig>(), Arg.Any<CancellationToken>())
            .Returns(new AuthenticationToken
            {
                SessionToken = "session",
                MasterToken = "master",
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(1),
                MasterExpiresAt = DateTimeOffset.UtcNow.AddHours(4),
            });

        var pool = new ConnectionPoolManager(authService, sessionLifecycle: null);
        try
        {
            var config = new ConnectionConfig
            {
                Account = "test",
                User = "user",
                ClientSessionKeepAlive = true,
                HeartbeatFrequency = TimeSpan.FromMinutes(15),
            };

            var connection = await pool.AcquireConnectionAsync(config);
            pool.ReleaseConnection(connection);
        }
        finally
        {
            pool.Dispose();
        }
    }

    [Fact]
    public async Task HeartbeatDueConnections_FiresForDueConnection_AndRecordsHeartbeat()
    {
        // Given one due keep-alive connection
        var now = DateTimeOffset.UtcNow;
        var connection = DueConnection(now);
        var calls = 0;

        // When the heartbeat pass runs
        await ConnectionPoolManager.HeartbeatDueConnectionsAsync(
            new[] { connection },
            (_, _, _) => { calls++; return Task.CompletedTask; },
            now,
            CancellationToken.None);

        // Then the delegate fired once and the heartbeat was recorded (resetting the due clock)
        Assert.Equal(1, calls);
        Assert.Equal(1, connection.RecordHeartbeatCount);
    }

    [Fact]
    public async Task HeartbeatDueConnections_SkipsDisabledNotDueAndDisposed()
    {
        // Given connections that should each be skipped for a different reason
        var now = DateTimeOffset.UtcNow;
        var keepAliveOff = new FakePooledConnection
        {
            Config = new ConnectionConfig { ClientSessionKeepAlive = false, HeartbeatFrequency = TimeSpan.FromMinutes(15) },
            LastUsedAt = now - TimeSpan.FromHours(1),
            LastHeartbeatAt = now - TimeSpan.FromHours(1),
        };
        var recentlyUsed = new FakePooledConnection
        {
            Config = new ConnectionConfig { ClientSessionKeepAlive = true, HeartbeatFrequency = TimeSpan.FromMinutes(15) },
            LastUsedAt = now - TimeSpan.FromMinutes(1),
            LastHeartbeatAt = now - TimeSpan.FromMinutes(1),
        };
        var disposed = new FakePooledConnection
        {
            Config = new ConnectionConfig { ClientSessionKeepAlive = true, HeartbeatFrequency = TimeSpan.FromMinutes(15) },
            LastUsedAt = now - TimeSpan.FromHours(1),
            LastHeartbeatAt = now - TimeSpan.FromHours(1),
            IsDisposed = true,
        };
        var calls = 0;

        // When the heartbeat pass runs
        await ConnectionPoolManager.HeartbeatDueConnectionsAsync(
            new[] { keepAliveOff, recentlyUsed, disposed },
            (_, _, _) => { calls++; return Task.CompletedTask; },
            now,
            CancellationToken.None);

        // Then none are heartbeated
        Assert.Equal(0, calls);
        Assert.Equal(0, keepAliveOff.RecordHeartbeatCount);
        Assert.Equal(0, recentlyUsed.RecordHeartbeatCount);
        Assert.Equal(0, disposed.RecordHeartbeatCount);
    }

    [Fact]
    public async Task HeartbeatDueConnections_WhenTokenCancelled_FiresNothing()
    {
        // Given a due connection but an already-cancelled token (e.g. the pool is being disposed)
        var now = DateTimeOffset.UtcNow;
        var connection = DueConnection(now);
        var calls = 0;
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // When the heartbeat pass runs under that token
        await ConnectionPoolManager.HeartbeatDueConnectionsAsync(
            new[] { connection },
            (_, _, _) => { calls++; return Task.CompletedTask; },
            now,
            cts.Token);

        // Then nothing is heartbeated
        Assert.Equal(0, calls);
        Assert.Equal(0, connection.RecordHeartbeatCount);
    }

    [Fact]
    public async Task BackgroundLoop_OnTick_HeartbeatsDueIdleConnection()
    {
        // Given a pool on a fake clock with a heartbeat delegate and a keep-alive connection. The
        // token's expiry is evaluated on the same clock, so it stays valid as fake time advances.
        var fakeTime = new FakeTimeProvider();
        var authService = Substitute.For<IAuthenticationService>();
        authService.AuthenticateAsync(Arg.Any<ConnectionConfig>(), Arg.Any<CancellationToken>())
            .Returns(new AuthenticationToken
            {
                SessionToken = "session",
                MasterToken = "master",
                ExpiresAt = fakeTime.GetUtcNow().AddHours(1),
                MasterExpiresAt = fakeTime.GetUtcNow().AddHours(4),
            });

        var heartbeatFired = new TaskCompletionSource();
        var sessionLifecycle = Substitute.For<ISessionLifecycle>();
        sessionLifecycle
            .HeartbeatAsync(Arg.Any<AuthenticationToken>(), Arg.Any<ConnectionConfig>(), Arg.Any<CancellationToken>())
            .Returns(_ => { heartbeatFired.TrySetResult(); return Task.CompletedTask; });
        using var pool = new ConnectionPoolManager(
            authService,
            sessionLifecycle,
            timeProvider: fakeTime);

        var config = new ConnectionConfig
        {
            Account = "test",
            User = "user",
            ClientSessionKeepAlive = true,
            HeartbeatFrequency = TimeSpan.FromSeconds(30), // due before one 60s loop tick, within the 10m idle timeout
        };

        // Acquire then release so the connection sits idle in the pool; this also starts the loop.
        var connection = await pool.AcquireConnectionAsync(config);
        pool.ReleaseConnection(connection);

        // When the background loop's 60s timer ticks once (the connection is now 60s idle, past 30s)
        fakeTime.Advance(TimeSpan.FromSeconds(60));

        // Then the loop heartbeats the idle connection (the delegate completes the signal)
        await heartbeatFired.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task HeartbeatDueConnections_WhenOneHeartbeatThrows_ContinuesWithTheRest()
    {
        // Given two due connections where the first connection's heartbeat fails
        var now = DateTimeOffset.UtcNow;
        var failing = DueConnection(now);
        var succeeding = DueConnection(now);
        var attempts = 0;

        // When the heartbeat pass runs
        await ConnectionPoolManager.HeartbeatDueConnectionsAsync(
            new[] { failing, succeeding },
            (_, _, _) =>
            {
                attempts++;
                return attempts == 1
                    ? Task.FromException(new InvalidOperationException("transient"))
                    : Task.CompletedTask;
            },
            now,
            CancellationToken.None);

        // Then the failure is swallowed, the second connection is still attempted, and only the
        // successful one records a heartbeat
        Assert.Equal(2, attempts);
        Assert.Equal(0, failing.RecordHeartbeatCount);
        Assert.Equal(1, succeeding.RecordHeartbeatCount);
    }

    [Fact]
    public async Task AcquireConnection_WhenPoolExhausted_TimesOutWithAdbcException()
    {
        var authService = Substitute.For<IAuthenticationService>();
        authService.AuthenticateAsync(Arg.Any<ConnectionConfig>(), Arg.Any<CancellationToken>())
            .Returns(new AuthenticationToken
            {
                SessionToken = "session",
                MasterToken = "master",
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(1),
                MasterExpiresAt = DateTimeOffset.UtcNow.AddHours(4),
            });

        using var pool = new ConnectionPoolManager(authService);
        var config = new ConnectionConfig
        {
            Account = "test",
            User = "user",
            PoolConfig = new ConnectionPoolConfig { MaxPoolSize = 1, AcquireTimeout = TimeSpan.FromMilliseconds(100) },
        };

        // The first acquire takes the pool's only slot and is deliberately not released.
        _ = await pool.AcquireConnectionAsync(config);

        // The second acquire finds the pool at capacity and times out instead of hanging forever.
        var ex = await Assert.ThrowsAsync<AdbcException>(() => pool.AcquireConnectionAsync(config));
        Assert.Contains("capacity", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AcquireConnection_WaiterIsWokenByReleaseToIdle()
    {
        // Pool of 1: a second acquire must block, then complete promptly (not after AcquireTimeout)
        // when the first connection is released to idle — and it must reuse that connection.
        var authService = SubstituteAuthService();
        using var pool = new ConnectionPoolManager(authService);
        var config = new ConnectionConfig
        {
            Account = "test",
            User = "user",
            PoolConfig = new ConnectionPoolConfig { MaxPoolSize = 1, AcquireTimeout = TimeSpan.FromSeconds(30) },
        };

        var first = await pool.AcquireConnectionAsync(config);
        Task<IPooledConnection> waiter = pool.AcquireConnectionAsync(config);
        await Task.Delay(200);
        Assert.False(waiter.IsCompleted, "waiter should be blocked while the pool is at capacity");

        pool.ReleaseConnection(first);

        // Well under the 30s AcquireTimeout, so completion proves the release woke the waiter.
        var second = await waiter.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(first.ConnectionId, second.ConnectionId);
    }

    [Fact]
    public async Task AcquireConnection_WaiterIsWokenByReleaseOfFaultedConnection()
    {
        // A discarded (faulted) connection must also free capacity for a blocked waiter, which
        // then creates a fresh connection.
        var authService = SubstituteAuthService();
        using var pool = new ConnectionPoolManager(authService);
        var config = new ConnectionConfig
        {
            Account = "test",
            User = "user",
            PoolConfig = new ConnectionPoolConfig { MaxPoolSize = 1, AcquireTimeout = TimeSpan.FromSeconds(30) },
        };

        var first = await pool.AcquireConnectionAsync(config);
        Task<IPooledConnection> waiter = pool.AcquireConnectionAsync(config);
        await Task.Delay(200);
        Assert.False(waiter.IsCompleted, "waiter should be blocked while the pool is at capacity");

        first.IsFaulted = true;
        pool.ReleaseConnection(first);

        var second = await waiter.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(first.IsDisposed);
        Assert.NotEqual(first.ConnectionId, second.ConnectionId);
    }

    [Fact]
    public async Task ReleaseConnection_Twice_ReturnsOnlyOnePermit()
    {
        // Double release must be a no-op (the connection is no longer active), not a second permit:
        // with a pool of 1, two subsequent held acquires would otherwise both succeed.
        var authService = SubstituteAuthService();
        using var pool = new ConnectionPoolManager(authService);
        var config = new ConnectionConfig
        {
            Account = "test",
            User = "user",
            PoolConfig = new ConnectionPoolConfig { MaxPoolSize = 1, AcquireTimeout = TimeSpan.FromMilliseconds(200) },
        };

        var connection = await pool.AcquireConnectionAsync(config);
        pool.ReleaseConnection(connection);
        pool.ReleaseConnection(connection);

        // One permit available: the first acquire succeeds and holds it; the second must time out.
        _ = await pool.AcquireConnectionAsync(config);
        await Assert.ThrowsAsync<AdbcException>(() => pool.AcquireConnectionAsync(config));
    }

    private static IAuthenticationService SubstituteAuthService()
    {
        var authService = Substitute.For<IAuthenticationService>();
        authService.AuthenticateAsync(Arg.Any<ConnectionConfig>(), Arg.Any<CancellationToken>())
            .Returns(_ => new AuthenticationToken
            {
                SessionToken = "session",
                MasterToken = "master",
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(1),
                MasterExpiresAt = DateTimeOffset.UtcNow.AddHours(4),
            });
        return authService;
    }

    [Fact]
    public async Task ReleaseConnection_WhenFaulted_DiscardsInsteadOfPooling()
    {
        var authService = Substitute.For<IAuthenticationService>();
        authService.AuthenticateAsync(Arg.Any<ConnectionConfig>(), Arg.Any<CancellationToken>())
            .Returns(_ => new AuthenticationToken
            {
                SessionToken = "session",
                MasterToken = "master",
                ExpiresAt = DateTimeOffset.UtcNow.AddHours(1),
                MasterExpiresAt = DateTimeOffset.UtcNow.AddHours(4),
            });

        using var pool = new ConnectionPoolManager(authService);
        var config = new ConnectionConfig { Account = "test", User = "user" };

        // A connection marked faulted (e.g. the executor hit a transport failure mid-query) must be
        // disposed on release, and the next acquire must get a fresh connection, not the faulted one.
        var faulted = await pool.AcquireConnectionAsync(config);
        faulted.IsFaulted = true;
        pool.ReleaseConnection(faulted);

        var next = await pool.AcquireConnectionAsync(config);

        Assert.True(faulted.IsDisposed);
        Assert.NotEqual(faulted.ConnectionId, next.ConnectionId);
    }

    // ---- GeneratePoolKey ----

    private static ConnectionConfig BasicConfig() => new()
    {
        Account = "acct",
        User = "user",
        Database = "db",
        Schema = "sch",
        Warehouse = "wh",
        Role = "role",
        Authentication = new AuthenticationConfig { Type = AuthenticationType.UsernamePassword, Password = "secret" },
    };

    [Fact]
    public void GeneratePoolKey_IdenticalConfigs_ProduceTheSameKey()
    {
        Assert.Equal(
            ConnectionPoolManager.GeneratePoolKey(BasicConfig()),
            ConnectionPoolManager.GeneratePoolKey(BasicConfig()));
    }

    [Fact]
    public void GeneratePoolKey_DifferentPassword_ProducesDifferentKeys()
    {
        // Same user, different password → must not share a session
        var a = BasicConfig();
        var b = BasicConfig();
        b.Authentication = new AuthenticationConfig { Type = AuthenticationType.UsernamePassword, Password = "other" };

        Assert.NotEqual(ConnectionPoolManager.GeneratePoolKey(a), ConnectionPoolManager.GeneratePoolKey(b));
    }

    [Fact]
    public void GeneratePoolKey_DifferentOAuthTokenWithEmptyUser_ProducesDifferentKeys()
    {
        // OAuth derives the user from the token, so User is blank; keying on User alone would let two
        // different tokens share one pooled session. The credential fingerprint must separate them.
        var a = BasicConfig();
        a.User = string.Empty;
        a.Authentication = new AuthenticationConfig { Type = AuthenticationType.OAuth, Token = "token-A" };
        var b = BasicConfig();
        b.User = string.Empty;
        b.Authentication = new AuthenticationConfig { Type = AuthenticationType.OAuth, Token = "token-B" };

        Assert.NotEqual(ConnectionPoolManager.GeneratePoolKey(a), ConnectionPoolManager.GeneratePoolKey(b));
    }

    [Fact]
    public void GeneratePoolKey_DifferentHost_ProducesDifferentKeys()
    {
        var a = BasicConfig();
        var b = BasicConfig();
        b.Network = new NetworkConfig { Host = "other.snowflakecomputing.com" };

        Assert.NotEqual(ConnectionPoolManager.GeneratePoolKey(a), ConnectionPoolManager.GeneratePoolKey(b));
    }

    [Fact]
    public void GeneratePoolKey_KeepAliveDifferenceOnly_ProducesTheSameKey()
    {
        // Keep-alive is client-side only (not sent at login), so it must not fragment the pool
        var a = BasicConfig();
        var b = BasicConfig();
        b.ClientSessionKeepAlive = true;
        b.HeartbeatFrequency = TimeSpan.FromMinutes(15);

        Assert.Equal(ConnectionPoolManager.GeneratePoolKey(a), ConnectionPoolManager.GeneratePoolKey(b));
    }

    [Fact]
    public void GeneratePoolKey_DoesNotContainTheRawSecret()
    {
        // The password is hashed into the key, never embedded verbatim
        var config = BasicConfig();
        config.Authentication = new AuthenticationConfig { Type = AuthenticationType.UsernamePassword, Password = "hunter2" };

        Assert.DoesNotContain("hunter2", ConnectionPoolManager.GeneratePoolKey(config));
    }
}
