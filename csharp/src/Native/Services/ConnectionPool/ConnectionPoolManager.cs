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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native.Configuration;
using AdbcDrivers.Snowflake.Native.Services.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Apache.Arrow.Adbc;

namespace AdbcDrivers.Snowflake.Native.Services.ConnectionPool;

/// <summary>
/// Implements connection pooling for Snowflake connections.
/// </summary>
internal class ConnectionPoolManager : IConnectionPoolManager
{
    private readonly IAuthenticationService _authService;
    private readonly ISessionLifecycle? _sessionLifecycle;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<ConnectionPoolManager> _logger;
    private readonly ConcurrentDictionary<string, ConnectionPoolEntry> _pools;
    // Signals the background maintenance loop (idle eviction + keep-alive heartbeats) to stop; the
    // Cancel() lives in Dispose(), and the CTS itself is disposed only after the loop has exited.
    private readonly CancellationTokenSource _cleanupCts = new();
    private readonly object _cleanupStartLock = new();
    private Task? _cleanupTask;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="ConnectionPoolManager"/> class.
    /// </summary>
    /// <param name="authService">The authentication service.</param>
    /// <param name="sessionLifecycle">
    /// Optional collaborator for server-side session upkeep: it heartbeats idle keep-alive connections
    /// (so long-idle pooled sessions don't lapse to master-token expiry) and closes a connection's
    /// session when the pool discards it (so sessions aren't orphaned until they time out).
    /// </param>
    /// <param name="timeProvider">
    /// Clock used for pool timekeeping (idle/lifetime checks, heartbeat scheduling, the background
    /// timer). Defaults to <see cref="TimeProvider.System"/>; tests inject a fake to drive the loop
    /// deterministically.
    /// </param>
    /// <param name="logger">Logger for pool maintenance events (heartbeat/cleanup failures).</param>
    public ConnectionPoolManager(
        IAuthenticationService authService,
        ISessionLifecycle? sessionLifecycle = null,
        TimeProvider? timeProvider = null,
        ILogger<ConnectionPoolManager>? logger = null)
    {
        _authService = authService ?? throw new ArgumentNullException(nameof(authService));
        _sessionLifecycle = sessionLifecycle;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger<ConnectionPoolManager>.Instance;
        _pools = new ConcurrentDictionary<string, ConnectionPoolEntry>();
    }

    /// <inheritdoc/>
    public async Task<IPooledConnection> AcquireConnectionAsync(
        ConnectionConfig config,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(config);

        EnsureCleanupStarted();

        var poolKey = GeneratePoolKey(config);
        var poolEntry = _pools.GetOrAdd(poolKey, static (_, cfg) => new ConnectionPoolEntry(cfg), config);
        
        await WaitForCapacityAsync(poolEntry, config, cancellationToken).ConfigureAwait(false);

        try
        {
            if (TryAcquireIdleConnection(poolEntry, out var idleConnection))
                return idleConnection!;

            var newConnection = await CreateConnectionAsync(poolKey, config, cancellationToken).ConfigureAwait(false);
            poolEntry.ActiveConnections.TryAdd(newConnection.ConnectionId, newConnection);
            return newConnection;
        }
        catch
        {
            poolEntry.CapacitySemaphore.Release();
            throw;
        }
    }

    /// <summary>
    /// Waits (bounded by <see cref="ConnectionPoolConfig.AcquireTimeout"/>) for a capacity permit on
    /// the pool entry. On return the caller
    /// holds one permit — the right to one active connection — which is returned by
    /// <see cref="ReleaseConnection"/> (or by the acquire failure path). Throws
    /// <see cref="AdbcException"/> if the pool stays at capacity past the timeout; a cancelled token
    /// propagates as <see cref="OperationCanceledException"/>. Neither a timeout nor a cancellation
    /// consumes a permit.
    /// </summary>
    private static async Task WaitForCapacityAsync(
        ConnectionPoolEntry poolEntry, ConnectionConfig config, CancellationToken cancellationToken)
    {
        bool entered = await poolEntry.CapacitySemaphore
            .WaitAsync(config.PoolConfig.AcquireTimeout, cancellationToken).ConfigureAwait(false);

        if (!entered)
            throw new AdbcException(
                $"Timed out after {config.PoolConfig.AcquireTimeout.TotalSeconds:0}s waiting for an available " +
                $"connection; the pool is at capacity (max {config.PoolConfig.MaxPoolSize}).");
    }

    // Takes the evaluation instant as a parameter (rather than reading _timeProvider itself) so the
    // clock is never touched while a caller holds poolEntry.IdleLock — no external call under a lock,
    // and every connection in one operation is judged against the same instant.
    private static bool IsConnectionValid(IPooledConnection connection, DateTimeOffset now)
    {
        return connection is { IsDisposed: false, IsFaulted: false, IsTokenExpired: false } &&
               (now - connection.CreatedAt) <= connection.Config.PoolConfig.MaxConnectionLifetime;
    }

    /// <summary>
    /// Seats the caller — who must already hold a capacity permit — on a valid idle connection if
    /// one exists. Idle connections hold no permit, so the stale ones discarded along the way
    /// involve no permit accounting.
    /// </summary>
    private bool TryAcquireIdleConnection(ConnectionPoolEntry poolEntry, out IPooledConnection? idleConnection)
    {
        var now = _timeProvider.GetUtcNow();
        List<IPooledConnection>? stale = null;
        idleConnection = null;

        lock (poolEntry.IdleLock)
        {
            while (poolEntry.IdleConnections.TryPop(out var connection))
            {
                if (IsConnectionValid(connection, now))
                {
                    connection.UpdateLastUsedAt();
                    poolEntry.ActiveConnections.TryAdd(connection.ConnectionId, connection);
                    idleConnection = connection;
                    break;
                }

                (stale ??= []).Add(connection);
            }
        }

        // Dispose OUTSIDE the lock: Dispose best-effort closes the server-side session — a bounded
        // network wait — and holding IdleLock across it would stall every other acquire/release on
        // this pool entry for up to 5s per stale connection.
        if (stale != null)
        {
            foreach (var connection in stale)
                connection.Dispose();
        }

        return idleConnection != null;
    }

    public void ReleaseConnection(IPooledConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);

        if (!_pools.TryGetValue(connection.PoolKey, out var poolEntry))
            return;

        // The permit travels with the active connection; a connection that isn't active (double
        // release, or already discarded) has no permit to return.
        if (!poolEntry.ActiveConnections.TryRemove(connection.ConnectionId, out _))
            return;

        if (IsConnectionValid(connection, _timeProvider.GetUtcNow()))
            poolEntry.IdleConnections.Push(connection);
        else
            connection.Dispose();

        // Return the permit only after the connection is seated on the idle stack (or discarded),
        // so a waiter woken by this release always finds the capacity it was promised.
        poolEntry.CapacitySemaphore.Release();
    }

    /// <summary>
    /// Disposes the connection pool and all pooled connections.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        _cleanupCts.Cancel();

        if (_cleanupTask != null)
        {
            try
            {
                _cleanupTask.Wait(TimeSpan.FromSeconds(5));
            }
            catch (AggregateException)
            {
                // The cleanup loop observed cancellation while shutting down — expected.
            }
        }

        // Dispose the CTS only after the cleanup loop has stopped using its token.
        _cleanupCts.Dispose();

        foreach (var poolEntry in _pools.Values)
        {
            foreach (var connection in poolEntry.ActiveConnections.Values)
                connection.Dispose();
            foreach (var connection in poolEntry.IdleConnections)
                connection.Dispose();

            poolEntry.CapacitySemaphore.Dispose();
        }

        _pools.Clear();
    }

    private void EnsureCleanupStarted()
    {
        if (_cleanupTask != null) return;
        lock (_cleanupStartLock)
        {
            _cleanupTask ??= CleanupLoopAsync();
        }
    }

    private async Task CleanupLoopAsync()
    {
        var timer = new PeriodicTimer(TimeSpan.FromSeconds(60), _timeProvider);
        CancellationToken token = _cleanupCts.Token;
        try
        {
            while (await timer.WaitForNextTickAsync(token).ConfigureAwait(false))
            {
                try
                {
                    Cleanup();
                    await HeartbeatIdleConnectionsAsync(token).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    // Swallow to keep the maintenance loop alive.
                    _logger.LogWarning(ex, "Pool maintenance pass failed; will retry on the next tick.");
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            timer.Dispose();
        }
    }

    private void Cleanup()
    {
        if (_disposed)
            return;

        foreach (var poolEntry in _pools.Values)
        {
            var now = _timeProvider.GetUtcNow();
            var connectionsToKeep = new List<IPooledConnection>();
            var connectionsToRemove = new List<IPooledConnection>();
            lock (poolEntry.IdleLock)
            {
                while (poolEntry.IdleConnections.TryPop(out var connection))
                {
                    var idleTime = now - connection.LastUsedAt;
                    if (!IsConnectionValid(connection, now) ||
                        idleTime > poolEntry.Config.PoolConfig.IdleTimeout)
                        connectionsToRemove.Add(connection);
                    else
                        connectionsToKeep.Add(connection);
                }

                foreach (IPooledConnection pooledConnection in connectionsToKeep)
                {
                    poolEntry.IdleConnections.Push(pooledConnection);
                }
            }
            
            foreach (var connection in connectionsToRemove)
                connection.Dispose();
        }
    }

    /// <summary>
    /// True when a connection has keep-alive enabled and has had no query or heartbeat for at least
    /// its configured heartbeat frequency.
    /// </summary>
    internal static bool IsHeartbeatDue(IPooledConnection connection, DateTimeOffset now)
    {
        if (!connection.Config.ClientSessionKeepAlive)
            return false;

        var lastActivity = connection.LastUsedAt > connection.LastHeartbeatAt
            ? connection.LastUsedAt
            : connection.LastHeartbeatAt;
        return now - lastActivity >= connection.Config.HeartbeatFrequency;
    }

    /// <summary>
    /// Pings the keep-alive endpoint for every idle connection that is due. Idle-only by design:
    /// idle connections have no in-flight query, so a heartbeat can't race the reactive token renewal
    /// a running query may trigger.
    /// </summary>
    private Task HeartbeatIdleConnectionsAsync(CancellationToken token)
    {
        if (_sessionLifecycle == null)
            return Task.CompletedTask;

        // ConcurrentStack.ToArray is a lock-free snapshot; a connection acquired between here and the
        // heartbeat will have a fresh LastUsedAt and so won't be due.
        var idle = new List<IPooledConnection>();
        foreach (var poolEntry in _pools.Values)
            idle.AddRange(poolEntry.IdleConnections.ToArray());

        return HeartbeatDueConnectionsAsync(idle, _sessionLifecycle.HeartbeatAsync, _timeProvider.GetUtcNow(), token, _logger);
    }

    /// <summary>
    /// Heartbeats each connection that is due (see <see cref="IsHeartbeatDue"/>), recording the
    /// heartbeat on success. Pure over the supplied connections and clock so the scheduling can be
    /// tested without the background timer. Best-effort: a failing heartbeat is swallowed so one bad
    /// connection doesn't stop the rest — it is recovered by reactive renewal on the next query.
    /// </summary>
    internal static async Task HeartbeatDueConnectionsAsync(
        IEnumerable<IPooledConnection> connections,
        Func<AuthenticationToken, ConnectionConfig, CancellationToken, Task> heartbeat,
        DateTimeOffset now,
        CancellationToken token,
        ILogger? logger = null)
    {
        foreach (var connection in connections)
        {
            if (token.IsCancellationRequested)
                return;
            if (connection.IsDisposed || !IsHeartbeatDue(connection, now))
                continue;

            try
            {
                await heartbeat(connection.AuthToken, connection.Config, token).ConfigureAwait(false);
                connection.RecordHeartbeat();
            }
            catch (Exception ex)
            {
                // Best-effort keep-alive; a transient failure is recovered by reactive renewal later.
                logger?.LogWarning(ex, "Keep-alive heartbeat failed for pooled connection {ConnectionId}.",
                    connection.ConnectionId);
            }
        }
    }

    private async Task<IPooledConnection> CreateConnectionAsync(
        string poolKey,
        ConnectionConfig config,
        CancellationToken cancellationToken)
    {
        // LoginTimeout bounds the network round trips; an external-browser login also has a
        // human in the loop, so the interactive step gets the SSO browser allowance on top —
        // otherwise the default 60s login timeout would fire before the 120s browser timeout
        // ever could.
        TimeSpan loginTimeout = config.Authentication.Type is AuthenticationType.Sso or AuthenticationType.ExternalBrowser
            ? config.LoginTimeout + SsoAuthenticator.DefaultBrowserTimeout
            : config.LoginTimeout;

        using var loginCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        loginCts.CancelAfter(loginTimeout);

        AuthenticationToken authToken;
        try
        {
            authToken = await _authService.AuthenticateAsync(config, loginCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new AdbcException($"Login timed out after {loginTimeout.TotalSeconds:0}s.");
        }

        return new PooledConnection(
            Guid.NewGuid().ToString(),
            poolKey,
            authToken,
            config,
            _sessionLifecycle,
            _timeProvider);
    }

    /// <summary>
    /// Builds the key that decides which connections may be pooled together. Connections share a key
    /// only when reusing one for the other is safe — so the key spans everything that determines the
    /// authenticated session's identity: the account/user/db/schema/warehouse/role, the auth type and
    /// a fingerprint of its secret, and the network endpoint. It deliberately excludes client-side-only
    /// settings (keep-alive/heartbeat cadence, query timeout, compression, pool sizing) that don't
    /// change the server session, so they don't needlessly fragment the pool.
    /// </summary>
    internal static string GeneratePoolKey(ConnectionConfig config)
    {
        var auth = config.Authentication;
        var network = config.Network;
        return string.Join('|',
            config.Account, config.User, config.Database, config.Schema, config.Warehouse, config.Role,
            auth.Type, HashSecret(CredentialSecret(auth)),
            network.Host, network.Port, network.Protocol, network.NoProxy, network.TlsSkipVerify);
    }

    /// <summary>
    /// The secret that distinguishes one credential from another for the current auth type, or null
    /// when the type carries no static secret (SSO / external browser).
    /// </summary>
    private static string? CredentialSecret(AuthenticationConfig auth) => auth.Type switch
    {
        AuthenticationType.UsernamePassword => auth.Password,
        AuthenticationType.OAuth or AuthenticationType.Pat => auth.Token,
        AuthenticationType.KeyPair => $"{auth.PrivateKey ?? auth.PrivateKeyPath} {auth.PrivateKeyPassphrase}",
        _ => null,
    };

    /// <summary>
    /// Hashes a credential into a fingerprint so different secrets land in different pools without the
    /// raw secret ever appearing in the key string (which could otherwise surface in logs or dumps).
    /// </summary>
    private static string HashSecret(string? secret) =>
        string.IsNullOrEmpty(secret)
            ? string.Empty
            : Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(secret)));

}
