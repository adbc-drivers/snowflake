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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Configuration;
using AdbcDrivers.Snowflake.Services.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Apache.Arrow.Adbc;

namespace AdbcDrivers.Snowflake.Services.ConnectionPool;

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
    // Cancelled when disposal begins. Stops the background maintenance loop (idle eviction +
    // keep-alive heartbeats) and wakes acquires waiting for capacity. Deliberately never disposed:
    // a waiter may still be linking to its token as disposal runs, and a CTS without a timer holds
    // nothing that disposal would release.
    private readonly CancellationTokenSource _shutdownCts = new();
    private Task? _cleanupTask;

    // Serializes every change to which connections the pool holds (seating a new connection, taking
    // an idle one, returning one to idle, the maintenance sweep) with teardown's one-time detach, so
    // a connection is always either in the snapshot teardown disposes or refused and disposed by the
    // caller. Held only for in-memory bookkeeping: no I/O and no connection disposal under it.
    private readonly object _lifecycleLock = new();
    private int _disposed; // 0 = live, 1 = disposal started; written under _lifecycleLock

    /// <summary>
    /// Separates the fields of a composite credential before it is hashed, so that different
    /// field boundaries cannot produce the same fingerprint (key "AB" + passphrase "C" must not
    /// collide with key "A" + passphrase "BC"). NUL is used because it cannot occur in a PEM or
    /// a passphrase, unlike any printable character.
    /// </summary>
    private const char CredentialFieldDelimiter = '\0';

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

        var poolKey = GeneratePoolKey(config);
        ConnectionPoolEntry poolEntry;

        // Atomic with teardown, so an acquire can't add an entry to a pool that was just cleared.
        lock (_lifecycleLock)
        {
            ObjectDisposedException.ThrowIf(IsDisposed, this);
            poolEntry = _pools.GetOrAdd(poolKey, static (_, cfg) => new ConnectionPoolEntry(cfg), config);
        }

        EnsureCleanupStarted();

        await WaitForCapacityAsync(poolEntry, config, cancellationToken).ConfigureAwait(false);

        try
        {
            if (TryAcquireIdleConnection(poolEntry, out var idleConnection))
                return idleConnection!;

            var newConnection = await CreateConnectionAsync(poolKey, config, cancellationToken).ConfigureAwait(false);

            // The login is a long await: the pool may have been disposed meanwhile, in which case
            // teardown's snapshot is already taken and this connection is ours to close.
            bool seated;
            lock (_lifecycleLock)
            {
                seated = !IsDisposed;
                if (seated)
                    poolEntry.ActiveConnections.TryAdd(newConnection.ConnectionId, newConnection);
            }

            if (!seated)
            {
                await newConnection.DisposeAsync().ConfigureAwait(false);
                throw new ObjectDisposedException(GetType().FullName);
            }

            return newConnection;
        }
        catch
        {
            ReturnPermit(poolEntry);
            throw;
        }
    }

    /// <summary>
    /// Waits (bounded by <see cref="ConnectionPoolConfig.AcquireTimeout"/>) for a capacity permit on
    /// the pool entry. On return the caller
    /// holds one permit — the right to one active connection — which is returned by
    /// <see cref="ReleaseConnection"/> (or by the acquire failure path). Throws
    /// <see cref="AdbcException"/> if the pool stays at capacity past the timeout; a cancelled token
    /// propagates as <see cref="OperationCanceledException"/>, and disposing the pool ends the wait
    /// with <see cref="ObjectDisposedException"/>. None of these consumes a permit.
    /// </summary>
    private async Task WaitForCapacityAsync(
        ConnectionPoolEntry poolEntry, ConnectionConfig config, CancellationToken cancellationToken)
    {
        // Disposing a SemaphoreSlim doesn't wake its waiters, so the wait also observes the pool's
        // shutdown token, which teardown cancels before it disposes the semaphores.
        using var wait = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownCts.Token);
        bool entered;
        try
        {
            entered = await poolEntry.CapacitySemaphore
                .WaitAsync(config.PoolConfig.AcquireTimeout, wait.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new ObjectDisposedException(GetType().FullName);
        }

        if (!entered)
            throw new AdbcException(
                $"Timed out after {config.PoolConfig.AcquireTimeout.TotalSeconds:0}s waiting for an available " +
                $"connection; the pool is at capacity (max {config.PoolConfig.MaxPoolSize}).",
                AdbcStatusCode.Timeout);
    }

    // Takes the evaluation instant as a parameter (rather than reading _timeProvider itself) so
    // every connection in one operation is judged against the same instant.
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

        // Pop and seat under the lock, so teardown can't snapshot while a connection is in neither
        // the idle stack nor the active set.
        lock (_lifecycleLock)
        {
            ObjectDisposedException.ThrowIf(IsDisposed, this);

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

        // Dispose after the drain rather than inside it: Dispose best-effort closes the server-side
        // session, a bounded network wait, and doing that mid-drain would hold up this acquire for
        // up to 5s per stale connection.
        if (stale != null)
        {
            foreach (var connection in stale)
                connection.Dispose();
        }

        return idleConnection != null;
    }

    public void ReleaseConnection(IPooledConnection connection)
    {
        var discarded = Release(connection, out var poolEntry);
        try
        {
            discarded?.Dispose();
        }
        finally
        {
            ReturnPermit(poolEntry);
        }
    }

    public async ValueTask ReleaseConnectionAsync(IPooledConnection connection)
    {
        var discarded = Release(connection, out var poolEntry);
        try
        {
            if (discarded != null)
                await discarded.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            ReturnPermit(poolEntry);
        }
    }

    /// <summary>
    /// Moves an active connection back to the idle stack, or returns it to be discarded when it is
    /// no longer valid. <paramref name="poolEntry"/> is set only when the connection held a permit
    /// that <see cref="ReturnPermit"/> must return once any discard has finished.
    /// </summary>
    private IPooledConnection? Release(IPooledConnection connection, out ConnectionPoolEntry? poolEntry)
    {
        ArgumentNullException.ThrowIfNull(connection);
        poolEntry = null;

        // Atomic with teardown: once the pool is detached the entry is gone and the connection was
        // in teardown's snapshot, so there is nothing to move and no permit to return.
        lock (_lifecycleLock)
        {
            if (!_pools.TryGetValue(connection.PoolKey, out var entry))
                return null;

            // The permit travels with the active connection; a connection that isn't active (double
            // release, or already discarded) has no permit to return.
            if (!entry.ActiveConnections.TryRemove(connection.ConnectionId, out _))
                return null;

            poolEntry = entry;
            if (IsConnectionValid(connection, _timeProvider.GetUtcNow()))
            {
                entry.IdleConnections.Push(connection);
                return null;
            }

            return connection;
        }
    }

    // Return the permit only after the connection is seated on the idle stack (or discarded),
    // so a waiter woken by this release always finds the capacity it was promised.
    // The semaphores are never disposed (see CompleteDispose), so this is safe after teardown too.
    private static void ReturnPermit(ConnectionPoolEntry? poolEntry) => poolEntry?.CapacitySemaphore.Release();

    /// <summary>
    /// Disposes the connection pool and all pooled connections.
    /// </summary>
    public void Dispose()
    {
        if (!TryBeginDispose())
            return;

        // Every step of the loop's shutdown is bounded (see CleanupLoopAsync), so this wait is too.
        try { _cleanupTask?.Wait(); } catch (AggregateException) { }

        foreach (var connection in CompleteDispose())
            connection.Dispose();
    }

    /// <summary>
    /// Asynchronously disposes the connection pool and all pooled connections. The pooled
    /// connections' server-side sessions are closed concurrently rather than one after another.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (!TryBeginDispose())
            return;

        if (_cleanupTask != null)
        {
            try { await _cleanupTask.ConfigureAwait(false); } catch (Exception) { }
        }

        await Task.WhenAll(CompleteDispose().Select(c => c.DisposeAsync().AsTask())).ConfigureAwait(false);
    }

    private bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    /// <summary>
    /// Claims disposal and stops the maintenance loop. Under the lifecycle lock, so no acquire can
    /// start the loop (or seat a connection) after this returns.
    /// </summary>
    private bool TryBeginDispose()
    {
        lock (_lifecycleLock)
        {
            if (IsDisposed)
                return false;

            Volatile.Write(ref _disposed, 1);
        }

        _shutdownCts.Cancel();
        return true;
    }

    /// <summary>
    /// Runs once the maintenance loop has exited: detaches every pooled connection atomically with
    /// respect to acquire and release, and returns them for the caller to dispose (outside the lock).
    /// </summary>
    /// <remarks>
    /// The capacity semaphores are deliberately not disposed. SemaphoreSlim.Dispose drops its list
    /// of async waiters, so a capacity wait whose cancellation (by the shutdown token) is still
    /// completing can no longer remove itself and would hang forever. The pool never touches
    /// AvailableWaitHandle, so there is nothing for Dispose to release, and a late release returns
    /// its permit harmlessly.
    /// </remarks>
    private List<IPooledConnection> CompleteDispose()
    {
        var connections = new List<IPooledConnection>();
        lock (_lifecycleLock)
        {
            foreach (var poolEntry in _pools.Values)
            {
                connections.AddRange(poolEntry.ActiveConnections.Values);
                connections.AddRange(poolEntry.IdleConnections);
            }

            _pools.Clear();
        }

        return connections;
    }

    private void EnsureCleanupStarted()
    {
        if (_cleanupTask != null) return;
        lock (_lifecycleLock)
        {
            // Never start the loop once disposal has claimed it: teardown would not wait for it.
            if (IsDisposed || _cleanupTask != null)
                return;

            // The timer is created here, so the schedule starts from this acquire; the loop itself
            // runs via Task.Run so none of its body executes on this thread while the lock is held.
            var timer = new PeriodicTimer(TimeSpan.FromSeconds(60), _timeProvider);
            _cleanupTask = Task.Run(() => CleanupLoopAsync(timer));
        }
    }

    private async Task CleanupLoopAsync(PeriodicTimer timer)
    {
        CancellationToken token = _shutdownCts.Token;
        try
        {
            // Shutdown is bounded: the timer observes the token, an in-flight heartbeat gets at most
            // HeartbeatShutdownTimeout to finish, and the sweep's session closes run concurrently,
            // each bounded by PooledConnection.SessionCloseTimeout. Teardown relies on this to wait
            // for the loop without a timeout.
            while (await timer.WaitForNextTickAsync(token).ConfigureAwait(false))
            {
                try
                {
                    await CleanupAsync().ConfigureAwait(false);
                    await HeartbeatIdleConnectionsAsync(token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;
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

    private Task CleanupAsync()
    {
        var connectionsToRemove = new List<IPooledConnection>();

        // The pop-and-repush runs under the lifecycle lock so teardown can't snapshot the idle stack
        // while it is drained. It is in-memory and at most MaxPoolSize per entry, so acquires wait
        // for it only briefly.
        lock (_lifecycleLock)
        {
            if (IsDisposed)
                return Task.CompletedTask;

            foreach (var poolEntry in _pools.Values)
            {
                var now = _timeProvider.GetUtcNow();
                var connectionsToKeep = new List<IPooledConnection>();

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
                    poolEntry.IdleConnections.Push(pooledConnection);
            }
        }

        // The evicted connections are out of the pool and owned by this sweep alone; close their
        // sessions concurrently, outside the lock.
        return Task.WhenAll(connectionsToRemove.Select(c => c.DisposeAsync().AsTask()));
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

        return HeartbeatDueConnectionsAsync(idle, _sessionLifecycle.HeartbeatAsync, _timeProvider.GetUtcNow(), token, _logger, _timeProvider);
    }

    /// <summary>
    /// Heartbeats each connection that is due (see <see cref="IsHeartbeatDue"/>), recording the
    /// heartbeat on success. Pure over the supplied connections and clock so the scheduling can be
    /// tested without the background timer. Best-effort: a failing heartbeat is swallowed so one bad
    /// connection doesn't stop the rest — it is recovered by reactive renewal on the next query.
    /// </summary>
    // How long shutdown waits for a heartbeat that is still in flight; heartbeats honour their token,
    // so in practice it returns almost at once.
    internal static readonly TimeSpan HeartbeatShutdownTimeout = TimeSpan.FromSeconds(5);

    internal static async Task HeartbeatDueConnectionsAsync(
        IEnumerable<IPooledConnection> connections,
        Func<AuthenticationToken, ConnectionConfig, CancellationToken, Task> heartbeat,
        DateTimeOffset now,
        CancellationToken token,
        ILogger? logger = null,
        TimeProvider? timeProvider = null)
    {
        foreach (var connection in connections)
        {
            if (token.IsCancellationRequested)
                return;
            if (connection.IsDisposed || !IsHeartbeatDue(connection, now))
                continue;

            Task inFlight = heartbeat(connection.AuthToken, connection.Config, token);
            try
            {
                await inFlight.WaitAsync(token).ConfigureAwait(false);
                connection.RecordHeartbeat();
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // Shutting down. Teardown is about to close this session and the shared HttpClient,
                // so give the heartbeat a bounded chance to observe its token and finish first, and
                // observe its outcome so a late failure isn't left unobserved.
                try
                {
                    await inFlight.WaitAsync(HeartbeatShutdownTimeout, timeProvider ?? TimeProvider.System).ConfigureAwait(false);
                }
                catch
                {
                    // ignore — the pool is shutting down
                }
                return;
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
            throw new AdbcException(
                $"Login timed out after {loginTimeout.TotalSeconds:0}s.", AdbcStatusCode.Timeout);
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
        AuthenticationType.KeyPair =>
            $"{auth.PrivateKey ?? auth.PrivateKeyPath}{CredentialFieldDelimiter}{auth.PrivateKeyPassphrase}",
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
