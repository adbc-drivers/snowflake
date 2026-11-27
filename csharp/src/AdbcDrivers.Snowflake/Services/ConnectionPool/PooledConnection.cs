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

namespace AdbcDrivers.Snowflake.Services.ConnectionPool;

/// <summary>
/// Represents a pooled Snowflake connection.
/// </summary>
internal class PooledConnection : IPooledConnection
{
    private DateTimeOffset _lastUsedAt;
    private DateTimeOffset _lastHeartbeatAt;
    private int _disposed; // 0 = live, 1 = disposed (set atomically, so the session is closed once)
    private readonly ISessionLifecycle? _sessionLifecycle;
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="PooledConnection"/> class.
    /// </summary>
    /// <param name="connectionId">The connection ID.</param>
    /// <param name="poolKey">the unique identifier for the connection pool</param>
    /// <param name="authToken">The authentication token.</param>
    /// <param name="config">The connection configuration.</param>
    /// <param name="sessionLifecycle">
    /// Optional session-lifecycle collaborator; its <see cref="ISessionLifecycle.CloseAsync"/> is
    /// invoked when the pool discards this connection (not when it is returned to the idle pool for
    /// reuse), so the server-side session isn't orphaned.
    /// </param>
    /// <param name="timeProvider">Clock for the connection's timestamps. Defaults to <see cref="TimeProvider.System"/>.</param>
    public PooledConnection(
        string connectionId,
        string poolKey,
        AuthenticationToken authToken,
        ConnectionConfig config,
        ISessionLifecycle? sessionLifecycle = null,
        TimeProvider? timeProvider = null)
    {
        ConnectionId = connectionId ?? throw new ArgumentNullException(nameof(connectionId));
        PoolKey = poolKey ?? throw new ArgumentNullException(nameof(poolKey));
        AuthToken = authToken ?? throw new ArgumentNullException(nameof(authToken));
        Config = config ?? throw new ArgumentNullException(nameof(config));
        _timeProvider = timeProvider ?? TimeProvider.System;
        CreatedAt = _timeProvider.GetUtcNow();
        _lastUsedAt = CreatedAt;
        _lastHeartbeatAt = CreatedAt;
        _sessionLifecycle = sessionLifecycle;
    }

    public string ConnectionId { get; }

    public string PoolKey { get; }

    public AuthenticationToken AuthToken { get; }

    public ConnectionConfig Config { get; }

    public DateTimeOffset CreatedAt { get; }

    public DateTimeOffset LastUsedAt => _lastUsedAt;

    public DateTimeOffset LastHeartbeatAt => _lastHeartbeatAt;

    public bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    // Keyed on the master-token expiry (not the ~1h session), because a session-expired connection is
    // still usable via reactive renewal until the master lapses — so the pool should only discard it
    // once it's truly beyond recovery. Evaluated on the pool's clock so all pool timekeeping shares one.
    public bool IsTokenExpired => _timeProvider.GetUtcNow() >= AuthToken.MasterExpiresAt;

    public bool IsFaulted { get; set; }

    /// <summary>
    /// Updates the last used timestamp (internal use only).
    /// </summary>
    void IPooledConnection.UpdateLastUsedAt() => _lastUsedAt = _timeProvider.GetUtcNow();

    /// <summary>
    /// Records that a keep-alive heartbeat just succeeded (internal use only).
    /// </summary>
    void IPooledConnection.RecordHeartbeat() => _lastHeartbeatAt = _timeProvider.GetUtcNow();

    // Bounds the best-effort session close so a slow or hung close can't stall teardown; a
    // failure or timeout just lets the server-side session expire on its own.
    internal static readonly TimeSpan SessionCloseTimeout = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Disposes the connection, closing the server-side session (best-effort, bounded).
    /// </summary>
    public void Dispose()
    {
        if (!TryMarkDisposed())
            return;

        // The close is bounded by SessionCloseTimeout and swallows its own failures.
        CloseSessionBestEffortAsync().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Asynchronously disposes the connection, closing the server-side session (best-effort, bounded).
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (!TryMarkDisposed())
            return;

        await CloseSessionBestEffortAsync().ConfigureAwait(false);
    }

    private bool TryMarkDisposed()
    {
        // Atomic: the pool's cleanup sweep and its own teardown can dispose the same connection
        // concurrently, and only one of them may send the session close.
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return false;

        GC.SuppressFinalize(this);
        return true;
    }

    // Best-effort: close the server-side session so it isn't orphaned until it times out.
    private async Task CloseSessionBestEffortAsync()
    {
        if (_sessionLifecycle == null)
            return;

        try
        {
            // The token asks the close to stop; WaitAsync bounds the wait even if the close
            // doesn't observe it, so disposal can never hang on an unresponsive server.
            using var timeout = new CancellationTokenSource(SessionCloseTimeout, _timeProvider);
            await _sessionLifecycle.CloseAsync(AuthToken, Config, timeout.Token)
                .WaitAsync(SessionCloseTimeout, _timeProvider).ConfigureAwait(false);
        }
        catch
        {
            // ignore — closing the session is best-effort
        }
    }
}
