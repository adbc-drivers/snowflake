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
using AdbcDrivers.Snowflake.Native.Configuration;
using AdbcDrivers.Snowflake.Native.Services.Authentication;

namespace AdbcDrivers.Snowflake.Native.Services.ConnectionPool;

/// <summary>
/// Represents a pooled Snowflake connection.
/// </summary>
internal class PooledConnection : IPooledConnection
{
    private DateTimeOffset _lastUsedAt;
    private DateTimeOffset _lastHeartbeatAt;
    private bool _disposed;
    private readonly ISessionLifecycle? _sessionLifecycle;
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="PooledConnection"/> class.
    /// </summary>
    /// <param name="connectionId">The connection ID.</param>
    /// <param name="poolKey">the unique identfier for the connection pool</param>
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

    public bool IsDisposed => _disposed;

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

    /// <summary>
    /// Disposes the connection.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        // Best-effort: close the server-side session so it isn't orphaned until it times out.
        // Bounded so a slow/hung close can't stall teardown; a failure just lets the session expire.
        if (_sessionLifecycle != null)
        {
            try
            {
                _sessionLifecycle.CloseAsync(AuthToken, Config, CancellationToken.None).Wait(TimeSpan.FromSeconds(5));
            }
            catch
            {
                // ignore — closing the session is best-effort
            }
        }

        GC.SuppressFinalize(this);
    }
}
