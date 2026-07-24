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
using AdbcDrivers.Snowflake.Native.Configuration;
using AdbcDrivers.Snowflake.Native.Services.Authentication;

namespace AdbcDrivers.Snowflake.Native.Services.ConnectionPool;

/// <summary>
/// Represents a pooled connection.
/// </summary>
internal interface IPooledConnection : IDisposable
{
    /// <summary>
    /// Gets the connection ID.
    /// </summary>
    string ConnectionId { get; }

    /// <summary>
    /// Gets the pool key this connection was created under. Computed once at creation (it hashes
    /// the credential), so releases don't recompute it.
    /// </summary>
    string PoolKey { get; }

    /// <summary>
    /// Gets the authentication token for this connection.
    /// </summary>
    AuthenticationToken AuthToken { get; }

    /// <summary>
    /// Gets the connection configuration.
    /// </summary>
    ConnectionConfig Config { get; }

    /// <summary>
    /// Gets the time when the connection was created.
    /// </summary>
    DateTimeOffset CreatedAt { get; }

    /// <summary>
    /// Gets or sets the time when the connection was last used.
    /// </summary>
    DateTimeOffset LastUsedAt { get; }

    /// <summary>
    /// Updates the last used timestamp (internal use only).
    /// </summary>
    internal void UpdateLastUsedAt();

    /// <summary>
    /// Gets the time of the last successful keep-alive heartbeat (or creation, if none yet).
    /// </summary>
    DateTimeOffset LastHeartbeatAt { get; }

    /// <summary>
    /// Records that a keep-alive heartbeat just succeeded (internal use only).
    /// </summary>
    internal void RecordHeartbeat();

    /// <summary>
    /// Gets a value indicating whether the connection is disposed.
    /// </summary>
    bool IsDisposed { get; }

    /// <summary>
    /// Gets a value indicating whether the authentication token is expired.
    /// </summary>
    bool IsTokenExpired { get; }

    /// <summary>
    /// Gets or sets a value indicating whether the connection is faulted (its session is unusable
    /// or in an unknown state), so the pool discards it on release instead of reusing it.
    /// Set-once: there is no un-faulting a connection.
    /// </summary>
    bool IsFaulted { get; internal set; }
}
