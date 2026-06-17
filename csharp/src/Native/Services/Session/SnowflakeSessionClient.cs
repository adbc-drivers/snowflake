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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native.Configuration;
using AdbcDrivers.Snowflake.Native.Services.Authentication;
using AdbcDrivers.Snowflake.Native.Services.ConnectionPool;
using AdbcDrivers.Snowflake.Native.Services.Query;
using AdbcDrivers.Snowflake.Native.Services.Transport;
using AdbcDrivers.Snowflake.Native.Services.TypeConversion;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AdbcDrivers.Snowflake.Native.Services.Session;

/// <summary>
/// Implements the session operations the connection pool needs (keep-alive heartbeat, close) over the
/// shared <see cref="HttpClient"/>. This is the single place that knows how to assemble the transport
/// for those operations, so the database and the pool don't have to. Heartbeat runs through a
/// <see cref="QueryExecutor"/> (which owns the endpoint and renew-on-expiry fallback); close runs
/// through <see cref="SnowflakeLoginClient"/>. The executor is built per call because it is keyed on
/// the connection's account/network, which vary by config.
/// </summary>
internal sealed class SnowflakeSessionClient(
    SnowflakeLoginClient loginClient,
    HttpClient httpClient,
    ILoggerFactory? loggerFactory = null)
    : ISessionLifecycle
{
    private readonly SnowflakeLoginClient _loginClient = loginClient ?? throw new ArgumentNullException(nameof(loginClient));
    private readonly HttpClient _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
    private readonly ILoggerFactory _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;

    /// <inheritdoc/>
    public Task HeartbeatAsync(AuthenticationToken token, ConnectionConfig config, CancellationToken cancellationToken = default)
    {
        var executor = new QueryExecutor(
            new RestApiClient(_httpClient, config.EnableCompression,
                logger: _loggerFactory.CreateLogger<RestApiClient>()),
            TypeConverter.Shared,
            config.Account,
            config.Network,
            _loggerFactory.CreateLogger<QueryExecutor>(),
            // This executor serves the pool's own idle heartbeats, not a checked-out connection, so
            // there is no connection to fault: the pool swallows a failed heartbeat and the session
            // recovers via reactive renewal on the next query.
            onConnectionFault: static () => { });
        return executor.HeartbeatAsync(token, cancellationToken);
    }

    /// <inheritdoc/>
    public Task CloseAsync(AuthenticationToken token, ConnectionConfig config, CancellationToken cancellationToken = default) =>
        _loginClient.CloseSessionAsync(token, config, cancellationToken);
}
