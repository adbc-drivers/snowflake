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
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native.Configuration;
using Microsoft.Extensions.Logging;
using AdbcDrivers.Snowflake.Native.Services.Authentication;
using AdbcDrivers.Snowflake.Native.Services.ConnectionPool;
using AdbcDrivers.Snowflake.Native.Services.Session;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Snowflake.Native;

/// <summary>
/// Snowflake database implementation for ADBC.
/// </summary>
public sealed class SnowflakeDatabase : AdbcDatabase
{
    private readonly IReadOnlyDictionary<string, string>? _parameters;
    private readonly IConnectionPoolManager _connectionPool;
    private readonly HttpClient _httpClient;
    private readonly ILoggerFactory? _loggerFactory;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="SnowflakeDatabase"/> class.
    /// </summary>
    /// <param name="parameters">The ADBC connection parameters.</param>
    /// <param name="handler">Custom HttpMessageHandler. When provided, the caller retains ownership
    /// and is responsible for disposing it after this <see cref="SnowflakeDatabase"/> instance is disposed.
    /// The handler must remain alive for the lifetime of this instance.</param>
    /// <param name="loggerFactory">Logger factory for driver diagnostics (connection lifecycle,
    /// transport retries, pool maintenance). Like the handler, it can only be supplied by
    /// constructing <see cref="SnowflakeDatabase"/> directly — an object can't ride the ADBC
    /// string-dictionary <c>Open</c>. When null the driver logs nothing.</param>
    public SnowflakeDatabase(IReadOnlyDictionary<string, string>? parameters = null, HttpMessageHandler? handler = null, ILoggerFactory? loggerFactory = null)
    {
        _parameters = parameters;
        _loggerFactory = loggerFactory;
        _httpClient = CreateHttpClient(parameters, handler, loggerFactory);

        var loginClient = new SnowflakeLoginClient(_httpClient);
        var basicAuth = new BasicAuthenticator(loginClient);
        var keyPairAuth = new KeyPairAuthenticator(loginClient);
        var oauthAuth = new OAuthAuthenticator(loginClient);
        var patAuth = new PatAuthenticator(loginClient);
        var ssoAuth = new SsoAuthenticator(loginClient, _httpClient);

        var authService = new AuthenticationService(basicAuth, keyPairAuth, oauthAuth, patAuth, ssoAuth);
        var sessionClient = new SnowflakeSessionClient(loginClient, _httpClient, _loggerFactory);
        _connectionPool = new ConnectionPoolManager(authService, sessionClient,
            logger: _loggerFactory?.CreateLogger<ConnectionPoolManager>());
    }
    
    private static HttpClient CreateHttpClient(
        IReadOnlyDictionary<string, string>? parameters, HttpMessageHandler? handler, ILoggerFactory? loggerFactory)
    {
        var network = ConnectionStringParser.ParseNetworkConfig(parameters);

        if (handler != null)
        {
            if ((network.TlsSkipVerify || network.NoProxy) && loggerFactory != null)
            {
                loggerFactory.CreateLogger<SnowflakeDatabase>().LogWarning(
                    "Network settings (tls_skip_verify, no_proxy) are ignored when a custom HttpMessageHandler is provided.");
            }

            return new HttpClient(handler, disposeHandler: false);
        }

        var defaultHandler = new HttpClientHandler();
        if (network.TlsSkipVerify)
            defaultHandler.ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;

        if (network.NoProxy)
            defaultHandler.UseProxy = false;

        return new HttpClient(defaultHandler, disposeHandler: true);
    }

    /// <summary>
    /// Creates a new connection to the Snowflake database.
    /// </summary>
    /// <param name="parameters">Connection-specific parameters that override database parameters.</param>
    /// <returns>An AdbcConnection instance.</returns>
    public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? parameters)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return ConnectAsync(parameters).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Asynchronously create a new connection to the Snowflake database. The token cancels the
    /// wait for pool capacity and the login round trip.
    /// </summary>
    public async Task<AdbcConnection> ConnectAsync(IReadOnlyDictionary<string, string>? parameters, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var config = ConnectionStringParser.ParseParameters(parameters, _parameters);
        return await SnowflakeConnection.CreateAsync(config, _httpClient, _connectionPool, _loggerFactory, cancellationToken).ConfigureAwait(false);
    }


    /// <summary>
    /// Disposes the database and releases any resources.
    /// </summary>
    public override void Dispose()
    {
        if (!_disposed)
        {
            // Pool first: its best-effort session closes still need a live client.
            _connectionPool.Dispose();
            _httpClient.Dispose();
            _disposed = true;
        }
        base.Dispose();
    }
}
