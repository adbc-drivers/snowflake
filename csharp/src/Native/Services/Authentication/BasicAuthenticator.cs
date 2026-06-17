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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native.Configuration;

namespace AdbcDrivers.Snowflake.Native.Services.Authentication;

/// <summary>
/// Implements basic username/password authentication for Snowflake.
/// </summary>
internal class BasicAuthenticator : IBasicAuthenticator
{
    private readonly SnowflakeLoginClient _loginClient;

    /// <summary>
    /// Initializes a new instance of the <see cref="BasicAuthenticator"/> class.
    /// </summary>
    /// <param name="loginClient">The shared login client.</param>
    public BasicAuthenticator(SnowflakeLoginClient loginClient)
    {
        _loginClient = loginClient ?? throw new ArgumentNullException(nameof(loginClient));
    }

    /// <inheritdoc/>
    public async Task<AuthenticationToken> AuthenticateAsync(
        ConnectionConfig config,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(config);
        ValidateRequirements(config);

        var authData = new LoginRequestData
        {
            AUTHENTICATOR = "snowflake",
            LOGIN_NAME = config.User,
            PASSWORD = config.Authentication.Password
        };

        return await _loginClient.LoginAsync(config.Account, authData, config, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Reports everything missing for username/password auth in a single error.</summary>
    internal static void ValidateRequirements(ConnectionConfig config)
    {
        var missing = new List<string>();
        if (string.IsNullOrEmpty(config.Account))
            missing.Add("account");
        if (string.IsNullOrEmpty(config.User))
            missing.Add("user");
        if (string.IsNullOrEmpty(config.Authentication.Password))
            missing.Add("password");

        if (missing.Count > 0)
            throw new ArgumentException($"Username/password authentication requires: {string.Join(", ", missing)}.", nameof(config));
    }
}
