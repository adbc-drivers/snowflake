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

namespace AdbcDrivers.Snowflake.Native.Services.Authentication;

/// <summary>
/// Provides authentication services for Snowflake connections.
/// </summary>
internal class AuthenticationService : IAuthenticationService
{
    private readonly IBasicAuthenticator _basicAuth;
    private readonly IKeyPairAuthenticator _keyPairAuth;
    private readonly IOAuthAuthenticator _oauthAuth;
    private readonly IPatAuthenticator _patAuth;
    private readonly ISsoAuthenticator _ssoAuth;

    /// <summary>
    /// Initializes a new instance of the <see cref="AuthenticationService"/> class.
    /// </summary>
    /// <param name="basicAuth">The basic authenticator.</param>
    /// <param name="keyPairAuth">The key pair authenticator.</param>
    /// <param name="oauthAuth">The OAuth authenticator.</param>
    /// <param name="patAuth">The programmatic-access-token authenticator.</param>
    /// <param name="ssoAuth">The SSO authenticator.</param>
    public AuthenticationService(
        IBasicAuthenticator basicAuth,
        IKeyPairAuthenticator keyPairAuth,
        IOAuthAuthenticator oauthAuth,
        IPatAuthenticator patAuth,
        ISsoAuthenticator ssoAuth)
    {
        _basicAuth = basicAuth ?? throw new ArgumentNullException(nameof(basicAuth));
        _keyPairAuth = keyPairAuth ?? throw new ArgumentNullException(nameof(keyPairAuth));
        _oauthAuth = oauthAuth ?? throw new ArgumentNullException(nameof(oauthAuth));
        _patAuth = patAuth ?? throw new ArgumentNullException(nameof(patAuth));
        _ssoAuth = ssoAuth ?? throw new ArgumentNullException(nameof(ssoAuth));
    }

    /// <inheritdoc/>
    public async Task<AuthenticationToken> AuthenticateAsync(
        ConnectionConfig config,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(config);
        
        return config.Authentication.Type switch
        {
            AuthenticationType.UsernamePassword => await _basicAuth.AuthenticateAsync(config, cancellationToken).ConfigureAwait(false),
            AuthenticationType.KeyPair => await _keyPairAuth.AuthenticateAsync(config, cancellationToken).ConfigureAwait(false),
            AuthenticationType.OAuth => await _oauthAuth.AuthenticateAsync(config, cancellationToken).ConfigureAwait(false),
            AuthenticationType.Pat => await _patAuth.AuthenticateAsync(config, cancellationToken).ConfigureAwait(false),
            AuthenticationType.Sso or AuthenticationType.ExternalBrowser => await _ssoAuth.AuthenticateAsync(config, cancellationToken).ConfigureAwait(false),
            _ => throw new NotSupportedException($"Authentication type {config.Authentication.Type} is not supported.")
        };
    }
}
