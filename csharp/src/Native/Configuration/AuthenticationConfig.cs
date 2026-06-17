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

using System.Collections.Generic;

namespace AdbcDrivers.Snowflake.Native.Configuration;

/// <summary>
/// Represents authentication configuration for Snowflake connections.
/// </summary>
internal class AuthenticationConfig
{
    /// <summary>
    /// Gets or sets the authentication type.
    /// </summary>
    public AuthenticationType Type { get; set; } = AuthenticationType.UsernamePassword;

    /// <summary>
    /// Gets or sets the password for basic authentication.
    /// </summary>
    public string? Password { get; set; }

    /// <summary>
    /// Gets or sets the path to the RSA private key file.
    /// </summary>
    public string? PrivateKeyPath { get; set; }

    /// <summary>
    /// Gets or sets the RSA private key value in PKCS8 format (inline, not from file).
    /// </summary>
    public string? PrivateKey { get; set; }

    /// <summary>
    /// Gets or sets the passphrase for encrypted private keys.
    /// </summary>
    public string? PrivateKeyPassphrase { get; set; }

    /// <summary>
    /// Gets or sets the access token for token-based authentication: an OAuth access token
    /// (<see cref="AuthenticationType.OAuth"/>) or a programmatic access token
    /// (<see cref="AuthenticationType.Pat"/>). Both arrive via the same ADBC option
    /// (<c>adbc.snowflake.sql.client_option.auth_token</c>); the auth type decides how it is
    /// presented to Snowflake.
    /// </summary>
    public string? Token { get; set; }

    /// <summary>
    /// Gets or sets additional SSO properties.
    /// </summary>
    public Dictionary<string, string> SsoProperties { get; set; } = new();
}