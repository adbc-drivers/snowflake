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

using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native.Configuration;

namespace AdbcDrivers.Snowflake.Native.Services.Authentication;

/// <summary>
/// Provides OAuth 2.0 authentication for Snowflake.
/// </summary>
internal interface IOAuthAuthenticator
{
    /// <summary>
    /// Authenticates using the OAuth 2.0 access token in the connection configuration.
    /// User identity is derived from the token by Snowflake, so no user is required.
    /// Validates its own requirements (account, token) and reports everything missing in
    /// a single error.
    /// </summary>
    /// <param name="config">The connection configuration.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An authentication token.</returns>
    Task<AuthenticationToken> AuthenticateAsync(
        ConnectionConfig config,
        CancellationToken cancellationToken = default);
}
