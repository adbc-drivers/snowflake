/*
* Copyright (c) 2025 ADBC Drivers Contributors
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

namespace AdbcDrivers.Snowflake.Native.Configuration;

/// <summary>
/// Represents the available authentication types for Snowflake.
/// </summary>
internal enum AuthenticationType
{
    /// <summary>
    /// Username and password authentication.
    /// </summary>
    UsernamePassword,

    /// <summary>
    /// RSA key pair authentication.
    /// </summary>
    KeyPair,

    /// <summary>
    /// OAuth 2.0 token authentication.
    /// </summary>
    OAuth,

    /// <summary>
    /// Programmatic access token (PAT) authentication — Snowflake's replacement for
    /// password-style programmatic access. The user must be subject to a network policy.
    /// </summary>
    Pat,

    /// <summary>
    /// Single Sign-On authentication.
    /// </summary>
    Sso,

    /// <summary>
    /// External browser authentication.
    /// </summary>
    ExternalBrowser
}
