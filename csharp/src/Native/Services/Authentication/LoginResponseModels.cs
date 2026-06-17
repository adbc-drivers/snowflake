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

namespace AdbcDrivers.Snowflake.Native.Services.Authentication;

/// <summary>
/// Represents the login response from Snowflake.
/// </summary>
internal class LoginResponse
{
    public bool Success { get; set; }
    public string? Message { get; set; }
    public LoginData? Data { get; set; }
}

/// <summary>
/// Represents the data portion of the login response.
/// </summary>
internal class LoginData
{
    public string? Token { get; set; }
    public long? SessionId { get; set; }
    public string? MasterToken { get; set; }

    /// <summary>Session-token validity in seconds (Snowflake <c>validityInSeconds</c>, ~1h).</summary>
    public int ValidityInSeconds { get; set; } = 3600;

    /// <summary>Master-token validity in seconds (Snowflake <c>masterValidityInSeconds</c>, ~4h).</summary>
    public int MasterValidityInSeconds { get; set; } = 14400;
}

/// <summary>
/// Represents the authenticator-request response from Snowflake (for SSO).
/// </summary>
internal class AuthenticatorResponse
{
    public bool Success { get; set; }
    public AuthenticatorResponseData? Data { get; set; }
}

/// <summary>
/// Represents the data portion of the authenticator-request response.
/// </summary>
internal class AuthenticatorResponseData
{
    public string? SsoUrl { get; set; }
    public string? ProofKey { get; set; }
}
