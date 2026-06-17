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

using System.Text.Json.Serialization;

using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;

namespace AdbcDrivers.Snowflake.Native.Tests.Integration;

/// <summary>
/// Configuration settings for working with native Snowflake driver.
/// Uses the same JSON format as Interop tests for compatibility.
/// </summary>
internal class IntegrationTestConfiguration : TestConfiguration
{
    /// <summary>
    /// The Snowflake account.
    /// </summary>
    [JsonPropertyName("account")]
    public string Account { get; set; } = string.Empty;

    /// <summary>
    /// The Snowflake host (optional, derived from account if not provided).
    /// </summary>
    [JsonPropertyName("host")]
    public string Host { get; set; } = string.Empty;

    /// <summary>
    /// The Snowflake database.
    /// </summary>
    [JsonPropertyName("database")]
    public string Database { get; set; } = string.Empty;

    /// <summary>
    /// The Snowflake schema.
    /// </summary>
    [JsonPropertyName("schema")]
    public string Schema { get; set; } = string.Empty;

    /// <summary>
    /// The Snowflake user.
    /// </summary>
    [JsonPropertyName("user")]
    public string User { get; set; } = string.Empty;

    /// <summary>
    /// The Snowflake password (if using).
    /// </summary>
    [JsonPropertyName("password")]
    public string Password { get; set; } = string.Empty;

    /// <summary>
    /// The Snowflake warehouse.
    /// </summary>
    [JsonPropertyName("warehouse")]
    public string Warehouse { get; set; } = string.Empty;

    /// <summary>
    /// The Snowflake role.
    /// </summary>
    [JsonPropertyName("role")]
    public string Role { get; set; } = string.Empty;

    /// <summary>
    /// The snowflake Authentication
    /// </summary>
    [JsonPropertyName("authentication")]
    public SnowflakeAuthentication Authentication { get; set; } = new();

    /// <summary>
    /// Whether to skip TLS certificate verification (e.g., for privatelink endpoints).
    /// </summary>
    [JsonPropertyName("tls_skip_verify")]
    public bool TlsSkipVerify { get; set; } = false;


}

public class SnowflakeAuthentication
{
    const string AuthOAuth = "auth_oauth";
    const string AuthJwt = "auth_jwt";
    const string AuthPat = "auth_pat";
    const string AuthSnowflake = "auth_snowflake";
    const string AuthExternalBrowser = "auth_externalbrowser";

    [JsonPropertyName(AuthOAuth)]
    public OAuthAuthentication? OAuth { get; set; }

    [JsonPropertyName(AuthJwt)]
    public JwtAuthentication? SnowflakeJwt { get; set; }

    [JsonPropertyName(AuthPat)]
    public PatAuthentication? Pat { get; set; }

    [JsonPropertyName(AuthSnowflake)]
    public DefaultAuthentication? Default { get; set; }

    [JsonPropertyName(AuthExternalBrowser)]
    public ExternalBrowserAuthentication? ExternalBrowser { get; set; }
}

public class PatAuthentication
{
    [JsonPropertyName("user")]
    public string User { get; set; } = string.Empty;

    [JsonPropertyName("token")]
    public string Token { get; set; } = string.Empty;
}

public class OAuthAuthentication
{
    [JsonPropertyName("token")]
    public string Token { get; set; } = string.Empty;

    [JsonPropertyName("user")]
    public string User { get; set; } = string.Empty;
}

public class JwtAuthentication
{
    [JsonPropertyName("private_key")]
    public string PrivateKey { get; set; } = string.Empty;

    [JsonPropertyName("private_key_file")]
    public string PrivateKeyFile { get; set; } = string.Empty;

    [JsonPropertyName("private_key_pwd")]
    public string PrivateKeyPassPhrase { get; set; } = string.Empty;

    [JsonPropertyName("user")]
    public string User { get; set; } = string.Empty;
}

public class DefaultAuthentication
{
    [JsonPropertyName("user")]
    public string User { get; set; } = string.Empty;

    [JsonPropertyName("password")]
    public string Password { get; set; } = string.Empty;
}

public class ExternalBrowserAuthentication
{
    [JsonPropertyName("user")]
    public string User { get; set; } = string.Empty;
}
