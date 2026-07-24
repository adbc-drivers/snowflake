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
using System.Text.Json.Serialization;

namespace AdbcDrivers.Snowflake.Native.Services.Authentication;

/// <summary>
/// Represents the login request body sent to Snowflake.
/// </summary>
internal class LoginRequestBody
{
    /// <summary>
    /// Gets or sets the login request data.
    /// </summary>
    [JsonPropertyName("data")]
    public LoginRequestData Data { get; set; } = new();
}

/// <summary>
/// Represents the data portion of the login request.
/// </summary>
internal class LoginRequestData
{
    /// <summary>
    /// Gets or sets the client application ID.
    /// </summary>
    [JsonPropertyName("CLIENT_APP_ID")]
    public string CLIENT_APP_ID { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the client application version.
    /// </summary>
    [JsonPropertyName("CLIENT_APP_VERSION")]
    public string CLIENT_APP_VERSION { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the Snowflake account name.
    /// </summary>
    [JsonPropertyName("ACCOUNT_NAME")]
    public string ACCOUNT_NAME { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the login name (username).
    /// </summary>
    [JsonPropertyName("LOGIN_NAME")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? LOGIN_NAME { get; set; }

    /// <summary>
    /// Gets or sets the password.
    /// </summary>
    [JsonPropertyName("PASSWORD")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? PASSWORD { get; set; }

    /// <summary>
    /// Gets or sets the OAuth token.
    /// </summary>
    [JsonPropertyName("TOKEN")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? TOKEN { get; set; }

    /// <summary>
    /// Gets or sets the authenticator type.
    /// </summary>
    [JsonPropertyName("AUTHENTICATOR")]
    public string AUTHENTICATOR { get; set; } = "snowflake";

    /// <summary>
    /// Gets or sets the client environment information.
    /// </summary>
    [JsonPropertyName("CLIENT_ENVIRONMENT")]
    public ClientEnvironment CLIENT_ENVIRONMENT { get; set; } = new();

    /// <summary>
    /// Gets or sets the raw SAML response for SSO authentication.
    /// </summary>
    [JsonPropertyName("RAW_SAML_RESPONSE")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? RAW_SAML_RESPONSE { get; set; }

    /// <summary>
    /// Gets or sets the proof key for external browser authentication.
    /// </summary>
    [JsonPropertyName("PROOF_KEY")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? PROOF_KEY { get; set; }

    /// <summary>
    /// Gets or sets the browser mode redirect port for external browser authentication.
    /// </summary>
    [JsonPropertyName("BROWSER_MODE_REDIRECT_PORT")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? BROWSER_MODE_REDIRECT_PORT { get; set; }

    /// <summary>
    /// Gets or sets the session parameters.
    /// </summary>
    [JsonPropertyName("SESSION_PARAMETERS")]
    public Dictionary<string, object> SESSION_PARAMETERS { get; set; } = new();
}
