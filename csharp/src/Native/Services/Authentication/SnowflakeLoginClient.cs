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
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using AdbcDrivers.Snowflake.Native.Configuration;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Snowflake.Native.Services.Authentication;

/// <summary>
/// Shared client for the Snowflake login protocol.
/// </summary>
internal class SnowflakeLoginClient
{
    readonly HttpClient _httpClient;

    const string LoginEndpoint = "/session/v1/login-request";
    internal const string AuthenticatorEndpoint = "/session/authenticator-request";
    const string SessionEndpoint = "/session";

    /// <summary>
    /// Initializes a new instance of the <see cref="SnowflakeLoginClient"/> class.
    /// </summary>
    /// <param name="httpClient">The HTTP client for making requests.</param>
    public SnowflakeLoginClient(HttpClient httpClient)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
    }

    /// <summary>
    /// Performs a login request to Snowflake.
    /// </summary>
    /// <param name="account">The Snowflake account identifier.</param>
    /// <param name="authData">Auth-specific fields set by the caller.</param>
    /// <param name="config">Optional connection configuration.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An authentication token.</returns>
    public async Task<AuthenticationToken> LoginAsync(
        string account,
        LoginRequestData authData,
        ConnectionConfig? config = null,
        CancellationToken cancellationToken = default)
    {
        // Fill in common fields. CLIENT_APP_ID/CLIENT_APP_VERSION are NOT free-form identity:
        // Snowflake gates server-side capabilities on them — a ".NET" client below the version
        // that introduced Arrow support gets JSON results regardless of the requested
        // DOTNET_QUERY_RESULT_FORMAT. So this must claim an Arrow-capable connector-net version,
        // not this driver's own assembly version.
        authData.CLIENT_APP_ID = ".NET";
        authData.CLIENT_APP_VERSION = "3.1.0";
        authData.ACCOUNT_NAME = account;
        authData.CLIENT_ENVIRONMENT = ClientEnvironment.Create();
        authData.SESSION_PARAMETERS = new Dictionary<string, object>
        {
            { "DOTNET_QUERY_RESULT_FORMAT", "ARROW" }
        };

        var loginUrl = BuildUrl(account, LoginEndpoint, config);
        var loginRequest = new LoginRequestBody { Data = authData };

        try
        {
            var response = await _httpClient.PostAsJsonAsync(loginUrl, loginRequest, cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

            var responseContent = await response.Content.ReadFromJsonAsync<LoginResponse>(cancellationToken).ConfigureAwait(false);

            if (responseContent?.Data == null)
                throw new AdbcException("Invalid response from Snowflake authentication service.");

            if (!responseContent.Success)
            {
                var errorMessage = responseContent.Message ?? "Authentication failed.";
                throw new AdbcException($"Snowflake authentication failed: {errorMessage}");
            }

            return new AuthenticationToken
            {
                SessionToken = responseContent.Data.Token ?? throw new AdbcException("No token received from Snowflake."),
                SessionId = responseContent.Data.SessionId?.ToString(),
                MasterToken = responseContent.Data.MasterToken,
                ExpiresAt = DateTimeOffset.UtcNow.AddSeconds(responseContent.Data.ValidityInSeconds),
                MasterExpiresAt = DateTimeOffset.UtcNow.AddSeconds(responseContent.Data.MasterValidityInSeconds)
            };
        }
        catch (HttpRequestException ex)
        {
            throw new AdbcException($"Failed to authenticate with Snowflake: {ex.Message}", ex);
        }
        catch (JsonException ex)
        {
            throw new AdbcException($"Failed to parse Snowflake authentication response: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Best-effort close of a Snowflake session (<c>POST /session?delete=true</c>) so it is not
    /// left orphaned on the server until it times out. Failures are ignored — an un-closed session
    /// simply expires naturally.
    /// </summary>
    internal async Task CloseSessionAsync(AuthenticationToken token, ConnectionConfig config, CancellationToken cancellationToken = default)
    {
        // Nothing to close without a session token (it can legitimately be null/empty).
        if (string.IsNullOrEmpty(token.SessionToken))
            return;

        var accountUrl = SnowflakeAccountUrl.Build(config.Account, config.Network);
        var requestId = Guid.NewGuid().ToString();
        var requestGuid = Guid.NewGuid().ToString();
        var url = $"{accountUrl}{SessionEndpoint}?delete=true&requestId={requestId}&request_guid={requestGuid}";

        using var request = new HttpRequestMessage(HttpMethod.Post, url);
        request.Headers.TryAddWithoutValidation("Authorization", $"Snowflake Token=\"{token.SessionToken}\"");
        request.Headers.TryAddWithoutValidation("Accept", "application/snowflake");

        using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Builds a Snowflake URL for the given account and endpoint.
    /// </summary>
    /// <param name="account">The Snowflake account identifier.</param>
    /// <param name="endpoint">The API endpoint path.</param>
    /// <param name="config">Optional connection configuration for query parameters.</param>
    /// <returns>The fully-qualified URL.</returns>
    internal string BuildUrl(string account, string endpoint, ConnectionConfig? config = null)
    {
        var accountUrl = SnowflakeAccountUrl.Build(account, config?.Network);

        var uriBuilder = new UriBuilder($"{accountUrl}{endpoint}");
        var query = HttpUtility.ParseQueryString(string.Empty);

        if (config != null)
        {
            if (!string.IsNullOrEmpty(config.Warehouse))
                query["warehouse"] = config.Warehouse;

            if (!string.IsNullOrEmpty(config.Database))
                query["databaseName"] = config.Database;

            if (!string.IsNullOrEmpty(config.Schema))
                query["schemaName"] = config.Schema;

            if (!string.IsNullOrEmpty(config.Role))
                query["roleName"] = config.Role;
        }

        query["requestId"] = Guid.NewGuid().ToString();
        query["request_guid"] = Guid.NewGuid().ToString();

        uriBuilder.Query = query.ToString();
        return uriBuilder.ToString();
    }
}
