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
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native.Configuration;

using Apache.Arrow.Adbc;

namespace AdbcDrivers.Snowflake.Native.Services.Authentication;

/// <summary>
/// Implements Single Sign-On (SSO) authentication for Snowflake using external browser.
/// Follows the same flow as the official snowflake-connector-net:
/// 1. Start local HTTP listener on a random port
/// 2. POST to /session/authenticator-request with the port → get ssoUrl + proofKey
/// 3. Open browser to ssoUrl
/// 4. Snowflake redirects back to localhost with ?token=... 
/// 5. Send login request with Token + ProofKey
/// </summary>
internal class SsoAuthenticator : ISsoAuthenticator
{
    internal static readonly TimeSpan DefaultBrowserTimeout = TimeSpan.FromSeconds(120);

    private static readonly string SuccessHtml =
        "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"/>" +
        "<title>Snowflake Authentication</title></head>" +
        "<body><h1>Authentication Successful</h1>" +
        "<p>Your identity was confirmed. You can close this window and return to your application.</p>" +
        "</body></html>";

    private static readonly string ErrorHtml =
        "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"/>" +
        "<title>Snowflake Authentication</title></head>" +
        "<body><h1>Authentication Failed</h1>" +
        "<p>Unable to extract authentication token. Please try again.</p>" +
        "</body></html>";

    private const string TOKEN_QUERY_PREFIX = "?token=";

    private readonly SnowflakeLoginClient _loginClient;
    private readonly HttpClient _httpClient;

    /// <summary>
    /// Initializes a new instance of the <see cref="SsoAuthenticator"/> class.
    /// </summary>
    /// <param name="loginClient">The shared login client.</param>
    /// <param name="httpClient">The HTTP client for the authenticator-request endpoint.</param>
    public SsoAuthenticator(SnowflakeLoginClient loginClient, HttpClient httpClient)
    {
        _loginClient = loginClient ?? throw new ArgumentNullException(nameof(loginClient));
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
    }

    /// <inheritdoc/>
    public async Task<AuthenticationToken> AuthenticateAsync(
        ConnectionConfig config,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(config);
        ValidateRequirements(config);

        string account = config.Account;
        string user = config.User;

        // Step 1: Find a free port and start the local HTTP listener
        int localPort = GetRandomUnusedPort();
        using var listener = CreateHttpListener(localPort);
        listener.Start();

        try
        {
            // Step 2: Get SSO URL and proof key from Snowflake
            var (ssoUrl, proofKey) = await GetSsoUrlAndProofKeyAsync(
                account, user, localPort, cancellationToken).ConfigureAwait(false);

            // Step 3: Open browser for user authentication
            OpenBrowser(ssoUrl);

            // Step 4: Wait for the redirect callback with the token
            var token = await WaitForTokenAsync(listener, cancellationToken).ConfigureAwait(false);

            // Step 5: Complete authentication with token and proof key
            var authData = new LoginRequestData
            {
                AUTHENTICATOR = "EXTERNALBROWSER",
                LOGIN_NAME = user,
                TOKEN = token,
                PROOF_KEY = proofKey
            };

            return await _loginClient.LoginAsync(account, authData, config, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            listener.Stop();
        }
    }

    static void ValidateRequirements(ConnectionConfig config)
    {
        var missing = new List<string>();
        if (string.IsNullOrEmpty(config.Account))
            missing.Add("account");
        if (string.IsNullOrEmpty(config.User))
            missing.Add("user");

        if (missing.Count > 0)
            throw new ArgumentException($"External-browser SSO authentication requires: {string.Join(", ", missing)}.", nameof(config));
    }

    private async Task<(string SsoUrl, string ProofKey)> GetSsoUrlAndProofKeyAsync(
        string account,
        string user,
        int localPort,
        CancellationToken cancellationToken)
    {
        var authenticatorUrl = _loginClient.BuildUrl(account, SnowflakeLoginClient.AuthenticatorEndpoint);
        var authenticatorRequest = new LoginRequestBody
        {
            Data = new LoginRequestData
            {
                ACCOUNT_NAME = account,
                LOGIN_NAME = user,
                AUTHENTICATOR = "EXTERNALBROWSER",
                BROWSER_MODE_REDIRECT_PORT = localPort.ToString(),
                CLIENT_APP_ID = ".NET",
                CLIENT_APP_VERSION = "3.1.0",
                CLIENT_ENVIRONMENT = ClientEnvironment.Create(),
                SESSION_PARAMETERS = new Dictionary<string, object>
                {
                    { "DOTNET_QUERY_RESULT_FORMAT", "ARROW" }
                }
            }
        };

        try
        {
            var response = await _httpClient.PostAsJsonAsync(authenticatorUrl, authenticatorRequest, cancellationToken).ConfigureAwait(false);
            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

            var responseContent = JsonSerializer.Deserialize<AuthenticatorResponse>(responseBody,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

            if (responseContent?.Data?.SsoUrl == null)
                throw new AdbcException($"Failed to retrieve SSO URL from Snowflake. Response: {responseBody}");

            if (responseContent.Data.ProofKey == null)
                throw new AdbcException($"Failed to retrieve proof key from Snowflake. Response: {responseBody}");

            return (responseContent.Data.SsoUrl, responseContent.Data.ProofKey);
        }
        catch (HttpRequestException ex)
        {
            throw new AdbcException($"Failed to get SSO URL from Snowflake: {ex.Message}", ex);
        }
    }

    private async Task<string> WaitForTokenAsync(
        HttpListener listener,
        CancellationToken cancellationToken)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(DefaultBrowserTimeout);

        // When the wait is abandoned (timeout or caller cancellation), stopping the listener
        // faults this still-pending accept; observe it so it can never surface through
        // TaskScheduler.UnobservedTaskException.
        Task<HttpListenerContext> contextTask = listener.GetContextAsync();
        _ = ObserveAbandonedAcceptAsync(contextTask);

        try
        {
            var context = await contextTask.WaitAsync(timeoutCts.Token).ConfigureAwait(false);

            var query = context.Request.Url?.Query;
            string? token = null;

            if (query != null && query.StartsWith(TOKEN_QUERY_PREFIX, StringComparison.Ordinal))
            {
                token = Uri.UnescapeDataString(query.Substring(TOKEN_QUERY_PREFIX.Length));
            }

            if (string.IsNullOrEmpty(token))
            {
                await SendResponseAsync(context, ErrorHtml).ConfigureAwait(false);
                throw new AdbcException("No authentication token received from Snowflake SSO. " +
                    $"Received query: {query}");
            }

            await SendResponseAsync(context, SuccessHtml).ConfigureAwait(false);
            return token;
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new AdbcException(
                $"Browser authentication timed out after {DefaultBrowserTimeout.TotalSeconds} seconds. " +
                "Please ensure your browser completed the SSO login.");
        }
    }

    /// <summary>
    /// Awaits the listener accept purely to observe it: after the redirect wait is abandoned,
    /// the fault raised by stopping the listener is expected and must not go unobserved. On
    /// the success path the main flow has already consumed the context; awaiting again is a
    /// no-op.
    /// </summary>
    private static async Task ObserveAbandonedAcceptAsync(Task<HttpListenerContext> contextTask)
    {
        try
        {
            await contextTask.ConfigureAwait(false);
        }
        catch
        {
            // Expected when the listener is stopped with the accept still pending.
        }
    }

    private static async Task SendResponseAsync(HttpListenerContext context, string html)
    {
        var responseBytes = System.Text.Encoding.UTF8.GetBytes(html);
        context.Response.ContentType = "text/html; charset=UTF-8";
        context.Response.ContentLength64 = responseBytes.Length;
        await context.Response.OutputStream.WriteAsync(responseBytes).ConfigureAwait(false);
        context.Response.Close();
    }

    private static HttpListener CreateHttpListener(int port)
    {
        var listener = new HttpListener();
        // Bind both 127.0.0.1 and localhost to handle either redirect target
        listener.Prefixes.Add($"http://127.0.0.1:{port}/");
        listener.Prefixes.Add($"http://localhost:{port}/");
        return listener;
    }

    private static int GetRandomUnusedPort()
    {
        using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        return ((IPEndPoint)socket.LocalEndPoint!).Port;
    }

    private static void OpenBrowser(string url)
    {
        try
        {
            Process.Start(new ProcessStartInfo
            {
                FileName = url,
                UseShellExecute = true
            });
        }
        catch (Exception ex)
        {
            throw new AdbcException(
                $"Failed to open browser for SSO authentication. " +
                $"Please manually open: {url}", ex);
        }
    }
}
