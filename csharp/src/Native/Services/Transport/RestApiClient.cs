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
using System.IO;
using System.IO.Compression;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native.Services.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AdbcDrivers.Snowflake.Native.Services.Transport;

/// <summary>
/// Implements HTTP communication with Snowflake's REST API.
/// </summary>
internal class RestApiClient : IRestApiClient
{
    readonly HttpClient _httpClient;
    readonly bool _enableCompression;
    readonly int _maxRetries;
    readonly TimeSpan _baseRetryDelay;
    readonly ILogger _logger;

    // Serializer options backed by the source-generated context (see SnowflakeJsonContext).
    static readonly JsonSerializerOptions JsonOptions = new()
    {
        TypeInfoResolver = SnowflakeJsonContext.Default
    };

    // Header values are immutable; build them once instead of re-parsing per request.
    static readonly MediaTypeWithQualityHeaderValue SnowflakeAccept = new("application/snowflake");
    static readonly MediaTypeWithQualityHeaderValue ArrowStreamAccept = new("application/vnd.apache.arrow.stream");
    static readonly StringWithQualityHeaderValue GzipEncoding = new("gzip");
    static readonly StringWithQualityHeaderValue DeflateEncoding = new("deflate");

    /// <summary>The driver's own version, reported in the user agent and login payload.</summary>
    static readonly string DriverVersion =
        typeof(RestApiClient).Assembly.GetName().Version?.ToString(3) ?? "1.0.0";

    // The server requires the leading ".NET/{version}" product token to enable Arrow results; the
    // OS comment and runtime token are derived from the actual environment rather than hardcoded.
    static readonly ProductInfoHeaderValue[] UserAgent =
    [
        new(".NET", DriverVersion),
        new(
            $"({System.Runtime.InteropServices.RuntimeInformation.OSDescription.Replace('(', '[').Replace(')', ']').Trim()})"),
        new(".NETCoreApp", Environment.Version.ToString(2)),
    ];

    /// <summary>
    /// Initializes a new instance of the <see cref="RestApiClient"/> class.
    /// </summary>
    /// <param name="httpClient">The HTTP client.</param>
    /// <param name="enableCompression">Whether to enable compression.</param>
    /// <param name="maxRetries">Maximum number of retries for transient errors.</param>
    /// <param name="baseRetryDelay">Base delay for exponential backoff.</param>
    /// <param name="logger">Logger for transport-level events (retried transient failures).</param>
    public RestApiClient(
        HttpClient httpClient,
        bool enableCompression = true,
        int maxRetries = 3,
        TimeSpan? baseRetryDelay = null,
        ILogger? logger = null)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _enableCompression = enableCompression;
        _maxRetries = maxRetries;
        _baseRetryDelay = baseRetryDelay ?? TimeSpan.FromMilliseconds(100);
        _logger = logger ?? NullLogger.Instance;
    }

    /// <inheritdoc/>
    public async Task<ApiResponse<TResponse>> PostAsync<TRequest, TResponse>(
        string endpoint,
        TRequest request,
        AuthenticationToken token,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint, nameof(endpoint));
        ArgumentNullException.ThrowIfNull(token, nameof(token));

        return await ExecuteWithRetryAsync(async () =>
        {
            using var requestMessage = new HttpRequestMessage(HttpMethod.Post, endpoint);
            ConfigureRequest(requestMessage, token);

            requestMessage.Content = JsonContent.Create(request, options: JsonOptions);
            AddCompressionHeadersIfEnabled(requestMessage);

            using var response = await _httpClient.SendAsync(
                requestMessage, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

            return await ReadApiResponseAsync<TResponse>(response, cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<ApiResponse<TResponse>> GetAsync<TResponse>(
        string endpoint,
        AuthenticationToken token,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint, nameof(endpoint));
        ArgumentNullException.ThrowIfNull(token, nameof(token));

        return await ExecuteWithRetryAsync(async () =>
        {
            using var requestMessage = new HttpRequestMessage(HttpMethod.Get, endpoint);
            ConfigureRequest(requestMessage, token);
            AddCompressionHeadersIfEnabled(requestMessage);

            using var response = await _httpClient.SendAsync(
                requestMessage, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

            return await ReadApiResponseAsync<TResponse>(response, cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<Stream> GetArrowStreamAsync(
        string url,
        AuthenticationToken token,
        Dictionary<string, string>? chunkHeaders = null,
        string? qrmk = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(url, nameof(url));
        ArgumentNullException.ThrowIfNull(token, nameof(token));

        return await ExecuteWithRetryAsync(async () =>
        {
            using var requestMessage = new HttpRequestMessage(HttpMethod.Get, url);
            if (chunkHeaders == null && string.IsNullOrEmpty(qrmk))
            {
                ConfigureRequest(requestMessage, token);
            }
            else
            {
                if (chunkHeaders != null)
                {
                    foreach (var header in chunkHeaders)
                    {
                        requestMessage.Headers.TryAddWithoutValidation(header.Key, header.Value);
                    }
                }
                else
                {
                    requestMessage.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-algorithm",
                        "AES256");
                    requestMessage.Headers.TryAddWithoutValidation("x-amz-server-side-encryption-customer-key", qrmk);
                }
            }

            requestMessage.Headers.Accept.Add(ArrowStreamAccept);

            var response = await _httpClient.SendAsync(
                requestMessage, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

            // The response is deliberately not disposed: the caller owns the returned live body
            // stream, and disposing the stream releases the connection.
            return await GetResponseStreamAsync(response, cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }

    void AddCompressionHeadersIfEnabled(HttpRequestMessage request)
    {
        if (!_enableCompression) return;

        request.Headers.AcceptEncoding.Add(GzipEncoding);
        request.Headers.AcceptEncoding.Add(DeflateEncoding);
    }

    async Task<Stream> GetResponseStreamAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        if (response.Content.Headers.ContentEncoding.Contains("gzip"))
            return new GZipStream(stream, CompressionMode.Decompress);

        if (response.Content.Headers.ContentEncoding.Contains("deflate"))
            return new DeflateStream(stream, CompressionMode.Decompress);

        return stream;
    }

    async Task<ApiResponse<T>> ReadApiResponseAsync<T>(HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        await using var stream = await GetResponseStreamAsync(response, cancellationToken).ConfigureAwait(false);
        return await JsonSerializer.DeserializeAsync<ApiResponse<T>>(stream, JsonOptions, cancellationToken)
                   .ConfigureAwait(false)
               ?? throw new InvalidOperationException("Failed to deserialize API response.");
    }

    void ConfigureRequest(HttpRequestMessage request, AuthenticationToken token)
    {
        request.Headers.Add("Authorization", $"Snowflake Token=\"{token.SessionToken}\"");
        request.Headers.Accept.Add(SnowflakeAccept);

        foreach (var part in UserAgent)
            request.Headers.UserAgent.Add(part);
    }

    async Task<TResult> ExecuteWithRetryAsync<TResult>(
        Func<Task<TResult>> operation,
        CancellationToken cancellationToken)
    {
        Exception? lastException = null;

        for (var attempt = 0; attempt < _maxRetries; attempt++)
        {
            try
            {
                return await operation().ConfigureAwait(false);
            }
            catch (HttpRequestException ex) when (IsTransientError(ex) && attempt < _maxRetries - 1)
            {
                lastException = ex;
                LogRetry(ex, attempt);
                await DelayAsync(attempt, cancellationToken).ConfigureAwait(false);
            }
            catch (TaskCanceledException ex) when (ex.InnerException is TimeoutException && attempt < _maxRetries - 1)
            {
                lastException = ex;
                LogRetry(ex, attempt);
                await DelayAsync(attempt, cancellationToken).ConfigureAwait(false);
            }
        }

        throw lastException ?? new InvalidOperationException("Operation failed after retries.");
    }

    void LogRetry(Exception ex, int attempt) =>
        _logger.LogWarning(ex, "Transient failure on Snowflake request (attempt {Attempt} of {MaxAttempts}); retrying.",
            attempt + 1, _maxRetries);

    async Task DelayAsync(int attempt, CancellationToken cancellationToken)
    {
        var delay = TimeSpan.FromMilliseconds(
            _baseRetryDelay.TotalMilliseconds * Math.Pow(2, attempt) +
            Random.Shared.Next(0, 100));
        await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
    }

    static bool IsTransientError(HttpRequestException ex)
    {
        // Check for transient HTTP status codes
        if (ex.StatusCode.HasValue)
        {
            var statusCode = (int)ex.StatusCode.Value;
            return statusCode == 408 || // Request Timeout
                   statusCode == 429 || // Too Many Requests
                   statusCode == 503 || // Service Unavailable
                   statusCode == 504; // Gateway Timeout
        }

        // Check for network-related errors
        return ex.InnerException is System.Net.Sockets.SocketException ||
               ex.InnerException is IOException;
    }
}
