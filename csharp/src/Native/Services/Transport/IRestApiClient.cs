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

using System.IO;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native.Services.Authentication;

namespace AdbcDrivers.Snowflake.Native.Services.Transport;

/// <summary>
/// Provides HTTP communication with Snowflake's REST API.
/// </summary>
internal interface IRestApiClient
{
    /// <summary>
    /// Sends a POST request to the specified endpoint.
    /// </summary>
    /// <typeparam name="TRequest">The request body type.</typeparam>
    /// <typeparam name="TResponse">The response type.</typeparam>
    /// <param name="endpoint">The API endpoint.</param>
    /// <param name="request">The request payload.</param>
    /// <param name="token">The authentication token.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The API response.</returns>
    Task<ApiResponse<TResponse>> PostAsync<TRequest, TResponse>(
        string endpoint,
        TRequest request,
        AuthenticationToken token,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a GET request to the specified endpoint and reads a Snowflake API envelope
    /// (used e.g. to poll a long-running query's result URL).
    /// </summary>
    /// <typeparam name="TResponse">The response type.</typeparam>
    /// <param name="endpoint">The API endpoint.</param>
    /// <param name="token">The authentication token.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The API response.</returns>
    Task<ApiResponse<TResponse>> GetAsync<TResponse>(
        string endpoint,
        AuthenticationToken token,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets an Arrow stream from the specified URL.
    /// </summary>
    /// <param name="url">The URL to fetch the Arrow stream from.</param>
    /// <param name="token">The authentication token.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A stream containing Arrow data.</returns>
    Task<Stream> GetArrowStreamAsync(
        string url,
        AuthenticationToken token,
        Dictionary<string, string>? chunkHeaders = null,
        string? qrmk = null,
        CancellationToken cancellationToken = default);
}
