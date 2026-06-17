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
using AdbcDrivers.Snowflake.Native.Services.Authentication;

namespace AdbcDrivers.Snowflake.Native.Services.Query;

/// <summary>
/// Provides query execution services for Snowflake connections.
/// </summary>
internal interface IQueryExecutor
{
    /// <summary>
    /// Executes a query and returns the result.
    /// </summary>
    /// <param name="request">The query request.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The query result.</returns>
    Task<QueryResult> ExecuteQueryAsync(QueryRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Describes (prepares) a statement without executing it, returning its result schema.
    /// </summary>
    /// <param name="request">The query request describing the statement.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A prepared statement with the result schema populated.</returns>
    Task<PreparedStatement> DescribeAsync(QueryRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Mints a fresh session token from the master token (<c>POST /session/token-request</c>),
    /// replacing the token's session token in place. Queries do this automatically when they hit a
    /// session-expired response; it's exposed for explicit/proactive renewal and for tests.
    /// Distinct from <see cref="HeartbeatAsync"/>, which keeps the existing session alive and only
    /// renews as a fallback.
    /// </summary>
    /// <param name="authToken">The token to renew; its session token is replaced in place.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task RenewSessionAsync(AuthenticationToken authToken, CancellationToken cancellationToken = default);

    /// <summary>
    /// Keeps the session alive by pinging <c>/session/heartbeat</c> with the current session token
    /// (resets the server-side idle clock — it does <i>not</i> mint a new token). If the session has
    /// already expired it falls back to <see cref="RenewSessionAsync"/>. Intended for a periodic
    /// keep-alive timer so long-idle connections never reach master-token expiry.
    /// </summary>
    /// <param name="authToken">The session's authentication token.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task HeartbeatAsync(AuthenticationToken authToken, CancellationToken cancellationToken = default);

    /// <summary>
    /// Cancels a running query by aborting the request it was submitted with
    /// (<c>POST /queries/v1/abort-request</c>). Snowflake keys the abort on the original
    /// <paramref name="requestId"/>, not the queryId it returns, so the caller must hold the id it
    /// submitted the query with. The abort is authenticated with the session token.
    /// </summary>
    /// <param name="requestId">The request id the running query was submitted with.</param>
    /// <param name="authToken">The session's authentication token, used to authenticate the abort.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the cancellation operation.</returns>
    Task CancelQueryAsync(string requestId, AuthenticationToken authToken, CancellationToken cancellationToken = default);
}