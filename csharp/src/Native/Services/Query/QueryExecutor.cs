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
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native.Services.Authentication;
using AdbcDrivers.Snowflake.Native.Services.Transport;
using AdbcDrivers.Snowflake.Native.Services.TypeConversion;
using Microsoft.Extensions.Logging;

using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;

namespace AdbcDrivers.Snowflake.Native.Services.Query;

/// <summary>
/// Implements query execution for Snowflake connections.
/// </summary>
internal class QueryExecutor : IQueryExecutor
{
    private readonly IRestApiClient _apiClient;
    private readonly QueryResultFactory _resultFactory;
    private readonly string _accountUrl;
    private readonly ILogger<QueryExecutor> _logger;
    private readonly Action _onConnectionFault;
    // Serializes session renewal on this connection so concurrent statements don't double-renew.
    private readonly SemaphoreSlim _renewLock = new(1, 1);
    private const string QueryEndpoint = "/queries/v1/query-request";
    private const string AbortEndpoint = "/queries/v1/abort-request";
    private const string TokenRequestEndpoint = "/session/token-request";
    private const string HeartbeatEndpoint = "/session/heartbeat";

    // GS error code Snowflake returns when the session token has expired.
    const string SessionExpiredCode = "390112";

    // GS error code Snowflake returns when the master token has also expired; the session cannot
    // be recovered by renewal — the user must authenticate again.
    const string MasterTokenExpiredCode = "390114";

    // GS codes Snowflake returns while a query is still executing server-side (the query outlived
    // the synchronous response window); the response carries a getResultUrl to poll instead of a
    // result. 333334 is the async/detached variant of 333333.
    const string QueryInProgressCode = "333333";
    const string QueryInProgressAsyncCode = "333334";

    /// <summary>
    /// Initializes a new instance of the <see cref="QueryExecutor"/> class.
    /// </summary>
    /// <param name="apiClient">The REST API client.</param>
    /// <param name="typeConverter">The Snowflake/Arrow type converter.</param>
    /// <param name="account">The Snowflake account identifier.</param>
    /// <param name="network">The network configuration.</param>
    /// <param name="logger">The ILogger instance for logging.</param>
    /// <param name="onConnectionFault">
    /// Invoked when a failure leaves the session unusable or in an unknown state — a transport-level
    /// error mid-request, a failed renewal, or a session-fatal GS code — so the owner (the pooled
    /// connection) can be flagged for discard instead of being reused. Ordinary SQL errors and
    /// caller cancellations do not trigger it.
    /// </param>
    public QueryExecutor(
        IRestApiClient apiClient,
        ITypeConverter typeConverter,
        string account,
        Configuration.NetworkConfig? network,
        ILogger<QueryExecutor> logger,
        Action onConnectionFault)
    {
        ArgumentNullException.ThrowIfNull(apiClient);
        ArgumentNullException.ThrowIfNull(typeConverter);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(onConnectionFault);
        ArgumentException.ThrowIfNullOrEmpty(account);

        _apiClient = apiClient;
        _resultFactory = new QueryResultFactory(apiClient, typeConverter);
        _logger = logger;
        _onConnectionFault = onConnectionFault;

        _accountUrl = SnowflakeAccountUrl.Build(account, network);
    }

    /// <inheritdoc/>
    public async Task<QueryResult> ExecuteQueryAsync(
        QueryRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrEmpty(request.Statement, nameof(request.Statement));
        ArgumentNullException.ThrowIfNull(request.AuthToken);
        var authToken = request.AuthToken;

        try
        {
            var response = await PostQueryWithRenewalAsync(request, describeOnly: false, authToken, cancellationToken).ConfigureAwait(false);

            if (!response.Success || response.Data == null)
                return CreateFailedResponseResult(response);

            var data = response.Data;
            ResultShape shape = QueryResultFactory.Classify(data);
            _logger.LogDebug(
                "ResultShape={ResultShape}, QueryResultFormat={QueryResultFormat}, HasRowSetBase64={HasRowSetBase64}, ChunkCount={ChunkCount}, HasRowSet={HasRowSet}, HasRowType={HasRowType}",
                shape,
                data.QueryResultFormat,
                !string.IsNullOrEmpty(data.RowSetBase64),
                data.Chunks?.Count ?? 0,
                data.RowSet != null,
                data.RowType != null);

            return await _resultFactory.CreateResultAsync(shape, data, authToken, request.PrefetchConcurrency, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return QueryResult.Cancelled();
        }
        catch (Exception ex)
        {
            return QueryResult.Failed("EXECUTION_ERROR", $"Query execution failed: {ex.Message}", ex);
        }
    }

    private static QueryResult CreateFailedResponseResult(ApiResponse<SnowflakeQueryResponse> response) =>
        QueryResult.Failed(response.Code ?? "UNKNOWN", response.Message ?? "Query execution failed.");

    /// <inheritdoc/>
    public async Task<PreparedStatement> DescribeAsync(
        QueryRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrEmpty(request.Statement, nameof(request.Statement));
        ArgumentNullException.ThrowIfNull(request.AuthToken);

        // Snowflake's internal protocol has no dedicated prepare endpoint; a statement is
        // described (compiled without executing) by sending it to the query-request endpoint
        // with describeOnly=true. The response's rowtype is the result schema.
        var response = await PostQueryWithRenewalAsync(request, describeOnly: true, request.AuthToken, cancellationToken).ConfigureAwait(false);

        if (!response.Success || response.Data == null)
            throw new AdbcException($"Failed to describe statement: {response.Message ?? "Unknown error"}");

        return new PreparedStatement
        {
            ResultSchema = _resultFactory.BuildSchemaFromRowType(response.Data.RowType)
        };
    }

    private SnowflakeQueryRequestBody BuildQueryRequest(QueryRequest request, out string endpoint, bool describeOnly = false)
    {
        var queryRequest = RequestBuilder.BuildQueryRequest(
            request.Statement,
            request.Database,
            request.Schema,
            request.Warehouse,
            request.Role,
            request.QueryTag,
            (int)request.Timeout.TotalSeconds,
            request.Bindings,
            request.IsMultiStatement,
            describeOnly);

        // A caller-supplied request id lets the statement abort this exact request later; the
        // request_guid is per-attempt and is regenerated on the renewal retry.
        var requestId = string.IsNullOrEmpty(request.RequestId) ? Guid.NewGuid().ToString() : request.RequestId;
        var requestGuid = Guid.NewGuid().ToString();
        var startTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString();
        endpoint = $"{_accountUrl}{QueryEndpoint}?requestId={requestId}&request_guid={requestGuid}&startTime={startTime}";
        var sessionId = request.AuthToken?.SessionId;
        if (!string.IsNullOrEmpty(sessionId))
            endpoint += $"&sid={sessionId}";
        return queryRequest;
    }

    /// <summary>
    /// Posts a query/describe request (renewing an expired session token and retrying once — see
    /// <see cref="PostQueryCoreAsync"/>) and classifies any failure for the pool: outcomes that leave
    /// the session unusable or in an unknown state fault the pooled connection so it is discarded
    /// instead of reused; a caller cancellation or an ordinary statement error does not.
    /// </summary>
    private async Task<ApiResponse<SnowflakeQueryResponse>> PostQueryWithRenewalAsync(
        QueryRequest request, bool describeOnly, AuthenticationToken authToken, CancellationToken cancellationToken)
    {
        ApiResponse<SnowflakeQueryResponse> response;
        try
        {
            response = await PostQueryCoreAsync(request, describeOnly, authToken, cancellationToken).ConfigureAwait(false);
            response = await WaitForQueryCompletionAsync(response, authToken, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The caller cancelled; the session itself is still good.
            throw;
        }
        catch
        {
            // A transport-level failure (network error, timeout, malformed response) or a failed
            // renewal: the session's state is unknown or unusable, so the pooled connection must
            // not be handed to the next caller.
            _onConnectionFault();
            throw;
        }

        // The session is fatally rejected: 390112 that couldn't be renewed (no master token, or the
        // renewed token was rejected again on retry), or 390114 (master token expired too).
        if (!IsSessionFatal(response)) 
            return response;
        
        _logger.LogDebug("Snowflake session is unrecoverable (code {Code}); faulting the connection.", response.Code);
        _onConnectionFault();

        return response;
    }

    /// <summary>
    /// Posts the query/describe request; on a session-expired response (390112) it renews the
    /// session token with the master token and retries once, rebuilding the request with a fresh
    /// request id so the retry is not treated as a duplicate of the rejected attempt.
    /// </summary>
    private async Task<ApiResponse<SnowflakeQueryResponse>> PostQueryCoreAsync(
        QueryRequest request, bool describeOnly, AuthenticationToken authToken, CancellationToken cancellationToken)
    {
        var body = BuildQueryRequest(request, out string endpoint, describeOnly);
        // The session token this attempt authenticates with; renewal only proceeds if it's still
        // current (so concurrent statements don't each renew after the same expiry).
        string? tokenUsed = authToken.SessionToken;
        var response = await _apiClient.PostAsync<SnowflakeQueryRequestBody, SnowflakeQueryResponse>(
            endpoint, body, authToken, cancellationToken).ConfigureAwait(false);

        if (!IsSessionExpired(response) || string.IsNullOrEmpty(authToken.MasterToken))
            return response;

        _logger.LogDebug("Snowflake session token expired (code {Code}); renewing and retrying.", response.Code);
        await RenewSessionCoreAsync(authToken, renewIfSessionTokenIs: tokenUsed, cancellationToken).ConfigureAwait(false);

        body = BuildQueryRequest(request, out endpoint, describeOnly);
        return await _apiClient.PostAsync<SnowflakeQueryRequestBody, SnowflakeQueryResponse>(
            endpoint, body, authToken, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Completes a query that outlived the synchronous response window. Snowflake then answers
    /// with a query-in-progress GS code and a <c>getResultUrl</c>; each GET on that URL
    /// long-polls until the server either finishes the query or hands out the next URL.
    /// Mirrors gosnowflake's ping-pong loop. A session token that expires while the query runs
    /// is renewed and the same URL re-polled.
    /// </summary>
    private async Task<ApiResponse<SnowflakeQueryResponse>> WaitForQueryCompletionAsync(
        ApiResponse<SnowflakeQueryResponse> response, AuthenticationToken authToken, CancellationToken cancellationToken)
    {
        while (IsQueryInProgress(response))
        {
            string? resultUrl = response.Data?.GetResultUrl;
            if (string.IsNullOrEmpty(resultUrl))
                throw new AdbcException("Query is in progress but the response carried no result URL to poll.");

            _logger.LogDebug("Query in progress (code {Code}); polling {ResultUrl}.", response.Code, resultUrl);

            string? tokenUsed = authToken.SessionToken;
            response = await _apiClient.GetAsync<SnowflakeQueryResponse>(
                $"{_accountUrl}{resultUrl}", authToken, cancellationToken).ConfigureAwait(false);

            if (IsSessionExpired(response) && !string.IsNullOrEmpty(authToken.MasterToken))
            {
                _logger.LogDebug("Session token expired while polling; renewing and re-polling.");
                await RenewSessionCoreAsync(authToken, renewIfSessionTokenIs: tokenUsed, cancellationToken).ConfigureAwait(false);
                response = await _apiClient.GetAsync<SnowflakeQueryResponse>(
                    $"{_accountUrl}{resultUrl}", authToken, cancellationToken).ConfigureAwait(false);
            }
        }

        return response;
    }

    /// <summary>
    /// True when a response reports the query is still executing server-side (GS codes 333333 /
    /// 333334) and the final result must be fetched from the response's <c>getResultUrl</c>.
    /// </summary>
    private static bool IsQueryInProgress(ApiResponse<SnowflakeQueryResponse> response) =>
        string.Equals(response.Code, QueryInProgressCode, StringComparison.Ordinal) ||
        string.Equals(response.Code, QueryInProgressAsyncCode, StringComparison.Ordinal);

    /// <summary>
    /// True when a response indicates the session token has expired (GS code 390112).
    /// </summary>
    internal static bool IsSessionExpired(ApiResponse<SnowflakeQueryResponse> response) =>
        response is { Success: false } && string.Equals(response.Code, SessionExpiredCode, StringComparison.Ordinal);

    /// <summary>
    /// True when a response indicates the session can no longer authenticate requests: the session
    /// token is expired (390112 — fatal here because renewal was either impossible or has already
    /// been attempted) or the master token is expired (390114).
    /// </summary>
    static bool IsSessionFatal(ApiResponse<SnowflakeQueryResponse> response) =>
        response is { Success: false } &&
        (string.Equals(response.Code, SessionExpiredCode, StringComparison.Ordinal) ||
         string.Equals(response.Code, MasterTokenExpiredCode, StringComparison.Ordinal));

    /// <summary>
    /// Renews an expired session token in place via <c>/session/token-request</c>. Snowflake issues
    /// a short-lived session token (~1h) backed by a longer master token (~4h); when the session
    /// token expires the still-valid master token mints a new one. The renewal request is itself
    /// authenticated with the master token.
    /// </summary>
    /// <inheritdoc/>
    public async Task HeartbeatAsync(AuthenticationToken authToken, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(authToken);

        var requestId = Guid.NewGuid().ToString();
        var requestGuid = Guid.NewGuid().ToString();
        var endpoint = $"{_accountUrl}{HeartbeatEndpoint}?requestId={requestId}&request_guid={requestGuid}";

        var response = await _apiClient.PostAsync<EmptyRequestBody, SnowflakeQueryResponse>(
            endpoint, EmptyRequestBody.Instance, authToken, cancellationToken).ConfigureAwait(false);

        // The heartbeat keeps the session alive; if the session token has already expired the
        // heartbeat itself comes back 390112, so renew with the master token.
        if (IsSessionExpired(response))
        {
            if (!string.IsNullOrEmpty(authToken.MasterToken))
                await RenewSessionAsync(authToken, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (!response.Success)
            throw new AdbcException($"Snowflake heartbeat failed (code {response.Code ?? "unknown"}).");
    }

    /// <inheritdoc/>
    public Task RenewSessionAsync(AuthenticationToken authToken, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(authToken);
        // Proactive renewal (e.g. heartbeat): renew unconditionally.
        return RenewSessionCoreAsync(authToken, renewIfSessionTokenIs: null, cancellationToken);
    }

    /// <summary>
    /// Renews the session token under a per-connection lock so concurrent statements serialize.
    /// When <paramref name="renewIfSessionTokenIs"/> is non-null, the renewal is skipped if the
    /// session token has already changed (another caller renewed it after the same expiry) — the
    /// caller then just retries with the current token. Mirrors gosnowflake's renewal guard.
    /// </summary>
    private async Task RenewSessionCoreAsync(AuthenticationToken authToken, string? renewIfSessionTokenIs, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(authToken.MasterToken))
            throw new AdbcException("Cannot renew the Snowflake session: no master token is available.");

        await _renewLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (renewIfSessionTokenIs != null && authToken.SessionToken != renewIfSessionTokenIs)
                return;

            var requestId = Guid.NewGuid().ToString();
            var requestGuid = Guid.NewGuid().ToString();
            var endpoint = $"{_accountUrl}{TokenRequestEndpoint}?requestId={requestId}&request_guid={requestGuid}";

            var body = new SnowflakeRenewSessionBody { OldSessionToken = authToken.SessionToken };
            // Authenticate the renewal with the master token by carrying it in the auth-header slot.
            var masterAuth = new AuthenticationToken { SessionToken = authToken.MasterToken };

            var response = await _apiClient.PostAsync<SnowflakeRenewSessionBody, SnowflakeRenewSessionData>(
                endpoint, body, masterAuth, cancellationToken).ConfigureAwait(false);

            if (!response.Success || string.IsNullOrEmpty(response.Data?.SessionToken))
            {
                // The server rejected the renewal, so the session cannot authenticate any further
                // requests — flag the pooled connection so it is discarded rather than reused.
                _onConnectionFault();
                throw new AdbcException($"Failed to renew the Snowflake session token (code {response.Code ?? "unknown"}).");
            }

            authToken.SessionToken = response.Data.SessionToken;
            if (!string.IsNullOrEmpty(response.Data.MasterToken))
                authToken.MasterToken = response.Data.MasterToken;
            // Renewal returns fresh session + master validities; roll both ceilings forward.
            if (response.Data.ValidityInSeconds > 0)
                authToken.ExpiresAt = DateTimeOffset.UtcNow.AddSeconds(response.Data.ValidityInSeconds);
            if (response.Data.MasterValidityInSeconds > 0)
                authToken.MasterExpiresAt = DateTimeOffset.UtcNow.AddSeconds(response.Data.MasterValidityInSeconds);
        }
        finally
        {
            _renewLock.Release();
        }
    }

    /// <inheritdoc/>
    public async Task CancelQueryAsync(string requestId, AuthenticationToken authToken, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(requestId);
        ArgumentNullException.ThrowIfNull(authToken);

        // The abort request carries its own fresh requestId/guid; the running query is identified
        // by the requestId echoed in the body.
        var abortRequestId = Guid.NewGuid().ToString();
        var requestGuid = Guid.NewGuid().ToString();
        var endpoint = $"{_accountUrl}{AbortEndpoint}?requestId={abortRequestId}&request_guid={requestGuid}";

        var body = RequestBuilder.BuildCancelRequest(requestId);
        var response = await _apiClient.PostAsync<SnowflakeCancelRequestBody, SnowflakeQueryResponse>(
            endpoint, body, authToken, cancellationToken).ConfigureAwait(false);

        // A successful abort returns success; if the query already finished there is simply nothing
        // to cancel. Surface other failures so a genuinely broken abort isn't silently swallowed.
        if (!response.Success)
            throw new AdbcException($"Failed to cancel the Snowflake query (code {response.Code ?? "unknown"}).");
    }

}
