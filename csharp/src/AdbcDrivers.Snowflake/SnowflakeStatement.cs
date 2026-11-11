/*
* Copyright (c) 2026 ADBC Drivers Contributors
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

using System;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Configuration;
using AdbcDrivers.Snowflake.Services.Authentication;
using AdbcDrivers.Snowflake.Services.ConnectionPool;
using AdbcDrivers.Snowflake.Services.Query;
using AdbcDrivers.Snowflake.Services.TypeConversion;

using Apache.Arrow;
using Apache.Arrow.Adbc;

// Disambiguate from AdbcDrivers.Snowflake.Services.Query.QueryResult.
using QueryResult = Apache.Arrow.Adbc.QueryResult;

namespace AdbcDrivers.Snowflake;

/// <summary>
/// Snowflake statement implementation for ADBC.
/// </summary>
public sealed class SnowflakeStatement : AdbcStatement
{
    internal const string QueryTagOption = SnowflakeParameters.QueryTag;

    private readonly ConnectionConfig _config;
    private readonly IPooledConnection _pooledConnection;
    private readonly IQueryExecutor _queryExecutor;
    private readonly ITypeConverter _typeConverter;
    private string? _queryTag;
    private RecordBatch? _boundParameters;
    // The request id of the in-flight query, set before each execution so Cancel (called from
    // another thread) can abort that specific request. Volatile for cross-thread visibility.
    private volatile string? _currentRequestId;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="SnowflakeStatement"/> class.
    /// </summary>
    /// <param name="config">The connection configuration.</param>
    /// <param name="pooledConnection">The pooled connection.</param>
    /// <param name="queryExecutor">The query executor.</param>
    internal SnowflakeStatement(
        ConnectionConfig config,
        IPooledConnection pooledConnection,
        IQueryExecutor queryExecutor)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _pooledConnection = pooledConnection ?? throw new ArgumentNullException(nameof(pooledConnection));
        _queryExecutor = queryExecutor ?? throw new ArgumentNullException(nameof(queryExecutor));
        _typeConverter = TypeConverter.Shared;
        _queryTag = config.QueryTag;
    }

    /// <summary>
    /// Sets a statement option. Supported: <see cref="QueryTagOption"/> — the query tag surfaced in
    /// the Snowsight query history; an empty value clears it.
    /// </summary>
    public override void SetOption(string key, string value)
    {
        ThrowIfDisposed();

        if (!string.Equals(key, QueryTagOption, StringComparison.Ordinal))
            throw AdbcException.NotImplemented($"Option '{key}' is not supported.");

        _queryTag = string.IsNullOrEmpty(value) ? null : value;
    }

    /// <summary>
    /// Binds parameters to the statement using a RecordBatch.
    /// </summary>
    /// <param name="batch">The RecordBatch containing parameter values.</param>
    /// <param name="schema">The schema of the RecordBatch.</param>
    public override void Bind(RecordBatch batch, Schema schema)
    {
        ThrowIfDisposed();

        ArgumentNullException.ThrowIfNull(batch);
        ArgumentNullException.ThrowIfNull(schema);

        if (ReferenceEquals(_boundParameters, batch))
            return;

        _boundParameters?.Dispose();
        _boundParameters = batch;
    }

    /// <summary>
    /// Executes the query and returns a QueryResult.
    /// </summary>
    /// <returns>A QueryResult containing the query results.</returns>
    public override QueryResult ExecuteQuery()
    {
        // Use async-first pattern: sync version calls async with proper blocking
        return ExecuteQueryAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Executes the query asynchronously and returns a QueryResult.
    /// </summary>
    /// <returns>A QueryResult containing the query results.</returns>
    public override ValueTask<QueryResult> ExecuteQueryAsync() => ExecuteQueryAsync(CancellationToken.None);

    /// <summary>
    /// Executes the query asynchronously and returns a QueryResult.
    /// </summary>
    /// <param name="cancellationToken">
    /// Cancels execution: the client stops waiting and a best-effort abort is sent to the server.
    /// The token covers execution only; once this returns, cancelling it does not affect reading
    /// the returned stream. Cancellation surfaces as <see cref="OperationCanceledException"/>.
    /// </param>
    /// <returns>A QueryResult containing the query results.</returns>
    public override async ValueTask<QueryResult> ExecuteQueryAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        if (string.IsNullOrWhiteSpace(SqlQuery))
            throw new InvalidOperationException("SQL query must be set before execution.");

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            var result = await ExecuteRequestAsync(cancellationToken).ConfigureAwait(false);

            if (result.Status == QueryStatus.Cancelled)
            {
                cancellationToken.ThrowIfCancellationRequested();
                throw new AdbcException("Query was cancelled.", AdbcStatusCode.Cancelled);
            }

            if (result.Status != QueryStatus.Success)
                throw ToAdbcException("Query failed", result);

            // Every Success shape from the executor carries a stream (unsupported response
            // shapes fail before reaching here).
            return new QueryResult(result.RowCount, result.ResultStream!);
        }
        catch (AdbcException)
        {
            throw;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new AdbcException($"Query execution failed: {ex.Message}", StatusOf(ex), ex);
        }
    }

    /// <summary>
    /// Executes an update query and returns the number of affected rows.
    /// </summary>
    /// <returns>An UpdateResult containing the number of affected rows.</returns>
    public override UpdateResult ExecuteUpdate()
    {
        // Use async-first pattern: sync version calls async with proper blocking
        return ExecuteUpdateAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Executes an update query asynchronously and returns the number of affected rows.
    /// </summary>
    /// <returns>An UpdateResult containing the number of affected rows.</returns>
    public override Task<UpdateResult> ExecuteUpdateAsync() => ExecuteUpdateAsync(CancellationToken.None);

    /// <summary>
    /// Executes an update query asynchronously and returns the number of affected rows.
    /// </summary>
    /// <param name="cancellationToken">
    /// Cancels execution: the client stops waiting and a best-effort abort is sent to the server.
    /// Cancellation surfaces as <see cref="OperationCanceledException"/>.
    /// </param>
    /// <returns>An UpdateResult containing the number of affected rows.</returns>
    public override async Task<UpdateResult> ExecuteUpdateAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        if (string.IsNullOrWhiteSpace(SqlQuery))
            throw new InvalidOperationException("SQL query must be set before execution.");

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            var result = await ExecuteRequestAsync(cancellationToken).ConfigureAwait(false);

            if (result.Status == QueryStatus.Cancelled)
            {
                cancellationToken.ThrowIfCancellationRequested();
                throw new AdbcException("Update was cancelled.", AdbcStatusCode.Cancelled);
            }

            if (result.Status != QueryStatus.Success)
                throw ToAdbcException("Update failed", result);

            // DML statements report the affected-row count parsed from the JSON row-count
            // summary in QueryExecutor. Any other statement (SELECT, DDL status rows, ...)
            // affects no rows, so report -1 (unknown/not applicable) per the ADBC contract.
            long affectedRows = result.AffectedRows ?? -1;

            // ExecuteUpdate has no use for the result set (DML/DDL surface theirs for
            // ExecuteQuery); release it rather than hold the batch until finalization.
            result.ResultStream?.Dispose();
            return new UpdateResult(affectedRows);
        }
        catch (AdbcException)
        {
            throw;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new AdbcException($"Update execution failed: {ex.Message}", StatusOf(ex), ex);
        }
    }

    /// <summary>
    /// Cancels the query currently executing on this statement, if any. Safe to call from a
    /// different thread than the one running the query (the typical use). No-ops when nothing is
    /// executing. The server-side abort is best-effort: a query that has already finished is not
    /// an error.
    /// </summary>
    public override void Cancel() => CancelAsync().GetAwaiter().GetResult();

    /// <summary>
    /// Asynchronous form of <see cref="Cancel"/>.
    /// </summary>
    public async Task CancelAsync()
    {
        ThrowIfDisposed();

        var requestId = _currentRequestId;
        if (string.IsNullOrEmpty(requestId))
            return;

        await _queryExecutor.CancelQueryAsync(requestId, _pooledConnection.AuthToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Builds the request for the current SQL and bindings and runs it. When the caller's token
    /// can be cancelled, cancellation also sends a best-effort server-side abort for this request
    /// (the executor's token alone only stops the client from waiting). The executor sees a
    /// token linked to the caller's and scoped to this call: the result stream links that token
    /// into its chunk prefetch, and the caller's token should not reach past execution.
    /// </summary>
    private async Task<Services.Query.QueryResult> ExecuteRequestAsync(CancellationToken cancellationToken)
    {
        var requestId = NewRequestId();
        var authToken = _pooledConnection.AuthToken;
        var request = new QueryRequest
        {
            Statement = SqlQuery!,
            Database = _config.Database,
            Schema = _config.Schema,
            Warehouse = _config.Warehouse,
            Role = _config.Role,
            QueryTag = _queryTag,
            Timeout = _config.QueryTimeout,
            PrefetchConcurrency = _config.PrefetchConcurrency,
            RequestId = requestId,
            AuthToken = authToken
        };

        if (_boundParameters != null)
        {
            var parameterSet = _typeConverter.ConvertArrowBatchToParameters(_boundParameters);
            foreach (var kvp in parameterSet.Parameters)
                request.Bindings[kvp.Key] = kvp.Value;
        }

        if (!cancellationToken.CanBeCanceled)
            return await _queryExecutor.ExecuteQueryAsync(request).ConfigureAwait(false);

        using var scope = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        Services.Query.QueryResult result;
        using (cancellationToken.Register(() => _ = AbortOnServerAsync(requestId, authToken)))
        {
            result = await _queryExecutor.ExecuteQueryAsync(request, scope.Token).ConfigureAwait(false);
        }

        // Unlink from the caller's token before deciding: once disposed (which waits out an
        // in-flight link callback), the scope can no longer become cancelled. A cancellation
        // that landed after the executor built the result stream, but before this point, has
        // already cancelled that stream's prefetch, so the result must not be handed out.
        scope.Dispose();
        if (scope.IsCancellationRequested)
        {
            result.ResultStream?.Dispose();
            throw new OperationCanceledException(cancellationToken);
        }
        return result;
    }

    // Long enough for a healthy abort round trip; bounds the request so a stalled response
    // can't keep the task and its connection alive indefinitely.
    internal static readonly TimeSpan AbortTimeout = TimeSpan.FromSeconds(30);

    // Fire-and-forget from a cancellation callback: never block the cancelling thread, and never
    // let a failed abort surface as an unobserved exception.
    private async Task AbortOnServerAsync(string requestId, AuthenticationToken authToken)
    {
        try
        {
            using var timeout = new CancellationTokenSource(AbortTimeout);
            await _queryExecutor.CancelQueryAsync(requestId, authToken, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Best-effort, like Cancel: the query may already have finished.
        }
    }

    // Generates and records the request id for the execution that is about to start, so a
    // concurrent Cancel can abort exactly this request.
    private string NewRequestId()
    {
        var requestId = Guid.NewGuid().ToString();
        _currentRequestId = requestId;
        return requestId;
    }

    /// <summary>
    /// Keeps the status an inner AdbcException already carried, so wrapping a failure at the
    /// statement boundary does not flatten an authentication or timeout error into UnknownError.
    /// </summary>
    private static AdbcStatusCode StatusOf(Exception ex) =>
        ex is AdbcException adbc ? adbc.Status : AdbcStatusCode.UnknownError;

    /// <summary>
    /// Builds the failure exception from a failed result, carrying the originating exception as the
    /// inner exception (when there was one) so the full stack survives instead of just its message.
    /// </summary>
    private static AdbcException ToAdbcException(string prefix, Services.Query.QueryResult result)
    {
        var errorMessages = result.Errors.Count > 0
            ? string.Join("; ", result.Errors.ConvertAll(e => $"[{e.ErrorCode}] {e.Message}"))
            : "Unknown error";
        var cause = result.Errors.Find(e => e.Exception != null)?.Exception;
        return cause is null
            ? new AdbcException($"{prefix}: {errorMessages}", AdbcStatusCode.UnknownError)
            : new AdbcException($"{prefix}: {errorMessages}", StatusOf(cause), cause);
    }

    /// <summary>
    /// Prepares the statement for execution.
    /// </summary>
    /// <remarks>
    /// Snowflake has no server-side prepare step -- statements are compiled when they are
    /// executed -- so this only validates that a query has been set and otherwise does nothing.
    /// </remarks>
    public override void Prepare()
    {
        ThrowIfDisposed();

        if (string.IsNullOrWhiteSpace(SqlQuery))
            throw new InvalidOperationException("SQL query must be set before preparation.");
    }

    /// <summary>
    /// Gets the parameter schema for a prepared statement.
    /// </summary>
    /// <remarks>
    /// Not supported: Snowflake's protocol does not report the types of a statement's bind
    /// parameters, so a parameter schema cannot be determined.
    /// </remarks>
    public override Schema GetParameterSchema()
    {
        ThrowIfDisposed();

        throw AdbcException.NotImplemented("Snowflake does not provide a parameter schema.");
    }

    /// <summary>
    /// Disposes the statement and releases any resources.
    /// </summary>
    public override void Dispose()
    {
        if (!_disposed)
        {
            _boundParameters?.Dispose();
            _boundParameters = null;

            _disposed = true;
        }
        base.Dispose();
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }
}
