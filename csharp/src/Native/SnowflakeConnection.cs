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
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Apache.Arrow.Ipc;
using AdbcDrivers.Snowflake.Native.Configuration;
using AdbcDrivers.Snowflake.Native.Services.ConnectionPool;
using AdbcDrivers.Snowflake.Native.Services.Transport;
using AdbcDrivers.Snowflake.Native.Services.TypeConversion;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native.Services.Query;

using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Types;

namespace AdbcDrivers.Snowflake.Native;

/// <summary>
/// Snowflake connection implementation for ADBC.
/// </summary>
public sealed partial class SnowflakeConnection : AdbcConnection
{
    private readonly ConnectionConfig _config;
    private readonly IConnectionPoolManager _connectionPool;
    private IPooledConnection? _pooledConnection;
    private readonly IQueryExecutor? _queryExecutor;
    private bool _disposed;
    private bool _autocommit = true;
    private readonly ILogger<SnowflakeConnection> _logger;

    internal SnowflakeConnection(ConnectionConfig config, IConnectionPoolManager connectionPool,
        IPooledConnection pooledConnection, IQueryExecutor queryExecutor,
        ILogger<SnowflakeConnection> logger)
    {
        _config = config;
        _connectionPool = connectionPool;
        _pooledConnection = pooledConnection;
        _queryExecutor = queryExecutor;
        _logger = logger;
    }

    /// <summary>
    /// Asynchronously creates and initializes a new SnowflakeConnection. The token cancels the
    /// wait for pool capacity and the login round trip.
    /// </summary>
    internal static async Task<SnowflakeConnection> CreateAsync(ConnectionConfig config, HttpClient httpClient, IConnectionPoolManager connectionPool, ILoggerFactory? loggerFactory = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(connectionPool);
        loggerFactory ??= NullLoggerFactory.Instance;
        var log = loggerFactory.CreateLogger<SnowflakeConnection>();

        log.LogDebug("Acquiring pooled connection for user {User} account {Account}", config.User, config.Account);
        var pooledConnection = await connectionPool.AcquireConnectionAsync(config, cancellationToken).ConfigureAwait(false);
        if (pooledConnection is null)
        {
            throw new AdbcException("Failed to acquire pooled connection.");
        }
        log.LogInformation("Acquired pooled connection {ConnectionId}", pooledConnection.ConnectionId);

        var apiClient = new RestApiClient(httpClient, config.EnableCompression,
            logger: loggerFactory.CreateLogger<RestApiClient>());
        var typeConverter = TypeConverter.Shared;
        
        var queryExecutor = new QueryExecutor(apiClient, typeConverter, config.Account, config.Network,
            loggerFactory.CreateLogger<QueryExecutor>(), () => pooledConnection.IsFaulted = true);

        return new SnowflakeConnection(config, connectionPool, pooledConnection, queryExecutor, log);
    }

    /// <summary>
    /// Creates a new statement for executing queries.
    /// </summary>
    /// <returns>An AdbcStatement instance.</returns>
    public override AdbcStatement CreateStatement()
    {
        ThrowIfDisposed();

        if (_pooledConnection == null || _queryExecutor == null)
            throw new AdbcException("Connection is not properly initialized.");

        return new SnowflakeStatement(_config, _pooledConnection, _queryExecutor);
    }

    /// <summary>
    /// The session's authentication token (session/master tokens). Exposed for tests and proactive
    /// session management (e.g. heartbeat).
    /// </summary>
    internal Services.Authentication.AuthenticationToken? AuthToken => _pooledConnection?.AuthToken;

    /// <summary>
    /// Proactively renews this connection's session token using the master token.
    /// </summary>
    internal Task RenewSessionAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        if (_pooledConnection == null || _queryExecutor == null)
            throw new AdbcException("Connection is not properly initialized.");

        return _queryExecutor.RenewSessionAsync(_pooledConnection.AuthToken, cancellationToken);
    }

    /// <summary>
    /// Pings the session heartbeat endpoint to keep this connection's session alive.
    /// </summary>
    internal Task HeartbeatAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        if (_pooledConnection == null || _queryExecutor == null)
            throw new AdbcException("Connection is not properly initialized.");

        return _queryExecutor.HeartbeatAsync(_pooledConnection.AuthToken, cancellationToken);
    }


    /// <summary>
    /// The table types reported by the Snowflake driver. Matches the Go driver's
    /// ListTableTypes (TABLE, VIEW).
    /// </summary>
    private static readonly string[] SnowflakeTableTypes = ["TABLE", "VIEW"];

    /// <summary>
    /// Gets the supported table types.
    /// </summary>
    /// <returns>An IArrowArrayStream containing the table types.</returns>
    public override IArrowArrayStream GetTableTypes()
    {
        ThrowIfDisposed();

        var tableTypesBuilder = new StringArray.Builder();
        tableTypesBuilder.AppendRange(SnowflakeTableTypes);

        IArrowArray[] dataArrays = [tableTypesBuilder.Build()];

        return new InMemoryArrowStream(StandardSchemas.TableTypesSchema, dataArrays);
    }

    /// <summary>
    /// Gets the Arrow schema for a specific table.
    /// </summary>
    /// <param name="catalog">The catalog name (database).</param>
    /// <param name="dbSchema">The schema name.</param>
    /// <param name="tableName">The table name.</param>
    /// <returns>The Arrow schema for the table.</returns>
    public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName)
    {
        ThrowIfDisposed();

        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        if (_queryExecutor == null || _pooledConnection == null)
            throw new AdbcException("Connection is not properly initialized.");

        var parts = new List<string>(3);
        if (!string.IsNullOrEmpty(catalog))
            parts.Add(QuoteIdentifier(catalog));
        if (!string.IsNullOrEmpty(dbSchema))
            parts.Add(QuoteIdentifier(dbSchema));
        parts.Add(QuoteIdentifier(tableName));
        string fullyQualifiedTable = string.Join(".", parts);

        // describeOnly compiles "SELECT * FROM <table>" and returns the column metadata
        // (rowtype) without executing, so this requires no warehouse and fetches no rows.
        var request = new QueryRequest
        {
            Statement = $"SELECT * FROM {fullyQualifiedTable}",
            Warehouse = _config.Warehouse,
            Role = _config.Role,
            Timeout = _config.QueryTimeout,
            AuthToken = _pooledConnection.AuthToken
        };
        
        PreparedStatement prepared = _queryExecutor.DescribeAsync(request).GetAwaiter().GetResult();

        return prepared.ResultSchema
            ?? throw new AdbcException($"Unable to determine schema for table '{tableName}'.");
    }

    /// <summary>
    /// Quotes a Snowflake identifier (matches the Go driver's quoteIdentifier).
    /// </summary>
    private static string QuoteIdentifier(string identifier) =>
        "\"" + identifier.Replace("\"", "\"\"") + "\"";

    /// <summary>The driver name reported by GetInfo.</summary>
    private const string InfoDriverName = "ADBC Snowflake Driver";

    /// <summary>The database vendor name reported by GetInfo.</summary>
    private const string InfoVendorName = "Snowflake";

    private static readonly string InfoDriverVersion =
        typeof(SnowflakeConnection).Assembly.GetName().Version?.ToString() ?? "0.0.0";

    private static readonly string InfoDriverArrowVersion =
        typeof(IArrowArray).Assembly.GetName().Version?.ToString() ?? "0.0.0";

    private static readonly AdbcInfoCode[] InfoSupportedCodes =
    [
        AdbcInfoCode.VendorName,
        AdbcInfoCode.DriverName,
        AdbcInfoCode.DriverVersion,
        AdbcInfoCode.DriverArrowVersion
    ];

    /// <summary>
    /// Gets metadata about the driver and database.
    /// </summary>
    /// <param name="codes">The info codes to fetch; if empty, all supported codes are returned.</param>
    /// <returns>An IArrowArrayStream of (info_name, info_value) rows.</returns>
    public override IArrowArrayStream GetInfo(IReadOnlyList<AdbcInfoCode> codes)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(codes);

        if (codes.Count == 0)
            codes = InfoSupportedCodes;

        const int stringValueTypeId = 0;

        var infoUnionType = new UnionType(
            [
                new Field("string_value", StringType.Default, true),
                new Field("bool_value", BooleanType.Default, true),
                new Field("int64_value", Int64Type.Default, true),
                new Field("int32_bitmask", Int32Type.Default, true),
                new Field("string_list", new ListType(new Field("item", StringType.Default, true)), false),
                new Field("int32_to_int32_list_map",
                    new ListType(new Field("entries", new StructType([
                        new Field("key", Int32Type.Default, false),
                        new Field("value", Int32Type.Default, true)
                    ]), false)),
                    true)
            ],
            [0, 1, 2, 3, 4, 5],
            UnionMode.Dense);

        var infoNameBuilder = new UInt32Array.Builder();
        var typeBuilder = new ArrowBuffer.Builder<byte>();
        var offsetBuilder = new ArrowBuffer.Builder<int>();
        var stringValueBuilder = new StringArray.Builder();
        int nullCount = 0;

        foreach (AdbcInfoCode code in codes)
        {
            infoNameBuilder.Append((uint)code);
            typeBuilder.Append((byte)stringValueTypeId);
            offsetBuilder.Append(stringValueBuilder.Length);

            string? value = code switch
            {
                AdbcInfoCode.DriverName => InfoDriverName,
                AdbcInfoCode.DriverVersion => InfoDriverVersion,
                AdbcInfoCode.DriverArrowVersion => InfoDriverArrowVersion,
                AdbcInfoCode.VendorName => InfoVendorName,
                _ => null
            };

            if (value is null)
            {
                stringValueBuilder.AppendNull();
                nullCount++;
            }
            else
            {
                stringValueBuilder.Append(value);
            }
        }

        var entryType = new StructType([
            new Field("key", Int32Type.Default, false),
            new Field("value", Int32Type.Default, true)
        ]);

        var entriesDataArray = new StructArray(
            entryType,
            0,
            [new Int32Array.Builder().Build(), new Int32Array.Builder().Build()],
            new ArrowBuffer.BitmapBuilder().Build());

        IArrowArray[] childArrays =
        [
            stringValueBuilder.Build(),
            new BooleanArray.Builder().Build(),
            new Int64Array.Builder().Build(),
            new Int32Array.Builder().Build(),
            new ListArray.Builder(StringType.Default).Build(),
            new List<IArrowArray?> { entriesDataArray }.BuildListArrayForType(entryType)
        ];

        var infoValue = new DenseUnionArray(
            infoUnionType,
            codes.Count,
            childArrays,
            typeBuilder.Build(),
            offsetBuilder.Build(),
            nullCount);

        IArrowArray[] dataArrays =
        [
            infoNameBuilder.Build(),
            infoValue
        ];

        return new InMemoryArrowStream(StandardSchemas.GetInfoSchema, dataArrays);
    }

    /// <summary>
    /// Sets a connection option. Supported: <see cref="AdbcOptions.Connection.Autocommit"/>.
    /// Snowflake sessions default to autocommit on; disabling it opens a transaction scope
    /// that <see cref="Commit"/> / <see cref="Rollback"/> end. Re-enabling autocommit first
    /// commits any pending work (the ADBC contract).
    /// </summary>
    public override void SetOption(string key, string value)
    {
        ThrowIfDisposed();

        if (!string.Equals(key, AdbcOptions.Connection.Autocommit, StringComparison.Ordinal))
            throw AdbcException.NotImplemented($"Option '{key}' is not supported.");

        bool enable = AdbcOptions.GetEnabled(value);
        if (enable == _autocommit)
            return;

        if (enable)
            ExecuteSessionStatement("COMMIT");

        ExecuteSessionStatement($"ALTER SESSION SET AUTOCOMMIT = {(enable ? "TRUE" : "FALSE")}");
        _autocommit = enable;
    }

    /// <summary>
    /// Commits the current transaction. Valid only while autocommit is disabled.
    /// </summary>
    public override void Commit()
    {
        ThrowIfDisposed();
        ThrowIfAutocommit();
        ExecuteSessionStatement("COMMIT");
    }

    /// <summary>
    /// Rolls back the current transaction. Valid only while autocommit is disabled.
    /// </summary>
    public override void Rollback()
    {
        ThrowIfDisposed();
        ThrowIfAutocommit();
        ExecuteSessionStatement("ROLLBACK");
    }

    private void ThrowIfAutocommit()
    {
        if (_autocommit)
            throw new AdbcException(
                $"No transaction is in progress: autocommit is enabled. Disable {AdbcOptions.Connection.Autocommit} first.");
    }

    /// <summary>
    /// Runs a session-scoped statement (COMMIT/ROLLBACK/ALTER SESSION) and throws on failure.
    /// Sync-over-async at the ADBC boundary (SetOption/Commit/Rollback have no async forms);
    /// safe to block — the async core awaits with ConfigureAwait(false) throughout.
    /// </summary>
    private void ExecuteSessionStatement(string sql)
    {
        if (_pooledConnection == null || _queryExecutor == null)
            throw new AdbcException("Connection is not properly initialized.");

        var request = new QueryRequest
        {
            Statement = sql,
            Timeout = _config.QueryTimeout,
            AuthToken = _pooledConnection.AuthToken
        };

        var result = _queryExecutor.ExecuteQueryAsync(request).GetAwaiter().GetResult();
        result.ResultStream?.Dispose();

        if (result.Status != QueryStatus.Success)
        {
            string message = result.Errors.Count > 0 ? result.Errors[0].Message : "Unknown error";
            throw new AdbcException($"'{sql}' failed: {message}");
        }
    }

    /// <summary>
    /// Disposes the connection and releases any resources.
    /// </summary>
    public override void Dispose()
    {
        if (!_disposed)
        {
            if (_pooledConnection != null)
            {
                if (!_autocommit)
                    ResetTransactionStateBestEffort();

                _connectionPool.ReleaseConnection(_pooledConnection);
                _pooledConnection = null;
            }
            _logger.LogDebug("Disposing SnowflakeConnection for account {Account}", _config.Account);
            _disposed = true;
        }
        base.Dispose();
    }

    /// <summary>
    /// A connection released mid-transaction must not hand uncommitted work — or a session
    /// stuck in AUTOCOMMIT=FALSE — to the pool's next borrower: roll back and restore
    /// autocommit, and discard the connection if that fails (matching gosnowflake's
    /// release behavior).
    /// </summary>
    private void ResetTransactionStateBestEffort()
    {
        try
        {
            ExecuteSessionStatement("ROLLBACK");
            ExecuteSessionStatement("ALTER SESSION SET AUTOCOMMIT = TRUE");
            _autocommit = true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to reset transaction state on release; discarding the pooled connection.");
            _pooledConnection!.IsFaulted = true;
        }
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }
}
