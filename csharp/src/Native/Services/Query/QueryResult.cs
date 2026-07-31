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
using Apache.Arrow.Ipc;

namespace AdbcDrivers.Snowflake.Native.Services.Query;

/// <summary>
/// Represents the result of a query execution.
/// </summary>
internal class QueryResult
{
    /// <summary>
    /// Gets or sets the query execution status.
    /// </summary>
    public QueryStatus Status { get; set; }

    /// <summary>
    /// Gets or sets the Arrow array stream containing the results.
    /// </summary>
    public IArrowArrayStream? ResultStream { get; set; }

    /// <summary>
    /// Gets or sets the number of rows affected or returned.
    /// </summary>
    public long RowCount { get; set; }

    /// <summary>
    /// Gets or sets the affected-row count parsed from a DML statement's row-count summary,
    /// or null when the statement was not DML. Kept separate from <see cref="RowCount"/> and
    /// <see cref="ResultStream"/> so a DML result can carry both its summary result set (for
    /// ExecuteQuery) and the count (for ExecuteUpdate).
    /// </summary>
    public long? AffectedRows { get; set; }

    /// <summary>
    /// Gets or sets any errors that occurred during execution.
    /// </summary>
    public List<QueryError> Errors { get; set; } = [];

    /// <summary>Creates a successful result carrying a result-set stream.</summary>
    /// <param name="resultStream">The Arrow stream with the result set.</param>
    /// <param name="rowCount">The row count to report (returned rows, or the affected count for DML).</param>
    /// <param name="affectedRows">The DML affected-row count; null for non-DML statements.</param>
    public static QueryResult Success(IArrowArrayStream resultStream, long rowCount, long? affectedRows = null) =>
        new()
        {
            Status = QueryStatus.Success,
            ResultStream = resultStream,
            RowCount = rowCount,
            AffectedRows = affectedRows
        };

    /// <summary>Creates a failed result with a single error.</summary>
    public static QueryResult Failed(string errorCode, string message, Exception? exception = null) =>
        new()
        {
            Status = QueryStatus.Failed,
            Errors =
            [
                new QueryError
                {
                    ErrorCode = errorCode,
                    Message = message,
                    Exception = exception
                }
            ]
        };

    /// <summary>Creates a cancelled result (no stream, no errors).</summary>
    public static QueryResult Cancelled() =>
        new()
        {
            Status = QueryStatus.Cancelled
        };
}
