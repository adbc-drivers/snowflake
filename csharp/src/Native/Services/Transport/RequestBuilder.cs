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

namespace AdbcDrivers.Snowflake.Native.Services.Transport;

/// <summary>
/// Builds request bodies for the Snowflake query API.
/// </summary>
internal static class RequestBuilder
{
    /// <summary>
    /// Builds a query execution request.
    /// </summary>
    /// <param name="statement">The SQL statement to execute.</param>
    /// <param name="database">The database name (optional).</param>
    /// <param name="schema">The schema name (optional).</param>
    /// <param name="warehouse">The warehouse name (optional).</param>
    /// <param name="role">The role name (optional).</param>
    /// <param name="queryTag">The query tag surfaced in the Snowsight query history (optional).</param>
    /// <param name="timeout">The query timeout in seconds (optional).</param>
    /// <param name="bindings">Positional bind variables (optional).</param>
    /// <param name="isMultiStatement">Whether this is a multi-statement query.</param>
    /// <param name="describeOnly">Whether to only describe (compile) the statement and return its metadata without executing it.</param>
    /// <returns>A query execution request body.</returns>
    public static SnowflakeQueryRequestBody BuildQueryRequest(
        string statement,
        string? database = null,
        string? schema = null,
        string? warehouse = null,
        string? role = null,
        string? queryTag = null,
        int? timeout = null,
        Dictionary<string, SnowflakeBinding>? bindings = null,
        bool isMultiStatement = false,
        bool describeOnly = false)
    {
        if (string.IsNullOrEmpty(statement))
            throw new ArgumentException("Statement cannot be null or empty.", nameof(statement));

        var sessionParams = new Dictionary<string, string>();

        if (!string.IsNullOrEmpty(database))
            sessionParams[SessionParameterNames.Database] = database;

        if (!string.IsNullOrEmpty(schema))
            sessionParams[SessionParameterNames.Schema] = schema;

        if (!string.IsNullOrEmpty(warehouse))
            sessionParams[SessionParameterNames.Warehouse] = warehouse;

        if (!string.IsNullOrEmpty(role))
            sessionParams[SessionParameterNames.Role] = role;

        if (!string.IsNullOrEmpty(queryTag))
            sessionParams[SessionParameterNames.QueryTag] = queryTag;

        if (timeout.HasValue && timeout.Value > 0)
            sessionParams[SessionParameterNames.StatementTimeoutInSeconds] = timeout.Value.ToString(CultureInfo.InvariantCulture);

        sessionParams[SessionParameterNames.QueryResultFormat] = SessionParameterValues.ArrowResultFormat;

        if (isMultiStatement)
            sessionParams[SessionParameterNames.MultiStatementCount] = SessionParameterValues.VariableStatementCount;

        return new SnowflakeQueryRequestBody
        {
            SqlText = statement,
            AsyncExec = false,
            DescribeOnly = describeOnly,
            Parameters = sessionParams,
            Bindings = bindings is { Count: > 0 } ? bindings : null,
        };
    }

    /// <summary>
    /// Builds a query cancellation request.
    /// </summary>
    /// <param name="requestId">The request id the query was submitted with.</param>
    /// <returns>A query cancellation request body.</returns>
    public static SnowflakeCancelRequestBody BuildCancelRequest(string requestId)
    {
        return string.IsNullOrEmpty(requestId)
            ? throw new ArgumentException("Request id cannot be null or empty.", nameof(requestId))
            : new SnowflakeCancelRequestBody { RequestId = requestId };
    }

}
