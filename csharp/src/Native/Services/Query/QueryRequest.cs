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
using AdbcDrivers.Snowflake.Native.Services.Authentication;
using AdbcDrivers.Snowflake.Native.Services.Transport;

namespace AdbcDrivers.Snowflake.Native.Services.Query;

/// <summary>
/// Represents a query execution request.
/// </summary>
internal class QueryRequest
{
    /// <summary>
    /// Gets or sets the SQL statement to execute.
    /// </summary>
    public string Statement { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the database context for the query.
    /// </summary>
    public string? Database { get; set; }

    /// <summary>
    /// Gets or sets the schema context for the query.
    /// </summary>
    public string? Schema { get; set; }

    /// <summary>
    /// Gets or sets the warehouse to use for query execution.
    /// </summary>
    public string? Warehouse { get; set; }

    /// <summary>
    /// Gets or sets the role to use for query execution.
    /// </summary>
    public string? Role { get; set; }

    /// <summary>
    /// Gets or sets the query tag surfaced in the Snowsight query history for this statement.
    /// </summary>
    public string? QueryTag { get; set; }

    /// <summary>
    /// Gets or sets the query timeout.
    /// </summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets how many result-set chunks to download in parallel while streaming the result.
    /// </summary>
    public int PrefetchConcurrency { get; set; } = 10;

    /// <summary>
    /// Gets or sets the positional bind variables for the statement's '?' placeholders.
    /// </summary>
    public Dictionary<string, SnowflakeBinding> Bindings { get; set; } = new();

    /// <summary>
    /// Gets or sets a value indicating whether this is a multi-statement query.
    /// </summary>
    public bool IsMultiStatement { get; set; }

    /// <summary>
    /// Gets or sets the request id to submit the query with. When set, the same id can later be
    /// passed to <see cref="IQueryExecutor.CancelQueryAsync"/> to abort this specific request.
    /// When null, the executor generates one per attempt.
    /// </summary>
    public string? RequestId { get; set; }

    /// <summary>
    /// Gets or sets the authentication token for the request.
    /// </summary>
    public AuthenticationToken? AuthToken { get; set; }
}
