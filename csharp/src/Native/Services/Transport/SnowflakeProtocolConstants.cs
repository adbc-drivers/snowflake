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

namespace AdbcDrivers.Snowflake.Native.Services.Transport;

/// <summary>
/// Names of the session-level settings sent in the <c>parameters</c> map of a query request.
/// </summary>
internal static class SessionParameterNames
{
    public const string Database = "DATABASE";
    public const string Schema = "SCHEMA";
    public const string Warehouse = "WAREHOUSE";
    public const string Role = "ROLE";
    public const string StatementTimeoutInSeconds = "STATEMENT_TIMEOUT_IN_SECONDS";
    public const string QueryResultFormat = "DOTNET_QUERY_RESULT_FORMAT";
    public const string MultiStatementCount = "MULTI_STATEMENT_COUNT";
    public const string QueryTag = "QUERY_TAG";
}

/// <summary>
/// Fixed values used for session parameters.
/// </summary>
internal static class SessionParameterValues
{
    /// <summary>Requests query results in Apache Arrow format.</summary>
    public const string ArrowResultFormat = "ARROW";

    /// <summary>Allows a variable number of statements in a multi-statement request.</summary>
    public const string VariableStatementCount = "0";
}

/// <summary>
/// Snowflake data-type names used when binding parameter values.
/// </summary>
internal static class BindTypeNames
{
    public const string Text = "TEXT";
    public const string Fixed = "FIXED";
    public const string Real = "REAL";
    public const string Boolean = "BOOLEAN";
    public const string Date = "DATE";
    public const string Time = "TIME";
    public const string TimestampNtz = "TIMESTAMP_NTZ";
    public const string TimestampLtz = "TIMESTAMP_LTZ";
    public const string Binary = "BINARY";
}
