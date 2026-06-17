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

using System.Text.Json.Serialization;
using AdbcDrivers.Snowflake.Native.Services.Query;

namespace AdbcDrivers.Snowflake.Native.Services.Transport;

/// <summary>
/// Source-generated serializer metadata for the query-protocol wire model (a small, closed set of
/// types), used by <see cref="RestApiClient"/>. Compile-time generation removes the reflection
/// warm-up and per-call metadata allocations of the default serializer and keeps this path
/// trimming/AOT-safe. The login path (<c>SnowflakeLoginClient</c>) intentionally stays on the
/// reflection-based web defaults — it runs once per connection and relies on case-insensitive
/// matching that these models don't.
/// </summary>
[JsonSerializable(typeof(ApiResponse<SnowflakeQueryResponse>))]
[JsonSerializable(typeof(ApiResponse<SnowflakeRenewSessionData>))]
[JsonSerializable(typeof(SnowflakeQueryRequestBody))]
[JsonSerializable(typeof(SnowflakeCancelRequestBody))]
[JsonSerializable(typeof(SnowflakeRenewSessionBody))]
[JsonSerializable(typeof(EmptyRequestBody))]
internal sealed partial class SnowflakeJsonContext : JsonSerializerContext;
