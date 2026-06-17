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

namespace AdbcDrivers.Snowflake.Native.Services.Authentication;

/// <summary>
/// Represents an authentication token for Snowflake connections.
/// </summary>
internal class AuthenticationToken
{
    /// <summary>
    /// Gets or sets when the <b>session</b> token expires (~1h). Once past, a query gets GS
    /// <c>390112</c> and the session is renewed from the master token; so this is informational,
    /// not a hard wall.
    /// </summary>
    public DateTimeOffset ExpiresAt { get; set; }

    /// <summary>
    /// Gets or sets when the <b>master</b> token expires (~4h) — the point past which the connection
    /// is beyond recovery (renewal itself fails, GS <c>390114</c>). This is what pool eviction keys on:
    /// a session-expired-but-master-alive connection is still usable via renewal.
    /// </summary>
    public DateTimeOffset MasterExpiresAt { get; set; }

    /// <summary>
    /// Gets or sets the session token (if available).
    /// </summary>
    public string? SessionToken { get; set; }

    /// <summary>
    /// Gets or sets the master token (if available).
    /// </summary>
    public string? MasterToken { get; set; }

    /// <summary>
    /// Gets or sets the session ID.
    /// </summary>
    public string? SessionId { get; set; }
}
