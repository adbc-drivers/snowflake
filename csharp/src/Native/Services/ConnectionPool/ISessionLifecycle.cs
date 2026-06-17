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
using AdbcDrivers.Snowflake.Native.Configuration;
using AdbcDrivers.Snowflake.Native.Services.Authentication;

namespace AdbcDrivers.Snowflake.Native.Services.ConnectionPool;

/// <summary>
/// Server-side session operations the connection pool needs to maintain and tear down pooled
/// sessions, without the pool having to know about the transport/query layer.
/// </summary>
internal interface ISessionLifecycle
{
    /// <summary>
    /// Pings the session keep-alive endpoint for the given token so an idle session doesn't lapse.
    /// </summary>
    Task HeartbeatAsync(AuthenticationToken token, ConnectionConfig config, CancellationToken cancellationToken = default);

    /// <summary>
    /// Closes the server-side session for the given token so it isn't orphaned until it times out.
    /// </summary>
    Task CloseAsync(AuthenticationToken token, ConnectionConfig config, CancellationToken cancellationToken = default);
}
