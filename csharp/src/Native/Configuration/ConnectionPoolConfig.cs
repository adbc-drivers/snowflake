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
using System.ComponentModel.DataAnnotations;

namespace AdbcDrivers.Snowflake.Native.Configuration;

/// <summary>
/// Represents connection pool configuration parameters.
/// </summary>
internal class ConnectionPoolConfig
{
    /// <summary>
    /// Gets or sets the maximum number of connections in the pool.
    /// </summary>
    [Range(1, 100)]
    public int MaxPoolSize { get; set; } = 10;

    /// <summary>
    /// Gets or sets the maximum idle time before a connection is removed from the pool.
    /// </summary>
    public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Gets or sets the maximum lifetime of a connection in the pool.
    /// </summary>
    public TimeSpan MaxConnectionLifetime { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Gets or sets how long to wait for an available connection when the pool is at
    /// <see cref="MaxPoolSize"/> before failing, rather than blocking indefinitely. Defaults to
    /// 120 seconds (matching the Snowflake .NET connector's <c>waitingForIdleSessionTimeout</c>).
    /// </summary>
    public TimeSpan AcquireTimeout { get; set; } = TimeSpan.FromSeconds(120);
}
