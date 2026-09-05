/*
* Copyright (c) 2025 ADBC Drivers Contributors
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
using AdbcDrivers.Snowflake.Native.Configuration;

namespace AdbcDrivers.Snowflake.Native.Services.ConnectionPool;

/// <summary>
/// Manages connection pooling for Snowflake connections.
/// </summary>
internal interface IConnectionPoolManager : IDisposable
{
    /// <summary>
    /// Acquires a connection from the pool.
    /// </summary>
    /// <param name="config">The connection configuration.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A pooled connection.</returns>
    Task<IPooledConnection> AcquireConnectionAsync(
        ConnectionConfig config,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Releases a connection back to the pool.
    /// </summary>
    /// <param name="connection">The connection to release.</param>
    void ReleaseConnection(IPooledConnection connection);
}
