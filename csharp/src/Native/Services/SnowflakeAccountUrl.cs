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

namespace AdbcDrivers.Snowflake.Native.Services;

/// <summary>
/// Builds the base account URL for Snowflake API requests.
/// </summary>
internal static class SnowflakeAccountUrl
{
    /// <summary>
    /// Builds the HTTPS base URL for a Snowflake account.
    /// Handles privatelink accounts correctly by only treating the value as a
    /// full hostname if it already contains 'snowflakecomputing.com'.
    /// </summary>
    internal static string Build(string account)
    {
        return account.Contains("snowflakecomputing.com", System.StringComparison.OrdinalIgnoreCase)
            ? $"https://{account}"
            : $"https://{account}.snowflakecomputing.com";
    }

    /// <summary>
    /// Builds the base URL for a Snowflake account, using explicit host/port/protocol if provided.
    /// </summary>
    internal static string Build(string account, Configuration.NetworkConfig? network)
    {
        if (network != null && !string.IsNullOrEmpty(network.Host))
        {
            var port = network.Port != 443 ? $":{network.Port}" : string.Empty;
            return $"{network.Protocol}://{network.Host}{port}";
        }

        var protocol = network?.Protocol ?? "https";
        var host = account.Contains("snowflakecomputing.com", System.StringComparison.OrdinalIgnoreCase)
            ? account
            : $"{account}.snowflakecomputing.com";
        var portSuffix = (network != null && network.Port != 443) ? $":{network.Port}" : string.Empty;
        return $"{protocol}://{host}{portSuffix}";
    }
}
