/*
* Copyright (c) 2026 ADBC Drivers Contributors
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

namespace AdbcDrivers.Snowflake.Native.Services;

/// <summary>
/// Builds the base account URL for Snowflake API requests.
/// </summary>
internal static class SnowflakeAccountUrl
{
    const string DefaultDomain = ".snowflakecomputing.com";
    const string ChinaDomain = ".snowflakecomputing.cn";
    const int DefaultPort = 443;

    /// <summary>
    /// Builds the base URL for a Snowflake account. An explicit host override is used as-is;
    /// otherwise the host is derived from the account identifier, which carries its region and
    /// cloud suffix (for example xy12345.us-east-1.aws).
    /// </summary>
    internal static string Build(string account, Configuration.NetworkConfig? network)
    {
        var host = network?.Host;
        if (string.IsNullOrEmpty(host))
            host = BuildHost(account, network?.Region);

        var protocol = network?.Protocol ?? "https";
        var port = network != null && network.Port != DefaultPort ? $":{network.Port}" : string.Empty;

        return $"{protocol}://{host}{port}";
    }

    /// <summary>
    /// The region reaches us either as a suffix on the account identifier (xy12345.us-east-1) or
    /// as a separate parameter; the parser rejects configurations that supply both.
    /// </summary>
    static string BuildHost(string account, string? region)
    {
        if (!string.IsNullOrEmpty(region))
            return $"{account}.{region}{DomainFor(region)}";

        int regionSeparator = account.IndexOf('.');
        var accountRegion = regionSeparator > 0 ? account[(regionSeparator + 1)..] : string.Empty;

        return $"{account}{DomainFor(accountRegion)}";
    }

    /// <summary>
    /// China accounts live under their own top-level domain, identified by the cn- region prefix
    /// (for example xy12345.cn-north-1). Every other region uses the default domain.
    /// </summary>
    static string DomainFor(string region) =>
        region.StartsWith("cn-", StringComparison.OrdinalIgnoreCase) ? ChinaDomain : DefaultDomain;
}
