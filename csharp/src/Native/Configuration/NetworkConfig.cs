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

namespace AdbcDrivers.Snowflake.Native.Configuration;

/// <summary>
/// Network configuration for HTTP transport (host override, proxy settings).
/// </summary>
internal class NetworkConfig
{
    /// <summary>Explicit host override. When set, used directly instead of deriving from account.</summary>
    public string? Host { get; set; }

    /// <summary>Port override. Default is 443.</summary>
    public int Port { get; set; } = 443;

    /// <summary>Protocol (https or http). Default is https.</summary>
    public string Protocol { get; set; } = "https";

    /// <summary>When true, explicitly disables all proxy usage (ignores system proxy settings).</summary>
    public bool NoProxy { get; set; }

    /// <summary>Whether to skip TLS certificate verification.</summary>
    public bool TlsSkipVerify { get; set; }
}
