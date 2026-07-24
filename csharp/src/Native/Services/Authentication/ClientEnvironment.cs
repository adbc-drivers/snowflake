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
using System.Runtime.InteropServices;
using System.Text.Json.Serialization;

namespace AdbcDrivers.Snowflake.Native.Services.Authentication;

/// <summary>
/// Represents client environment information for Snowflake authentication.
/// </summary>
internal class ClientEnvironment
{
    /// <summary>
    /// Gets or sets the application name.
    /// </summary>
    [JsonPropertyName("APPLICATION")]
    public string APPLICATION { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the operating system version.
    /// </summary>
    [JsonPropertyName("OS_VERSION")]
    public string OS_VERSION { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the .NET runtime identifier.
    /// </summary>
    [JsonPropertyName("NET_RUNTIME")]
    public string NET_RUNTIME { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the .NET version.
    /// </summary>
    [JsonPropertyName("NET_VERSION")]
    public string NET_VERSION { get; set; } = string.Empty;

    /// <summary>
    /// Creates a ClientEnvironment instance with system information.
    /// </summary>
    /// <returns>A populated ClientEnvironment instance.</returns>
    public static ClientEnvironment Create()
    {
        return new ClientEnvironment
        {
            APPLICATION = "ADBC",
            OS_VERSION = RuntimeInformation.OSDescription,
            NET_RUNTIME = RuntimeInformation.FrameworkDescription,
            NET_VERSION = Environment.Version.ToString()
        };
    }
}
