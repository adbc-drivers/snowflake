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

using System.Collections.Generic;
using AdbcDrivers.Snowflake.Native.Services.Transport;

namespace AdbcDrivers.Snowflake.Native.Services.TypeConversion;

/// <summary>
/// Represents a set of positional bind variables for Snowflake query execution, keyed by
/// 1-based placeholder position ("1", "2", ...) to match the '?' placeholders in the SQL.
/// </summary>
internal class ParameterSet
{
    /// <summary>
    /// Gets or sets the bind variables, keyed by 1-based placeholder position.
    /// </summary>
    public Dictionary<string, SnowflakeBinding> Parameters { get; init; } = new();
}
