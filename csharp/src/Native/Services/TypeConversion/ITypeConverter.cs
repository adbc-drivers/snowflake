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

using Apache.Arrow.Types;

using Apache.Arrow;

namespace AdbcDrivers.Snowflake.Native.Services.TypeConversion;

/// <summary>
/// Converts between Snowflake and Arrow data types.
/// </summary>
internal interface ITypeConverter
{
    /// <summary>
    /// Converts a Snowflake data type to an Arrow type.
    /// </summary>
    /// <param name="snowflakeType">The Snowflake data type.</param>
    /// <returns>The corresponding Arrow type.</returns>
    IArrowType ConvertSnowflakeTypeToArrow(SnowflakeDataType snowflakeType);

    /// <summary>
    /// Converts an Arrow record batch to Snowflake parameter bindings.
    /// </summary>
    /// <param name="batch">The Arrow record batch.</param>
    /// <returns>A parameter set for Snowflake query execution.</returns>
    ParameterSet ConvertArrowBatchToParameters(RecordBatch batch);
}
