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
using System.Collections.Generic;
using System.Globalization;
using AdbcDrivers.Snowflake.Native.Services.Transport;
using Apache.Arrow.Types;

using Apache.Arrow;

namespace AdbcDrivers.Snowflake.Native.Services.TypeConversion;

/// <summary>
/// Implements type conversion between Snowflake and Arrow formats.
/// </summary>
internal class TypeConverter : ITypeConverter
{
    /// <summary>Shared instance — the converter is stateless, so one serves every consumer.</summary>
    internal static TypeConverter Shared { get; } = new();

    /// <inheritdoc/>
    public IArrowType ConvertSnowflakeTypeToArrow(SnowflakeDataType snowflakeType)
    {
        ArgumentNullException.ThrowIfNull(snowflakeType);

        return snowflakeType.TypeCode switch
        {
            SnowflakeTypeCode.Boolean => BooleanType.Default,

            // INTEGER/INT/BIGINT/SMALLINT/TINYINT/BYTEINT are all NUMBER(38,0) in Snowflake, so
            // they size by precision the same way FIXED/NUMBER does.
            SnowflakeTypeCode.Integer or
            SnowflakeTypeCode.Number => FixedToArrowType(
                snowflakeType.Precision.GetValueOrDefault(38),
                snowflakeType.Scale.GetValueOrDefault(0)),

            SnowflakeTypeCode.Float => FloatType.Default,

            SnowflakeTypeCode.Double => DoubleType.Default,

            SnowflakeTypeCode.Varchar => StringType.Default,

            SnowflakeTypeCode.Binary => BinaryType.Default,

            SnowflakeTypeCode.Date => Date32Type.Default,

            SnowflakeTypeCode.Time => TimeType.Nanosecond,

            SnowflakeTypeCode.Timestamp or
            SnowflakeTypeCode.TimestampNtz => new TimestampType(TimeUnit.Nanosecond, timezone: (string?)null),

            SnowflakeTypeCode.TimestampLtz => new TimestampType(TimeUnit.Nanosecond, timezone: "UTC"),

            // The result decoder stores TIMESTAMP_TZ as its UTC instant (a single Arrow column
            // cannot carry a per-row offset), so the described type matches: Timestamp[ns] "UTC".
            SnowflakeTypeCode.TimestampTz => new TimestampType(TimeUnit.Nanosecond, timezone: "UTC"),

            SnowflakeTypeCode.Variant or
            SnowflakeTypeCode.Object => StringType.Default, // JSON as string

            SnowflakeTypeCode.Array => new ListType(StringType.Default), // Array of JSON strings

            SnowflakeTypeCode.Geography or
            SnowflakeTypeCode.Geometry => StringType.Default, // GeoJSON as string

            _ => throw new NotSupportedException($"Snowflake type {snowflakeType.TypeName} is not supported.")
        };
    }

    // Largest decimal precision guaranteed to fit each integer width (Int32 holds 9 full digits,
    // Int64 holds 18).
    private const int MaxFixedInt32Precision = 9;
    private const int MaxFixedInt64Precision = 18;

    /// <summary>
    /// Sizes a FIXED (NUMBER/DECIMAL) column to a stable Arrow type from its declared precision and
    /// scale — the same rule the result decoder (<c>SnowflakeResultArrowStream</c>) applies, so the
    /// schema this describes matches what a query actually returns: scale &gt; 0 → Decimal128;
    /// scale 0 → Int32 (precision ≤ 9) / Int64 (≤ 18) / Decimal128 (a NUMBER(38,0) can exceed Int64).
    /// </summary>
    private static IArrowType FixedToArrowType(int precision, int scale)
    {
        if (scale > 0)
            return new Decimal128Type(precision, scale);
        if (precision <= MaxFixedInt32Precision)
            return Int32Type.Default;
        if (precision <= MaxFixedInt64Precision)
            return Int64Type.Default;
        return new Decimal128Type(precision, scale);
    }

    /// <inheritdoc/>
    public ParameterSet ConvertArrowBatchToParameters(RecordBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);

        var parameters = new Dictionary<string, SnowflakeBinding>();

        if (batch.Length <= 0)
            return new ParameterSet { Parameters = parameters };

        for (var i = 0; i < batch.Schema.FieldsList.Count; i++)
        {
            // Snowflake binds '?' placeholders positionally: each column is the parameter
            // at its 1-based ordinal, keyed "1", "2", ... (not by column name). A single-row
            // batch binds scalar values; a multi-row batch binds one value array per
            // parameter and the server executes the statement once per row (executemany).
            var key = (i + 1).ToString(CultureInfo.InvariantCulture);
            parameters[key] = batch.Length == 1
                ? ToBinding(batch.Column(i), 0)
                : ToArrayBinding(batch.Column(i), batch.Length);
        }

        return new ParameterSet { Parameters = parameters };
    }

    /// <summary>
    /// Converts a whole Arrow column into an array bind: the same per-value wire format as a
    /// scalar bind (<see cref="ToBinding"/>), one entry per row. The bind type is derived from
    /// the column's Arrow type, so it is identical for every row.
    /// </summary>
    private SnowflakeBinding ToArrayBinding(IArrowArray column, int rowCount)
    {
        var values = new string?[rowCount];
        string type = string.Empty;
        for (int row = 0; row < rowCount; row++)
        {
            SnowflakeBinding rowBinding = ToBinding(column, row);
            type = rowBinding.Type;
            values[row] = rowBinding.Value;
        }

        return new SnowflakeBinding(type, values);
    }

    /// <summary>
    /// Converts a single Arrow array value into a Snowflake bind variable (type + string value).
    /// </summary>
    private SnowflakeBinding ToBinding(IArrowArray array, int index)
    {
        // Keyed off the Arrow array type (not the CLR value), since a DateTime alone can't tell a
        // DATE bind from a TIMESTAMP. A null value keeps the column's bind type. The string value
        // formats match Snowflake's bind protocol: DATE = ms since epoch, TIME = ns of day,
        // TIMESTAMP = ns since epoch, BINARY = hex.
        bool isNull = array.IsNull(index);
        return array switch
        {
            BooleanArray a => new SnowflakeBinding(BindTypeNames.Boolean, isNull ? null : (a.GetValue(index)!.Value ? "true" : "false")),
            Int8Array a => new SnowflakeBinding(BindTypeNames.Fixed, isNull ? null : a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture)),
            Int16Array a => new SnowflakeBinding(BindTypeNames.Fixed, isNull ? null : a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture)),
            Int32Array a => new SnowflakeBinding(BindTypeNames.Fixed, isNull ? null : a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture)),
            Int64Array a => new SnowflakeBinding(BindTypeNames.Fixed, isNull ? null : a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture)),
            UInt8Array a => new SnowflakeBinding(BindTypeNames.Fixed, isNull ? null : a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture)),
            UInt16Array a => new SnowflakeBinding(BindTypeNames.Fixed, isNull ? null : a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture)),
            UInt32Array a => new SnowflakeBinding(BindTypeNames.Fixed, isNull ? null : a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture)),
            UInt64Array a => new SnowflakeBinding(BindTypeNames.Fixed, isNull ? null : a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture)),
            Decimal128Array a => new SnowflakeBinding(BindTypeNames.Fixed, isNull ? null : a.GetValue(index)?.ToString(CultureInfo.InvariantCulture)),
            Decimal256Array a => new SnowflakeBinding(BindTypeNames.Fixed, isNull ? null : a.GetString(index)),
            FloatArray a => new SnowflakeBinding(BindTypeNames.Real, isNull ? null : a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture)),
            DoubleArray a => new SnowflakeBinding(BindTypeNames.Real, isNull ? null : a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture)),
            StringArray a => new SnowflakeBinding(BindTypeNames.Text, isNull ? null : a.GetString(index)),
            BinaryArray a => new SnowflakeBinding(BindTypeNames.Binary, isNull ? null : Convert.ToHexString(a.GetBytes(index)).ToLowerInvariant()),
            Date32Array a => new SnowflakeBinding(BindTypeNames.Date, isNull ? null : DateMillisSinceEpoch(a.GetDateTime(index)!.Value).ToString(CultureInfo.InvariantCulture)),
            Date64Array a => new SnowflakeBinding(BindTypeNames.Date, isNull ? null : DateMillisSinceEpoch(a.GetDateTime(index)!.Value).ToString(CultureInfo.InvariantCulture)),
            Time32Array a => new SnowflakeBinding(BindTypeNames.Time, isNull ? null : NanosecondsOfDay(a.Values[index], ((Time32Type)a.Data.DataType).Unit).ToString(CultureInfo.InvariantCulture)),
            Time64Array a => new SnowflakeBinding(BindTypeNames.Time, isNull ? null : NanosecondsOfDay(a.Values[index], ((Time64Type)a.Data.DataType).Unit).ToString(CultureInfo.InvariantCulture)),
            TimestampArray a => ToTimestampBinding(a, index, isNull),
            _ => throw new NotSupportedException($"Binding an Arrow {array.GetType().Name} parameter is not supported.")
        };
    }

    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static SnowflakeBinding ToTimestampBinding(TimestampArray array, int index, bool isNull)
    {
        var type = (TimestampType)array.Data.DataType;
        // No zone → wall-clock NTZ; a zone means the stored value is the UTC instant → bind as LTZ
        // (Arrow can't carry a per-row offset, so TZ would lose nothing meaningful over LTZ here).
        string bindType = type.Timezone == null ? BindTypeNames.TimestampNtz : BindTypeNames.TimestampLtz;
        if (isNull)
            return new SnowflakeBinding(bindType, (string?)null);

        long nanos = array.Values[index] * NanosecondsPerUnit(type.Unit);
        return new SnowflakeBinding(bindType, nanos.ToString(CultureInfo.InvariantCulture));
    }

    private static long DateMillisSinceEpoch(DateTime date) =>
        (long)(date.Date - UnixEpoch).TotalMilliseconds;

    private static long NanosecondsOfDay(long rawValue, TimeUnit unit) => rawValue * NanosecondsPerUnit(unit);

    private static long NanosecondsPerUnit(TimeUnit unit) => unit switch
    {
        TimeUnit.Second => 1_000_000_000L,
        TimeUnit.Millisecond => 1_000_000L,
        TimeUnit.Microsecond => 1_000L,
        _ => 1L // Nanosecond
    };
}
