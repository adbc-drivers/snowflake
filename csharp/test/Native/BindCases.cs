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
using System.Linq;

using Apache.Arrow;
using Apache.Arrow.Types;

namespace AdbcDrivers.Snowflake.Native.Tests;

/// <summary>
/// One row per Arrow array type the driver can bind. Single source of truth for both layers
/// of bind testing: <see cref="TypeConverterTests"/> asserts the offline wire format, and the
/// live <c>StatementTests.CanBindParameter</c> theory binds the same array against the real
/// server. Adding or removing a bindable type is a one-line edit here.
/// </summary>
public sealed record BindCase(
    string Name,
    Func<IArrowArray> BuildArray,
    string ExpectedBindType,
    string? ExpectedValue,
    string? LivePredicate);

public static class BindCases
{
    /// <summary>DATE = ms since epoch, TIME = ns of day, TIMESTAMP = ns since epoch, BINARY = lower hex.</summary>
    static readonly IReadOnlyList<BindCase> All =
    [
        new("Boolean",
            () => new BooleanArray.Builder().Append(true).Build(),
            "BOOLEAN", "true", "? = TRUE"),
        new("Int8",
            () => new Int8Array.Builder().Append((sbyte)7).Build(),
            "FIXED", "7", "? = 7"),
        new("Int16",
            () => new Int16Array.Builder().Append((short)1234).Build(),
            "FIXED", "1234", "? = 1234"),
        new("Int32",
            () => new Int32Array.Builder().Append(42).Build(),
            "FIXED", "42", "? = 42"),
        new("Int64",
            () => new Int64Array.Builder().Append(9999999999L).Build(),
            "FIXED", "9999999999", "? = 9999999999"),
        // Unsigned ints and Date64/Decimal256 share a wire format with already-live-proven types
        // (UInt* → FIXED string, Date64 → ms, Decimal256 → FIXED string), so they are offline-only.
        new("UInt8",
            () => new UInt8Array.Builder().Append((byte)7).Build(),
            "FIXED", "7", null),
        new("UInt16",
            () => new UInt16Array.Builder().Append((ushort)1234).Build(),
            "FIXED", "1234", null),
        new("UInt32",
            () => new UInt32Array.Builder().Append(42u).Build(),
            "FIXED", "42", null),
        new("UInt64",
            () => new UInt64Array.Builder().Append(9999999999UL).Build(),
            "FIXED", "9999999999", null),
        new("Float",
            () => new FloatArray.Builder().Append(1.5f).Build(),
            "REAL", "1.5", "? = 1.5"),
        new("Double",
            () => new DoubleArray.Builder().Append(1.5).Build(),
            "REAL", "1.5", "? = 1.5"),
        new("Decimal128",
            () => new Decimal128Array.Builder(new Decimal128Type(10, 2)).Append(9.99m).Build(),
            "FIXED", "9.99", "? = 9.99"),
        new("Decimal256",
            () => new Decimal256Array.Builder(new Decimal256Type(10, 2)).Append(9.99m).Build(),
            "FIXED", "9.99", null),
        new("String",
            () => new StringArray.Builder().Append("hi").Build(),
            "TEXT", "hi", "? = 'hi'"),
        new("Binary",
            () => new BinaryArray.Builder().Append([0xAB, 0xCD]).Build(),
            "BINARY", "abcd", "? = TO_BINARY('ABCD','HEX')"),
        new("Date32",
            () => new Date32Array.Builder().Append(new DateTime(2020, 1, 1)).Build(),
            "DATE", "1577836800000", "? = '2020-01-01'::DATE"),
        new("Date64",
            () => new Date64Array.Builder().Append(new DateTime(2020, 1, 1)).Build(),
            "DATE", "1577836800000", null),
        new("Time32",
            () => new Time32Array.Builder(new Time32Type()).Append(45_296_789).Build(),
            "TIME", "45296789000000", "? = '12:34:56.789'::TIME"),
        new("Time64",
            () => new Time64Array.Builder(new Time64Type()).Append(45_296_000_000_000L).Build(),
            "TIME", "45296000000000", "? = '12:34:56'::TIME"),
        new("TimestampNtz",
            () => new TimestampArray.Builder(new TimestampType(TimeUnit.Nanosecond, (string?)null))
                .Append(new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero)).Build(),
            "TIMESTAMP_NTZ", "1577836800000000000", "? = '2020-01-01 00:00:00'::TIMESTAMP_NTZ"),
        // TIMESTAMP_LTZ binds an instant; the offline assertion proves the wire format. A live
        // equality check would depend on session time zone, so it is left to the NTZ case.
        new("TimestampLtz",
            () => new TimestampArray.Builder(new TimestampType(TimeUnit.Nanosecond, "UTC"))
                .Append(new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero)).Build(),
            "TIMESTAMP_LTZ", "1577836800000000000", null)
    ];

    /// <summary>Every case, as xunit theory data (the serializable case name).</summary>
    public static IEnumerable<object[]> Names() => All.Select(c => new object[] { c.Name });

    /// <summary>Only the cases that carry a live equality predicate.</summary>
    public static IEnumerable<object[]> LiveNames() =>
        All.Where(c => c.LivePredicate != null).Select(c => new object[] { c.Name });

    public static BindCase Get(string name) => All.Single(c => c.Name == name);
}
