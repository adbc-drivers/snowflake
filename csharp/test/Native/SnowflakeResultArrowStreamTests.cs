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

using System.Collections.Generic;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native.Services.Query;
using Apache.Arrow;
using Apache.Arrow.Types;
using Xunit;

namespace AdbcDrivers.Snowflake.Native.Tests;

/// <summary>
/// Offline tests for <see cref="SnowflakeResultArrowStream"/> — the wire→stable-type fixups
/// (FIXED sizing, TIME/TIMESTAMP rescaling), driven by field <c>logicalType</c> metadata over an
/// in-memory inner stream. Complements the live <c>TypeDecodingTests</c> (real wire shapes) with
/// deterministic coverage of value math and null handling, which the live literals barely hit.
/// </summary>
[Trait("Category", "Unit")]
public class SnowflakeResultArrowStreamTests
{
    private static Dictionary<string, string> FixedMeta(int precision, int scale) => new()
    {
        ["logicalType"] = "FIXED",
        ["precision"] = precision.ToString(),
        ["scale"] = scale.ToString(),
    };

    private static async Task<RecordBatch> TransformAsync(Field field, IArrowArray column)
    {
        var schema = new Schema([field], null);
        var stream = new SnowflakeResultArrowStream(new InMemoryArrowStream(schema, [column]));
        RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
        Assert.NotNull(batch);
        return batch!;
    }

    [Fact]
    public async Task Fixed_Int8WithNulls_WidensToInt32_PreservingNulls()
    {
        // Given a FIXED(5,0) column arriving as Int8 with a null in the middle
        var column = new Int8Array.Builder().Append(7).AppendNull().Append(-3).Build();
        var field = new Field("c", Int8Type.Default, true, FixedMeta(5, 0));

        using RecordBatch batch = await TransformAsync(field, column);

        // Then it becomes Int32 with values and the null preserved
        var result = Assert.IsType<Int32Array>(batch.Column(0));
        Assert.Equal(7, result.GetValue(0));
        Assert.True(result.IsNull(1));
        Assert.Equal(-3, result.GetValue(2));
    }

    [Fact]
    public async Task Fixed_Int16NoNulls_WidensToInt32()
    {
        // No-null columns take the fast path that skips validity building entirely
        var column = new Int16Array.Builder().Append(1000).Append(-1000).Build();
        var field = new Field("c", Int16Type.Default, false, FixedMeta(9, 0));

        using RecordBatch batch = await TransformAsync(field, column);

        var result = Assert.IsType<Int32Array>(batch.Column(0));
        Assert.Equal(0, result.NullCount);
        Assert.Equal(1000, result.GetValue(0));
        Assert.Equal(-1000, result.GetValue(1));
    }

    [Fact]
    public async Task Fixed_ScaledInt64WithNulls_RescalesToDecimal()
    {
        // Given NUMBER(10,2): 9.99 arrives as 999, -0.05 as -5
        var column = new Int64Array.Builder().Append(999).AppendNull().Append(-5).Build();
        var field = new Field("c", Int64Type.Default, true, FixedMeta(10, 2));

        using RecordBatch batch = await TransformAsync(field, column);

        var result = Assert.IsType<Decimal128Array>(batch.Column(0));
        Assert.Equal(9.99m, result.GetValue(0));
        Assert.True(result.IsNull(1));
        Assert.Equal(-0.05m, result.GetValue(2));
    }

    [Fact]
    public async Task Time_Int32Scale3_RescalesToNanoseconds()
    {
        // Given TIME(3): 12:34:56.789 arrives as 45_296_789 (ms of day)
        var column = new Int32Array.Builder().Append(45_296_789).AppendNull().Build();
        var field = new Field("c", Int32Type.Default, true, new Dictionary<string, string>
        {
            ["logicalType"] = "TIME",
            ["scale"] = "3",
        });

        using RecordBatch batch = await TransformAsync(field, column);

        var result = Assert.IsType<Time64Array>(batch.Column(0));
        Assert.Equal(45_296_789_000_000L, result.GetValue(0));
        Assert.True(result.IsNull(1));
    }

    [Fact]
    public async Task TimestampNtz_EpochFractionStructWithNull_CombinesToNanoseconds()
    {
        // Given TIMESTAMP_NTZ(9) in its two-field struct shape: epoch seconds + nanosecond
        // fraction, with the middle row null (children hold garbage there)
        var epoch = new Int64Array.Builder().Append(1).Append(0).Append(2).Build();
        var fraction = new Int32Array.Builder().Append(500).Append(0).Append(750).Build();
        var structType = new StructType(
        [
            new Field("epoch", Int64Type.Default, false),
            new Field("fraction", Int32Type.Default, false),
        ]);
        var bitmap = new ArrowBuffer.BitmapBuilder();
        bitmap.Append(true); bitmap.Append(false); bitmap.Append(true);
        var column = new StructArray(structType, 3, [epoch, fraction], bitmap.Build(), nullCount: 1);
        var field = new Field("c", structType, true, new Dictionary<string, string>
        {
            ["logicalType"] = "TIMESTAMP_NTZ",
            ["scale"] = "9",
        });

        using RecordBatch batch = await TransformAsync(field, column);

        // Then epoch*1e9 + fraction, null preserved, type Timestamp[ns] without a zone
        var result = Assert.IsType<TimestampArray>(batch.Column(0));
        Assert.Null(((TimestampType)result.Data.DataType).Timezone);
        Assert.Equal(1_000_000_500L, result.Values[0]);
        Assert.True(result.IsNull(1));
        Assert.Equal(2_000_000_750L, result.Values[2]);
    }

    [Fact]
    public async Task TimestampLtz_SingleInt64Scale3_RescalesToUtcNanoseconds()
    {
        // Given TIMESTAMP_LTZ(3) in its single-integer shape (ms since epoch)
        var column = new Int64Array.Builder().Append(1_577_836_800_123L).Build();
        var field = new Field("c", Int64Type.Default, true, new Dictionary<string, string>
        {
            ["logicalType"] = "TIMESTAMP_LTZ",
            ["scale"] = "3",
        });

        using RecordBatch batch = await TransformAsync(field, column);

        var result = Assert.IsType<TimestampArray>(batch.Column(0));
        Assert.Equal("UTC", ((TimestampType)result.Data.DataType).Timezone);
        Assert.Equal(1_577_836_800_123_000_000L, result.Values[0]);
    }

    [Fact]
    public async Task UntaggedColumns_PassThroughUntouched()
    {
        // Given a plain string column with no Snowflake logicalType metadata
        var column = new StringArray.Builder().Append("hi").Build();
        var field = new Field("c", StringType.Default, true);

        using RecordBatch batch = await TransformAsync(field, column);

        var result = Assert.IsType<StringArray>(batch.Column(0));
        Assert.Equal("hi", result.GetString(0));
    }
}
