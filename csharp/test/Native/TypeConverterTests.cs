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
using AdbcDrivers.Snowflake.Native.Services.TypeConversion;
using Xunit;

using Apache.Arrow;
using Apache.Arrow.Types;

namespace AdbcDrivers.Snowflake.Native.Tests;

/// <summary>
/// Offline unit tests for <see cref="TypeConverter"/> — the <b>describe/metadata mapping</b>
/// (layer 1 of 3, see below). It translates a Snowflake type <i>name</i> to an Arrow type and
/// back, maps an Arrow batch to bind parameters, and builds an Arrow batch from a JSON row set.
/// This is the path behind GetTableSchema/describe; being a pure function over type names, it
/// needs no connection.
///
/// The driver has three type-fidelity layers, each tested in its own place:
/// <list type="number">
///   <item><b>this</b> — Snowflake type name ⇄ Arrow (describe path, offline);</item>
///   <item><see cref="Integration.TypeDecodingTests"/> — Snowflake result <i>wire</i> → Arrow,
///     over a live query (the source of truth for what a SELECT actually returns);</item>
///   <item><c>Integration.ClientTests.Reader_ConvertsColumnTypesToClr</c> — Arrow → CLR via the
///     ADO.NET client.</item>
/// </list>
/// Layers 1 and 2 are distinct code paths and can differ — e.g. NUMBER(38,0) maps to Int64 here
/// but decodes to Decimal128 off the wire.
/// </summary>
[Trait("Category", "Unit")]
public class TypeConverterTests
{
    private readonly TypeConverter _converter = new();

    // ---- ConvertSnowflakeTypeToArrow ----

    [Theory]
    [InlineData("BOOLEAN", typeof(BooleanType))]
    // INTEGER/INT/BIGINT are NUMBER(38,0) in Snowflake → sized like NUMBER(38,0) (Decimal128).
    [InlineData("INTEGER", typeof(Decimal128Type))]
    [InlineData("INT", typeof(Decimal128Type))]
    [InlineData("BIGINT", typeof(Decimal128Type))]
    [InlineData("FLOAT", typeof(FloatType))]
    [InlineData("DOUBLE", typeof(DoubleType))]
    [InlineData("REAL", typeof(DoubleType))]
    [InlineData("VARCHAR", typeof(StringType))]
    [InlineData("STRING", typeof(StringType))]
    [InlineData("TEXT", typeof(StringType))]
    [InlineData("BINARY", typeof(BinaryType))]
    [InlineData("DATE", typeof(Date32Type))]
    [InlineData("TIME", typeof(Time64Type))]
    [InlineData("VARIANT", typeof(StringType))]
    [InlineData("OBJECT", typeof(StringType))]
    [InlineData("ARRAY", typeof(ListType))]
    [InlineData("GEOGRAPHY", typeof(StringType))]
    [InlineData("GEOMETRY", typeof(StringType))]
    public void ConvertSnowflakeTypeToArrow_MapsScalarTypes(string typeName, Type expectedArrowType)
    {
        IArrowType result = _converter.ConvertSnowflakeTypeToArrow(new SnowflakeDataType { TypeName = typeName });
        Assert.IsType(expectedArrowType, result);
    }

    [Theory]
    [InlineData(9, typeof(Int32Type))]
    [InlineData(18, typeof(Int64Type))]
    [InlineData(38, typeof(Decimal128Type))]
    public void ConvertSnowflakeTypeToArrow_ScaleZeroNumber_SizedByPrecision(int precision, Type expectedArrowType)
    {
        // Matches the result decoder: a scale-0 NUMBER is sized by its declared precision so the
        // described schema agrees with what a query returns.
        IArrowType result = _converter.ConvertSnowflakeTypeToArrow(
            new SnowflakeDataType { TypeName = "NUMBER", Precision = precision, Scale = 0 });
        Assert.IsType(expectedArrowType, result);
    }

    [Fact]
    public void ConvertSnowflakeTypeToArrow_NumberWithScale_IsDecimal128()
    {
        IArrowType result = _converter.ConvertSnowflakeTypeToArrow(
            new SnowflakeDataType { TypeName = "NUMBER", Precision = 18, Scale = 2 });
        var decimalType = Assert.IsType<Decimal128Type>(result);
        Assert.Equal(18, decimalType.Precision);
        Assert.Equal(2, decimalType.Scale);
    }

    [Fact]
    public void ConvertSnowflakeTypeToArrow_Array_IsListOfString()
    {
        IArrowType result = _converter.ConvertSnowflakeTypeToArrow(new SnowflakeDataType { TypeName = "ARRAY" });
        var listType = Assert.IsType<ListType>(result);
        Assert.IsType<StringType>(listType.ValueDataType);
    }

    [Theory]
    [InlineData("TIMESTAMP_NTZ", null)]
    [InlineData("TIMESTAMP", null)]
    [InlineData("DATETIME", null)]
    [InlineData("TIMESTAMP_LTZ", "UTC")]
    public void ConvertSnowflakeTypeToArrow_Timestamp_HasExpectedTimezone(string typeName, string? expectedTimezone)
    {
        var result = Assert.IsType<TimestampType>(
            _converter.ConvertSnowflakeTypeToArrow(new SnowflakeDataType { TypeName = typeName }));
        Assert.Equal(TimeUnit.Nanosecond, result.Unit);
        Assert.Equal(expectedTimezone, result.Timezone);
    }

    [Fact]
    public void ConvertSnowflakeTypeToArrow_TimestampTz_IsTaggedUtc()
    {
        // TIMESTAMP_TZ decodes to its UTC instant (a single Arrow column cannot carry a per-row
        // offset), so the described type matches the result: Timestamp[ns] tagged "UTC", whatever
        // the column's own timezone.
        var result = Assert.IsType<TimestampType>(_converter.ConvertSnowflakeTypeToArrow(
            new SnowflakeDataType { TypeName = "TIMESTAMP_TZ", Timezone = "America/New_York" }));
        Assert.Equal("UTC", result.Timezone);
    }

    [Fact]
    public void ConvertSnowflakeTypeToArrow_UnknownType_Throws()
    {
        Assert.Throws<NotSupportedException>(
            () => _converter.ConvertSnowflakeTypeToArrow(new SnowflakeDataType { TypeName = "NONSENSE" }));
    }

    [Fact]
    public void ConvertSnowflakeTypeToArrow_Null_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => _converter.ConvertSnowflakeTypeToArrow(null!));
    }

    // ---- ConvertArrowBatchToParameters ----

    [Fact]
    public void ConvertArrowBatchToParameters_KeysBindingsPositionallyWithTypes()
    {
        // Given a two-column Arrow batch (an Int64 and a string)
        var schema = new Schema(
            [new Field("A", Int64Type.Default, true), new Field("B", StringType.Default, true)],
            null);
        IArrowArray idArray = new Int64Array.Builder().Append(42).Build();
        IArrowArray nameArray = new StringArray.Builder().Append("hello").Build();
        using var batch = new RecordBatch(schema, [idArray, nameArray], 1);

        // When it is converted to bind parameters
        var result = _converter.ConvertArrowBatchToParameters(batch);

        // Then bindings are keyed by 1-based placeholder position (matching '?'), not by column
        // name, with the Snowflake bind type mapped from the Arrow type.
        Assert.Equal(2, result.Parameters.Count);
        Assert.Equal("FIXED", result.Parameters["1"].Type);
        Assert.Equal("42", result.Parameters["1"].Value);
        Assert.Equal("TEXT", result.Parameters["2"].Type);
        Assert.Equal("hello", result.Parameters["2"].Value);
    }

    [Fact]
    public void ConvertArrowBatchToParameters_EmptyBatch_ReturnsNoParameters()
    {
        var schema = new Schema([new Field("A", Int64Type.Default, true)], null);
        IArrowArray empty = new Int64Array.Builder().Build();
        using var batch = new RecordBatch(schema, [empty], 0);

        ParameterSet result = _converter.ConvertArrowBatchToParameters(batch);

        Assert.Empty(result.Parameters);
    }

    [Fact]
    public void ConvertArrowBatchToParameters_Null_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => _converter.ConvertArrowBatchToParameters(null!));
    }

    [Theory]
    [MemberData(nameof(BindCases.Names), MemberType = typeof(BindCases))]
    public void ConvertArrowBatchToParameters_FormatsEachTypePerSnowflakeBindProtocol(string caseName)
    {
        // One row per bindable Arrow type, defined once in BindCases; this asserts the wire format.
        var bindCase = BindCases.Get(caseName);
        AssertBind(bindCase.BuildArray(), bindCase.ExpectedBindType, bindCase.ExpectedValue);
    }

    [Fact]
    public void ConvertArrowBatchToParameters_NullValue_KeepsTheColumnBindType()
    {
        // A null in a DATE column still binds as a typed DATE null, not an untyped TEXT null.
        AssertBind(new Date32Array.Builder().AppendNull().Build(), "DATE", null);
    }

    [Fact]
    public void ConvertArrowBatchToParameters_MultiRowBatch_BindsValueArrays()
    {
        // Array binding (executemany): each column becomes one array bind with a wire value
        // per row — same per-value format as a scalar bind, nulls preserved.
        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, true),
            new Field("name", StringType.Default, true),
        ], null);
        var ids = new Int64Array.Builder().Append(1).AppendNull().Append(3).Build();
        var names = new StringArray.Builder().Append("a").Append("b").AppendNull().Build();
        using var batch = new RecordBatch(schema, [ids, names], 3);

        var parameters = _converter.ConvertArrowBatchToParameters(batch).Parameters;

        Assert.Equal("FIXED", parameters["1"].Type);
        Assert.Null(parameters["1"].Value);
        Assert.Equal(["1", null, "3"], parameters["1"].Values);
        Assert.Equal("TEXT", parameters["2"].Type);
        Assert.Equal(["a", "b", null], parameters["2"].Values);
    }

    [Fact]
    public void ConvertArrowBatchToParameters_SingleRowBatch_StaysScalar()
    {
        // Wire compatibility: a single-row batch keeps the scalar "value" shape.
        var schema = new Schema([new Field("p", Int64Type.Default, true)], null);
        var values = new Int64Array.Builder().Append(42).Build();
        using var batch = new RecordBatch(schema, [values], 1);

        var parameters = _converter.ConvertArrowBatchToParameters(batch).Parameters;

        Assert.Equal("42", parameters["1"].Value);
        Assert.Null(parameters["1"].Values);
    }

    [Fact]
    public void SnowflakeBinding_SerializesScalarAndArrayValueShapes()
    {
        // The bind protocol carries both shapes under the same "value" key: a string for a
        // scalar bind, an array of strings/nulls for an array bind.
        string scalar = System.Text.Json.JsonSerializer.Serialize(
            new Services.Transport.SnowflakeBinding("TEXT", "x"));
        Assert.Equal("""{"type":"TEXT","value":"x"}""", scalar);

        string scalarNull = System.Text.Json.JsonSerializer.Serialize(
            new Services.Transport.SnowflakeBinding("TEXT", (string?)null));
        Assert.Equal("""{"type":"TEXT","value":null}""", scalarNull);

        string array = System.Text.Json.JsonSerializer.Serialize(
            new Services.Transport.SnowflakeBinding("FIXED", ["1", null, "3"]));
        Assert.Equal("""{"type":"FIXED","value":["1",null,"3"]}""", array);
    }

    [Fact]
    public void ConvertArrowBatchToParameters_UnsupportedArrowType_Throws()
    {
        // An untyped null column can't be bound to a Snowflake type — throw rather than guess.
        var schema = new Schema([new Field("p", NullType.Default, true)], null);
        using var batch = new RecordBatch(schema, [new NullArray(1)], 1);

        Assert.Throws<NotSupportedException>(() => _converter.ConvertArrowBatchToParameters(batch));
    }

    private void AssertBind(IArrowArray array, string expectedType, string? expectedValue)
    {
        var schema = new Schema([new Field("p", array.Data.DataType, true)], null);
        using var batch = new RecordBatch(schema, [array], array.Length);

        var binding = _converter.ConvertArrowBatchToParameters(batch).Parameters["1"];

        Assert.Equal(expectedType, binding.Type);
        Assert.Equal(expectedValue, binding.Value);
    }
}
