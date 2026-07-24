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
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native;
using Xunit;
using Xunit.Abstractions;

using Apache.Arrow;
using Apache.Arrow.Types;

namespace AdbcDrivers.Snowflake.Native.Tests.Integration;

/// <summary>
/// The driver's <b>over-the-wire result decoding</b> — the canonical per-type matrix (layer 2 of
/// 3). For each Snowflake data type it runs <c>SELECT &lt;literal&gt;</c> and asserts the Arrow
/// type (and value) the result stream actually produces, via
/// <see cref="Services.Query.SnowflakeResultArrowStream"/>. This is the source of truth for what
/// a query returns.
///
/// See <see cref="Tests.TypeConverterTests"/> (layer 1: the offline describe-path name mapping)
/// and <see cref="ClientTests"/>'s <c>Reader_ConvertsColumnTypesToClr</c> (layer 3: Arrow → CLR
/// via the ADO.NET client). The result-wire decode here may diverge from the describe-path
/// mapping — e.g. NUMBER(38,0) decodes to Decimal128 here but the describe path declares Int64.
///
/// Uses pure literals, so it needs only a live account (no sample data, no writable schema); set
/// SNOWFLAKE_TEST_CONFIG_FILE.
/// </summary>
[Trait("Category", "Integration")]
public class TypeDecodingTests
{
    private readonly ITestOutputHelper _output;
    private readonly IntegrationTestConfiguration _testConfiguration;

    public TypeDecodingTests(ITestOutputHelper output)
    {
        _output = output;
        _testConfiguration = IntegrationTestingUtils.TestConfiguration;

        Skip.If(string.IsNullOrEmpty(_testConfiguration.Account),
            $"Cannot execute test configuration from environment variable `{IntegrationTestingUtils.SnowflakeTestConfigVariable}`");
    }

    /// <summary>
    /// Result-decode gaps that are not yet implemented. These are skipped (not failed) so the
    /// backlog is visible; remove an entry once the decoder handles that type and the assertion
    /// will start enforcing it. See <see cref="Services.Query.SnowflakeResultArrowStream"/>.
    /// </summary>
    private static readonly Dictionary<string, string> NotYetDecoded = new();

    [SkippableTheory]
    [InlineData("TRUE::BOOLEAN", typeof(BooleanType))]
    // Scale-0 NUMBER is sized by its declared precision: ≤9 → Int32, ≤18 → Int64, else Decimal128.
    [InlineData("123::NUMBER(9,0)", typeof(Int32Type))]
    [InlineData("123::NUMBER(18,0)", typeof(Int64Type))]
    [InlineData("42::NUMBER(38,0)", typeof(Decimal128Type))]
    [InlineData("9.99::NUMBER(10,2)", typeof(Decimal128Type))]
    [InlineData("1.5::FLOAT", typeof(DoubleType))]
    [InlineData("'hello'::VARCHAR", typeof(StringType))]
    [InlineData("TO_BINARY('AB','HEX')", typeof(BinaryType))]
    [InlineData("'2020-01-01'::DATE", typeof(Date32Type))]
    [InlineData("'12:34:56'::TIME", typeof(Time64Type))]
    [InlineData("'2020-01-01 12:00:00'::TIMESTAMP_NTZ", typeof(TimestampType))]
    [InlineData("'2020-01-01 12:00:00'::TIMESTAMP_LTZ", typeof(TimestampType))]
    [InlineData("'2020-01-01 12:00:00 +00:00'::TIMESTAMP_TZ", typeof(TimestampType))]
    [InlineData("TO_VARIANT(1)", typeof(StringType))]
    [InlineData("OBJECT_CONSTRUCT('a', 1)", typeof(StringType))]
    // Snowflake returns semi-structured ARRAY/OBJECT/VARIANT as a JSON string.
    [InlineData("ARRAY_CONSTRUCT(1, 2)", typeof(StringType))]
    public async Task ResultColumn_HasExpectedArrowType(string sqlLiteral, Type expectedArrowType)
    {
        Skip.If(NotYetDecoded.TryGetValue(sqlLiteral, out string? reason), reason);

        // Given a query of a single typed literal
        using var connection = Connect();
        using var statement = connection.CreateStatement();
        statement.SqlQuery = $"SELECT {sqlLiteral} AS V";

        // When the result schema is read
        var result = await statement.ExecuteQueryAsync();
        Assert.NotNull(result.Stream);
        using var stream = result.Stream!;

        // Then the column has the expected Arrow type
        IArrowType actual = stream.Schema.FieldsList[0].DataType;
        _output.WriteLine($"{sqlLiteral} -> {actual.GetType().Name} (expected {expectedArrowType.Name})");
        Assert.IsType(expectedArrowType, actual);
    }

    [SkippableFact]
    public async Task ScaledNumber_DecodesValueWithScale()
    {
        // Given a scaled NUMBER query — Snowflake sends it as an integer (999) with the scale in
        // field metadata, so the driver must rescale it to a Decimal128 to recover 9.99
        using var connection = Connect();
        using var statement = connection.CreateStatement();
        statement.SqlQuery = "SELECT 9.99::NUMBER(10,2) AS V";

        // When the first batch is read
        var result = await statement.ExecuteQueryAsync();
        Assert.NotNull(result.Stream);
        using var stream = result.Stream!;
        var batch = await stream.ReadNextRecordBatchAsync();

        // Then the column is a Decimal128 carrying the real value
        Assert.NotNull(batch);
        var column = Assert.IsType<Decimal128Array>(batch!.Column(0));
        Assert.Equal(9.99m, column.GetValue(0));
    }

    /// <summary>
    /// Proves Snowflake reports the column's <b>declared</b> precision distinctly in the Arrow
    /// field metadata (9 vs 18 vs 38), independent of the values. This is the foundation for
    /// precision-driven sizing of scale-0 NUMBER (precision ≤ 9 → Int32, ≤ 18 → Int64, else
    /// Decimal128): without a reliable declared precision that sizing would be unsafe.
    /// </summary>
    [SkippableTheory]
    [InlineData("123::NUMBER(9,0)", 9)]
    [InlineData("123::NUMBER(18,0)", 18)]
    [InlineData("123::NUMBER(38,0)", 38)]
    public async Task ScaleZeroNumber_ReportsDeclaredPrecisionInMetadata(string sqlLiteral, int expectedPrecision)
    {
        // Given a query of a precision-qualified NUMBER literal
        using var connection = Connect();
        using var statement = connection.CreateStatement();
        statement.SqlQuery = $"SELECT {sqlLiteral} AS V";

        // When the result field metadata is read
        var result = await statement.ExecuteQueryAsync();
        Assert.NotNull(result.Stream);
        using var stream = result.Stream!;
        Field field = stream.Schema.FieldsList[0];

        // Then it reports FIXED with the declared precision
        Assert.True(field.HasMetadata, "FIXED column should carry Snowflake field metadata.");
        Assert.True(field.Metadata.TryGetValue("logicalType", out string? logicalType));
        Assert.Equal("FIXED", logicalType);
        Assert.True(field.Metadata.TryGetValue("precision", out string? precision),
            "FIXED column metadata should include 'precision'.");
        _output.WriteLine($"{sqlLiteral} -> precision={precision} (expected {expectedPrecision})");
        Assert.Equal(expectedPrecision, int.Parse(precision!, System.Globalization.CultureInfo.InvariantCulture));
    }

    [SkippableFact]
    public async Task ScaleZeroNumber_LargeValue_RoundTripsAsDecimal128()
    {
        // Given a 20-digit NUMBER(38,0) — larger than Int64.MaxValue (9,223,372,036,854,775,807),
        // so sizing it to Int64 would overflow; it must decode to Decimal128 with the value intact
        using var connection = Connect();
        using var statement = connection.CreateStatement();
        const string literal = "12345678901234567890";
        statement.SqlQuery = $"SELECT {literal}::NUMBER(38,0) AS V";

        // When the first batch is read
        var result = await statement.ExecuteQueryAsync();
        Assert.NotNull(result.Stream);
        using var stream = result.Stream!;
        var batch = await stream.ReadNextRecordBatchAsync();

        // Then it is a Decimal128 with the full value preserved
        Assert.NotNull(batch);
        var column = Assert.IsType<Decimal128Array>(batch!.Column(0));
        Assert.Equal(decimal.Parse(literal, System.Globalization.CultureInfo.InvariantCulture), column.GetValue(0));
    }

    /// <summary>
    /// TIME decodes to Time64 nanoseconds-of-day, rescaled from the wire's 10^-scale units.
    /// 12:34:56.789 = 45296.789 s = 45,296,789,000,000 ns. Asserts the raw value (and that the
    /// reduced-scale TIME(3) wire form rescales identically to full precision).
    /// </summary>
    [SkippableTheory]
    [InlineData("'12:34:56.789'::TIME(3)")]
    [InlineData("'12:34:56.789'::TIME(9)")]
    public async Task Time_DecodesToNanosecondsOfDay(string sqlLiteral)
    {
        // Given a TIME query
        using var connection = Connect();
        using var statement = connection.CreateStatement();
        statement.SqlQuery = $"SELECT {sqlLiteral} AS V";

        // When the first batch is read
        var result = await statement.ExecuteQueryAsync();
        using var stream = result.Stream!;
        var batch = await stream.ReadNextRecordBatchAsync();

        // Then it is a Time64 of nanoseconds-of-day
        var column = Assert.IsType<Time64Array>(batch!.Column(0));
        Assert.Equal(45_296_789_000_000L, column.Values[0]);
    }

    /// <summary>
    /// Each TIMESTAMP flavour decodes to a Timestamp[ns] carrying the correct UTC instant, read as
    /// the raw nanosecond value to prove full precision (DateTimeOffset would round to 100 ns).
    /// NTZ has no zone; LTZ/TZ are tagged UTC. The +05:00 input is normalized to its UTC instant.
    /// </summary>
    [SkippableTheory]
    // 2020-03-04 12:34:56.123456789, treated as UTC (epoch 1583325296 s).
    [InlineData("'2020-03-04 12:34:56.123456789'::TIMESTAMP_NTZ", 1583325296123456789L, null)]
    // Reduced scale arrives as a single combined integer, not a struct; must rescale the same way.
    [InlineData("'2020-03-04 12:34:56.123'::TIMESTAMP_NTZ(3)", 1583325296123000000L, null)]
    // 12:34:56.123456789 +05:00 -> 07:34:56.123456789 UTC (epoch 1583307296 s).
    [InlineData("'2020-03-04 12:34:56.123456789 +05:00'::TIMESTAMP_TZ", 1583307296123456789L, "UTC")]
    [InlineData("'2020-03-04 12:34:56.123 +05:00'::TIMESTAMP_TZ(3)", 1583307296123000000L, "UTC")]
    public async Task Timestamp_DecodesToUtcInstant(string sqlLiteral, long expectedNanos, string? expectedTimezone)
    {
        // Given a TIMESTAMP query
        using var connection = Connect();
        using var statement = connection.CreateStatement();
        statement.SqlQuery = $"SELECT {sqlLiteral} AS V";

        // When the result schema and first batch are read
        var result = await statement.ExecuteQueryAsync();
        using var stream = result.Stream!;

        // Then the column is a Timestamp[ns] with the expected zone and UTC instant
        var timestampType = Assert.IsType<TimestampType>(stream.Schema.FieldsList[0].DataType);
        Assert.Equal(TimeUnit.Nanosecond, timestampType.Unit);
        Assert.Equal(expectedTimezone, timestampType.Timezone);

        var batch = await stream.ReadNextRecordBatchAsync();
        var column = Assert.IsType<TimestampArray>(batch!.Column(0));
        Assert.Equal(expectedNanos, column.Values[0]);
    }

    private SnowflakeConnection Connect()
    {
        var driver = IntegrationTestingUtils.GetSnowflakeAdbcDriver(_testConfiguration, out var parameters);
        var database = driver.Open(parameters);
        var connection = database.Connect(new Dictionary<string, string>());
        return (SnowflakeConnection)connection;
    }
}
