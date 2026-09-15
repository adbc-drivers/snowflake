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
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

using AdbcDrivers.Snowflake.Native.Configuration;
using AdbcDrivers.Snowflake.Native.Services.Authentication;
using AdbcDrivers.Snowflake.Native.Services.Query;
using AdbcDrivers.Snowflake.Native.Services.Transport;
using AdbcDrivers.Snowflake.Native.Services.TypeConversion;

using Apache.Arrow;
using Apache.Arrow.Types;

using Microsoft.Extensions.Logging.Abstractions;

using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Snowflake.Native.Tests.Integration;

/// <summary>
/// Covers the metadata path: the type names Snowflake reports for a column, and the schema the
/// driver builds from them. Those names are the server's internal logical types (FIXED, REAL,
/// TEXT ...), never the SQL spelling a column was declared with, so an arm of
/// <see cref="SnowflakeDataType.TypeCode"/> for a name the server never sends is dead code.
///
/// Also holds that schema against the one a result carries. The two are built by different code
/// from different inputs, so they can drift silently, as they did for ARRAY, which described as a
/// list while the data was a string.
///
/// <see cref="TypeDecodingTests"/> is the counterpart: it covers the data path and the decoded
/// values. The name-to-Arrow mapping is unit-tested in TypeConverterTests; only the names and the
/// agreement between the two paths need a live server.
///
/// Geospatial columns are the one place the two paths can still diverge: with GEOGRAPHY_OUTPUT_FORMAT
/// set to WKB or EWKB the data arrives as binary while the metadata still reports 'object', and it
/// carries nothing to tell the two apart. These columns are asserted under the session default.
/// </summary>
[Trait("Category", "Integration")]
public class TypeMetadataTests
{
    private readonly ITestOutputHelper _output;
    private readonly IntegrationTestConfiguration _testConfiguration;

    public TypeMetadataTests(ITestOutputHelper output)
    {
        _output = output;
        _testConfiguration = IntegrationTestingUtils.TestConfiguration;

        Skip.If(string.IsNullOrEmpty(_testConfiguration.Account),
            $"Cannot execute test configuration from environment variable `{IntegrationTestingUtils.SnowflakeTestConfigVariable}`");
    }

    /// <summary>
    /// One row per column, with the CREATE and INSERT generated from it so a column's declaration,
    /// its data and its expected logical type cannot drift apart.
    /// </summary>
    private static readonly (string Column, string Declared, string Value, string LogicalType)[] Columns =
    {
        ("C_NUMBER_38", "NUMBER(38,0)", "12345678901234567890123456789012345678", "fixed"),
        ("C_NUMBER_18", "NUMBER(18,0)", "123456789012345678", "fixed"),
        ("C_NUMBER_9", "NUMBER(9,0)", "123456789", "fixed"),
        ("C_DECIMAL", "DECIMAL(10,2)", "99.99", "fixed"),
        ("C_NUMERIC", "NUMERIC(12,4)", "12.3456", "fixed"),
        ("C_INT", "INT", "42", "fixed"),
        ("C_INTEGER", "INTEGER", "42", "fixed"),
        ("C_BIGINT", "BIGINT", "42", "fixed"),
        ("C_SMALLINT", "SMALLINT", "42", "fixed"),
        ("C_TINYINT", "TINYINT", "42", "fixed"),
        ("C_BYTEINT", "BYTEINT", "42", "fixed"),
        ("C_FLOAT", "FLOAT", "1.5", "real"),
        ("C_FLOAT4", "FLOAT4", "1.5", "real"),
        ("C_FLOAT8", "FLOAT8", "1.5", "real"),
        ("C_DOUBLE", "DOUBLE", "1.5", "real"),
        ("C_DOUBLE_PREC", "DOUBLE PRECISION", "1.5", "real"),
        ("C_REAL", "REAL", "1.5", "real"),
        ("C_VARCHAR", "VARCHAR(50)", "'varchar value'", "text"),
        ("C_STRING", "STRING", "'string value'", "text"),
        ("C_TEXT", "TEXT", "'text value'", "text"),
        ("C_CHAR", "CHAR(5)", "'char5'", "text"),
        ("C_CHARACTER", "CHARACTER(5)", "'chr5'", "text"),
        ("C_BINARY", "BINARY", "TO_BINARY('DEADBEEF','HEX')", "binary"),
        ("C_VARBINARY", "VARBINARY", "TO_BINARY('CAFE','HEX')", "binary"),
        ("C_BOOLEAN", "BOOLEAN", "TRUE", "boolean"),
        ("C_DATE", "DATE", "'2026-09-02'::DATE", "date"),
        ("C_TIME", "TIME", "'12:34:56.789'::TIME", "time"),
        ("C_DATETIME", "DATETIME", "'2026-09-02 12:34:56'::DATETIME", "timestamp_ntz"),
        ("C_TIMESTAMP", "TIMESTAMP", "'2026-09-02 12:34:56'::TIMESTAMP", "timestamp_ntz"),
        ("C_TIMESTAMP_NTZ", "TIMESTAMP_NTZ", "'2026-09-02 12:34:56'::TIMESTAMP_NTZ", "timestamp_ntz"),
        ("C_TIMESTAMP_LTZ", "TIMESTAMP_LTZ", "'2026-09-02 12:34:56'::TIMESTAMP_LTZ", "timestamp_ltz"),
        ("C_TIMESTAMP_TZ", "TIMESTAMP_TZ", "'2026-09-02 12:34:56 +02:00'::TIMESTAMP_TZ", "timestamp_tz"),
        ("C_VARIANT", "VARIANT", "TO_VARIANT('variant value')", "variant"),
        ("C_OBJECT", "OBJECT", "OBJECT_CONSTRUCT('k', 1, 'nested', OBJECT_CONSTRUCT('a', TRUE))", "object"),
        ("C_ARRAY", "ARRAY", "ARRAY_CONSTRUCT(1, 'two', NULL, 4.5)", "array"),
        ("C_ARRAY_TYPED", "ARRAY(NUMBER(38,0))", "[10, 20, 30]::ARRAY(NUMBER(38,0))", "array"),
        ("C_GEOGRAPHY", "GEOGRAPHY", "TO_GEOGRAPHY('POINT(-122.35 37.55)')", "object"),
        ("C_GEOMETRY", "GEOMETRY", "TO_GEOMETRY('POINT(1 2)')", "object"),
        // INT and FLOAT are the only element types Snowflake accepts for a vector.
        ("C_VECTOR_FLOAT", "VECTOR(FLOAT,3)", "[1.5, 2.5, 3.5]::VECTOR(FLOAT,3)", "vector"),
        ("C_VECTOR_INT", "VECTOR(INT,4)", "[1, 2, 3, 4]::VECTOR(INT,4)", "vector"),
    };

    [SkippableFact]
    public async Task ColumnTypes_AreDescribedAsLogicalTypesAndMatchTheDataPath()
    {
        string table = $"{_testConfiguration.Metadata.Catalog}.{_testConfiguration.Metadata.Schema}" +
                       $".ALL_TYPES_{Guid.NewGuid():N}";

        using var driver = IntegrationTestingUtils.GetSnowflakeAdbcDriver(
            _testConfiguration, out Dictionary<string, string> parameters);
        var config = ConnectionStringParser.ParseParameters(parameters);

        var database = driver.Open(parameters);
        using var connection = (SnowflakeConnection)database.Connect(new Dictionary<string, string>());

        try
        {
            await CreatePopulatedTableAsync(connection, table);

            List<(string LogicalType, string Arrow)> described = await DescribeColumnsAsync(config, table);
            Schema materialized = await ReadResultSchemaAsync(connection, table);

            Assert.Equal(Columns.Length, described.Count);
            Assert.Equal(Columns.Length, materialized.FieldsList.Count);

            var failures = new List<string>();
            for (int i = 0; i < Columns.Length; i++)
            {
                (string column, string declared, _, string expectedLogicalType) = Columns[i];
                (string logicalType, string describedArrow) = described[i];
                string materializedArrow = Describe(materialized.FieldsList[i].DataType);

                _output.WriteLine(
                    $"{declared,-20} {column,-16} {logicalType,-14} described {describedArrow,-12} materialized {materializedArrow}");

                if (!string.Equals(logicalType, expectedLogicalType, StringComparison.OrdinalIgnoreCase))
                {
                    failures.Add($"{column} ({declared}): logical type '{logicalType}', expected '{expectedLogicalType}'");
                }
                else if (describedArrow != materializedArrow)
                {
                    failures.Add(
                        $"{column} ({declared}, '{logicalType}'): described {describedArrow}, materialized {materializedArrow}");
                }
            }

            Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {table}");
        }
    }

    /// <summary>
    /// Names an Arrow type precisely enough to compare the two paths: a fixed-size list's size and
    /// element type are the whole point for VECTOR, and are not in <see cref="IArrowType.Name"/>.
    /// </summary>
    private static string Describe(IArrowType type) => type switch
    {
        FixedSizeListType list => $"{list.Name}<{list.ValueDataType.Name}>[{list.ListSize}]",
        _ => type.Name
    };

    private static async Task CreatePopulatedTableAsync(SnowflakeConnection connection, string table)
    {
        string declarations = string.Join(", ", Columns.Select(c => $"{c.Column} {c.Declared}"));
        string values = string.Join(", ", Columns.Select(c => c.Value));

        await ExecuteAsync(connection, $"CREATE OR REPLACE TABLE {table} ({declarations})");

        // Semi-structured and geospatial values cannot appear in a VALUES clause.
        await ExecuteAsync(connection, $"INSERT INTO {table} SELECT {values}");
    }

    private static async Task ExecuteAsync(SnowflakeConnection connection, string sql)
    {
        using var statement = connection.CreateStatement();
        statement.SqlQuery = sql;
        await statement.ExecuteUpdateAsync();
    }

    private static async Task<List<(string LogicalType, string Arrow)>> DescribeColumnsAsync(
        ConnectionConfig config, string table)
    {
        using var httpClient = new HttpClient();
        var token = await new BasicAuthenticator(new SnowflakeLoginClient(httpClient))
            .AuthenticateAsync(config);

        var recorder = new RecordingTypeConverter();
        var executor = new QueryExecutor(
            new RestApiClient(httpClient, config.EnableCompression, logger: NullLogger.Instance),
            recorder, config.Account, config.Network,
            NullLogger<QueryExecutor>.Instance, onConnectionFault: () => { });

        await executor.DescribeAsync(new QueryRequest
        {
            Statement = $"SELECT * FROM {table}",
            Warehouse = config.Warehouse,
            Role = config.Role,
            AuthToken = token
        });

        return recorder.Seen;
    }

    private static async Task<Schema> ReadResultSchemaAsync(SnowflakeConnection connection, string table)
    {
        using var statement = connection.CreateStatement();
        statement.SqlQuery = $"SELECT * FROM {table}";

        var result = await statement.ExecuteQueryAsync();
        using var stream = result.Stream!;
        return stream.Schema;
    }

    /// <summary>
    /// Records the logical type name handed to the converter, which is otherwise not visible from
    /// outside the driver, and the Arrow type it maps to.
    /// </summary>
    private sealed class RecordingTypeConverter : ITypeConverter
    {
        public List<(string LogicalType, string Arrow)> Seen { get; } = new();

        public IArrowType ConvertSnowflakeTypeToArrow(SnowflakeDataType snowflakeType)
        {
            string arrow;
            try
            {
                arrow = Describe(TypeConverter.Shared.ConvertSnowflakeTypeToArrow(snowflakeType));
            }
            catch (NotSupportedException)
            {
                arrow = "<unmapped>";
            }

            Seen.Add((snowflakeType.TypeName, arrow));
            return StringType.Default;
        }

        public ParameterSet ConvertArrowBatchToParameters(RecordBatch batch) =>
            TypeConverter.Shared.ConvertArrowBatchToParameters(batch);
    }
}
