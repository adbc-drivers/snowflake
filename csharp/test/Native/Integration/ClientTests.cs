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
using System.Data.Common;
using AdbcDrivers.Snowflake.Native;
using Xunit;
using Xunit.Abstractions;

using AdbcClient = Apache.Arrow.Adbc.Client;

namespace AdbcDrivers.Snowflake.Native.Tests.Integration;

/// <summary>
/// Tests the <b>ADO.NET client layer</b> (<c>Apache.Arrow.Adbc.Client</c>) over the native
/// Snowflake driver — i.e. using the driver as a standard <c>System.Data.Common</c> provider:
/// a <c>DbConnection</c> / <c>DbCommand</c> / <c>DbDataReader</c> that returns CLR values.
///
/// This is the counterpart to the Arrow-native ADBC API suites (<see cref="StatementTests"/>,
/// <see cref="QueryAndMetadataTests"/>, <see cref="TypeDecodingTests"/>, which use
/// <c>AdbcConnection</c>/<c>AdbcStatement</c> returning Arrow <c>RecordBatch</c>es); these verify
/// the parts that <i>only</i> the client layer runs:
/// <list type="bullet">
///   <item>connection-string parsing into driver parameters,</item>
///   <item>the <c>DbDataReader</c> row/column iteration model (<c>Read</c>, ordinals, <c>FieldCount</c>),</item>
///   <item>conversion of Arrow arrays into boxed CLR values across column types.</item>
/// </list>
///
/// Each test is self-contained: it creates and populates a session-scoped <c>TEMPORARY</c>
/// table (auto-dropped at session end), so the suite needs no sample data, seeding, or test
/// ordering. It does require a <b>writable</b> database/schema (config <c>metadata.catalog</c>
/// / <c>metadata.schema</c>); without one, the tests Skip. Requires a live account; set
/// SNOWFLAKE_TEST_CONFIG_FILE.
///
/// If you do not already have a writable schema, create one once and point the config at it:
/// <code>
/// CREATE DATABASE IF NOT EXISTS ADBC_TEST;   -- a PUBLIC schema is created automatically
/// </code>
/// then in the test config: <c>"metadata": { "catalog": "ADBC_TEST", "schema": "PUBLIC" }</c>.
/// The tests create only temporary tables there — nothing is seeded or left behind.
/// </summary>
[Trait("Category", "Integration")]
public class ClientTests
{
    private readonly ITestOutputHelper _output;
    private readonly IntegrationTestConfiguration _testConfiguration;

    public ClientTests(ITestOutputHelper output)
    {
        _output = output;
        _testConfiguration = IntegrationTestingUtils.TestConfiguration;

        Skip.If(string.IsNullOrEmpty(_testConfiguration.Account),
            $"Cannot execute test configuration from environment variable `{IntegrationTestingUtils.SnowflakeTestConfigVariable}`");
        Skip.If(string.IsNullOrWhiteSpace(_testConfiguration.Metadata.Catalog)
                || string.IsNullOrWhiteSpace(_testConfiguration.Metadata.Schema),
            "ClientTests require a writable database/schema (metadata.catalog / metadata.schema).");
    }

    /// <summary>
    /// The reader walks every row of a multi-row result via <c>Read()</c> and returns them in the
    /// queried ORDER BY sequence. (CLR string conversion itself is covered by the type-contract
    /// theory; this test is about row iteration and ordering.)
    /// </summary>
    [SkippableFact]
    public void Reader_IteratesAllRowsInOrder()
    {
        // Given a two-row table queried with an explicit order
        using var connection = OpenClientConnection();
        string table = CreateAndPopulateTempTable(connection);
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT NAME FROM {table} ORDER BY ID";

        // When every row is read in sequence
        using var reader = command.ExecuteReader();
        var names = new List<string?>();
        while (reader.Read())
            names.Add(reader.GetString(0));

        // Then they come back in the queried order
        Assert.Equal(new[] { "alpha", "beta" }, names);
    }

    /// <summary>
    /// The reader exposes the result's column count and names (<c>FieldCount</c> / <c>GetName</c>
    /// by ordinal).
    /// </summary>
    [SkippableFact]
    public void Reader_ExposesColumnMetadata()
    {
        // Given a five-column result
        using var connection = OpenClientConnection();
        string table = CreateAndPopulateTempTable(connection);
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT ID, NAME, ACTIVE, AMOUNT, PRICE FROM {table}";

        // When the reader is opened
        using var reader = command.ExecuteReader();

        // Then it exposes the column count and names
        Assert.Equal(5, reader.FieldCount);
        Assert.Equal("ID", reader.GetName(0));
        Assert.Equal("NAME", reader.GetName(1));
        Assert.Equal("ACTIVE", reader.GetName(2));
        Assert.Equal("AMOUNT", reader.GetName(3));
        Assert.Equal("PRICE", reader.GetName(4));
    }

    [SkippableFact]
    public void Reader_CountStar_ReturnsRowCount()
    {
        // Given a two-row table (AdbcCommand.ExecuteScalar throws NotImplementedException in the
        // client layer, so the single aggregate value is read via the reader instead)
        using var connection = OpenClientConnection();
        string table = CreateAndPopulateTempTable(connection);
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {table}";

        // When the COUNT(*) is read
        using var reader = command.ExecuteReader();

        // Then it returns the row count
        Assert.True(reader.Read());
        Assert.Equal(2, Convert.ToInt32(reader.GetValue(0)));
    }

    [SkippableFact]
    public void ConnectionString_OpensAndQueries()
    {
        // Given driver parameters round-tripped through a connection string (rather than passed
        // directly) — this is what exercises the client's connection-string parsing path
        var driver = IntegrationTestingUtils.GetSnowflakeAdbcDriver(_testConfiguration, out var parameters);
        var builder = new DbConnectionStringBuilder(useOdbcRules: true);
        foreach (var kvp in parameters)
            builder[kvp.Key] = kvp.Value;

        // When a connection is opened from that string and queried
        using var connection = new AdbcClient.AdbcConnection(builder.ConnectionString) { AdbcDriver = driver };
        connection.Open();
        string table = CreateAndPopulateTempTable(connection);
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {table}";
        using var reader = command.ExecuteReader();

        // Then it connects and returns results
        Assert.True(reader.Read());
        Assert.Equal(2, Convert.ToInt32(reader.GetValue(0)));
    }

    /// <summary>
    /// <c>ExecuteNonQuery</c> runs DML and returns the affected-row count (2 inserted rows here).
    /// </summary>
    [SkippableFact]
    public void ExecuteNonQuery_RunsDml()
    {
        // Given an empty temporary table
        using var connection = OpenClientConnection();
        string table = $"{_testConfiguration.Metadata.Catalog}.{_testConfiguration.Metadata.Schema}.CLIENT_DML_{Guid.NewGuid():N}";
        using (var create = connection.CreateCommand())
        {
            create.CommandText = $"CREATE TEMPORARY TABLE {table} (id INT)";
            create.ExecuteNonQuery();
        }

        // When two rows are inserted via ExecuteNonQuery
        using var insert = connection.CreateCommand();
        insert.CommandText = $"INSERT INTO {table} (id) VALUES (1), (2)";
        int affected = insert.ExecuteNonQuery();

        // Then it reports the affected-row count
        _output.WriteLine($"ExecuteNonQuery reported {affected} affected rows");
        Assert.Equal(2, affected);
    }

    /// <summary>
    /// The end-to-end CLR type contract: the <see cref="Type"/> a consumer gets from the reader
    /// for each Snowflake type. The integer cases are the payoff of the driver's precision-driven
    /// Arrow sizing (scale-0 NUMBER ≤ 9 → int, ≤ 18 → long, else Decimal128); the rest is the
    /// upstream client's Arrow → CLR mapping, pinned here only as the seam. A NUMBER that can
    /// exceed long surfaces as <see cref="System.Data.SqlTypes.SqlDecimal"/> (the default
    /// <c>DecimalBehavior</c>): it holds the full NUMBER(38) range but is not IConvertible, so read
    /// it via <c>((SqlDecimal)value).Value</c> rather than <c>Convert.*</c>. Value correctness of
    /// the decode is owned by <see cref="TypeDecodingTests"/>; the client's own conversion matrix
    /// (and the <c>DecimalBehavior</c> toggle) is covered upstream.
    /// </summary>
    [SkippableTheory]
    [InlineData("123::NUMBER(9,0)", typeof(int))]
    [InlineData("123::NUMBER(18,0)", typeof(long))]
    [InlineData("123::NUMBER(38,0)", typeof(System.Data.SqlTypes.SqlDecimal))]
    [InlineData("9.99::NUMBER(10,2)", typeof(System.Data.SqlTypes.SqlDecimal))]
    [InlineData("'x'::VARCHAR", typeof(string))]
    [InlineData("TRUE::BOOLEAN", typeof(bool))]
    [InlineData("1.5::FLOAT", typeof(double))]
    public void Reader_YieldsExpectedClrTypePerSnowflakeType(string sqlLiteral, Type expectedClrType)
    {
        // Given a single typed literal
        using var connection = OpenClientConnection();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {sqlLiteral} AS V";

        // When it is read through the client
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());

        // Then the CLR value has the expected type
        Assert.Equal(expectedClrType, reader.GetValue(0).GetType());
    }

    private AdbcClient.AdbcConnection OpenClientConnection()
    {
        var driver = IntegrationTestingUtils.GetSnowflakeAdbcDriver(_testConfiguration, out var parameters);
        var connection = new AdbcClient.AdbcConnection(driver, parameters, new Dictionary<string, string>());
        connection.Open();
        return connection;
    }

    /// <summary>
    /// Creates a session-scoped temporary table with a row of each common column type and two
    /// rows of data, returning the fully-qualified table name. Self-contained: the temporary
    /// table is visible only on this connection's session and is dropped when it closes.
    /// </summary>
    private string CreateAndPopulateTempTable(AdbcClient.AdbcConnection connection)
    {
        string table = $"{_testConfiguration.Metadata.Catalog}.{_testConfiguration.Metadata.Schema}.CLIENT_T_{Guid.NewGuid():N}";

        using (var create = connection.CreateCommand())
        {
            create.CommandText =
                $"CREATE TEMPORARY TABLE {table} " +
                "(ID NUMBER(38,0), NAME VARCHAR, ACTIVE BOOLEAN, AMOUNT FLOAT, PRICE NUMBER(10,2))";
            create.ExecuteNonQuery();
        }

        using (var insert = connection.CreateCommand())
        {
            insert.CommandText =
                $"INSERT INTO {table} (ID, NAME, ACTIVE, AMOUNT, PRICE) VALUES " +
                "(1, 'alpha', TRUE, 3.5, 9.99), (2, 'beta', FALSE, 7.25, 0.01)";
            insert.ExecuteNonQuery();
        }

        return table;
    }
}
