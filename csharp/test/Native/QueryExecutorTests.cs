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
using System.Text.Json;
using AdbcDrivers.Snowflake.Native.Services.Query;
using AdbcDrivers.Snowflake.Native.Services.Transport;
using Xunit;

namespace AdbcDrivers.Snowflake.Native.Tests;

/// <summary>
/// Offline unit tests for <see cref="QueryExecutor"/> result classification, especially
/// the DML affected-row detection that parses the JSON row-count summary.
/// </summary>
[Trait("Category", "Unit")]
public class QueryExecutorTests
{
    private static SnowflakeQueryResponse Response(string[] columnNames, params string[][] rows)
    {
        var rowType = new List<RowType>();
        foreach (string name in columnNames)
            rowType.Add(new RowType { Name = name });

        var rowSet = new List<List<string>>();
        foreach (string[] row in rows)
            rowSet.Add([..row]);

        return new SnowflakeQueryResponse { RowType = rowType, RowSet = rowSet };
    }

    [Fact]
    public void TryGetDmlAffectedRows_Insert_ReturnsCount()
    {
        SnowflakeQueryResponse data = Response(["number of rows inserted"], ["2"]);

        Assert.True(QueryResultFactory.TryGetDmlAffectedRows(data, out long affected));
        Assert.Equal(2, affected);
    }

    [Fact]
    public void TryGetDmlAffectedRows_Merge_SumsAllCountColumns()
    {
        SnowflakeQueryResponse data = Response(
            ["number of rows inserted", "number of rows updated"],
            ["3", "2"]);

        Assert.True(QueryResultFactory.TryGetDmlAffectedRows(data, out long affected));
        Assert.Equal(5, affected);
    }

    [Fact]
    public void TryGetDmlAffectedRows_ReadsRowSetNotReturnedCount()
    {
        // A DELETE affecting 5 rows: RowSet carries 5 even though the payload is a single row.
        SnowflakeQueryResponse data = Response(["number of rows deleted"], ["5"]);
        data.Returned = 1;

        Assert.True(QueryResultFactory.TryGetDmlAffectedRows(data, out long affected));
        Assert.Equal(5, affected);
    }

    [Fact]
    public void TryGetDmlAffectedRows_NonDmlColumns_ReturnsFalse()
    {
        SnowflakeQueryResponse data = Response(["MY_COLUMN"], ["1"]);

        Assert.False(QueryResultFactory.TryGetDmlAffectedRows(data, out long affected));
        Assert.Equal(0, affected);
    }

    [Fact]
    public void TryGetDmlAffectedRows_EmptyResult_ReturnsFalse()
    {
        Assert.False(QueryResultFactory.TryGetDmlAffectedRows(new SnowflakeQueryResponse(), out long affected));
        Assert.Equal(0, affected);
    }

    [Fact]
    public void IsSessionExpired_TrueForExpiredTokenCode()
    {
        var response = new ApiResponse<SnowflakeQueryResponse> { Success = false, Code = "390112" };
        Assert.True(QueryExecutor.IsSessionExpired(response));
    }

    [Theory]
    [InlineData(true, "390112")]  // a successful response is never "expired", whatever the code
    [InlineData(false, "000")]    // a different (non-session-expired) error
    [InlineData(false, null)]     // no code at all
    public void IsSessionExpired_FalseOtherwise(bool success, string? code)
    {
        var response = new ApiResponse<SnowflakeQueryResponse> { Success = success, Code = code };
        Assert.False(QueryExecutor.IsSessionExpired(response));
    }

    [Fact]
    public void RenewSessionBody_SerializesAsRenewRequest()
    {
        string json = JsonSerializer.Serialize(new SnowflakeRenewSessionBody { OldSessionToken = "old-token" });

        Assert.Contains("\"oldSessionToken\":\"old-token\"", json);
        Assert.Contains("\"requestType\":\"RENEW\"", json);
    }
}
