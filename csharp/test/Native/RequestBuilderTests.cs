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
using AdbcDrivers.Snowflake.Native.Services.Transport;
using Xunit;

namespace AdbcDrivers.Snowflake.Native.Tests;

/// <summary>
/// Offline unit tests for <see cref="RequestBuilder"/> request-body construction.
/// </summary>
[Trait("Category", "Unit")]
public class RequestBuilderTests
{
    private static SnowflakeQueryRequestBody BuildQuery(
        string statement,
        string? database = null,
        string? schema = null,
        string? warehouse = null,
        string? role = null,
        string? queryTag = null,
        int? timeout = null,
        Dictionary<string, SnowflakeBinding>? bindings = null,
        bool isMultiStatement = false,
        bool describeOnly = false)
        => RequestBuilder.BuildQueryRequest(
            statement, database, schema, warehouse, role, queryTag, timeout, bindings, isMultiStatement, describeOnly);

    [Fact]
    public void BuildQueryRequest_SetsCoreFieldsAndRequestsArrow()
    {
        SnowflakeQueryRequestBody request = BuildQuery("SELECT 1");

        Assert.Equal("SELECT 1", request.SqlText);
        Assert.False(request.AsyncExec);
        Assert.False(request.DescribeOnly);
        Assert.Null(request.Bindings);

        Assert.NotNull(request.Parameters);
        Assert.Equal("ARROW", request.Parameters!["DOTNET_QUERY_RESULT_FORMAT"]);
    }

    [Fact]
    public void BuildQueryRequest_IncludesSessionParameters()
    {
        SnowflakeQueryRequestBody request = BuildQuery(
            "SELECT 1", database: "DB", schema: "SC", warehouse: "WH", role: "R", timeout: 30);

        var parameters = request.Parameters!;
        Assert.Equal("DB", parameters["DATABASE"]);
        Assert.Equal("SC", parameters["SCHEMA"]);
        Assert.Equal("WH", parameters["WAREHOUSE"]);
        Assert.Equal("R", parameters["ROLE"]);
        Assert.Equal("30", parameters["STATEMENT_TIMEOUT_IN_SECONDS"]);
    }

    [Fact]
    public void BuildQueryRequest_IncludesQueryTag()
    {
        SnowflakeQueryRequestBody request = BuildQuery("SELECT 1", queryTag: "etl-nightly");
        Assert.Equal("etl-nightly", request.Parameters!["QUERY_TAG"]);
    }

    [Fact]
    public void BuildQueryRequest_WithoutQueryTag_OmitsIt()
    {
        SnowflakeQueryRequestBody request = BuildQuery("SELECT 1");
        Assert.False(request.Parameters!.ContainsKey("QUERY_TAG"));
    }

    [Fact]
    public void BuildQueryRequest_DescribeOnly_SetsFlag()
    {
        SnowflakeQueryRequestBody request = BuildQuery("SELECT 1", describeOnly: true);
        Assert.True(request.DescribeOnly);
    }

    [Fact]
    public void BuildQueryRequest_WithBindings_IncludesThem()
    {
        var bindings = new Dictionary<string, SnowflakeBinding> { ["1"] = new("TEXT", "x") };
        SnowflakeQueryRequestBody request = BuildQuery("SELECT ?", bindings: bindings);

        Assert.NotNull(request.Bindings);
        Assert.Same(bindings, request.Bindings);
        Assert.Equal("TEXT", request.Bindings!["1"].Type);
        Assert.Equal("x", request.Bindings!["1"].Value);
    }

    [Fact]
    public void BuildQueryRequest_MultiStatement_SetsCount()
    {
        SnowflakeQueryRequestBody request = BuildQuery("SELECT 1; SELECT 2", isMultiStatement: true);
        Assert.Equal("0", request.Parameters!["MULTI_STATEMENT_COUNT"]);
    }

    [Fact]
    public void BuildQueryRequest_EmptyStatement_Throws()
    {
        Assert.Throws<ArgumentException>(() => RequestBuilder.BuildQueryRequest(string.Empty));
    }

    [Fact]
    public void BuildCancelRequest_SetsRequestId()
    {
        SnowflakeCancelRequestBody request = RequestBuilder.BuildCancelRequest("request-123");
        Assert.Equal("request-123", request.RequestId);
    }

    [Fact]
    public void BuildCancelRequest_Empty_Throws()
    {
        Assert.Throws<ArgumentException>(() => RequestBuilder.BuildCancelRequest(string.Empty));
    }
}
