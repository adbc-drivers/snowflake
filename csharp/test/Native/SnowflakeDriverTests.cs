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
using FluentAssertions;
using Xunit;

using Apache.Arrow;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Snowflake.Native.Tests;

[Trait("Category", "Unit")]
public class SnowflakeDriverTests : IDisposable
{
    private readonly SnowflakeDriver _driver;

    public SnowflakeDriverTests()
    {
        _driver = new SnowflakeDriver();
    }

    public void Dispose()
    {
        _driver?.Dispose();
    }

    [Fact]
    public async Task OpenAsync_WithInvalidParameters_ShouldThrowArgumentException()
    {
        // Given invalid connection parameters
        var parameters = new Dictionary<string, string>
        {
            ["invalid"] = "parameter"
        };

        // When the database is opened and a connection attempted
        using SnowflakeDatabase database = (SnowflakeDatabase)_driver.Open(parameters);

        // Then ConnectAsync throws, complaining about the missing account
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => database.ConnectAsync(null!));
        ex.Message.Should().Contain("account");
    }

    [Fact]
    public void Open_WithValidParameters_ShouldReturnDatabase()
    {
        // Given valid connection parameters
        var parameters = new Dictionary<string, string>
        {
            ["account"] = "testaccount",
            ["user"] = "testuser",
            ["password"] = "testpass"
        };

        // When the driver opens a database
        using var database = _driver.Open(parameters);

        // Then a SnowflakeDatabase is returned
        database.Should().NotBeNull();
        database.Should().BeOfType<SnowflakeDatabase>();
    }

    [Fact]
    public void Open_WithNullParameters_ShouldThrowArgumentNullException()
    {
        // When / Then opening with null parameters throws ArgumentNullException
        var exception = Assert.Throws<ArgumentNullException>(() => _driver.Open((IReadOnlyDictionary<string, string>)null!));
        exception.ParamName.Should().Be("parameters");
    }

    [Fact]
    public void Open_WithInvalidParameters_ShouldThrowArgumentException()
    {
        // Given invalid connection parameters
        var parameters = new Dictionary<string, string>
        {
            ["invalid"] = "parameter"
        };

        // When the database is opened and a connection attempted
        var database = _driver.Open(parameters);

        // Then Connect throws, complaining about the missing account
        var exception = Assert.Throws<ArgumentException>(() => database.Connect(null));
        exception.Message.Should().Contain("account");
    }

    [Fact]
    public void Open_WithMissingRequiredParameters_ShouldThrowArgumentException()
    {
        // Given parameters missing the required account
        var parameters = new Dictionary<string, string>
        {
            ["user"] = "testuser",
            ["password"] = "testpass"
        };

        // When the database is opened and a connection attempted
        var database = _driver.Open(parameters);

        // Then Connect throws, complaining about the missing account
        var exception = Assert.Throws<ArgumentException>(() => database.Connect(null));
        exception.Message.Should().Contain("account");
    }

    [Fact]
    public void Dispose_ShouldNotThrow()
    {
        // When / Then disposing once does not throw
        var ex = Record.Exception(() => _driver.Dispose());
        Assert.Null(ex);
    }

    [Fact]
    public void Dispose_CalledMultipleTimes_ShouldNotThrow()
    {
        // When / Then disposing twice does not throw
        var ex = Record.Exception(() =>
        {
            _driver.Dispose();
            _driver.Dispose();
        });
        Assert.Null(ex);
    }
}
