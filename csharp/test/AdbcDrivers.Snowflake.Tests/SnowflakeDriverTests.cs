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
using System.Threading.Tasks;
using AdbcDrivers.Snowflake;
using FluentAssertions;
using Xunit;

using Apache.Arrow;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Snowflake.Tests;

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
        await using var database = await _driver.OpenAsync(parameters);

        // Then ConnectAsync throws, complaining about the missing account
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => database.ConnectAsync(null!));
        ex.Message.Should().Contain("account");
    }

    [Fact]
    public async Task OpenAsync_WithValidParameters_ShouldReturnDatabase()
    {
        // Given valid connection parameters
        var parameters = new Dictionary<string, string>
        {
            ["account"] = "testaccount",
            ["user"] = "testuser",
            ["password"] = "testpass"
        };

        // When the driver opens a database
        await using var database = await _driver.OpenAsync(parameters);

        // Then a SnowflakeDatabase is returned
        database.Should().NotBeNull();
        database.Should().BeOfType<SnowflakeDatabase>();
    }

    [Fact]
    public async Task OpenAsync_WithNullParameters_ShouldThrowArgumentNullException()
    {
        // When / Then opening with null parameters fails the task with ArgumentNullException
        Task<AdbcDatabase> open = _driver.OpenAsync(null!);
        var exception = await Assert.ThrowsAsync<ArgumentNullException>(() => open);
        exception.ParamName.Should().Be("parameters");
    }

    [Fact]
    public async Task OpenAsync_WithCancelledToken_IsCancelled()
    {
        using var cts = new System.Threading.CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => _driver.OpenAsync(new Dictionary<string, string> { ["account"] = "testaccount" }, cts.Token));
    }

    [Fact]
    public async Task OpenAsync_WithMissingRequiredParameters_ShouldThrowArgumentException()
    {
        // Given parameters missing the required account
        var parameters = new Dictionary<string, string>
        {
            ["user"] = "testuser",
            ["password"] = "testpass"
        };

        // When the database is opened and a connection attempted
        await using var database = await _driver.OpenAsync(parameters);

        // Then ConnectAsync throws, complaining about the missing account
        var exception = await Assert.ThrowsAsync<ArgumentException>(() => database.ConnectAsync(null));
        exception.Message.Should().Contain("account");
    }

    [Fact]
    public void Sync_OpenAndConnect_SurfaceTheSameErrors()
    {
        // The sync wrappers must rethrow the original exception, not an AggregateException.
        var exception = Assert.Throws<ArgumentNullException>(() => _driver.Open((IReadOnlyDictionary<string, string>)null!));
        exception.ParamName.Should().Be("parameters");

        using var database = _driver.Open(new Dictionary<string, string> { ["invalid"] = "parameter" });
        database.Should().BeOfType<SnowflakeDatabase>();
        Assert.Throws<ArgumentException>(() => database.Connect(null)).Message.Should().Contain("account");
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
