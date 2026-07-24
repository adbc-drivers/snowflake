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
using AdbcDrivers.Snowflake.Native.Configuration;
using Xunit;

using Apache.Arrow;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Snowflake.Native.Tests.Configuration;

[Trait("Category", "Unit")]
public class ConnectionStringParserTests
{
    private static Dictionary<string, string> ParseConnectionString(string connectionString)
    {
        var parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var pairs = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries);

        foreach (var pair in pairs)
        {
            var parts = pair.Split('=', 2);
            if (parts.Length == 2)
            {
                parameters[parts[0].Trim()] = parts[1].Trim();
            }
        }

        return parameters;
    }

    [Fact]
    public void Parse_WithValidBasicConnectionString_ShouldReturnValidConfig()
    {
        // Arrange
        var parameters = ParseConnectionString("adbc.snowflake.sql.account=testaccount;username=testuser;password=testpass;adbc.snowflake.sql.db=testdb");

        // Act
        var config = ConnectionStringParser.ParseParameters(parameters);

        // Assert
        Assert.NotNull(config);
        Assert.Equal("testaccount", config.Account);
        Assert.Equal("testuser", config.User);
        Assert.Equal("testdb", config.Database);
        Assert.Equal(AuthenticationType.UsernamePassword, config.Authentication.Type);
        Assert.Equal("testpass", config.Authentication.Password);
    }

    [Theory]
    [InlineData("auth_jwt")]           // canonical (ADBC Snowflake driver reference)
    [InlineData("snowflake_jwt")]      // connector-net alias
    [InlineData("jwt")]                // shorthand alias
    public void Parse_WithKeyPairAuthentication_ShouldReturnValidConfig(string authType)
    {
        // Arrange
        var parameters = ParseConnectionString($"adbc.snowflake.sql.account=testaccount;username=testuser;adbc.snowflake.sql.auth_type={authType};adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value=PRIVATE_KEY_CONTENT");

        // Act
        var config = ConnectionStringParser.ParseParameters(parameters);

        // Assert
        Assert.NotNull(config);
        Assert.Equal("testaccount", config.Account);
        Assert.Equal("testuser", config.User);
        Assert.Equal(AuthenticationType.KeyPair, config.Authentication.Type);
        Assert.Equal("PRIVATE_KEY_CONTENT", config.Authentication.PrivateKey);
    }

    [Theory]
    [InlineData("auth_snowflake")]
    [InlineData("snowflake")]
    public void Parse_WithExplicitPasswordAuthType_ShouldReturnValidConfig(string authType)
    {
        var parameters = ParseConnectionString(
            $"adbc.snowflake.sql.account=testaccount;username=testuser;password=testpass;adbc.snowflake.sql.auth_type={authType}");

        var config = ConnectionStringParser.ParseParameters(parameters);

        Assert.Equal(AuthenticationType.UsernamePassword, config.Authentication.Type);
    }

    [Theory]
    [InlineData("auth_oauth")]
    [InlineData("oauth")]
    public void Parse_WithOAuthAuthentication_ShouldReturnValidConfig(string authType)
    {
        // Arrange
        var parameters = ParseConnectionString($"adbc.snowflake.sql.account=testaccount;username=testuser;adbc.snowflake.sql.auth_type={authType};adbc.snowflake.sql.client_option.auth_token=test_token");

        // Act
        var config = ConnectionStringParser.ParseParameters(parameters);

        // Assert
        Assert.NotNull(config);
        Assert.Equal("testaccount", config.Account);
        Assert.Equal("testuser", config.User);
        Assert.Equal(AuthenticationType.OAuth, config.Authentication.Type);
        Assert.Equal("test_token", config.Authentication.Token);
    }

    [Theory]
    [InlineData("auth_pat")]
    [InlineData("programmatic_access_token")]
    [InlineData("pat")]
    public void Parse_WithPatAuthentication_ShouldReturnValidConfig(string authType)
    {
        // Arrange - a PAT rides the same auth_token option as OAuth; auth_type selects how
        // it is presented to Snowflake.
        var parameters = ParseConnectionString(
            $"adbc.snowflake.sql.account=testaccount;username=testuser;adbc.snowflake.sql.auth_type={authType};adbc.snowflake.sql.client_option.auth_token=test_pat");

        // Act
        var config = ConnectionStringParser.ParseParameters(parameters);

        // Assert
        Assert.Equal(AuthenticationType.Pat, config.Authentication.Type);
        Assert.Equal("test_pat", config.Authentication.Token);
    }

    [Theory]
    [InlineData("auth_okta")]
    [InlineData("auth_mfa")]
    [InlineData("auth_wif")]
    public void Parse_WithRecognizedButUnsupportedAuthType_SaysSoExplicitly(string authType)
    {
        // Canonical ADBC values the driver doesn't implement yet must be distinguishable
        // from a typo.
        var parameters = ParseConnectionString(
            $"adbc.snowflake.sql.account=testaccount;username=testuser;adbc.snowflake.sql.auth_type={authType}");

        var ex = Assert.Throws<ArgumentException>(() => ConnectionStringParser.ParseParameters(parameters));
        Assert.Contains("not supported by this driver yet", ex.Message);
    }

    [Fact]
    public void Parse_WithRequestTimeout_SetsQueryTimeout()
    {
        // Arrange - request_timeout maps to the statement/query timeout (STATEMENT_TIMEOUT_IN_SECONDS)
        var parameters = ParseConnectionString("adbc.snowflake.sql.account=testaccount;username=testuser;password=testpass;adbc.snowflake.sql.client_option.request_timeout=300");

        // Act
        var config = ConnectionStringParser.ParseParameters(parameters);

        // Assert
        Assert.NotNull(config);
        Assert.Equal(TimeSpan.FromSeconds(300), config.QueryTimeout);
    }

    [Fact]
    public void Parse_WithoutKeepAlive_DefaultsToOffAndOneHour()
    {
        var parameters = ParseConnectionString("adbc.snowflake.sql.account=testaccount;username=testuser;password=testpass");

        var config = ConnectionStringParser.ParseParameters(parameters);

        Assert.False(config.ClientSessionKeepAlive);
        Assert.Equal(TimeSpan.FromHours(1), config.HeartbeatFrequency);
    }

    [Fact]
    public void Parse_WithKeepAliveEnabled_SetsFlagAndFrequency()
    {
        // Given keep-alive on with a 1800s (30m) heartbeat frequency inside the allowed band
        var parameters = ParseConnectionString(
            "adbc.snowflake.sql.account=testaccount;username=testuser;password=testpass;" +
            "adbc.snowflake.sql.client_option.keep_session_alive=true;" +
            "adbc.snowflake.sql.client_option.keep_session_alive_heartbeat_frequency=1800");

        var config = ConnectionStringParser.ParseParameters(parameters);

        Assert.True(config.ClientSessionKeepAlive);
        Assert.Equal(TimeSpan.FromMinutes(30), config.HeartbeatFrequency);
    }

    [Fact]
    public void Parse_WithOutOfRangeHeartbeatFrequency_ClampsToBand()
    {
        // Given a frequency far below the 15-minute floor, it clamps up rather than letting the
        // session be hammered (or, at the other extreme, lapse).
        var parameters = ParseConnectionString(
            "adbc.snowflake.sql.account=testaccount;username=testuser;password=testpass;" +
            "adbc.snowflake.sql.client_option.keep_session_alive=true;" +
            "adbc.snowflake.sql.client_option.keep_session_alive_heartbeat_frequency=5");

        var config = ConnectionStringParser.ParseParameters(parameters);

        Assert.Equal(TimeSpan.FromMinutes(15), config.HeartbeatFrequency);
    }

    [Fact]
    public void Parse_WithoutAcquireTimeout_DefaultsTo120Seconds()
    {
        var parameters = ParseConnectionString("adbc.snowflake.sql.account=testaccount;username=testuser;password=testpass");

        var config = ConnectionStringParser.ParseParameters(parameters);

        Assert.Equal(TimeSpan.FromSeconds(120), config.PoolConfig.AcquireTimeout);
    }

    [Fact]
    public void Parse_WithPoolAcquireTimeout_SetsAcquireTimeout()
    {
        var parameters = ParseConnectionString("adbc.snowflake.sql.account=testaccount;username=testuser;password=testpass;adbc.snowflake.pool.acquire_timeout=30");

        var config = ConnectionStringParser.ParseParameters(parameters);

        Assert.Equal(TimeSpan.FromSeconds(30), config.PoolConfig.AcquireTimeout);
        Assert.Equal(TimeSpan.FromMinutes(10), config.PoolConfig.IdleTimeout); // unchanged (default)
    }

    [Fact]
    public void Parse_WithPoolMaxSize_SetsMaxPoolSize()
    {
        var parameters = ParseConnectionString("adbc.snowflake.sql.account=testaccount;username=testuser;password=testpass;adbc.snowflake.pool.max_size=20");

        var config = ConnectionStringParser.ParseParameters(parameters);

        Assert.Equal(20, config.PoolConfig.MaxPoolSize);
    }

    [Fact]
    public void Parse_WithQueryTag_SetsConnectionDefault()
    {
        var parameters = ParseConnectionString(
            "adbc.snowflake.sql.account=testaccount;username=testuser;password=testpass;" +
            "adbc.snowflake.statement.query_tag=etl-nightly");

        var config = ConnectionStringParser.ParseParameters(parameters);

        Assert.Equal("etl-nightly", config.QueryTag);
    }

    [Fact]
    public void Parse_WithoutQueryTag_LeavesItNull()
    {
        var parameters = ParseConnectionString("adbc.snowflake.sql.account=testaccount;username=testuser;password=testpass");

        var config = ConnectionStringParser.ParseParameters(parameters);

        Assert.Null(config.QueryTag);
    }

    [Fact]
    public void Parse_WithLoginTimeoutAndPrefetch_SetsBoth()
    {
        var parameters = ParseConnectionString(
            "adbc.snowflake.sql.account=testaccount;username=testuser;password=testpass;" +
            "adbc.snowflake.sql.client_option.login_timeout=45;adbc.snowflake.rpc.prefetch_concurrency=4");

        var config = ConnectionStringParser.ParseParameters(parameters);

        Assert.Equal(TimeSpan.FromSeconds(45), config.LoginTimeout);
        Assert.Equal(4, config.PrefetchConcurrency);
    }

    [Fact]
    public void Parse_WithoutOptionalTimeouts_UsesDefaults()
    {
        var parameters = ParseConnectionString("adbc.snowflake.sql.account=testaccount;username=testuser;password=testpass");

        var config = ConnectionStringParser.ParseParameters(parameters);

        Assert.Equal(TimeSpan.FromSeconds(60), config.LoginTimeout);
        Assert.Equal(10, config.PrefetchConcurrency);
    }

    [Fact]
    public void Parse_WithSsoProperties_ShouldReturnValidConfig()
    {
        // Arrange - SSO not currently supported in ADBC standard, removing this test
        var parameters = ParseConnectionString("adbc.snowflake.sql.account=testaccount;username=testuser;adbc.snowflake.sql.auth_type=auth_ext_browser");

        // Act
        var config = ConnectionStringParser.ParseParameters(parameters);

        // Assert
        Assert.NotNull(config);
        Assert.Equal(AuthenticationType.ExternalBrowser, config.Authentication.Type);
    }

    [Fact]
    public void Parse_WithStandardConnectionCatalogAndSchema_SetsDatabaseAndSchema()
    {
        // The canonical ADBC connection options map to Snowflake's current database/schema
        var parameters = ParseConnectionString(
            "adbc.snowflake.sql.account=testaccount;username=testuser;password=testpass;" +
            "adbc.connection.catalog=MYDB;adbc.connection.db_schema=MYSCHEMA");

        var config = ConnectionStringParser.ParseParameters(parameters);

        Assert.Equal("MYDB", config.Database);
        Assert.Equal("MYSCHEMA", config.Schema);
    }

    [Fact]
    public void Parse_StandardConnectionCatalog_TakesPrecedenceOverDriverAlias()
    {
        // adbc.connection.catalog wins over the adbc.snowflake.sql.db alias when both are present
        var parameters = ParseConnectionString(
            "adbc.snowflake.sql.account=testaccount;username=testuser;password=testpass;" +
            "adbc.snowflake.sql.db=ALIAS_DB;adbc.connection.catalog=CANONICAL_DB");

        var config = ConnectionStringParser.ParseParameters(parameters);

        Assert.Equal("CANONICAL_DB", config.Database);
    }

    [Fact]
    public void Parse_ConnectionCatalog_OverridesDatabaseDefaultSchema()
    {
        // A per-connection catalog/schema (via Connect) overrides the database-level default
        var databaseDefaults = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "adbc.snowflake.sql.account", "testaccount" },
            { "username", "testuser" },
            { "password", "testpass" },
            { "adbc.snowflake.sql.db", "DEFAULT_DB" },
            { "adbc.snowflake.sql.schema", "DEFAULT_SCHEMA" },
        };
        var connectionParams = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "adbc.connection.catalog", "CONN_DB" },
            { "adbc.connection.db_schema", "CONN_SCHEMA" },
        };

        var config = ConnectionStringParser.ParseParameters(connectionParams, databaseDefaults);

        Assert.Equal("CONN_DB", config.Database);
        Assert.Equal("CONN_SCHEMA", config.Schema);
    }

    [Fact]
    public void Parse_WithTlsSkipVerify_SetsFlag()
    {
        var parameters = ParseConnectionString(
            "adbc.snowflake.sql.account=testaccount;username=testuser;password=testpass;" +
            "adbc.snowflake.sql.client_option.tls_skip_verify=true");

        var config = ConnectionStringParser.ParseParameters(parameters);

        Assert.True(config.Network.TlsSkipVerify);
    }

    [Fact]
    public void Parse_WithNullParameters_ShouldThrowArgumentException()
    {
        // Act & Assert - null parameters result in empty dictionary which fails validation
        var exception = Assert.Throws<ArgumentException>(() => ConnectionStringParser.ParseParameters(null));
        Assert.Contains("account", exception.Message);
    }

    [Fact]
    public void Parse_WithMissingRequiredParameter_ShouldThrowArgumentException()
    {
        // Arrange
        var parameters = ParseConnectionString("username=testuser;password=testpass"); // Missing account

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() => ConnectionStringParser.ParseParameters(parameters));
        Assert.Contains("account", exception.Message);
    }

    [Fact]
    public void Parse_WithInvalidAuthenticator_ShouldThrowArgumentException()
    {
        // Arrange
        var parameters = ParseConnectionString("adbc.snowflake.sql.account=testaccount;username=testuser;adbc.snowflake.sql.auth_type=invalid_auth");

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() => ConnectionStringParser.ParseParameters(parameters));
        Assert.Contains("Unsupported auth_type", exception.Message);
    }

    [Fact]
    public void Parse_WithCaseInsensitiveParameters_ShouldReturnValidConfig()
    {
        // Arrange
        var parameters = ParseConnectionString("ADBC.SNOWFLAKE.SQL.ACCOUNT=testaccount;Username=testuser;PASSWORD=testpass;ADBC.SNOWFLAKE.SQL.DB=testdb");

        // Act
        var config = ConnectionStringParser.ParseParameters(parameters);

        // Assert
        Assert.NotNull(config);
        Assert.Equal("testaccount", config.Account);
        Assert.Equal("testuser", config.User);
        Assert.Equal("testdb", config.Database);
        Assert.Equal("testpass", config.Authentication.Password);
    }

    [Fact]
    public void ParseParameters_WithConnectionOverrides_ShouldMergeCorrectly()
    {
        // Arrange - database parameters
        var databaseParams = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "adbc.snowflake.sql.account", "testaccount" },
            { "username", "testuser" },
            { "password", "testpass" },
            { "adbc.snowflake.sql.warehouse", "DEFAULT_WH" },
            { "adbc.snowflake.sql.db", "DEFAULT_DB" }
        };

        // Connection-specific overrides
        var connectionParams = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "adbc.snowflake.sql.warehouse", "ANALYTICS_WH" },  // Override
            { "adbc.snowflake.sql.schema", "PUBLIC" }  // New parameter
        };

        // Act
        var config = ConnectionStringParser.ParseParameters(connectionParams, databaseParams);

        // Assert
        Assert.NotNull(config);
        Assert.Equal("testaccount", config.Account);
        Assert.Equal("testuser", config.User);
        Assert.Equal("ANALYTICS_WH", config.Warehouse); // Overridden value
        Assert.Equal("DEFAULT_DB", config.Database); // From database params
        Assert.Equal("PUBLIC", config.Schema); // From connection params
    }

    [Fact]
    public void ParseParameters_WithCaseInsensitiveMerge_ShouldHandleCorrectly()
    {
        // Arrange - test that case-insensitive merge works correctly
        var databaseParams = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "ADBC.SNOWFLAKE.SQL.ACCOUNT", "testaccount" },
            { "Username", "testuser" },
            { "ADBC.SNOWFLAKE.SQL.WAREHOUSE", "DEFAULT_WH" }
        };

        var connectionParams = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "adbc.snowflake.sql.warehouse", "OVERRIDE_WH" },  // Different casing, should override
            { "password", "testpass" }
        };

        // Act
        var config = ConnectionStringParser.ParseParameters(connectionParams, databaseParams);

        // Assert
        Assert.NotNull(config);
        Assert.Equal("testaccount", config.Account);
        Assert.Equal("testuser", config.User);
        Assert.Equal("OVERRIDE_WH", config.Warehouse); // Should use connection override despite case difference
        Assert.Equal("testpass", config.Authentication.Password);
    }
}
