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
using Xunit;

namespace AdbcDrivers.Snowflake.Native.Tests.Integration;

/// <summary>
/// Live connectivity / lifecycle smoke for the native Snowflake driver: that the
/// <c>Driver → Database → Connection</c> open path actually works against a real account.
/// This is the fast first-line diagnostic that isolates "cannot connect" from "a query failed" —
/// every other integration suite relies on this path as setup but does not assert it directly.
///
/// Deeper behaviour lives in the concern-specific suites: statement execution / update / bind in
/// <see cref="StatementTests"/>; query results and the metadata methods (GetObjects /
/// GetTableSchema / GetTableTypes / GetInfo) in <see cref="QueryAndMetadataTests"/>; over-the-wire
/// type decoding in <see cref="TypeDecodingTests"/>; and the ADO.NET client layer in
/// <see cref="ClientTests"/>. Offline parameter validation is in <c>SnowflakeDriverTests</c>.
///
/// Requires a live Snowflake instance; set SNOWFLAKE_TEST_CONFIG_FILE.
/// </summary>
[Trait("Category", "Integration")]
public class ConnectionTests
{
    private readonly IntegrationTestConfiguration _testConfiguration;

    public ConnectionTests()
    {
        _testConfiguration = IntegrationTestingUtils.TestConfiguration;

        Skip.If(string.IsNullOrEmpty(_testConfiguration.Account),
            $"Cannot execute test configuration from environment variable `{IntegrationTestingUtils.SnowflakeTestConfigVariable}`");
    }

    [SkippableFact]
    public void DisposingDatabase_ClosesSessionWithoutError()
    {
        // Given a connected database (a live server-side session)
        var driver = IntegrationTestingUtils.GetSnowflakeAdbcDriver(_testConfiguration, out var parameters);
        var database = driver.Open(parameters);
        var connection = database.Connect(new Dictionary<string, string>());
        connection.Dispose();   // returns the pooled connection to idle — session kept for reuse

        // When the database is disposed, the pool discards its pooled connections and best-effort
        // closes their server-side sessions (POST /session?delete=true).
        // Then it completes without throwing.
        var exception = Record.Exception(database.Dispose);
        Assert.Null(exception);
    }

    [SkippableFact]
    public void OpenAndConnect_Succeeds()
    {
        // Given driver parameters
        var driver = IntegrationTestingUtils.GetSnowflakeAdbcDriver(_testConfiguration, out var parameters);
        using var database = driver.Open(parameters);

        // When a connection is opened
        using var connection = database.Connect(new Dictionary<string, string>());

        // Then it succeeds
        Assert.NotNull(connection);
    }

    [SkippableFact]
    public void OpenAndConnect_WithKeyPair_Succeeds()
    {
        // Requires an auth_jwt block in the test configuration (a user with an RSA public key
        // registered via ALTER USER ... SET RSA_PUBLIC_KEY and its private key on disk/inline).
        JwtAuthentication? jwt = _testConfiguration.Authentication.SnowflakeJwt;
        Skip.If(jwt is null, "No auth_jwt block in the test configuration");

        // Given a configuration carrying ONLY key-pair credentials, so the parameter mapping
        // cannot fall back to password auth. The top-level role is deliberately NOT copied:
        // it belongs to the password test user; the service user runs under its own
        // DEFAULT_ROLE. (Database/schema are omitted for the same reason — this test only
        // needs SELECT 1.)
        var keyPairOnly = new IntegrationTestConfiguration
        {
            Account = _testConfiguration.Account,
            Warehouse = _testConfiguration.Warehouse,
            TlsSkipVerify = _testConfiguration.TlsSkipVerify,
            Authentication = new SnowflakeAuthentication { SnowflakeJwt = jwt },
        };

        // When a connection is opened and a query is run over the JWT-authenticated session
        var driver = IntegrationTestingUtils.GetSnowflakeAdbcDriver(keyPairOnly, out var parameters);
        using var database = driver.Open(parameters);
        using var connection = database.Connect(new Dictionary<string, string>());
        using var statement = connection.CreateStatement();
        statement.SqlQuery = "SELECT 1";
        var result = statement.ExecuteQuery();

        // Then the login and the query both succeed
        Assert.NotNull(result.Stream);
        result.Stream.Dispose();
    }

    [SkippableFact]
    public void OpenAndConnect_WithPat_Succeeds()
    {
        // Requires an auth_pat block in the test configuration: a programmatic access token
        // for a user that is subject to a network policy (Snowflake rejects PAT logins
        // otherwise).
        PatAuthentication? pat = _testConfiguration.Authentication.Pat;
        Skip.If(pat is null, "No auth_pat block in the test configuration");

        // Given a configuration carrying ONLY the PAT credentials — no role/database/schema,
        // so the service user's own defaults apply (as with the key-pair test).
        var patOnly = new IntegrationTestConfiguration
        {
            Account = _testConfiguration.Account,
            Warehouse = _testConfiguration.Warehouse,
            TlsSkipVerify = _testConfiguration.TlsSkipVerify,
            Authentication = new SnowflakeAuthentication { Pat = pat },
        };

        // When a connection is opened and a query is run over the PAT-authenticated session
        var driver = IntegrationTestingUtils.GetSnowflakeAdbcDriver(patOnly, out var parameters);
        using var database = driver.Open(parameters);
        using var connection = database.Connect(new Dictionary<string, string>());
        using var statement = connection.CreateStatement();
        statement.SqlQuery = "SELECT 1";
        var result = statement.ExecuteQuery();

        // Then the login and the query both succeed
        Assert.NotNull(result.Stream);
        result.Stream.Dispose();
    }
}
