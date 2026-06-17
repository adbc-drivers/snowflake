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
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;

namespace AdbcDrivers.Snowflake.Native.Tests.Integration;

internal static class IntegrationTestingUtils
{
    internal static readonly IntegrationTestConfiguration TestConfiguration;

    internal const string SnowflakeTestConfigVariable = "SNOWFLAKE_TEST_CONFIG_FILE";

    internal const string RunSlowTestsVariable = "SNOWFLAKE_RUN_SLOW_TESTS";

    /// <summary>
    /// True when deliberately slow tests (multi-minute wall-clock waits, e.g. the long-running
    /// query polling test) should run. Off by default so the ordinary suite stays fast; enable
    /// with <c>SNOWFLAKE_RUN_SLOW_TESTS=1</c> (or <c>true</c>).
    /// </summary>
    internal static bool RunSlowTests
    {
        get
        {
            string? value = Environment.GetEnvironmentVariable(RunSlowTestsVariable);
            return string.Equals(value, "1", StringComparison.Ordinal)
                || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
        }
    }

    static IntegrationTestingUtils()
    {
        TestConfiguration = new IntegrationTestConfiguration();
        if (!string.IsNullOrEmpty(TestConfiguration.Account)) return;
        try
        {
            TestConfiguration = Utils.LoadTestConfiguration<IntegrationTestConfiguration>(SnowflakeTestConfigVariable);
        }
        catch (InvalidOperationException ex)
        {
            Console.WriteLine($"Cannot load test configuration from environment variable `{SnowflakeTestConfigVariable}`");
            Console.WriteLine(ex.Message);
            TestConfiguration = new IntegrationTestConfiguration();
        }
    }

    /// <summary>
    /// Gets the native Snowflake ADBC driver with settings from the
    /// <see cref="IntegrationTestConfiguration"/>.
    /// </summary>
    /// <param name="testConfiguration"></param>
    /// <param name="parameters"></param>
    /// <returns></returns>
    internal static AdbcDriver GetSnowflakeAdbcDriver(
        IntegrationTestConfiguration testConfiguration,
        out Dictionary<string, string> parameters)
    {
        parameters = new Dictionary<string, string>
        {
            { "adbc.snowflake.sql.account", Parameter(testConfiguration.Account, "account") }
        };

        // Add username if provided (not required for OAuth)
        if (!string.IsNullOrWhiteSpace(testConfiguration.User))
        {
            parameters["username"] = testConfiguration.User;
        }

        // Add authentication
        if (testConfiguration.Authentication.Default is not null)
        {
            parameters["username"] = Parameter(testConfiguration.Authentication.Default.User, "user");
            parameters["password"] = Parameter(testConfiguration.Authentication.Default.Password, "password");
        }
        else if (testConfiguration.Authentication.SnowflakeJwt is not null)
        {
            parameters["username"] = Parameter(testConfiguration.Authentication.SnowflakeJwt.User, "user");
            parameters["adbc.snowflake.sql.auth_type"] = "auth_jwt";

            if (!string.IsNullOrWhiteSpace(testConfiguration.Authentication.SnowflakeJwt.PrivateKeyFile))
            {
                parameters["adbc.snowflake.sql.client_option.jwt_private_key"] = testConfiguration.Authentication.SnowflakeJwt.PrivateKeyFile;
            }
            else if (!string.IsNullOrWhiteSpace(testConfiguration.Authentication.SnowflakeJwt.PrivateKey))
            {
                parameters["adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value"] = testConfiguration.Authentication.SnowflakeJwt.PrivateKey;
            }

            if (!string.IsNullOrWhiteSpace(testConfiguration.Authentication.SnowflakeJwt.PrivateKeyPassPhrase))
            {
                parameters["adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_password"] = testConfiguration.Authentication.SnowflakeJwt.PrivateKeyPassPhrase;
            }
        }
        else if (testConfiguration.Authentication.Pat is not null)
        {
            parameters["username"] = Parameter(testConfiguration.Authentication.Pat.User, "user");
            parameters["adbc.snowflake.sql.auth_type"] = "auth_pat";
            parameters["adbc.snowflake.sql.client_option.auth_token"] = Parameter(testConfiguration.Authentication.Pat.Token, "pat_token");
        }
        else if (testConfiguration.Authentication.OAuth is not null)
        {
            parameters["username"] = Parameter(testConfiguration.Authentication.OAuth.User, "user");
            parameters["adbc.snowflake.sql.auth_type"] = "auth_oauth";
            parameters["adbc.snowflake.sql.client_option.auth_token"] = Parameter(testConfiguration.Authentication.OAuth.Token, "oauth_token");
        }
        else if (testConfiguration.Authentication.ExternalBrowser is not null)
        {
            parameters["username"] = Parameter(testConfiguration.Authentication.ExternalBrowser.User, "user");
            parameters["adbc.snowflake.sql.auth_type"] = "auth_ext_browser";
        }
        else
        {
            // Fallback to top-level user/password
            parameters["password"] = Parameter(testConfiguration.Password, "password");
        }

        // Add optional parameters
        if (!string.IsNullOrWhiteSpace(testConfiguration.Database))
        {
            parameters["adbc.snowflake.sql.db"] = testConfiguration.Database;
        }

        if (!string.IsNullOrWhiteSpace(testConfiguration.Schema))
        {
            parameters["adbc.snowflake.sql.schema"] = testConfiguration.Schema;
        }

        if (!string.IsNullOrWhiteSpace(testConfiguration.Warehouse))
        {
            parameters["adbc.snowflake.sql.warehouse"] = testConfiguration.Warehouse;
        }

        if (!string.IsNullOrWhiteSpace(testConfiguration.Role))
        {
            parameters["adbc.snowflake.sql.role"] = testConfiguration.Role;
        }
        
        if (testConfiguration.TlsSkipVerify)
        {
            parameters["adbc.snowflake.sql.client_option.tls_skip_verify"] = "true";
        }

        return new SnowflakeDriver();
    }

    private static string Parameter(string? value, string parameterName)
    {
        return value ?? throw new ArgumentNullException(parameterName);
    }
}
