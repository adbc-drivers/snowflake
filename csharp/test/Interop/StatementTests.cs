/*
* Copyright (c) 2026 ADBC Drivers Contributors
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

using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Tests;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Snowflake.Interop.Tests
{
    public class StatementTests
    {
        readonly ITestOutputHelper _outputHelper;

        public StatementTests(ITestOutputHelper outputHelper)
        {
            _outputHelper = outputHelper;
            Skip.IfNot(Utils.CanExecuteTestConfig(SnowflakeTestingUtils.SNOWFLAKE_TEST_CONFIG_VARIABLE));
        }

        private static AdbcConnection CreateConnection(SnowflakeTestConfiguration testConfiguration, Dictionary<string, string>? options = null)
        {
            Dictionary<string, string> parameters;
            AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(testConfiguration, out parameters);
            AdbcDatabase adbcDatabase = snowflakeDriver.Open(parameters);
            return adbcDatabase.Connect(options);
        }

        [SkippableTheory()]
        [InlineData("SELECT SYSTEM$WAIT(30)", true)]
        [InlineData("SELECT SYSTEM$WAIT(1)", false)]
        internal virtual async Task CanCancelStatementTest(string query, bool testCancel)
        {
            const int millisecondsDelay = 500;

            var snowflakeTestConfiguration = SnowflakeTestingUtils.TestConfiguration;

            AdbcConnection connection = StatementTests.CreateConnection(snowflakeTestConfiguration);
            AdbcStatement statement = connection.CreateStatement();
            // Note: for this test to be valid, the query needs to run for more time than the delay value!
            statement.SqlQuery = query;
            for (int i = 0; i < 10; i++)
            {
                // Reuse the statement to check for issue that might arise from using the Statement multiple times.
                try
                {
                    Task<QueryResult> queryTask = Task.Run(statement.ExecuteQuery);

                    if (testCancel)
                    {
                        await Task.Delay(millisecondsDelay);
                        statement.Cancel();
                    }

                    QueryResult queryResult = await queryTask;
                    _outputHelper?.WriteLine($"QueryResultRowCount: {queryResult.RowCount}");
                    if (testCancel)
                    {
                        Assert.Fail("Expecting query to timeout, but it did not.");
                    }
                }
                catch (AdbcException ex)
                {
                    Assert.True(testCancel, $"Unexpected exception: {ex}");
                    Assert.Contains("[snowflake] context canceled", ex.Message, StringComparison.OrdinalIgnoreCase);
                }
            }
        }
    }
}
