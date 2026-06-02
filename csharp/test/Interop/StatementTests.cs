/*
* Copyright (c) 2026 ADBC Drivers Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
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

        [SkippableFact]
        public async Task CanCancelStatementTest()
        {
            const int millisecondsDelay = 500;
            const string query = "SELECT SYSTEM$WAIT(30)";

            var snowflakeTestConfiguration = SnowflakeTestingUtils.TestConfiguration;

            using AdbcDriver snowflakeDriver = SnowflakeTestingUtils.GetSnowflakeAdbcDriver(snowflakeTestConfiguration, out Dictionary<string, string> parameters);
            using AdbcDatabase adbcDatabase = snowflakeDriver.Open(parameters);
            using AdbcConnection connection = adbcDatabase.Connect(new Dictionary<string, string>());
            using AdbcStatement statement = connection.CreateStatement();

            // Note: for this test to be valid, the query needs to run for more time than the delay value!
            statement.SqlQuery = query;
            for (int i = 0; i < 5; i++)
            {
                // Reuse the statement to check for issue that might arise from using the Statement multiple times.
                try
                {
                    Task<QueryResult> queryTask = Task.Run(statement.ExecuteQuery);

                    await Task.Delay(millisecondsDelay);
                    statement.Cancel();

                    QueryResult queryResult = await queryTask;
                    _outputHelper?.WriteLine($"QueryResultRowCount: {queryResult.RowCount}");
                    queryResult.Stream?.Dispose();
                    Assert.Fail("Expected query to be cancelled, but it completed successfully.");
                }
                catch (AdbcException ex)
                {
                    Assert.Contains("[snowflake] context canceled", ex.Message, StringComparison.OrdinalIgnoreCase);
                }
            }
        }
    }
}
