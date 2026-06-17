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
using AdbcDrivers.Snowflake.Native.Configuration;
using AdbcDrivers.Snowflake.Native.Services.Authentication;
using AdbcDrivers.Snowflake.Native.Services.ConnectionPool;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace AdbcDrivers.Snowflake.Native.Tests.ConnectionPool;

[Trait("Category", "Unit")]
public class PooledConnectionTests
{
    [Fact]
    public void IsTokenExpired_IsKeyedOnMasterExpiry_AgainstTheInjectedClock()
    {
        // Given a connection whose session token expires in 1h but the master (recoverability
        // ceiling) expires in 4h — eviction should track the master, since a session-expired
        // connection is still usable via renewal until the master lapses.
        var fakeTime = new FakeTimeProvider();
        var token = new AuthenticationToken
        {
            SessionToken = "session",
            ExpiresAt = fakeTime.GetUtcNow().AddHours(1),
            MasterExpiresAt = fakeTime.GetUtcNow().AddHours(4),
        };
        var connection = new PooledConnection(
            "id",
            "pool-key",
            token,
            new ConnectionConfig { Account = "test" },
            sessionLifecycle: null,
            timeProvider: fakeTime);

        // Then it stays valid past the session expiry, and only flips once the master lapses
        Assert.False(connection.IsTokenExpired);

        fakeTime.Advance(TimeSpan.FromHours(2)); // session (1h) is long gone, master (4h) still alive
        Assert.False(connection.IsTokenExpired);

        fakeTime.Advance(TimeSpan.FromHours(2) + TimeSpan.FromMinutes(1)); // now past the 4h master
        Assert.True(connection.IsTokenExpired);
    }
}
