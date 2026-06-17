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
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace AdbcDrivers.Snowflake.Native.Tests;

/// <summary>
/// Offline tests for the database's HttpMessageHandler contract: a caller-supplied handler
/// carries all driver traffic but is never disposed by the driver (ownership stays with the
/// caller — the shape IHttpMessageHandlerFactory requires), while a driver-built handler is
/// owned and disposed with the database.
/// </summary>
[Trait("Category", "Unit")]
public class SnowflakeDatabaseTests
{
    private sealed class FakeHandler : HttpMessageHandler
    {
        private int _requests;

        public int Requests => Volatile.Read(ref _requests);

        public bool Disposed { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _requests);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.InternalServerError));
        }

        protected override void Dispose(bool disposing)
        {
            Disposed = true;
            base.Dispose(disposing);
        }
    }

    private static readonly Dictionary<string, string> Parameters = new()
    {
        ["adbc.snowflake.sql.account"] = "testaccount",
        ["username"] = "testuser",
        ["password"] = "testpass",
    };

    [Fact]
    public void Dispose_DoesNotDisposeCallerSuppliedHandler()
    {
        using var handler = new FakeHandler();

        var database = new SnowflakeDatabase(Parameters, handler);
        database.Dispose();

        // The caller retains ownership: the driver's dispose must leave the handler usable.
        Assert.False(handler.Disposed);
    }

    [Fact]
    public void CustomHandler_CarriesDriverTraffic()
    {
        using var handler = new FakeHandler();
        using var database = new SnowflakeDatabase(Parameters, handler);

        // The handler fails every request, so the connect fails — but it must have been
        // reached: proof the caller's handler carries the login traffic.
        Assert.NotNull(Record.Exception(() => database.Connect(new Dictionary<string, string>())));
        Assert.True(handler.Requests > 0, "the caller-supplied handler never saw a request");
    }
}
