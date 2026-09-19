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

using AdbcDrivers.Snowflake.Native.Configuration;
using AdbcDrivers.Snowflake.Native.Services;
using Xunit;

namespace AdbcDrivers.Snowflake.Native.Tests;

/// <summary>
/// Offline unit tests for <see cref="SnowflakeAccountUrl"/> URL building.
/// </summary>
[Trait("Category", "Unit")]
public class SnowflakeAccountUrlTests
{
    [Fact]
    public void Build_PlainAccount_AppendsSnowflakeDomain()
    {
        Assert.Equal(
            "https://xy12345.snowflakecomputing.com",
            SnowflakeAccountUrl.Build("xy12345", network: null));
    }

    [Fact]
    public void Build_RegionSuffixedAccount_KeepsSuffixInHost()
    {
        Assert.Equal(
            "https://xy12345.us-east-1.aws.snowflakecomputing.com",
            SnowflakeAccountUrl.Build("xy12345.us-east-1.aws", network: null));
    }

    [Fact]
    public void Build_ChinaRegionAccount_UsesChinaDomain()
    {
        Assert.Equal(
            "https://xy12345.cn-north-1.snowflakecomputing.cn",
            SnowflakeAccountUrl.Build("xy12345.cn-north-1", network: null));
    }

    [Fact]
    public void Build_ChinaRegionAccount_IsCaseInsensitive()
    {
        Assert.Equal(
            "https://xy12345.CN-NORTH-1.snowflakecomputing.cn",
            SnowflakeAccountUrl.Build("xy12345.CN-NORTH-1", network: null));
    }

    [Fact]
    public void Build_NonChinaRegionStartingWithC_UsesDefaultDomain()
    {
        Assert.Equal(
            "https://xy12345.ca-central-1.snowflakecomputing.com",
            SnowflakeAccountUrl.Build("xy12345.ca-central-1", network: null));
    }

    [Fact]
    public void Build_NetworkHostOnDefaultPort_UsesHostNoPort()
    {
        var network = new NetworkConfig { Host = "myhost.example.com" };
        Assert.Equal("https://myhost.example.com", SnowflakeAccountUrl.Build("xy12345", network));
    }

    [Fact]
    public void Build_NetworkHostWithNonDefaultPort_IncludesPort()
    {
        var network = new NetworkConfig { Host = "myhost.example.com", Port = 8080 };
        Assert.Equal("https://myhost.example.com:8080", SnowflakeAccountUrl.Build("xy12345", network));
    }

    [Fact]
    public void Build_NetworkHostWithProtocolAndPort_UsesBoth()
    {
        var network = new NetworkConfig { Host = "localhost", Protocol = "http", Port = 80 };
        Assert.Equal("http://localhost:80", SnowflakeAccountUrl.Build("xy12345", network));
    }

    [Fact]
    public void Build_NetworkWithoutHost_DerivesFromAccountWithPort()
    {
        var network = new NetworkConfig { Port = 8443 };
        Assert.Equal("https://xy12345.snowflakecomputing.com:8443", SnowflakeAccountUrl.Build("xy12345", network));
    }

    [Fact]
    public void Build_ExplicitRegion_IsAppendedToAccount()
    {
        var network = new NetworkConfig { Region = "us-east-1" };
        Assert.Equal(
            "https://xy12345.us-east-1.snowflakecomputing.com",
            SnowflakeAccountUrl.Build("xy12345", network));
    }

    [Fact]
    public void Build_ExplicitRegionWithCloud_IsAppendedToAccount()
    {
        var network = new NetworkConfig { Region = "us-east-1.aws" };
        Assert.Equal(
            "https://xy12345.us-east-1.aws.snowflakecomputing.com",
            SnowflakeAccountUrl.Build("xy12345", network));
    }

    [Fact]
    public void Build_ExplicitChinaRegion_UsesChinaDomain()
    {
        var network = new NetworkConfig { Region = "cn-north-1" };
        Assert.Equal(
            "https://xy12345.cn-north-1.snowflakecomputing.cn",
            SnowflakeAccountUrl.Build("xy12345", network));
    }

    [Fact]
    public void Build_NetworkHost_TakesPrecedenceOverRegion()
    {
        var network = new NetworkConfig { Host = "myhost.example.com", Region = "us-east-1" };
        Assert.Equal("https://myhost.example.com", SnowflakeAccountUrl.Build("xy12345", network));
    }

    [Fact]
    public void Build_NetworkHost_TakesPrecedenceOverAccount()
    {
        var network = new NetworkConfig { Host = "xy12345.privatelink.snowflakecomputing.com" };
        Assert.Equal(
            "https://xy12345.privatelink.snowflakecomputing.com",
            SnowflakeAccountUrl.Build("xy12345.cn-north-1", network));
    }
}
