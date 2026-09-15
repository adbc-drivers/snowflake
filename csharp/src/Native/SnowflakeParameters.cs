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

namespace AdbcDrivers.Snowflake.Native;

/// <summary>
/// The parameter names this driver accepts, for callers building the option dictionary passed to
/// <see cref="SnowflakeDriver.Open"/> and <see cref="SnowflakeDatabase.Connect"/>. The names follow
/// the ADBC Snowflake driver reference, so a connection string written for that driver works here.
/// </summary>
public static class SnowflakeParameters
{
    // Identity and session context.
    public const string Account = "adbc.snowflake.sql.account";
    public const string Username = "username";
    public const string Password = "password";
    public const string Database = "adbc.snowflake.sql.db";
    public const string Schema = "adbc.snowflake.sql.schema";
    public const string Warehouse = "adbc.snowflake.sql.warehouse";
    public const string Role = "adbc.snowflake.sql.role";

    // Authentication.
    public const string AuthType = "adbc.snowflake.sql.auth_type";
    public const string AuthToken = "adbc.snowflake.sql.client_option.auth_token";
    public const string JwtPrivateKeyPath = "adbc.snowflake.sql.client_option.jwt_private_key";
    public const string JwtPrivateKeyValue = "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value";
    public const string JwtPrivateKeyPassphrase = "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_password";

    // Endpoint. Region applies only to an account given without its region suffix.
    public const string Region = "adbc.snowflake.sql.region";
    public const string Host = "adbc.snowflake.sql.uri.host";
    public const string Port = "adbc.snowflake.sql.uri.port";
    public const string Protocol = "adbc.snowflake.sql.uri.protocol";
    public const string NoProxy = "adbc.snowflake.sql.client_option.no_proxy";
    public const string TlsSkipVerify = "adbc.snowflake.sql.client_option.tls_skip_verify";

    // Session behaviour.
    public const string RequestTimeout = "adbc.snowflake.sql.client_option.request_timeout";
    public const string LoginTimeout = "adbc.snowflake.sql.client_option.login_timeout";
    public const string EnableCompression = "adbc.snowflake.sql.client_option.enable_compression";
    public const string KeepSessionAlive = "adbc.snowflake.sql.client_option.keep_session_alive";
    public const string KeepSessionAliveHeartbeatFrequency =
        "adbc.snowflake.sql.client_option.keep_session_alive_heartbeat_frequency";
    public const string PrefetchConcurrency = "adbc.snowflake.rpc.prefetch_concurrency";

    // Client-side connection pooling, which this driver adds; the reference driver has none.
    public const string PoolMaxSize = "adbc.snowflake.pool.max_size";
    public const string PoolMaxLifetime = "adbc.snowflake.pool.max_lifetime";
    public const string PoolIdleTimeout = "adbc.snowflake.pool.idle_timeout";
    public const string PoolAcquireTimeout = "adbc.snowflake.pool.acquire_timeout";

    // Statement options, set through AdbcStatement.SetOption.
    public const string QueryTag = "adbc.snowflake.statement.query_tag";
}
