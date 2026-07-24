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
using System.ComponentModel.DataAnnotations;
using System.Linq;

using Apache.Arrow.Adbc;

namespace AdbcDrivers.Snowflake.Native.Configuration;

/// <summary>
/// Parses ADBC parameters into ConnectionConfig objects.
/// </summary>
internal static class ConnectionStringParser
{
    /// <summary>Lower bound for the keep-alive heartbeat frequency.</summary>
    private static readonly TimeSpan MinHeartbeatFrequency = TimeSpan.FromMinutes(15);

    /// <summary>Upper bound for the keep-alive heartbeat frequency.</summary>
    private static readonly TimeSpan MaxHeartbeatFrequency = TimeSpan.FromHours(1);

    /// <summary>
    /// Parses ADBC parameters with connection-specific overrides into a ConnectionConfig object.
    /// Connection parameters take precedence over database defaults.
    /// </summary>
    /// <param name="connectionParameters">Connection-specific parameters (take precedence).</param>
    /// <param name="databaseDefaults">Database default parameters.</param>
    /// <returns>A configured ConnectionConfig object with merged parameters.</returns>
    /// <exception cref="ArgumentException">Thrown when the parameters are invalid.</exception>
    public static ConnectionConfig ParseParameters(
        IReadOnlyDictionary<string, string>? connectionParameters = null,
        IReadOnlyDictionary<string, string>? databaseDefaults = null)
    {
        // If both are null, create empty dictionary (will fail validation)
        if (connectionParameters == null && databaseDefaults == null)
        {
            return BuildConfig(new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));
        }

        // If only one is provided, use it directly
        if (databaseDefaults == null || databaseDefaults.Count == 0)
        {
            return BuildConfig(new Dictionary<string, string>(connectionParameters!, StringComparer.OrdinalIgnoreCase));
        }

        if (connectionParameters == null || connectionParameters.Count == 0)
        {
            return BuildConfig(new Dictionary<string, string>(databaseDefaults, StringComparer.OrdinalIgnoreCase));
        }

        // Both provided - merge with connection parameters taking precedence
        var merged = new Dictionary<string, string>(connectionParameters, StringComparer.OrdinalIgnoreCase);

        foreach (var kvp in databaseDefaults)
        {
            if (!merged.ContainsKey(kvp.Key))
            {
                merged[kvp.Key] = kvp.Value;
            }
        }

        return BuildConfig(merged);
    }

    private static ConnectionConfig BuildConfig(IReadOnlyDictionary<string, string> parameters)
    {
        var config = new ConnectionConfig
        {
            Account = GetRequiredParameter(parameters, "adbc.snowflake.sql.account"),
            User = GetOptionalParameter(parameters, "username") ?? string.Empty,
            Database = GetOptionalParameter(parameters, AdbcOptions.Connection.CurrentCatalog)
                       ?? GetOptionalParameter(parameters, "adbc.snowflake.sql.db"),
            Schema = GetOptionalParameter(parameters, AdbcOptions.Connection.CurrentDbSchema)
                     ?? GetOptionalParameter(parameters, "adbc.snowflake.sql.schema"),
            Warehouse = GetOptionalParameter(parameters, "adbc.snowflake.sql.warehouse"),
            Role = GetOptionalParameter(parameters, "adbc.snowflake.sql.role"),
            QueryTag = GetOptionalParameter(parameters, SnowflakeStatement.QueryTagOption),
            Authentication = ParseAuthenticationConfig(parameters)
        };

        if (parameters.TryGetValue("adbc.snowflake.sql.client_option.request_timeout", out string? requestTimeoutStr) &&
            int.TryParse(requestTimeoutStr, out int requestTimeoutSeconds))
        {
            config.QueryTimeout = TimeSpan.FromSeconds(requestTimeoutSeconds);
        }

        if (parameters.TryGetValue("adbc.snowflake.sql.client_option.login_timeout", out string? loginTimeoutStr) &&
            int.TryParse(loginTimeoutStr, out int loginTimeoutSeconds))
        {
            config.LoginTimeout = TimeSpan.FromSeconds(loginTimeoutSeconds);
        }

        if (parameters.TryGetValue("adbc.snowflake.rpc.prefetch_concurrency", out string? prefetchStr) &&
            int.TryParse(prefetchStr, out int prefetch))
        {
            config.PrefetchConcurrency = Math.Max(1, prefetch);
        }

        if (parameters.TryGetValue("adbc.snowflake.sql.client_option.enable_compression", out string? compressionStr) &&
            bool.TryParse(compressionStr, out bool enableCompression))
        {
            config.EnableCompression = enableCompression;
        }

        if (parameters.TryGetValue("adbc.snowflake.sql.client_option.keep_session_alive", out string? keepAliveStr) &&
            bool.TryParse(keepAliveStr, out bool keepAlive))
        {
            config.ClientSessionKeepAlive = keepAlive;
        }

        if (parameters.TryGetValue("adbc.snowflake.sql.client_option.keep_session_alive_heartbeat_frequency", out string? freqStr) &&
            int.TryParse(freqStr, out int freqSeconds))
        {
            // Clamp to a safe band: frequent enough to stay under the ~4h master window, but not
            // so frequent it hammers the server. Mirrors gosnowflake's heartbeat-frequency bounds.
            var clamped = Math.Clamp(freqSeconds, (int)MinHeartbeatFrequency.TotalSeconds, (int)MaxHeartbeatFrequency.TotalSeconds);
            config.HeartbeatFrequency = TimeSpan.FromSeconds(clamped);
        }

        config.PoolConfig = ParseConnectionPoolConfig(parameters);
        config.Network = ParseNetworkConfig(parameters);

        ValidateConfiguration(config);

        return config;
    }

    private static AuthenticationConfig ParseAuthenticationConfig(IReadOnlyDictionary<string, string> parameters)
    {
        var authConfig = new AuthenticationConfig();

        // ADBC standard: adbc.snowflake.sql.auth_type
        string? authTypeStr = GetOptionalParameter(parameters, "adbc.snowflake.sql.auth_type");

        if (authTypeStr != null)
        {
            // Canonical values follow the ADBC Snowflake driver reference (auth_snowflake,
            // auth_jwt, ...); the connector-net-style spellings are kept as aliases.
            authConfig.Type = authTypeStr.ToLowerInvariant() switch
            {
                "auth_snowflake" or "snowflake" => AuthenticationType.UsernamePassword,
                "auth_jwt" or "snowflake_jwt" or "jwt" => AuthenticationType.KeyPair,
                "auth_oauth" or "oauth" => AuthenticationType.OAuth,
                "auth_pat" or "programmatic_access_token" or "pat" => AuthenticationType.Pat,
                "auth_ext_browser" or "externalbrowser" => AuthenticationType.ExternalBrowser,
                "auth_okta" or "auth_mfa" or "auth_wif" => throw new ArgumentException(
                    $"auth_type '{authTypeStr}' is a recognized ADBC Snowflake auth method but is not supported by this driver yet."),
                _ => throw new ArgumentException($"Unsupported auth_type: {authTypeStr}")
            };
        }

        // Password - ADBC standard doesn't prefix this
        authConfig.Password = GetOptionalParameter(parameters, "password");

        // Private key file path - ADBC standard: adbc.snowflake.sql.client_option.jwt_private_key
        authConfig.PrivateKeyPath = GetOptionalParameter(parameters, "adbc.snowflake.sql.client_option.jwt_private_key");

        // Private key value (inline PEM) - ADBC standard: adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value
        authConfig.PrivateKey = GetOptionalParameter(parameters, "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value");

        // Private key passphrase - ADBC standard: adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_password
        authConfig.PrivateKeyPassphrase = GetOptionalParameter(parameters, "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_password");

        // Access token (OAuth or PAT, per auth_type) - ADBC standard: adbc.snowflake.sql.client_option.auth_token
        authConfig.Token = GetOptionalParameter(parameters, "adbc.snowflake.sql.client_option.auth_token");

        return authConfig;
    }

    private static ConnectionPoolConfig ParseConnectionPoolConfig(IReadOnlyDictionary<string, string> parameters)
    {
        var poolConfig = new ConnectionPoolConfig();

        // Client-side pooling is our own feature (the ADBC Snowflake/gosnowflake driver has none), so
        // these keys live under our own adbc.snowflake.pool.* namespace for consistency with the rest.
        if (parameters.TryGetValue("adbc.snowflake.pool.max_size", out string? maxPoolSizeStr) &&
            int.TryParse(maxPoolSizeStr, out int maxPoolSize))
        {
            poolConfig.MaxPoolSize = maxPoolSize;
        }

        if (parameters.TryGetValue("adbc.snowflake.pool.idle_timeout", out string? idleTimeoutStr))
        {
            poolConfig.IdleTimeout = ParseTimeSpan(idleTimeoutStr);
        }

        if (parameters.TryGetValue("adbc.snowflake.pool.acquire_timeout", out string? acquireTimeoutStr))
        {
            poolConfig.AcquireTimeout = ParseTimeSpan(acquireTimeoutStr);
        }

        if (parameters.TryGetValue("adbc.snowflake.pool.max_lifetime", out string? maxLifetimeStr))
        {
            poolConfig.MaxConnectionLifetime = ParseTimeSpan(maxLifetimeStr);
        }

        return poolConfig;
    }

    private static TimeSpan ParseTimeSpan(string value)
    {
        // Support Snowflake format (e.g., "30s", "60m") and plain seconds
        if (int.TryParse(value, out int seconds))
            return TimeSpan.FromSeconds(seconds);

        if (value.EndsWith("s", StringComparison.OrdinalIgnoreCase))
        {
            if (int.TryParse(value[..^1], out int s))
            {
                return TimeSpan.FromSeconds(s);
            }
        }
        else if (value.EndsWith("m", StringComparison.OrdinalIgnoreCase))
        {
            if (int.TryParse(value[..^1], out int m))
            {
                return TimeSpan.FromMinutes(m);
            }
        }
        else if (value.EndsWith("h", StringComparison.OrdinalIgnoreCase))
        {
            if (int.TryParse(value[..^1], out int h))
            {
                return TimeSpan.FromHours(h);
            }
        }

        throw new ArgumentException($"Invalid timespan format: {value}. Expected format: number with optional suffix (s, m, h) or plain seconds.");
    }
    
    internal static NetworkConfig ParseNetworkConfig(IReadOnlyDictionary<string, string>? parameters)
    {
        var network = new NetworkConfig();
        if (parameters is null)
            return network;

        network.Host = GetOptionalParameter(parameters, "adbc.snowflake.sql.uri.host");

        if (parameters.TryGetValue("adbc.snowflake.sql.uri.port", out string? portStr) &&
            int.TryParse(portStr, out int port))
        {
            network.Port = port;
        }

        var protocol = GetOptionalParameter(parameters, "adbc.snowflake.sql.uri.protocol");
        if (protocol != null)
            network.Protocol = protocol;

        if (IsTrueOption(parameters, "adbc.snowflake.sql.client_option.no_proxy"))
            network.NoProxy = true;

        if (IsTrueOption(parameters, "adbc.snowflake.sql.client_option.tls_skip_verify"))
            network.TlsSkipVerify = true;

        return network;
    }

    private static string GetRequiredParameter(IReadOnlyDictionary<string, string> parameters, string key)
    {
        if (!parameters.TryGetValue(key, out string? value) || string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException($"Required parameter '{key}' is missing or empty.");
        }
        return value;
    }

    private static string? GetOptionalParameter(IReadOnlyDictionary<string, string> parameters, string key)
    {
        parameters.TryGetValue(key, out string? value);
        return string.IsNullOrWhiteSpace(value) ? null : value;
    }

    private static bool IsTrueOption(IReadOnlyDictionary<string, string> parameters, string key) =>
        parameters.TryGetValue(key, out string? value) && string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);

    private static void ValidateConfiguration(ConnectionConfig config)
    {
        var validationResults = new List<ValidationResult>();
        var validationContext = new ValidationContext(config);

        Validator.TryValidateObject(config, validationContext, validationResults, true);
        
        var poolValidationContext = new ValidationContext(config.PoolConfig);
        Validator.TryValidateObject(config.PoolConfig, poolValidationContext, validationResults, true);

        if (!validationResults.Any())
            return;

        var errorMessages = validationResults.Select(vr => vr.ErrorMessage).ToArray();
        throw new ArgumentException($"Configuration validation failed: {string.Join("; ", errorMessages)}");
    }
}
