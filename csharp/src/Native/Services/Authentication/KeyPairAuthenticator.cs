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
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native.Configuration;

using Apache.Arrow.Adbc;

namespace AdbcDrivers.Snowflake.Native.Services.Authentication;

/// <summary>
/// Implements RSA key pair authentication for Snowflake.
/// </summary>
internal class KeyPairAuthenticator : IKeyPairAuthenticator
{
    private readonly SnowflakeLoginClient _loginClient;

    /// <summary>
    /// Initializes a new instance of the <see cref="KeyPairAuthenticator"/> class.
    /// </summary>
    /// <param name="loginClient">The shared login client.</param>
    public KeyPairAuthenticator(SnowflakeLoginClient loginClient)
    {
        _loginClient = loginClient ?? throw new ArgumentNullException(nameof(loginClient));
    }

    /// <inheritdoc/>
    public async Task<AuthenticationToken> AuthenticateAsync(
        ConnectionConfig config,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(config);
        ValidateRequirements(config);

        string privateKeyPem = await ResolvePrivateKeyPemAsync(config.Authentication, cancellationToken).ConfigureAwait(false);
        var jwtToken = GenerateJwtToken(config.Account, config.User, privateKeyPem, config.Authentication.PrivateKeyPassphrase);

        var authData = new LoginRequestData
        {
            AUTHENTICATOR = "SNOWFLAKE_JWT",
            LOGIN_NAME = config.User,
            TOKEN = jwtToken
        };

        return await _loginClient.LoginAsync(config.Account, authData, config, cancellationToken).ConfigureAwait(false);
    }
    
    internal static void ValidateRequirements(ConnectionConfig config)
    {
        var missing = new List<string>();
        if (string.IsNullOrEmpty(config.Account))
            missing.Add("account");
        if (string.IsNullOrEmpty(config.User))
            missing.Add("user");
        if (string.IsNullOrEmpty(config.Authentication.PrivateKeyPath) && string.IsNullOrEmpty(config.Authentication.PrivateKey))
            missing.Add("a private key (file path or inline PKCS#8 value)");

        if (missing.Count > 0)
            throw new ArgumentException($"Key-pair authentication requires: {string.Join(", ", missing)}.", nameof(config));
    }

    /// <summary>
    /// Resolves the key material: a configured file path is read from disk; otherwise the
    /// inline PEM (jwt_private_key_pkcs8_value) is used as-is — never treated as a path.
    /// </summary>
    internal static async Task<string> ResolvePrivateKeyPemAsync(AuthenticationConfig authConfig, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(authConfig.PrivateKeyPath))
            return authConfig.PrivateKey!;

        if (!File.Exists(authConfig.PrivateKeyPath))
            throw new AdbcException($"Private key file not found: {authConfig.PrivateKeyPath}");

        return await File.ReadAllTextAsync(authConfig.PrivateKeyPath, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Builds the RS256-signed login JWT. The issuer and subject use the bare account locator —
    /// a region/cloud suffix (anything after the first '.') is dropped — and the issuer carries
    /// the public-key fingerprint as <c>SHA256:</c> + base64(SHA-256(SubjectPublicKeyInfo)),
    /// matching gosnowflake and connector-net; Snowflake rejects the token without the prefix.
    /// </summary>
    internal static string GenerateJwtToken(string account, string user, string privateKeyPem, string? passphrase)
    {
        try
        {
            using var rsa = RSA.Create();

            if (!string.IsNullOrEmpty(passphrase))
            {
                rsa.ImportFromEncryptedPem(privateKeyPem, passphrase);
            }
            else
            {
                rsa.ImportFromPem(privateKeyPem);
            }

            var publicKey = rsa.ExportSubjectPublicKeyInfo();
            var publicKeyFingerprint = "SHA256:" + Convert.ToBase64String(SHA256.HashData(publicKey));

            int regionSeparator = account.IndexOf('.');
            string accountName = (regionSeparator > 0 ? account[..regionSeparator] : account).ToUpperInvariant();
            string userName = user.ToUpperInvariant();

            var header = new
            {
                alg = "RS256",
                typ = "JWT"
            };

            var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var payload = new
            {
                iss = $"{accountName}.{userName}.{publicKeyFingerprint}",
                sub = $"{accountName}.{userName}",
                iat = now,
                exp = now + 3600
            };

            var headerBase64 = Base64UrlEncode(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(header)));
            var payloadBase64 = Base64UrlEncode(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(payload)));

            var signatureInput = $"{headerBase64}.{payloadBase64}";
            var signatureBytes = rsa.SignData(
                Encoding.UTF8.GetBytes(signatureInput),
                HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1);

            return $"{signatureInput}.{Base64UrlEncode(signatureBytes)}";
        }
        catch (CryptographicException ex)
        {
            throw new AdbcException($"Failed to process private key: {ex.Message}", ex);
        }
        catch (ArgumentException ex)
        {
            // ImportFromPem reports text with no recognizable PEM block as ArgumentException.
            throw new AdbcException($"Failed to process private key: {ex.Message}", ex);
        }
    }

    private static string Base64UrlEncode(byte[] input)
    {
        return Convert.ToBase64String(input)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }
}
