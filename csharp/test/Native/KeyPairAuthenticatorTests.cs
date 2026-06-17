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
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native.Configuration;
using AdbcDrivers.Snowflake.Native.Services.Authentication;
using Apache.Arrow.Adbc;
using NSubstitute;
using Xunit;

namespace AdbcDrivers.Snowflake.Native.Tests;

/// <summary>
/// Offline tests for key-pair (SNOWFLAKE_JWT) authentication: the login JWT's claims and
/// signature (verifiable without a server by checking against the key that signed it), and
/// the AuthenticationService wiring that resolves an inline PEM vs. a private-key file path.
/// </summary>
[Trait("Category", "Unit")]
public class KeyPairAuthenticatorTests
{
    private const string Account = "testaccount";
    private const string User = "testuser";

    private static (RSA Rsa, string Pem) CreateKey()
    {
        var rsa = RSA.Create(2048);
        return (rsa, rsa.ExportPkcs8PrivateKeyPem());
    }

    private static JsonDocument DecodeSegment(string jwt, int segment)
    {
        string part = jwt.Split('.')[segment];
        return JsonDocument.Parse(FromBase64Url(part));
    }

    private static byte[] FromBase64Url(string value)
    {
        string padded = value.Replace('-', '+').Replace('_', '/');
        padded += (padded.Length % 4) switch { 2 => "==", 3 => "=", _ => string.Empty };
        return Convert.FromBase64String(padded);
    }

    [Fact]
    public void GenerateJwtToken_HasSnowflakeClaimShapes()
    {
        (RSA rsa, string pem) = CreateKey();
        using (rsa)
        {
            string jwt = KeyPairAuthenticator.GenerateJwtToken(Account, User, pem, passphrase: null);

            using JsonDocument header = DecodeSegment(jwt, 0);
            Assert.Equal("RS256", header.RootElement.GetProperty("alg").GetString());
            Assert.Equal("JWT", header.RootElement.GetProperty("typ").GetString());

            // Snowflake requires iss = ACCOUNT.USER.SHA256:<base64 fingerprint of the public key>
            // and sub = ACCOUNT.USER, both upper-cased.
            string fingerprint = Convert.ToBase64String(SHA256.HashData(rsa.ExportSubjectPublicKeyInfo()));
            using JsonDocument payload = DecodeSegment(jwt, 1);
            Assert.Equal($"TESTACCOUNT.TESTUSER.SHA256:{fingerprint}", payload.RootElement.GetProperty("iss").GetString());
            Assert.Equal("TESTACCOUNT.TESTUSER", payload.RootElement.GetProperty("sub").GetString());
            Assert.Equal(3600, payload.RootElement.GetProperty("exp").GetInt64() - payload.RootElement.GetProperty("iat").GetInt64());
        }
    }

    [Fact]
    public void GenerateJwtToken_SignatureVerifiesWithPublicKey()
    {
        (RSA rsa, string pem) = CreateKey();
        using (rsa)
        {
            string jwt = KeyPairAuthenticator.GenerateJwtToken(Account, User, pem, passphrase: null);
            string[] parts = jwt.Split('.');
            Assert.Equal(3, parts.Length);

            bool valid = rsa.VerifyData(
                Encoding.UTF8.GetBytes($"{parts[0]}.{parts[1]}"),
                FromBase64Url(parts[2]),
                HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1);
            Assert.True(valid);
        }
    }

    [Fact]
    public void GenerateJwtToken_AccountWithRegionSuffix_UsesBareAccountLocator()
    {
        // Account identifiers can carry a region/cloud suffix (xy12345.eu-west-1); the JWT
        // claims must use only the bare locator, like gosnowflake/connector-net.
        (RSA rsa, string pem) = CreateKey();
        using (rsa)
        {
            string jwt = KeyPairAuthenticator.GenerateJwtToken("xy12345.eu-west-1", User, pem, passphrase: null);

            using JsonDocument payload = DecodeSegment(jwt, 1);
            Assert.Equal("XY12345.TESTUSER", payload.RootElement.GetProperty("sub").GetString());
            Assert.StartsWith("XY12345.TESTUSER.SHA256:", payload.RootElement.GetProperty("iss").GetString());
        }
    }

    [Fact]
    public void GenerateJwtToken_EncryptedKey_DecryptsWithPassphrase()
    {
        (RSA rsa, string _) = CreateKey();
        using (rsa)
        {
            string encryptedPem = rsa.ExportEncryptedPkcs8PrivateKeyPem(
                "key-passphrase",
                new PbeParameters(PbeEncryptionAlgorithm.Aes256Cbc, HashAlgorithmName.SHA256, 100_000));

            string jwt = KeyPairAuthenticator.GenerateJwtToken(Account, User, encryptedPem, "key-passphrase");
            Assert.Equal(3, jwt.Split('.').Length);

            Assert.Throws<AdbcException>(() =>
                KeyPairAuthenticator.GenerateJwtToken(Account, User, encryptedPem, "wrong-passphrase"));
        }
    }

    [Fact]
    public void GenerateJwtToken_InvalidPem_ThrowsAdbcException()
    {
        var ex = Assert.Throws<AdbcException>(() =>
            KeyPairAuthenticator.GenerateJwtToken(Account, User, "not a pem key", passphrase: null));
        Assert.Contains("private key", ex.Message);
    }

    // ---- Key-material resolution: inline PEM vs. private-key file path ----

    private static ConnectionConfig KeyPairConfig(AuthenticationConfig authConfig) => new()
    {
        Account = Account,
        User = User,
        Authentication = authConfig,
    };

    [Fact]
    public async Task ResolvePrivateKeyPem_InlineKey_PassesPemThroughUnchanged()
    {
        (RSA rsa, string pem) = CreateKey();
        using (rsa)
        {
            var authConfig = new AuthenticationConfig { PrivateKey = pem };
            Assert.Equal(pem, await KeyPairAuthenticator.ResolvePrivateKeyPemAsync(authConfig, CancellationToken.None));
        }
    }

    [Fact]
    public async Task ResolvePrivateKeyPem_KeyFilePath_ReadsFileContent()
    {
        (RSA rsa, string pem) = CreateKey();
        using (rsa)
        {
            string keyFile = Path.GetTempFileName();
            try
            {
                await File.WriteAllTextAsync(keyFile, pem);
                var authConfig = new AuthenticationConfig { PrivateKeyPath = keyFile };

                // The file's CONTENT is resolved, never the path itself.
                Assert.Equal(pem, await KeyPairAuthenticator.ResolvePrivateKeyPemAsync(authConfig, CancellationToken.None));
            }
            finally
            {
                File.Delete(keyFile);
            }
        }
    }

    [Fact]
    public async Task ResolvePrivateKeyPem_MissingKeyFile_ThrowsAdbcException()
    {
        var authConfig = new AuthenticationConfig
        {
            PrivateKeyPath = Path.Combine(Path.GetTempPath(), "does-not-exist.p8"),
        };

        var ex = await Assert.ThrowsAsync<AdbcException>(
            () => KeyPairAuthenticator.ResolvePrivateKeyPemAsync(authConfig, CancellationToken.None));
        Assert.Contains("not found", ex.Message);
    }

    // ---- Requirement validation: each authenticator reports everything missing at once ----

    [Fact]
    public void ValidateRequirements_KeyPair_ListsEveryMissingItem()
    {
        var config = new ConnectionConfig { Account = Account }; // no user, no key material

        var ex = Assert.Throws<ArgumentException>(() => KeyPairAuthenticator.ValidateRequirements(config));
        Assert.Contains("user", ex.Message);
        Assert.Contains("private key", ex.Message);
    }

    [Fact]
    public void ValidateRequirements_KeyPair_CompleteConfig_DoesNotThrow()
    {
        var config = KeyPairConfig(new AuthenticationConfig { PrivateKey = "pem" });
        KeyPairAuthenticator.ValidateRequirements(config);
    }

    [Fact]
    public void ValidateRequirements_Basic_ListsEveryMissingItem()
    {
        var config = new ConnectionConfig { Account = Account }; // no user, no password

        var ex = Assert.Throws<ArgumentException>(() => BasicAuthenticator.ValidateRequirements(config));
        Assert.Contains("user", ex.Message);
        Assert.Contains("password", ex.Message);
    }

    [Fact]
    public void ValidateRequirements_OAuth_DoesNotRequireUser()
    {
        // Snowflake derives the identity from the token, so a user-less config is valid.
        var config = new ConnectionConfig
        {
            Account = Account,
            Authentication = new AuthenticationConfig { Token = "token" },
        };
        OAuthAuthenticator.ValidateRequirements(config);

        var ex = Assert.Throws<ArgumentException>(() => OAuthAuthenticator.ValidateRequirements(
            new ConnectionConfig { Account = Account }));
        Assert.Contains("OAuth token", ex.Message);
    }

    [Fact]
    public void ValidateRequirements_Pat_RequiresUserAndToken()
    {
        // A PAT is bound to a user (unlike OAuth), so both must be present — and every
        // missing item is reported at once.
        var ex = Assert.Throws<ArgumentException>(() => PatAuthenticator.ValidateRequirements(
            new ConnectionConfig { Account = Account }));
        Assert.Contains("user", ex.Message);
        Assert.Contains("programmatic access token", ex.Message);

        PatAuthenticator.ValidateRequirements(new ConnectionConfig
        {
            Account = Account,
            User = User,
            Authentication = new AuthenticationConfig { Token = "pat-token" },
        });
    }

    // ---- AuthenticationService: pure dispatch on the configured auth type ----

    [Fact]
    public async Task AuthenticationService_KeyPairType_DelegatesConfigToKeyPairAuthenticator()
    {
        var keyPairAuth = Substitute.For<IKeyPairAuthenticator>();
        var service = new AuthenticationService(
            Substitute.For<IBasicAuthenticator>(),
            keyPairAuth,
            Substitute.For<IOAuthAuthenticator>(),
            Substitute.For<IPatAuthenticator>(),
            Substitute.For<ISsoAuthenticator>());
        var config = KeyPairConfig(new AuthenticationConfig { Type = AuthenticationType.KeyPair, PrivateKey = "pem" });

        await service.AuthenticateAsync(config);

        await keyPairAuth.Received(1).AuthenticateAsync(config, Arg.Any<CancellationToken>());
    }
}
