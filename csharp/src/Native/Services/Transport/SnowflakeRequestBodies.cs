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
using System.Text.Json;
using System.Text.Json.Serialization;

namespace AdbcDrivers.Snowflake.Native.Services.Transport;

/// <summary>
/// Request body for the query-execution endpoint. <c>describeOnly</c> compiles the statement
/// and returns its result metadata (rowtype) without executing it.
/// </summary>
internal sealed class SnowflakeQueryRequestBody
{
    [JsonPropertyName("sqlText")]
    public required string SqlText { get; init; }

    [JsonPropertyName("asyncExec")]
    public bool AsyncExec { get; init; }

    [JsonPropertyName("describeOnly")]
    public bool DescribeOnly { get; init; }

    /// <summary>Session-level settings (see <see cref="SessionParameterNames"/>).</summary>
    [JsonPropertyName("parameters")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, string>? Parameters { get; init; }

    /// <summary>Positional bind variables keyed "1", "2", ... matching the '?' placeholders.</summary>
    [JsonPropertyName("bindings")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, SnowflakeBinding>? Bindings { get; init; }
}

/// <summary>
/// A bound parameter with its Snowflake data type (see <see cref="BindTypeNames"/>): either a
/// scalar (<see cref="Value"/>) or, for array binding / executemany, one wire value per row
/// (<see cref="Values"/> — the server then executes the statement once per row).
/// </summary>
[JsonConverter(typeof(SnowflakeBindingJsonConverter))]
internal sealed record SnowflakeBinding(string Type, string? Value)
{
    /// <summary>Per-row values for an array bind; null for a scalar bind.</summary>
    public IReadOnlyList<string?>? Values { get; init; }

    public SnowflakeBinding(string type, IReadOnlyList<string?> values)
        : this(type, (string?)null) => Values = values;
}

/// <summary>
/// Writes a binding as <c>{"type": "...", "value": ...}</c> where <c>value</c> is a string (or
/// null) for a scalar bind and an array of strings/nulls for an array bind — the two shapes
/// Snowflake's bind protocol accepts under the same key. Bindings are request-only, so reading
/// is not supported.
/// </summary>
internal sealed class SnowflakeBindingJsonConverter : JsonConverter<SnowflakeBinding>
{
    public override void Write(Utf8JsonWriter writer, SnowflakeBinding binding, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString("type", binding.Type);

        if (binding.Values is { } rows)
        {
            writer.WritePropertyName("value");
            writer.WriteStartArray();
            foreach (string? row in rows)
            {
                if (row == null)
                    writer.WriteNullValue();
                else
                    writer.WriteStringValue(row);
            }
            writer.WriteEndArray();
        }
        else if (binding.Value == null)
        {
            writer.WriteNull("value");
        }
        else
        {
            writer.WriteString("value", binding.Value);
        }

        writer.WriteEndObject();
    }

    public override SnowflakeBinding Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        throw new NotSupportedException("Bindings are serialized into requests, never read back.");
}

/// <summary>
/// An intentionally empty request body (e.g. the heartbeat POST), typed so the source-generated
/// serializer can handle it without falling back to <c>object</c>.
/// </summary>
internal sealed class EmptyRequestBody
{
    internal static readonly EmptyRequestBody Instance = new();
}

/// <summary>
/// Request body for cancelling a running query. Snowflake aborts by the <c>requestId</c> the
/// query was submitted with (not the queryId it returns), so the original request id is echoed here.
/// </summary>
internal sealed class SnowflakeCancelRequestBody
{
    [JsonPropertyName("requestId")]
    public required string RequestId { get; init; }
}

/// <summary>
/// Request body for renewing an expired session token via <c>/session/token-request</c>. The
/// request is authenticated with the master token (not the expired session token).
/// </summary>
internal sealed class SnowflakeRenewSessionBody
{
    [JsonPropertyName("oldSessionToken")]
    public string? OldSessionToken { get; init; }

    [JsonPropertyName("requestType")]
    public string RequestType { get; init; } = "RENEW";
}

/// <summary>
/// Data returned by <c>/session/token-request</c>: the fresh session token (and refreshed master
/// token) with their validity windows.
/// </summary>
internal sealed class SnowflakeRenewSessionData
{
    [JsonPropertyName("sessionToken")]
    public string? SessionToken { get; init; }

    // Session-token validity ("ST"); the renew endpoint names it differently from the login response.
    [JsonPropertyName("validityInSecondsST")]
    public int ValidityInSeconds { get; init; }

    [JsonPropertyName("masterToken")]
    public string? MasterToken { get; init; }

    [JsonPropertyName("validityInSecondsMT")]
    public int MasterValidityInSeconds { get; init; }
}
