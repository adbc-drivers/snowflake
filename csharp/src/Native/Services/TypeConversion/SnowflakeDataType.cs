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

namespace AdbcDrivers.Snowflake.Native.Services.TypeConversion;

/// <summary>
/// Represents a Snowflake data type with metadata.
/// </summary>
internal class SnowflakeDataType
{
    /// <summary>
    /// Gets or sets the type name.
    /// </summary>
    public string TypeName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the precision (for numeric types).
    /// </summary>
    public int? Precision { get; set; }

    /// <summary>
    /// Gets or sets the scale (for numeric types).
    /// </summary>
    public int? Scale { get; set; }

    /// <summary>
    /// Gets or sets the length (for string/binary types).
    /// </summary>
    public int? Length { get; set; }

    /// <summary>
    /// Gets or sets whether the type is nullable.
    /// </summary>
    public bool IsNullable { get; set; } = true;

    /// <summary>
    /// Gets or sets the element count of a VECTOR column.
    /// </summary>
    public int? VectorDimension { get; set; }

    /// <summary>
    /// Gets or sets the element type of a VECTOR column: 'fixed' for VECTOR(INT, n), 'real' for
    /// VECTOR(FLOAT, n). Snowflake accepts no other element type.
    /// </summary>
    public string? ElementTypeName { get; set; }

    /// <summary>
    /// Gets or sets the timezone (for timestamp types).
    /// </summary>
    public string? Timezone { get; set; }

    /// <summary>
    /// Gets the Snowflake type code.
    /// </summary>
    public SnowflakeTypeCode TypeCode => ParseTypeCode(TypeName);

    private static SnowflakeTypeCode ParseTypeCode(string typeName)
    {
        return typeName.ToUpperInvariant() switch
        {
            "FIXED" => SnowflakeTypeCode.Number,
            "REAL" => SnowflakeTypeCode.Double,
            "TEXT" => SnowflakeTypeCode.Varchar,
            "BINARY" => SnowflakeTypeCode.Binary,
            "BOOLEAN" => SnowflakeTypeCode.Boolean,
            "DATE" => SnowflakeTypeCode.Date,
            "TIME" => SnowflakeTypeCode.Time,
            "TIMESTAMP_LTZ" => SnowflakeTypeCode.TimestampLtz,
            "TIMESTAMP_NTZ" => SnowflakeTypeCode.TimestampNtz,
            "TIMESTAMP_TZ" => SnowflakeTypeCode.TimestampTz,
            "VARIANT" => SnowflakeTypeCode.Variant,
            "OBJECT" => SnowflakeTypeCode.Object,
            "ARRAY" => SnowflakeTypeCode.Array,
            "VECTOR" => SnowflakeTypeCode.Vector,
            _ => SnowflakeTypeCode.Unknown
        };
    }
}
