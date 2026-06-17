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
using System.Globalization;
using AdbcDrivers.Snowflake.Native.Services.Query;
using AdbcDrivers.Snowflake.Native.Services.Transport;
using Apache.Arrow.Ipc;

using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Types;

namespace AdbcDrivers.Snowflake.Native;

public sealed partial class SnowflakeConnection
{
    /// <summary>
    /// Gets database objects (catalogs, schemas, tables, columns) as the ADBC
    /// hierarchical result. Built by querying INFORMATION_SCHEMA and assembling the
    /// nested Arrow structure level by level, mirroring the Apache C# ADBC drivers.
    /// </summary>
    /// <remarks>
    /// Table constraints report names and types only; per-constraint column names and FK
    /// referenced-column usage are not populated (Snowflake's INFORMATION_SCHEMA lacks
    /// KEY_COLUMN_USAGE -- that needs SHOW PRIMARY/UNIQUE/IMPORTED KEYS + RESULT_SCAN).
    /// </remarks>
    public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern,
        string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
    {
        ThrowIfDisposed();

        IArrowArray[] dataArrays = GetCatalogs(
            depth, catalogPattern, dbSchemaPattern, tableNamePattern, tableTypes, columnNamePattern);

        return new InMemoryArrowStream(StandardSchemas.GetObjectsSchema, dataArrays);
    }

    private IArrowArray[] GetCatalogs(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern,
        string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
    {
        var catalogNameBuilder = new StringArray.Builder();
        var catalogDbSchemasValues = new List<IArrowArray?>();

        var binds = new List<string?>();
        string sql = "SELECT DATABASE_NAME::VARCHAR AS name FROM INFORMATION_SCHEMA.DATABASES";
        if (!string.IsNullOrEmpty(catalogPattern))
        {
            sql += " WHERE DATABASE_NAME ILIKE ?";
            binds.Add(catalogPattern);
        }
        sql += " ORDER BY DATABASE_NAME";

        foreach (Dictionary<string, string?> row in RunMetadataQuery(sql, binds))
        {
            string? catalog = row["name"];
            if (catalog == null)
                continue;

            catalogNameBuilder.Append(catalog);
            catalogDbSchemasValues.Add(depth == GetObjectsDepth.Catalogs
                ? null
                : GetDbSchemas(depth, catalog, dbSchemaPattern, tableNamePattern, tableTypes, columnNamePattern));
        }

        return
        [
            catalogNameBuilder.Build(),
            catalogDbSchemasValues.BuildListArrayForType(new StructType(StandardSchemas.DbSchemaSchema))
        ];
    }

    private StructArray GetDbSchemas(GetObjectsDepth depth, string catalog, string? dbSchemaPattern,
        string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
    {
        var dbSchemaNameBuilder = new StringArray.Builder();
        var dbSchemaTablesValues = new List<IArrowArray?>();
        var nullBitmap = new ArrowBuffer.BitmapBuilder();
        int length = 0;

        var binds = new List<string?>();
        string sql = $"SELECT SCHEMA_NAME::VARCHAR AS name FROM {QuoteIdentifier(catalog)}.INFORMATION_SCHEMA.SCHEMATA";
        if (!string.IsNullOrEmpty(dbSchemaPattern))
        {
            sql += " WHERE SCHEMA_NAME ILIKE ?";
            binds.Add(dbSchemaPattern);
        }
        sql += " ORDER BY SCHEMA_NAME";

        foreach (Dictionary<string, string?> row in RunMetadataQuery(sql, binds))
        {
            string? schemaName = row["name"];
            if (schemaName == null)
                continue;

            dbSchemaNameBuilder.Append(schemaName);
            nullBitmap.Append(true);
            length++;

            dbSchemaTablesValues.Add(depth == GetObjectsDepth.DbSchemas
                ? null
                : GetTableSchemas(depth, catalog, schemaName, tableNamePattern, tableTypes, columnNamePattern));
        }

        IArrowArray[] dataArrays =
        [
            dbSchemaNameBuilder.Build(),
            dbSchemaTablesValues.BuildListArrayForType(new StructType(StandardSchemas.TableSchema))
        ];

        return new StructArray(new StructType(StandardSchemas.DbSchemaSchema), length, dataArrays, nullBitmap.Build());
    }

    private StructArray GetTableSchemas(GetObjectsDepth depth, string catalog, string dbSchema,
        string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
    {
        var tableNameBuilder = new StringArray.Builder();
        var tableTypeBuilder = new StringArray.Builder();
        var tableColumnsValues = new List<IArrowArray?>();
        var tableConstraintsValues = new List<IArrowArray?>();
        var nullBitmap = new ArrowBuffer.BitmapBuilder();
        int length = 0;

        var binds = new List<string?> { dbSchema };
        string sql = $"SELECT TABLE_NAME::VARCHAR AS name, TABLE_TYPE::VARCHAR AS type "
            + $"FROM {QuoteIdentifier(catalog)}.INFORMATION_SCHEMA.TABLES "
            + "WHERE TABLE_SCHEMA ILIKE ?";
        if (!string.IsNullOrEmpty(tableNamePattern))
        {
            sql += " AND TABLE_NAME ILIKE ?";
            binds.Add(tableNamePattern);
        }
        sql += " ORDER BY TABLE_NAME";

        foreach (Dictionary<string, string?> row in RunMetadataQuery(sql, binds))
        {
            string? tableName = row["name"];
            if (tableName == null)
                continue;

            string tableType = NormalizeTableType(row["type"]);
            if (tableTypes is { Count: > 0 } && !ContainsIgnoreCase(tableTypes, tableType))
                continue;

            tableNameBuilder.Append(tableName);
            tableTypeBuilder.Append(tableType);
            nullBitmap.Append(true);
            length++;

            // Columns and constraints are populated at All depth.
            tableColumnsValues.Add(depth == GetObjectsDepth.All
                ? GetColumns(catalog, dbSchema, tableName, columnNamePattern)
                : null);
            tableConstraintsValues.Add(depth == GetObjectsDepth.All
                ? GetConstraints(catalog, dbSchema, tableName)
                : null);
        }

        IArrowArray[] dataArrays =
        [
            tableNameBuilder.Build(),
            tableTypeBuilder.Build(),
            tableColumnsValues.BuildListArrayForType(new StructType(StandardSchemas.ColumnSchema)),
            tableConstraintsValues.BuildListArrayForType(new StructType(StandardSchemas.ConstraintSchema))
        ];

        return new StructArray(new StructType(StandardSchemas.TableSchema), length, dataArrays, nullBitmap.Build());
    }

    private StructArray GetColumns(string catalog, string dbSchema, string tableName, string? columnNamePattern)
    {
        var columnNameBuilder = new StringArray.Builder();
        var ordinalPositionBuilder = new Int32Array.Builder();
        var remarksBuilder = new StringArray.Builder();
        var xdbcDataTypeBuilder = new Int16Array.Builder();
        var xdbcTypeNameBuilder = new StringArray.Builder();
        var xdbcColumnSizeBuilder = new Int32Array.Builder();
        var xdbcDecimalDigitsBuilder = new Int16Array.Builder();
        var xdbcNumPrecRadixBuilder = new Int16Array.Builder();
        var xdbcNullableBuilder = new Int16Array.Builder();
        var xdbcColumnDefBuilder = new StringArray.Builder();
        var xdbcSqlDataTypeBuilder = new Int16Array.Builder();
        var xdbcDatetimeSubBuilder = new Int16Array.Builder();
        var xdbcCharOctetLengthBuilder = new Int32Array.Builder();
        var xdbcIsNullableBuilder = new StringArray.Builder();
        var xdbcScopeCatalogBuilder = new StringArray.Builder();
        var xdbcScopeSchemaBuilder = new StringArray.Builder();
        var xdbcScopeTableBuilder = new StringArray.Builder();
        var xdbcIsAutoincrementBuilder = new BooleanArray.Builder();
        var xdbcIsGeneratedColumnBuilder = new BooleanArray.Builder();
        var nullBitmap = new ArrowBuffer.BitmapBuilder();
        int length = 0;

        string sql = "SELECT COLUMN_NAME::VARCHAR AS column_name, ORDINAL_POSITION::VARCHAR AS ordinal_position, "
            + "COMMENT::VARCHAR AS remarks, DATA_TYPE::VARCHAR AS type_name, IS_NULLABLE::VARCHAR AS is_nullable, "
            + "CHARACTER_MAXIMUM_LENGTH::VARCHAR AS char_max_length, NUMERIC_PRECISION::VARCHAR AS numeric_precision, "
            + "NUMERIC_SCALE::VARCHAR AS numeric_scale, NUMERIC_PRECISION_RADIX::VARCHAR AS numeric_precision_radix, "
            + "DATETIME_PRECISION::VARCHAR AS datetime_precision, CHARACTER_OCTET_LENGTH::VARCHAR AS char_octet_length, "
            + "COLUMN_DEFAULT::VARCHAR AS column_default "
            + $"FROM {QuoteIdentifier(catalog)}.INFORMATION_SCHEMA.COLUMNS "
            + "WHERE TABLE_SCHEMA ILIKE ? AND TABLE_NAME ILIKE ?";
        var binds = new List<string?> { dbSchema, tableName };
        if (!string.IsNullOrEmpty(columnNamePattern))
        {
            sql += " AND COLUMN_NAME ILIKE ?";
            binds.Add(columnNamePattern);
        }
        sql += " ORDER BY ORDINAL_POSITION";

        foreach (Dictionary<string, string?> row in RunMetadataQuery(sql, binds))
        {
            string? columnName = row["column_name"];
            if (columnName == null)
                continue;

            columnNameBuilder.Append(columnName);
            AppendNullableInt32(ordinalPositionBuilder, row["ordinal_position"]);
            AppendNullableString(remarksBuilder, row["remarks"]);
            xdbcDataTypeBuilder.AppendNull();
            AppendNullableString(xdbcTypeNameBuilder, row["type_name"]);
            // xdbc_column_size: character length if present, else numeric precision.
            AppendNullableInt32(xdbcColumnSizeBuilder, row["char_max_length"] ?? row["numeric_precision"]);
            AppendNullableInt16(xdbcDecimalDigitsBuilder, row["numeric_scale"]);
            AppendNullableInt16(xdbcNumPrecRadixBuilder, row["numeric_precision_radix"]);
            xdbcNullableBuilder.Append((short)(IsYes(row["is_nullable"]) ? 1 : 0));
            AppendNullableString(xdbcColumnDefBuilder, row["column_default"]);
            xdbcSqlDataTypeBuilder.AppendNull();
            AppendNullableInt16(xdbcDatetimeSubBuilder, row["datetime_precision"]);
            AppendNullableInt32(xdbcCharOctetLengthBuilder, row["char_octet_length"]);
            AppendNullableString(xdbcIsNullableBuilder, row["is_nullable"]);
            xdbcScopeCatalogBuilder.AppendNull();
            xdbcScopeSchemaBuilder.AppendNull();
            xdbcScopeTableBuilder.AppendNull();
            xdbcIsAutoincrementBuilder.AppendNull();
            xdbcIsGeneratedColumnBuilder.AppendNull();
            nullBitmap.Append(true);
            length++;
        }

        IArrowArray[] dataArrays =
        [
            columnNameBuilder.Build(),
            ordinalPositionBuilder.Build(),
            remarksBuilder.Build(),
            xdbcDataTypeBuilder.Build(),
            xdbcTypeNameBuilder.Build(),
            xdbcColumnSizeBuilder.Build(),
            xdbcDecimalDigitsBuilder.Build(),
            xdbcNumPrecRadixBuilder.Build(),
            xdbcNullableBuilder.Build(),
            xdbcColumnDefBuilder.Build(),
            xdbcSqlDataTypeBuilder.Build(),
            xdbcDatetimeSubBuilder.Build(),
            xdbcCharOctetLengthBuilder.Build(),
            xdbcIsNullableBuilder.Build(),
            xdbcScopeCatalogBuilder.Build(),
            xdbcScopeSchemaBuilder.Build(),
            xdbcScopeTableBuilder.Build(),
            xdbcIsAutoincrementBuilder.Build(),
            xdbcIsGeneratedColumnBuilder.Build()
        ];

        return new StructArray(new StructType(StandardSchemas.ColumnSchema), length, dataArrays, nullBitmap.Build());
    }

    private StructArray GetConstraints(string catalog, string dbSchema, string tableName)
    {
        var constraintNameBuilder = new StringArray.Builder();
        var constraintTypeBuilder = new StringArray.Builder();
        var constraintColumnNamesValues = new List<IArrowArray?>();
        var constraintColumnUsageValues = new List<IArrowArray?>();
        var nullBitmap = new ArrowBuffer.BitmapBuilder();
        int length = 0;

        // Snowflake's INFORMATION_SCHEMA exposes TABLE_CONSTRAINTS but NOT KEY_COLUMN_USAGE,
        // so only constraint names/types are available here. Per-constraint column names and
        // FK column-usage would require SHOW PRIMARY/UNIQUE/IMPORTED KEYS + RESULT_SCAN.
        var binds = new List<string?> { dbSchema, tableName };
        string sql = "SELECT CONSTRAINT_NAME::VARCHAR AS constraint_name, "
            + "CONSTRAINT_TYPE::VARCHAR AS constraint_type "
            + $"FROM {QuoteIdentifier(catalog)}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS "
            + "WHERE TABLE_SCHEMA ILIKE ? AND TABLE_NAME ILIKE ? "
            + "ORDER BY CONSTRAINT_NAME";

        foreach (Dictionary<string, string?> row in RunMetadataQuery(sql, binds))
        {
            string? name = row["constraint_name"];
            if (name == null)
                continue;

            constraintNameBuilder.Append(name);
            constraintTypeBuilder.Append(row["constraint_type"] ?? string.Empty);
            constraintColumnNamesValues.Add(new StringArray.Builder().Build());
            constraintColumnUsageValues.Add(null);
            nullBitmap.Append(true);
            length++;
        }

        IArrowArray[] dataArrays =
        [
            constraintNameBuilder.Build(),
            constraintTypeBuilder.Build(),
            constraintColumnNamesValues.BuildListArrayForType(StringType.Default),
            constraintColumnUsageValues.BuildListArrayForType(new StructType(StandardSchemas.UsageSchema))
        ];

        return new StructArray(new StructType(StandardSchemas.ConstraintSchema), length, dataArrays, nullBitmap.Build());
    }

    private static void AppendNullableString(StringArray.Builder builder, string? value)
    {
        if (value == null)
            builder.AppendNull();
        else
            builder.Append(value);
    }

    private static void AppendNullableInt32(Int32Array.Builder builder, string? value)
    {
        if (int.TryParse(value, out int parsed))
            builder.Append(parsed);
        else
            builder.AppendNull();
    }

    private static void AppendNullableInt16(Int16Array.Builder builder, string? value)
    {
        if (short.TryParse(value, out short parsed))
            builder.Append(parsed);
        else
            builder.AppendNull();
    }

    private static bool IsYes(string? value) => string.Equals(value, "YES", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Runs a metadata query (whose columns are all cast to VARCHAR) and materializes
    /// the result rows as string dictionaries keyed by column name (case-insensitive).
    /// </summary>
    private List<Dictionary<string, string?>> RunMetadataQuery(string sql, IReadOnlyList<string?>? bindValues = null)
    {
        if (_queryExecutor == null || _pooledConnection == null)
            throw new AdbcException("Connection is not properly initialized.");

        var request = new QueryRequest
        {
            Statement = sql,
            Warehouse = _config.Warehouse,
            Role = _config.Role,
            Timeout = _config.QueryTimeout,
            AuthToken = _pooledConnection.AuthToken
        };

        // Bind the '?' placeholders as positional (1-based) bind variables. All metadata
        // filter values are strings, so the bind type is TEXT. Server-side binding means a
        // caller-supplied pattern can never be parsed as SQL.
        if (bindValues != null)
        {
            for (int i = 0; i < bindValues.Count; i++)
            {
                request.Bindings[(i + 1).ToString(CultureInfo.InvariantCulture)] =
                    new SnowflakeBinding(BindTypeNames.Text, bindValues[i]);
            }
        }
        
        var result = _queryExecutor.ExecuteQueryAsync(request).GetAwaiter().GetResult();
        if (result.Status == QueryStatus.Failed)
        {
            string message = result.Errors.Count > 0 ? result.Errors[0].Message : "Unknown error";
            throw new AdbcException($"Metadata query failed: {message}");
        }

        var rows = new List<Dictionary<string, string?>>();
        if (result.ResultStream == null)
            return rows;

        using IArrowArrayStream stream = result.ResultStream;
        Schema schema = stream.Schema;

        while (true)
        {
            RecordBatch? batch = stream.ReadNextRecordBatchAsync().GetAwaiter().GetResult();
            if (batch == null)
                break;

            using (batch)
            {
                for (int r = 0; r < batch.Length; r++)
                {
                    var row = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
                    for (int c = 0; c < schema.FieldsList.Count; c++)
                        row[schema.FieldsList[c].Name] = GetStringValue(batch.Column(c), r);
                    rows.Add(row);
                }
            }
        }

        return rows;
    }

    private static string? GetStringValue(IArrowArray array, int index) =>
        array is StringArray stringArray ? stringArray.GetString(index) : null;

    private static string NormalizeTableType(string? informationSchemaType) =>
        string.Equals(informationSchemaType, "BASE TABLE", StringComparison.OrdinalIgnoreCase)
            ? "TABLE"
            : informationSchemaType ?? string.Empty;

    private static bool ContainsIgnoreCase(IReadOnlyList<string> values, string value)
    {
        foreach (string candidate in values)
        {
            if (string.Equals(candidate, value, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }
}
