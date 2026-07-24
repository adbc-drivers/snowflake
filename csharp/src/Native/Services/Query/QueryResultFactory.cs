using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Snowflake.Native.Services.Authentication;
using AdbcDrivers.Snowflake.Native.Services.Transport;
using AdbcDrivers.Snowflake.Native.Services.TypeConversion;

using Apache.Arrow;
using Apache.Arrow.Types;

namespace AdbcDrivers.Snowflake.Native.Services.Query;

/// <summary>
/// The shapes a successful query response can take. <see cref="QueryResultFactory"/> classifies
/// a response into one of these and builds the matching <see cref="QueryResult"/>; the checks in
/// <see cref="QueryResultFactory.Classify"/> run in declaration order.
/// </summary>
internal enum ResultShape
{
    /// <summary>Arrow data, inline (rowsetBase64) and/or in downloadable chunks: a SELECT that returned rows.</summary>
    ArrowData,

    /// <summary>Arrow format with no data at all: a SELECT that matched zero rows; only the rowtype metadata carries the schema.</summary>
    EmptyArrow,

    /// <summary>The JSON affected-count summary row of a DML statement (INSERT/UPDATE/DELETE/MERGE).</summary>
    DmlSummary,

    /// <summary>Any other JSON rowset: command output such as DDL status messages, USE/ALTER SESSION, SHOW.</summary>
    CommandRowSet,

    /// <summary>
    /// Neither Arrow data nor a rowset — a response the driver cannot represent as a result:
    /// shapes it does not support (multi-statement parents) or malformed payloads. Surfaced
    /// as a failed result, not a success. (Query-in-progress responses never reach
    /// classification — the executor polls them to completion first.)
    /// </summary>
    Unsupported,
}

/// <summary>
/// Turns a successful Snowflake query response into the <see cref="QueryResult"/> that matches
/// its <see cref="ResultShape"/>. Owns all result materialization — Arrow stream assembly,
/// empty-result schemas, DML summaries, command rowsets — leaving <see cref="QueryExecutor"/>
/// with the transport and session concerns.
/// </summary>
internal sealed class QueryResultFactory(IRestApiClient apiClient, ITypeConverter typeConverter)
{
    private readonly IRestApiClient _apiClient = apiClient ?? throw new ArgumentNullException(nameof(apiClient));
    private readonly ITypeConverter _typeConverter = typeConverter ?? throw new ArgumentNullException(nameof(typeConverter));

    internal static ResultShape Classify(SnowflakeQueryResponse data)
    {
        if (HasArrowData(data))
            return ResultShape.ArrowData;

        if (IsZeroRowArrowResult(data))
            return ResultShape.EmptyArrow;

        if (TryGetDmlAffectedRows(data, out _))
            return ResultShape.DmlSummary;

        if (data is { RowType: { Count: > 0 }, RowSet: not null })
            return ResultShape.CommandRowSet;

        return ResultShape.Unsupported;
    }

    internal async Task<QueryResult> CreateResultAsync(
        ResultShape shape,
        SnowflakeQueryResponse data,
        AuthenticationToken authToken,
        int prefetchConcurrency,
        CancellationToken cancellationToken)
    {
        return shape switch
        {
            ResultShape.ArrowData => await CreateArrowStreamResultAsync(data, authToken, prefetchConcurrency, cancellationToken).ConfigureAwait(false),
            ResultShape.EmptyArrow => CreateEmptyArrowResult(data),
            ResultShape.DmlSummary => CreateDmlSummaryResult(data),
            ResultShape.CommandRowSet => CreateCommandRowSetResult(data),
            ResultShape.Unsupported => CreateUnsupportedShapeResult(data),
            _ => throw new NotSupportedException($"Result shape {shape} has no handler.")
        };
    }

    private static bool HasArrowData(SnowflakeQueryResponse data) =>
        !string.IsNullOrEmpty(data.RowSetBase64) || (data.Chunks?.Count > 0);

    /// <summary>
    /// Arrow format with rowtype metadata but no row data anywhere: a SELECT that matched zero
    /// rows. Requiring the rowset to be absent or empty keeps this check order-independent of
    /// the JSON-rowset shapes — a response carrying JSON rows (a DML summary or command
    /// output) can never classify as an empty Arrow result, whatever format it reports.
    /// </summary>
    private static bool IsZeroRowArrowResult(SnowflakeQueryResponse data) =>
        IsArrowFormat(data)
        && data is { RowType: { Count: > 0 }, RowSet: not { Count: > 0 } };

    private static bool IsArrowFormat(SnowflakeQueryResponse data) =>
        string.Equals(data.QueryResultFormat, "arrow", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Detects a DML row-count summary result and sums the affected-row counts.
    /// Snowflake returns DML results as a JSON rowset whose columns are named
    /// "number of rows inserted" / "...updated" / "...deleted" (MERGE returns several).
    /// The summary is normally a single row, but every row is summed so a multi-row
    /// summary would still report the full count.
    /// </summary>
    internal static bool TryGetDmlAffectedRows(SnowflakeQueryResponse data, out long affectedRows)
    {
        affectedRows = 0;

        List<RowType>? rowTypes = data.RowType;
        List<List<string>>? rowSet = data.RowSet;
        if (rowTypes == null || rowTypes.Count == 0 || rowSet == null || rowSet.Count == 0)
            return false;

        foreach (RowType rowType in rowTypes)
        {
            if (rowType.Name == null ||
                !rowType.Name.StartsWith("number of ", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        long total = 0;
        foreach (List<string>? row in rowSet)
        {
            if (row == null)
                continue;

            foreach (string cell in row)
            {
                if (long.TryParse(cell, NumberStyles.Integer, CultureInfo.InvariantCulture, out long count))
                    total += count;
            }
        }

        affectedRows = total;
        return true;
    }

    private async Task<QueryResult> CreateArrowStreamResultAsync(
        SnowflakeQueryResponse data,
        AuthenticationToken authToken,
        int prefetchConcurrency,
        CancellationToken cancellationToken)
    {
        var arrayStream = await ChunkedArrowArrayStream.CreateAsync(
            _apiClient,
            authToken,
            data.RowSetBase64,
            data.Chunks,
            data.ChunkHeaders,
            data.Qrmk,
            cancellationToken,
            prefetchConcurrency).ConfigureAwait(false);

        // Apply Snowflake-specific result fixups (e.g. rescaling FIXED-with-scale integer
        // columns to Decimal128) before exposing the stream.
        return QueryResult.Success(new SnowflakeResultArrowStream(arrayStream), data.Returned ?? 0);
    }

    /// <summary>
    /// Builds the result for a zero-row Arrow response: an empty stream whose schema is built
    /// from the rowtype metadata, so callers read an empty result set instead of failing on a
    /// missing stream. The schema matches what a non-empty result would surface, because the
    /// type converter applies the same FIXED sizing rule as the result decoder.
    /// </summary>
    private QueryResult CreateEmptyArrowResult(SnowflakeQueryResponse data)
    {
        Schema? schema;
        try
        {
            schema = BuildSchemaFromRowType(data.RowType);
        }
        catch (NotSupportedException ex)
        {
            // A rowtype column type the converter cannot map (e.g. a type newer than the
            // driver). A non-empty result would pass such a column through in whatever Arrow
            // encoding Snowflake sends, but with zero rows there is no wire type to fall back
            // on — fail with the real reason instead of a generic execution error.
            return QueryResult.Failed(
                "UNSUPPORTED_RESULT_SCHEMA",
                "The statement returned zero rows and the driver cannot build the result schema " +
                    $"from rowtype metadata: {ex.Message}",
                ex);
        }

        if (schema == null)
            return CreateUnsupportedShapeResult(data);

        return QueryResult.Success(new EmptyArrowArrayStream(schema), data.Returned ?? 0);
    }

    /// <summary>
    /// Builds the result for a DML statement. The affected-count summary row is surfaced as a
    /// result set (an Int64 column per count, matching the Go driver) so ExecuteQuery on a DML
    /// statement returns something readable; <see cref="QueryResult.AffectedRows"/> carries the
    /// summed count for ExecuteUpdate.
    /// </summary>
    private static QueryResult CreateDmlSummaryResult(SnowflakeQueryResponse data)
    {
        if (!TryGetDmlAffectedRows(data, out long affectedRows))
            throw new NotSupportedException("Response is not a DML row-count summary.");

        // TryGetDmlAffectedRows validated that rowtype and rowset are present and non-empty.
        // The summary is normally a single row, but every row is surfaced.
        List<RowType> rowTypes = data.RowType!;
        List<List<string>> rowSet = data.RowSet!;

        var fields = new List<Field>(rowTypes.Count);
        var columns = new List<IArrowArray>(rowTypes.Count);
        for (int i = 0; i < rowTypes.Count; i++)
        {
            fields.Add(new Field(rowTypes[i].Name ?? string.Empty, Int64Type.Default, nullable: true));
            var builder = new Int64Array.Builder();
            foreach (List<string>? row in rowSet)
            {
                if (row != null && i < row.Count && long.TryParse(row[i], NumberStyles.Integer, CultureInfo.InvariantCulture, out long count))
                    builder.Append(count);
                else
                    builder.AppendNull();
            }
            columns.Add(builder.Build());
        }

        return QueryResult.Success(
            new InMemoryArrowStream(new Schema(fields, null), columns),
            rowCount: affectedRows,
            affectedRows: affectedRows);
    }

    /// <summary>
    /// Surfaces a JSON rowset (DDL/USE status messages, SHOW output, ...) as a result set of
    /// string columns. SELECT results always arrive as Arrow (the session forces
    /// DOTNET_QUERY_RESULT_FORMAT=ARROW), so a JSON rowset is a command output whose wire
    /// values are strings.
    /// </summary>
    private static QueryResult CreateCommandRowSetResult(SnowflakeQueryResponse data)
    {
        List<RowType> rowTypes = data.RowType!;
        List<List<string>> rowSet = data.RowSet!;

        var fields = new List<Field>(rowTypes.Count);
        var columns = new List<IArrowArray>(rowTypes.Count);
        for (int i = 0; i < rowTypes.Count; i++)
        {
            fields.Add(new Field(rowTypes[i].Name ?? string.Empty, StringType.Default, nullable: true));
            var builder = new StringArray.Builder();
            foreach (List<string>? row in rowSet)
            {
                string? value = row != null && i < row.Count ? row[i] : null;
                if (value == null)
                    builder.AppendNull();
                else
                    builder.Append(value);
            }
            columns.Add(builder.Build());
        }

        return QueryResult.Success(
            new InMemoryArrowStream(new Schema(fields, null), columns),
            data.Returned ?? rowSet.Count);
    }

    /// <summary>
    /// Builds a failed result for a response the driver cannot represent (see
    /// <see cref="ResultShape.Unsupported"/>). Failing here is deliberate: returning a
    /// stream-less success would let ExecuteUpdate report a bogus completed-with-0-rows for a
    /// statement that did something the caller cannot observe.
    /// </summary>
    private static QueryResult CreateUnsupportedShapeResult(SnowflakeQueryResponse data) =>
        QueryResult.Failed(
            "UNSUPPORTED_RESULT_SHAPE",
            "The query succeeded but returned a response the driver cannot represent " +
                $"(queryResultFormat={data.QueryResultFormat ?? "<null>"}, hasRowType={data.RowType != null}, hasRowSet={data.RowSet != null}). " +
                "Multi-statement requests are not supported.");

    /// <summary>
    /// Builds an Arrow schema from the response's rowtype metadata (used for describe results
    /// and zero-row result sets), applying the same FIXED sizing rule as the result decoder so
    /// described and materialized schemas always agree.
    /// </summary>
    internal Schema? BuildSchemaFromRowType(List<RowType>? rowTypes)
    {
        if (rowTypes == null || rowTypes.Count == 0)
            return null;

        var fields = new List<Field>(rowTypes.Count);
        foreach (RowType rowType in rowTypes)
        {
            var snowflakeType = new SnowflakeDataType
            {
                TypeName = rowType.Type ?? string.Empty,
                Precision = rowType.Precision,
                Scale = rowType.Scale,
                Length = rowType.Length,
                IsNullable = rowType.Nullable ?? true
            };

            fields.Add(new Field(
                rowType.Name ?? string.Empty,
                _typeConverter.ConvertSnowflakeTypeToArrow(snowflakeType),
                rowType.Nullable ?? true));
        }

        return new Schema(fields, null);
    }
}
