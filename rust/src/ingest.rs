// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use adbc_core::error::{Error, Result, Status};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema, TimeUnit};
use sf_core::apis::database_driver_v1::{ApiError, BindingType, DataPtr};

use crate::statement::{Statement, arrow_batches_to_json_bindings, format_arrow_value_for_csv};

const INGEST_CHUNK_ROWS: usize = 500;
const INGEST_CHUNK_BYTES: usize = 900_000;
const DEFAULT_STAGE_ARRAY_BINDING_THRESHOLD: u64 = 65_280;
const STAGE_ARRAY_BINDING_THRESHOLD_PARAMETER: &str = "CLIENT_STAGE_ARRAY_BINDING_THRESHOLD";

struct EncodedBindingChunk {
    rows: usize,
    json: String,
}

pub(crate) fn execute_ingest(stmt: &Statement) -> Result<Option<i64>> {
    if stmt.bound_batches.is_empty() {
        return Err(Error::with_message_and_status(
            "ingest requires bound data — call bind() or bind_stream() first",
            Status::InvalidState,
        ));
    }

    let table = stmt.target_table.as_deref().ok_or_else(|| {
        Error::with_message_and_status("target_table not set", Status::InvalidState)
    })?;
    let qname = qualified_name(
        table,
        stmt.ingest_catalog.as_deref(),
        stmt.ingest_schema.as_deref(),
    );
    let schema = stmt.bound_batches[0].schema();

    match stmt
        .ingest_mode
        .as_deref()
        .unwrap_or("adbc.ingest.mode.create")
    {
        "adbc.ingest.mode.create" => {
            stmt.inner.execute_temporary_statement(
                stmt.conn_handle,
                build_create_sql(&qname, &schema, false)?,
                stmt.query_timeout_seconds,
            )?;
        }
        "adbc.ingest.mode.append" => {}
        "adbc.ingest.mode.replace" => {
            stmt.inner.execute_temporary_statement(
                stmt.conn_handle,
                format!("DROP TABLE IF EXISTS {qname}"),
                stmt.query_timeout_seconds,
            )?;
            stmt.inner.execute_temporary_statement(
                stmt.conn_handle,
                build_create_sql(&qname, &schema, false)?,
                stmt.query_timeout_seconds,
            )?;
        }
        "adbc.ingest.mode.create_append" => {
            stmt.inner.execute_temporary_statement(
                stmt.conn_handle,
                build_create_sql(&qname, &schema, true)?,
                stmt.query_timeout_seconds,
            )?;
        }
        other => {
            return Err(Error::with_message_and_status(
                format!("unknown ingest mode: {other}"),
                Status::InvalidArguments,
            ));
        }
    }

    let insert_sql = build_insert_sql(&qname, &schema);
    let threshold = stage_binding_threshold(stmt)?;
    if let Some(csv) = encode_csv_stage_binding(&stmt.bound_batches, threshold)? {
        let csv_len = i64::try_from(csv.len()).map_err(|_| {
            Error::with_message_and_status(
                "CSV stage binding is too large",
                Status::InvalidArguments,
            )
        })?;
        let binding = BindingType::Csv(DataPtr::new(csv.as_ptr(), csv_len));
        // Keep the CSV owner in scope until sf_core finishes uploading it. A
        // StageBinding error occurs before the INSERT is submitted, so it is
        // the only safe error on which to retry with inline JSON bindings.
        match stmt.inner.execute_query_raw_with_timeout(
            stmt.stmt_handle,
            insert_sql.clone(),
            Some(binding),
            stmt.query_timeout_seconds,
        ) {
            Ok(result) => {
                let result = stmt.inner.acquire_execute_result(result)?;
                return Ok(result.descriptor.rows_affected);
            }
            Err(error) if should_fallback_from_stage_binding(&error) => {}
            Err(error) => return Err(crate::error::api_error_to_adbc_error(error)),
        }
    }

    let mut total = Some(0i64);
    for batch in &stmt.bound_batches {
        for chunk in encode_binding_chunks(batch)? {
            debug_assert!((1..=INGEST_CHUNK_ROWS).contains(&chunk.rows));
            let json_len = i64::try_from(chunk.json.len()).map_err(|_| {
                Error::with_message_and_status(
                    "JSON binding is too large",
                    Status::InvalidArguments,
                )
            })?;
            let binding = BindingType::Json(DataPtr::new(chunk.json.as_ptr(), json_len));
            // The encoded String stays in scope until execute_query's block_on completes.
            let result = stmt.execute_query(insert_sql.clone(), Some(binding))?;

            total = match (total, result.descriptor.rows_affected) {
                (Some(acc), Some(rows)) => Some(acc.checked_add(rows).ok_or_else(|| {
                    Error::with_message_and_status("ingest row count overflow", Status::Internal)
                })?),
                _ => None,
            };
        }
    }
    Ok(total)
}

fn should_fallback_from_stage_binding(error: &ApiError) -> bool {
    matches!(error, ApiError::StageBinding { .. })
}

fn parse_stage_binding_threshold(raw: Option<&str>) -> u64 {
    raw.and_then(|value| value.parse::<u32>().ok())
        .map(u64::from)
        .unwrap_or(DEFAULT_STAGE_ARRAY_BINDING_THRESHOLD)
}

fn stage_binding_threshold(stmt: &Statement) -> Result<u64> {
    let raw = stmt
        .inner
        .runtime
        .block_on(stmt.inner.sf.connection_get_parameter(
            stmt.conn_handle,
            STAGE_ARRAY_BINDING_THRESHOLD_PARAMETER.to_string(),
        ))
        .map_err(crate::error::api_error_to_adbc_error)?;
    Ok(parse_stage_binding_threshold(raw.as_deref()))
}

fn effective_binding_cells(batches: &[RecordBatch]) -> Option<u64> {
    batches.iter().try_fold(0u64, |total, batch| {
        let rows = u64::try_from(batch.num_rows()).ok()?;
        let columns = u64::try_from(batch.num_columns()).ok()?;
        total.checked_add(rows.checked_mul(columns)?)
    })
}

fn csv_stage_type_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Boolean
            | DataType::Date32
            | DataType::Date64
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
    )
}

fn append_csv_cell(output: &mut String, value: &str) {
    // sf_core's stage COPY uses FIELD_OPTIONALLY_ENCLOSED_BY='"'. Quoting every
    // non-NULL cell is RFC-4180-compatible and preserves NULL (bare empty) vs
    // empty string (quoted empty); embedded quotes are doubled.
    output.push('"');
    for character in value.chars() {
        if character == '"' {
            output.push('"');
        }
        output.push(character);
    }
    output.push('"');
}

/// Build one row-major CSV payload for stage binding. Returning `None` is an
/// intentional signal to retain the existing chunked JSON path.
fn encode_csv_stage_binding(batches: &[RecordBatch], threshold: u64) -> Result<Option<String>> {
    let Some(effective_cells) = effective_binding_cells(batches) else {
        return Ok(None);
    };
    if threshold == 0 || effective_cells == 0 || effective_cells < threshold {
        return Ok(None);
    }

    let Some(first) = batches.first() else {
        return Ok(None);
    };
    let schema = first.schema();
    if schema
        .fields()
        .iter()
        .any(|field| !csv_stage_type_supported(field.data_type()))
        || batches.iter().any(|batch| batch.schema() != schema)
    {
        return Ok(None);
    }

    let mut output = String::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            for column in 0..batch.num_columns() {
                if column != 0 {
                    output.push(',');
                }
                let array = batch.column(column);
                if array.is_null(row) {
                    continue;
                }
                let value = format_arrow_value_for_csv(
                    array.as_ref(),
                    row,
                    schema.field(column).data_type(),
                )?;
                let Some(value) = value else {
                    // Non-finite floating point values follow the JSON path's
                    // NULL behavior but remain explicitly unsupported for CSV.
                    return Ok(None);
                };
                if matches!(
                    schema.field(column).data_type(),
                    DataType::Utf8 | DataType::LargeUtf8
                ) && value == r"\N"
                {
                    // sf_core COPY uses NULL_IF=('\\N'); quoting does not make
                    // this literal distinguishable from NULL on that path.
                    return Ok(None);
                }
                append_csv_cell(&mut output, &value);
            }
            output.push('\n');
        }
    }
    Ok(Some(output))
}

fn encode_binding_chunks(batch: &RecordBatch) -> Result<Vec<EncodedBindingChunk>> {
    let mut chunks = Vec::new();
    let mut offset = 0;
    while offset < batch.num_rows() {
        let rows = INGEST_CHUNK_ROWS.min(batch.num_rows() - offset);
        encode_binding_range(batch, offset, rows, &mut chunks)?;
        offset += rows;
    }
    Ok(chunks)
}

fn encode_binding_range(
    batch: &RecordBatch,
    offset: usize,
    rows: usize,
    output: &mut Vec<EncodedBindingChunk>,
) -> Result<()> {
    let slice = batch.slice(offset, rows);
    let json = arrow_batches_to_json_bindings(std::slice::from_ref(&slice))?;
    if json.len() <= INGEST_CHUNK_BYTES {
        output.push(EncodedBindingChunk { rows, json });
        return Ok(());
    }
    if rows == 1 {
        return Err(Error::with_message_and_status(
            format!("single-row JSON binding exceeds the {INGEST_CHUNK_BYTES}-byte ingest limit"),
            Status::InvalidArguments,
        ));
    }

    let first_rows = rows / 2;
    encode_binding_range(batch, offset, first_rows, output)?;
    encode_binding_range(batch, offset + first_rows, rows - first_rows, output)
}

fn qualified_name(table: &str, catalog: Option<&str>, schema: Option<&str>) -> String {
    let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    match (catalog, schema) {
        (Some(c), Some(s)) => format!("{}.{}.{}", quote(c), quote(s), quote(table)),
        (None, Some(s)) => format!("{}.{}", quote(s), quote(table)),
        (Some(c), None) => format!("{}..{}", quote(c), quote(table)),
        (None, None) => quote(table),
    }
}

fn build_create_sql(qname: &str, schema: &Schema, if_not_exists: bool) -> Result<String> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let sf_type = to_sf_ddl(field.data_type())?;
            let null_clause = if field.is_nullable() { "" } else { " NOT NULL" };
            Ok(format!(
                "\"{}\" {sf_type}{null_clause}",
                field.name().replace('"', "\"\"")
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let exists = if if_not_exists { " IF NOT EXISTS" } else { "" };
    Ok(format!(
        "CREATE TABLE{exists} {qname} ({})",
        columns.join(", ")
    ))
}

fn build_insert_sql(qname: &str, schema: &Schema) -> String {
    let columns = schema
        .fields()
        .iter()
        .map(|field| format!("\"{}\"", field.name().replace('"', "\"\"")))
        .collect::<Vec<_>>();
    let placeholders = vec!["?"; columns.len()];
    format!(
        "INSERT INTO {qname} ({}) VALUES ({})",
        columns.join(", "),
        placeholders.join(", ")
    )
}

fn to_sf_ddl(dt: &DataType) -> Result<String> {
    Ok(match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => "integer".to_string(),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "double".to_string(),
        DataType::Decimal128(precision, scale) => format!("NUMERIC({precision},{scale})"),
        DataType::Utf8 | DataType::LargeUtf8 => "text".to_string(),
        DataType::Binary | DataType::LargeBinary => "binary".to_string(),
        DataType::FixedSizeBinary(size) => format!("binary({size})"),
        DataType::Boolean => "boolean".to_string(),
        DataType::Date32 | DataType::Date64 => "date".to_string(),
        DataType::Time32(unit) | DataType::Time64(unit) => {
            format!("time({})", time_unit_precision(unit))
        }
        DataType::Timestamp(unit, timezone) => {
            let kind = if timezone.is_some() {
                "timestamp_ltz"
            } else {
                "timestamp_ntz"
            };
            format!("{kind}({})", time_unit_precision(unit))
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_)
        | DataType::Map(_, _) => {
            return Err(Error::with_message_and_status(
                format!("ingest of nested type {dt:?} is not yet supported"),
                Status::NotImplemented,
            ));
        }
        other => {
            return Err(Error::with_message_and_status(
                format!("unsupported ingest type: {other:?}"),
                Status::NotImplemented,
            ));
        }
    })
}

fn time_unit_precision(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow_array::{
        ArrayRef, BinaryArray, Date32Array, Decimal128Array, Int64Array, NullArray, RecordBatch,
        StringArray, Time64NanosecondArray, TimestampMicrosecondArray,
    };
    use arrow_schema::Field;

    #[test]
    fn catalog_without_schema_uses_snowflake_double_dot_qualification() {
        assert_eq!(
            qualified_name("table", Some("catalog"), None),
            "\"catalog\"..\"table\""
        );
    }

    #[test]
    fn insert_sql_uses_quoted_columns_and_placeholders_only() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("odd\"name", DataType::Utf8, true),
        ]);
        assert_eq!(
            build_insert_sql("\"db\".\"table\"", &schema),
            "INSERT INTO \"db\".\"table\" (\"id\", \"odd\"\"name\") VALUES (?, ?)"
        );
    }

    #[test]
    fn ingest_binding_reuses_arrow_json_encoder_for_values_and_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("not SQL ' text"), None])) as ArrayRef,
            ],
        )
        .unwrap();

        let sql = build_insert_sql("\"target\"", &schema);
        let json = arrow_batches_to_json_bindings(&[batch]).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO \"target\" (\"id\", \"name\") VALUES (?, ?)"
        );
        assert!(!sql.contains("not SQL"));
        assert!(json.contains("\"value\":[\"not SQL ' text\",null]"));
    }

    #[test]
    fn stage_threshold_defaults_and_preserves_explicit_zero() {
        assert_eq!(parse_stage_binding_threshold(None), 65_280);
        assert_eq!(parse_stage_binding_threshold(Some("invalid")), 65_280);
        assert_eq!(parse_stage_binding_threshold(Some("-1")), 65_280);
        assert_eq!(parse_stage_binding_threshold(Some("4294967296")), 65_280);
        assert_eq!(parse_stage_binding_threshold(Some("0")), 0);
        assert_eq!(parse_stage_binding_threshold(Some("20")), 20);
    }

    #[test]
    fn csv_selection_uses_effective_cells_at_exact_threshold() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("left", DataType::Int64, false),
            Field::new("right", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(0..10)) as ArrayRef,
                Arc::new(Int64Array::from_iter_values(10..20)) as ArrayRef,
            ],
        )
        .unwrap();

        assert!(
            encode_csv_stage_binding(std::slice::from_ref(&batch), 20)
                .unwrap()
                .is_some()
        );
        assert!(encode_csv_stage_binding(&[batch], 21).unwrap().is_none());
    }

    #[test]
    fn csv_distinguishes_null_and_empty_and_escapes_rfc4180_hazards() {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                None,
                Some(""),
                Some("comma, quote \" and\nnewline"),
            ])) as ArrayRef],
        )
        .unwrap();

        let csv = encode_csv_stage_binding(&[batch], 1).unwrap().unwrap();
        assert_eq!(csv, "\n\"\"\n\"comma, quote \"\" and\nnewline\"\n");
    }

    #[test]
    fn csv_literal_backslash_n_text_falls_back_to_json() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![r"\N"])) as ArrayRef],
        )
        .unwrap();

        assert!(encode_csv_stage_binding(&[batch], 1).unwrap().is_none());
    }

    #[test]
    fn only_typed_stage_binding_errors_allow_json_retry() {
        use sf_core::stage_binding::StageBindingError;

        let stage_error = ApiError::StageBinding {
            source: Box::new(StageBindingError::Disabled {
                location: snafu::Location::default(),
            }),
            location: snafu::Location::default(),
        };
        let submitted_query_error = ApiError::Cancelled {
            location: snafu::Location::default(),
        };

        assert!(should_fallback_from_stage_binding(&stage_error));
        assert!(!should_fallback_from_stage_binding(&submitted_query_error));
    }

    #[test]
    fn csv_preserves_decimal_binary_and_temporal_wire_values() {
        let decimal = Decimal128Array::from(vec![12345i128])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("decimal", DataType::Decimal128(10, 2), false),
            Field::new("binary", DataType::Binary, false),
            Field::new("date", DataType::Date32, false),
            Field::new("time", DataType::Time64(TimeUnit::Nanosecond), false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(decimal) as ArrayRef,
                Arc::new(BinaryArray::from(vec![&b"\x00\xff"[..]])) as ArrayRef,
                Arc::new(Date32Array::from(vec![365_000])) as ArrayRef,
                Arc::new(Time64NanosecondArray::from(vec![123])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![1_234_567])) as ArrayRef,
            ],
        )
        .unwrap();

        assert_eq!(
            encode_csv_stage_binding(&[batch], 1).unwrap().unwrap(),
            "\"123.45\",\"00ff\",\"31536000000000000000\",\"123\",\"1234567000\"\n"
        );
    }

    #[test]
    fn csv_combines_all_compatible_batches_into_one_payload() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let first = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .unwrap();
        let second = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![3])) as ArrayRef],
        )
        .unwrap();

        assert_eq!(
            encode_csv_stage_binding(&[first, second], 3)
                .unwrap()
                .unwrap(),
            "\"1\"\n\"2\"\n\"3\"\n"
        );
    }

    #[test]
    fn unsupported_csv_type_deliberately_falls_back_to_json_path() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "unsupported",
            DataType::Null,
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(NullArray::new(1)) as ArrayRef]).unwrap();

        assert!(encode_csv_stage_binding(&[batch], 1).unwrap().is_none());
    }

    #[test]
    fn binding_batches_are_chunked_to_at_most_five_hundred_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from_iter_values(0..1_201)) as ArrayRef],
        )
        .unwrap();

        let chunks = encode_binding_chunks(&batch).unwrap();
        assert_eq!(
            chunks.iter().map(|chunk| chunk.rows).collect::<Vec<_>>(),
            vec![500, 500, 201]
        );
        assert!(chunks.iter().all(|chunk| chunk.rows <= INGEST_CHUNK_ROWS));
    }

    #[test]
    fn large_json_chunks_are_split_below_the_byte_ceiling() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let values = (0..10).map(|_| "x".repeat(100_000)).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(values)) as ArrayRef],
        )
        .unwrap();

        let chunks = encode_binding_chunks(&batch).unwrap();
        assert!(chunks.len() > 1);
        assert_eq!(chunks.iter().map(|chunk| chunk.rows).sum::<usize>(), 10);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.json.len() <= INGEST_CHUNK_BYTES)
        );
    }

    #[test]
    fn empty_schema_still_generates_a_parameterized_shape() {
        assert_eq!(
            build_insert_sql("\"target\"", &Schema::empty()),
            "INSERT INTO \"target\" () VALUES ()"
        );
    }
}
