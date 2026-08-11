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

use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema, TimeUnit};
use sf_core::apis::database_driver_v1::{BindingType, DataPtr};

use crate::driver::Inner;
use crate::statement::{Statement, arrow_batches_to_json_bindings};

const INGEST_CHUNK_ROWS: usize = 500;
const INGEST_CHUNK_BYTES: usize = 900_000;

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
            run_sql(
                &stmt.inner,
                stmt.conn_handle,
                &build_create_sql(&qname, &schema, false)?,
            )?;
        }
        "adbc.ingest.mode.append" => {}
        "adbc.ingest.mode.replace" => {
            run_sql(
                &stmt.inner,
                stmt.conn_handle,
                &format!("DROP TABLE IF EXISTS {qname}"),
            )?;
            run_sql(
                &stmt.inner,
                stmt.conn_handle,
                &build_create_sql(&qname, &schema, false)?,
            )?;
        }
        "adbc.ingest.mode.create_append" => {
            run_sql(
                &stmt.inner,
                stmt.conn_handle,
                &build_create_sql(&qname, &schema, true)?,
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
            let result =
                stmt.inner
                    .execute_query(stmt.stmt_handle, insert_sql.clone(), Some(binding))?;

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
        (Some(c), None) => format!("{}.{}", quote(c), quote(table)),
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

fn run_sql(
    inner: &Arc<Inner>,
    connection: sf_core::handle_manager::Handle,
    sql: &str,
) -> Result<()> {
    let statement = inner
        .sf
        .statement_new(connection)
        .map_err(crate::error::api_error_to_adbc_error)?;
    let result = inner.execute_query(statement, sql.to_string(), None);
    let release = inner.sf.statement_release(statement);
    match result {
        Ok(_) => release
            .map_err(crate::error::api_error_to_adbc_error)
            .map(|_| ()),
        Err(error) => {
            let _ = release;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::Field;

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
