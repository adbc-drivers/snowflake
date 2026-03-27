// Copyright (c) 2026 ADBC Drivers Contributors
//
// This file has been modified from its original version, which is
// under the Apache License:
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// src/ingest.rs
//
// Bulk ingest implementation using a VALUES-based INSERT pipeline.
// For each bound batch: CREATE TABLE (if needed) then INSERT … VALUES.
// Rows are chunked so individual SQL statements stay well under 1 MB.

use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Schema, TimeUnit};

use crate::driver::Inner;
use crate::statement::Statement;

const INSERT_CHUNK_ROWS: usize = 500;

// ── public entry point ────────────────────────────────────────────────────────

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

    let mode = stmt
        .ingest_mode
        .as_deref()
        .unwrap_or("adbc.ingest.mode.create");
    let schema = stmt.bound_batches[0].schema();

    match mode {
        "adbc.ingest.mode.create" => {
            let ddl = build_create_sql(&qname, &schema, false)?;
            run_sql(&stmt.inner, stmt.conn_handle, &ddl)?;
        }
        "adbc.ingest.mode.append" => {
            // table must already exist — no DDL needed
        }
        "adbc.ingest.mode.replace" => {
            run_sql(
                &stmt.inner,
                stmt.conn_handle,
                &format!("DROP TABLE IF EXISTS {qname}"),
            )?;
            let ddl = build_create_sql(&qname, &schema, false)?;
            run_sql(&stmt.inner, stmt.conn_handle, &ddl)?;
        }
        "adbc.ingest.mode.create_append" => {
            let ddl = build_create_sql(&qname, &schema, true)?;
            run_sql(&stmt.inner, stmt.conn_handle, &ddl)?;
        }
        other => {
            return Err(Error::with_message_and_status(
                format!("unknown ingest mode: {other}"),
                Status::InvalidArguments,
            ));
        }
    }

    let mut total = 0i64;
    for batch in &stmt.bound_batches {
        total += insert_batch(&stmt.inner, stmt.conn_handle, &qname, batch)?;
    }
    Ok(Some(total))
}

// ── DDL helpers ───────────────────────────────────────────────────────────────

/// Returns the fully-qualified, double-quoted table name.
fn qualified_name(table: &str, catalog: Option<&str>, schema: Option<&str>) -> String {
    let q = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    match (catalog, schema) {
        (Some(c), Some(s)) => format!("{}.{}.{}", q(c), q(s), q(table)),
        (None, Some(s)) => format!("{}.{}", q(s), q(table)),
        (Some(c), None) => format!("{}.{}", q(c), q(table)),
        (None, None) => q(table),
    }
}

/// Builds `CREATE [OR REPLACE] TABLE [IF NOT EXISTS] <name> (cols…)`.
fn build_create_sql(qname: &str, schema: &Schema, if_not_exists: bool) -> Result<String> {
    let mut cols = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let sf_type = to_sf_ddl(field.data_type())?;
        let null_clause = if field.is_nullable() { "" } else { " NOT NULL" };
        cols.push(format!(
            "\"{}\" {sf_type}{null_clause}",
            field.name().replace('"', "\"\"")
        ));
    }
    let exists = if if_not_exists { " IF NOT EXISTS" } else { "" };
    Ok(format!(
        "CREATE TABLE{exists} {qname} ({})",
        cols.join(", ")
    ))
}

/// Maps an Arrow DataType to its Snowflake DDL type string.
/// Matches the Go driver's `toSnowflakeType` function.
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

        DataType::Decimal128(p, s) => format!("NUMERIC({p},{s})"),

        DataType::Utf8 | DataType::LargeUtf8 => "text".to_string(),

        DataType::Binary | DataType::LargeBinary => "binary".to_string(),

        DataType::FixedSizeBinary(n) => format!("binary({n})"),

        DataType::Boolean => "boolean".to_string(),

        DataType::Date32 | DataType::Date64 => "date".to_string(),

        DataType::Time32(u) => {
            let prec = time_unit_prec(u);
            format!("time({prec})")
        }
        DataType::Time64(u) => {
            let prec = time_unit_prec(u);
            format!("time({prec})")
        }

        DataType::Timestamp(u, tz) => {
            let prec = time_unit_prec(u);
            if tz.is_some() {
                format!("timestamp_ltz({prec})")
            } else {
                format!("timestamp_ntz({prec})")
            }
        }

        // List/Struct/Map are intentionally excluded: DDL generation would succeed
        // but value_to_sql has no handler for these types, causing every data row
        // to fail.  Return NotImplemented so callers get a clear error up-front.
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_)
        | DataType::Map(_, _) => {
            return Err(Error::with_message_and_status(
                format!("ingest of nested type {dt:?} is not yet supported"),
                Status::NotImplemented,
            ))
        }

        other => {
            return Err(Error::with_message_and_status(
                format!("unsupported ingest type: {other:?}"),
                Status::NotImplemented,
            ));
        }
    })
}

fn time_unit_prec(u: &TimeUnit) -> u8 {
    match u {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

// ── INSERT helpers ────────────────────────────────────────────────────────────

fn insert_batch(
    inner: &Arc<Inner>,
    conn: sf_core::handle_manager::Handle,
    qname: &str,
    batch: &RecordBatch,
) -> Result<i64> {
    if batch.num_rows() == 0 {
        return Ok(0);
    }

    let schema = batch.schema();
    let col_list: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name().replace('"', "\"\"")))
        .collect();
    let col_clause = col_list.join(", ");

    let mut inserted = 0i64;
    let mut row = 0usize;

    while row < batch.num_rows() {
        let end = (row + INSERT_CHUNK_ROWS).min(batch.num_rows());
        let mut rows_sql = Vec::with_capacity(end - row);

        for r in row..end {
            let mut vals = Vec::with_capacity(schema.fields().len());
            for (c, field) in schema.fields().iter().enumerate() {
                vals.push(value_to_sql(
                    batch.column(c).as_ref(),
                    r,
                    field.data_type(),
                )?);
            }
            rows_sql.push(format!("({})", vals.join(", ")));
        }

        let sql = format!(
            "INSERT INTO {qname} ({col_clause}) VALUES {}",
            rows_sql.join(", ")
        );
        run_sql(inner, conn, &sql)?;
        inserted += (end - row) as i64;
        row = end;
    }

    Ok(inserted)
}

// ── SQL execution helper ──────────────────────────────────────────────────────

fn run_sql(inner: &Arc<Inner>, conn: sf_core::handle_manager::Handle, sql: &str) -> Result<()> {
    let tmp = inner
        .sf
        .statement_new(conn)
        .map_err(crate::error::api_error_to_adbc_error)?;
    let result = inner.runtime.block_on(async {
        inner
            .sf
            .statement_set_sql_query(tmp, sql.to_string())
            .await?;
        inner.sf.statement_execute_query(tmp, None).await
    });
    let _ = inner.sf.statement_release(tmp);
    result
        .map(|_| ())
        .map_err(crate::error::api_error_to_adbc_error)
}

// ── value formatting ──────────────────────────────────────────────────────────

/// Formats a single Arrow column value as a Snowflake SQL literal.
fn value_to_sql(arr: &dyn Array, row: usize, dt: &DataType) -> Result<String> {
    if arr.is_null(row) {
        return Ok("NULL".to_string());
    }

    use arrow_array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeBinaryArray, LargeStringArray, StringArray, Time32MillisecondArray, Time32SecondArray,
        Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };

    macro_rules! num {
        ($T:ty) => {
            if let Some(a) = arr.as_any().downcast_ref::<$T>() {
                return Ok(format!("{}", a.value(row)));
            }
        };
    }

    num!(Int8Array);
    num!(Int16Array);
    num!(Int32Array);
    num!(Int64Array);
    num!(UInt8Array);
    num!(UInt16Array);
    num!(UInt32Array);
    num!(UInt64Array);
    // Floats: use {:?} to always emit a decimal or exponent (avoids huge integer strings).
    // NaN and ±Inf cannot be represented in SQL; they are mapped to NULL, which is a
    // deliberate lossy conversion — callers should avoid ingesting non-finite values.
    if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        let v = a.value(row);
        return if v.is_finite() {
            Ok(format!("{:?}", v as f64))
        } else {
            Ok("NULL".to_string())
        };
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        let v = a.value(row);
        return if v.is_finite() {
            Ok(format!("{v:?}"))
        } else {
            Ok("NULL".to_string())
        };
    }

    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return Ok(if a.value(row) { "TRUE" } else { "FALSE" }.to_string());
    }

    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Ok(sql_str(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(sql_str(a.value(row)));
    }

    if let Some(a) = arr.as_any().downcast_ref::<BinaryArray>() {
        return Ok(sql_binary(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(sql_binary(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        return Ok(sql_binary(a.value(row)));
    }

    if let Some(a) = arr.as_any().downcast_ref::<Date32Array>() {
        let days = a.value(row) as i64;
        return Ok(format!("'{}'::DATE", days_to_date(days)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow_array::Date64Array>() {
        // Date64 stores milliseconds since epoch; divide to get days.
        let days = a.value(row) / 86_400_000;
        return Ok(format!("'{}'::DATE", days_to_date(days)));
    }

    // TIME
    if let Some(a) = arr.as_any().downcast_ref::<Time64NanosecondArray>() {
        return Ok(format!("'{}'::TIME(9)", ns_to_time(a.value(row))));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Time64MicrosecondArray>() {
        return Ok(format!("'{}'::TIME(6)", us_to_time(a.value(row))));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Time32MillisecondArray>() {
        return Ok(format!("'{}'::TIME(3)", ms_to_time(a.value(row) as i64)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Time32SecondArray>() {
        return Ok(format!("'{}'::TIME(0)", s_to_time(a.value(row) as i64)));
    }

    // TIMESTAMP — use TO_TIMESTAMP_NTZ / TO_TIMESTAMP_LTZ with epoch + precision
    if let Some(a) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
        let v = a.value(row);
        return Ok(match dt {
            DataType::Timestamp(_, Some(_)) => format!("TO_TIMESTAMP_LTZ({v}, 9)"),
            _ => format!("TO_TIMESTAMP_NTZ({v}, 9)"),
        });
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        let v = a.value(row);
        return Ok(match dt {
            DataType::Timestamp(_, Some(_)) => format!("TO_TIMESTAMP_LTZ({v}, 6)"),
            _ => format!("TO_TIMESTAMP_NTZ({v}, 6)"),
        });
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
        let v = a.value(row);
        return Ok(match dt {
            DataType::Timestamp(_, Some(_)) => format!("TO_TIMESTAMP_LTZ({v}, 3)"),
            _ => format!("TO_TIMESTAMP_NTZ({v}, 3)"),
        });
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampSecondArray>() {
        let v = a.value(row);
        return Ok(match dt {
            DataType::Timestamp(_, Some(_)) => format!("TO_TIMESTAMP_LTZ({v}, 0)"),
            _ => format!("TO_TIMESTAMP_NTZ({v}, 0)"),
        });
    }

    if let Some(a) = arr.as_any().downcast_ref::<Decimal128Array>()
        && let DataType::Decimal128(_, scale) = dt {
            return Ok(decimal128_to_str(a.value(row), *scale));
        }

    Err(Error::with_message_and_status(
        format!("unsupported ingest value type: {dt:?}"),
        Status::NotImplemented,
    ))
}

// ── value formatting helpers ──────────────────────────────────────────────────

fn sql_str(s: &str) -> String {
    // Double backslashes before doubling single quotes — see statement.rs sql_str_lit.
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

fn sql_binary(b: &[u8]) -> String {
    let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("TO_BINARY('{hex}', 'HEX')")
}

/// Thin wrapper so ingest.rs shares the single implementation in statement.rs.
fn days_to_date(days: i64) -> String {
    crate::statement::days_since_epoch_to_date_str(days)
}

fn ns_to_time(ns: i64) -> String {
    let h = ns / 3_600_000_000_000;
    let r = ns % 3_600_000_000_000;
    let m = r / 60_000_000_000;
    let r = r % 60_000_000_000;
    let s = r / 1_000_000_000;
    let f = r % 1_000_000_000;
    format!("{h:02}:{m:02}:{s:02}.{f:09}")
}

fn us_to_time(us: i64) -> String {
    let h = us / 3_600_000_000;
    let r = us % 3_600_000_000;
    let m = r / 60_000_000;
    let r = r % 60_000_000;
    let s = r / 1_000_000;
    let f = r % 1_000_000;
    format!("{h:02}:{m:02}:{s:02}.{f:06}")
}

fn ms_to_time(ms: i64) -> String {
    let h = ms / 3_600_000;
    let r = ms % 3_600_000;
    let m = r / 60_000;
    let r = r % 60_000;
    let s = r / 1_000;
    let f = r % 1_000;
    format!("{h:02}:{m:02}:{s:02}.{f:03}")
}

fn s_to_time(s: i64) -> String {
    let h = s / 3600;
    let r = s % 3600;
    let m = r / 60;
    let s = r % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

fn decimal128_to_str(raw: i128, scale: i8) -> String {
    if scale <= 0 {
        // Negative scale means multiply by 10^(-scale); just format as integer
        // (Arrow guarantees the stored value is already the scaled integer).
        return format!("{raw}");
    }
    let scale = scale as usize;
    let neg = raw < 0;
    let abs = raw.unsigned_abs();
    let s = format!("{abs:0>width$}", width = scale + 1);
    let int_part = &s[..s.len() - scale];
    let frac_part = &s[s.len() - scale..];
    format!("{}{int_part}.{frac_part}", if neg { "-" } else { "" })
}
