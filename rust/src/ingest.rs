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

use std::{
    collections::HashSet,
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

use adbc_core::{
    error::{Error, Result, Status},
    options::OptionValue,
};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_cast::{cast, display::array_value_to_string};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::{
    arrow::ArrowWriter,
    basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel},
    file::properties::{EnabledStatistics, WriterProperties},
};
use sf_core::apis::{database_driver_v1::Handle, operation_ctx::OperationCtx};
use tokio::{sync::Semaphore, task::JoinHandle};

use crate::{
    connection::{qualified_table_identifier, quote_identifier},
    driver::Inner,
    statement::{Bound, Statement},
};

const INGEST_OPTION_PREFIX: &str = "adbc.snowflake.statement.";
const GEO_TYPE_OPTION: &str = "adbc.snowflake.statement.ingest_geo_type";
static NEXT_STAGE: AtomicU64 = AtomicU64::new(0);

type BatchIter = Box<dyn Iterator<Item = std::result::Result<RecordBatch, ArrowError>> + Send>;

#[derive(Clone, Debug)]
pub(crate) struct IngestOptions {
    writer_concurrency: usize,
    upload_concurrency: usize,
    copy_concurrency: usize,
    target_file_size: usize,
    compression_codec: String,
    compression_level: i64,
    vectorized_scanner: bool,
    geo_type: String,
    geo_type_explicit: bool,
}

impl Default for IngestOptions {
    fn default() -> Self {
        Self {
            writer_concurrency: std::thread::available_parallelism().map_or(1, usize::from),
            upload_concurrency: 8,
            copy_concurrency: 4,
            target_file_size: 10 * 1024 * 1024,
            compression_codec: "SNAPPY".into(),
            compression_level: -1,
            vectorized_scanner: true,
            geo_type: "geography".into(),
            geo_type_explicit: false,
        }
    }
}

impl IngestOptions {
    /// Returns false only for an unrecognized ingest option.
    pub(crate) fn set(&mut self, key: &str, value: &OptionValue) -> Result<bool> {
        let Some(name) = key.strip_prefix(INGEST_OPTION_PREFIX) else {
            return Ok(false);
        };
        match name {
            "ingest_writer_concurrency"
            | "ingest_upload_concurrency"
            | "ingest_copy_concurrency"
            | "ingest_target_file_size" => {
                let n = ingest_int(key, value)?;
                if n < 0 {
                    return Err(invalid_option(key, n));
                }
                let n = usize::try_from(n).map_err(|_| invalid_option(key, n))?;
                match name {
                    "ingest_writer_concurrency" => {
                        self.writer_concurrency = if n == 0 {
                            Self::default().writer_concurrency
                        } else {
                            n
                        };
                    }
                    "ingest_upload_concurrency" => {
                        self.upload_concurrency = if n == 0 { 8 } else { n };
                    }
                    // In Go 0 means unlimited *background* copies, not final COPY only.
                    "ingest_copy_concurrency" => self.copy_concurrency = n,
                    // In Go 0 splits after every nonempty batch, not unlimited size.
                    "ingest_target_file_size" => self.target_file_size = n,
                    _ => unreachable!(),
                }
            }
            "ingest_compression_level" => {
                self.compression_level = ingest_int(key, value)?;
            }
            "ingest_compression_codec" => {
                let val = ingest_string(key, value)?;
                let codec = val.trim().to_ascii_uppercase();
                if !matches!(
                    codec.as_str(),
                    "UNCOMPRESSED" | "SNAPPY" | "GZIP" | "BROTLI" | "ZSTD" | "LZ4_RAW"
                ) {
                    return Err(Error::with_message_and_status(
                        format!(
                            "[Snowflake] invalid compression codec '{val}': must be one of uncompressed, snappy, gzip, brotli, zstd, lz4_raw"
                        ),
                        Status::InvalidArguments,
                    ));
                }
                self.compression_codec = codec;
            }
            "ingest_use_vectorized_scanner" => {
                let val = ingest_string(key, value)?;
                self.vectorized_scanner = match val {
                    "true" | "TRUE" | "True" | "1" | "t" | "T" => true,
                    "false" | "FALSE" | "False" | "0" | "f" | "F" => false,
                    _ => {
                        return Err(Error::with_message_and_status(
                            format!(
                                "[Snowflake] could not parse '{val}' as bool for option '{key}'"
                            ),
                            Status::InvalidArguments,
                        ));
                    }
                };
            }
            "ingest_geo_type" => {
                let val = ingest_string(key, value)?;
                match val.to_ascii_lowercase().as_str() {
                    "geography" | "geometry" => {
                        self.geo_type = val.to_ascii_lowercase();
                        self.geo_type_explicit = true;
                    }
                    "" => {
                        self.geo_type = "geography".into();
                        self.geo_type_explicit = false;
                    }
                    _ => {
                        return Err(Error::with_message_and_status(
                            format!(
                                "[Snowflake] invalid geo type '{val}': must be 'geography' or 'geometry'"
                            ),
                            Status::InvalidArguments,
                        ));
                    }
                }
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    pub(crate) fn get(&self, key: &str) -> Option<String> {
        Some(match key.strip_prefix(INGEST_OPTION_PREFIX)? {
            "ingest_writer_concurrency" => self.writer_concurrency.to_string(),
            "ingest_upload_concurrency" => self.upload_concurrency.to_string(),
            "ingest_copy_concurrency" => self.copy_concurrency.to_string(),
            "ingest_target_file_size" => self.target_file_size.to_string(),
            "ingest_compression_codec" => self.compression_codec.clone(),
            "ingest_compression_level" => self.compression_level.to_string(),
            "ingest_use_vectorized_scanner" => self.vectorized_scanner.to_string(),
            "ingest_geo_type" => self.geo_type.clone(),
            _ => return None,
        })
    }

    fn writer_properties(&self) -> Result<WriterProperties> {
        let level = self.compression_level;
        let compression = match self.compression_codec.as_str() {
            "UNCOMPRESSED" => Compression::UNCOMPRESSED,
            "SNAPPY" => Compression::SNAPPY,
            "LZ4_RAW" => Compression::LZ4_RAW,
            "GZIP" => Compression::GZIP(if level == -1 {
                GzipLevel::default()
            } else {
                GzipLevel::try_new(level.try_into().map_err(|_| invalid_level(level))?)
                    .map_err(|_| invalid_level(level))?
            }),
            "BROTLI" => Compression::BROTLI(if level == -1 {
                BrotliLevel::default()
            } else {
                BrotliLevel::try_new(level.try_into().map_err(|_| invalid_level(level))?)
                    .map_err(|_| invalid_level(level))?
            }),
            "ZSTD" => Compression::ZSTD(if level == -1 {
                ZstdLevel::default()
            } else {
                ZstdLevel::try_new(level.try_into().map_err(|_| invalid_level(level))?)
                    .map_err(|_| invalid_level(level))?
            }),
            _ => unreachable!("codec validated on assignment"),
        };
        Ok(WriterProperties::builder()
            .set_compression(compression)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_max_row_group_row_count(None)
            .build())
    }

    fn geo_type_for(&self, index: usize, field: &Field) -> Result<&str> {
        if self.geo_type_explicit {
            return Ok(&self.geo_type);
        }
        let (srid, edges) = geo_metadata(field);
        if edges == "spherical" {
            if srid == 4326 {
                return Ok("geography");
            }
            return Err(Error::with_message_and_status(
                format!(
                    "[snowflake] field #{} ({}) is a GeoArrow array with spherical edges but an SRID of {srid}; Snowflake GEOGRAPHY is always SRID 4326, so explicitly set {GEO_TYPE_OPTION} to choose whether to ingest this as GEOGRAPHY or GEOMETRY",
                    index + 1,
                    quote_identifier(field.name())
                ),
                Status::InvalidData,
            ));
        }
        Ok("geometry")
    }
}

fn invalid_option(key: &str, n: i64) -> Error {
    Error::with_message_and_status(
        format!("invalid value ('{n}') for option '{key}', must be >= 0"),
        Status::InvalidArguments,
    )
}

fn invalid_level(level: i64) -> Error {
    Error::with_message_and_status(
        format!("invalid Parquet compression level {level}"),
        Status::InvalidArguments,
    )
}

fn ingest_string<'a>(key: &str, value: &'a OptionValue) -> Result<&'a str> {
    match value {
        OptionValue::String(s) => Ok(s),
        _ => Err(Error::with_message_and_status(
            format!("option '{key}' must be a string"),
            Status::InvalidArguments,
        )),
    }
}

fn ingest_int(key: &str, value: &OptionValue) -> Result<i64> {
    match value {
        OptionValue::Int(n) => Ok(*n),
        OptionValue::String(s) => s.parse().map_err(|_| {
            Error::with_message_and_status(
                format!("[Snowflake] could not parse '{s}' as int for option '{key}'"),
                Status::InvalidArguments,
            )
        }),
        _ => Err(Error::with_message_and_status(
            format!("option '{key}' must be an int"),
            Status::InvalidArguments,
        )),
    }
}

fn parquet_error(err: impl std::fmt::Display) -> Error {
    Error::with_message_and_status(format!("Parquet ingest: {err}"), Status::InvalidData)
}

fn cancelled() -> Error {
    Error::with_message_and_status("ingest cancelled", Status::Cancelled)
}

fn join_error(err: tokio::task::JoinError) -> Error {
    Error::with_message_and_status(format!("ingest worker failed: {err}"), Status::Internal)
}

/// A single cancellable operation spans DDL, uploads, and all COPY tasks.
/// Taking the binding first ensures a failed ingest also consumes it.
pub(crate) fn execute_ingest(stmt: &mut Statement) -> Result<Option<i64>> {
    let bound = std::mem::replace(&mut stmt.bound, Bound::Empty);
    let (schema, batches): (SchemaRef, BatchIter) = match bound {
        Bound::Empty => {
            return Err(Error::with_message_and_status(
                "ingest requires bound data — call bind() or bind_stream() first",
                Status::InvalidState,
            ));
        }
        Bound::Batches(v) if v.is_empty() => {
            return Err(Error::with_message_and_status(
                "ingest requires bound data — call bind() or bind_stream() first",
                Status::InvalidState,
            ));
        }
        Bound::Batches(v) => (v[0].schema(), Box::new(v.into_iter().map(Ok))),
        Bound::Stream(reader) => (reader.schema(), Box::new(reader)),
    };
    let table = stmt.target_table.as_deref().ok_or_else(|| {
        Error::with_message_and_status("target_table not set", Status::InvalidState)
    })?;
    let qname = qualified_table_identifier(
        stmt.ingest_catalog.as_deref(),
        stmt.ingest_schema.as_deref(),
        table,
    );
    let operation = stmt.begin_operation();
    // A connection-level ADBC$BIND stage is shared by all statements on that
    // connection in Go. Give each ingest its own stage to avoid dropping or
    // consuming a concurrent statement's files.
    let stage = format!(
        "ADBC$BIND_{}_{}",
        std::process::id(),
        NEXT_STAGE.fetch_add(1, Ordering::Relaxed),
    );
    let copy_plan = build_copy_plan(&qname, &schema, &stmt.ingest_options, &stage)?;
    // Reject input Parquet cannot encode (e.g. unions) and invalid writer
    // settings before any DDL, so a failed create-mode ingest leaves no table.
    let normalized = normalize_schema(&schema)?;
    let props = stmt.ingest_options.writer_properties()?;
    initialize_target(stmt, &operation, &qname, &schema, &copy_plan.overrides)?;

    let stage_sql = create_stage_sql(&stage, stmt.ingest_options.vectorized_scanner);
    stmt.inner.execute_temporary_statement(
        Some(&operation),
        stmt.conn_handle,
        stage_sql,
        stmt.query_timeout_seconds,
    )?;
    let result = ingest_batches(
        IngestJob {
            inner: stmt.inner.clone(),
            conn: stmt.conn_handle,
            op: operation.clone(),
            stage: stage.clone(),
            copy_sql: copy_plan.sql,
            timeout: stmt.query_timeout_seconds,
            options: stmt.ingest_options.clone(),
            normalized,
            props,
        },
        batches,
        schema,
    );
    // Cleanup must work even after cancellation. A temporary stage also dies
    // when the session ends, should DROP itself fail.
    let cleanup = stmt.inner.execute_temporary_statement(
        None,
        stmt.conn_handle,
        format!("DROP STAGE IF EXISTS {stage}"),
        stmt.query_timeout_seconds,
    );
    match result {
        Err(err) => Err(err),
        Ok(rows) => {
            cleanup?;
            Ok(Some(rows))
        }
    }
}

fn create_stage_sql(stage: &str, vectorized: bool) -> String {
    let scanner = if vectorized {
        " USE_VECTORIZED_SCANNER = TRUE"
    } else {
        ""
    };
    format!(
        "CREATE TEMPORARY STAGE {stage} FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE BINARY_AS_TEXT = FALSE{scanner} REPLACE_INVALID_CHARACTERS = TRUE)"
    )
}

fn put_sql(stage: &str, filename: &str) -> String {
    // The stream-upload API uses this URI's basename as the destination name;
    // the source is the Vec<u8> passed to connection_upload_stream_chunk.
    format!(
        "PUT 'file:///tmp/placeholder/{filename}' @{stage} AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = PARQUET OVERWRITE = TRUE"
    )
}

fn plain_copy_sql(qname: &str, stage: &str) -> String {
    format!("COPY INTO {qname} FROM @{stage} MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE")
}

#[derive(Debug)]
struct CopyPlan {
    sql: String,
    overrides: Vec<Option<String>>,
}

fn build_copy_plan(
    qname: &str,
    schema: &Schema,
    options: &IngestOptions,
    stage: &str,
) -> Result<CopyPlan> {
    if !schema.fields().iter().any(|field| {
        matches!(
            field
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some("geoarrow.wkb" | "geoarrow.wkt")
        )
    }) {
        let overrides = schema
            .fields()
            .iter()
            .map(|field| {
                (field
                    .metadata()
                    .get("ARROW:extension:name")
                    .map(String::as_str)
                    == Some("arrow.uuid"))
                .then(|| "uuid".to_string())
            })
            .collect();
        return Ok(CopyPlan {
            sql: plain_copy_sql(qname, stage),
            overrides,
        });
    }
    let mut overrides = Vec::with_capacity(schema.fields().len());
    let mut transforms = Vec::with_capacity(schema.fields().len());
    for (index, field) in schema.fields().iter().enumerate() {
        let quoted = quote_identifier(field.name());
        let parquet_ref = format!("$1:{quoted}");
        let extension = field
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str);
        let (override_type, expression) = match extension {
            Some("arrow.uuid") => (
                Some("uuid".to_string()),
                format!(
                    "REGEXP_REPLACE(HEX_ENCODE({parquet_ref}::BINARY, 0), '(.{{8}})(.{{4}})(.{{4}})(.{{4}})(.{{12}})', '\\\\1-\\\\2-\\\\3-\\\\4-\\\\5') AS {quoted}"
                ),
            ),
            Some("geoarrow.wkb" | "geoarrow.wkt") => {
                let kind = options.geo_type_for(index, field)?;
                let cast = if extension == Some("geoarrow.wkb") {
                    "BINARY"
                } else {
                    "VARCHAR"
                };
                let func = if kind == "geography" {
                    if cast == "BINARY" {
                        "TO_GEOGRAPHY"
                    } else {
                        "TRY_TO_GEOGRAPHY"
                    }
                } else {
                    "TO_GEOMETRY"
                };
                let converted = format!(
                    "{func}({parquet_ref}::{cast}{})",
                    if kind == "geography" && cast == "BINARY" {
                        ", true"
                    } else {
                        ""
                    }
                );
                let (srid, _) = geo_metadata(field);
                let converted = if kind == "geometry" && srid != 0 {
                    format!("ST_SETSRID({converted}, {srid})")
                } else {
                    converted
                };
                (Some(kind.to_string()), format!("{converted} AS {quoted}"))
            }
            _ => (None, format!("{parquet_ref} AS {quoted}")),
        };
        overrides.push(override_type);
        transforms.push(expression);
    }
    let sql = format!(
        "COPY INTO {qname} FROM (SELECT {} FROM @{stage})",
        transforms.join(", "),
    );
    Ok(CopyPlan { sql, overrides })
}

fn geo_metadata(field: &Field) -> (i64, &str) {
    // The Arrow extension metadata is JSON. Invalid/unrecognized CRS defaults
    // to SRID 0, exactly as Go's extractSRIDFromMeta.
    let Some(raw) = field.metadata().get("ARROW:extension:metadata") else {
        return (0, "");
    };
    // Only the literal "spherical" affects the type decision; borrowed JSON
    // strings cannot outlive the parsed metadata.
    let Ok(meta) = serde_json::from_str::<serde_json::Value>(raw) else {
        return (0, "");
    };
    let edges = if meta["edges"] == "spherical" {
        "spherical"
    } else {
        ""
    };
    let crs = &meta["crs"];
    let srid = if let Some(name) = crs.as_str() {
        if name == "OGC:CRS84" {
            4326
        } else {
            name.strip_prefix("EPSG:")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0)
        }
    } else {
        let id = &crs["id"];
        let authority = id["authority"].as_str().unwrap_or("");
        if authority.eq_ignore_ascii_case("EPSG") {
            id["code"]
                .as_i64()
                .or_else(|| id["code"].as_str().and_then(|s| s.parse().ok()))
                .unwrap_or(0)
        } else if authority.eq_ignore_ascii_case("OGC") && id["code"] == "CRS84" {
            4326
        } else {
            0
        }
    };
    (srid, edges)
}

fn initialize_target(
    stmt: &Statement,
    op: &OperationCtx,
    qname: &str,
    schema: &Schema,
    overrides: &[Option<String>],
) -> Result<()> {
    let mode = stmt
        .ingest_mode
        .as_deref()
        .unwrap_or("adbc.ingest.mode.create");
    match mode {
        "adbc.ingest.mode.create" => stmt.inner.execute_temporary_statement(
            Some(op),
            stmt.conn_handle,
            build_create_sql(qname, schema, overrides, CreateMode::Create)?,
            stmt.query_timeout_seconds,
        ),
        // CREATE OR REPLACE is atomic in Snowflake: if the new table cannot
        // be created (for any reason, including server-side DDL rejection),
        // the existing table is left untouched.
        "adbc.ingest.mode.replace" => stmt.inner.execute_temporary_statement(
            Some(op),
            stmt.conn_handle,
            build_create_sql(qname, schema, overrides, CreateMode::Replace)?,
            stmt.query_timeout_seconds,
        ),
        "adbc.ingest.mode.create_append" => {
            stmt.inner.execute_temporary_statement(
                Some(op),
                stmt.conn_handle,
                build_create_sql(qname, schema, overrides, CreateMode::IfNotExists)?,
                stmt.query_timeout_seconds,
            )?;
            validate_target_schema(&stmt.inner, stmt.conn_handle, op, qname, schema)
        }
        "adbc.ingest.mode.append" => {
            validate_target_schema(&stmt.inner, stmt.conn_handle, op, qname, schema)
        }
        other => Err(Error::with_message_and_status(
            format!("unknown ingest mode: {other}"),
            Status::InvalidArguments,
        )),
    }
}

#[derive(Clone, Copy)]
enum CreateMode {
    Create,
    Replace,
    IfNotExists,
}

fn build_create_sql(
    qname: &str,
    schema: &Schema,
    overrides: &[Option<String>],
    mode: CreateMode,
) -> Result<String> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (field, override_type) in schema.fields().iter().zip(overrides) {
        let ty = match override_type {
            Some(ty) => ty.clone(),
            None => to_sf_ddl(field.data_type())?,
        };
        let not_null = if field.is_nullable() { "" } else { " NOT NULL" };
        columns.push(format!("{} {ty}{not_null}", quote_identifier(field.name())));
    }
    let create = match mode {
        CreateMode::Create => "CREATE TABLE",
        CreateMode::Replace => "CREATE OR REPLACE TABLE",
        CreateMode::IfNotExists => "CREATE TABLE IF NOT EXISTS",
    };
    Ok(format!("{create} {qname} ({})", columns.join(", ")))
}

fn to_sf_ddl(ty: &DataType) -> Result<String> {
    Ok(match ty {
        DataType::Dictionary(_, value) => return to_sf_ddl(value),
        DataType::RunEndEncoded(_, value) => return to_sf_ddl(value.data_type()),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => "integer".into(),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "double".into(),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
            // Snowflake NUMBER: precision 1..=38, scale 0..=37 and <= precision.
            if *p > 38 || *s < 0 || *s > 37 || i16::from(*s) > i16::from(*p) {
                return Err(Error::with_message_and_status(
                    format!(
                        "decimal({p},{s}) cannot be ingested: Snowflake NUMBER supports \
                         precision up to 38 and scale from 0 to 37 (not exceeding precision)"
                    ),
                    Status::NotImplemented,
                ));
            }
            format!("NUMERIC({p},{s})")
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "text".into(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "binary".into(),
        DataType::FixedSizeBinary(size) => format!("binary({size})"),
        DataType::Boolean => "boolean".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Time32(unit) | DataType::Time64(unit) => format!("time({})", precision(unit)),
        DataType::Timestamp(unit, zone) => {
            let kind = if zone.as_ref().is_some_and(|zone| !zone.is_empty()) {
                "ltz"
            } else {
                "ntz"
            };
            format!("timestamp_{kind}({})", precision(unit))
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _) => "array".into(),
        DataType::Struct(_) | DataType::Map(_, _) => "object".into(),
        DataType::Union(_, _) => "variant".into(),
        other => {
            return Err(Error::with_message_and_status(
                format!("unsupported ingest type: {other:?}"),
                Status::NotImplemented,
            ));
        }
    })
}

fn precision(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

fn column_names_equivalent(expected: &Schema, actual: &Schema) -> bool {
    expected.fields().len() == actual.fields().len() && {
        let mut matched = vec![false; actual.fields().len()];
        expected.fields().iter().all(|field| {
            actual.fields().iter().enumerate().any(|(i, candidate)| {
                if matched[i] || field.name().to_lowercase() != candidate.name().to_lowercase() {
                    return false;
                }
                matched[i] = true;
                true
            })
        })
    }
}

fn validate_target_schema(
    inner: &Inner,
    conn: Handle,
    op: &OperationCtx,
    qname: &str,
    expected: &Schema,
) -> Result<()> {
    let handle = inner
        .sf
        .statement_new(conn)
        .map_err(crate::error::api_error_to_adbc_error)?;
    let prepared = inner.prepare_statement(Some(op), handle, format!("SELECT * FROM {qname}"));
    let released = inner.sf.statement_release(handle);
    let schema = prepared?.reader.schema();
    released.map_err(crate::error::api_error_to_adbc_error)?;
    if column_names_equivalent(expected, &schema) {
        Ok(())
    } else {
        Err(Error::with_message_and_status(
            format!("[Snowflake] target table {qname} already exists with an incompatible schema"),
            Status::AlreadyExists,
        ))
    }
}

fn requires_normalization(ty: &DataType) -> Result<bool> {
    match ty {
        DataType::Float16 | DataType::Dictionary(_, _) | DataType::RunEndEncoded(_, _) => Ok(true),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => requires_normalization(field.data_type()),
        DataType::Struct(fields) => {
            let mut changed = false;
            for field in fields.iter() {
                changed |= requires_normalization(field.data_type())?;
            }
            Ok(changed)
        }
        DataType::Union(_, _) => Err(Error::with_message_and_status(
            "Parquet writing of Arrow union arrays is not supported",
            Status::NotImplemented,
        )),
        _ => Ok(false),
    }
}

fn normalized_type(ty: &DataType) -> Result<DataType> {
    Ok(match ty {
        DataType::Float16 => DataType::Float32,
        DataType::Dictionary(_, value) => return normalized_type(value),
        DataType::RunEndEncoded(_, value) => return normalized_type(value.data_type()),
        DataType::List(child) => DataType::List(Arc::new(normalized_field(child)?)),
        DataType::LargeList(child) => DataType::LargeList(Arc::new(normalized_field(child)?)),
        DataType::ListView(child) => DataType::ListView(Arc::new(normalized_field(child)?)),
        DataType::LargeListView(child) => {
            DataType::LargeListView(Arc::new(normalized_field(child)?))
        }
        DataType::FixedSizeList(child, size) => {
            DataType::FixedSizeList(Arc::new(normalized_field(child)?), *size)
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|f| normalized_field(f).map(Arc::new))
                .collect::<Result<Vec<_>>>()?
                .into(),
        ),
        DataType::Map(child, sorted) => DataType::Map(Arc::new(normalized_field(child)?), *sorted),
        // parquet-rs cannot encode Arrow unions. Return an error rather than
        // entering its ArrowSchemaConverter's unimplemented!() branch.
        DataType::Union(_, _) => {
            return Err(Error::with_message_and_status(
                "Parquet writing of Arrow union arrays is not supported",
                Status::NotImplemented,
            ));
        }
        other => other.clone(),
    })
}

fn normalized_field(field: &Field) -> Result<Field> {
    Ok(field
        .clone()
        .with_data_type(normalized_type(field.data_type())?))
}

fn normalize_schema(schema: &SchemaRef) -> Result<SchemaRef> {
    let mut changed = false;
    for field in schema.fields() {
        changed |= requires_normalization(field.data_type())?;
    }
    if !changed {
        return Ok(Arc::clone(schema));
    }
    Ok(Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| normalized_field(field))
            .collect::<Result<Vec<_>>>()?,
        schema.metadata().clone(),
    )))
}

fn normalize_batch(batch: RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    if Arc::ptr_eq(batch.schema_ref(), target) || batch.schema_ref() == target {
        return Ok(batch);
    }
    let columns = batch
        .columns()
        .iter()
        .zip(target.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                cast(column, field.data_type()).map_err(parquet_error)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::clone(target), columns).map_err(parquet_error)
}

// Workers consume batches via a bounded channel, and each writes whole batches
// until the estimated compressed file size passes the target. A target of 0
// produces one file per nonempty batch, matching Go's implementation.
fn write_files(
    receiver: Arc<Mutex<mpsc::Receiver<RecordBatch>>>,
    input_schema: SchemaRef,
    schema: SchemaRef,
    properties: WriterProperties,
    target_size: usize,
    next_file: Arc<AtomicUsize>,
    on_file: impl Fn(String, Vec<u8>) -> Result<()>,
) -> Result<()> {
    loop {
        // Do not create an empty Parquet writer for idle workers (or after the
        // last file). Creating one serializes schema metadata into the buffer.
        let first = loop {
            match receiver.lock().unwrap_or_else(|e| e.into_inner()).recv() {
                Ok(batch) if batch.num_rows() == 0 => continue,
                Ok(batch) => break batch,
                Err(_) => return Ok(()),
            }
        };
        let mut bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut bytes, Arc::clone(&schema), Some(properties.clone()))
                .map_err(parquet_error)?;
        let mut next = Some(first);
        loop {
            let batch = match next.take() {
                Some(batch) => Some(batch),
                None => receiver
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .recv()
                    .ok(),
            };
            match batch {
                Some(batch) if batch.num_rows() == 0 => continue,
                Some(batch) => {
                    if !Arc::ptr_eq(batch.schema_ref(), &input_schema)
                        && batch.schema_ref() != &input_schema
                    {
                        return Err(Error::with_message_and_status(
                            "bound stream batch schema does not match its declared schema",
                            Status::InvalidData,
                        ));
                    }
                    let batch = normalize_batch(batch, &schema)?;
                    writer.write(&batch).map_err(parquet_error)?;
                    if writer.in_progress_size() >= target_size {
                        break;
                    }
                }
                None => break,
            }
        }
        writer.close().map_err(parquet_error)?;
        let name = format!("{}.parquet", next_file.fetch_add(1, Ordering::Relaxed));
        on_file(name, bytes)?;
    }
}

/// Per-ingest state shared by the writer, upload, and COPY workers.
struct IngestJob {
    inner: Arc<Inner>,
    conn: Handle,
    op: Arc<OperationCtx>,
    stage: String,
    copy_sql: String,
    timeout: Option<u32>,
    options: IngestOptions,
    /// Parquet schema and writer settings, validated before any DDL runs.
    normalized: SchemaRef,
    props: WriterProperties,
}

fn ingest_batches(job: IngestJob, batches: BatchIter, schema: SchemaRef) -> Result<i64> {
    let IngestJob {
        inner,
        conn,
        op,
        stage,
        copy_sql,
        timeout,
        options,
        normalized,
        props,
    } = job;
    let uploads: Arc<Mutex<Vec<JoinHandle<Result<()>>>>> = Arc::new(Mutex::new(Vec::new()));
    let copies: Arc<Mutex<Vec<JoinHandle<Result<i64>>>>> = Arc::new(Mutex::new(Vec::new()));
    let outstanding = Arc::new(Mutex::new(HashSet::<String>::new()));
    let next_file = Arc::new(AtomicUsize::new(0));
    let upload_limit = Arc::new(Semaphore::new(options.upload_concurrency));
    let copy_limit = if options.copy_concurrency == 0 {
        None
    } else {
        Some(Arc::new(Semaphore::new(options.copy_concurrency)))
    };
    let runtime = inner.runtime.handle().clone();
    let stage = Arc::new(stage);
    let copy_sql = Arc::new(copy_sql);
    // Uploads coalesce COPY notifications (like Go's one-slot ready channel)
    // rather than queuing one background job per file.
    let (copy_ready, mut copy_notifications) = tokio::sync::mpsc::channel::<()>(1);
    let copy_dispatch = {
        let copies = Arc::clone(&copies);
        let outstanding = Arc::clone(&outstanding);
        let inner = Arc::clone(&inner);
        let op = Arc::clone(&op);
        let copy_sql = Arc::clone(&copy_sql);
        runtime.spawn(async move {
            while copy_notifications.recv().await.is_some() {
                let permit = match &copy_limit {
                    Some(limit) => Some(
                        Arc::clone(limit)
                            .acquire_owned()
                            .await
                            .map_err(|_| cancelled())?,
                    ),
                    None => None,
                };
                let inner = Arc::clone(&inner);
                let op = Arc::clone(&op);
                let outstanding = Arc::clone(&outstanding);
                let copy_sql = Arc::clone(&copy_sql);
                let task = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    execute_copy(&inner, conn, &op, copy_sql.as_str(), timeout, &outstanding)
                });
                copies.lock().unwrap_or_else(|e| e.into_inner()).push(task);
            }
            Ok::<_, Error>(())
        })
    };
    let (sender, receiver) = mpsc::sync_channel(options.writer_concurrency);
    let receiver = Arc::new(Mutex::new(receiver));
    let mut writers = Vec::with_capacity(options.writer_concurrency);
    for _ in 0..options.writer_concurrency {
        let receiver = Arc::clone(&receiver);
        let input_schema = Arc::clone(&schema);
        let normalized = Arc::clone(&normalized);
        let props = props.clone();
        let next_file = Arc::clone(&next_file);
        let uploads = Arc::clone(&uploads);
        let copy_ready = copy_ready.clone();
        let outstanding = Arc::clone(&outstanding);
        let upload_limit = Arc::clone(&upload_limit);
        let inner = Arc::clone(&inner);
        let op = Arc::clone(&op);
        let stage = Arc::clone(&stage);
        let worker_runtime = runtime.clone();
        writers.push(runtime.spawn_blocking(move || {
            write_files(
                receiver,
                input_schema,
                normalized,
                props,
                options.target_file_size,
                next_file,
                |filename, bytes| {
                    if op.token().is_cancelled() {
                        return Err(cancelled());
                    }
                    // Reserving an upload slot before dispatch also bounds the
                    // number of completed in-memory Parquet files awaiting PUT.
                    let permit = worker_runtime
                        .block_on(Arc::clone(&upload_limit).acquire_owned())
                        .map_err(|_| cancelled())?;
                    let inner = Arc::clone(&inner);
                    let op = Arc::clone(&op);
                    let outstanding = Arc::clone(&outstanding);
                    let copy_ready = copy_ready.clone();
                    let upload_sql = put_sql(&stage, &filename);
                    let spawned = worker_runtime.spawn(async move {
                        let _permit = permit;
                        upload_parquet(&inner, conn, &op, upload_sql, bytes).await?;
                        outstanding
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .insert(filename);
                        let _ = copy_ready.try_send(());
                        Ok(())
                    });
                    uploads
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(spawned);
                    Ok(())
                },
            )
        }));
    }

    let mut read_error = None;
    let mut batches = batches;
    loop {
        if op.token().is_cancelled() {
            read_error = Some(cancelled());
            break;
        }
        let batch = match catch_unwind(AssertUnwindSafe(|| batches.next())) {
            Ok(Some(batch)) => batch,
            Ok(None) => break,
            Err(payload) => {
                let message = payload
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| payload.downcast_ref::<&str>().copied())
                    .unwrap_or("unknown panic");
                read_error = Some(Error::with_message_and_status(
                    format!("failed to read record batch from bound stream: {message}"),
                    Status::InvalidArguments,
                ));
                break;
            }
        };
        match batch {
            Ok(batch) => {
                if sender.send(batch).is_err() {
                    // All readers exited; their join result has the actual
                    // writer error (or panic), which takes precedence here.
                    break;
                }
            }
            Err(error) => {
                read_error = Some(Error::with_message_and_status(
                    format!("failed to read record batch from bound stream: {error}"),
                    Status::InvalidArguments,
                ));
                break;
            }
        }
    }
    if read_error.is_some() {
        op.cancel();
    }
    drop(sender);
    let mut first_error = read_error;
    for worker in writers {
        match inner
            .runtime
            .block_on(worker)
            .map_err(join_error)
            .and_then(|r| r)
        {
            Ok(()) => {}
            Err(err) if first_error.is_none() => first_error = Some(err),
            Err(_) => {}
        }
    }
    let upload_handles = std::mem::take(&mut *uploads.lock().unwrap_or_else(|e| e.into_inner()));
    for upload in upload_handles {
        match inner
            .runtime
            .block_on(upload)
            .map_err(join_error)
            .and_then(|r| r)
        {
            Ok(()) => {}
            Err(err) if first_error.is_none() => first_error = Some(err),
            Err(_) => {}
        }
    }
    drop(copy_ready);
    match inner
        .runtime
        .block_on(copy_dispatch)
        .map_err(join_error)
        .and_then(|r| r)
    {
        Ok(()) => {}
        Err(err) if first_error.is_none() => first_error = Some(err),
        Err(_) => {}
    }
    let copy_handles = std::mem::take(&mut *copies.lock().unwrap_or_else(|e| e.into_inner()));
    let mut loaded = 0i64;
    for copy in copy_handles {
        match inner
            .runtime
            .block_on(copy)
            .map_err(join_error)
            .and_then(|r| r)
        {
            Ok(n) => {
                loaded = loaded.checked_add(n).ok_or_else(|| {
                    Error::with_message_and_status("ingest row count overflow", Status::Internal)
                })?
            }
            Err(err) if first_error.is_none() => first_error = Some(err),
            Err(_) => {}
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    if next_file.load(Ordering::Relaxed) == 0 {
        return Err(Error::with_message_and_status(
            "no records in stream to write",
            Status::InvalidArguments,
        ));
    }
    // A COPY may return before an uploaded file becomes visible in the stage;
    // retry any such files with Go's 100/200/400/800/1600 ms backoff.
    for attempt in 0..=5 {
        if outstanding
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
        {
            return Ok(loaded);
        }
        if attempt != 0 {
            std::thread::sleep(Duration::from_millis(100 * (1 << (attempt - 1))));
        }
        let n = execute_copy(&inner, conn, &op, copy_sql.as_str(), timeout, &outstanding)?;
        loaded = loaded.checked_add(n).ok_or_else(|| {
            Error::with_message_and_status("ingest row count overflow", Status::Internal)
        })?;
    }
    let remaining = outstanding.lock().unwrap_or_else(|e| e.into_inner()).len();
    Err(Error::with_message_and_status(
        format!("some files not loaded by COPY command, {remaining} files remain after 5 retries"),
        Status::IO,
    ))
}

async fn upload_parquet(
    inner: &Inner,
    conn: Handle,
    op: &OperationCtx,
    sql: String,
    bytes: Vec<u8>,
) -> Result<()> {
    let sf = &inner.sf;
    let handle = op
        .run(
            "ingest upload begin",
            sf.connection_upload_stream_begin(conn, sql),
        )
        .await
        .map_err(crate::error::api_error_to_adbc_error)?;
    let uploaded = async {
        op.run(
            "ingest upload chunk",
            sf.connection_upload_stream_chunk(handle, bytes),
        )
        .await?;
        op.run(
            "ingest upload finish",
            sf.connection_upload_stream_finish(handle),
        )
        .await
    }
    .await;
    match uploaded {
        Ok(info) => sf
            .result_set_release(info.handle)
            .map_err(crate::error::api_error_to_adbc_error),
        Err(error) => {
            // finish removes its handle first. abort's "not found" is harmless.
            let _ = sf.connection_upload_stream_abort(handle).await;
            Err(crate::error::api_error_to_adbc_error(error))
        }
    }
}

fn execute_copy(
    inner: &Inner,
    conn: Handle,
    op: &OperationCtx,
    sql: &str,
    timeout: Option<u32>,
    outstanding: &Mutex<HashSet<String>>,
) -> Result<i64> {
    let handle = inner
        .sf
        .statement_new(conn)
        .map_err(crate::error::api_error_to_adbc_error)?;
    let response =
        inner.execute_query_with_timeout(Some(op), handle, sql.to_owned(), None, timeout);
    let release = inner.sf.statement_release(handle);
    let result = response?;
    release.map_err(crate::error::api_error_to_adbc_error)?;
    count_copied_rows(result.reader, outstanding)
}

fn count_copied_rows(
    mut reader: Box<dyn RecordBatchReader + Send>,
    outstanding: &Mutex<HashSet<String>>,
) -> Result<i64> {
    let fields = reader.schema();
    let file_index = fields
        .fields()
        .iter()
        .position(|f| f.name().eq_ignore_ascii_case("file"));
    let rows_index = fields
        .fields()
        .iter()
        .position(|f| f.name().eq_ignore_ascii_case("rows_loaded"));
    let (Some(file_index), Some(rows_index)) = (file_index, rows_index) else {
        // Snowflake omits the `file` column when COPY sees no files.
        return Ok(0);
    };
    let mut loaded = 0i64;
    for batch in reader.by_ref() {
        let batch = batch.map_err(parquet_error)?;
        for row in 0..batch.num_rows() {
            let files = batch.column(file_index);
            if files.is_null(row) {
                continue;
            }
            let file = array_value_to_string(files, row).map_err(parquet_error)?;
            let filename = Path::new(&file)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or(&file);
            outstanding
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(filename);
            let rows = batch.column(rows_index);
            if !rows.is_null(row) {
                let count = array_value_to_string(rows, row).map_err(parquet_error)?;
                let count = count.parse::<i64>().map_err(parquet_error)?;
                loaded = loaded.checked_add(count).ok_or_else(|| {
                    Error::with_message_and_status("ingest row count overflow", Status::Internal)
                })?;
            }
        }
    }
    Ok(loaded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, DictionaryArray,
        FixedSizeBinaryArray, Float32Array, Int32Array, Int64Array, ListArray, StringArray,
        StringViewArray, Time64NanosecondArray, TimestampMicrosecondArray, types::Int8Type,
    };
    use arrow_schema::extension::Uuid;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn option(name: &str) -> String {
        format!("{INGEST_OPTION_PREFIX}{name}")
    }

    #[test]
    fn ingest_options_defaults_round_trip_and_zero_behavior() {
        let mut opts = IngestOptions::default();
        assert_eq!(
            opts.get(&option("ingest_writer_concurrency"))
                .unwrap()
                .parse::<usize>()
                .unwrap(),
            std::thread::available_parallelism().map_or(1, usize::from)
        );
        for (key, expected) in [
            ("ingest_upload_concurrency", "8"),
            ("ingest_copy_concurrency", "4"),
            ("ingest_target_file_size", "10485760"),
            ("ingest_compression_codec", "SNAPPY"),
            ("ingest_compression_level", "-1"),
            ("ingest_use_vectorized_scanner", "true"),
            ("ingest_geo_type", "geography"),
        ] {
            assert_eq!(opts.get(&option(key)).as_deref(), Some(expected), "{key}");
        }
        for (key, input, expected) in [
            ("ingest_writer_concurrency", 2, "2"),
            ("ingest_upload_concurrency", 3, "3"),
            ("ingest_copy_concurrency", 0, "0"),
            ("ingest_target_file_size", 0, "0"),
            ("ingest_compression_level", -4, "-4"),
        ] {
            assert!(opts.set(&option(key), &OptionValue::Int(input)).unwrap());
            assert_eq!(opts.get(&option(key)).as_deref(), Some(expected));
        }
        opts.set(&option("ingest_writer_concurrency"), &OptionValue::Int(0))
            .unwrap();
        opts.set(&option("ingest_upload_concurrency"), &OptionValue::Int(0))
            .unwrap();
        assert_eq!(
            opts.writer_concurrency,
            IngestOptions::default().writer_concurrency
        );
        assert_eq!(opts.upload_concurrency, 8);
        opts.set(
            &option("ingest_compression_level"),
            &OptionValue::Int(i64::MAX),
        )
        .unwrap();
        assert_eq!(
            opts.get(&option("ingest_compression_level")).as_deref(),
            Some("9223372036854775807")
        );
        opts.set(&option("ingest_compression_level"), &OptionValue::Int(-1))
            .unwrap();
        for codec in [
            "uncompressed",
            "SNAPPY",
            "Gzip",
            "brotli",
            "zstd",
            "lz4_raw",
        ] {
            opts.set(
                &option("ingest_compression_codec"),
                &OptionValue::String(codec.into()),
            )
            .unwrap();
            assert_eq!(
                opts.get(&option("ingest_compression_codec")).unwrap(),
                codec.to_ascii_uppercase()
            );
            opts.writer_properties().unwrap();
        }
        opts.set(
            &option("ingest_use_vectorized_scanner"),
            &OptionValue::String("0".into()),
        )
        .unwrap();
        assert_eq!(
            opts.get(&option("ingest_use_vectorized_scanner"))
                .as_deref(),
            Some("false")
        );
        opts.set(
            &option("ingest_geo_type"),
            &OptionValue::String("GEOMETRY".into()),
        )
        .unwrap();
        assert_eq!(
            opts.get(&option("ingest_geo_type")).as_deref(),
            Some("geometry")
        );
        assert!(opts.geo_type_explicit);
        opts.set(&option("ingest_geo_type"), &OptionValue::String("".into()))
            .unwrap();
        assert!(!opts.geo_type_explicit);
        assert_eq!(
            opts.get(&option("ingest_geo_type")).as_deref(),
            Some("geography")
        );
        assert!(!opts.set("unknown", &OptionValue::Int(1)).unwrap());
        assert_eq!(opts.get("unknown"), None);
    }

    #[test]
    fn ingest_options_reject_go_invalid_values() {
        let mut opts = IngestOptions::default();
        for key in [
            "ingest_writer_concurrency",
            "ingest_upload_concurrency",
            "ingest_copy_concurrency",
            "ingest_target_file_size",
        ] {
            let error = opts.set(&option(key), &OptionValue::Int(-1)).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            assert_eq!(
                error.message,
                format!(
                    "invalid value ('-1') for option '{}', must be >= 0",
                    option(key)
                )
            );
            let error = opts
                .set(&option(key), &OptionValue::String("oops".into()))
                .unwrap_err();
            assert!(error.message.contains("could not parse 'oops' as int"));
        }
        let error = opts
            .set(
                &option("ingest_compression_codec"),
                &OptionValue::String("lz4".into()),
            )
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(error.message.contains("invalid compression codec 'lz4'"));
        let error = opts
            .set(
                &option("ingest_use_vectorized_scanner"),
                &OptionValue::String("yes".into()),
            )
            .unwrap_err();
        assert!(error.message.contains("could not parse 'yes' as bool"));
        let error = opts
            .set(
                &option("ingest_geo_type"),
                &OptionValue::String("point".into()),
            )
            .unwrap_err();
        assert!(error.message.contains("invalid geo type 'point'"));
    }

    #[test]
    fn ddl_maps_arrow_types_and_rejects_unsupported_types() {
        for (ty, expected) in [
            (DataType::Int8, "integer"),
            (DataType::UInt64, "integer"),
            (DataType::Float16, "double"),
            (DataType::Decimal128(10, 2), "NUMERIC(10,2)"),
            (DataType::Utf8View, "text"),
            (DataType::BinaryView, "binary"),
            (DataType::FixedSizeBinary(16), "binary(16)"),
            (DataType::Boolean, "boolean"),
            (DataType::Date64, "date"),
            (DataType::Time64(TimeUnit::Nanosecond), "time(9)"),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                "timestamp_ntz(6)",
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("".into())),
                "timestamp_ntz(6)",
            ),
            (
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                "timestamp_ltz(3)",
            ),
            (
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                "array",
            ),
            (
                DataType::ListView(Arc::new(Field::new("item", DataType::Int32, true))),
                "array",
            ),
            (
                DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true))),
                "array",
            ),
            (
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 3),
                "array",
            ),
            (
                DataType::Struct(vec![Field::new("item", DataType::Utf8, true)].into()),
                "object",
            ),
            (
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::Int32, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                "object",
            ),
            (
                DataType::Union(
                    arrow_schema::UnionFields::try_new(
                        vec![0, 1],
                        vec![
                            Field::new("a", DataType::Int32, true),
                            Field::new("b", DataType::Utf8, true),
                        ],
                    )
                    .unwrap(),
                    arrow_schema::UnionMode::Dense,
                ),
                "variant",
            ),
            (
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                "text",
            ),
            (
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int32, false)),
                    Arc::new(Field::new("values", DataType::Int64, true)),
                ),
                "integer",
            ),
        ] {
            assert_eq!(to_sf_ddl(&ty).unwrap(), expected, "{ty:?}");
        }
        let union = DataType::Union(
            arrow_schema::UnionFields::try_new(
                vec![0],
                vec![Field::new("value", DataType::Int32, true)],
            )
            .unwrap(),
            arrow_schema::UnionMode::Dense,
        );
        let schema = Arc::new(Schema::new(vec![Field::new("choice", union, true)]));
        assert_eq!(
            normalize_schema(&schema).unwrap_err().status,
            Status::NotImplemented
        );
        assert_eq!(
            to_sf_ddl(&DataType::Duration(TimeUnit::Second))
                .unwrap_err()
                .status,
            Status::NotImplemented
        );
        assert_eq!(
            build_create_sql(
                "\"table\"",
                &Schema::new(vec![Field::new("odd\"name", DataType::Boolean, false)]),
                &[None],
                CreateMode::IfNotExists
            )
            .unwrap(),
            "CREATE TABLE IF NOT EXISTS \"table\" (\"odd\"\"name\" boolean NOT NULL)"
        );
        assert_eq!(
            build_create_sql(
                "\"table\"",
                &Schema::new(vec![Field::new("flag", DataType::Boolean, true)]),
                &[None],
                CreateMode::Replace
            )
            .unwrap(),
            "CREATE OR REPLACE TABLE \"table\" (\"flag\" boolean)"
        );
        assert_eq!(
            to_sf_ddl(&DataType::Decimal256(38, 37)).unwrap(),
            "NUMERIC(38,37)"
        );
        for unsupported in [
            DataType::Decimal256(39, 0),
            DataType::Decimal128(10, -2),
            DataType::Decimal128(38, 38),
            DataType::Decimal128(5, 6),
        ] {
            assert_eq!(
                to_sf_ddl(&unsupported).unwrap_err().status,
                Status::NotImplemented,
                "{unsupported}"
            );
        }
    }

    #[test]
    fn replace_mode_rejects_unsupported_ddl_before_touching_the_table() {
        // The statement's handles are invalid, so issuing any DDL first would
        // fail inside sf_core instead of reporting the unsupported type.
        let mut stmt = crate::statement::tests::make_stmt();
        stmt.ingest_mode = Some("adbc.ingest.mode.replace".into());
        let schema = Schema::new(vec![Field::new(
            "elapsed",
            DataType::Duration(TimeUnit::Second),
            true,
        )]);
        let op = OperationCtx::with_own_token();
        let error = initialize_target(&stmt, &op, "\"target\"", &schema, &[None]).unwrap_err();
        assert_eq!(error.status, Status::NotImplemented, "{error:?}");
    }

    #[test]
    fn schema_validation_is_case_insensitive_and_one_to_one() {
        let expected = Schema::new(vec![
            Field::new("First", DataType::Int64, false),
            Field::new("second", DataType::Utf8, true),
        ]);
        let reordered = Schema::new(vec![
            Field::new("SECOND", DataType::Boolean, false),
            Field::new("first", DataType::Binary, true),
        ]);
        assert!(column_names_equivalent(&expected, &reordered));
        assert!(!column_names_equivalent(
            &expected,
            &Schema::new(vec![Field::new("first", DataType::Int64, false)])
        ));
        assert!(!column_names_equivalent(
            &expected,
            &Schema::new(vec![
                Field::new("first", DataType::Int64, false),
                Field::new("FIRST", DataType::Utf8, true),
            ])
        ));
    }

    #[test]
    fn copy_result_sums_rows_loaded_and_tracks_successful_files() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("file", DataType::Utf8, true),
            Field::new("rows_loaded", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    "@stage/0.parquet",
                    "@stage/1.parquet",
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![3, 4])) as ArrayRef,
            ],
        )
        .unwrap();
        let reader = Box::new(arrow_array::RecordBatchIterator::new(
            std::iter::once(Ok(batch)),
            schema,
        ));
        let outstanding = Mutex::new(HashSet::from([
            "0.parquet".to_string(),
            "1.parquet".to_string(),
        ]));
        assert_eq!(count_copied_rows(reader, &outstanding).unwrap(), 7);
        assert!(outstanding.lock().unwrap().is_empty());
    }

    #[test]
    fn stage_put_copy_and_geo_sql() {
        assert_eq!(
            create_stage_sql("ADBC$BIND_12", true),
            "CREATE TEMPORARY STAGE ADBC$BIND_12 FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE BINARY_AS_TEXT = FALSE USE_VECTORIZED_SCANNER = TRUE REPLACE_INVALID_CHARACTERS = TRUE)"
        );
        assert!(!create_stage_sql("ADBC$BIND_12", false).contains("USE_VECTORIZED_SCANNER"));
        assert_eq!(
            put_sql("ADBC$BIND_12", "4.parquet"),
            "PUT 'file:///tmp/placeholder/4.parquet' @ADBC$BIND_12 AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = PARQUET OVERWRITE = TRUE"
        );
        assert_eq!(
            plain_copy_sql("\"DB\".\"table\"", "ADBC$BIND_12"),
            "COPY INTO \"DB\".\"table\" FROM @ADBC$BIND_12 MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
        );
        let uuid = Field::new("identifier", DataType::FixedSizeBinary(16), false)
            .with_extension_type(Uuid);
        let uuid_only = build_copy_plan(
            "\"dest\"",
            &Schema::new(vec![uuid.clone()]),
            &IngestOptions::default(),
            "ADBC$BIND_12",
        )
        .unwrap();
        assert_eq!(uuid_only.sql, plain_copy_sql("\"dest\"", "ADBC$BIND_12"));
        assert_eq!(uuid_only.overrides, vec![Some("uuid".into())]);
        let wkb = Field::new("loc", DataType::Binary, true).with_metadata(
            std::collections::HashMap::from([
                ("ARROW:extension:name".into(), "geoarrow.wkb".into()),
                (
                    "ARROW:extension:metadata".into(),
                    r#"{"crs":"EPSG:3857"}"#.into(),
                ),
            ]),
        );
        let schema = Schema::new(vec![uuid, wkb]);
        let plan = build_copy_plan(
            "\"dest\"",
            &schema,
            &IngestOptions::default(),
            "ADBC$BIND_12",
        )
        .unwrap();
        assert_eq!(
            plan.overrides,
            vec![Some("uuid".into()), Some("geometry".into())]
        );
        assert!(
            plan.sql
                .contains("REGEXP_REPLACE(HEX_ENCODE($1:\"identifier\"::BINARY, 0)")
        );
        assert!(
            plan.sql
                .contains("ST_SETSRID(TO_GEOMETRY($1:\"loc\"::BINARY), 3857) AS \"loc\"")
        );
        let wkt =
            Field::new("g", DataType::Utf8, true).with_metadata(std::collections::HashMap::from([
                ("ARROW:extension:name".into(), "geoarrow.wkt".into()),
                (
                    "ARROW:extension:metadata".into(),
                    r#"{"crs":"EPSG:4326","edges":"spherical"}"#.into(),
                ),
            ]));
        let plan = build_copy_plan(
            "\"dest\"",
            &Schema::new(vec![wkt.clone()]),
            &IngestOptions::default(),
            "ADBC$BIND_12",
        )
        .unwrap();
        assert_eq!(plan.overrides, vec![Some("geography".into())]);
        assert!(plan.sql.contains("TRY_TO_GEOGRAPHY($1:\"g\"::VARCHAR)"));
        let invalid = wkt.with_metadata(std::collections::HashMap::from([
            ("ARROW:extension:name".into(), "geoarrow.wkt".into()),
            (
                "ARROW:extension:metadata".into(),
                r#"{"crs":"EPSG:3857","edges":"spherical"}"#.into(),
            ),
        ]));
        assert_eq!(
            build_copy_plan(
                "\"dest\"",
                &Schema::new(vec![invalid]),
                &IngestOptions::default(),
                "ADBC$BIND_12"
            )
            .unwrap_err()
            .status,
            Status::InvalidData
        );
        let literal = build_copy_plan(
            "\"FROM @ADBC$BIND\"",
            &Schema::new(vec![Field::new("x", DataType::Int32, true)]),
            &IngestOptions::default(),
            "ADBC$BIND_12",
        )
        .unwrap();
        assert_eq!(
            literal.sql,
            "COPY INTO \"FROM @ADBC$BIND\" FROM @ADBC$BIND_12 MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
        );
    }

    fn encode(batches: Vec<RecordBatch>, target_size: usize) -> Vec<(String, Vec<u8>)> {
        let schema = batches[0].schema();
        let (sender, receiver) = mpsc::channel();
        for batch in batches {
            sender.send(batch).unwrap();
        }
        drop(sender);
        let files = Mutex::new(Vec::new());
        write_files(
            Arc::new(Mutex::new(receiver)),
            schema.clone(),
            normalize_schema(&schema).unwrap(),
            IngestOptions::default().writer_properties().unwrap(),
            target_size,
            Arc::new(AtomicUsize::new(0)),
            |name, bytes| {
                files.lock().unwrap().push((name, bytes));
                Ok(())
            },
        )
        .unwrap();
        files.into_inner().unwrap()
    }

    #[test]
    fn parquet_round_trips_values_and_normalizes_float16_dictionary_and_run_ends() {
        let half = cast(
            &(Arc::new(Float32Array::from(vec![Some(1.5), None])) as ArrayRef),
            &DataType::Float16,
        )
        .unwrap();
        let dict = DictionaryArray::<Int8Type>::try_new(
            arrow_array::Int8Array::from(vec![Some(0), None]),
            Arc::new(StringArray::from(vec!["hello"])) as ArrayRef,
        )
        .unwrap();
        let runs = arrow_array::RunArray::<arrow_array::types::Int32Type>::try_new(
            &Int32Array::from(vec![2]),
            &Int64Array::from(vec![7]),
        )
        .unwrap();
        let columns: Vec<(&str, ArrayRef)> = vec![
            ("half", half),
            ("dict", Arc::new(dict)),
            ("runs", Arc::new(runs)),
            (
                "time",
                Arc::new(Time64NanosecondArray::from(vec![Some(1), None])),
            ),
            (
                "timestamp",
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(1000), None]).with_timezone("UTC"),
                ),
            ),
            ("date", Arc::new(Date32Array::from(vec![Some(1), None]))),
            ("int", Arc::new(Int32Array::from(vec![Some(12), None]))),
            ("bool", Arc::new(BooleanArray::from(vec![Some(true), None]))),
            (
                "binary",
                Arc::new(BinaryArray::from(vec![Some(b"abc".as_slice()), None])),
            ),
            (
                "view",
                Arc::new(StringViewArray::from(vec![Some("hello world"), None])),
            ),
            (
                "list",
                Arc::new(ListArray::from_iter_primitive::<
                    arrow_array::types::Int32Type,
                    _,
                    _,
                >(vec![Some(vec![Some(2)]), None])),
            ),
            (
                "decimal",
                Arc::new(
                    Decimal128Array::from(vec![Some(123), None])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
            ),
        ];
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(
            schema,
            columns.into_iter().map(|(_, array)| array).collect(),
        )
        .unwrap();
        let files = encode(vec![batch], usize::MAX);
        assert_eq!(files.len(), 1);
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(
            files.into_iter().next().unwrap().1,
        ))
        .unwrap()
        .build()
        .unwrap();
        let result = reader.into_iter().next().unwrap().unwrap();
        assert_eq!(result.schema().field(0).data_type(), &DataType::Float32);
        assert_eq!(result.schema().field(1).data_type(), &DataType::Utf8);
        assert_eq!(result.schema().field(2).data_type(), &DataType::Int64);
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0),
            1.5
        );
        assert!(result.column(0).is_null(1));
        assert_eq!(
            result
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "hello"
        );
        assert_eq!(
            result
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(1),
            7
        );
        assert_eq!(
            result
                .column(4)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            1000
        );
        assert_eq!(
            result
                .column(11)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            123
        );
    }

    #[test]
    fn parquet_writes_remaining_scalar_and_nested_arrow_types() {
        use arrow_array::{
            BinaryViewArray, Date64Array, Decimal256Array, FixedSizeListArray, Float64Array,
            Int8Array, LargeBinaryArray, LargeListArray, LargeStringArray, StructArray,
            Time32MillisecondArray, TimestampNanosecondArray, UInt8Array, UInt64Array,
            builder::{Int32Builder, MapBuilder, StringBuilder},
        };
        use arrow_buffer::i256;

        let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        map.keys().append_value("key");
        map.values().append_value(9);
        map.append(true).unwrap();
        let nested = StructArray::from(vec![(
            Arc::new(Field::new("member", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(3)])) as ArrayRef,
        )]);
        let columns: Vec<(&str, ArrayRef)> = vec![
            ("i8", Arc::new(Int8Array::from(vec![8]))),
            ("u8", Arc::new(UInt8Array::from(vec![8]))),
            ("u64", Arc::new(UInt64Array::from(vec![8]))),
            ("f64", Arc::new(Float64Array::from(vec![8.25]))),
            ("date64", Arc::new(Date64Array::from(vec![86_400_000]))),
            ("time32", Arc::new(Time32MillisecondArray::from(vec![1200]))),
            (
                "timestamp_ntz",
                Arc::new(TimestampNanosecondArray::from(vec![42])),
            ),
            (
                "large_string",
                Arc::new(LargeStringArray::from(vec!["text"])),
            ),
            (
                "large_binary",
                Arc::new(LargeBinaryArray::from(vec![b"byte".as_slice()])),
            ),
            (
                "binary_view",
                Arc::new(BinaryViewArray::from(vec![b"abc".as_slice()])),
            ),
            (
                "fixed_list",
                Arc::new(FixedSizeListArray::from_iter_primitive::<
                    arrow_array::types::Int32Type,
                    _,
                    _,
                >(vec![Some(vec![Some(1), Some(2)])], 2)),
            ),
            (
                "large_list",
                Arc::new(LargeListArray::from_iter_primitive::<
                    arrow_array::types::Int32Type,
                    _,
                    _,
                >(vec![Some(vec![Some(4)])])),
            ),
            ("struct", Arc::new(nested)),
            ("map", Arc::new(map.finish())),
            (
                "decimal256",
                Arc::new(
                    Decimal256Array::from(vec![i256::from_i128(123)])
                        .with_precision_and_scale(39, 0)
                        .unwrap(),
                ),
            ),
        ];
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(
            schema,
            columns.into_iter().map(|(_, array)| array).collect(),
        )
        .unwrap();
        let file = encode(vec![batch], 1).remove(0).1;
        let result = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(file))
            .unwrap()
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            result
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            8
        );
        assert_eq!(
            result
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            8.25
        );
        assert_eq!(
            result
                .column(7)
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(0),
            "text"
        );
        assert_eq!(
            result
                .column(9)
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .unwrap()
                .value(0),
            b"abc"
        );
        assert_eq!(
            result
                .column(14)
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap()
                .value(0),
            i256::from_i128(123)
        );
        assert!(matches!(
            result.schema().field(13).data_type(),
            DataType::Map(_, _)
        ));
    }

    #[test]
    fn parquet_uuid_logical_type_and_file_size_split() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("uuid", DataType::FixedSizeBinary(16), true).with_extension_type(Uuid),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    vec![Some([1_u8; 16]), None].into_iter(),
                    16,
                )
                .unwrap(),
            ) as ArrayRef],
        )
        .unwrap();
        let files = encode(vec![batch.clone(), batch.clone(), batch], 0);
        assert_eq!(files.len(), 3);
        for (index, (name, data)) in files.into_iter().enumerate() {
            assert_eq!(name, format!("{index}.parquet"));
            let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(data)).unwrap();
            let column = reader.parquet_schema().column(0);
            let uuid = column.logical_type_ref();
            assert_eq!(uuid, Some(&parquet::basic::LogicalType::Uuid));
            let batch = reader.build().unwrap().next().unwrap().unwrap();
            assert_eq!(batch.num_rows(), 2);
        }
    }
}
