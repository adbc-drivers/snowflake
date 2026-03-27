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

// src/statement.rs
use std::sync::Arc;

use adbc_core::{

    Optionable, PartitionedResult,
    error::{Error, Result, Status},
    options::{OptionStatement, OptionValue},
};
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchReader};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use sf_core::apis::database_driver_v1::Handle;

use crate::driver::{Inner, TimestampPrecision};

pub struct Statement {
    pub(crate) inner: Arc<Inner>,
    pub(crate) stmt_handle: Handle,
    pub(crate) conn_handle: Handle,
    pub(crate) query: Option<String>,
    pub(crate) target_table: Option<String>,
    pub(crate) ingest_catalog: Option<String>,
    pub(crate) ingest_schema: Option<String>,
    pub(crate) ingest_mode: Option<String>,
    pub(crate) query_tag: Option<String>,
    pub(crate) use_high_precision: bool,
    pub(crate) timestamp_precision: TimestampPrecision,
    /// Parameter batches stored by bind() / bind_stream(). Each row is one execution.
    pub(crate) bound_batches: Vec<RecordBatch>,
}

impl Drop for Statement {
    fn drop(&mut self) {
        let _ = self.inner.sf.statement_release(self.stmt_handle);
    }
}

impl Optionable for Statement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key {
            OptionStatement::TargetTable => {
                if let OptionValue::String(s) = value {
                    self.query = None;
                    self.target_table = Some(s);
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        "target_table must be a string",
                        Status::InvalidArguments,
                    ))
                }
            }
            OptionStatement::IngestMode => {
                if let OptionValue::String(s) = value {
                    self.ingest_mode = Some(s);
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        "ingest_mode must be a string",
                        Status::InvalidArguments,
                    ))
                }
            }
            OptionStatement::Other(ref k)
                if k == "adbc.snowflake.sql.client_option.use_high_precision" =>
            {
                if let OptionValue::String(s) = value {
                    self.use_high_precision = s == "enabled" || s == "true";
                }
                Ok(())
            }
            OptionStatement::Other(ref k)
                if k == "adbc.snowflake.sql.client_option.max_timestamp_precision" =>
            {
                if let OptionValue::String(s) = value {
                    self.timestamp_precision = match s.as_str() {
                        "microseconds" => TimestampPrecision::Microseconds,
                        "nanoseconds_error_on_overflow" => {
                            TimestampPrecision::NanosecondsErrorOnOverflow
                        }
                        _ => TimestampPrecision::Nanoseconds,
                    };
                }
                Ok(())
            }
            OptionStatement::Temporary => {
                // Accepted silently; used to select CREATE TEMPORARY TABLE during ingest.
                Ok(())
            }
            OptionStatement::TargetCatalog => {
                if let OptionValue::String(s) = value {
                    self.ingest_catalog = Some(s);
                }
                Ok(())
            }
            OptionStatement::TargetDbSchema => {
                if let OptionValue::String(s) = value {
                    self.ingest_schema = Some(s);
                }
                Ok(())
            }
            OptionStatement::Other(ref k) if k == "adbc.snowflake.statement.query_tag" => {
                if let OptionValue::String(s) = value {
                    self.query_tag = Some(s);
                    Ok(())
                } else {
                    Err(Error::with_message_and_status(
                        "query_tag must be a string",
                        Status::InvalidArguments,
                    ))
                }
            }
            _ => Err(Error::with_message_and_status(
                format!("unknown statement option: {}", key.as_ref()),
                Status::NotFound,
            )),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionStatement::Other(ref k) if k == "adbc.snowflake.statement.query_tag" => {
                Ok(self.query_tag.clone().unwrap_or_default())
            }
            _ => Err(Error::with_message_and_status(
                format!("option not found: {}", key.as_ref()),
                Status::NotFound,
            )),
        }
    }

    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            "option not found",
            Status::NotFound,
        ))
    }

    fn get_option_int(&self, _key: Self::Option) -> Result<i64> {
        Err(Error::with_message_and_status(
            "option not found",
            Status::NotFound,
        ))
    }

    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        Err(Error::with_message_and_status(
            "option not found",
            Status::NotFound,
        ))
    }
}

impl Statement {
    /// Execute a parameterized query once per row of every bound batch.
    /// Parameter values are substituted directly as SQL literals — this avoids
    /// relying on sf_core's JSON binding path and works with all Snowflake
    /// server versions without session configuration.
    fn execute_bound(&self, query: String) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        let mut result_schema: Option<Arc<Schema>> = None;

        for bound_batch in &self.bound_batches {
            for row_idx in 0..bound_batch.num_rows() {
                let bound_sql = substitute_params(&query, bound_batch, row_idx)?;

                let result = self
                    .inner
                    .runtime
                    .block_on(async {
                        self.inner
                            .sf
                            .statement_set_sql_query(self.stmt_handle, bound_sql)
                            .await?;
                        self.inner
                            .sf
                            .statement_execute_query(self.stmt_handle, None)
                            .await
                    })
                    .map_err(crate::error::api_error_to_adbc_error)?;

                // Safety: same as execute().
                let raw = Box::into_raw(result.stream)
                    as *mut arrow_array::ffi_stream::FFI_ArrowArrayStream;
                let reader =
                    unsafe { arrow_array::ffi_stream::ArrowArrayStreamReader::from_raw(raw) }
                        .map_err(|e| Error::with_message_and_status(e.to_string(), Status::IO))?;

                if result_schema.is_none() {
                    result_schema = Some(reader.schema());
                }
                for batch in reader {
                    let batch = batch
                        .map_err(|e| Error::with_message_and_status(e.to_string(), Status::IO))?;
                    all_batches.push(batch);
                }
            }
        }

         let schema = result_schema.unwrap_or_else(|| Arc::new(Schema::empty()));
         Ok(Box::new(ConvertingReader::new(
             ConcatReader {
                 batches: all_batches.into_iter(),
                 schema,
             },
             self.use_high_precision,
             self.timestamp_precision.time_unit(),
         )))
    }

    fn apply_query_tag(&self) -> Result<()> {
        if let Some(ref tag) = self.query_tag {
            let escaped = tag.replace('\'', "''");
            let set_sql = format!("ALTER SESSION SET QUERY_TAG = '{escaped}'");
            let tmp_handle = self
                .inner
                .sf
                .statement_new(self.conn_handle)
                .map_err(crate::error::api_error_to_adbc_error)?;
            let set_result = self.inner.runtime.block_on(async {
                self.inner
                    .sf
                    .statement_set_sql_query(tmp_handle, set_sql)
                    .await?;
                self.inner
                    .sf
                    .statement_execute_query(tmp_handle, None)
                    .await
            });
            let _ = self.inner.sf.statement_release(tmp_handle);
            set_result.map_err(crate::error::api_error_to_adbc_error)?;
        }
        Ok(())
    }
}

impl adbc_core::Statement for Statement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        self.bound_batches = vec![batch];
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        let mut batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| Error::with_message_and_status(e.to_string(), Status::IO))?;
            batches.push(batch);
        }
        self.bound_batches = batches;
        Ok(())
    }

    #[allow(refining_impl_trait)]
    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        if self.target_table.is_some() {
            // Ingest via execute() — run the ingest and return an empty reader.
            crate::ingest::execute_ingest(self)?;
            let batch =
                arrow_array::RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty()));
            return Ok(Box::new(crate::connection::SingleBatchReader::new(batch)));
        }
        let query = self.query.clone().ok_or_else(|| {
            Error::with_message_and_status("cannot execute without a query", Status::InvalidState)
        })?;

        self.apply_query_tag()?;

        // If parameters are bound, execute once per row and concatenate results.
        if !self.bound_batches.is_empty() {
            return self.execute_bound(query);
        }

        let result = self
            .inner
            .runtime
            .block_on(async {
                self.inner
                    .sf
                    .statement_set_sql_query(self.stmt_handle, query)
                    .await?;
                self.inner
                    .sf
                    .statement_execute_query(self.stmt_handle, None)
                    .await
            })
            .map_err(crate::error::api_error_to_adbc_error)?;

        // Safety: result.stream is a valid FFI stream from sf_core. Ownership is transferred
        // to ArrowArrayStreamReader. The C ABI layout is stable per the Arrow C Data Interface.
        let raw =
            Box::into_raw(result.stream) as *mut arrow_array::ffi_stream::FFI_ArrowArrayStream;
         let reader = unsafe { arrow_array::ffi_stream::ArrowArrayStreamReader::from_raw(raw) }
             .map_err(|e| Error::with_message_and_status(e.to_string(), Status::IO))?;
         Ok(Box::new(ConvertingReader::new(reader, self.use_high_precision, self.timestamp_precision.time_unit())))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        if self.target_table.is_some() {
            return crate::ingest::execute_ingest(self);
        }
        let query = self.query.clone().ok_or_else(|| {
            Error::with_message_and_status("cannot execute without a query", Status::InvalidState)
        })?;

        self.apply_query_tag()?;

        // Parameterised DML: execute once per bound row and sum row counts.
        if !self.bound_batches.is_empty() {
            let mut total: i64 = 0;
            for bound_batch in &self.bound_batches {
                for row_idx in 0..bound_batch.num_rows() {
                    let sql = substitute_params(&query, bound_batch, row_idx)?;
                    let result = self
                        .inner
                        .runtime
                        .block_on(async {
                            self.inner
                                .sf
                                .statement_set_sql_query(self.stmt_handle, sql)
                                .await?;
                            self.inner
                                .sf
                                .statement_execute_query(self.stmt_handle, None)
                                .await
                        })
                        .map_err(crate::error::api_error_to_adbc_error)?;
                    // Drain the stream via ArrowArrayStreamReader so sf_core's
                    // release callback fires before the handle is reused.
                    let raw = Box::into_raw(result.stream)
                        as *mut arrow_array::ffi_stream::FFI_ArrowArrayStream;
                    if let Ok(reader) =
                        unsafe { arrow_array::ffi_stream::ArrowArrayStreamReader::from_raw(raw) }
                    {
                        for _ in reader {} // consume all batches to trigger release
                    }
                    total += result.rows_affected.unwrap_or(0);
                }
            }
            return Ok(if is_ddl(&query) { None } else { Some(total) });
        }

        let result = self
            .inner
            .runtime
            .block_on(async {
                self.inner
                    .sf
                    .statement_set_sql_query(self.stmt_handle, query)
                    .await?;
                self.inner
                    .sf
                    .statement_execute_query(self.stmt_handle, None)
                    .await
            })
            .map_err(crate::error::api_error_to_adbc_error)?;

        // DDL statements (CREATE, DROP, ALTER, TRUNCATE) return a non-meaningful row
        // count from Snowflake (typically 1 for "success"). Per the ADBC convention,
        // return None (-1 in Python) for DDL so callers can distinguish it from DML.
        let rows = if is_ddl(self.query.as_deref().unwrap_or("")) {
            None
        } else {
            result.rows_affected
        };
        Ok(rows)
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        let query = self.query.clone().ok_or_else(|| {
            Error::with_message_and_status("cannot execute without a query", Status::InvalidState)
        })?;

        self.apply_query_tag()?;

        let result = self
            .inner
            .runtime
            .block_on(async {
                self.inner
                    .sf
                    .statement_set_sql_query(self.stmt_handle, query)
                    .await?;
                self.inner
                    .sf
                    .statement_execute_query(self.stmt_handle, None)
                    .await
            })
            .map_err(crate::error::api_error_to_adbc_error)?;

        // Safety: result.stream is a valid FFI stream from sf_core. Ownership is transferred
        // to ArrowArrayStreamReader. The C ABI layout is stable per the Arrow C Data Interface.
        let raw =
            Box::into_raw(result.stream) as *mut arrow_array::ffi_stream::FFI_ArrowArrayStream;
        let reader = unsafe { arrow_array::ffi_stream::ArrowArrayStreamReader::from_raw(raw) }
            .map_err(|e| Error::with_message_and_status(e.to_string(), Status::IO))?;
         // .schema() calls get_schema on the FFI stream without consuming any record batches.
         // Dropping the reader invokes the stream's release callback.
         Ok(adjust_schema(&reader.schema(), self.use_high_precision, self.timestamp_precision.time_unit()).as_ref().clone())
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Err(crate::error::not_implemented("execute_partitions"))
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        Err(crate::error::not_implemented("get_parameter_schema"))
    }

    fn prepare(&mut self) -> Result<()> {
        if self.query.is_none() {
            return Err(Error::with_message_and_status(
                "cannot prepare statement with no query",
                Status::InvalidState,
            ));
        }
        Ok(()) // No-op: Snowflake has no server-side prepare
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.query = Some(query.as_ref().to_string());
        self.target_table = None;
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(crate::error::not_implemented(
            "Snowflake does not support Substrait plans",
        ))
    }

    fn cancel(&mut self) -> Result<()> {
        Err(crate::error::not_implemented("cancel"))
    }
}

// ── ConcatReader: chains multiple RecordBatches into a single reader ──────────

struct ConcatReader {
    batches: std::vec::IntoIter<RecordBatch>,
    schema: Arc<Schema>,
}

impl Iterator for ConcatReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.batches.next().map(Ok)
    }
}

impl RecordBatchReader for ConcatReader {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

// ── Schema adjustment and type conversions ────────────────────────────────

fn scale_to_time_unit(scale: u32) -> TimeUnit {
    match scale / 3 {
        0 => TimeUnit::Second,
        1 => TimeUnit::Millisecond,
        2 => TimeUnit::Microsecond,
        _ => TimeUnit::Nanosecond,
    }
}

fn timestamp_target_unit(scale: i64, ts_unit: TimeUnit) -> TimeUnit {
    let natural = scale_to_time_unit(scale as u32);
    let natural_rank = time_unit_rank(natural);
    let max_rank = time_unit_rank(ts_unit);
    if natural_rank <= max_rank {
        natural
    } else {
        ts_unit
    }
}

fn time_unit_rank(unit: TimeUnit) -> u32 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

pub(crate) fn adjust_schema(
    schema: &Schema,
    use_high_precision: bool,
    ts_unit: TimeUnit,
) -> Arc<Schema> {
    let adjusted_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            let target = compute_target_type(f, use_high_precision, ts_unit);
            Field::new(f.name(), target, f.is_nullable())
        })
        .collect();
    Arc::new(Schema::new_with_metadata(
        adjusted_fields,
        schema.metadata().clone(),
    ))
}

fn compute_target_type(field: &Field, use_high_precision: bool, ts_unit: TimeUnit) -> DataType {
    let logical_type = field
        .metadata()
        .get("logicalType")
        .map(|s| s.as_str())
        .unwrap_or("");
    let scale: i64 = field
        .metadata()
        .get("scale")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    match logical_type {
        "FIXED" => {
            if scale == 0 || use_high_precision {
                DataType::Int64
            } else {
                DataType::Float64
            }
        }
        "TIME" => {
            let unit = scale_to_time_unit(scale as u32);
            if scale < 6 {
                DataType::Time32(unit)
            } else {
                DataType::Time64(unit)
            }
        }
        "TIMESTAMP_NTZ" => {
            let unit = timestamp_target_unit(scale, ts_unit);
            DataType::Timestamp(unit, None)
        }
        "REAL" => DataType::Float64,
        "TIMESTAMP_LTZ" => {
            let unit = timestamp_target_unit(scale, ts_unit);
            DataType::Timestamp(unit, Some(Arc::from("UTC")))
        }
        "TIMESTAMP_TZ" => {
            let unit = timestamp_target_unit(scale, ts_unit);
            DataType::Timestamp(unit, Some(Arc::from("UTC")))
        }
        _ => match field.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 => DataType::Int64,
            other => other.clone(),
        },
    }
}

#[cfg(test)]
pub(crate) fn adjust_schema_integers(schema: &Schema) -> Arc<Schema> {
    let adjusted_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 => {
                Field::new(f.name(), DataType::Int64, f.is_nullable())
            }
            _ => f.as_ref().clone(),
        })
        .collect();
    Arc::new(Schema::new_with_metadata(
        adjusted_fields,
        schema.metadata().clone(),
    ))
}

pub(crate) struct ConvertingReader<R: RecordBatchReader> {
    inner: R,
    schema: Arc<Schema>,
    use_high_precision: bool,
    ts_unit: TimeUnit,
    logical_types: Vec<String>,
    scales: Vec<i64>,
}

impl<R: RecordBatchReader> ConvertingReader<R> {
    pub(crate) fn new(inner: R, use_high_precision: bool, ts_unit: TimeUnit) -> Self {
        let orig_schema = inner.schema();
        let logical_types: Vec<String> = orig_schema
            .fields()
            .iter()
            .map(|f| {
                f.metadata()
                    .get("logicalType")
                    .cloned()
                    .unwrap_or_default()
            })
            .collect();
        let scales: Vec<i64> = orig_schema
            .fields()
            .iter()
            .map(|f| {
                f.metadata()
                    .get("scale")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0)
            })
            .collect();
        let schema = adjust_schema(&orig_schema, use_high_precision, ts_unit);
        Self {
            inner,
            schema,
            use_high_precision,
            ts_unit,
            logical_types,
            scales,
        }
    }

    fn convert_column(
        col: &ArrayRef,
        logical_type: &str,
        scale: i64,
        target_type: &DataType,
        use_high_precision: bool,
        ts_unit: TimeUnit,
        check_overflow: bool,
    ) -> std::result::Result<ArrayRef, arrow_schema::ArrowError> {
        match logical_type {
            "FIXED" => {
                if scale == 0 {
                    match col.data_type() {
                        DataType::Int64 => Ok(col.clone()),
                        _ => arrow_cast::cast(col.as_ref(), &DataType::Int64),
                    }
                } else if use_high_precision {
                    Ok(col.clone())
                } else {
                    // Cast to Float64, then divide by 10^scale to restore decimal value
                    let casted = arrow_cast::cast(col.as_ref(), &DataType::Float64)?;
                    let divisor = 10f64.powi(scale as i32);
                    let float_arr = casted
                        .as_any()
                        .downcast_ref::<arrow_array::Float64Array>()
                        .ok_or_else(|| {
                            arrow_schema::ArrowError::CastError(
                                "expected Float64Array after cast".into(),
                            )
                        })?;
                    let divided: arrow_array::Float64Array =
                        float_arr.iter().map(|v| v.map(|x| x / divisor)).collect();
                    Ok(Arc::new(divided) as ArrayRef)
                }
            }
            "TIME" => arrow_cast::cast(col.as_ref(), target_type),
            "REAL" => arrow_cast::cast(col.as_ref(), &DataType::Float64),
            "TIMESTAMP_NTZ" => convert_timestamp_ntz(col, scale, ts_unit, check_overflow, None),
            "TIMESTAMP_LTZ" => convert_timestamp_ntz(
                col,
                scale,
                ts_unit,
                check_overflow,
                Some(Arc::from("UTC")),
            ),
            "TIMESTAMP_TZ" => convert_timestamp_tz(col, scale, ts_unit, check_overflow),
            _ => match col.data_type() {
                DataType::Int8 | DataType::Int16 | DataType::Int32 => {
                    arrow_cast::cast(col.as_ref(), &DataType::Int64)
                }
                _ => Ok(col.clone()),
            },
        }
    }
}

fn convert_timestamp_ntz(
    col: &ArrayRef,
    scale: i64,
    ts_unit: TimeUnit,
    check_overflow: bool,
    tz_str: Option<Arc<str>>,
) -> std::result::Result<ArrayRef, arrow_schema::ArrowError> {
    let unit = timestamp_target_unit(scale, ts_unit);
    let target = DataType::Timestamp(unit, tz_str);

    match col.data_type() {
        DataType::Int64 => arrow_cast::cast(col.as_ref(), &target),
        DataType::Struct(_) => {
            use arrow_array::{Int32Array, Int64Array, StructArray};
            let struct_arr = col
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| arrow_schema::ArrowError::CastError("expected StructArray".into()))?;
            let epoch = struct_arr
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    arrow_schema::ArrowError::CastError("expected Int64 epoch".into())
                })?;
            let fraction = struct_arr
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| {
                    arrow_schema::ArrowError::CastError("expected Int32 fraction".into())
                })?;

            build_timestamp_from_epoch_fraction(epoch, fraction, struct_arr, scale, unit, check_overflow, target)
        }
        _ => arrow_cast::cast(col.as_ref(), &target),
    }
}

fn convert_timestamp_tz(
    col: &ArrayRef,
    scale: i64,
    ts_unit: TimeUnit,
    check_overflow: bool,
) -> std::result::Result<ArrayRef, arrow_schema::ArrowError> {
    use arrow_array::{Int32Array, Int64Array, StructArray};

    let unit = timestamp_target_unit(scale, ts_unit);
    let target = DataType::Timestamp(unit, Some(Arc::from("UTC")));

    let struct_arr = col
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| arrow_schema::ArrowError::CastError("expected StructArray for TIMESTAMP_TZ".into()))?;

    let num_fields = struct_arr.num_columns();
    let epoch = struct_arr
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| arrow_schema::ArrowError::CastError("expected Int64 epoch".into()))?;

    if num_fields == 2 {
        let tzoffset = struct_arr
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| arrow_schema::ArrowError::CastError("expected Int32 timezone".into()))?;

        build_timestamp_tz_2field(epoch, tzoffset, struct_arr, scale, unit, check_overflow, target)
    } else {
        let fraction = struct_arr
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| arrow_schema::ArrowError::CastError("expected Int32 fraction".into()))?;
        let tzoffset = struct_arr
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| arrow_schema::ArrowError::CastError("expected Int32 timezone".into()))?;

        build_timestamp_tz_3field(epoch, fraction, tzoffset, struct_arr, scale, unit, check_overflow, target)
    }
}

fn ns_to_unit(ns: i128, unit: TimeUnit) -> i64 {
    let divisor: i128 = match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };
    (ns / divisor) as i64
}

fn check_ns_overflow(ns: i128) -> std::result::Result<(), arrow_schema::ArrowError> {
    if ns > i64::MAX as i128 || ns < i64::MIN as i128 {
        Err(arrow_schema::ArrowError::CastError(format!(
            "timestamp value {ns} nanoseconds overflows i64"
        )))
    } else {
        Ok(())
    }
}

fn build_timestamp_from_epoch_fraction(
    epoch: &arrow_array::Int64Array,
    fraction: &arrow_array::Int32Array,
    struct_arr: &arrow_array::StructArray,
    scale: i64,
    unit: TimeUnit,
    check_overflow: bool,
    target: DataType,
) -> std::result::Result<ArrayRef, arrow_schema::ArrowError> {
    use arrow_array::builder::PrimitiveBuilder;
    use arrow_array::types::TimestampNanosecondType;

    let len = epoch.len();
    let frac_to_ns: i128 = if scale <= 9 {
        10i128.pow((9 - scale) as u32)
    } else {
        1
    };

    let mut builder = PrimitiveBuilder::<TimestampNanosecondType>::with_capacity(len);
    for i in 0..len {
        if struct_arr.is_null(i) {
            builder.append_null();
        } else {
            let ns: i128 = if epoch.value(i) >= 0 {
                epoch.value(i) as i128 * 1_000_000_000
                    + fraction.value(i) as i128 * frac_to_ns
            } else {
                epoch.value(i) as i128 * 1_000_000_000
                    - fraction.value(i) as i128 * frac_to_ns
            };
            if check_overflow {
                check_ns_overflow(ns)?;
            }
            let val = ns_to_unit(ns, unit);
            builder.append_value(val);
        }
    }
    let ns_arr = builder.finish();
    let intermediate: Arc<dyn Array> = Arc::new(ns_arr);
    arrow_cast::cast(intermediate.as_ref(), &target)
}

fn build_timestamp_tz_2field(
    epoch: &arrow_array::Int64Array,
    tzoffset: &arrow_array::Int32Array,
    struct_arr: &arrow_array::StructArray,
    scale: i64,
    unit: TimeUnit,
    check_overflow: bool,
    target: DataType,
) -> std::result::Result<ArrayRef, arrow_schema::ArrowError> {
    use arrow_array::builder::PrimitiveBuilder;
    use arrow_array::types::TimestampNanosecondType;

    let len = epoch.len();
    let mut builder = PrimitiveBuilder::<TimestampNanosecondType>::with_capacity(len);

    for i in 0..len {
        if struct_arr.is_null(i) {
            builder.append_null();
        } else {
            let tz_offset_minutes: i128 = (tzoffset.value(i) as i128) - 1440;
            let tz_offset_ns: i128 = tz_offset_minutes * 60 * 1_000_000_000;

            let epoch_ns: i128 = match scale {
                0..=2 => epoch.value(i) as i128 * 1_000_000_000,
                3..=5 => epoch.value(i) as i128 * 1_000_000,
                6..=8 => epoch.value(i) as i128 * 1_000,
                9 => epoch.value(i) as i128,
                _ => epoch.value(i) as i128 * 1_000_000_000,
            };

            let utc_ns = epoch_ns - tz_offset_ns;
            if check_overflow {
                check_ns_overflow(utc_ns)?;
            }
            builder.append_value(ns_to_unit(utc_ns, unit));
        }
    }
    let ns_arr = builder.finish();
    let intermediate: Arc<dyn Array> = Arc::new(ns_arr);
    arrow_cast::cast(intermediate.as_ref(), &target)
}

#[allow(clippy::too_many_arguments)]
fn build_timestamp_tz_3field(
    epoch: &arrow_array::Int64Array,
    fraction: &arrow_array::Int32Array,
    tzoffset: &arrow_array::Int32Array,
    struct_arr: &arrow_array::StructArray,
    scale: i64,
    unit: TimeUnit,
    check_overflow: bool,
    target: DataType,
) -> std::result::Result<ArrayRef, arrow_schema::ArrowError> {
    use arrow_array::builder::PrimitiveBuilder;
    use arrow_array::types::TimestampNanosecondType;

    let len = epoch.len();
    let frac_to_ns: i128 = if scale <= 9 {
        10i128.pow((9 - scale) as u32)
    } else {
        1
    };
    let mut builder = PrimitiveBuilder::<TimestampNanosecondType>::with_capacity(len);

    for i in 0..len {
        if struct_arr.is_null(i) {
            builder.append_null();
        } else {
            let tz_offset_minutes: i128 = (tzoffset.value(i) as i128) - 1440;
            let tz_offset_ns: i128 = tz_offset_minutes * 60 * 1_000_000_000;

            let epoch_ns: i128 = if epoch.value(i) >= 0 {
                epoch.value(i) as i128 * 1_000_000_000
                    + fraction.value(i) as i128 * frac_to_ns
            } else {
                epoch.value(i) as i128 * 1_000_000_000
                    - fraction.value(i) as i128 * frac_to_ns
            };

            let utc_ns = epoch_ns - tz_offset_ns;
            if check_overflow {
                check_ns_overflow(utc_ns)?;
            }
            builder.append_value(ns_to_unit(utc_ns, unit));
        }
    }
    let ns_arr = builder.finish();
    let intermediate: Arc<dyn Array> = Arc::new(ns_arr);
    arrow_cast::cast(intermediate.as_ref(), &target)
}

impl<R: RecordBatchReader> Iterator for ConvertingReader<R> {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = match self.inner.next()? {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };

        let check_overflow = false;

        let adjusted_columns: std::result::Result<Vec<ArrayRef>, arrow_schema::ArrowError> = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let logical_type = &self.logical_types[i];
                let scale = self.scales[i];
                let target_type = self.schema.field(i).data_type();
                ConvertingReader::<R>::convert_column(
                    col,
                    logical_type,
                    scale,
                    target_type,
                    self.use_high_precision,
                    self.ts_unit,
                    check_overflow,
                )
            })
            .collect();

        Some(RecordBatch::try_new(
            self.schema.clone(),
            match adjusted_columns {
                Ok(cols) => cols,
                Err(e) => return Some(Err(e)),
            },
        ))
    }
}

impl<R: RecordBatchReader> RecordBatchReader for ConvertingReader<R> {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

// ── Parameter substitution ────────────────────────────────────────────────────

/// Replaces each `?` placeholder in `query` with the SQL literal value of the
/// corresponding bound column at `row_idx`.
///
/// Skips `?` inside SQL string literals (`'…'`), line comments (`--…`), and
/// block comments (`/*…*/`) so only true parameter markers are substituted.
/// Returns `InvalidArguments` if there are more `?` markers than bound columns.
fn substitute_params(query: &str, batch: &RecordBatch, row_idx: usize) -> Result<String> {
    let mut result = String::with_capacity(query.len() * 2);
    let mut param_idx = 0usize;
    let mut chars = query.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            // SQL string literal — copy verbatim; '' is an escaped quote (stay in string)
            '\'' => {
                result.push('\'');
                loop {
                    match chars.next() {
                        None => break,
                        Some('\'') => {
                            result.push('\'');
                            if chars.peek() == Some(&'\'') {
                                result.push(chars.next().unwrap()); // escaped ''
                            } else {
                                break; // end of string
                            }
                        }
                        Some(c) => result.push(c),
                    }
                }
            }
            // Line comment -- copy until end of line
            '-' if chars.peek() == Some(&'-') => {
                result.push('-');
                result.push(chars.next().unwrap());
                for c in chars.by_ref() {
                    result.push(c);
                    if c == '\n' {
                        break;
                    }
                }
            }
            // Block comment /* … */ — copy verbatim
            '/' if chars.peek() == Some(&'*') => {
                result.push('/');
                result.push(chars.next().unwrap());
                let mut prev = '\0';
                for c in chars.by_ref() {
                    result.push(c);
                    if prev == '*' && c == '/' {
                        break;
                    }
                    prev = c;
                }
            }
            // Parameter placeholder
            '?' => {
                if param_idx >= batch.num_columns() {
                    return Err(Error::with_message_and_status(
                        format!(
                            "query has more '?' placeholders than bound columns (have {})",
                            batch.num_columns()
                        ),
                        Status::InvalidArguments,
                    ));
                }
                let col = batch.column(param_idx);
                result.push_str(&arrow_value_to_sql_literal(col.as_ref(), row_idx)?);
                param_idx += 1;
            }
            c => result.push(c),
        }
    }
    Ok(result)
}

/// Formats an Arrow column value at `row` as a Snowflake SQL literal.
/// NULL → `NULL`; strings are single-quoted; numbers are unquoted.
fn arrow_value_to_sql_literal(arr: &dyn Array, row: usize) -> Result<String> {
    if arr.is_null(row) {
        return Ok("NULL".to_string());
    }
    use arrow_array::{
        BooleanArray, Date32Array, Int16Array, Int32Array, Int64Array,
        LargeStringArray, StringArray,
    };
    macro_rules! num_lit {
        ($T:ty) => {
            if let Some(a) = arr.as_any().downcast_ref::<$T>() {
                return Ok(format!("{}", a.value(row)));
            }
        };
    }
    // Floats need {:?} format which always emits a decimal point or exponent
    // (e.g. "3.14", "1.7976931348623157e308"). The {} Display format may produce
    // a huge integer string (e.g. "179769300...") that Snowflake rejects.
    if let Some(a) = arr.as_any().downcast_ref::<arrow_array::Float64Array>() {
        let v = a.value(row);
        return if v.is_finite() {
            Ok(format!("{v:?}"))
        } else {
            Ok("NULL".to_string())
        };
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow_array::Float32Array>() {
        let v = a.value(row);
        return if v.is_finite() {
            Ok(format!("{:?}", v as f64))
        } else {
            Ok("NULL".to_string())
        };
    }
    num_lit!(Int64Array);
    num_lit!(Int32Array);
    num_lit!(Int16Array);
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Ok(sql_str_lit(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(sql_str_lit(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return Ok(if a.value(row) { "TRUE" } else { "FALSE" }.to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Date32Array>() {
        return Ok(format!(
            "'{}'::DATE",
            days_since_epoch_to_date_str(a.value(row) as i64)
        ));
    }
    Err(Error::with_message_and_status(
        format!("unsupported bind parameter type: {:?}", arr.data_type()),
        Status::NotImplemented,
    ))
}

/// Wraps `s` in single quotes.
/// Backslashes are doubled first (some Snowflake sessions treat `\'` as an escape
/// sequence, which would prematurely close the literal), then single quotes are
/// doubled per ANSI SQL.
fn sql_str_lit(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

/// Converts days since Unix epoch (1970-01-01) to a YYYY-MM-DD string.
pub(crate) fn days_since_epoch_to_date_str(days: i64) -> String {
    // Algorithm: civil date from days (Gregorian proleptic)
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", y, m, d)
}

/// Strips leading SQL whitespace, line comments (`--…`), and block
/// comments (`/*…*/`) from `query`, returning the remaining slice.
fn strip_sql_comments(query: &str) -> &str {
    let mut s = query.trim_start();
    loop {
        if s.starts_with("--") {
            s = s[s.find('\n').map(|i| i + 1).unwrap_or(s.len())..].trim_start();
        } else if s.starts_with("/*") {
            if let Some(end) = s.find("*/") {
                s = s[end + 2..].trim_start();
            } else {
                break;
            }
        } else {
            break;
        }
    }
    s
}

fn is_ddl(query: &str) -> bool {
    let upper = strip_sql_comments(query).to_uppercase();
    upper.starts_with("CREATE ")
        || upper.starts_with("DROP ")
        || upper.starts_with("ALTER ")
        || upper.starts_with("TRUNCATE ")
        || upper.starts_with("RENAME ")
        || upper.starts_with("COMMENT ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::Statement as _;

    fn make_stmt() -> Statement {
        let driver = crate::driver::Driver::default();
        Statement {
            inner: driver.inner.clone(),
            stmt_handle: sf_core::apis::database_driver_v1::Handle { id: 0, magic: 0 },
            conn_handle: sf_core::apis::database_driver_v1::Handle { id: 0, magic: 0 },
            query: None,
            target_table: None,
            ingest_catalog: None,
            ingest_schema: None,
            ingest_mode: None,
            query_tag: None,
            use_high_precision: true,
            timestamp_precision: TimestampPrecision::Nanoseconds,
            bound_batches: vec![],
        }
    }

    #[test]
    fn set_sql_query_stores_query() {
        let mut stmt = make_stmt();
        stmt.set_sql_query("SELECT 1").unwrap();
        assert_eq!(stmt.query.as_deref(), Some("SELECT 1"));
    }

    #[test]
    fn execute_without_query_returns_invalid_state() {
        let mut stmt = make_stmt();
        match stmt.execute() {
            Err(err) => assert_eq!(err.status, adbc_core::error::Status::InvalidState),
            Ok(_) => panic!("execute should have returned an error"),
        }
    }

    #[test]
    fn execute_schema_without_query_returns_invalid_state() {
        let mut stmt = make_stmt();
        match stmt.execute_schema() {
            Err(err) => assert_eq!(err.status, adbc_core::error::Status::InvalidState),
            Ok(_) => panic!("execute_schema should have returned an error"),
        }
    }

    #[test]
    fn execute_with_target_table_no_data_returns_invalid_state() {
        let driver = crate::driver::Driver::default();
        let mut stmt = Statement {
            inner: driver.inner.clone(),
            stmt_handle: sf_core::apis::database_driver_v1::Handle { id: 0, magic: 0 },
            conn_handle: sf_core::apis::database_driver_v1::Handle { id: 0, magic: 0 },
            query: None,
            target_table: Some("mytable".into()),
            ingest_catalog: None,
            ingest_schema: None,
            ingest_mode: None,
            query_tag: None,
            use_high_precision: true,
            timestamp_precision: TimestampPrecision::Nanoseconds,
            bound_batches: vec![],
        };
        match stmt.execute() {
            Err(err) => assert_eq!(err.status, adbc_core::error::Status::InvalidState),
            Ok(_) => panic!("execute should have returned an error"),
        }
    }

    #[test]
    fn set_query_clears_target_table() {
        let driver = crate::driver::Driver::default();
        let mut stmt = Statement {
            inner: driver.inner.clone(),
            stmt_handle: sf_core::apis::database_driver_v1::Handle { id: 0, magic: 0 },
            conn_handle: sf_core::apis::database_driver_v1::Handle { id: 0, magic: 0 },
            query: None,
            target_table: Some("mytable".into()),
            ingest_catalog: None,
            ingest_schema: None,
            ingest_mode: None,
            query_tag: None,
            use_high_precision: true,
            timestamp_precision: TimestampPrecision::Nanoseconds,
            bound_batches: vec![],
        };
        stmt.set_sql_query("SELECT 1").unwrap();
        assert!(stmt.target_table.is_none());
    }

    #[test]
    fn prepare_without_query_returns_invalid_state() {
        let mut stmt = make_stmt();
        let err = stmt.prepare().unwrap_err();
        assert_eq!(err.status, adbc_core::error::Status::InvalidState);
    }

    #[test]
    fn prepare_with_query_is_noop() {
        let mut stmt = make_stmt();
        stmt.set_sql_query("SELECT 1").unwrap();
        stmt.prepare().unwrap();
    }

    #[test]
    fn set_target_table_option() {
        let mut stmt = make_stmt();
        stmt.set_option(
            OptionStatement::TargetTable,
            OptionValue::String("mytable".into()),
        )
        .unwrap();
        assert_eq!(stmt.target_table.as_deref(), Some("mytable"));
    }

    #[test]
    fn unknown_option_returns_not_found() {
        let mut stmt = make_stmt();
        let err = stmt
            .set_option(
                OptionStatement::Other("unknown.option".into()),
                OptionValue::String("val".into()),
            )
            .unwrap_err();
        assert_eq!(err.status, adbc_core::error::Status::NotFound);
    }

    #[test]
    fn set_query_tag_stored_and_readable() {
        let mut stmt = make_stmt();
        stmt.set_option(
            OptionStatement::Other("adbc.snowflake.statement.query_tag".into()),
            OptionValue::String("my_tag".into()),
        )
        .unwrap();
        assert_eq!(
            stmt.get_option_string(OptionStatement::Other(
                "adbc.snowflake.statement.query_tag".into()
            ))
            .unwrap(),
            "my_tag"
        );
        // Verify conn_handle is present on the struct (compile-time check)
        let _ = stmt.conn_handle;
    }

    #[test]
    fn test_adjust_schema_int8_to_int64() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int8, false)]);
        let result = adjust_schema_integers(&schema);
        assert_eq!(result.field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn test_adjust_schema_int16_to_int64() {
        let schema = Schema::new(vec![Field::new("b", DataType::Int16, true)]);
        let result = adjust_schema_integers(&schema);
        assert_eq!(result.field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn test_adjust_schema_int32_to_int64() {
        let schema = Schema::new(vec![Field::new("c", DataType::Int32, false)]);
        let result = adjust_schema_integers(&schema);
        assert_eq!(result.field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn test_adjust_schema_int64_passthrough() {
        let schema = Schema::new(vec![Field::new("d", DataType::Int64, false)]);
        let result = adjust_schema_integers(&schema);
        assert_eq!(result.field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn test_adjust_schema_mixed() {
        use arrow_schema::DataType;
        let schema = Schema::new(vec![
            Field::new("i8", DataType::Int8, false),
            Field::new("s", DataType::Utf8, false),
            Field::new("i32", DataType::Int32, true),
            Field::new("f64", DataType::Float64, false),
        ]);
        let result = adjust_schema_integers(&schema);
        assert_eq!(result.field(0).data_type(), &DataType::Int64);
        assert_eq!(result.field(1).data_type(), &DataType::Utf8);
        assert_eq!(result.field(2).data_type(), &DataType::Int64);
        assert_eq!(result.field(3).data_type(), &DataType::Float64);
    }

    #[test]
    fn test_converting_reader_int8_values() {
        use arrow_array::Int8Array;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int8, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int8Array::from(vec![1i8, 2, 3]))],
        )
        .unwrap();
        let reader = ConcatReader {
            batches: vec![batch].into_iter(),
            schema,
        };
        let mut cr = ConvertingReader::new(reader, true, TimeUnit::Nanosecond);
        let out = cr.next().unwrap().unwrap();
        assert_eq!(out.schema().field(0).data_type(), &DataType::Int64);
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(col.values(), &[1i64, 2, 3]);
    }

    #[test]
    fn test_converting_reader_null_values() {
        use arrow_array::Int16Array;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int16, true)]));
        let arr = Int16Array::from(vec![Some(10i16), None, Some(30)]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        let reader = ConcatReader {
            batches: vec![batch].into_iter(),
            schema,
        };
        let mut cr = ConvertingReader::new(reader, true, TimeUnit::Nanosecond);
        let out = cr.next().unwrap().unwrap();
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 10i64);
        assert!(col.is_null(1));
        assert_eq!(col.value(2), 30i64);
    }

    #[test]
    fn test_converting_reader_empty_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::new_empty(schema.clone());
        let reader = ConcatReader {
            batches: vec![batch].into_iter(),
            schema,
        };
        let mut cr = ConvertingReader::new(reader, true, TimeUnit::Nanosecond);
        let out = cr.next().unwrap().unwrap();
        assert_eq!(out.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(out.num_rows(), 0);
    }

    #[test]
    fn test_converting_reader_multiple_batches_different_widths() {
        use arrow_array::{Int32Array, Int8Array};
        struct TwoBatchReader {
            batches: std::vec::IntoIter<RecordBatch>,
            schema: Arc<Schema>,
        }
        impl Iterator for TwoBatchReader {
            type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;
            fn next(&mut self) -> Option<Self::Item> {
                self.batches.next().map(Ok)
            }
        }
        impl RecordBatchReader for TwoBatchReader {
            fn schema(&self) -> Arc<Schema> {
                self.schema.clone()
            }
        }

        let declared_schema =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));

        let schema_i8 = Arc::new(Schema::new(vec![Field::new("v", DataType::Int8, false)]));
        let schema_i32 = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch1 = RecordBatch::try_new(
            schema_i8,
            vec![Arc::new(Int8Array::from(vec![1i8, 2]))],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema_i32,
            vec![Arc::new(Int32Array::from(vec![100i32, 200]))],
        )
        .unwrap();

        let reader = TwoBatchReader {
            batches: vec![batch1, batch2].into_iter(),
            schema: declared_schema,
        };
        let mut cr = ConvertingReader::new(reader, true, TimeUnit::Nanosecond);

        let out1 = cr.next().unwrap().unwrap();
        assert_eq!(out1.column(0).data_type(), &DataType::Int64);
        let col1 = out1
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(col1.values(), &[1i64, 2]);

        let out2 = cr.next().unwrap().unwrap();
        assert_eq!(out2.column(0).data_type(), &DataType::Int64);
        let col2 = out2
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(col2.values(), &[100i64, 200]);
    }
    fn make_field_with_meta(name: &str, dt: DataType, logical_type: &str, scale: &str) -> Field {
        let mut md = std::collections::HashMap::new();
        md.insert("logicalType".to_string(), logical_type.to_string());
        md.insert("scale".to_string(), scale.to_string());
        Field::new(name, dt, true).with_metadata(md)
    }

    #[test]
    fn test_adjust_schema_time_scale3_is_time32_millisecond() {
        let f = make_field_with_meta("t", DataType::Int32, "TIME", "3");
        let schema = Schema::new(vec![f]);
        let result = adjust_schema(&schema, false, TimeUnit::Nanosecond);
        assert_eq!(result.field(0).data_type(), &DataType::Time32(TimeUnit::Millisecond));
    }

    #[test]
    fn test_adjust_schema_time_scale9_is_time64_nanosecond() {
        let f = make_field_with_meta("t", DataType::Int64, "TIME", "9");
        let schema = Schema::new(vec![f]);
        let result = adjust_schema(&schema, false, TimeUnit::Nanosecond);
        assert_eq!(result.field(0).data_type(), &DataType::Time64(TimeUnit::Nanosecond));
    }

    #[test]
    fn test_adjust_schema_fixed_scale2_is_float64() {
        let f = make_field_with_meta("x", DataType::Int64, "FIXED", "2");
        let schema = Schema::new(vec![f]);
        let result = adjust_schema(&schema, false, TimeUnit::Nanosecond);
        assert_eq!(result.field(0).data_type(), &DataType::Float64);
    }

    #[test]
    fn test_adjust_schema_fixed_scale2_high_precision_stays_int64() {
        let f = make_field_with_meta("x", DataType::Int64, "FIXED", "2");
        let schema = Schema::new(vec![f]);
        let result = adjust_schema(&schema, true, TimeUnit::Nanosecond);
        assert_eq!(result.field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn test_adjust_schema_timestamp_ntz_int64_to_timestamp_ns() {
        let f = make_field_with_meta("ts", DataType::Int64, "TIMESTAMP_NTZ", "9");
        let schema = Schema::new(vec![f]);
        let result = adjust_schema(&schema, false, TimeUnit::Nanosecond);
        assert_eq!(result.field(0).data_type(), &DataType::Timestamp(TimeUnit::Nanosecond, None));
    }

    #[test]
    fn test_adjust_schema_timestamp_ltz_is_utc() {
         let f = make_field_with_meta("ts", DataType::Int64, "TIMESTAMP_LTZ", "6");
         let schema = Schema::new(vec![f]);
         let result = adjust_schema(&schema, false, TimeUnit::Microsecond);
         assert_eq!(
             result.field(0).data_type(),
             &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
         );
     }

    #[test]
    fn test_converting_reader_fixed_scale2_produces_float64() {
        let f = make_field_with_meta("x", DataType::Int64, "FIXED", "2");
        let schema = Arc::new(Schema::new(vec![f]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![12345i64, 255]))],
        )
        .unwrap();
        let reader = ConcatReader { batches: vec![batch].into_iter(), schema };
        let mut cr = ConvertingReader::new(reader, false, TimeUnit::Nanosecond);
        let out = cr.next().unwrap().unwrap();
        assert_eq!(out.schema().field(0).data_type(), &DataType::Float64);
        let col = out.column(0).as_any().downcast_ref::<arrow_array::Float64Array>().unwrap();
        assert!((col.value(0) - 123.45).abs() < 1e-9);
        assert!((col.value(1) - 2.55).abs() < 1e-9);
    }

    #[test]
    fn test_converting_reader_timestamp_ntz_int64_cast() {
        let f = make_field_with_meta("ts", DataType::Int64, "TIMESTAMP_NTZ", "9");
        let schema = Arc::new(Schema::new(vec![f]));
        let epoch_ns: i64 = 1_000_000_000;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![epoch_ns]))],
        )
        .unwrap();
        let reader = ConcatReader { batches: vec![batch].into_iter(), schema };
        let mut cr = ConvertingReader::new(reader, false, TimeUnit::Nanosecond);
        let out = cr.next().unwrap().unwrap();
        assert_eq!(
            out.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(col.value(0), epoch_ns);
    }

    #[test]
    fn test_adjust_schema_timestamp_ntz_capped_by_ts_unit() {
        let f = make_field_with_meta("ts", DataType::Int64, "TIMESTAMP_NTZ", "9");
        let schema = Schema::new(vec![f]);
        let result = adjust_schema(&schema, false, TimeUnit::Microsecond);
        assert_eq!(result.field(0).data_type(), &DataType::Timestamp(TimeUnit::Microsecond, None));
    }

}