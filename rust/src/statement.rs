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

// src/statement.rs
use std::{collections::VecDeque, sync::Arc};

use adbc_core::{
    Optionable, PartitionedResult,
    error::{Error, Result, Status},
    options::{OptionStatement, OptionValue},
};
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchReader};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use sf_core::apis::database_driver_v1::{BindingType, ColumnMetadata, DataPtr, Handle};
use sf_core::config::{param_registry::param_names, settings::Setting};
use sf_core::rest::snowflake::AbortOutcome;

use crate::driver::{Inner, QueryResult, TimestampPrecision};

/// Per-statement Snowflake query timeout, in seconds. A value of zero clears
/// the override and lets the session/core default apply.
const QUERY_TIMEOUT_OPTION: &str = "adbc.snowflake.statement.query_timeout";

fn parse_query_timeout(value: &OptionValue) -> Result<Option<u32>> {
    let seconds = match value {
        OptionValue::Int(value) => u32::try_from(*value).map_err(|_| {
            Error::with_message_and_status(
                "query_timeout must be between 0 and 4294967295 seconds",
                Status::InvalidArguments,
            )
        })?,
        OptionValue::String(value) => {
            let raw = value.strip_suffix('s').unwrap_or(value);
            raw.parse::<u32>().map_err(|_| {
                Error::with_message_and_status(
                    format!("invalid query_timeout value: {value}"),
                    Status::InvalidArguments,
                )
            })?
        }
        _ => {
            return Err(Error::with_message_and_status(
                "query_timeout must be a string or int",
                Status::InvalidArguments,
            ));
        }
    };
    Ok((seconds != 0).then_some(seconds))
}

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
    pub(crate) query_timeout_seconds: Option<u32>,
    pub(crate) use_high_precision: bool,
    pub(crate) timestamp_precision: TimestampPrecision,
    /// Parameter batches stored by bind() / bind_stream(). Each row is one execution.
    pub(crate) bound_batches: Vec<RecordBatch>,
    /// Whether bind() / bind_stream() was called, including with an empty stream.
    pub(crate) binding_supplied: bool,
    pub(crate) last_query_id: Option<String>,
    /// Cached only when sf_core's prepare bind count and metadata agree.
    pub(crate) prepared_parameter_schema: Option<Schema>,
    /// Truthful result schema returned by Snowflake's describe-only prepare.
    pub(crate) prepared_result_schema: Option<Schema>,
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
                    self.prepared_parameter_schema = None;
                    self.prepared_result_schema = None;
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
                    self.prepared_parameter_schema = None;
                    self.prepared_result_schema = None;
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
                self.prepared_parameter_schema = None;
                self.prepared_result_schema = None;
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
                self.prepared_parameter_schema = None;
                self.prepared_result_schema = None;
                Ok(())
            }
            OptionStatement::Temporary => {
                // Accepted silently; used to select CREATE TEMPORARY TABLE during ingest.
                self.prepared_parameter_schema = None;
                self.prepared_result_schema = None;
                Ok(())
            }
            OptionStatement::TargetCatalog => {
                if let OptionValue::String(s) = value {
                    self.ingest_catalog = Some(s);
                }
                self.prepared_parameter_schema = None;
                self.prepared_result_schema = None;
                Ok(())
            }
            OptionStatement::TargetDbSchema => {
                if let OptionValue::String(s) = value {
                    self.ingest_schema = Some(s);
                }
                self.prepared_parameter_schema = None;
                self.prepared_result_schema = None;
                Ok(())
            }
            OptionStatement::Other(ref k) if k == QUERY_TIMEOUT_OPTION => {
                self.query_timeout_seconds = parse_query_timeout(&value)?;
                Ok(())
            }
            OptionStatement::Other(ref k) if k == "adbc.snowflake.statement.query_tag" => {
                if let OptionValue::String(s) = value {
                    self.inner
                        .runtime
                        .block_on(self.inner.sf.statement_set_option(
                            self.stmt_handle,
                            param_names::QUERY_TAG.into(),
                            Setting::String(s.clone()),
                        ))
                        .map_err(crate::error::api_error_to_adbc_error)?;
                    // Update ADBC-visible storage only after sf_core accepts the option.
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
            OptionStatement::Other(ref k) if k == QUERY_TIMEOUT_OPTION => {
                Ok(self.query_timeout_seconds.unwrap_or(0).to_string())
            }
            OptionStatement::Other(ref k) if k == "adbc.snowflake.statement.query_tag" => {
                Ok(self.query_tag.clone().unwrap_or_default())
            }
            OptionStatement::Other(ref k) if k == "adbc.snowflake.sql.query_id" => {
                self.last_query_id.clone().ok_or_else(|| {
                    Error::with_message_and_status(
                        "no query has been executed yet",
                        Status::NotFound,
                    )
                })
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

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        match key {
            OptionStatement::Other(ref k) if k == QUERY_TIMEOUT_OPTION => {
                Ok(i64::from(self.query_timeout_seconds.unwrap_or(0)))
            }
            _ => Err(Error::with_message_and_status(
                "option not found",
                Status::NotFound,
            )),
        }
    }

    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        Err(Error::with_message_and_status(
            "option not found",
            Status::NotFound,
        ))
    }
}

impl Statement {
    fn execution_timeout_seconds(&self) -> Option<u32> {
        self.query_timeout_seconds
    }

    /// Route every user-statement execution through the statement timeout.
    /// The binding owner must remain alive until this synchronous call returns.
    pub(crate) fn execute_query<'a>(
        &self,
        query: String,
        bindings: Option<BindingType<'a>>,
    ) -> Result<QueryResult> {
        self.inner.execute_query_with_timeout(
            self.stmt_handle,
            query,
            bindings,
            self.execution_timeout_seconds(),
        )
    }

    /// Execute a parameterized query once per bound row and concatenate results.
    fn execute_bound(
        &mut self,
        query: String,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let row_count: usize = self.bound_batches.iter().map(RecordBatch::num_rows).sum();
        if row_count == 0 {
            // ADBC defines this as zero executions. Describe-only prepare provides
            // the result schema without accidentally running the SQL unbound.
            self.last_query_id = None;
            let schema = if let Some(schema) = &self.prepared_result_schema {
                Arc::new(schema.clone())
            } else {
                let prepared = self.inner.prepare_statement(self.stmt_handle, query)?;
                self.prepared_parameter_schema = parameter_schema_from_prepare(
                    prepared.number_of_binds,
                    &prepared.binds,
                    self.use_high_precision,
                    self.timestamp_precision.time_unit(),
                );
                let schema = adjust_schema(
                    &prepared.reader.schema(),
                    self.use_high_precision,
                    self.timestamp_precision.time_unit(),
                );
                self.prepared_result_schema = Some(schema.as_ref().clone());
                schema
            };
            return execute_bound_rows(&self.bound_batches, schema, |_| {
                unreachable!("zero-row bindings must not execute")
            });
        }

        let inner = self.inner.clone();
        let stmt_handle = self.stmt_handle;
        let timeout_seconds = self.execution_timeout_seconds();
        let use_high_precision = self.use_high_precision;
        let timestamp_precision = self.timestamp_precision;
        let mut last_query_id = self.last_query_id.clone();
        let result = execute_bound_rows(&self.bound_batches, Arc::new(Schema::empty()), |row| {
            let json_bytes = arrow_batches_to_json_bindings(&[row])?;
            let data_ptr = DataPtr::new(json_bytes.as_ptr(), json_bytes.len() as i64);
            let result = inner.execute_query_with_timeout(
                stmt_handle,
                query.clone(),
                Some(BindingType::Json(data_ptr)),
                timeout_seconds,
            )?;
            last_query_id = Some(result.descriptor.query_id.clone());
            Ok(Box::new(ConvertingReader::new(
                result.reader,
                use_high_precision,
                timestamp_precision.time_unit(),
                timestamp_precision,
            )))
        });
        // On a later-row failure, retain the ID of the last successful execution.
        self.last_query_id = last_query_id;
        result
    }
}

impl adbc_core::Statement for Statement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        self.bound_batches = vec![batch];
        self.binding_supplied = true;
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
        self.binding_supplied = true;
        Ok(())
    }

    #[allow(refining_impl_trait)]
    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        if self.target_table.is_some() {
            // Ingest via execute() — run the ingest and return an empty reader.
            let result = crate::ingest::execute_ingest(self);
            self.bound_batches.clear();
            self.binding_supplied = false;
            result?;
            let batch =
                arrow_array::RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty()));
            return Ok(Box::new(crate::connection::SingleBatchReader::new(batch)));
        }
        let query = self.query.clone().ok_or_else(|| {
            Error::with_message_and_status("cannot execute without a query", Status::InvalidState)
        })?;

        if self.binding_supplied {
            return self.execute_bound(query);
        }

        let result = self.execute_query(query, None)?;

        self.last_query_id = Some(result.descriptor.query_id.clone());
        let reader = result.reader;
        Ok(Box::new(ConvertingReader::new(
            reader,
            self.use_high_precision,
            self.timestamp_precision.time_unit(),
            self.timestamp_precision,
        )))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        if self.target_table.is_some() {
            let result = crate::ingest::execute_ingest(self);
            self.bound_batches.clear();
            self.binding_supplied = false;
            return result;
        }
        let query = self.query.clone().ok_or_else(|| {
            Error::with_message_and_status("cannot execute without a query", Status::InvalidState)
        })?;

        // A supplied binding with no rows represents zero executions, so do not
        // run the statement unbound or send an empty binding.
        if self.binding_supplied {
            let row_count: usize = self.bound_batches.iter().map(RecordBatch::num_rows).sum();
            if row_count == 0 {
                self.last_query_id = None;
                return Ok(Some(0));
            }
        }

        // Parameterised DML: execute once with JSON bindings.
        if self.binding_supplied {
            let json_bytes = arrow_batches_to_json_bindings(&self.bound_batches)?;
            let data_ptr = DataPtr::new(json_bytes.as_ptr(), json_bytes.len() as i64);
            let binding = BindingType::Json(data_ptr);

            let result = self.execute_query(query, Some(binding))?;

            self.last_query_id = Some(result.descriptor.query_id.clone());

            return Ok(result.descriptor.rows_affected);
        }

        let result = self.execute_query(query, None)?;

        self.last_query_id = Some(result.descriptor.query_id.clone());

        Ok(result.descriptor.rows_affected)
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        let query = self.query.clone().ok_or_else(|| {
            Error::with_message_and_status("cannot execute without a query", Status::InvalidState)
        })?;

        self.prepared_parameter_schema = None;
        self.prepared_result_schema = None;
        let prepared = self.inner.prepare_statement(self.stmt_handle, query)?;
        self.last_query_id = Some(prepared.query_id.clone());
        self.prepared_parameter_schema = parameter_schema_from_prepare(
            prepared.number_of_binds,
            &prepared.binds,
            self.use_high_precision,
            self.timestamp_precision.time_unit(),
        );
        let schema = adjust_schema(
            &prepared.reader.schema(),
            self.use_high_precision,
            self.timestamp_precision.time_unit(),
        )
        .as_ref()
        .clone();
        self.prepared_result_schema = Some(schema.clone());
        Ok(schema)
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Err(crate::error::not_implemented("execute_partitions"))
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        self.prepared_parameter_schema.clone().ok_or_else(|| {
            Error::with_message_and_status(
                "parameter metadata is unavailable; prepare the statement first",
                Status::NotImplemented,
            )
        })
    }

    fn prepare(&mut self) -> Result<()> {
        let query = self.query.clone().ok_or_else(|| {
            Error::with_message_and_status(
                "cannot prepare statement with no query",
                Status::InvalidState,
            )
        })?;
        self.prepared_parameter_schema = None;
        self.prepared_result_schema = None;
        let prepared = self.inner.prepare_statement(self.stmt_handle, query)?;
        self.last_query_id = Some(prepared.query_id);
        self.prepared_parameter_schema = parameter_schema_from_prepare(
            prepared.number_of_binds,
            &prepared.binds,
            self.use_high_precision,
            self.timestamp_precision.time_unit(),
        );
        self.prepared_result_schema = Some(
            adjust_schema(
                &prepared.reader.schema(),
                self.use_high_precision,
                self.timestamp_precision.time_unit(),
            )
            .as_ref()
            .clone(),
        );
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.query = Some(query.as_ref().to_string());
        self.target_table = None;
        self.bound_batches.clear();
        self.binding_supplied = false;
        self.prepared_parameter_schema = None;
        self.prepared_result_schema = None;
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(crate::error::not_implemented(
            "Snowflake does not support Substrait plans",
        ))
    }

    fn cancel(&mut self) -> Result<()> {
        match self
            .inner
            .runtime
            .block_on(self.inner.sf.statement_cancel(self.stmt_handle))
            .map_err(crate::error::api_error_to_adbc_error)?
        {
            AbortOutcome::Aborted | AbortOutcome::NotRunning => Ok(()),
        }
    }
}

// ── Bound query result concatenation ─────────────────────────────────────────

struct ChainedReader {
    readers: VecDeque<Box<dyn RecordBatchReader + Send>>,
    schema: Arc<Schema>,
}

impl Iterator for ChainedReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let reader = self.readers.front_mut()?;
            if let Some(batch) = reader.next() {
                return Some(batch);
            }
            self.readers.pop_front();
        }
    }
}

impl RecordBatchReader for ChainedReader {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

fn execute_bound_rows<F>(
    batches: &[RecordBatch],
    empty_schema: Arc<Schema>,
    mut execute: F,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>>
where
    F: FnMut(RecordBatch) -> Result<Box<dyn RecordBatchReader + Send + 'static>>,
{
    let mut readers = VecDeque::new();
    let mut result_schema = None;

    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let reader = execute(batch.slice(row_index, 1))?;
            let schema = reader.schema();
            if let Some(expected) = &result_schema {
                if schema.as_ref() != expected {
                    return Err(Error::with_message_and_status(
                        format!(
                            "bound query result schema mismatch: expected {expected:?}, got {schema:?}"
                        ),
                        Status::InvalidData,
                    ));
                }
            } else {
                result_schema = Some(schema.as_ref().clone());
            }
            readers.push_back(reader);
        }
    }

    Ok(Box::new(ChainedReader {
        readers,
        schema: Arc::new(result_schema.unwrap_or_else(|| empty_schema.as_ref().clone())),
    }))
}

// Test reader for constructing deterministic Arrow streams.
#[cfg(test)]
struct ConcatReader {
    batches: std::vec::IntoIter<RecordBatch>,
    schema: Arc<Schema>,
}

#[cfg(test)]
impl Iterator for ConcatReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.batches.next().map(Ok)
    }
}

#[cfg(test)]
impl RecordBatchReader for ConcatReader {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

// ── Schema adjustment and type conversions ────────────────────────────────

fn parameter_schema_from_prepare(
    number_of_binds: i32,
    binds: &[ColumnMetadata],
    use_high_precision: bool,
    ts_unit: TimeUnit,
) -> Option<Schema> {
    let bind_count = usize::try_from(number_of_binds).ok()?;
    if bind_count != binds.len() || binds.iter().any(|bind| bind.r#type.trim().is_empty()) {
        return None;
    }

    let fields = binds
        .iter()
        .map(|bind| {
            let logical_type = bind.r#type.to_ascii_uppercase();
            let scale = bind.scale.unwrap_or(0);
            let physical_type = match logical_type.as_str() {
                "FIXED" => DataType::Int64,
                "REAL" => DataType::Float64,
                "BOOLEAN" => DataType::Boolean,
                "DATE" => DataType::Date32,
                "TIME" if scale < 6 => DataType::Int32,
                "TIME" => DataType::Int64,
                "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" => DataType::Int64,
                "BINARY" => DataType::Binary,
                "TEXT" | "ARRAY" | "OBJECT" | "VARIANT" | "GEOGRAPHY" | "GEOMETRY" => {
                    DataType::Utf8
                }
                _ => DataType::Null,
            };
            let mut metadata = std::collections::HashMap::from([
                ("logicalType".to_string(), logical_type),
                ("nullable".to_string(), bind.nullable.to_string()),
            ]);
            if let Some(precision) = bind.precision {
                metadata.insert("precision".into(), precision.to_string());
            }
            if let Some(scale) = bind.scale {
                metadata.insert("scale".into(), scale.to_string());
            }
            Field::new(&bind.name, physical_type, bind.nullable).with_metadata(metadata)
        })
        .collect::<Vec<_>>();
    Some(
        adjust_schema(&Schema::new(fields), use_high_precision, ts_unit)
            .as_ref()
            .clone(),
    )
}

fn scale_to_time_unit(scale: u32) -> TimeUnit {
    match scale {
        0 => TimeUnit::Second,
        1..=3 => TimeUnit::Millisecond,
        4..=6 => TimeUnit::Microsecond,
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
    let precision: u8 = field
        .metadata()
        .get("precision")
        .and_then(|s| s.parse().ok())
        .unwrap_or(38)
        .min(38);

    match logical_type {
        "FIXED" => {
            if use_high_precision && scale > 0 {
                DataType::Decimal128(precision, scale as i8)
            } else if scale == 0 {
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
    check_overflow: bool,
    logical_types: Vec<String>,
    scales: Vec<i64>,
}

impl<R: RecordBatchReader> ConvertingReader<R> {
    pub(crate) fn new(
        inner: R,
        use_high_precision: bool,
        ts_unit: TimeUnit,
        ts_precision: TimestampPrecision,
    ) -> Self {
        let orig_schema = inner.schema();
        let logical_types: Vec<String> = orig_schema
            .fields()
            .iter()
            .map(|f| f.metadata().get("logicalType").cloned().unwrap_or_default())
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
            check_overflow: ts_precision == TimestampPrecision::NanosecondsErrorOnOverflow,
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
                    match col.data_type() {
                        // Source is already Decimal128 — cast to ensure target_type metadata is applied exactly
                        DataType::Decimal128(_, _) => arrow_cast::cast(col.as_ref(), target_type),
                        // Source is Int64 (scaled integer) — convert to Decimal128(precision, scale)
                        // following Go's integerToDecimal128: cast Int64 → Decimal128(20,0),
                        // then reinterpret as Decimal128(precision, scale).
                        _ => {
                            let intermediate =
                                arrow_cast::cast(col.as_ref(), &DataType::Decimal128(20, 0))?;
                            let data = intermediate.to_data();
                            // Safety: Decimal128 and Decimal128(20,0) share identical 128-bit
                            // buffer layouts, so this reinterpret is memory-safe.
                            // Values that exceed target_type's precision are not validated here
                            // and will silently overflow the declared precision.
                            let retyped = unsafe {
                                data.into_builder()
                                    .data_type(target_type.clone())
                                    .build_unchecked()
                            };
                            Ok(arrow_array::make_array(retyped))
                        }
                    }
                } else {
                    match col.data_type() {
                        DataType::Decimal128(_, _) => {
                            arrow_cast::cast(col.as_ref(), &DataType::Float64)
                        }
                        _ => {
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
                }
            }
            "TIME" => arrow_cast::cast(col.as_ref(), target_type),
            "REAL" => arrow_cast::cast(col.as_ref(), &DataType::Float64),
            "TIMESTAMP_NTZ" => convert_timestamp_ntz(col, scale, ts_unit, check_overflow, None),
            "TIMESTAMP_LTZ" => {
                convert_timestamp_ntz(col, scale, ts_unit, check_overflow, Some(Arc::from("UTC")))
            }
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
    let target = DataType::Timestamp(unit, tz_str.clone());

    match col.data_type() {
        DataType::Int64 => {
            let natural_unit = scale_to_time_unit(scale.max(0) as u32);
            if natural_unit == unit {
                arrow_cast::cast(col.as_ref(), &target)
            } else {
                let natural_target = DataType::Timestamp(natural_unit, tz_str);
                let intermediate = arrow_cast::cast(col.as_ref(), &natural_target)?;
                arrow_cast::cast(intermediate.as_ref(), &target)
            }
        }
        DataType::Struct(_) => {
            use arrow_array::{Int32Array, Int64Array, StructArray};
            let struct_arr = col.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                arrow_schema::ArrowError::CastError("expected StructArray".into())
            })?;
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

            build_timestamp_from_epoch_fraction(
                epoch,
                fraction,
                struct_arr,
                scale,
                check_overflow,
                target,
            )
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

    let struct_arr = col.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
        arrow_schema::ArrowError::CastError("expected StructArray for TIMESTAMP_TZ".into())
    })?;

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

        build_timestamp_tz_2field(epoch, tzoffset, struct_arr, scale, check_overflow, target)
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

        build_timestamp_tz_3field(
            epoch,
            fraction,
            tzoffset,
            struct_arr,
            scale,
            check_overflow,
            target,
        )
    }
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

fn ns_to_unit(ns: i128, unit: TimeUnit) -> i64 {
    let val = match unit {
        TimeUnit::Second => ns / 1_000_000_000,
        TimeUnit::Millisecond => ns / 1_000_000,
        TimeUnit::Microsecond => ns / 1_000,
        // Intentional truncation: ns outside i64::MIN..=i64::MAX wraps. Use check_overflow=true in the caller to reject out-of-range values *before* this cast is reached.
        TimeUnit::Nanosecond => ns,
    };
    val as i64
}

fn build_timestamp_from_epoch_fraction(
    epoch: &arrow_array::Int64Array,
    fraction: &arrow_array::Int32Array,
    struct_arr: &arrow_array::StructArray,
    scale: i64,
    check_overflow: bool,
    target: DataType,
) -> std::result::Result<ArrayRef, arrow_schema::ArrowError> {
    let unit = match &target {
        DataType::Timestamp(u, _) => *u,
        _ => unreachable!("target must be Timestamp"),
    };

    let len = epoch.len();
    let scale = scale.clamp(0, 9);
    let frac_to_ns: i128 = 10i128.pow((9 - scale) as u32);

    let mut values: Vec<Option<i64>> = Vec::with_capacity(len);
    for i in 0..len {
        if struct_arr.is_null(i) {
            values.push(None);
        } else {
            let ns: i128 =
                epoch.value(i) as i128 * 1_000_000_000 + fraction.value(i) as i128 * frac_to_ns;
            if check_overflow {
                check_ns_overflow(ns)?;
            }
            values.push(Some(ns_to_unit(ns, unit)));
        }
    }
    let int_arr = arrow_array::Int64Array::from(values);
    let data = unsafe {
        int_arr
            .into_data()
            .into_builder()
            .data_type(target.clone())
            .build_unchecked()
    };
    Ok(Arc::new(arrow_array::make_array(data)) as ArrayRef)
}

fn build_timestamp_tz_2field(
    epoch: &arrow_array::Int64Array,
    tzoffset: &arrow_array::Int32Array,
    struct_arr: &arrow_array::StructArray,
    scale: i64,
    check_overflow: bool,
    target: DataType,
) -> std::result::Result<ArrayRef, arrow_schema::ArrowError> {
    let unit = match &target {
        DataType::Timestamp(u, _) => *u,
        _ => unreachable!("target must be Timestamp"),
    };

    let len = epoch.len();
    let mut values: Vec<Option<i64>> = Vec::with_capacity(len);

    for i in 0..len {
        if struct_arr.is_null(i) {
            values.push(None);
        } else {
            let tz_offset_minutes: i128 = (tzoffset.value(i) as i128) - 1440;
            let tz_offset_ns: i128 = tz_offset_minutes * 60 * 1_000_000_000;

            let epoch_ns: i128 = epoch.value(i) as i128
                * 10i128.pow((9u32).saturating_sub(scale.clamp(0, 9) as u32));

            let utc_ns = epoch_ns - tz_offset_ns;
            if check_overflow {
                check_ns_overflow(utc_ns)?;
            }
            values.push(Some(ns_to_unit(utc_ns, unit)));
        }
    }
    let int_arr = arrow_array::Int64Array::from(values);
    let data = unsafe {
        int_arr
            .into_data()
            .into_builder()
            .data_type(target.clone())
            .build_unchecked()
    };
    Ok(Arc::new(arrow_array::make_array(data)) as ArrayRef)
}

#[allow(clippy::too_many_arguments)]
fn build_timestamp_tz_3field(
    epoch: &arrow_array::Int64Array,
    fraction: &arrow_array::Int32Array,
    tzoffset: &arrow_array::Int32Array,
    struct_arr: &arrow_array::StructArray,
    scale: i64,
    check_overflow: bool,
    target: DataType,
) -> std::result::Result<ArrayRef, arrow_schema::ArrowError> {
    let unit = match &target {
        DataType::Timestamp(u, _) => *u,
        _ => unreachable!("target must be Timestamp"),
    };

    let len = epoch.len();
    let scale = scale.clamp(0, 9);
    let frac_to_ns: i128 = 10i128.pow((9 - scale) as u32);
    let mut values: Vec<Option<i64>> = Vec::with_capacity(len);

    for i in 0..len {
        if struct_arr.is_null(i) {
            values.push(None);
        } else {
            let tz_offset_minutes: i128 = (tzoffset.value(i) as i128) - 1440;
            let tz_offset_ns: i128 = tz_offset_minutes * 60 * 1_000_000_000;

            let epoch_ns: i128 =
                epoch.value(i) as i128 * 1_000_000_000 + fraction.value(i) as i128 * frac_to_ns;

            let utc_ns = epoch_ns - tz_offset_ns;
            if check_overflow {
                check_ns_overflow(utc_ns)?;
            }
            values.push(Some(ns_to_unit(utc_ns, unit)));
        }
    }
    let int_arr = arrow_array::Int64Array::from(values);
    let data = unsafe {
        int_arr
            .into_data()
            .into_builder()
            .data_type(target.clone())
            .build_unchecked()
    };
    Ok(Arc::new(arrow_array::make_array(data)) as ArrayRef)
}

impl<R: RecordBatchReader> Iterator for ConvertingReader<R> {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = match self.inner.next()? {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };

        let check_overflow = self.check_overflow;

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

// ── JSON parameter bindings ───────────────────────────────────────────────────

fn snowflake_type_name(dt: &DataType) -> Result<&'static str> {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Decimal128(_, _) => Ok("FIXED"),
        DataType::Float32 | DataType::Float64 => Ok("REAL"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("TEXT"),
        DataType::Boolean => Ok("BOOLEAN"),
        DataType::Date32 | DataType::Date64 => Ok("DATE"),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::BinaryView => Ok("BINARY"),
        DataType::Time32(_) | DataType::Time64(_) => Ok("TIME"),
        DataType::Timestamp(_, None) => Ok("TIMESTAMP_NTZ"),
        DataType::Timestamp(_, Some(_)) => Ok("TIMESTAMP_LTZ"),
        _ => Err(Error::with_message_and_status(
            format!("unsupported bind parameter type: {dt:?}"),
            Status::NotImplemented,
        )),
    }
}

fn escape_json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

pub(crate) fn arrow_batches_to_json_bindings(batches: &[RecordBatch]) -> Result<String> {
    if batches.is_empty() {
        return Ok("{}".to_string());
    }

    let num_cols = batches[0].num_columns();
    let schema = batches[0].schema();

    let mut col_types: Vec<&'static str> = Vec::with_capacity(num_cols);
    for i in 0..num_cols {
        col_types.push(snowflake_type_name(schema.field(i).data_type())?);
    }

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    let mut col_values: Vec<Vec<Option<String>>> = vec![Vec::with_capacity(total_rows); num_cols];

    for batch in batches {
        for (col_idx, values) in col_values.iter_mut().enumerate() {
            let col = batch.column(col_idx);
            let dt = col.data_type();
            for row in 0..batch.num_rows() {
                if col.is_null(row) {
                    values.push(None);
                    continue;
                }
                let val = format_arrow_value(col.as_ref(), row, dt)?;
                values.push(val);
            }
        }
    }

    let mut json = String::from("{");
    for col_idx in 0..num_cols {
        if col_idx > 0 {
            json.push(',');
        }
        let key = col_idx + 1;
        json.push_str(&format!(
            "\"{}\":{{\"type\":\"{}\",\"value\":",
            key, col_types[col_idx]
        ));

        if total_rows == 1 {
            match &col_values[col_idx][0] {
                None => json.push_str("null"),
                Some(v) => {
                    json.push('"');
                    json.push_str(&escape_json_string(v));
                    json.push('"');
                }
            }
        } else {
            json.push('[');
            for (i, v) in col_values[col_idx].iter().enumerate() {
                if i > 0 {
                    json.push(',');
                }
                match v {
                    None => json.push_str("null"),
                    Some(s) => {
                        json.push('"');
                        json.push_str(&escape_json_string(s));
                        json.push('"');
                    }
                }
            }
            json.push(']');
        }
        json.push('}');
    }
    json.push('}');
    Ok(json)
}

fn format_arrow_value(arr: &dyn Array, row: usize, dt: &DataType) -> Result<Option<String>> {
    use arrow_array::*;
    match dt {
        DataType::Int8 => Ok(Some(
            arr.as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(row)
                .to_string(),
        )),
        DataType::Int16 => Ok(Some(
            arr.as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row)
                .to_string(),
        )),
        DataType::Int32 => Ok(Some(
            arr.as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row)
                .to_string(),
        )),
        DataType::Int64 => Ok(Some(
            arr.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row)
                .to_string(),
        )),
        DataType::UInt8 => Ok(Some(
            arr.as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(row)
                .to_string(),
        )),
        DataType::UInt16 => Ok(Some(
            arr.as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(row)
                .to_string(),
        )),
        DataType::UInt32 => Ok(Some(
            arr.as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row)
                .to_string(),
        )),
        DataType::UInt64 => Ok(Some(
            arr.as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row)
                .to_string(),
        )),
        DataType::Float32 => {
            let v = arr
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row);
            if v.is_finite() {
                Ok(Some(format!("{v:?}")))
            } else {
                Ok(None)
            }
        }
        DataType::Float64 => {
            let v = arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            if v.is_finite() {
                Ok(Some(format!("{v:?}")))
            } else {
                Ok(None)
            }
        }
        DataType::Utf8 => Ok(Some(
            arr.as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        )),
        DataType::LargeUtf8 => Ok(Some(
            arr.as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        )),
        DataType::Boolean => Ok(Some(
            arr.as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row)
                .to_string(),
        )),
        DataType::Date32 => {
            let days = arr
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(row) as i64;
            Ok(Some((days * 86_400_000).to_string()))
        }
        DataType::Date64 => {
            let ms = arr
                .as_any()
                .downcast_ref::<Date64Array>()
                .unwrap()
                .value(row);
            Ok(Some(ms.to_string()))
        }
        DataType::Decimal128(_, scale) => {
            let val = arr
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(row);
            Ok(Some(decimal128_to_string(val, *scale)))
        }
        DataType::Binary => {
            let bytes = arr
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(row);
            Ok(Some(bytes.iter().map(|b| format!("{b:02x}")).collect()))
        }
        DataType::LargeBinary => {
            let bytes = arr
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(row);
            Ok(Some(bytes.iter().map(|b| format!("{b:02x}")).collect()))
        }
        DataType::FixedSizeBinary(_) => {
            let bytes = arr
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(row);
            Ok(Some(bytes.iter().map(|b| format!("{b:02x}")).collect()))
        }
        DataType::BinaryView => {
            let bytes = arr
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .unwrap()
                .value(row);
            Ok(Some(bytes.iter().map(|b| format!("{b:02x}")).collect()))
        }
        DataType::Utf8View => Ok(Some(
            arr.as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .value(row)
                .to_string(),
        )),
        DataType::Time32(TimeUnit::Second) => {
            let v = arr
                .as_any()
                .downcast_ref::<Time32SecondArray>()
                .unwrap()
                .value(row) as i64;
            Ok(Some((v * 1_000_000_000).to_string()))
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let v = arr
                .as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .unwrap()
                .value(row) as i64;
            Ok(Some((v * 1_000_000).to_string()))
        }
        DataType::Time32(_) => {
            unreachable!("Time32 only supports Second and Millisecond units")
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let v = arr
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap()
                .value(row);
            Ok(Some((v * 1_000).to_string()))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let v = arr
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .unwrap()
                .value(row);
            Ok(Some(v.to_string()))
        }
        DataType::Time64(_) => {
            unreachable!("Time64 only supports Microsecond and Nanosecond units")
        }
        DataType::Timestamp(unit, _) => {
            let (v, multiplier) = match unit {
                TimeUnit::Second => (
                    arr.as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap()
                        .value(row),
                    1_000_000_000i64,
                ),
                TimeUnit::Millisecond => (
                    arr.as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .value(row),
                    1_000_000i64,
                ),
                TimeUnit::Microsecond => (
                    arr.as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .value(row),
                    1_000i64,
                ),
                TimeUnit::Nanosecond => (
                    arr.as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .value(row),
                    1i64,
                ),
            };
            Ok(Some((v as i128 * multiplier as i128).to_string()))
        }
        _ => Err(Error::with_message_and_status(
            format!("unsupported bind parameter type: {dt:?}"),
            Status::NotImplemented,
        )),
    }
}

/// Format a value for sf_core's CSV/stage binding path. Snowflake bulk DATE
/// binding uses epoch milliseconds until that representation reaches the
/// server overflow boundary, then switches to epoch nanoseconds, matching the
/// universal-driver Python serializer.
pub(crate) fn format_arrow_value_for_csv(
    arr: &dyn Array,
    row: usize,
    dt: &DataType,
) -> Result<Option<String>> {
    use arrow_array::{Date32Array, Date64Array};

    const DATE_BULK_INSERTION_MS_OVERFLOW: i128 = 31_536_000_000_000;
    let epoch_ms = match dt {
        DataType::Date32 => {
            let array = arr.as_any().downcast_ref::<Date32Array>().ok_or_else(|| {
                Error::with_message_and_status(
                    "Date32 data type is not backed by Date32Array",
                    Status::InvalidArguments,
                )
            })?;
            Some(i128::from(array.value(row)) * 86_400_000)
        }
        DataType::Date64 => {
            let array = arr.as_any().downcast_ref::<Date64Array>().ok_or_else(|| {
                Error::with_message_and_status(
                    "Date64 data type is not backed by Date64Array",
                    Status::InvalidArguments,
                )
            })?;
            Some(i128::from(array.value(row)))
        }
        _ => None,
    };

    if let Some(epoch_ms) = epoch_ms {
        return Ok(Some(
            if epoch_ms < DATE_BULK_INSERTION_MS_OVERFLOW {
                epoch_ms
            } else {
                epoch_ms * 1_000_000
            }
            .to_string(),
        ));
    }
    format_arrow_value(arr, row, dt)
}

fn decimal128_to_string(value: i128, scale: i8) -> String {
    if scale < 0 {
        return format!("{value}{}", "0".repeat(scale.unsigned_abs() as usize));
    }
    if scale == 0 {
        return value.to_string();
    }
    let sign = if value < 0 { "-" } else { "" };
    let abs = value.unsigned_abs();
    let divisor = 10u128.pow(scale as u32);
    let integer_part = abs / divisor;
    let fractional_part = abs % divisor;
    format!(
        "{sign}{integer_part}.{fractional_part:0>width$}",
        width = scale as usize
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::Statement as _;

    fn bind_metadata(
        name: &str,
        logical_type: &str,
        precision: Option<i64>,
        scale: Option<i64>,
    ) -> ColumnMetadata {
        ColumnMetadata {
            name: name.into(),
            r#type: logical_type.into(),
            precision,
            scale,
            length: None,
            byte_length: None,
            nullable: true,
            dimension: None,
            fixed: false,
            column_src_database: String::new(),
            column_src_schema: String::new(),
            column_src_table: String::new(),
            is_auto_increment: false,
            ext_col_type_name: String::new(),
            udt_output_type: String::new(),
        }
    }

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
            query_timeout_seconds: None,
            use_high_precision: true,
            timestamp_precision: TimestampPrecision::Nanoseconds,
            bound_batches: vec![],
            binding_supplied: false,
            last_query_id: None,
            prepared_parameter_schema: None,
            prepared_result_schema: None,
        }
    }

    #[test]
    fn set_sql_query_stores_query() {
        let mut stmt = make_stmt();
        stmt.set_sql_query("SELECT 1").unwrap();
        assert_eq!(stmt.query.as_deref(), Some("SELECT 1"));
    }

    #[test]
    fn query_timeout_parses_round_trips_and_clears() {
        let mut stmt = make_stmt();
        let key = OptionStatement::Other(QUERY_TIMEOUT_OPTION.into());

        stmt.set_option(key.clone(), OptionValue::String("30".into()))
            .unwrap();
        assert_eq!(stmt.execution_timeout_seconds(), Some(30));
        stmt.set_option(key.clone(), OptionValue::String("30s".into()))
            .unwrap();
        assert_eq!(stmt.execution_timeout_seconds(), Some(30));
        assert_eq!(stmt.get_option_string(key.clone()).unwrap(), "30");
        assert_eq!(stmt.get_option_int(key.clone()).unwrap(), 30);

        stmt.set_option(key.clone(), OptionValue::Int(7)).unwrap();
        assert_eq!(stmt.execution_timeout_seconds(), Some(7));

        stmt.set_option(key.clone(), OptionValue::String("0".into()))
            .unwrap();
        assert_eq!(stmt.execution_timeout_seconds(), None);
        assert_eq!(stmt.get_option_int(key).unwrap(), 0);
    }

    #[test]
    fn query_timeout_rejects_invalid_values_without_changing_state() {
        let mut stmt = make_stmt();
        let key = OptionStatement::Other(QUERY_TIMEOUT_OPTION.into());
        stmt.set_option(key.clone(), OptionValue::Int(5)).unwrap();

        for value in [
            OptionValue::Int(-1),
            OptionValue::String(String::new()),
            OptionValue::String("30ms".into()),
            OptionValue::Double(30.0),
        ] {
            let error = stmt.set_option(key.clone(), value).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            assert_eq!(stmt.execution_timeout_seconds(), Some(5));
        }
    }

    #[test]
    fn cancel_without_inflight_query_succeeds_on_valid_core_statement() {
        let mut stmt = make_stmt();
        let inner = stmt.inner.clone();
        let conn_handle = inner.sf.connection_new();
        stmt.conn_handle = conn_handle;
        stmt.stmt_handle = inner.sf.statement_new(conn_handle).unwrap();

        stmt.cancel().unwrap();

        drop(stmt);
        // Statement::drop released the statement; release the core connection.
        inner.sf.connection_release(conn_handle).unwrap();
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
    fn test_zero_row_query_binding() {
        let mut stmt = make_stmt();
        stmt.set_sql_query("SELECT ?").unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "VALUE",
            DataType::Int64,
            true,
        )]));
        stmt.prepared_result_schema = Some(schema.as_ref().clone());
        stmt.last_query_id = Some("previous-query-id".into());
        stmt.bind(RecordBatch::new_empty(Arc::new(Schema::new(vec![
            Field::new("parameter", DataType::Int64, false),
        ]))))
        .unwrap();

        let mut reader = stmt.execute().expect("zero rows should not execute SQL");
        assert_eq!(reader.schema(), schema);
        assert!(reader.next().is_none());
        assert_eq!(stmt.last_query_id, None);
    }

    #[test]
    fn execute_update_with_empty_bind_stream_returns_zero_rows() {
        let mut stmt = make_stmt();
        stmt.set_sql_query("DELETE FROM example").unwrap();

        let reader = ConcatReader {
            batches: vec![].into_iter(),
            schema: Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
        };
        stmt.bind_stream(Box::new(reader)).unwrap();

        assert_eq!(stmt.execute_update().unwrap(), Some(0));
        assert_eq!(stmt.last_query_id, None);
    }

    #[test]
    fn execute_update_with_zero_row_batch_returns_zero_rows() {
        let mut stmt = make_stmt();
        stmt.set_sql_query("DELETE FROM example WHERE id = ?")
            .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        stmt.bind(RecordBatch::new_empty(schema)).unwrap();

        assert_eq!(stmt.execute_update().unwrap(), Some(0));
        assert_eq!(stmt.last_query_id, None);
    }

    #[test]
    fn execute_update_with_zero_rows_clears_previous_query_id_without_execution() {
        let mut stmt = make_stmt();
        stmt.set_sql_query("DELETE FROM example WHERE id = ?")
            .unwrap();
        stmt.query_tag = Some("should_not_be_applied".into());
        stmt.last_query_id = Some("previous-query-id".into());

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        stmt.bind(RecordBatch::new_empty(schema)).unwrap();

        assert_eq!(stmt.execute_update().unwrap(), Some(0));
        assert_eq!(stmt.last_query_id, None);
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
            query_timeout_seconds: None,
            use_high_precision: true,
            timestamp_precision: TimestampPrecision::Nanoseconds,
            bound_batches: vec![],
            binding_supplied: false,
            last_query_id: None,
            prepared_parameter_schema: None,
            prepared_result_schema: None,
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
            query_timeout_seconds: None,
            use_high_precision: true,
            timestamp_precision: TimestampPrecision::Nanoseconds,
            bound_batches: vec![],
            binding_supplied: false,
            last_query_id: None,
            prepared_parameter_schema: None,
            prepared_result_schema: None,
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
    fn valid_prepare_bind_metadata_uses_stable_names_and_existing_conversions() {
        let schema = parameter_schema_from_prepare(
            3,
            &[
                bind_metadata("", "FIXED", Some(10), Some(2)),
                bind_metadata("named", "TIMESTAMP_NTZ", None, Some(6)),
                bind_metadata("unknown", "FUTURE_TYPE", None, None),
            ],
            true,
            TimeUnit::Nanosecond,
        )
        .unwrap();

        assert_eq!(schema.field(0).name(), "");
        assert_eq!(schema.field(0).data_type(), &DataType::Decimal128(10, 2));
        assert_eq!(schema.field(1).name(), "named");
        assert_eq!(
            schema.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(schema.field(2).name(), "unknown");
        assert_eq!(schema.field(2).data_type(), &DataType::Null);
    }

    #[test]
    fn invalid_prepare_bind_metadata_is_not_exposed() {
        let bind = bind_metadata("", "FIXED", Some(10), Some(0));
        assert!(
            parameter_schema_from_prepare(
                2,
                std::slice::from_ref(&bind),
                true,
                TimeUnit::Nanosecond
            )
            .is_none()
        );
        let mut missing_type = bind;
        missing_type.r#type.clear();
        assert!(
            parameter_schema_from_prepare(1, &[missing_type], true, TimeUnit::Nanosecond).is_none()
        );
        assert!(parameter_schema_from_prepare(-1, &[], true, TimeUnit::Nanosecond).is_none());
    }

    #[test]
    fn parameter_schema_requires_valid_prepared_metadata_and_query_reset_clears_it() {
        let mut stmt = make_stmt();
        assert_eq!(
            stmt.get_parameter_schema().unwrap_err().status,
            Status::NotImplemented
        );
        stmt.prepared_parameter_schema = Some(Schema::empty());
        assert!(stmt.get_parameter_schema().unwrap().fields().is_empty());
        stmt.set_sql_query("SELECT 1").unwrap();
        assert!(stmt.prepared_parameter_schema.is_none());
    }

    #[test]
    fn parameter_schema_cache_is_invalidated_by_conversion_and_ingest_options() {
        let cases = [
            (
                OptionStatement::Other(
                    "adbc.snowflake.sql.client_option.use_high_precision".into(),
                ),
                OptionValue::String("disabled".into()),
            ),
            (
                OptionStatement::Other(
                    "adbc.snowflake.sql.client_option.max_timestamp_precision".into(),
                ),
                OptionValue::String("microseconds".into()),
            ),
            (
                OptionStatement::TargetTable,
                OptionValue::String("target".into()),
            ),
            (
                OptionStatement::IngestMode,
                OptionValue::String("append".into()),
            ),
            (
                OptionStatement::TargetCatalog,
                OptionValue::String("catalog".into()),
            ),
            (
                OptionStatement::TargetDbSchema,
                OptionValue::String("schema".into()),
            ),
            (
                OptionStatement::Temporary,
                OptionValue::String("true".into()),
            ),
        ];

        for (option, value) in cases {
            let mut stmt = make_stmt();
            stmt.prepared_parameter_schema = Some(Schema::empty());
            stmt.set_option(option, value).unwrap();
            assert!(stmt.prepared_parameter_schema.is_none());
        }
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
        let inner = stmt.inner.clone();
        let conn_handle = stmt.inner.sf.connection_new();
        stmt.conn_handle = conn_handle;
        stmt.stmt_handle = stmt.inner.sf.statement_new(conn_handle).unwrap();

        stmt.set_sql_query("SELECT 1").unwrap();
        stmt.set_option(
            OptionStatement::Other("adbc.snowflake.statement.query_tag".into()),
            OptionValue::String("my_tag".into()),
        )
        .unwrap();
        stmt.set_sql_query("SELECT 2").unwrap();
        assert_eq!(stmt.query.as_deref(), Some("SELECT 2"));
        assert_eq!(
            stmt.get_option_string(OptionStatement::Other(
                "adbc.snowflake.statement.query_tag".into()
            ))
            .unwrap(),
            "my_tag"
        );

        drop(stmt);
        // Statement::drop releases the statement before its connection.
        inner.sf.connection_release(conn_handle).unwrap();
    }

    #[test]
    fn rejected_query_tag_does_not_update_adbc_storage() {
        let mut stmt = make_stmt();
        let error = stmt
            .set_option(
                OptionStatement::Other("adbc.snowflake.statement.query_tag".into()),
                OptionValue::String("not_stored".into()),
            )
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert_eq!(stmt.query_tag, None);
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
        let mut cr = ConvertingReader::new(
            reader,
            true,
            TimeUnit::Nanosecond,
            TimestampPrecision::Nanoseconds,
        );
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
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        let reader = ConcatReader {
            batches: vec![batch].into_iter(),
            schema,
        };
        let mut cr = ConvertingReader::new(
            reader,
            true,
            TimeUnit::Nanosecond,
            TimestampPrecision::Nanoseconds,
        );
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
        let mut cr = ConvertingReader::new(
            reader,
            true,
            TimeUnit::Nanosecond,
            TimestampPrecision::Nanoseconds,
        );
        let out = cr.next().unwrap().unwrap();
        assert_eq!(out.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(out.num_rows(), 0);
    }

    #[test]
    fn test_converting_reader_multiple_batches_different_widths() {
        use arrow_array::{Int8Array, Int32Array};
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

        let declared_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));

        let schema_i8 = Arc::new(Schema::new(vec![Field::new("v", DataType::Int8, false)]));
        let schema_i32 = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch1 =
            RecordBatch::try_new(schema_i8, vec![Arc::new(Int8Array::from(vec![1i8, 2]))]).unwrap();
        let batch2 = RecordBatch::try_new(
            schema_i32,
            vec![Arc::new(Int32Array::from(vec![100i32, 200]))],
        )
        .unwrap();

        let reader = TwoBatchReader {
            batches: vec![batch1, batch2].into_iter(),
            schema: declared_schema,
        };
        let mut cr = ConvertingReader::new(
            reader,
            true,
            TimeUnit::Nanosecond,
            TimestampPrecision::Nanoseconds,
        );

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
        make_field_with_precision(name, dt, logical_type, scale, "38")
    }

    fn make_field_with_precision(
        name: &str,
        dt: DataType,
        logical_type: &str,
        scale: &str,
        precision: &str,
    ) -> Field {
        let mut md = std::collections::HashMap::new();
        md.insert("logicalType".to_string(), logical_type.to_string());
        md.insert("scale".to_string(), scale.to_string());
        md.insert("precision".to_string(), precision.to_string());
        Field::new(name, dt, true).with_metadata(md)
    }

    #[test]
    fn test_adjust_schema_time_scale3_is_time32_millisecond() {
        let f = make_field_with_meta("t", DataType::Int32, "TIME", "3");
        let schema = Schema::new(vec![f]);
        let result = adjust_schema(&schema, false, TimeUnit::Nanosecond);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Time32(TimeUnit::Millisecond)
        );
    }

    #[test]
    fn test_adjust_schema_time_scale9_is_time64_nanosecond() {
        let f = make_field_with_meta("t", DataType::Int64, "TIME", "9");
        let schema = Schema::new(vec![f]);
        let result = adjust_schema(&schema, false, TimeUnit::Nanosecond);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Time64(TimeUnit::Nanosecond)
        );
    }

    #[test]
    fn test_adjust_schema_fixed_scale2_is_float64() {
        let f = make_field_with_meta("x", DataType::Int64, "FIXED", "2");
        let schema = Schema::new(vec![f]);
        let result = adjust_schema(&schema, false, TimeUnit::Nanosecond);
        assert_eq!(result.field(0).data_type(), &DataType::Float64);
    }

    #[test]
    fn test_adjust_schema_fixed_scale2_high_precision_is_decimal128() {
        let f = make_field_with_precision("x", DataType::Int64, "FIXED", "2", "10");
        let schema = Schema::new(vec![f]);
        let result = adjust_schema(&schema, true, TimeUnit::Nanosecond);
        assert_eq!(result.field(0).data_type(), &DataType::Decimal128(10, 2));
    }

    #[test]
    fn test_adjust_schema_timestamp_ntz_int64_to_timestamp_ns() {
        let f = make_field_with_meta("ts", DataType::Int64, "TIMESTAMP_NTZ", "9");
        let schema = Schema::new(vec![f]);
        let result = adjust_schema(&schema, false, TimeUnit::Nanosecond);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
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
        let reader = ConcatReader {
            batches: vec![batch].into_iter(),
            schema,
        };
        let mut cr = ConvertingReader::new(
            reader,
            false,
            TimeUnit::Nanosecond,
            TimestampPrecision::Nanoseconds,
        );
        let out = cr.next().unwrap().unwrap();
        assert_eq!(out.schema().field(0).data_type(), &DataType::Float64);
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        assert!((col.value(0) - 123.45).abs() < 1e-9);
        assert!((col.value(1) - 2.55).abs() < 1e-9);
    }

    #[test]
    fn test_converting_reader_fixed_scale2_high_precision_produces_decimal128() {
        let f = make_field_with_precision("x", DataType::Int64, "FIXED", "2", "10");
        let schema = Arc::new(Schema::new(vec![f]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![
                12345i64, -255,
            ]))],
        )
        .unwrap();
        let reader = ConcatReader {
            batches: vec![batch].into_iter(),
            schema,
        };
        let mut cr = ConvertingReader::new(
            reader,
            true,
            TimeUnit::Nanosecond,
            TimestampPrecision::Nanoseconds,
        );
        let out = cr.next().unwrap().unwrap();
        assert_eq!(
            out.schema().field(0).data_type(),
            &DataType::Decimal128(10, 2)
        );
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        // 12345 with scale 2 = 123.45
        assert_eq!(col.value(0), 12345i128);
        // -255 with scale 2 = -2.55
        assert_eq!(col.value(1), -255i128);
    }

    #[test]
    fn test_converting_reader_fixed_scale0_high_precision_stays_int64() {
        let f = make_field_with_precision("x", DataType::Int64, "FIXED", "0", "10");
        let schema = Arc::new(Schema::new(vec![f]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![42i64]))],
        )
        .unwrap();
        let reader = ConcatReader {
            batches: vec![batch].into_iter(),
            schema,
        };
        let mut cr = ConvertingReader::new(
            reader,
            true,
            TimeUnit::Nanosecond,
            TimestampPrecision::Nanoseconds,
        );
        let out = cr.next().unwrap().unwrap();
        assert_eq!(out.schema().field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn test_converting_reader_decimal128_source_high_precision_identity() {
        let f = make_field_with_precision("x", DataType::Decimal128(10, 2), "FIXED", "2", "10");
        let schema = Arc::new(Schema::new(vec![f]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                arrow_array::Decimal128Array::from(vec![12345i128, -255])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            )],
        )
        .unwrap();
        let reader = ConcatReader {
            batches: vec![batch].into_iter(),
            schema,
        };
        let mut cr = ConvertingReader::new(
            reader,
            true,
            TimeUnit::Nanosecond,
            TimestampPrecision::Nanoseconds,
        );
        let out = cr.next().unwrap().unwrap();
        assert_eq!(
            out.schema().field(0).data_type(),
            &DataType::Decimal128(10, 2)
        );
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert_eq!(col.value(0), 12345i128);
        assert_eq!(col.value(1), -255i128);
    }

    #[test]
    fn test_converting_reader_decimal128_source_no_high_precision_casts_to_float64() {
        let f = make_field_with_precision("x", DataType::Decimal128(10, 2), "FIXED", "2", "10");
        let schema = Arc::new(Schema::new(vec![f]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                arrow_array::Decimal128Array::from(vec![12345i128])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            )],
        )
        .unwrap();
        let reader = ConcatReader {
            batches: vec![batch].into_iter(),
            schema,
        };
        let mut cr = ConvertingReader::new(
            reader,
            false,
            TimeUnit::Nanosecond,
            TimestampPrecision::Nanoseconds,
        );
        let out = cr.next().unwrap().unwrap();
        assert_eq!(out.schema().field(0).data_type(), &DataType::Float64);
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        assert!((col.value(0) - 123.45).abs() < 1e-9);
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
        let reader = ConcatReader {
            batches: vec![batch].into_iter(),
            schema,
        };
        let mut cr = ConvertingReader::new(
            reader,
            false,
            TimeUnit::Nanosecond,
            TimestampPrecision::Nanoseconds,
        );
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
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_build_timestamp_tz_2field_year9999() {
        use arrow_array::{Int32Array, Int64Array, StructArray};
        use arrow_schema::Field as SchemaField;

        let epoch_us: i64 = 253402300799000000; // 9999-12-31T23:59:59Z in microseconds
        let epoch_arr = Int64Array::from(vec![epoch_us]);
        let tz_arr = Int32Array::from(vec![1440i32]); // UTC

        let fields = vec![
            Arc::new(SchemaField::new("epoch", DataType::Int64, false)),
            Arc::new(SchemaField::new("tz", DataType::Int32, false)),
        ];
        let struct_arr = StructArray::try_new(
            fields.into(),
            vec![
                Arc::new(epoch_arr) as ArrayRef,
                Arc::new(tz_arr) as ArrayRef,
            ],
            None,
        )
        .unwrap();

        let epoch_col = struct_arr
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let tz_col = struct_arr
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        let result = build_timestamp_tz_2field(
            epoch_col,
            tz_col,
            &struct_arr,
            6,
            false,
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
        )
        .unwrap();

        let ts = result
            .as_any()
            .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(
            ts.value(0),
            epoch_us,
            "year 9999 should round-trip as microseconds"
        );
    }

    #[test]
    fn test_build_timestamp_from_epoch_fraction_negative_scale_clamps() {
        let epoch = arrow_array::Int64Array::from(vec![1i64]);
        let fraction = arrow_array::Int32Array::from(vec![5i32]);
        let fields = [
            Field::new("epoch", DataType::Int64, true),
            Field::new("fraction", DataType::Int32, true),
        ];
        let struct_arr = arrow_array::StructArray::from(vec![
            (
                Arc::new(fields[0].clone()),
                Arc::new(epoch.clone()) as ArrayRef,
            ),
            (
                Arc::new(fields[1].clone()),
                Arc::new(fraction.clone()) as ArrayRef,
            ),
        ]);
        let result = build_timestamp_from_epoch_fraction(
            &epoch,
            &fraction,
            &struct_arr,
            -1,
            false,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .expect("should not panic");
        let ts = result
            .as_any()
            .downcast_ref::<arrow_array::TimestampNanosecondArray>()
            .unwrap();
        // scale -1 clamps to 0. frac_to_ns = 10^9.
        // ns = 1 * 10^9 + 5 * 10^9 = 6000000000.
        assert_eq!(ts.value(0), 6_000_000_000);
    }

    #[test]
    fn test_build_timestamp_from_epoch_fraction_oversized_scale_clamps() {
        let epoch = arrow_array::Int64Array::from(vec![1i64]);
        let fraction = arrow_array::Int32Array::from(vec![5i32]);
        let fields = [
            Field::new("epoch", DataType::Int64, true),
            Field::new("fraction", DataType::Int32, true),
        ];
        let struct_arr = arrow_array::StructArray::from(vec![
            (
                Arc::new(fields[0].clone()),
                Arc::new(epoch.clone()) as ArrayRef,
            ),
            (
                Arc::new(fields[1].clone()),
                Arc::new(fraction.clone()) as ArrayRef,
            ),
        ]);
        let result = build_timestamp_from_epoch_fraction(
            &epoch,
            &fraction,
            &struct_arr,
            10,
            false,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .expect("should not panic");
        let ts = result
            .as_any()
            .downcast_ref::<arrow_array::TimestampNanosecondArray>()
            .unwrap();
        // scale 10 clamps to 9. frac_to_ns = 10^0 = 1.
        // ns = 1 * 10^9 + 5 * 1 = 1000000005.
        assert_eq!(ts.value(0), 1_000_000_005);
    }

    #[test]
    fn test_build_timestamp_tz_3field_negative_scale_clamps() {
        let epoch = arrow_array::Int64Array::from(vec![1i64]);
        let fraction = arrow_array::Int32Array::from(vec![5i32]);
        let tzoffset = arrow_array::Int32Array::from(vec![1440i32]);
        let fields = [
            Field::new("epoch", DataType::Int64, true),
            Field::new("fraction", DataType::Int32, true),
            Field::new("tzoffset", DataType::Int32, true),
        ];
        let struct_arr = arrow_array::StructArray::from(vec![
            (
                Arc::new(fields[0].clone()),
                Arc::new(epoch.clone()) as ArrayRef,
            ),
            (
                Arc::new(fields[1].clone()),
                Arc::new(fraction.clone()) as ArrayRef,
            ),
            (
                Arc::new(fields[2].clone()),
                Arc::new(tzoffset.clone()) as ArrayRef,
            ),
        ]);
        let result = build_timestamp_tz_3field(
            &epoch,
            &fraction,
            &tzoffset,
            &struct_arr,
            -1,
            false,
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
        )
        .expect("should not panic");
        let ts = result
            .as_any()
            .downcast_ref::<arrow_array::TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 6_000_000_000);
    }

    #[test]
    fn test_build_timestamp_tz_3field_oversized_scale_clamps() {
        let epoch = arrow_array::Int64Array::from(vec![1i64]);
        let fraction = arrow_array::Int32Array::from(vec![5i32]);
        let tzoffset = arrow_array::Int32Array::from(vec![1440i32]);
        let fields = [
            Field::new("epoch", DataType::Int64, true),
            Field::new("fraction", DataType::Int32, true),
            Field::new("tzoffset", DataType::Int32, true),
        ];
        let struct_arr = arrow_array::StructArray::from(vec![
            (
                Arc::new(fields[0].clone()),
                Arc::new(epoch.clone()) as ArrayRef,
            ),
            (
                Arc::new(fields[1].clone()),
                Arc::new(fraction.clone()) as ArrayRef,
            ),
            (
                Arc::new(fields[2].clone()),
                Arc::new(tzoffset.clone()) as ArrayRef,
            ),
        ]);
        let result = build_timestamp_tz_3field(
            &epoch,
            &fraction,
            &tzoffset,
            &struct_arr,
            10,
            false,
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
        )
        .expect("should not panic");
        let ts = result
            .as_any()
            .downcast_ref::<arrow_array::TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_000_000_005);
    }

    #[test]
    fn test_arrow_batches_to_json_bindings_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![123, 456])) as ArrayRef,
                Arc::new(arrow_array::StringArray::from(vec!["hello", "world"])) as ArrayRef,
            ],
        )
        .unwrap();

        let json = arrow_batches_to_json_bindings(&[batch]).unwrap();
        assert!(json.contains("\"1\":{\"type\":\"FIXED\",\"value\":[\"123\",\"456\"]}"));
        assert!(json.contains("\"2\":{\"type\":\"TEXT\",\"value\":[\"hello\",\"world\"]}"));
    }

    #[test]
    fn test_single_row_query_binding() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int64Array::from(vec![7])) as ArrayRef],
        )
        .unwrap();
        let result_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let mut executions = 0;

        let mut reader = execute_bound_rows(&[batch], Arc::new(Schema::empty()), |row| {
            executions += 1;
            let value = row
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .value(0);
            let output = RecordBatch::try_new(
                result_schema.clone(),
                vec![Arc::new(arrow_array::Int64Array::from(vec![value]))],
            )
            .unwrap();
            Ok(Box::new(ConcatReader {
                batches: vec![output].into_iter(),
                schema: result_schema.clone(),
            }))
        })
        .unwrap();

        assert_eq!(executions, 1);
        let output = reader.next().unwrap().unwrap();
        assert_eq!(
            output
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .value(0),
            7
        );
        assert!(reader.next().is_none());
    }

    #[test]
    fn test_multi_row_query_binding() {
        let parameter_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batches = vec![
            RecordBatch::try_new(
                parameter_schema.clone(),
                vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2]))],
            )
            .unwrap(),
            RecordBatch::try_new(
                parameter_schema,
                vec![Arc::new(arrow_array::Int64Array::from(vec![3]))],
            )
            .unwrap(),
        ];
        let result_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let mut executed_values = Vec::new();

        let mut reader = execute_bound_rows(&batches, Arc::new(Schema::empty()), |row| {
            let value = row
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .value(0);
            executed_values.push(value);
            let output = RecordBatch::try_new(
                result_schema.clone(),
                vec![Arc::new(arrow_array::Int64Array::from(vec![value * 10]))],
            )
            .unwrap();
            Ok(Box::new(ConcatReader {
                batches: vec![output].into_iter(),
                schema: result_schema.clone(),
            }))
        })
        .unwrap();

        assert_eq!(executed_values, vec![1, 2, 3]);
        let values = reader
            .by_ref()
            .map(|batch| {
                batch
                    .unwrap()
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .value(0)
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![10, 20, 30]);
    }

    #[test]
    fn test_bound_query_schema_mismatch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        let mut execution = 0;

        let error = match execute_bound_rows(&[batch], Arc::new(Schema::empty()), |_| {
            execution += 1;
            let output_schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                if execution == 1 {
                    DataType::Int64
                } else {
                    DataType::Utf8
                },
                false,
            )]));
            let output = RecordBatch::new_empty(output_schema.clone());
            Ok(Box::new(ConcatReader {
                batches: vec![output].into_iter(),
                schema: output_schema,
            }))
        }) {
            Ok(_) => panic!("incompatible result schemas should fail"),
            Err(error) => error,
        };

        assert_eq!(error.status, Status::InvalidData);
        assert!(error.message.contains("schema mismatch"));
    }

    #[test]
    fn test_bound_query_stops_after_execution_failure() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut executions = 0;

        let error = match execute_bound_rows(&[batch], Arc::new(Schema::empty()), |_| {
            executions += 1;
            if executions == 2 {
                return Err(Error::with_message_and_status(
                    "injected execution failure",
                    Status::InvalidData,
                ));
            }
            Ok(Box::new(ConcatReader {
                batches: Vec::new().into_iter(),
                schema: schema.clone(),
            }))
        }) {
            Ok(_) => panic!("execution failure should be returned"),
            Err(error) => error,
        };

        assert_eq!(executions, 2);
        assert_eq!(error.status, Status::InvalidData);
        assert!(error.message.contains("injected execution failure"));
    }

    #[test]
    fn test_arrow_batches_to_json_bindings_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
                Arc::new(arrow_array::StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    None,
                ])) as ArrayRef,
            ],
        )
        .unwrap();

        let json = arrow_batches_to_json_bindings(&[batch]).unwrap();
        assert!(json.contains("\"1\":{\"type\":\"FIXED\",\"value\":[\"1\",null,\"3\"]}"));
        assert!(json.contains("\"2\":{\"type\":\"TEXT\",\"value\":[\"a\",\"b\",null]}"));
    }

    #[test]
    fn test_arrow_batches_to_json_bindings_float_precision() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Float64Array::from(vec![std::f64::consts::PI])) as ArrayRef],
        )
        .unwrap();

        let json = arrow_batches_to_json_bindings(&[batch]).unwrap();
        assert!(
            json.contains("3.141592653589793"),
            "should preserve full f64 precision, got: {json}"
        );
    }

    #[test]
    fn test_decimal128_to_string_negative_fractional() {
        // Values in (-1, 0) must preserve the negative sign
        assert_eq!(decimal128_to_string(-50, 2), "-0.50");
        assert_eq!(decimal128_to_string(-1, 1), "-0.1");
        assert_eq!(decimal128_to_string(-999, 3), "-0.999");
        // Normal negative values
        assert_eq!(decimal128_to_string(-150, 2), "-1.50");
        // Positive values unchanged
        assert_eq!(decimal128_to_string(50, 2), "0.50");
        assert_eq!(decimal128_to_string(150, 2), "1.50");
        // Zero
        assert_eq!(decimal128_to_string(0, 2), "0.00");
        // Zero and negative scales
        assert_eq!(decimal128_to_string(-42, 0), "-42");
        assert_eq!(decimal128_to_string(123, -2), "12300");
        assert_eq!(decimal128_to_string(-123, -2), "-12300");
    }
}
