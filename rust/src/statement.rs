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
use std::{
    collections::VecDeque,
    fmt::Write as _,
    sync::{Arc, Mutex},
};

use adbc_core::{
    Optionable, PartitionedResult,
    error::{Error, Result},
    options::{OptionStatement, OptionValue},
};
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchReader};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use driverbase::{bulk_ingest::BulkIngestState, error::ErrorHelper};
use sf_core::apis::database_driver_v1::{BindingType, ColumnMetadata, DataPtr, Handle};
use sf_core::apis::operation_ctx::OperationCtx;
use sf_core::chunks::DEFAULT_PREFETCH_THREADS;
use sf_core::config::{param_registry::param_names, settings::Setting};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::driver::{Inner, OperationCancelHandle, QueryResult, TimestampPrecision};
use crate::error::SnowflakeError;

/// Per-statement Snowflake query timeout, in seconds. A value of zero clears
/// the override and lets the session/core default apply.
const QUERY_TIMEOUT_OPTION: &str = "adbc.snowflake.statement.query_timeout";

fn parse_query_timeout(value: &OptionValue) -> Result<Option<u32>> {
    let seconds = match value {
        OptionValue::Int(value) => u32::try_from(*value).map_err(|_| {
            Error::from(
                SnowflakeError::invalid_argument()
                    .message("query_timeout must be between 0 and 4294967295 seconds"),
            )
        })?,
        OptionValue::String(value) => {
            let raw = value.strip_suffix('s').unwrap_or(value);
            raw.parse::<u32>().map_err(|_| {
                Error::from(
                    SnowflakeError::invalid_argument()
                        .format(format_args!("invalid query_timeout value: {value}")),
                )
            })?
        }
        _ => {
            return Err(SnowflakeError::invalid_argument()
                .message("query_timeout must be a string or int")
                .into());
        }
    };
    Ok((seconds != 0).then_some(seconds))
}

/// Go's statement options for result fetching, mapped to sf_core's
/// per-statement `CLIENT_PREFETCH_THREADS` and `CLIENT_RESULT_QUEUE_SIZE`.
const PREFETCH_CONCURRENCY_OPTION: &str = "adbc.snowflake.rpc.prefetch_concurrency";
const RESULT_QUEUE_SIZE_OPTION: &str = "adbc.rpc.result_queue_size";

fn parse_positive_int(key: &str, value: &OptionValue) -> Result<i64> {
    let number = match value {
        OptionValue::Int(value) => Some(*value),
        OptionValue::String(value) => value.trim().parse::<i64>().ok(),
        _ => {
            return Err(SnowflakeError::set_invalid_option(&key, value)
                .message("must be a string or int")
                .into());
        }
    };
    match number {
        Some(number) if number > 0 => Ok(number),
        _ => Err(SnowflakeError::set_invalid_option(&key, value)
            .message("must be a positive integer")
            .into()),
    }
}

/// Data supplied by bind()/bind_stream(). Empty means nothing is bound.
pub(crate) enum Bound {
    Empty,
    /// bind(), or a stream materialized for parameter binding.
    Batches(Vec<RecordBatch>),
    /// bind_stream() not yet consumed; bulk ingest streams without materializing.
    Stream(Box<dyn RecordBatchReader + Send>),
}

pub struct Statement {
    pub(crate) inner: Arc<Inner>,
    pub(crate) stmt_handle: Handle,
    pub(crate) conn_handle: Handle,
    pub(crate) trace_parent: Option<String>,
    pub(crate) connection_trace_parent: Option<String>,
    pub(crate) query: Option<String>,
    pub(crate) ingest_state: BulkIngestState<SnowflakeError>,
    pub(crate) query_tag: Option<String>,
    pub(crate) query_timeout_seconds: Option<u32>,
    pub(crate) prefetch_concurrency: Option<i64>,
    pub(crate) result_queue_size: Option<i64>,
    pub(crate) timestamp_precision: TimestampPrecision,
    pub(crate) ingest_options: crate::ingest::IngestOptions,
    pub(crate) bound: Bound,
    pub(crate) last_query_id: Option<String>,
    /// Set by `prepare()`. Like Go's no-op Prepare this costs no round trip;
    /// the describe runs on first need and is cached in `described`.
    pub(crate) prepared: bool,
    pub(crate) described: Mutex<Option<Arc<DescribedSchemas>>>,
    /// The most recently started sf_core operation. A retained cancellation
    /// handle can use this slot while an execute borrows the statement.
    /// Cancelling a finished operation is a no-op.
    pub(crate) operation: Arc<Mutex<Arc<OperationCtx>>>,
}

impl Drop for Statement {
    fn drop(&mut self) {
        let inner = Arc::clone(&self.inner);
        let parent = self.effective_trace_parent();
        let _ = inner.traced("statement.Close", parent, |_| {
            inner
                .sf
                .statement_release(self.stmt_handle)
                .map_err(crate::error::api_error_to_adbc_error)
        });
    }
}

impl Optionable for Statement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key {
            key @ OptionStatement::TargetTable => {
                self.ingest_state
                    .set_option(&key, &value)
                    .map_err(Error::from)?;
                self.query = None;
                self.unprepare();
                Ok(())
            }
            key @ (OptionStatement::IngestMode
            | OptionStatement::TargetCatalog
            | OptionStatement::TargetDbSchema) => {
                self.ingest_state
                    .set_option(&key, &value)
                    .map_err(Error::from)?;
                self.invalidate_described();
                Ok(())
            }
            OptionStatement::Other(ref k)
                if k == "adbc.snowflake.sql.client_option.max_timestamp_precision" =>
            {
                let spelling = SnowflakeError::option_as_string(k, &value)?;
                let precision = match spelling {
                    "nanoseconds" => TimestampPrecision::Nanoseconds,
                    "microseconds" => TimestampPrecision::Microseconds,
                    "nanoseconds_error_on_overflow" => {
                        TimestampPrecision::NanosecondsErrorOnOverflow
                    }
                    _ => {
                        return Err(SnowflakeError::invalid_argument()
                            .format(format_args!(
                                "Invalid value for statement option '{k}': '{spelling}'"
                            ))
                            .into());
                    }
                };
                self.timestamp_precision = precision;
                self.invalidate_described();
                Ok(())
            }
            OptionStatement::Temporary => match value {
                OptionValue::String(value) => match value.as_str() {
                    "false" | "0" | "disabled" => Ok(()),
                    "true" | "1" | "enabled" => Err(SnowflakeError::not_implemented()
                        .message("temporary ingestion is not supported")
                        .into()),
                    _ => Err(SnowflakeError::invalid_argument()
                        .message("temporary ingestion must be enabled or disabled")
                        .into()),
                },
                _ => Err(SnowflakeError::invalid_argument()
                    .message("temporary ingestion must be a string")
                    .into()),
            },
            OptionStatement::Other(k) if k == driverbase::telemetry::TRACE_PARENT => {
                let parent = SnowflakeError::option_as_string(&k, &value)?;
                self.trace_parent = driverbase::telemetry::validate_trace_parent(parent)?;
                Ok(())
            }
            OptionStatement::Other(ref k) if k == QUERY_TIMEOUT_OPTION => {
                self.query_timeout_seconds = parse_query_timeout(&value)?;
                Ok(())
            }
            OptionStatement::Other(ref k) if k == "adbc.snowflake.statement.query_tag" => {
                if let OptionValue::String(s) = value {
                    self.inner
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
                    Err(SnowflakeError::set_invalid_option(k, &value)
                        .message("query_tag must be a string")
                        .into())
                }
            }
            OptionStatement::Other(ref k)
                if k == PREFETCH_CONCURRENCY_OPTION || k == RESULT_QUEUE_SIZE_OPTION =>
            {
                let value = parse_positive_int(k, &value)?;
                let prefetch = k == PREFETCH_CONCURRENCY_OPTION;
                let param = if prefetch {
                    param_names::CLIENT_PREFETCH_THREADS
                } else {
                    param_names::CLIENT_RESULT_QUEUE_SIZE
                };
                self.inner
                    .block_on(self.inner.sf.statement_set_option(
                        self.stmt_handle,
                        param.into(),
                        Setting::Int(value),
                    ))
                    .map_err(crate::error::api_error_to_adbc_error)?;
                if prefetch {
                    self.prefetch_concurrency = Some(value);
                } else {
                    self.result_queue_size = Some(value);
                }
                Ok(())
            }
            OptionStatement::Other(ref k) if self.ingest_options.set(k, &value)? => Ok(()),
            _ => Err(SnowflakeError::set_unknown_option(&key.as_ref()).into()),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let key_str = key.as_ref();
        match &key {
            OptionStatement::Other(k) if k == driverbase::telemetry::TRACE_PARENT => {
                Ok(self.effective_trace_parent().unwrap_or_default().to_owned())
            }
            OptionStatement::Other(k) if k == QUERY_TIMEOUT_OPTION => {
                Ok(self.query_timeout_seconds.unwrap_or(0).to_string())
            }
            OptionStatement::Other(k) if k == "adbc.snowflake.statement.query_tag" => {
                Ok(self.query_tag.clone().unwrap_or_default())
            }
            OptionStatement::Other(k) if k == "adbc.snowflake.sql.query_id" => {
                self.last_query_id.clone().ok_or_else(|| {
                    Error::from(
                        SnowflakeError::not_found().message("no query has been executed yet"),
                    )
                })
            }
            OptionStatement::Other(k)
                if k == PREFETCH_CONCURRENCY_OPTION || k == RESULT_QUEUE_SIZE_OPTION =>
            {
                self.result_tuning_value(k).map(|value| value.to_string())
            }
            OptionStatement::Other(k)
                if k == "adbc.snowflake.sql.client_option.max_timestamp_precision" =>
            {
                Ok(match self.timestamp_precision {
                    TimestampPrecision::Nanoseconds => "nanoseconds",
                    TimestampPrecision::Microseconds => "microseconds",
                    TimestampPrecision::NanosecondsErrorOnOverflow => {
                        "nanoseconds_error_on_overflow"
                    }
                }
                .into())
            }
            OptionStatement::Other(k) => self
                .ingest_options
                .get(k)
                .ok_or_else(|| SnowflakeError::get_unknown_option(&key_str).into()),
            _ => Err(SnowflakeError::get_unknown_option(&key_str).into()),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        let key_str = key.as_ref();
        Err(SnowflakeError::get_unknown_option(&key_str).into())
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        let key_str = key.as_ref();
        match &key {
            OptionStatement::Other(k) if k == QUERY_TIMEOUT_OPTION => {
                Ok(i64::from(self.query_timeout_seconds.unwrap_or(0)))
            }
            OptionStatement::Other(k)
                if k == PREFETCH_CONCURRENCY_OPTION || k == RESULT_QUEUE_SIZE_OPTION =>
            {
                self.result_tuning_value(k)
            }
            OptionStatement::Other(k) => self
                .ingest_options
                .get(k)
                .and_then(|value| value.parse::<i64>().ok())
                .ok_or_else(|| SnowflakeError::get_unknown_option(&key_str).into()),
            _ => Err(SnowflakeError::get_unknown_option(&key_str).into()),
        }
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        let key_str = key.as_ref();
        Err(SnowflakeError::get_unknown_option(&key_str).into())
    }
}

/// Result of Snowflake's describe-only prepare for the current query, valid
/// until the query or a conversion/ingest option changes.
#[derive(Debug)]
pub(crate) struct DescribedSchemas {
    query_id: String,
    /// Present only when sf_core's prepare bind count and metadata agree.
    parameters: Option<Schema>,
    result: Schema,
}

impl Statement {
    fn unprepare(&mut self) {
        self.prepared = false;
        self.invalidate_described();
    }

    fn invalidate_described(&mut self) {
        *self.described.get_mut().unwrap_or_else(|e| e.into_inner()) = None;
    }

    /// Describe the current query once, reusing the cached result.
    fn describe(&self, operation: &OperationCtx) -> Result<Arc<DescribedSchemas>> {
        let mut described = self.described.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(described) = described.as_ref() {
            return Ok(Arc::clone(described));
        }
        let query = self.query.clone().ok_or_else(|| {
            Error::from(SnowflakeError::invalid_state().message("cannot execute without a query"))
        })?;
        let prepared = self
            .inner
            .prepare_statement(Some(operation), self.stmt_handle, query)?;
        let timezone = self.inner.session_timezone(self.conn_handle)?;
        let schemas = Arc::new(DescribedSchemas {
            parameters: parameter_schema_from_prepare(
                prepared.number_of_binds,
                &prepared.binds,
                self.timestamp_precision.time_unit(),
                &timezone,
            ),
            result: adjust_schema(
                &prepared.reader.schema(),
                self.timestamp_precision.time_unit(),
                &timezone,
                None,
            )
            .as_ref()
            .clone(),
            query_id: prepared.query_id,
        });
        *described = Some(Arc::clone(&schemas));
        Ok(schemas)
    }

    /// Parameter binding requires random access; ingest consumes the stream directly.
    /// Take ownership before executing so failures cannot leave stale parameters bound.
    fn take_bound_batches(&mut self) -> Result<Vec<RecordBatch>> {
        match std::mem::replace(&mut self.bound, Bound::Empty) {
            Bound::Batches(batches) => Ok(batches),
            Bound::Stream(reader) => reader
                .map(|batch| {
                    batch.map_err(|e| Error::from(SnowflakeError::io().format(format_args!("{e}"))))
                })
                .collect(),
            Bound::Empty => unreachable!("take_bound_batches requires supplied bindings"),
        }
    }

    fn execution_timeout_seconds(&self) -> Option<u32> {
        self.query_timeout_seconds
    }

    /// The value in effect for a result fetch tuning option. An unset prefetch
    /// concurrency reports the session's value.
    fn result_tuning_value(&self, key: &str) -> Result<i64> {
        if key == PREFETCH_CONCURRENCY_OPTION {
            return match self.prefetch_concurrency {
                Some(value) => Ok(value),
                None => self.session_prefetch_threads(),
            };
        }
        self.result_queue_size.ok_or_else(|| {
            SnowflakeError::not_found()
                .message("result_queue_size is not set")
                .into()
        })
    }

    fn session_prefetch_threads(&self) -> Result<i64> {
        let setting = self
            .inner
            .block_on(self.inner.sf.connection_get_parameter(
                self.conn_handle,
                param_names::CLIENT_PREFETCH_THREADS.into(),
            ))
            .map_err(crate::error::api_error_to_adbc_error)?;
        Ok(match setting {
            Some(Setting::Int(value)) => value,
            Some(Setting::String(value)) => {
                value.parse().unwrap_or(DEFAULT_PREFETCH_THREADS as i64)
            }
            _ => DEFAULT_PREFETCH_THREADS as i64,
        })
    }

    fn effective_trace_parent(&self) -> Option<&str> {
        self.trace_parent
            .as_deref()
            .or(self.connection_trace_parent.as_deref())
    }

    fn trace_parent_for_span(&self) -> Option<String> {
        if self.inner.telemetry.is_enabled() {
            self.effective_trace_parent().map(str::to_owned)
        } else {
            None
        }
    }

    /// Start a cancellable sf_core operation and publish it for the cancel handle.
    /// Each operation needs a fresh context because cancellation is one-shot.
    pub(crate) fn begin_operation(&self) -> Arc<OperationCtx> {
        let operation = Arc::new(OperationCtx::with_own_token());
        *self.operation.lock().unwrap_or_else(|e| e.into_inner()) = Arc::clone(&operation);
        operation
    }

    /// Route every user-statement execution through the statement timeout.
    /// The binding owner must remain alive until this synchronous call returns.
    pub(crate) fn execute_query<'a>(
        &self,
        operation: &OperationCtx,
        query: String,
        bindings: Option<BindingType<'a>>,
    ) -> Result<QueryResult> {
        self.inner.execute_query_with_timeout(
            Some(operation),
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
        returned_rows: &mut i64,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let batches = self.take_bound_batches()?;
        let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let operation = self.begin_operation();
        *returned_rows = 0;
        if row_count == 0 {
            // ADBC defines this as zero executions. Describe-only prepare provides
            // the result schema without accidentally running the SQL unbound.
            self.last_query_id = None;
            let schema = Arc::new(self.describe(&operation)?.result.clone());
            return execute_bound_rows(&batches, schema, |_| {
                unreachable!("zero-row bindings must not execute")
            });
        }

        let inner = self.inner.clone();
        let stmt_handle = self.stmt_handle;
        let conn_handle = self.conn_handle;
        let timeout_seconds = self.execution_timeout_seconds();
        let timestamp_precision = self.timestamp_precision;
        let mut last_query_id = self.last_query_id.clone();
        let result = execute_bound_rows(&batches, Arc::new(Schema::empty()), |row| {
            let json_bytes = arrow_batches_to_json_bindings(&[row])?;
            let data_ptr = DataPtr::new(json_bytes.as_ptr(), json_bytes.len() as i64);
            let result = inner.execute_query_with_timeout(
                Some(&operation),
                stmt_handle,
                query.clone(),
                Some(BindingType::Json(data_ptr)),
                timeout_seconds,
            )?;
            if *returned_rows >= 0 {
                *returned_rows = result
                    .descriptor
                    .row_count
                    .and_then(|rows| returned_rows.checked_add(rows))
                    .unwrap_or(-1);
            }
            last_query_id = Some(result.descriptor.query_id.clone());
            let timezone = inner.session_timezone(conn_handle)?;
            Ok(Box::new(ConvertingReader::new(
                result.reader,
                timestamp_precision.time_unit(),
                timestamp_precision,
                timezone,
            )))
        });
        // On a later-row failure, retain the ID of the last successful execution.
        self.last_query_id = last_query_id;
        result
    }
}

impl adbc_core::Statement for Statement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        self.bound = Bound::Batches(vec![batch]);
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        self.bound = Bound::Stream(reader);
        Ok(())
    }

    #[allow(refining_impl_trait)]
    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let inner = Arc::clone(&self.inner);
        let parent = self.trace_parent_for_span();
        inner.traced("statement.ExecuteQuery", parent.as_deref(), |span| {
            let mut returned_rows = -1;
            let result = (|| {
                if self.ingest_state.is_set() {
                    // Ingest via execute() — run the ingest and return an empty reader.
                    let result = crate::ingest::execute_ingest(self);
                    self.bound = Bound::Empty;
                    returned_rows = result?.unwrap_or(-1);
                    let batch = arrow_array::RecordBatch::new_empty(Arc::new(
                        arrow_schema::Schema::empty(),
                    ));
                    return Ok(crate::connection::single_batch_reader(batch));
                }
                let query = self.query.clone().ok_or_else(|| {
                    Error::from(
                        SnowflakeError::invalid_state().message("cannot execute without a query"),
                    )
                })?;

                if !matches!(self.bound, Bound::Empty) {
                    return self.execute_bound(query, &mut returned_rows);
                }

                let operation = self.begin_operation();
                let result = self.execute_query(&operation, query, None)?;
                returned_rows = result.descriptor.row_count.unwrap_or(-1);

                self.last_query_id = Some(result.descriptor.query_id.clone());
                let reader = result.reader;
                Ok(Box::new(ConvertingReader::new(
                    reader,
                    self.timestamp_precision.time_unit(),
                    self.timestamp_precision,
                    self.inner.session_timezone(self.conn_handle)?,
                )))
            })();
            span.record("db.response.returned_rows", returned_rows);
            if result.is_ok()
                && self.inner.telemetry.is_enabled()
                && !self.ingest_state.is_set()
                && let Some(id) = self.last_query_id.as_deref().filter(|id| !id.is_empty())
            {
                span.set_attribute("snowflake.query.id", id.to_owned());
            }
            result
        })
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        let inner = Arc::clone(&self.inner);
        let parent = self.trace_parent_for_span();
        inner.traced("statement.ExecuteUpdate", parent.as_deref(), |span| {
            let result = (|| {
                if self.ingest_state.is_set() {
                    let result = crate::ingest::execute_ingest(self);
                    self.bound = Bound::Empty;
                    return result;
                }
                let query = self.query.clone().ok_or_else(|| {
                    Error::from(
                        SnowflakeError::invalid_state().message("cannot execute without a query"),
                    )
                })?;

                // A supplied binding with no rows represents zero executions, so do not
                // run the statement unbound or send an empty binding.
                if !matches!(self.bound, Bound::Empty) {
                    let batches = self.take_bound_batches()?;
                    if batches.iter().all(|batch| batch.num_rows() == 0) {
                        self.last_query_id = None;
                        return Ok(Some(0));
                    }

                    // Parameterised DML: execute once with JSON bindings.
                    let json_bytes = arrow_batches_to_json_bindings(&batches)?;
                    let data_ptr = DataPtr::new(json_bytes.as_ptr(), json_bytes.len() as i64);
                    let binding = BindingType::Json(data_ptr);

                    let operation = self.begin_operation();
                    let result = self.execute_query(&operation, query, Some(binding))?;

                    self.last_query_id = Some(result.descriptor.query_id.clone());
                    return Ok(result.descriptor.rows_affected);
                }

                let operation = self.begin_operation();
                let result = self.execute_query(&operation, query, None)?;

                self.last_query_id = Some(result.descriptor.query_id.clone());

                Ok(result.descriptor.rows_affected)
            })();
            span.record(
                "db.response.returned_rows",
                result.as_ref().ok().and_then(|rows| *rows).unwrap_or(-1),
            );
            if result.is_ok()
                && self.inner.telemetry.is_enabled()
                && !self.ingest_state.is_set()
                && let Some(id) = self.last_query_id.as_deref().filter(|id| !id.is_empty())
            {
                span.set_attribute("snowflake.query.id", id.to_owned());
            }
            result
        })
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        let inner = Arc::clone(&self.inner);
        let parent = self.trace_parent_for_span();
        inner.traced("statement.ExecuteSchema", parent.as_deref(), |span| {
            let result = (|| {
                self.invalidate_described();
                let operation = self.begin_operation();
                let described = self.describe(&operation)?;
                self.last_query_id = Some(described.query_id.clone());
                Ok(described.result.clone())
            })();
            if result.is_ok()
                && self.inner.telemetry.is_enabled()
                && let Some(id) = self.last_query_id.as_deref().filter(|id| !id.is_empty())
            {
                span.set_attribute("snowflake.query.id", id.to_owned());
            }
            result
        })
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Err(SnowflakeError::not_implemented()
            .message("execute_partitions")
            .into())
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        if !self.prepared {
            return Err(SnowflakeError::not_implemented()
                .message("parameter metadata is unavailable; prepare the statement first")
                .into());
        }
        let operation = self.begin_operation();
        self.describe(&operation)?
            .parameters
            .clone()
            .ok_or_else(|| {
                Error::from(
                    SnowflakeError::not_implemented()
                        .message("parameter metadata is unavailable for this statement"),
                )
            })
    }

    fn prepare(&mut self) -> Result<()> {
        if self.query.is_none() {
            return Err(SnowflakeError::invalid_state()
                .message("cannot prepare statement with no query")
                .into());
        }
        self.prepared = true;
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.query = Some(query.as_ref().to_string());
        self.ingest_state.clear();
        self.unprepare();
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(SnowflakeError::not_implemented()
            .message("Snowflake does not support Substrait plans")
            .into())
    }

    fn get_cancel_handle(&self) -> Box<dyn adbc_core::CancelHandle> {
        Box::new(OperationCancelHandle {
            operation: Arc::clone(&self.operation),
        })
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
                    return Err(SnowflakeError::invalid_data()
                        .format(format_args!(
                            "bound query result schema mismatch: expected {expected:?}, got {schema:?}"
                        ))
                        .into());
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
    ts_unit: TimeUnit,
    timezone: &str,
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
                // Binds carry no server Arrow type, so choose the narrowest exact
                // representation the declared precision and scale allow.
                "FIXED" => match (bind.precision, scale) {
                    (Some(precision), 0) if precision <= 18 => DataType::Int64,
                    (precision, scale) => DataType::Decimal128(
                        precision.unwrap_or(38).clamp(1, 38) as u8,
                        scale.clamp(0, 38) as i8,
                    ),
                },
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
            if let Some(length) = bind.length {
                metadata.insert("charLength".into(), length.to_string());
            }
            if let Some(byte_length) = bind.byte_length {
                metadata.insert("byteLength".into(), byte_length.to_string());
            }
            if !bind.ext_col_type_name.is_empty() {
                metadata.insert("extTypeName".into(), bind.ext_col_type_name.clone());
            }
            Field::new(&bind.name, physical_type, bind.nullable).with_metadata(metadata)
        })
        .collect::<Vec<_>>();
    Some(
        adjust_schema(&Schema::new(fields), ts_unit, timezone, None)
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

/// Map Snowflake's physical encodings to Arrow types without losing field
/// metadata. ARRAY and GeoArrow fields replace their source metadata, as in Go.
pub(crate) fn adjust_schema(
    schema: &Schema,
    ts_unit: TimeUnit,
    timezone: &str,
    geo_srids: Option<&[Option<u32>]>,
) -> Arc<Schema> {
    use arrow_schema::extension::{Json, Uuid};
    let adjusted_fields: Vec<Field> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let mut adjusted = field
                .as_ref()
                .clone()
                .with_data_type(compute_target_type(field, ts_unit, timezone));
            if is_uuid_field(field) && matches!(field.data_type(), DataType::Utf8) {
                return adjusted
                    .with_data_type(DataType::FixedSizeBinary(16))
                    .with_extension_type(Uuid);
            }
            if field.metadata().get("logicalType").is_some_and(|s| s == "ARRAY")
                && matches!(adjusted.data_type(), DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
            {
                return Field::new(field.name(), adjusted.data_type().clone(), field.is_nullable())
                    .with_extension_type(Json::default());
            }
            if let Some(geo_type) = geo_type(field)
                && matches!(adjusted.data_type(), DataType::Binary)
            {
                let srid = geo_srids
                    .and_then(|srids| srids.get(index))
                    .copied()
                    .flatten();
                let metadata = match (geo_srids, srid, geo_type) {
                    (Some(_), Some(srid), _) if srid != 0 => Some(format!(
                        "{{\"crs\":\"EPSG:{srid}\",\"crs_type\":\"authority_code\"}}"
                    )),
                    (None, _, "GEOGRAPHY") => Some(
                        "{\"crs\":\"EPSG:4326\",\"crs_type\":\"authority_code\",\"edges\":\"spherical\"}".to_owned()
                    ),
                    _ => None,
                };
                let mut extension = std::collections::HashMap::from([(
                    "ARROW:extension:name".to_owned(),
                    "geoarrow.wkb".to_owned(),
                )]);
                if let Some(metadata) = metadata {
                    extension.insert("ARROW:extension:metadata".to_owned(), metadata);
                }
                adjusted = adjusted.with_metadata(extension);
            }
            adjusted
        })
        .collect();
    Arc::new(Schema::new_with_metadata(
        adjusted_fields,
        schema.metadata().clone(),
    ))
}

fn is_uuid_field(field: &Field) -> bool {
    let metadata = field.metadata();
    metadata
        .get("logicalType")
        .is_some_and(|name| name.eq_ignore_ascii_case("TEXT"))
        && metadata
            .get("charLength")
            .or_else(|| metadata.get("length"))
            .is_some_and(|n| n == "0")
        && metadata.get("byteLength").is_none_or(|n| n != "0")
}

fn geo_type(field: &Field) -> Option<&str> {
    let name = field
        .metadata()
        .get("extTypeName")
        .or_else(|| field.metadata().get("logicalType"))?;
    if name.eq_ignore_ascii_case("GEOGRAPHY") {
        Some("GEOGRAPHY")
    } else if name.eq_ignore_ascii_case("GEOMETRY") {
        Some("GEOMETRY")
    } else {
        None
    }
}

/// Return the embedded SRID if this has a valid OGC/EWKB geometry header.
fn ewkb_srid(value: &[u8]) -> Option<Option<u32>> {
    let header: [u8; 4] = value.get(1..5)?.try_into().ok()?;
    let type_word = match value[0] {
        0 => u32::from_be_bytes(header),
        1 => u32::from_le_bytes(header),
        _ => return None,
    };
    let base = type_word & 0x0fff_ffff;
    if !(1..=7).contains(&base)
        && !(1001..=1007).contains(&base)
        && !(2001..=2007).contains(&base)
        && !(3001..=3007).contains(&base)
    {
        return None;
    }
    if type_word & 0x2000_0000 == 0 {
        return Some(None);
    }
    let srid: [u8; 4] = value.get(5..9)?.try_into().ok()?;
    Some(Some(if value[0] == 0 {
        u32::from_be_bytes(srid)
    } else {
        u32::from_le_bytes(srid)
    }))
}

fn geo_column_srid(column: &arrow_array::BinaryArray) -> Option<u32> {
    let mut observed = None;
    let mut classified = false;
    let mut inconsistent = false;
    for value in column.iter().flatten() {
        let header = ewkb_srid(value);
        if !classified {
            classified = true;
            if header.is_none() {
                break;
            }
        } else if header.is_none() {
            inconsistent = true;
        }
        if let Some(Some(srid)) = header {
            match observed {
                Some(previous) if previous != srid => inconsistent = true,
                None => observed = Some(srid),
                _ => {}
            }
        }
    }
    if inconsistent { None } else { observed }
}

/// Rewrite EWKB values as ISO WKB. `declared` is the SRID the field's CRS
/// metadata was built from; a value carrying any other SRID cannot be
/// described by that single column CRS, so it is rejected rather than having
/// its SRID silently dropped.
fn strip_geo_srid(
    column: &ArrayRef,
    field_name: &str,
    declared: Option<u32>,
) -> std::result::Result<ArrayRef, arrow_schema::ArrowError> {
    use std::io::Write as _;
    let binary = column
        .as_any()
        .downcast_ref::<arrow_array::BinaryArray>()
        .ok_or_else(|| arrow_schema::ArrowError::CastError("expected binary EWKB column".into()))?;
    let mut has_srid = false;
    for srid in binary
        .iter()
        .flatten()
        .filter_map(|value| ewkb_srid(value).flatten())
    {
        if declared != Some(srid) {
            return Err(arrow_schema::ArrowError::InvalidArgumentError(format!(
                "geo column '{field_name}' contains SRID {srid}, but its CRS metadata was \
                 inferred from the first batch as {}; set the geography/geometry output \
                 format to GeoJSON to read mixed-SRID results",
                declared.map_or_else(|| "unknown".to_owned(), |srid| format!("EPSG:{srid}")),
            )));
        }
        has_srid = true;
    }
    if !has_srid {
        return Ok(column.clone());
    }
    let mut builder =
        arrow_array::builder::BinaryBuilder::with_capacity(binary.len(), binary.value_data().len());
    for value in binary.iter() {
        let Some(value) = value else {
            builder.append_null();
            continue;
        };
        if ewkb_srid(value).is_some_and(|srid| srid.is_some()) {
            let header: [u8; 4] = value[1..5].try_into().expect("validated EWKB header");
            let type_word = if value[0] == 0 {
                u32::from_be_bytes(header) & !0x2000_0000
            } else {
                u32::from_le_bytes(header) & !0x2000_0000
            };
            builder.write_all(&value[..1]).expect("in-memory write");
            builder
                .write_all(&if value[0] == 0 {
                    type_word.to_be_bytes()
                } else {
                    type_word.to_le_bytes()
                })
                .expect("in-memory write");
            builder.append_value(&value[9..]);
        } else {
            builder.append_value(value);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn uuid_strings_to_bytes(
    column: &ArrayRef,
) -> std::result::Result<ArrayRef, arrow_schema::ArrowError> {
    let text = column
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .ok_or_else(|| {
            arrow_schema::ArrowError::CastError(format!(
                "expected utf8 data for UUID column, got {}",
                column.data_type()
            ))
        })?;
    let mut builder = arrow_array::builder::FixedSizeBinaryBuilder::with_capacity(text.len(), 16);
    for value in text.iter() {
        match value {
            Some(value) => {
                let parsed = uuid::Uuid::parse_str(value).map_err(|error| {
                    arrow_schema::ArrowError::ParseError(format!("invalid UUID '{value}': {error}"))
                })?;
                builder.append_value(parsed.as_bytes())?;
            }
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn compute_target_type(field: &Field, ts_unit: TimeUnit, timezone: &str) -> DataType {
    let logical_type = field
        .metadata()
        .get("logicalType")
        .map(String::as_str)
        .unwrap_or("");
    let scale: i64 = field
        .metadata()
        .get("scale")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    match logical_type {
        // Snowflake sends FIXED columns with scale > 0 as scaled integers
        // (e.g. NUMBER(15,2) 123.45 as Int16 12345); expose them as exact
        // decimals. Scale-0 integers and real decimals keep the server's type.
        "FIXED" if scale > 0 && field.data_type().is_integer() => {
            let precision = field
                .metadata()
                .get("precision")
                .and_then(|p| p.parse::<u8>().ok())
                .unwrap_or(38)
                .clamp(1, 38);
            DataType::Decimal128(precision, scale.clamp(0, 38) as i8)
        }
        "TIME" => {
            // Arrow Time32 only supports seconds/milliseconds.
            let unit = scale_to_time_unit(scale as u32);
            if scale <= 3 {
                DataType::Time32(unit)
            } else {
                DataType::Time64(unit)
            }
        }
        "TIMESTAMP_NTZ" => DataType::Timestamp(timestamp_target_unit(scale, ts_unit), None),
        "REAL" => DataType::Float64,
        "TIMESTAMP_LTZ" => DataType::Timestamp(
            timestamp_target_unit(scale, ts_unit),
            Some(Arc::from(timezone)),
        ),
        "TIMESTAMP_TZ" => DataType::Timestamp(
            timestamp_target_unit(scale, ts_unit),
            Some(Arc::from("UTC")),
        ),
        _ => field.data_type().clone(),
    }
}

pub(crate) struct ConvertingReader<R: RecordBatchReader> {
    inner: R,
    original_schema: Arc<Schema>,
    schema: Arc<Schema>,
    pending: Option<std::result::Result<RecordBatch, arrow_schema::ArrowError>>,
    /// Per-column SRID the GeoArrow CRS metadata was built from (empty when
    /// the result has no EWKB geo columns or no batches).
    geo_srids: Vec<Option<u32>>,
    timezone: Arc<str>,
    ts_unit: TimeUnit,
    check_overflow: bool,
}

impl<R: RecordBatchReader> ConvertingReader<R> {
    pub(crate) fn new(
        mut inner: R,
        ts_unit: TimeUnit,
        ts_precision: TimestampPrecision,
        timezone: Arc<str>,
    ) -> Self {
        let original_schema = inner.schema();
        // GeoArrow CRS metadata comes from the first batch's EWKB SRID. Only
        // geo result sets need an eager read; all others remain lazy.
        let has_geo = original_schema.fields().iter().any(|field| {
            geo_type(field).is_some() && matches!(field.data_type(), DataType::Binary)
        });
        let pending = has_geo.then(|| inner.next()).flatten();
        let srids = pending.as_ref().and_then(|result| {
            result.as_ref().ok().map(|batch| {
                original_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(i, field)| match geo_type(field) {
                        Some(geo) => batch
                            .column(i)
                            .as_any()
                            .downcast_ref::<arrow_array::BinaryArray>()
                            .and_then(geo_column_srid)
                            // GEOGRAPHY is always EPSG:4326, even when the
                            // first batch has no SRID to observe.
                            .or((geo == "GEOGRAPHY").then_some(4326)),
                        None => None,
                    })
                    .collect::<Vec<_>>()
            })
        });
        let schema = adjust_schema(&original_schema, ts_unit, &timezone, srids.as_deref());
        let geo_srids = srids.unwrap_or_default();

        Self {
            inner,
            original_schema,
            schema,
            pending,
            geo_srids,
            timezone,
            ts_unit,
            check_overflow: ts_precision == TimestampPrecision::NanosecondsErrorOnOverflow,
        }
    }

    fn convert_column(
        col: &ArrayRef,
        source_field: &Field,
        target_type: &DataType,
        ts_unit: TimeUnit,
        check_overflow: bool,
        timezone: &Arc<str>,
        geo_srid: Option<u32>,
    ) -> std::result::Result<ArrayRef, arrow_schema::ArrowError> {
        if is_uuid_field(source_field) && matches!(target_type, DataType::FixedSizeBinary(16)) {
            return uuid_strings_to_bytes(col);
        }
        if geo_type(source_field).is_some() && matches!(target_type, DataType::Binary) {
            return strip_geo_srid(col, source_field.name(), geo_srid);
        }
        let logical_type = source_field
            .metadata()
            .get("logicalType")
            .map(String::as_str)
            .unwrap_or("");
        let scale = source_field
            .metadata()
            .get("scale")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        match logical_type {
            "FIXED"
                if matches!(target_type, DataType::Decimal128(..))
                    && col.data_type().is_integer() =>
            {
                scaled_integer_to_decimal(col, target_type)
            }
            "TIME" => convert_time(col, scale, target_type),
            "REAL" => arrow_cast::cast(col.as_ref(), &DataType::Float64),
            "TIMESTAMP_NTZ" => convert_timestamp_ntz(col, scale, ts_unit, check_overflow, None),
            "TIMESTAMP_LTZ" => convert_timestamp_ntz(
                col,
                scale,
                ts_unit,
                check_overflow,
                Some(Arc::clone(timezone)),
            ),
            "TIMESTAMP_TZ" => convert_timestamp_tz(col, scale, ts_unit, check_overflow),
            _ => Ok(col.clone()),
        }
    }
}

/// Reinterpret scaled integers as Decimal128 without changing the stored
/// unscaled values (12345 at scale 2 is 123.45). The server only sends values
/// within the column's declared precision, so the widening skips arrow's
/// per-value precision check.
fn scaled_integer_to_decimal(
    col: &ArrayRef,
    target: &DataType,
) -> std::result::Result<ArrayRef, arrow_schema::ArrowError> {
    use arrow_array::{
        Decimal128Array,
        cast::AsArray,
        types::{Decimal128Type, Int8Type, Int16Type, Int32Type, Int64Type},
    };
    let DataType::Decimal128(precision, scale) = *target else {
        return Err(arrow_schema::ArrowError::CastError(format!(
            "expected a Decimal128 target, got {target}"
        )));
    };
    let unscaled: Decimal128Array = match col.data_type() {
        DataType::Int8 => col.as_primitive::<Int8Type>().unary(i128::from),
        DataType::Int16 => col.as_primitive::<Int16Type>().unary(i128::from),
        DataType::Int32 => col.as_primitive::<Int32Type>().unary(i128::from),
        DataType::Int64 => col.as_primitive::<Int64Type>().unary(i128::from),
        // Unsigned integers are not produced by Snowflake; keep the checked
        // cast for them rather than assuming they fit.
        _ => arrow_cast::cast(col.as_ref(), &DataType::Decimal128(38, 0))?
            .as_primitive::<Decimal128Type>()
            .clone(),
    };
    Ok(Arc::new(
        unscaled.with_precision_and_scale(precision, scale)?,
    ))
}

/// Snowflake TIME values are integer counts of 10^-scale seconds since
/// midnight; rescale them to the target Time32/Time64 unit.
fn convert_time(
    col: &ArrayRef,
    scale: i64,
    target: &DataType,
) -> std::result::Result<ArrayRef, arrow_schema::ArrowError> {
    use arrow_array::{cast::AsArray, types::Int64Type};
    let unit_digits = match target {
        DataType::Time32(TimeUnit::Second) => 0,
        DataType::Time32(TimeUnit::Millisecond) => 3,
        DataType::Time64(TimeUnit::Microsecond) => 6,
        DataType::Time64(TimeUnit::Nanosecond) => 9,
        other => {
            return Err(arrow_schema::ArrowError::CastError(format!(
                "unsupported TIME target type {other}"
            )));
        }
    };
    let factor = 10i64.pow((unit_digits - scale.clamp(0, 9)).max(0) as u32);
    let ints = arrow_cast::cast(col.as_ref(), &DataType::Int64)?;
    let scaled: arrow_array::Int64Array = ints.as_primitive::<Int64Type>().try_unary(|value| {
        value.checked_mul(factor).ok_or_else(|| {
            arrow_schema::ArrowError::CastError(format!(
                "TIME value {value} overflows when rescaled to {target}"
            ))
        })
    })?;
    let physical = if matches!(target, DataType::Time32(_)) {
        arrow_cast::cast(&scaled, &DataType::Int32)?
    } else {
        Arc::new(scaled)
    };
    arrow_cast::cast(physical.as_ref(), target)
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
        DataType::Int64 => {
            let (multiplier, divisor) = compact_timestamp_factors(scale, unit);
            if multiplier == 1 && divisor == 1 {
                return arrow_cast::cast(col.as_ref(), &target);
            }
            let raw = col
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .expect("Int64 column");
            let check_ns = check_overflow && unit == TimeUnit::Nanosecond;
            let values: arrow_array::Int64Array = raw.try_unary(|value| {
                compact_timestamp_to_unit(value, multiplier, divisor, check_ns)
            })?;
            arrow_cast::cast(&values, &target)
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
    } else if num_fields == 3 {
        let fraction = struct_arr
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| arrow_schema::ArrowError::CastError("expected Int32 fraction".into()))?;
        struct_arr
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| arrow_schema::ArrowError::CastError("expected Int32 timezone".into()))?;
        // The original display offset does not shift the UTC epoch/fraction.
        build_timestamp_from_epoch_fraction(
            epoch,
            fraction,
            struct_arr,
            scale,
            check_overflow,
            target,
        )
    } else {
        Err(arrow_schema::ArrowError::CastError(format!(
            "expected two or three TIMESTAMP_TZ fields, got {num_fields}"
        )))
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
        TimeUnit::Second => ns.div_euclid(1_000_000_000),
        TimeUnit::Millisecond => ns.div_euclid(1_000_000),
        TimeUnit::Microsecond => ns.div_euclid(1_000),
        // Without the overflow option, nanosecond values outside i64 wrap.
        TimeUnit::Nanosecond => ns,
    };
    val as i64
}

/// Compact timestamps hold epoch seconds multiplied by 10^scale, plus the
/// fractional digits at that exact scale (not the rounded-up Arrow unit).
fn compact_timestamp_factors(scale: i64, unit: TimeUnit) -> (i64, i64) {
    let scale = scale.clamp(0, 9);
    let unit_scale = (time_unit_rank(unit) * 3) as i64;
    if scale < unit_scale {
        (10i64.pow((unit_scale - scale) as u32), 1)
    } else {
        (1, 10i64.pow((scale - unit_scale) as u32))
    }
}

fn compact_timestamp_to_unit(
    value: i64,
    multiplier: i64,
    divisor: i64,
    check_overflow: bool,
) -> std::result::Result<i64, arrow_schema::ArrowError> {
    if divisor > 1 {
        return Ok(value.div_euclid(divisor));
    }
    if multiplier == 1 {
        return Ok(value);
    }
    let scaled = value as i128 * multiplier as i128;
    if check_overflow {
        check_ns_overflow(scaled)?;
    }
    Ok(scaled as i64)
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
            if check_overflow && unit == TimeUnit::Nanosecond {
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
            .data_type(target)
            .build_unchecked()
    };
    Ok(Arc::new(arrow_array::make_array(data)) as ArrayRef)
}

fn build_timestamp_tz_2field(
    epoch: &arrow_array::Int64Array,
    _tzoffset: &arrow_array::Int32Array,
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
    let (multiplier, divisor) = compact_timestamp_factors(scale, unit);
    let check_ns = check_overflow && unit == TimeUnit::Nanosecond;
    let mut values: Vec<Option<i64>> = Vec::with_capacity(len);

    for i in 0..len {
        if struct_arr.is_null(i) {
            values.push(None);
        } else {
            // The timezone offset is display metadata; the compact epoch
            // already identifies the UTC instant at the exact declared scale.
            values.push(Some(compact_timestamp_to_unit(
                epoch.value(i),
                multiplier,
                divisor,
                check_ns,
            )?));
        }
    }
    let int_arr = arrow_array::Int64Array::from(values);
    let data = unsafe {
        int_arr
            .into_data()
            .into_builder()
            .data_type(target)
            .build_unchecked()
    };
    Ok(Arc::new(arrow_array::make_array(data)) as ArrayRef)
}

impl<R: RecordBatchReader> Iterator for ConvertingReader<R> {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = match self.pending.take().or_else(|| self.inner.next())? {
            Ok(batch) => batch,
            Err(error) => return Some(Err(error)),
        };

        let adjusted_columns: std::result::Result<Vec<ArrayRef>, arrow_schema::ArrowError> = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(i, col)| {
                Self::convert_column(
                    col,
                    self.original_schema.field(i),
                    self.schema.field(i).data_type(),
                    self.ts_unit,
                    self.check_overflow,
                    &self.timezone,
                    self.geo_srids.get(i).copied().flatten(),
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
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => Ok("FIXED"),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Ok("REAL"),
        DataType::Null | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("TEXT"),
        DataType::Boolean => Ok("BOOLEAN"),
        DataType::Date32 | DataType::Date64 => Ok("DATE"),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::BinaryView => Ok("BINARY"),
        DataType::Time32(_) | DataType::Time64(_) => Ok("TIME"),
        DataType::Timestamp(_, None) => Ok("TIMESTAMP_NTZ"),
        DataType::Timestamp(_, Some(_)) => Ok("TIMESTAMP_LTZ"),
        _ => Err(SnowflakeError::not_implemented()
            .format(format_args!("unsupported bind parameter type: {dt:?}"))
            .into()),
    }
}

fn escape_json_string_to(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                write!(out, "\\u{:04x}", c as u32).expect("String write cannot fail");
            }
            c => out.push(c),
        }
    }
}

/// Write a non-null bind value directly where its Arrow storage permits it.
fn write_json_bind_value(out: &mut String, col: &dyn Array, row: usize, uuid: bool) -> Result<()> {
    use arrow_array::*;
    let dt = col.data_type();
    if matches!(dt, DataType::Null) || col.is_null(row) {
        out.push_str("null");
        return Ok(());
    }
    out.push('"');
    match dt {
        DataType::Utf8 => escape_json_string_to(
            out,
            col.as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row),
        ),
        DataType::LargeUtf8 => escape_json_string_to(
            out,
            col.as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row),
        ),
        DataType::Utf8View => escape_json_string_to(
            out,
            col.as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .value(row),
        ),
        DataType::Int8 => write!(
            out,
            "{}",
            col.as_any().downcast_ref::<Int8Array>().unwrap().value(row)
        )
        .unwrap(),
        DataType::Int16 => write!(
            out,
            "{}",
            col.as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row)
        )
        .unwrap(),
        DataType::Int32 => write!(
            out,
            "{}",
            col.as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row)
        )
        .unwrap(),
        DataType::Int64 => write!(
            out,
            "{}",
            col.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row)
        )
        .unwrap(),
        DataType::UInt8 => write!(
            out,
            "{}",
            col.as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(row)
        )
        .unwrap(),
        DataType::UInt16 => write!(
            out,
            "{}",
            col.as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(row)
        )
        .unwrap(),
        DataType::UInt32 => write!(
            out,
            "{}",
            col.as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row)
        )
        .unwrap(),
        DataType::UInt64 => write!(
            out,
            "{}",
            col.as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row)
        )
        .unwrap(),
        DataType::FixedSizeBinary(16) if uuid => {
            let bytes = col
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(row);
            write!(
                out,
                "{}",
                uuid::Uuid::from_bytes(bytes.try_into().expect("UUID storage is 16 bytes"))
            )
            .expect("String write cannot fail");
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::BinaryView => {
            let bytes: &[u8] = match dt {
                DataType::Binary => col
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(row),
                DataType::LargeBinary => col
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .unwrap()
                    .value(row),
                DataType::FixedSizeBinary(_) => col
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap()
                    .value(row),
                DataType::BinaryView => col
                    .as_any()
                    .downcast_ref::<BinaryViewArray>()
                    .unwrap()
                    .value(row),
                _ => unreachable!(),
            };
            for byte in bytes {
                write!(out, "{byte:02x}").expect("String write cannot fail");
            }
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            let value = match dt {
                DataType::Float16 => col
                    .as_any()
                    .downcast_ref::<Float16Array>()
                    .unwrap()
                    .value(row)
                    .to_f32() as f64,
                DataType::Float32 => col
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(row) as f64,
                DataType::Float64 => col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row),
                _ => unreachable!(),
            };
            if !value.is_finite() {
                out.pop();
                out.push_str("null");
                return Ok(());
            }
            // Float16 and Float32 use f32 Debug formatting, as in format_arrow_value.
            if matches!(dt, DataType::Float64) {
                write!(out, "{value:?}").unwrap();
            } else {
                write!(out, "{:?}", value as f32).unwrap();
            }
        }
        _ => match format_arrow_value(col, row, dt)? {
            Some(value) => escape_json_string_to(out, &value),
            None => {
                out.pop();
                out.push_str("null");
                return Ok(());
            }
        },
    }
    out.push('"');
    Ok(())
}

pub(crate) fn arrow_batches_to_json_bindings(batches: &[RecordBatch]) -> Result<String> {
    let Some(first) = batches.first() else {
        return Ok("{}".to_string());
    };
    let schema = first.schema();
    // A bind_stream reader can yield batches that disagree with its first
    // batch; reject them instead of indexing past the end or mislabeling types.
    for (index, batch) in batches.iter().enumerate().skip(1) {
        let matches = batch.num_columns() == first.num_columns()
            && batch
                .columns()
                .iter()
                .zip(schema.fields())
                .all(|(column, field)| column.data_type() == field.data_type());
        if !matches {
            return Err(SnowflakeError::invalid_data()
                .format(format_args!(
                    "bound batch {index} does not match the schema of the first bound batch: \
                     expected {schema:?}, got {:?}",
                    batch.schema()
                ))
                .into());
        }
    }
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let mut json = String::from("{");
    for col_idx in 0..first.num_columns() {
        if col_idx != 0 {
            json.push(',');
        }
        let field = schema.field(col_idx);
        let value_type = match field.data_type() {
            DataType::Dictionary(_, value_type) => value_type.as_ref(),
            data_type => data_type,
        };
        let uuid = matches!(value_type, DataType::FixedSizeBinary(16))
            && field
                .metadata()
                .get("ARROW:extension:name")
                .is_some_and(|name| name == "arrow.uuid");
        let snowflake_type = if uuid {
            "TEXT"
        } else {
            snowflake_type_name(value_type)?
        };
        write!(
            json,
            "\"{}\":{{\"type\":\"{}\",\"value\":",
            col_idx + 1,
            snowflake_type
        )
        .expect("String write cannot fail");
        if total_rows != 1 {
            json.push('[');
        }
        let mut emitted = 0;
        for batch in batches {
            let col = batch.column(col_idx);
            let decoded;
            let col: &dyn Array = if matches!(col.data_type(), DataType::Dictionary(..)) {
                decoded = arrow_cast::cast(col.as_ref(), value_type).map_err(|error| {
                    Error::from(SnowflakeError::invalid_data().format(format_args!(
                        "cannot decode bound dictionary column {}: {error}",
                        field.name()
                    )))
                })?;
                decoded.as_ref()
            } else {
                col.as_ref()
            };
            for row in 0..batch.num_rows() {
                if emitted != 0 {
                    json.push(',');
                }
                write_json_bind_value(&mut json, col, row, uuid)?;
                emitted += 1;
            }
        }
        if total_rows != 1 {
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
        DataType::Decimal256(_, scale) => {
            let val = arr
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap()
                .value(row);
            Ok(Some(decimal256_to_string(val, *scale)))
        }
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
        _ => Err(SnowflakeError::not_implemented()
            .format(format_args!("unsupported bind parameter type: {dt:?}"))
            .into()),
    }
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

fn decimal256_to_string(value: arrow_buffer::i256, scale: i8) -> String {
    let mut text = value.to_string();
    if scale < 0 {
        text.extend(std::iter::repeat_n('0', scale.unsigned_abs() as usize));
    } else if scale > 0 {
        let sign_len = usize::from(value.is_negative());
        let digits = text.len() - sign_len;
        let scale = scale as usize;
        if digits <= scale {
            text.insert_str(sign_len, &format!("0.{}", "0".repeat(scale - digits)));
        } else {
            text.insert(text.len() - scale, '.');
        }
    }
    text
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use adbc_core::{Statement as _, error::Status, options::IngestMode};

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
            fields: Vec::new(),
        }
    }

    /// A statement whose handles are invalid: any sf_core call on it fails.
    pub(crate) fn make_stmt() -> Statement {
        let driver = crate::driver::Driver::default();
        Statement {
            inner: driver.inner.clone(),
            stmt_handle: sf_core::apis::database_driver_v1::Handle { id: 0, magic: 0 },
            conn_handle: sf_core::apis::database_driver_v1::Handle { id: 0, magic: 0 },
            trace_parent: None,
            connection_trace_parent: None,
            query: None,
            ingest_state: BulkIngestState::new(),
            query_tag: None,
            query_timeout_seconds: None,
            prefetch_concurrency: None,
            result_queue_size: None,
            timestamp_precision: TimestampPrecision::Nanoseconds,
            ingest_options: crate::ingest::IngestOptions::default(),
            bound: Bound::Empty,
            last_query_id: None,
            prepared: false,
            described: Mutex::new(None),
            operation: Arc::new(Mutex::new(Arc::new(OperationCtx::with_own_token()))),
        }
    }

    fn seed_described(stmt: &mut Statement, parameters: Option<Schema>, result: Schema) {
        *stmt.described.get_mut().unwrap() = Some(Arc::new(DescribedSchemas {
            query_id: "described".into(),
            parameters,
            result,
        }));
    }

    fn described_result(stmt: &Statement) -> Option<Schema> {
        stmt.described
            .lock()
            .unwrap()
            .as_ref()
            .map(|described| described.result.clone())
    }

    #[test]
    fn set_sql_query_stores_query() {
        let mut stmt = make_stmt();
        stmt.set_sql_query("SELECT 1").unwrap();
        assert_eq!(stmt.query.as_deref(), Some("SELECT 1"));
    }

    #[test]
    fn set_sql_query_preserves_bound_parameters() {
        let mut stmt = make_stmt();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "parameter",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![7]))],
        )
        .unwrap();
        stmt.bind(batch).unwrap();
        seed_described(&mut stmt, Some(Schema::empty()), Schema::empty());
        stmt.prepared = true;
        stmt.set_sql_query("SELECT ?").unwrap();
        stmt.set_sql_query("SELECT ? + 1").unwrap();
        assert_eq!(stmt.query.as_deref(), Some("SELECT ? + 1"));
        assert!(!stmt.prepared);
        assert!(described_result(&stmt).is_none());
        assert!(matches!(&stmt.bound, Bound::Batches(batches) if batches.len() == 1));

        stmt.bind_stream(Box::new(ConcatReader {
            batches: Vec::new().into_iter(),
            schema,
        }))
        .unwrap();
        stmt.set_sql_query("SELECT ? + 2").unwrap();
        assert!(matches!(stmt.bound, Bound::Stream(_)));
    }

    #[test]
    fn trace_parent_inherits_overrides_and_clears() {
        let mut stmt = make_stmt();
        let key = OptionStatement::Other(driverbase::telemetry::TRACE_PARENT.into());
        let connection_parent = "00-11111111111111111111111111111111-2222222222222222-01";
        let statement_parent = "00-33333333333333333333333333333333-4444444444444444-01";
        stmt.connection_trace_parent = Some(connection_parent.into());
        assert_eq!(
            stmt.get_option_string(key.clone()).unwrap(),
            connection_parent
        );
        stmt.set_option(key.clone(), OptionValue::String(statement_parent.into()))
            .unwrap();
        assert_eq!(
            stmt.get_option_string(key.clone()).unwrap(),
            statement_parent
        );
        assert_eq!(stmt.trace_parent.as_deref(), Some(statement_parent));
        stmt.set_option(key.clone(), OptionValue::String(String::new()))
            .unwrap();
        assert_eq!(stmt.trace_parent, None);
        assert_eq!(
            stmt.get_option_string(key.clone()).unwrap(),
            connection_parent
        );

        assert_eq!(
            stmt.set_option(key.clone(), OptionValue::String("invalid".into()))
                .unwrap_err()
                .status,
            Status::InvalidArguments
        );
        assert_eq!(
            stmt.set_option(key.clone(), OptionValue::Int(1))
                .unwrap_err()
                .status,
            Status::InvalidArguments
        );
        assert_eq!(stmt.get_option_string(key).unwrap(), connection_parent);
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
    fn timestamp_precision_rejects_invalid_values_without_changing_state() {
        let mut stmt = make_stmt();
        let key = OptionStatement::Other(
            "adbc.snowflake.sql.client_option.max_timestamp_precision".into(),
        );
        stmt.set_option(key.clone(), OptionValue::String("microseconds".into()))
            .unwrap();
        let schema = Schema::new(vec![Field::new("prepared", DataType::Int64, true)]);
        seed_described(&mut stmt, None, schema.clone());
        for value in [
            OptionValue::String("bogus".into()),
            OptionValue::String(String::new()),
            OptionValue::Int(1),
        ] {
            let is_string = matches!(&value, OptionValue::String(_));
            let error = stmt.set_option(key.clone(), value).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            assert!(error.message.contains(if is_string {
                "Invalid value for statement option"
            } else {
                "must be a string"
            }));
            assert_eq!(stmt.timestamp_precision, TimestampPrecision::Microseconds);
            assert_eq!(described_result(&stmt), Some(schema.clone()));
        }
        assert_eq!(stmt.get_option_string(key).unwrap(), "microseconds");
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

        stmt.get_cancel_handle().try_cancel().unwrap();

        drop(stmt);
        // Statement::drop released the statement; release the core connection.
        inner.sf.connection_release(conn_handle).unwrap();
    }

    #[test]
    fn statement_cancel_handle_tracks_current_operation() {
        let stmt = make_stmt();
        let handle = stmt.get_cancel_handle();
        handle.try_cancel().unwrap();
        let old = stmt.begin_operation();
        let (started, waiting) = std::sync::mpsc::channel();
        let mut started = Some(started);
        let cancelled = std::thread::scope(|scope| {
            let cancel = &handle;
            let cancelling = scope.spawn(move || {
                waiting.recv().unwrap();
                cancel.try_cancel().unwrap();
            });
            let result = stmt.inner.block_on(old.run(
                "statement_cancel_unit",
                std::future::poll_fn(move |_| {
                    if let Some(started) = started.take() {
                        started.send(()).unwrap();
                    }
                    std::task::Poll::Pending::<
                        std::result::Result<(), sf_core::apis::database_driver_v1::ApiError>,
                    >
                }),
            ));
            cancelling.join().unwrap();
            result
        });
        assert_eq!(
            crate::error::api_error_to_adbc_error(cancelled.unwrap_err()).status,
            Status::Cancelled
        );
        let next = stmt.begin_operation();
        assert!(!next.token().is_cancelled());
        assert!(
            stmt.inner
                .block_on(next.run(
                    "statement_cancel_unit",
                    std::future::ready(Ok::<(), sf_core::apis::database_driver_v1::ApiError>(())),
                ))
                .is_ok()
        );
        handle.try_cancel().unwrap();
        assert!(next.token().is_cancelled());
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
        seed_described(&mut stmt, None, schema.as_ref().clone());
        stmt.last_query_id = Some("previous-query-id".into());
        stmt.bind(RecordBatch::new_empty(Arc::new(Schema::new(vec![
            Field::new("parameter", DataType::Int64, false),
        ]))))
        .unwrap();

        let mut reader = stmt.execute().expect("zero rows should not execute SQL");
        assert_eq!(reader.schema(), schema);
        assert!(reader.next().is_none());
        assert_eq!(stmt.last_query_id, None);
        assert!(matches!(stmt.bound, Bound::Empty));
    }

    #[test]
    fn bound_query_failure_consumes_parameters() {
        let mut stmt = make_stmt();
        stmt.set_sql_query("SELECT ?").unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "parameter",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int64Array::from(vec![7]))],
        )
        .unwrap();
        stmt.bind(batch).unwrap();

        assert!(stmt.execute().is_err()); // Invalid sf_core handle, not a live query.
        assert!(matches!(stmt.bound, Bound::Empty));
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
        assert!(matches!(stmt.bound, Bound::Stream(_)));

        assert_eq!(stmt.execute_update().unwrap(), Some(0));
        assert_eq!(stmt.last_query_id, None);
        assert!(matches!(stmt.bound, Bound::Empty));
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
        assert!(matches!(stmt.bound, Bound::Empty));
    }

    #[test]
    fn bound_update_failure_consumes_parameters() {
        let mut stmt = make_stmt();
        stmt.set_sql_query("DELETE FROM example WHERE id = ?")
            .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int64Array::from(vec![7]))],
        )
        .unwrap();
        stmt.bind(batch).unwrap();

        assert!(stmt.execute_update().is_err()); // Invalid sf_core handle.
        assert!(matches!(stmt.bound, Bound::Empty));
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
        let mut stmt = make_stmt();
        stmt.set_option(
            OptionStatement::TargetTable,
            OptionValue::String("mytable".into()),
        )
        .unwrap();
        match stmt.execute() {
            Err(err) => assert_eq!(err.status, adbc_core::error::Status::InvalidState),
            Ok(_) => panic!("execute should have returned an error"),
        }
    }

    #[test]
    fn set_query_clears_target_table() {
        let mut stmt = make_stmt();
        stmt.set_option(
            OptionStatement::TargetCatalog,
            OptionValue::String("db".into()),
        )
        .unwrap();
        stmt.set_option(
            OptionStatement::TargetDbSchema,
            OptionValue::String("sch".into()),
        )
        .unwrap();
        stmt.set_option(
            OptionStatement::IngestMode,
            OptionValue::String(adbc_core::constants::ADBC_INGEST_OPTION_MODE_APPEND.into()),
        )
        .unwrap();
        stmt.set_option(
            OptionStatement::TargetTable,
            OptionValue::String("mytable".into()),
        )
        .unwrap();
        stmt.set_sql_query("SELECT 1").unwrap();
        assert!(!stmt.ingest_state.is_set());
        assert_eq!(stmt.query.as_deref(), Some("SELECT 1"));
        assert_eq!(stmt.ingest_state.catalog.as_deref(), Some("db"));
        assert_eq!(stmt.ingest_state.schema.as_deref(), Some("sch"));
        assert_eq!(stmt.ingest_state.mode, IngestMode::Append);
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
            5,
            &[
                bind_metadata("", "FIXED", Some(10), Some(2)),
                bind_metadata("named", "TIMESTAMP_NTZ", None, Some(6)),
                bind_metadata("unknown", "FUTURE_TYPE", None, None),
                bind_metadata("int", "FIXED", Some(18), Some(0)),
                bind_metadata("wide", "FIXED", Some(38), Some(0)),
            ],
            TimeUnit::Nanosecond,
            "UTC",
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
        // Integers fit Int64 only up to 18 digits; wider ones need Decimal128.
        assert_eq!(schema.field(3).data_type(), &DataType::Int64);
        assert_eq!(schema.field(4).data_type(), &DataType::Decimal128(38, 0));
    }

    #[test]
    fn invalid_prepare_bind_metadata_is_not_exposed() {
        let bind = bind_metadata("", "FIXED", Some(10), Some(0));
        assert!(
            parameter_schema_from_prepare(
                2,
                std::slice::from_ref(&bind),
                TimeUnit::Nanosecond,
                "UTC"
            )
            .is_none()
        );
        let mut missing_type = bind;
        missing_type.r#type.clear();
        assert!(
            parameter_schema_from_prepare(1, &[missing_type], TimeUnit::Nanosecond, "UTC")
                .is_none()
        );
        assert!(parameter_schema_from_prepare(-1, &[], TimeUnit::Nanosecond, "UTC").is_none());
    }

    #[test]
    fn parameter_schema_requires_valid_prepared_metadata_and_query_reset_clears_it() {
        let mut stmt = make_stmt();
        assert_eq!(
            stmt.get_parameter_schema().unwrap_err().status,
            Status::NotImplemented
        );
        // Prepare is local: no connection exists, so any describe would fail.
        stmt.set_sql_query("SELECT ?").unwrap();
        stmt.prepare().unwrap();
        seed_described(&mut stmt, Some(Schema::empty()), Schema::empty());
        assert!(stmt.get_parameter_schema().unwrap().fields().is_empty());
        stmt.set_sql_query("SELECT 1").unwrap();
        assert!(!stmt.prepared);
        assert!(described_result(&stmt).is_none());
        assert_eq!(
            stmt.get_parameter_schema().unwrap_err().status,
            Status::NotImplemented
        );
    }

    #[test]
    fn parameter_schema_cache_is_invalidated_by_conversion_and_ingest_options() {
        let cases = [
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
                OptionValue::String(adbc_core::constants::ADBC_INGEST_OPTION_MODE_APPEND.into()),
            ),
            (
                OptionStatement::TargetCatalog,
                OptionValue::String("catalog".into()),
            ),
            (
                OptionStatement::TargetDbSchema,
                OptionValue::String("schema".into()),
            ),
        ];

        for (option, value) in cases {
            let mut stmt = make_stmt();
            seed_described(&mut stmt, Some(Schema::empty()), Schema::empty());
            stmt.set_option(option, value).unwrap();
            assert!(described_result(&stmt).is_none());
        }
    }

    #[test]
    fn shared_ingest_options_store_identifiers_and_canonical_modes() {
        use adbc_core::constants;

        let mut stmt = make_stmt();
        assert!(!stmt.ingest_state.is_set());
        assert_eq!(stmt.ingest_state.mode, IngestMode::Create);
        stmt.set_option(
            OptionStatement::TargetCatalog,
            OptionValue::String("catalog".into()),
        )
        .unwrap();
        stmt.set_option(
            OptionStatement::TargetDbSchema,
            OptionValue::String("schema".into()),
        )
        .unwrap();
        assert!(!stmt.ingest_state.is_set());
        assert_eq!(stmt.ingest_state.catalog.as_deref(), Some("catalog"));
        assert_eq!(stmt.ingest_state.schema.as_deref(), Some("schema"));

        for (value, expected) in [
            (
                constants::ADBC_INGEST_OPTION_MODE_CREATE,
                IngestMode::Create,
            ),
            (
                constants::ADBC_INGEST_OPTION_MODE_APPEND,
                IngestMode::Append,
            ),
            (
                constants::ADBC_INGEST_OPTION_MODE_REPLACE,
                IngestMode::Replace,
            ),
            (
                constants::ADBC_INGEST_OPTION_MODE_CREATE_APPEND,
                IngestMode::CreateAppend,
            ),
        ] {
            stmt.set_option(
                OptionStatement::IngestMode,
                OptionValue::String(value.into()),
            )
            .unwrap();
            assert_eq!(stmt.ingest_state.mode, expected);
        }
        stmt.set_option(
            OptionStatement::TargetTable,
            OptionValue::String("table".into()),
        )
        .unwrap();
        assert!(stmt.ingest_state.is_set());
        assert_eq!(stmt.ingest_state.table.as_deref(), Some("table"));
    }

    #[test]
    fn invalid_shared_ingest_options_preserve_sql_and_cached_metadata() {
        let mut stmt = make_stmt();
        stmt.set_sql_query("SELECT 1").unwrap();
        stmt.prepare().unwrap();
        let schema = Schema::new(vec![Field::new("one", DataType::Int64, true)]);
        seed_described(&mut stmt, None, schema.clone());
        for key in [
            OptionStatement::TargetTable,
            OptionStatement::TargetCatalog,
            OptionStatement::TargetDbSchema,
            OptionStatement::IngestMode,
        ] {
            let error = stmt.set_option(key, OptionValue::Int(1)).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            assert!(error.message.starts_with("[Snowflake] invalid option "));
            assert_eq!(stmt.query.as_deref(), Some("SELECT 1"));
            assert!(stmt.prepared);
            assert!(!stmt.ingest_state.is_set());
            assert!(stmt.ingest_state.catalog.is_none());
            assert!(stmt.ingest_state.schema.is_none());
            assert_eq!(stmt.ingest_state.mode, IngestMode::Create);
            assert_eq!(described_result(&stmt), Some(schema.clone()));
        }

        let error = stmt
            .set_option(
                OptionStatement::IngestMode,
                OptionValue::String("append".into()),
            )
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert_eq!(
            error.message,
            "[Snowflake] invalid option adbc.ingest.mode=append"
        );
        assert!(stmt.prepared);
        assert_eq!(described_result(&stmt), Some(schema));
        assert_eq!(stmt.ingest_state.mode, IngestMode::Create);
        stmt.set_option(
            OptionStatement::TargetTable,
            OptionValue::String("valid".into()),
        )
        .unwrap();
        let error = stmt
            .set_option(OptionStatement::TargetTable, OptionValue::Int(1))
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert_eq!(stmt.ingest_state.table.as_deref(), Some("valid"));
    }

    #[test]
    fn invalid_ingest_mode_keeps_previously_selected_mode() {
        let mut stmt = make_stmt();
        stmt.set_option(
            OptionStatement::IngestMode,
            OptionValue::String(adbc_core::constants::ADBC_INGEST_OPTION_MODE_REPLACE.into()),
        )
        .unwrap();
        let error = stmt
            .set_option(
                OptionStatement::IngestMode,
                OptionValue::String("replace".into()),
            )
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert_eq!(stmt.ingest_state.mode, IngestMode::Replace);
    }

    #[test]
    fn temporary_ingestion_rejects_enabled_values() {
        for value in ["true", "1", "enabled"] {
            let mut stmt = make_stmt();
            stmt.set_option(
                OptionStatement::TargetTable,
                OptionValue::String("table".into()),
            )
            .unwrap();
            let error = stmt
                .set_option(
                    OptionStatement::Temporary,
                    OptionValue::String(value.into()),
                )
                .unwrap_err();
            assert_eq!(error.status, Status::NotImplemented, "value {value}");
            assert_eq!(
                error.message,
                "[Snowflake] temporary ingestion is not supported"
            );
            assert_eq!(stmt.ingest_state.table.as_deref(), Some("table"));
        }
    }

    #[test]
    fn temporary_ingestion_accepts_disabled_values() {
        for value in ["false", "0", "disabled"] {
            let mut stmt = make_stmt();
            stmt.set_option(
                OptionStatement::TargetTable,
                OptionValue::String("table".into()),
            )
            .unwrap();
            stmt.set_option(
                OptionStatement::Temporary,
                OptionValue::String(value.into()),
            )
            .unwrap();
            assert_eq!(stmt.ingest_state.table.as_deref(), Some("table"));
        }
    }

    #[test]
    fn temporary_ingestion_rejects_invalid_values() {
        let mut stmt = make_stmt();
        stmt.set_option(
            OptionStatement::TargetTable,
            OptionValue::String("table".into()),
        )
        .unwrap();
        let error = stmt
            .set_option(
                OptionStatement::Temporary,
                OptionValue::String("sometimes".into()),
            )
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert_eq!(
            error.message,
            "[Snowflake] temporary ingestion must be enabled or disabled"
        );

        let error = stmt
            .set_option(OptionStatement::Temporary, OptionValue::Int(0))
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert_eq!(
            error.message,
            "[Snowflake] temporary ingestion must be a string"
        );
        assert_eq!(stmt.ingest_state.table.as_deref(), Some("table"));
    }

    #[test]
    fn set_target_table_option() {
        let mut stmt = make_stmt();
        stmt.set_sql_query("SELECT 1").unwrap();
        stmt.prepare().unwrap();
        seed_described(&mut stmt, Some(Schema::empty()), Schema::empty());
        stmt.set_option(
            OptionStatement::TargetTable,
            OptionValue::String("mytable".into()),
        )
        .unwrap();
        assert!(stmt.ingest_state.is_set());
        assert_eq!(stmt.ingest_state.table.as_deref(), Some("mytable"));
        assert!(stmt.query.is_none());
        assert!(!stmt.prepared);
        assert!(described_result(&stmt).is_none());
    }

    #[test]
    fn snowflake_ingest_tuning_remains_independent_of_shared_state() {
        let mut stmt = make_stmt();
        let key = OptionStatement::Other("adbc.snowflake.statement.ingest_copy_concurrency".into());
        stmt.set_option(key.clone(), OptionValue::Int(2)).unwrap();
        assert_eq!(stmt.get_option_int(key.clone()).unwrap(), 2);
        assert_eq!(stmt.get_option_string(key.clone()).unwrap(), "2");
        assert!(!stmt.ingest_state.is_set());
        stmt.set_option(
            OptionStatement::TargetTable,
            OptionValue::String("table".into()),
        )
        .unwrap();
        assert_eq!(stmt.get_option_int(key.clone()).unwrap(), 2);
        stmt.set_sql_query("SELECT 1").unwrap();
        assert!(!stmt.ingest_state.is_set());
        assert_eq!(stmt.get_option_int(key).unwrap(), 2);
    }

    #[test]
    fn unknown_option_is_not_implemented() {
        let mut stmt = make_stmt();
        let err = stmt
            .set_option(
                OptionStatement::Other("unknown.option".into()),
                OptionValue::String("val".into()),
            )
            .unwrap_err();
        assert_eq!(err.status, Status::NotImplemented);
        assert_eq!(err.message, "[Snowflake] unknown option \"unknown.option\"");
    }

    #[test]
    fn unknown_get_options_name_the_key() {
        let stmt = make_stmt();
        let key = OptionStatement::Other("unknown.option".into());
        for error in [
            stmt.get_option_string(key.clone()).unwrap_err(),
            stmt.get_option_bytes(key.clone()).unwrap_err(),
            stmt.get_option_int(key.clone()).unwrap_err(),
            stmt.get_option_double(key).unwrap_err(),
        ] {
            assert_eq!(error.status, Status::NotFound);
            assert_eq!(
                error.message,
                "[Snowflake] unknown option \"unknown.option\""
            );
        }
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
    fn scaled_integer_fixed_columns_become_exact_decimals() {
        // Live Snowflake sends NUMBER(15,2) 123.45 as Int16 12345 with scale 2.
        let field = make_field_with_precision("d", DataType::Int16, "FIXED", "2", "15");
        let schema = Arc::new(Schema::new(vec![field]));
        assert_eq!(
            adjust_schema(&schema, TimeUnit::Nanosecond, "UTC", None)
                .field(0)
                .data_type(),
            &DataType::Decimal128(15, 2)
        );
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int16Array::from(vec![
                Some(12345),
                Some(-150),
                None,
            ]))],
        )
        .unwrap();
        let mut reader = ConvertingReader::new(
            ConcatReader {
                batches: vec![batch].into_iter(),
                schema,
            },
            TimeUnit::Nanosecond,
            TimestampPrecision::Nanoseconds,
            Arc::from("UTC"),
        );
        let out = reader.next().unwrap().unwrap();
        let values = arrow_cast::cast(out.column(0), &DataType::Utf8).unwrap();
        let values = values
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "123.45");
        assert_eq!(values.value(1), "-1.50");
        assert!(values.is_null(2));
    }

    #[test]
    fn test_converting_reader_passes_server_numeric_types_through() {
        use arrow_array::{Decimal128Array, Int8Array};
        let schema = Arc::new(Schema::new(vec![
            make_field_with_precision("small", DataType::Int8, "FIXED", "0", "2"),
            make_field_with_precision("dec", DataType::Decimal128(10, 2), "FIXED", "2", "10"),
            Field::new("plain", DataType::Int16, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int8Array::from(vec![Some(7i8), None])),
                Arc::new(
                    Decimal128Array::from(vec![12345i128, -255])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
                Arc::new(arrow_array::Int16Array::from(vec![Some(1i16), Some(-2)])),
            ],
        )
        .unwrap();
        let reader = ConcatReader {
            batches: vec![batch.clone()].into_iter(),
            schema,
        };
        let mut cr = ConvertingReader::new(
            reader,
            TimeUnit::Nanosecond,
            TimestampPrecision::Nanoseconds,
            Arc::from("UTC"),
        );
        let out = cr.next().unwrap().unwrap();

        assert_eq!(out.schema().field(0).data_type(), &DataType::Int8);
        assert_eq!(
            out.schema().field(1).data_type(),
            &DataType::Decimal128(10, 2)
        );
        assert_eq!(out.schema().field(2).data_type(), &DataType::Int16);
        assert_eq!(out.columns(), batch.columns());
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
    fn schema_preserves_metadata_and_marks_only_array_json() {
        let array = make_field_with_meta("items", DataType::Utf8, "ARRAY", "0");
        let object = make_field_with_meta("object", DataType::Utf8, "OBJECT", "0");
        let time = make_field_with_meta("time", DataType::Int64, "TIME", "9");
        let schema = Schema::new(vec![array, object.clone(), time.clone()]);
        let adjusted = adjust_schema(&schema, TimeUnit::Nanosecond, "UTC", None);
        assert_eq!(
            adjusted
                .field(0)
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some("arrow.json")
        );
        assert!(adjusted.field(0).try_canonical_extension_type().is_ok());
        assert_eq!(adjusted.field(1).metadata(), object.metadata());
        assert_eq!(adjusted.field(2).metadata(), time.metadata());
        assert_eq!(
            adjusted.field(2).data_type(),
            &DataType::Time64(TimeUnit::Nanosecond)
        );
    }

    #[test]
    fn uuid_text_is_fixed_binary_except_untyped_null() {
        let uuid_field = Field::new("id", DataType::Utf8, true).with_metadata(
            [
                ("logicalType".to_owned(), "TEXT".to_owned()),
                ("charLength".to_owned(), "0".to_owned()),
                ("byteLength".to_owned(), "16".to_owned()),
            ]
            .into(),
        );
        let null_field = Field::new("untyped", DataType::Utf8, true).with_metadata(
            [
                ("logicalType".to_owned(), "TEXT".to_owned()),
                ("charLength".to_owned(), "0".to_owned()),
                ("byteLength".to_owned(), "0".to_owned()),
            ]
            .into(),
        );
        let schema = Arc::new(Schema::new(vec![uuid_field.clone(), null_field]));
        let text = "550e8400-e29b-41d4-a716-446655440000";
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::StringArray::from(vec![Some(text), None])),
                Arc::new(arrow_array::StringArray::from(vec![None::<&str>, None])),
            ],
        )
        .unwrap();
        let mut reader = ConvertingReader::new(
            ConcatReader {
                batches: vec![batch].into_iter(),
                schema,
            },
            TimeUnit::Nanosecond,
            TimestampPrecision::Nanoseconds,
            Arc::from("UTC"),
        );
        assert_eq!(
            reader.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert!(
            reader
                .schema()
                .field(0)
                .try_canonical_extension_type()
                .is_ok()
        );
        assert_eq!(reader.schema().field(1).data_type(), &DataType::Utf8);
        let converted = reader.next().unwrap().unwrap();
        let ids = converted
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(
            ids.value(0),
            uuid::Uuid::parse_str(text).unwrap().as_bytes()
        );
        assert!(ids.is_null(1));

        let invalid_schema = Arc::new(Schema::new(vec![uuid_field]));
        let bad = RecordBatch::try_new(
            invalid_schema.clone(),
            vec![Arc::new(arrow_array::StringArray::from(vec!["not-a-uuid"]))],
        )
        .unwrap();
        let mut invalid = ConvertingReader::new(
            ConcatReader {
                batches: vec![bad].into_iter(),
                schema: invalid_schema,
            },
            TimeUnit::Nanosecond,
            TimestampPrecision::Nanoseconds,
            Arc::from("UTC"),
        );
        assert!(
            invalid
                .next()
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("invalid UUID")
        );
    }

    #[test]
    fn geography_ewkb_lifts_srid_and_strips_prefix() {
        let field = Field::new("geo", DataType::Binary, true).with_metadata(
            [
                ("logicalType".to_owned(), "BINARY".to_owned()),
                ("extTypeName".to_owned(), "GEOGRAPHY".to_owned()),
            ]
            .into(),
        );
        let schema = Arc::new(Schema::new(vec![field]));
        let described = adjust_schema(&schema, TimeUnit::Nanosecond, "UTC", None);
        assert_eq!(
            described
                .field(0)
                .metadata()
                .get("ARROW:extension:metadata")
                .map(String::as_str),
            Some("{\"crs\":\"EPSG:4326\",\"crs_type\":\"authority_code\",\"edges\":\"spherical\"}")
        );
        let mut ewkb = vec![1, 1, 0, 0, 0x20, 0xe6, 0x10, 0, 0];
        ewkb.extend_from_slice(&[0; 16]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::BinaryArray::from(vec![
                Some(ewkb.as_slice()),
                None,
            ]))],
        )
        .unwrap();
        let mut reader = ConvertingReader::new(
            ConcatReader {
                batches: vec![batch].into_iter(),
                schema,
            },
            TimeUnit::Nanosecond,
            TimestampPrecision::Nanoseconds,
            Arc::from("UTC"),
        );
        assert_eq!(
            reader
                .schema()
                .field(0)
                .metadata()
                .get("ARROW:extension:metadata")
                .map(String::as_str),
            Some("{\"crs\":\"EPSG:4326\",\"crs_type\":\"authority_code\"}")
        );
        let batch = reader.next().unwrap().unwrap();
        let geo = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();
        let mut expected = vec![1, 1, 0, 0, 0];
        expected.extend_from_slice(&[0; 16]);
        assert_eq!(geo.value(0), expected);
        assert!(geo.is_null(1));
    }

    #[test]
    fn geometry_srid_not_matching_first_batch_crs_is_rejected() {
        let field = Field::new("geom", DataType::Binary, true).with_metadata(
            [
                ("logicalType".to_owned(), "BINARY".to_owned()),
                ("extTypeName".to_owned(), "GEOMETRY".to_owned()),
            ]
            .into(),
        );
        let schema = Arc::new(Schema::new(vec![field]));
        let point = |srid: u32| {
            let mut ewkb = vec![1, 1, 0, 0, 0x20];
            ewkb.extend_from_slice(&srid.to_le_bytes());
            ewkb.extend_from_slice(&[0; 16]);
            ewkb
        };
        let batch = |value: Option<Vec<u8>>| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arrow_array::BinaryArray::from(vec![
                    value.as_deref(),
                ]))],
            )
            .unwrap()
        };
        let reader = |batches: Vec<RecordBatch>| {
            ConvertingReader::new(
                ConcatReader {
                    batches: batches.into_iter(),
                    schema: schema.clone(),
                },
                TimeUnit::Nanosecond,
                TimestampPrecision::Nanoseconds,
                Arc::from("UTC"),
            )
        };

        // A later batch with the same SRID as the declared CRS is stripped.
        let mut same = reader(vec![batch(Some(point(3857))), batch(Some(point(3857)))]);
        assert!(same.next().unwrap().is_ok());
        assert!(same.next().unwrap().is_ok());

        // A later batch with a different SRID would be mislabeled.
        let mut different = reader(vec![batch(Some(point(3857))), batch(Some(point(4326)))]);
        assert!(different.next().unwrap().is_ok());
        let error = different.next().unwrap().unwrap_err().to_string();
        assert!(error.contains("SRID 4326"), "{error}");

        // A first batch with no SRID establishes no CRS, so a later SRID is rejected.
        let mut undeclared = reader(vec![batch(None), batch(Some(point(3857)))]);
        assert_eq!(
            undeclared
                .schema()
                .field(0)
                .metadata()
                .get("ARROW:extension:metadata"),
            None
        );
        assert!(undeclared.next().unwrap().is_ok());
        assert!(undeclared.next().unwrap().is_err());
    }

    #[test]
    fn json_bindings_reject_batches_inconsistent_with_the_first() {
        let first = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![1])),
                Arc::new(arrow_array::StringArray::from(vec!["x"])),
            ],
        )
        .unwrap();
        let fewer_columns = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)])),
            vec![Arc::new(arrow_array::Int64Array::from(vec![2]))],
        )
        .unwrap();
        let different_type = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Utf8, false),
                Field::new("b", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(arrow_array::StringArray::from(vec!["2"])),
                Arc::new(arrow_array::StringArray::from(vec!["y"])),
            ],
        )
        .unwrap();
        for bad in [fewer_columns, different_type] {
            let error = arrow_batches_to_json_bindings(&[first.clone(), bad]).unwrap_err();
            assert_eq!(error.status, Status::InvalidData);
        }
    }

    #[test]
    fn time_values_are_rescaled_to_a_valid_arrow_unit() {
        for (scale, raw, expected_type, expected) in [
            // 1234.5 seconds at scale 1 -> milliseconds.
            (
                "1",
                12_345i64,
                DataType::Time32(TimeUnit::Millisecond),
                1_234_500i64,
            ),
            // 1.2345 seconds at scale 4 -> microseconds (Time32 cannot hold it).
            (
                "4",
                12_345,
                DataType::Time64(TimeUnit::Microsecond),
                1_234_500,
            ),
            // 1.23456 seconds at scale 5 -> microseconds.
            (
                "5",
                123_456,
                DataType::Time64(TimeUnit::Microsecond),
                1_234_560,
            ),
            // 1.2345678 seconds at scale 7 -> nanoseconds.
            (
                "7",
                12_345_678,
                DataType::Time64(TimeUnit::Nanosecond),
                1_234_567_800,
            ),
            // Scales already matching a unit pass through unchanged.
            ("3", 1_234, DataType::Time32(TimeUnit::Millisecond), 1_234),
            ("9", 1_234, DataType::Time64(TimeUnit::Nanosecond), 1_234),
        ] {
            let field = make_field_with_meta("t", DataType::Int64, "TIME", scale);
            let schema = Arc::new(Schema::new(vec![field]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arrow_array::Int64Array::from(vec![
                    Some(raw),
                    None,
                ]))],
            )
            .unwrap();
            let mut reader = ConvertingReader::new(
                ConcatReader {
                    batches: vec![batch].into_iter(),
                    schema,
                },
                TimeUnit::Nanosecond,
                TimestampPrecision::Nanoseconds,
                Arc::from("UTC"),
            );
            let out = reader.next().unwrap().unwrap();
            let column = out.column(0);
            assert_eq!(column.data_type(), &expected_type, "scale {scale}");
            let as_int64 = if matches!(expected_type, DataType::Time32(_)) {
                let int32 = arrow_cast::cast(column, &DataType::Int32).unwrap();
                arrow_cast::cast(&int32, &DataType::Int64).unwrap()
            } else {
                arrow_cast::cast(column, &DataType::Int64).unwrap()
            };
            let values = as_int64
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            assert_eq!(values.value(0), expected, "scale {scale}");
            assert!(values.is_null(1));
        }
    }

    #[test]
    fn test_adjust_schema_timestamp_ntz_int64_to_timestamp_ns() {
        let f = make_field_with_meta("ts", DataType::Int64, "TIMESTAMP_NTZ", "9");
        let schema = Schema::new(vec![f]);
        let result = adjust_schema(&schema, TimeUnit::Nanosecond, "UTC", None);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn test_adjust_schema_timestamp_ltz_uses_session_timezone() {
        let f = make_field_with_meta("ts", DataType::Int64, "TIMESTAMP_LTZ", "6");
        let schema = Schema::new(vec![f]);
        let result = adjust_schema(&schema, TimeUnit::Microsecond, "America/Los_Angeles", None);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::from("America/Los_Angeles"))
            )
        );
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
            TimeUnit::Nanosecond,
            TimestampPrecision::Nanoseconds,
            Arc::from("UTC"),
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
    fn compact_ntz_and_ltz_timestamps_use_their_exact_decimal_scale() {
        for (logical_type, timezone) in [
            ("TIMESTAMP_NTZ", None),
            ("TIMESTAMP_LTZ", Some(Arc::from("America/Los_Angeles"))),
        ] {
            for (scale, raw, unit, expected) in [
                ("1", 1_234i64, TimeUnit::Millisecond, 123_400i64),
                ("4", 1_234_567, TimeUnit::Microsecond, 123_456_700),
                ("7", 1_234_567_890, TimeUnit::Nanosecond, 123_456_789_000),
            ] {
                let field = make_field_with_meta("ts", DataType::Int64, logical_type, scale);
                let schema = Arc::new(Schema::new(vec![field]));
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(arrow_array::Int64Array::from(vec![
                        Some(raw),
                        None,
                    ]))],
                )
                .unwrap();
                let mut reader = ConvertingReader::new(
                    ConcatReader {
                        batches: vec![batch].into_iter(),
                        schema,
                    },
                    TimeUnit::Nanosecond,
                    TimestampPrecision::Nanoseconds,
                    Arc::from("America/Los_Angeles"),
                );
                let out = reader.next().unwrap().unwrap();
                assert_eq!(
                    out.schema().field(0).data_type(),
                    &DataType::Timestamp(unit, timezone.clone()),
                    "{logical_type}({scale})"
                );
                let values = arrow_cast::cast(out.column(0), &DataType::Int64).unwrap();
                let values = values
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap();
                assert_eq!(values.value(0), expected, "{logical_type}({scale})");
                assert!(values.is_null(1));
            }
        }
    }

    #[test]
    fn negative_submicrosecond_timestamps_floor_when_downscaled() {
        let compact = Arc::new(arrow_array::Int64Array::from(vec![-1])) as ArrayRef;
        let converted =
            convert_timestamp_ntz(&compact, 7, TimeUnit::Microsecond, false, None).unwrap();
        let values = converted
            .as_any()
            .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(values.value(0), -1); // -100 ns floors to -1 µs.

        let epoch = arrow_array::Int64Array::from(vec![-1]);
        let fraction = arrow_array::Int32Array::from(vec![999_999_999]);
        let source = arrow_array::StructArray::from(vec![
            (
                Arc::new(Field::new("epoch", DataType::Int64, false)),
                Arc::new(epoch.clone()) as ArrayRef,
            ),
            (
                Arc::new(Field::new("fraction", DataType::Int32, false)),
                Arc::new(fraction.clone()) as ArrayRef,
            ),
        ]);
        let converted = build_timestamp_from_epoch_fraction(
            &epoch,
            &fraction,
            &source,
            9,
            false,
            DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();
        let values = converted
            .as_any()
            .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(values.value(0), -1); // -1 ns floors to -1 µs.
    }

    #[test]
    fn nanosecond_overflow_check_does_not_reject_representable_microseconds() {
        let epoch = arrow_array::Int64Array::from(vec![253_402_300_799i64]);
        let fraction = arrow_array::Int32Array::from(vec![0]);
        let source = arrow_array::StructArray::from(vec![
            (
                Arc::new(Field::new("epoch", DataType::Int64, false)),
                Arc::new(epoch.clone()) as ArrayRef,
            ),
            (
                Arc::new(Field::new("fraction", DataType::Int32, false)),
                Arc::new(fraction.clone()) as ArrayRef,
            ),
        ]);
        let microseconds = build_timestamp_from_epoch_fraction(
            &epoch,
            &fraction,
            &source,
            9,
            true,
            DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();
        let values = microseconds
            .as_any()
            .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(values.value(0), 253_402_300_799_000_000);
        assert!(
            build_timestamp_from_epoch_fraction(
                &epoch,
                &fraction,
                &source,
                9,
                true,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            )
            .is_err()
        );
    }

    #[test]
    fn test_adjust_schema_timestamp_ntz_capped_by_ts_unit() {
        let f = make_field_with_meta("ts", DataType::Int64, "TIMESTAMP_NTZ", "9");
        let schema = Schema::new(vec![f]);
        let result = adjust_schema(&schema, TimeUnit::Microsecond, "UTC", None);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_build_timestamp_tz_2field_year9999() {
        use arrow_array::{Int32Array, Int64Array, StructArray};
        use arrow_schema::Field as SchemaField;

        let epoch_ms: i64 = 253_402_300_799_000; // 9999-12-31T23:59:59Z in milliseconds
        let epoch_arr = Int64Array::from(vec![epoch_ms]);
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
            3,
            true,
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
        )
        .unwrap();

        let ts = result
            .as_any()
            .downcast_ref::<arrow_array::TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), epoch_ms);
    }

    #[test]
    fn test_build_timestamp_tz_2field_uses_exact_scale_and_ignores_offset() {
        use arrow_array::{Int32Array, Int64Array, StructArray};

        for (scale, epoch, expected_ns) in [
            (0, 123i64, 123_000_000_000i64),
            (1, 1_234, 123_400_000_000),
            (1, -1, -100_000_000),
            (2, 12_345, 123_450_000_000),
            (3, 123_456, 123_456_000_000),
        ] {
            let epoch = Int64Array::from(vec![epoch]);
            let offset = Int32Array::from(vec![960]); // -08:00 display offset
            let struct_arr = StructArray::from(vec![
                (
                    Arc::new(Field::new("epoch", DataType::Int64, false)),
                    Arc::new(epoch.clone()) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("offset", DataType::Int32, false)),
                    Arc::new(offset.clone()) as ArrayRef,
                ),
            ]);
            let result = build_timestamp_tz_2field(
                &epoch,
                &offset,
                &struct_arr,
                scale,
                false,
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
            )
            .unwrap();
            let timestamps = result
                .as_any()
                .downcast_ref::<arrow_array::TimestampNanosecondArray>()
                .unwrap();
            assert_eq!(timestamps.value(0), expected_ns, "scale {scale}");
        }
    }

    #[test]
    fn test_build_timestamp_from_epoch_fraction_clamps_scale() {
        let epoch = arrow_array::Int64Array::from(vec![1i64]);
        let fraction = arrow_array::Int32Array::from(vec![5i32]);
        let struct_arr = arrow_array::StructArray::from(vec![
            (
                Arc::new(Field::new("epoch", DataType::Int64, true)),
                Arc::new(epoch.clone()) as ArrayRef,
            ),
            (
                Arc::new(Field::new("fraction", DataType::Int32, true)),
                Arc::new(fraction.clone()) as ArrayRef,
            ),
        ]);
        for (scale, expected) in [(-1, 6_000_000_000), (10, 1_000_000_005)] {
            let result = build_timestamp_from_epoch_fraction(
                &epoch,
                &fraction,
                &struct_arr,
                scale,
                false,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            )
            .expect("scale is clamped");
            let ts = result
                .as_any()
                .downcast_ref::<arrow_array::TimestampNanosecondArray>()
                .unwrap();
            assert_eq!(ts.value(0), expected, "scale {scale}");
        }
    }

    #[test]
    fn test_build_timestamp_tz_3field_ignores_display_offset() {
        let epoch = arrow_array::Int64Array::from(vec![1i64]);
        let fraction = arrow_array::Int32Array::from(vec![500_000_000i32]);
        let offset = arrow_array::Int32Array::from(vec![960i32]); // -08:00
        let struct_arr = arrow_array::StructArray::from(vec![
            (
                Arc::new(Field::new("epoch", DataType::Int64, false)),
                Arc::new(epoch.clone()) as ArrayRef,
            ),
            (
                Arc::new(Field::new("fraction", DataType::Int32, false)),
                Arc::new(fraction.clone()) as ArrayRef,
            ),
            (
                Arc::new(Field::new("offset", DataType::Int32, false)),
                Arc::new(offset.clone()) as ArrayRef,
            ),
        ]);
        let result = convert_timestamp_tz(
            &(Arc::new(struct_arr) as ArrayRef),
            9,
            TimeUnit::Nanosecond,
            false,
        )
        .unwrap();
        let timestamps = result
            .as_any()
            .downcast_ref::<arrow_array::TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(timestamps.value(0), 1_500_000_000);
    }

    #[test]
    fn json_bindings_null_and_decimal256() {
        use arrow_buffer::i256;
        let large: i256 = "123456789012345678901234567890123456789".parse().unwrap();
        let decimals =
            arrow_array::Decimal256Array::from(vec![Some(large), Some(i256::from_i128(-5)), None])
                .with_precision_and_scale(39, 2)
                .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("null", DataType::Null, true),
            Field::new("decimal", DataType::Decimal256(39, 2), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::NullArray::new(3)), Arc::new(decimals)],
        )
        .unwrap();
        assert_eq!(
            arrow_batches_to_json_bindings(&[batch]).unwrap(),
            r#"{"1":{"type":"TEXT","value":[null,null,null]},"2":{"type":"FIXED","value":["1234567890123456789012345678901234567.89","-0.05",null]}}"#
        );
        let negative_scale =
            arrow_array::Decimal256Array::from(vec![Some(i256::from_i128(-12)), None])
                .with_precision_and_scale(10, -2)
                .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "decimal",
                DataType::Decimal256(10, -2),
                true,
            )])),
            vec![Arc::new(negative_scale)],
        )
        .unwrap();
        assert_eq!(
            arrow_batches_to_json_bindings(&[batch]).unwrap(),
            r#"{"1":{"type":"FIXED","value":["-1200",null]}}"#
        );
    }

    #[test]
    fn json_bindings_uuid_extension_and_plain_binary() {
        use arrow_array::FixedSizeBinaryArray;
        let bytes: &[u8] = &[
            0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab,
            0xcd, 0xef,
        ];
        let ids = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![Some(bytes), None].into_iter(),
            16,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("uuid", DataType::FixedSizeBinary(16), true)
                .with_metadata([("ARROW:extension:name".into(), "arrow.uuid".into())].into()),
            Field::new("binary", DataType::FixedSizeBinary(16), true),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(ids.clone()), Arc::new(ids)]).unwrap();
        assert_eq!(
            arrow_batches_to_json_bindings(&[batch]).unwrap(),
            r#"{"1":{"type":"TEXT","value":["12345678-90ab-cdef-1234-567890abcdef",null]},"2":{"type":"BINARY","value":["1234567890abcdef1234567890abcdef",null]}}"#
        );
    }

    #[test]
    fn json_bindings_decode_dictionary_across_batches() {
        use arrow_array::{DictionaryArray, Int8Array, StringArray, types::Int8Type};
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("one"), None, Some("three")]));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        )]));
        let first = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                DictionaryArray::<Int8Type>::try_new(
                    Int8Array::from(vec![Some(2), None]),
                    values.clone(),
                )
                .unwrap(),
            )],
        )
        .unwrap();
        let second = RecordBatch::try_new(
            schema,
            vec![Arc::new(
                DictionaryArray::<Int8Type>::try_new(
                    Int8Array::from(vec![Some(0), Some(1)]),
                    values,
                )
                .unwrap(),
            )],
        )
        .unwrap();
        assert_eq!(
            arrow_batches_to_json_bindings(&[first.clone(), second]).unwrap(),
            r#"{"1":{"type":"TEXT","value":["three",null,"one",null]}}"#
        );
        assert_eq!(
            arrow_batches_to_json_bindings(&[first.slice(0, 1)]).unwrap(),
            r#"{"1":{"type":"TEXT","value":"three"}}"#
        );
    }

    #[test]
    fn json_bindings_decode_dictionary_decimal_uuid_and_null() {
        use arrow_array::{
            Decimal256Array, DictionaryArray, FixedSizeBinaryArray, Int8Array, NullArray,
            types::Int8Type,
        };
        use arrow_buffer::i256;
        let keys = Int8Array::from(vec![Some(0), None]);
        let decimal = Decimal256Array::from(vec![i256::from_i128(-5)])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let uuid = FixedSizeBinaryArray::try_from_iter(
            [
                uuid::Uuid::parse_str("12345678-90ab-cdef-1234-567890abcdef")
                    .unwrap()
                    .into_bytes(),
            ]
            .into_iter(),
        )
        .unwrap();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(
                DictionaryArray::<Int8Type>::try_new(keys.clone(), Arc::new(decimal)).unwrap(),
            ),
            Arc::new(DictionaryArray::<Int8Type>::try_new(keys.clone(), Arc::new(uuid)).unwrap()),
            Arc::new(
                DictionaryArray::<Int8Type>::try_new(keys, Arc::new(NullArray::new(1))).unwrap(),
            ),
        ];
        let schema = Arc::new(Schema::new(vec![
            Field::new("decimal", columns[0].data_type().clone(), true),
            Field::new("uuid", columns[1].data_type().clone(), true)
                .with_metadata([("ARROW:extension:name".into(), "arrow.uuid".into())].into()),
            Field::new("null", columns[2].data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema, columns).unwrap();
        assert_eq!(
            arrow_batches_to_json_bindings(&[batch]).unwrap(),
            r#"{"1":{"type":"FIXED","value":["-0.05",null]},"2":{"type":"TEXT","value":["12345678-90ab-cdef-1234-567890abcdef",null]},"3":{"type":"TEXT","value":[null,null]}}"#
        );
    }

    #[test]
    fn json_bindings_reject_unsupported_dictionary_values() {
        use arrow_array::{DictionaryArray, Int8Array, ListArray, types::Int8Type};
        let values: ArrayRef = Arc::new(ListArray::from_iter_primitive::<
            arrow_array::types::Int32Type,
            _,
            _,
        >(vec![Some(vec![Some(1)])]));
        let dict =
            DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![Some(0)]), values).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "list",
                dict.data_type().clone(),
                true,
            )])),
            vec![Arc::new(dict)],
        )
        .unwrap();
        let error = arrow_batches_to_json_bindings(&[batch]).unwrap_err();
        assert_eq!(error.status, Status::NotImplemented);
        assert!(error.message.contains("List"));
    }

    #[test]
    #[ignore = "requires SNOWFLAKE_URI and a live Snowflake connection"]
    fn live_json_bind_types() {
        use adbc_core::{
            Connection as _, Database as _, Driver as _,
            options::{OptionDatabase, OptionValue},
        };
        use arrow_array::{
            Decimal256Array, DictionaryArray, FixedSizeBinaryArray, Int8Array, NullArray,
            RecordBatchIterator, StringArray, cast::AsArray, types::Int8Type,
        };
        use arrow_buffer::i256;
        let uri = std::env::var("SNOWFLAKE_URI").expect("SNOWFLAKE_URI is required");
        let mut driver = crate::Driver::default();
        let mut db = driver.new_database().unwrap();
        db.set_option(OptionDatabase::Uri, OptionValue::String(uri))
            .unwrap();
        for (env, option) in [
            ("SNOWFLAKE_DATABASE", "adbc.snowflake.sql.db"),
            ("SNOWFLAKE_SCHEMA", "adbc.snowflake.sql.schema"),
        ] {
            if let Ok(value) = std::env::var(env) {
                db.set_option(
                    OptionDatabase::Other(option.into()),
                    OptionValue::String(value),
                )
                .unwrap();
            }
        }
        let mut conn = db.new_connection().unwrap();
        let uuid = uuid::Uuid::parse_str("12345678-90ab-cdef-1234-567890abcdef").unwrap();
        let uuid_field = Field::new("uuid", DataType::FixedSizeBinary(16), true)
            .with_metadata([("ARROW:extension:name".into(), "arrow.uuid".into())].into());
        let decimal_field = Field::new("decimal", DataType::Decimal256(20, 2), true);
        let dict_values: ArrayRef = Arc::new(StringArray::from(vec!["first", "second"]));
        let dict = |keys: Vec<Option<i8>>| {
            Arc::new(
                DictionaryArray::<Int8Type>::try_new(Int8Array::from(keys), dict_values.clone())
                    .unwrap(),
            ) as ArrayRef
        };
        let params = Arc::new(Schema::new(vec![
            Field::new("null", DataType::Null, true),
            decimal_field.clone(),
            uuid_field.clone(),
            Field::new(
                "dict",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ),
        ]));
        let one = RecordBatch::try_new(
            params.clone(),
            vec![
                Arc::new(NullArray::new(1)),
                Arc::new(
                    Decimal256Array::from(vec![i256::from_i128(1234)])
                        .with_precision_and_scale(20, 2)
                        .unwrap(),
                ),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter([uuid.as_bytes().as_slice()].into_iter())
                        .unwrap(),
                ),
                dict(vec![Some(0)]),
            ],
        )
        .unwrap();
        let two = RecordBatch::try_new(
            params.clone(),
            vec![
                Arc::new(NullArray::new(1)),
                Arc::new(
                    Decimal256Array::from(vec![None::<i256>])
                        .with_precision_and_scale(20, 2)
                        .unwrap(),
                ),
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        [None::<&[u8]>].into_iter(),
                        16,
                    )
                    .unwrap(),
                ),
                dict(vec![Some(1)]),
            ],
        )
        .unwrap();
        for (sql, batch, expected) in [
            ("SELECT ?", one.slice(0, 1), Some("first")),
            ("SELECT ?", one.slice(0, 1), None),
        ] {
            let col = if expected.is_some() { 3 } else { 0 };
            let mut stmt = conn.new_statement().unwrap();
            stmt.set_sql_query(sql).unwrap();
            stmt.bind(
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![params.field(col).as_ref().clone()])),
                    vec![batch.column(col).clone()],
                )
                .unwrap(),
            )
            .unwrap();
            let row = stmt.execute().unwrap().next().unwrap().unwrap();
            if let Some(value) = expected {
                assert_eq!(row.column(0).as_string::<i32>().value(0), value);
            } else {
                assert!(row.column(0).is_null(0));
            }
        }
        let mut decimal = conn.new_statement().unwrap();
        decimal.set_sql_query("SELECT ?").unwrap();
        decimal
            .bind(
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![decimal_field])),
                    vec![one.column(1).clone()],
                )
                .unwrap(),
            )
            .unwrap();
        let decimal_row = decimal.execute().unwrap().next().unwrap().unwrap();
        let text = arrow_cast::cast(decimal_row.column(0), &DataType::Utf8).unwrap();
        assert_eq!(text.as_string::<i32>().value(0), "12.34");
        let mut select_uuid = conn.new_statement().unwrap();
        select_uuid.set_sql_query("SELECT ?::UUID").unwrap();
        select_uuid
            .bind(
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![uuid_field])),
                    vec![one.column(2).clone()],
                )
                .unwrap(),
            )
            .unwrap();
        let uuid_row = select_uuid.execute().unwrap().next().unwrap().unwrap();
        assert_eq!(
            uuid_row.column(0).as_string::<i32>().value(0),
            "12345678-90ab-cdef-1234-567890abcdef"
        );

        let table = format!("ADBC_BIND_TYPES_{}", uuid::Uuid::new_v4().simple());
        let mut create = conn.new_statement().unwrap();
        create
            .set_sql_query(format!(
                "CREATE TEMP TABLE {table} (N VARCHAR, D NUMBER(20,2), U VARCHAR(36), T VARCHAR)"
            ))
            .unwrap();
        create.execute_update().unwrap();
        let mut insert = conn.new_statement().unwrap();
        insert
            .set_sql_query(format!("INSERT INTO {table} VALUES (?, ?, ?, ?)"))
            .unwrap();
        insert
            .bind_stream(Box::new(RecordBatchIterator::new(
                vec![Ok(one), Ok(two)].into_iter(),
                params,
            )))
            .unwrap();
        assert_eq!(insert.execute_update().unwrap(), Some(2));
        let mut check = conn.new_statement().unwrap();
        check
            .set_sql_query(format!("SELECT N, D, U, T FROM {table} ORDER BY T"))
            .unwrap();
        let rows = check.execute().unwrap().next().unwrap().unwrap();
        assert_eq!(rows.num_rows(), 2);
        assert!(rows.column(0).is_null(0) && rows.column(0).is_null(1));
        assert_eq!(rows.column(2).as_string::<i32>().value(0), uuid.to_string());
        assert_eq!(rows.column(3).as_string::<i32>().value(0), "first");
        assert!(rows.column(1).is_null(1) && rows.column(2).is_null(1));
        assert_eq!(rows.column(3).as_string::<i32>().value(1), "second");
        let amounts = arrow_cast::cast(rows.column(1), &DataType::Utf8).unwrap();
        assert_eq!(amounts.as_string::<i32>().value(0), "12.34");
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
    fn float16_bindings_promote_finite_values_and_null_nonfinite() {
        let source = arrow_array::Float32Array::from(vec![
            Some(1.5),
            Some(f32::NAN),
            Some(f32::INFINITY),
            None,
        ]);
        let column = arrow_cast::cast(&source, &DataType::Float16).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "half",
            DataType::Float16,
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![column]).unwrap();
        assert_eq!(
            arrow_batches_to_json_bindings(&[batch.slice(0, 1)]).unwrap(),
            "{\"1\":{\"type\":\"REAL\",\"value\":\"1.5\"}}"
        );
        assert_eq!(
            arrow_batches_to_json_bindings(&[batch]).unwrap(),
            "{\"1\":{\"type\":\"REAL\",\"value\":[\"1.5\",null,null,null]}}"
        );
    }

    #[test]
    fn bindings_stream_columns_across_batches_without_changing_encoding() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("binary", DataType::Binary, true),
            Field::new("date", DataType::Date32, true),
        ]));
        let first = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::StringArray::from(vec![Some("a\"\u{0001}\n")])),
                Arc::new(arrow_array::BinaryArray::from(vec![Some(
                    &[0xff, 0x00][..],
                )])),
                Arc::new(arrow_array::Date32Array::from(vec![Some(1)])),
            ],
        )
        .unwrap();
        let second = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::StringArray::from(vec![None::<&str>])),
                Arc::new(arrow_array::BinaryArray::from(vec![None::<&[u8]>])),
                Arc::new(arrow_array::Date32Array::from(vec![None])),
            ],
        )
        .unwrap();
        assert_eq!(
            arrow_batches_to_json_bindings(&[first, second]).unwrap(),
            r#"{"1":{"type":"TEXT","value":["a\"\u0001\n",null]},"2":{"type":"BINARY","value":["ff00",null]},"3":{"type":"DATE","value":["86400000",null]}}"#
        );
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
                return Err(SnowflakeError::invalid_data()
                    .message("injected execution failure")
                    .into());
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
