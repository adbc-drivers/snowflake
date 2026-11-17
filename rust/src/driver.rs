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

// src/driver.rs
use std::cell::RefCell;
use std::future::Future;
use std::sync::Arc;

use adbc_core::{
    error::Result,
    options::{OptionDatabase, OptionValue},
};
use arrow_array::RecordBatchReader;
use arrow_schema::TimeUnit;
use driverbase::error::ErrorHelper;
use driverbase::telemetry::{Exporter, Telemetry};
use sf_core::apis::database_driver_v1::{
    ApiError, BindingType, ColumnMetadata, DatabaseDriverV1, DriverProviders, ExecuteQueryResult,
    Handle, ResultSetDescriptor, ResultSetInfo, WrapperPresets,
};
use sf_core::apis::operation_ctx::OperationCtx;
use tokio::runtime::Runtime;
use tracing::Instrument;

use crate::error::SnowflakeError;
use crate::telemetry::app_layer;

thread_local! {
    static WORKER_DISPATCH: RefCell<Option<tracing::dispatcher::DefaultGuard>> =
        const { RefCell::new(None) };
}

use crate::database::Database;

/// Controls the Arrow time unit used for Snowflake TIMESTAMP columns.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) enum TimestampPrecision {
    /// Nanosecond precision (default). May overflow for dates outside 1677–2262.
    #[default]
    Nanoseconds,
    /// Microsecond precision. Safe for all Snowflake-representable dates.
    Microseconds,
    /// Nanosecond precision; returns an error when a value would overflow.
    NanosecondsErrorOnOverflow,
}

impl TimestampPrecision {
    pub(crate) fn time_unit(self) -> TimeUnit {
        match self {
            Self::Microseconds => TimeUnit::Microsecond,
            _ => TimeUnit::Nanosecond,
        }
    }
}

/// Shares the current cancellable operation with a handle that outlives a
/// borrow of the connection or statement.
pub(crate) enum OperationCancelHandle {
    Connection(Arc<std::sync::Mutex<Arc<OperationCtx>>>),
    Statement(Arc<std::sync::Mutex<StatementOperationState>>),
}

impl adbc_core::CancelHandle for OperationCancelHandle {
    fn try_cancel(&self) -> Result<()> {
        let operation = match self {
            Self::Connection(operation) => {
                Arc::clone(&operation.lock().unwrap_or_else(|e| e.into_inner()))
            }
            Self::Statement(state) => {
                let state = state.lock().unwrap_or_else(|e| e.into_inner());
                if !state.active {
                    return Err(adbc_core::error::Error {
                        message: "AdbcStatementCancel: no active query to cancel".into(),
                        status: adbc_core::error::Status::InvalidState,
                        vendor_code: 0,
                        sqlstate: [0; 5],
                        details: None,
                    });
                }
                Arc::clone(&state.context)
            }
        };
        // Cancellation may wake tasks; never run it while holding the state lock.
        operation.cancel();
        Ok(())
    }
}

pub(crate) struct StatementOperationState {
    pub(crate) context: Arc<OperationCtx>,
    active: bool,
}

impl Default for StatementOperationState {
    fn default() -> Self {
        Self {
            context: Arc::new(OperationCtx::with_own_token()),
            active: false,
        }
    }
}

/// Keeps execution active through errors, unwinding, and returned stream ownership.
pub(crate) struct StatementOperation {
    state: Arc<std::sync::Mutex<StatementOperationState>>,
    context: Arc<OperationCtx>,
}

impl StatementOperation {
    pub(crate) fn start(state: &Arc<std::sync::Mutex<StatementOperationState>>) -> Self {
        let context = Arc::new(OperationCtx::with_own_token());
        let previous = {
            let mut state = state.lock().unwrap_or_else(|e| e.into_inner());
            state.active = true;
            std::mem::replace(&mut state.context, Arc::clone(&context))
        };
        previous.cancel();
        Self {
            state: Arc::clone(state),
            context,
        }
    }

    pub(crate) fn wrap_reader(
        self,
        reader: Box<dyn RecordBatchReader + Send>,
    ) -> Box<dyn RecordBatchReader + Send> {
        Box::new(OperationReader {
            reader,
            operation: Some(self),
        })
    }
}

impl std::ops::Deref for StatementOperation {
    type Target = OperationCtx;

    fn deref(&self) -> &Self::Target {
        &self.context
    }
}

impl Drop for StatementOperation {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if Arc::ptr_eq(&state.context, &self.context) {
            state.active = false;
        }
    }
}

struct OperationReader {
    reader: Box<dyn RecordBatchReader + Send>,
    operation: Option<StatementOperation>,
}

impl Iterator for OperationReader {
    type Item = std::result::Result<arrow_array::RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let operation = self.operation.as_ref()?;
        let mut result = if operation.token().is_cancelled() {
            None
        } else {
            self.reader.next()
        };
        if operation.token().is_cancelled() {
            let error: adbc_core::error::Error =
                SnowflakeError::status(adbc_core::error::Status::Cancelled)
                    .message("query cancelled")
                    .into();
            result = Some(Err(arrow_schema::ArrowError::ExternalError(Box::new(
                error,
            ))));
        }
        if !matches!(result, Some(Ok(_))) {
            self.operation = None;
        }
        result
    }
}

impl RecordBatchReader for OperationReader {
    fn schema(&self) -> Arc<arrow_schema::Schema> {
        self.reader.schema()
    }
}

pub(crate) struct Inner {
    pub runtime: Runtime,
    pub sf: DatabaseDriverV1,
    pub(crate) dispatch: tracing::Dispatch,
    pub(crate) telemetry: Arc<Telemetry>,
    tracing_config: std::sync::Mutex<()>,
    log_level: crate::telemetry::LogLevelHandle,
}

pub(crate) struct QueryResult {
    pub reader: Box<dyn RecordBatchReader + Send>,
    pub descriptor: ResultSetDescriptor,
    pub has_arrow_metadata: bool,
}

pub(crate) struct PreparedResult {
    pub reader: Box<dyn RecordBatchReader + Send>,
    pub query_id: String,
    pub columns: Vec<ColumnMetadata>,
    pub number_of_binds: i32,
    pub binds: Vec<ColumnMetadata>,
}

impl Inner {
    pub(crate) fn new() -> Result<Self> {
        let telemetry = Arc::new(Telemetry::new(
            "snowflake",
            env!("CARGO_PKG_VERSION"),
            "snowflake",
        ));
        let (app_sink, log_level) = app_layer(Arc::clone(&telemetry));
        let log_manager = sf_core::logging::LogManager::with_app_sink(
            sf_core::logging::LoggingConfig {
                enabled: false,
                ..Default::default()
            },
            app_sink,
            Default::default(),
        )
        .map_err(|error| {
            SnowflakeError::internal_no_location()
                .context("initialize sf_core telemetry")
                .format(format_args!("{error}"))
                .to_adbc()
        })?;
        let dispatch = log_manager.dispatch().clone();
        // sf_core's transfer and query futures are large; unoptimized builds
        // overflow tokio's default 2 MiB worker stacks (e.g. during ingest
        // uploads), so give workers more room. Keep a thread-local guard for
        // the entire worker lifetime, not just the on_thread_start callback.
        let worker_dispatch = dispatch.clone();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_stack_size(8 * 1024 * 1024)
            .on_thread_start(move || {
                WORKER_DISPATCH.with(|slot| {
                    *slot.borrow_mut() = Some(tracing::dispatcher::set_default(&worker_dispatch));
                });
            })
            .on_thread_stop(|| {
                WORKER_DISPATCH.with(|slot| {
                    slot.borrow_mut().take();
                });
            })
            .build()
            .map_err(|error| {
                SnowflakeError::io()
                    .context("create tokio runtime")
                    .format(format_args!("{error}"))
                    .to_adbc()
            })?;
        Ok(Self {
            runtime,
            sf: DatabaseDriverV1::with_providers(DriverProviders {
                log_manager: Some(log_manager),
                wrapper_presets: WrapperPresets {
                    optimistic_alter_session_param_cache: true,
                    ..WrapperPresets::default()
                },
                ..DriverProviders::default()
            }),
            dispatch,
            telemetry,
            tracing_config: std::sync::Mutex::new(()),
            log_level,
        })
    }

    pub(crate) fn configure_tracing(
        &self,
        exporter: Option<Exporter>,
        folder: Option<&str>,
        log_level: Option<&str>,
        client_config_file: &str,
    ) -> Result<()> {
        // Go reapplies tracing (fatal by default) on each connect, globally.
        // Exporters remain independent: an unset exporter must not disable one
        // another database configured. Reuse the dispatcher to avoid teardown races.
        let _guard = self
            .tracing_config
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // reload rebuilds tracing-core callsite interest using the current
        // dispatcher; without this guard it can disable sf_core's span pipeline.
        tracing::dispatcher::with_default(&self.dispatch, || {
            self.log_level.configure(log_level, client_config_file)
        })?;
        if exporter.is_none() && folder.is_none() && self.telemetry.is_enabled() {
            return Ok(());
        }
        self.telemetry.configure(exporter, folder)
    }

    /// Run the sf_core future under the same subscriber and parent span as its
    /// ADBC entry point. Tokio worker threads also hold the subscriber guard.
    pub(crate) fn block_on<F: Future>(&self, future: F) -> F::Output {
        tracing::dispatcher::with_default(&self.dispatch, || {
            self.runtime
                .block_on(future.instrument(tracing::Span::current()))
        })
    }

    pub(crate) fn traced<T>(
        &self,
        name: &'static str,
        parent: Option<&str>,
        body: impl FnOnce(&tracing::Span) -> Result<T>,
    ) -> Result<T> {
        self.telemetry.traced(&self.dispatch, name, parent, body)
    }

    /// Read the current session setting on each result/schema creation. The
    /// optimistic ALTER SESSION cache keeps this fresh when a response omits it.
    pub(crate) fn session_timezone(&self, connection: Handle) -> Result<Arc<str>> {
        let setting = self
            .block_on(
                self.sf
                    .connection_get_parameter(connection, "TIMEZONE".into()),
            )
            .map_err(crate::error::api_error_to_adbc_error)?;
        let timezone = match setting {
            Some(sf_core::config::settings::Setting::String(value)) => value,
            None => "UTC".to_string(),
            Some(value) => {
                return Err(SnowflakeError::invalid_data()
                    .context("read session timezone")
                    .format(format_args!("invalid timezone setting: {value:?}"))
                    .into());
            }
        };
        timezone
            .parse::<arrow_array::timezone::Tz>()
            .map_err(|error| {
                SnowflakeError::invalid_data()
                    .context("parse session timezone")
                    .format(format_args!("invalid timezone '{timezone}': {error}"))
                    .to_adbc()
            })?;
        Ok(Arc::from(timezone))
    }

    /// Execute SQL using a temporary sf_core statement and release the
    /// statement on every execution path. Execution errors take precedence over
    /// cleanup errors; a release error is returned only after successful execution.
    pub(crate) fn execute_temporary_statement(
        &self,
        operation_ctx: Option<&OperationCtx>,
        connection: Handle,
        query: impl Into<String>,
        timeout_seconds: Option<u32>,
    ) -> Result<()> {
        self.execute_temporary_statement_tagged(
            operation_ctx,
            connection,
            query,
            timeout_seconds,
            None,
        )
    }

    pub(crate) fn execute_temporary_statement_tagged(
        &self,
        operation_ctx: Option<&OperationCtx>,
        connection: Handle,
        query: impl Into<String>,
        timeout_seconds: Option<u32>,
        query_tag: Option<&str>,
    ) -> Result<()> {
        let statement = self
            .sf
            .statement_new(connection)
            .map_err(crate::error::api_error_to_adbc_error)?;
        let result = self.execute_query_with_timeout_tagged(
            operation_ctx,
            statement,
            query.into(),
            timeout_seconds,
            query_tag,
        );
        let release = self.sf.statement_release(statement);
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

    /// Execute a query while forwarding a per-statement timeout to sf_core.
    /// Metadata and session helpers use [`Self::execute_query`] and therefore
    /// retain the core/session default.
    pub fn execute_query_with_timeout<'a>(
        &self,
        operation_ctx: Option<&OperationCtx>,
        statement: Handle,
        query: String,
        bindings: Option<BindingType<'a>>,
        timeout_seconds: Option<u32>,
    ) -> Result<QueryResult> {
        let result = self
            .execute_query_raw_with_timeout(
                operation_ctx,
                statement,
                query,
                bindings,
                timeout_seconds,
            )
            .map_err(crate::error::api_error_to_adbc_error)?;
        self.acquire_execute_result(result)
    }

    pub(crate) fn execute_query_with_timeout_tagged(
        &self,
        operation_ctx: Option<&OperationCtx>,
        statement: Handle,
        query: String,
        timeout_seconds: Option<u32>,
        query_tag: Option<&str>,
    ) -> Result<QueryResult> {
        if let Some(query_tag) = query_tag {
            self.block_on(self.sf.statement_set_option(
                statement,
                sf_core::config::param_registry::param_names::QUERY_TAG.into(),
                sf_core::config::settings::Setting::String(query_tag.to_owned()),
            ))
            .map_err(crate::error::api_error_to_adbc_error)?;
        }
        self.execute_query_with_timeout(operation_ctx, statement, query, None, timeout_seconds)
    }

    /// Like [`Self::execute_query_with_timeout`], also recording the query ID:
    /// the result's on success and, as in Go, the failed query's when the
    /// server reports one.
    pub(crate) fn execute_query_recording_id<'a>(
        &self,
        operation_ctx: Option<&OperationCtx>,
        statement: Handle,
        query: String,
        bindings: Option<BindingType<'a>>,
        timeout_seconds: Option<u32>,
        last_query_id: &mut Option<String>,
    ) -> Result<QueryResult> {
        let result = self
            .execute_query_raw_with_timeout(
                operation_ctx,
                statement,
                query,
                bindings,
                timeout_seconds,
            )
            .map_err(|error| {
                *last_query_id = crate::error::failed_query_id(&error);
                crate::error::api_error_to_adbc_error(error)
            })?;
        let result = self.acquire_execute_result(result)?;
        *last_query_id = Some(result.descriptor.query_id.clone());
        Ok(result)
    }

    /// Execute through sf_core without erasing its typed error. Callers must
    /// pass successful results to [`Self::acquire_execute_result`] so result
    /// handles are released on every stream-acquisition path.
    pub(crate) fn execute_query_raw_with_timeout<'a>(
        &self,
        operation_ctx: Option<&OperationCtx>,
        statement: Handle,
        query: String,
        bindings: Option<BindingType<'a>>,
        timeout_seconds: Option<u32>,
    ) -> std::result::Result<ExecuteQueryResult, ApiError> {
        self.block_on(self.sf.statement_set_sql_query(statement, query))?;
        self.block_on(self.sf.statement_execute_query(
            operation_ctx,
            statement,
            bindings,
            timeout_seconds,
            None,
            None,
        ))
    }

    pub(crate) fn acquire_execute_result(&self, result: ExecuteQueryResult) -> Result<QueryResult> {
        let ExecuteQueryResult::Single { info, .. } = result else {
            return Err(SnowflakeError::not_implemented()
                .context("acquire query result")
                .message("multi-statement query results are not supported")
                .into());
        };
        self.acquire_result(info)
    }

    /// Synchronize SQL to sf_core and issue its describe-only prepare request.
    pub(crate) fn prepare_statement(
        &self,
        operation_ctx: Option<&OperationCtx>,
        statement: Handle,
        query: String,
    ) -> Result<PreparedResult> {
        self.block_on(self.sf.statement_set_sql_query(statement, query))
            .map_err(crate::error::api_error_to_adbc_error)?;
        let result = self
            .block_on(self.sf.statement_prepare(operation_ctx, statement, None))
            .map_err(crate::error::api_error_to_adbc_error)?;
        Ok(PreparedResult {
            reader: result.stream,
            query_id: result.query_id,
            columns: result.columns,
            number_of_binds: result.number_of_binds,
            binds: result.binds,
        })
    }

    /// Acquire a core result stream and release its handle on every path.
    pub fn acquire_result(&self, result: ResultSetInfo) -> Result<QueryResult> {
        let stream = self.block_on(async {
            let has_arrow_metadata = match (
                result.descriptor.query_result_format.as_deref(),
                result.descriptor.row_count,
            ) {
                (Some("arrow"), Some(0)) => {
                    // This snapshots stored chunk metadata without consuming the
                    // stream or making a request. Schema-only results have no IPC.
                    let chunks = self
                        .sf
                        .result_set_get_chunks(result.handle)
                        .await?
                        .chunk_data;
                    matches!(
                        chunks.inline,
                        sf_core::apis::database_driver_v1::InlineData::ArrowIpc(_)
                    ) || !chunks.remote_chunks.is_empty()
                }
                (Some("arrow"), _) => true,
                _ => false,
            };
            let reader = self
                .sf
                .result_set_get_stream_without_nullable_metadata(result.handle)
                .await?;
            Ok::<_, ApiError>((reader, has_arrow_metadata))
        });
        let release = self.sf.result_set_release(result.handle);

        let (reader, has_arrow_metadata) = match stream {
            Ok(reader) => {
                release.map_err(crate::error::api_error_to_adbc_error)?;
                reader
            }
            Err(error) => {
                // The release was attempted above even though stream acquisition failed.
                let _ = release;
                return Err(crate::error::api_error_to_adbc_error(error));
            }
        };

        Ok(QueryResult {
            reader,
            descriptor: result.descriptor,
            has_arrow_metadata,
        })
    }
}

/// Snowflake ADBC Driver.
pub struct Driver {
    pub(crate) inner: Arc<Inner>,
}

/// All drivers in a process share one sf_core instance, runtime, and tracing
/// dispatcher, as sf_core's own language wrappers do. It is never dropped:
/// tracing-core (0.1.36) upgrades and drops other dispatchers while holding
/// its global callsite lock when a new dispatcher is created, and dropping
/// sf_core's subscriber emits a tracing event, so creating one driver while
/// another is torn down deadlocks. Spans are flushed when a database is
/// released instead of at teardown.
static SHARED_INNER: std::sync::LazyLock<Arc<Inner>> =
    std::sync::LazyLock::new(|| Arc::new(Inner::new().expect("failed to initialize driver")));

impl Default for Driver {
    fn default() -> Self {
        Self {
            inner: Arc::clone(&SHARED_INNER),
        }
    }
}

impl adbc_core::Driver for Driver {
    type DatabaseType = Database;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        self.new_database_with_opts(std::iter::empty())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let db_handle = self.inner.sf.database_new();
        let sf_settings: std::collections::HashMap<String, sf_core::config::settings::Setting> =
            Default::default();
        let mut db = Database {
            inner: self.inner.clone(),
            db_handle,
            sf_settings,
            surfaced_warnings: Default::default(),
            timestamp_precision: TimestampPrecision::default(),
            geography_output_format: Default::default(),
            geometry_output_format: Default::default(),
            auth_type: None,
            okta_url: None,
            max_retry_count: None,
            duration_options: Default::default(),
            credential_options: Default::default(),
            tls_skip_verify: false,
            tracing_level: None,
            client_config_file: String::new(),
            private_key_password: None,
            trace_parent: None,
            traces_exporter: None,
            traces_folder_path: None,
            session_parameters: Default::default(),
        };
        db.set_initial_options(opts)?;
        Ok(db)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::Driver as _;

    #[test]
    fn driver_default_creates_successfully() {
        let _driver = Driver::default();
    }

    #[test]
    fn new_database_succeeds_with_no_options() {
        let mut driver = Driver::default();
        let _db = driver.new_database().expect("new_database failed");
    }
}
