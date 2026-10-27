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

// src/connection.rs

/// Arrow library version reported via get_info(DriverArrowVersion).
/// Must match the `arrow-array` version resolved in Cargo.lock.
const ARROW_VERSION: &str = "v58.4.1";

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use adbc_core::{
    Optionable, constants,
    error::Result,
    options::{InfoCode, ObjectDepth, OptionConnection, OptionValue},
    schemas,
};
use arrow_array::{Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
use arrow_schema::Schema;
use driverbase::InfoRegistry;
use driverbase::error::ErrorHelper;
use driverbase::telemetry::{TRACE_PARENT, validate_trace_parent};
use sf_core::apis::database_driver_v1::{ConnectionInfo, Handle};
use sf_core::apis::operation_ctx::OperationCtx;

use crate::database::{GEOGRAPHY_OUTPUT_FORMAT, GEOMETRY_OUTPUT_FORMAT, GeoOutputFormat};
use crate::driver::{Inner, OperationCancelHandle, TimestampPrecision};
use crate::error::SnowflakeError;
use crate::statement::{Statement, adjust_schema};

fn info_registry(vendor_version: &str) -> InfoRegistry {
    let mut registry = InfoRegistry::new();
    registry.add_string(InfoCode::VendorName, "Snowflake");
    registry.add_bool(InfoCode::VendorSql, true);
    registry.add_bool(InfoCode::VendorSubstrait, false);
    registry.add_string(InfoCode::DriverName, "ADBC Snowflake Driver (Rust)");
    registry.add_string(InfoCode::DriverVersion, env!("CARGO_PKG_VERSION"));
    registry.add_int64(
        InfoCode::DriverAdbcVersion,
        constants::ADBC_VERSION_1_1_0 as i64,
    );
    registry.add_string(InfoCode::VendorVersion, vendor_version);
    registry.add_string(InfoCode::DriverArrowVersion, ARROW_VERSION);
    registry
}

pub struct Connection {
    pub(crate) inner: Arc<Inner>,
    pub(crate) conn_handle: Handle,
    pub(crate) autocommit: bool,
    pub(crate) trace_parent: Option<String>,
    pub(crate) match_exactly: bool,
    pub(crate) timestamp_precision: TimestampPrecision,
    pub(crate) geography_output_format: GeoOutputFormat,
    pub(crate) geometry_output_format: GeoOutputFormat,
    /// Only connection-level operations use this slot; statement operations
    /// have separate contexts and are not cancelled by this handle.
    pub(crate) operation: Arc<Mutex<Arc<OperationCtx>>>,
}

type CleanupTask = Box<dyn FnOnce() + Send + 'static>;

fn attempt_connection_cleanup(
    close: impl FnOnce() + Send + 'static,
    release: impl FnOnce(),
    spawn: impl FnOnce(CleanupTask) -> std::io::Result<std::thread::JoinHandle<()>>,
) {
    // Runtime::block_on panics when entered from some Tokio runtime contexts.
    // Always run close on a dedicated OS thread, wait for sf_core's bounded
    // close behavior, then release the handle. Drop remains infallible even if
    // spawning, closing, joining, or releasing fails or panics.
    let close = Box::new(move || {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(close));
    });
    let spawned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| spawn(close)));
    if let Ok(Ok(thread)) = spawned {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = thread.join();
        }));
    }
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(release));
}

pub(crate) fn cleanup_connection_handle(
    inner: &Arc<Inner>,
    conn_handle: Handle,
    trace_parent: Option<&str>,
) {
    let close_inner = Arc::clone(inner);
    let close_parent = trace_parent.map(str::to_owned);
    attempt_connection_cleanup(
        move || {
            let _ = close_inner.traced("connectionImpl.Close", close_parent.as_deref(), |_| {
                close_inner
                    .block_on(close_inner.sf.connection_close(conn_handle))
                    .map_err(crate::error::api_error_to_adbc_error)
            });
        },
        || {
            let _ = inner.traced("connectionImpl.Release", trace_parent, |_| {
                inner
                    .sf
                    .connection_release(conn_handle)
                    .map_err(crate::error::api_error_to_adbc_error)
            });
        },
        |close| {
            std::thread::Builder::new()
                .name("snowflake-connection-close".into())
                .spawn(close)
        },
    );
}

impl Drop for Connection {
    fn drop(&mut self) {
        cleanup_connection_handle(&self.inner, self.conn_handle, self.trace_parent.as_deref());
    }
}

pub(crate) fn single_batch_reader(
    batch: RecordBatch,
) -> Box<dyn RecordBatchReader + Send + 'static> {
    let schema = batch.schema();
    Box::new(RecordBatchIterator::new(std::iter::once(Ok(batch)), schema))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionIdentifierKind {
    Database,
    Schema,
}

/// Preserve exact Snowflake identifier spelling while using a cancellable
/// statement for session changes.
fn session_identifier_sql(kind: SessionIdentifierKind, name: &str) -> String {
    let keyword = match kind {
        SessionIdentifierKind::Database => "DATABASE",
        SessionIdentifierKind::Schema => "SCHEMA",
    };
    format!("USE {keyword} {}", quote_identifier(name))
}

pub(crate) fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub(crate) fn qualified_table_identifier(
    catalog: Option<&str>,
    db_schema: Option<&str>,
    table_name: &str,
) -> String {
    let table_name = quote_identifier(table_name);
    match (catalog, db_schema) {
        (Some(catalog), Some(db_schema)) => format!(
            "{}.{}.{}",
            quote_identifier(catalog),
            quote_identifier(db_schema),
            table_name
        ),
        (None, Some(db_schema)) => format!("{}.{}", quote_identifier(db_schema), table_name),
        (Some(catalog), None) => format!("{}..{}", quote_identifier(catalog), table_name),
        (None, None) => table_name,
    }
}

#[derive(Debug, PartialEq, Eq)]
struct DescColumn {
    data_type: String,
    primary_key: String,
    comment: Option<String>,
}

fn desc_text(batch: &RecordBatch, column: usize, row: usize) -> Result<Option<&str>> {
    let value = batch.column(column);
    if value.is_null(row) {
        return Ok(None);
    }
    if let Some(strings) = value.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(strings.value(row)));
    }
    if let Some(strings) = value
        .as_any()
        .downcast_ref::<arrow_array::LargeStringArray>()
    {
        return Ok(Some(strings.value(row)));
    }
    if let Some(strings) = value
        .as_any()
        .downcast_ref::<arrow_array::StringViewArray>()
    {
        return Ok(Some(strings.value(row)));
    }
    Err(SnowflakeError::invalid_data()
        .message(format!("DESC TABLE column {column} is not a string"))
        .into())
}

fn desc_columns(reader: &mut dyn RecordBatchReader) -> Result<HashMap<String, DescColumn>> {
    let mut columns = HashMap::new();
    for batch in reader {
        let batch = batch.map_err(|error| {
            adbc_core::error::Error::from(SnowflakeError::io().message(error.to_string()))
        })?;
        if batch.num_columns() < 10 {
            return Err(SnowflakeError::invalid_data()
                .message("DESC TABLE returned fewer than 10 columns")
                .into());
        }
        // DESC TABLE returns name, type, kind, null?, default, primary key,
        // unique key, check, expression, comment, ... in that order.
        for row in 0..batch.num_rows() {
            let required = |index| {
                desc_text(&batch, index, row)?.ok_or_else(|| {
                    adbc_core::error::Error::from(
                        SnowflakeError::invalid_data()
                            .message(format!("DESC TABLE returned NULL in column {index}")),
                    )
                })
            };
            columns.insert(
                required(0)?.to_owned(),
                DescColumn {
                    data_type: required(1)?.to_owned(),
                    primary_key: required(5)?.to_owned(),
                    comment: desc_text(&batch, 9, row)?.map(str::to_owned),
                },
            );
        }
    }
    Ok(columns)
}

fn apply_desc_metadata(schema: &Schema, columns: &HashMap<String, DescColumn>) -> Result<Schema> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            let desc = columns.get(field.name()).ok_or_else(|| {
                adbc_core::error::Error::from(
                    SnowflakeError::invalid_data()
                        .message(format!("DESC TABLE omitted column {}", field.name())),
                )
            })?;
            let extension = field
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str);
            // Go's DESC TABLE mapping replaces metadata on ARRAY and EWKB geo fields.
            if matches!(extension, Some("arrow.json" | "geoarrow.wkb")) {
                return Ok(field.as_ref().clone());
            }
            let mut metadata = HashMap::from([
                ("DATA_TYPE".to_owned(), desc.data_type.clone()),
                ("PRIMARY_KEY".to_owned(), desc.primary_key.clone()),
            ]);
            if let Some(comment) = &desc.comment {
                metadata.insert("COMMENT".to_owned(), comment.clone());
            }
            if let Some(extension) = extension {
                metadata.insert("ARROW:extension:name".to_owned(), extension.to_owned());
                if let Some(value) = field.metadata().get("ARROW:extension:metadata") {
                    metadata.insert("ARROW:extension:metadata".to_owned(), value.clone());
                }
            }
            Ok(field.as_ref().clone().with_metadata(metadata))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

impl Connection {
    pub(crate) fn begin_operation(&self) -> Arc<OperationCtx> {
        let operation = Arc::new(OperationCtx::with_own_token());
        *self.operation.lock().unwrap_or_else(|e| e.into_inner()) = Arc::clone(&operation);
        operation
    }

    pub(crate) fn with_metadata_reader<T>(
        &self,
        operation: &OperationCtx,
        query: String,
        consume: impl FnOnce(&mut dyn RecordBatchReader) -> Result<T>,
    ) -> Result<T> {
        let statement = self
            .inner
            .sf
            .statement_new(self.conn_handle)
            .map_err(crate::error::api_error_to_adbc_error)?;
        let result = (|| {
            let mut query_result = self.inner.execute_query_with_timeout(
                Some(operation),
                statement,
                query,
                None,
                None,
            )?;
            let value = consume(query_result.reader.as_mut());
            if operation.token().is_cancelled() {
                return Err(SnowflakeError::cancelled()
                    .message("operation cancelled")
                    .into());
            }
            value
        })();
        let release = self.inner.sf.statement_release(statement);
        match result {
            Ok(value) => {
                release.map_err(crate::error::api_error_to_adbc_error)?;
                Ok(value)
            }
            Err(error) => {
                let _ = release;
                Err(error)
            }
        }
    }

    fn context_names(&self) -> Result<(Option<String>, Option<String>)> {
        // Deliberately extract only cached context names. ConnectionInfo also carries
        // sensitive tokens, which must never be surfaced through ADBC options.
        let operation = self.begin_operation();
        let ConnectionInfo {
            database, schema, ..
        } = self
            .inner
            .block_on(operation.run(
                "connection_get_info",
                self.inner.sf.connection_get_info(self.conn_handle),
            ))
            .map_err(crate::error::api_error_to_adbc_error)?;
        Ok((database, schema))
    }

    pub(crate) fn set_autocommit(&mut self, enabled: bool) -> Result<()> {
        let operation = self.begin_operation();
        self.inner.execute_temporary_statement(
            Some(&operation),
            self.conn_handle,
            format!("ALTER SESSION SET AUTOCOMMIT = {enabled}"),
            None,
        )?;
        self.autocommit = enabled;
        Ok(())
    }

    fn set_current_identifier(&self, kind: SessionIdentifierKind, name: &str) -> Result<()> {
        if name.trim().is_empty() {
            return Err(SnowflakeError::invalid_argument()
                .message("session identifier must not be empty")
                .into());
        }
        let operation = self.begin_operation();
        let sql = session_identifier_sql(kind, name);
        self.inner
            .execute_temporary_statement(Some(&operation), self.conn_handle, sql, None)
    }

    fn set_geo_output_format(&mut self, key: &str, value: &OptionValue) -> Result<()> {
        let format = GeoOutputFormat::parse(key, value)?;
        let session_parameter = if key == GEOGRAPHY_OUTPUT_FORMAT {
            "GEOGRAPHY_OUTPUT_FORMAT"
        } else {
            "GEOMETRY_OUTPUT_FORMAT"
        };
        let operation = self.begin_operation();
        self.inner.execute_temporary_statement(
            Some(&operation),
            self.conn_handle,
            format!(
                "ALTER SESSION SET {session_parameter} = '{}'",
                format.as_str()
            ),
            None,
        )?;
        if key == GEOGRAPHY_OUTPUT_FORMAT {
            self.geography_output_format = format;
        } else {
            self.geometry_output_format = format;
        }
        Ok(())
    }
}

/// Parse an autocommit option value, rejecting anything that is not an
/// explicit boolean spelling so a typo cannot silently disable autocommit.
fn parse_autocommit(value: &OptionValue) -> Result<bool> {
    match value {
        OptionValue::String(s) => match s.as_str() {
            "true" | "1" | "enabled" => Ok(true),
            "false" | "0" | "disabled" => Ok(false),
            other => Err(SnowflakeError::invalid_argument()
                .message(format!(
                    "invalid autocommit value {other:?}: expected true/false, 1/0, or enabled/disabled"
                ))
                .into()),
        },
        _ => Err(SnowflakeError::invalid_argument()
            .message("autocommit value must be a string")
            .into()),
    }
}

const MATCH_EXACTLY: &str = "adbc.connection.get_objects.match_exactly";

impl Optionable for Connection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        let key_str = key.as_ref();
        if key_str == TRACE_PARENT {
            let parent = SnowflakeError::option_as_string(&TRACE_PARENT, &value)
                .map_err(adbc_core::error::Error::from)?;
            self.trace_parent = validate_trace_parent(parent)?;
            return Ok(());
        }
        if key_str == MATCH_EXACTLY {
            self.match_exactly = SnowflakeError::option_as_bool(&MATCH_EXACTLY, &value)?;
            return Ok(());
        }
        match &key {
            OptionConnection::AutoCommit => {
                let enabled = parse_autocommit(&value)?;
                self.set_autocommit(enabled)
            }
            OptionConnection::CurrentCatalog => {
                let name = SnowflakeError::option_as_string(&key, &value)
                    .map_err(adbc_core::error::Error::from)?;
                self.set_current_identifier(SessionIdentifierKind::Database, name)
            }
            OptionConnection::CurrentSchema => {
                let name = SnowflakeError::option_as_string(&key, &value)
                    .map_err(adbc_core::error::Error::from)?;
                self.set_current_identifier(SessionIdentifierKind::Schema, name)
            }
            OptionConnection::Other(k)
                if k == GEOGRAPHY_OUTPUT_FORMAT || k == GEOMETRY_OUTPUT_FORMAT =>
            {
                self.set_geo_output_format(k, &value)
            }
            _ => Err(SnowflakeError::set_unknown_option(&key_str).into()),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let key_str = key.as_ref();
        if key_str == TRACE_PARENT {
            return Ok(self.trace_parent.clone().unwrap_or_default());
        }
        if key_str == MATCH_EXACTLY {
            return Ok(if self.match_exactly { "true" } else { "false" }.to_owned());
        }
        match &key {
            OptionConnection::AutoCommit => {
                Ok(if self.autocommit { "true" } else { "false" }.to_string())
            }
            OptionConnection::CurrentCatalog => self.context_names()?.0.ok_or_else(|| {
                SnowflakeError::not_found()
                    .message("current catalog is not set")
                    .into()
            }),
            OptionConnection::CurrentSchema => self.context_names()?.1.ok_or_else(|| {
                SnowflakeError::not_found()
                    .message("current schema is not set")
                    .into()
            }),
            OptionConnection::Other(k) if k == GEOGRAPHY_OUTPUT_FORMAT => {
                Ok(self.geography_output_format.as_str().to_string())
            }
            OptionConnection::Other(k) if k == GEOMETRY_OUTPUT_FORMAT => {
                Ok(self.geometry_output_format.as_str().to_string())
            }
            _ => Err(SnowflakeError::get_unknown_option(&key_str).into()),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        let key_str = key.as_ref();
        Err(SnowflakeError::get_unknown_option(&key_str).into())
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        let key_str = key.as_ref();
        Err(SnowflakeError::get_unknown_option(&key_str).into())
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        let key_str = key.as_ref();
        Err(SnowflakeError::get_unknown_option(&key_str).into())
    }
}

impl adbc_core::Connection for Connection {
    type StatementType = Statement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        let stmt_handle = self
            .inner
            .sf
            .statement_new(self.conn_handle)
            .map_err(crate::error::api_error_to_adbc_error)?;
        Ok(Statement {
            inner: self.inner.clone(),
            stmt_handle,
            conn_handle: self.conn_handle,
            trace_parent: None,
            connection_trace_parent: self.trace_parent.clone(),
            query: None,
            ingest_state: driverbase::bulk_ingest::BulkIngestState::new(),
            query_tag: None,
            query_timeout_seconds: None,
            timestamp_precision: self.timestamp_precision,
            ingest_options: crate::ingest::IngestOptions::default(),
            bound: crate::statement::Bound::Empty,
            last_query_id: None,
            prepared: false,
            described: Mutex::new(None),
            operation: Arc::new(Mutex::new(Arc::new(OperationCtx::with_own_token()))),
        })
    }

    fn get_cancel_handle(&self) -> Box<dyn adbc_core::CancelHandle> {
        Box::new(OperationCancelHandle {
            operation: Arc::clone(&self.operation),
        })
    }

    #[allow(refining_impl_trait)]
    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.inner.traced(
            "connectionImpl.GetInfo",
            self.trace_parent.as_deref(),
            |_| {
                let need_vendor_version = codes
                    .as_ref()
                    .is_none_or(|s| s.contains(&InfoCode::VendorVersion));
                let vendor_version = if need_vendor_version {
                    let operation = self.begin_operation();
                    self.inner
                        .block_on(
                            operation.run(
                                "connection_get_server_version",
                                self.inner
                                    .sf
                                    .connection_get_server_version(self.conn_handle),
                            ),
                        )
                        .map_err(crate::error::api_error_to_adbc_error)?
                        .unwrap_or_default()
                } else {
                    String::new()
                };

                Ok(info_registry(&vendor_version).get_info(codes).build())
            },
        )
    }

    #[allow(refining_impl_trait)]
    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.inner.traced(
            "connectionImpl.GetObjects",
            self.trace_parent.as_deref(),
            |_| {
                crate::get_objects::execute_get_objects(
                    self,
                    &depth,
                    catalog,
                    db_schema,
                    table_name,
                    table_type,
                    column_name,
                )
            },
        )
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        self.inner.traced(
            "connectionImpl.GetTableSchema",
            self.trace_parent.as_deref(),
            |_| {
                let operation = self.begin_operation();
                let qualified = qualified_table_identifier(catalog, db_schema, table_name);
                let stmt_handle = self
                    .inner
                    .sf
                    .statement_new(self.conn_handle)
                    .map_err(crate::error::api_error_to_adbc_error)?;

                let result = (|| {
                    let prepared = self.inner.prepare_statement(
                        Some(&operation),
                        stmt_handle,
                        format!("SELECT * FROM {qualified}"),
                    )?;
                    let schema = prepared.reader.schema();
                    if schema.fields().is_empty() {
                        return Err(SnowflakeError::internal_no_location()
                            .message(format!(
                                "sf_core prepared an empty schema for table {qualified}"
                            ))
                            .into());
                    }
                    let timezone = self.inner.session_timezone(self.conn_handle)?;
                    let adjusted = adjust_schema(
                        &schema,
                        self.timestamp_precision.time_unit(),
                        &timezone,
                        None,
                    );
                    let columns = self.with_metadata_reader(
                        &operation,
                        format!("DESC TABLE {qualified}"),
                        desc_columns,
                    )?;
                    apply_desc_metadata(&adjusted, &columns)
                })();
                let release = self.inner.sf.statement_release(stmt_handle);
                match result {
                    Ok(schema) => {
                        release.map_err(crate::error::api_error_to_adbc_error)?;
                        Ok(schema)
                    }
                    Err(error) => {
                        let _ = release;
                        Err(error)
                    }
                }
            },
        )
    }

    #[allow(refining_impl_trait)]
    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.inner.traced(
            "connectionImpl.GetTableTypes",
            self.trace_parent.as_deref(),
            |_| {
                let array = Arc::new(StringArray::from(vec!["TABLE", "VIEW"]));
                let batch =
                    RecordBatch::try_new(schemas::GET_TABLE_TYPES_SCHEMA.clone(), vec![array])
                        .map_err(|e| {
                            adbc_core::error::Error::from(
                                SnowflakeError::internal_no_location().message(e.to_string()),
                            )
                        })?;
                Ok(single_batch_reader(batch))
            },
        )
    }

    #[allow(refining_impl_trait)]
    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        crate::statistics::get_statistic_names()
    }

    #[allow(refining_impl_trait)]
    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.inner.traced(
            "connectionImpl.GetStatistics",
            self.trace_parent.as_deref(),
            |_| {
                crate::statistics::get_statistics(self, catalog, db_schema, table_name, approximate)
            },
        )
    }

    fn commit(&mut self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        inner.traced(
            "connectionImpl.Commit",
            self.trace_parent.as_deref(),
            |_| {
                if self.autocommit {
                    return Err(SnowflakeError::invalid_state()
                        .message("cannot commit: autocommit is enabled")
                        .into());
                }
                let operation = self.begin_operation();
                self.inner.execute_temporary_statement(
                    Some(&operation),
                    self.conn_handle,
                    "COMMIT",
                    None,
                )
            },
        )
    }

    fn rollback(&mut self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        inner.traced(
            "connectionImpl.Rollback",
            self.trace_parent.as_deref(),
            |_| {
                if self.autocommit {
                    return Err(SnowflakeError::invalid_state()
                        .message("cannot rollback: autocommit is enabled")
                        .into());
                }
                let operation = self.begin_operation();
                self.inner.execute_temporary_statement(
                    Some(&operation),
                    self.conn_handle,
                    "ROLLBACK",
                    None,
                )
            },
        )
    }

    #[allow(refining_impl_trait)]
    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(SnowflakeError::not_implemented()
            .context("read partition")
            .message("reading partitions is not supported")
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::error::Status;
    use arrow_array::ArrayRef;
    use arrow_schema::{DataType, Field};

    fn fake_connection() -> Connection {
        let driver = crate::driver::Driver::default();
        Connection {
            inner: driver.inner.clone(),
            conn_handle: Handle { id: 0, magic: 0 },
            autocommit: true,
            trace_parent: None,
            match_exactly: false,
            timestamp_precision: TimestampPrecision::Nanoseconds,
            geography_output_format: GeoOutputFormat::GeoJson,
            geometry_output_format: GeoOutputFormat::GeoJson,
            operation: Arc::new(Mutex::new(Arc::new(OperationCtx::with_own_token()))),
        }
    }

    #[test]
    fn info_registry_reports_standard_strings_booleans_and_int64() {
        use arrow_array::{BooleanArray, Int64Array, UInt32Array, UnionArray};

        let registry = info_registry("10.1.2");
        for (code, expected) in [
            (InfoCode::VendorName, "Snowflake"),
            (InfoCode::DriverName, "ADBC Snowflake Driver (Rust)"),
            (InfoCode::DriverVersion, env!("CARGO_PKG_VERSION")),
            (InfoCode::VendorVersion, "10.1.2"),
            (InfoCode::DriverArrowVersion, ARROW_VERSION),
        ] {
            let mut reader = registry.get_info(Some(HashSet::from([code]))).build();
            let batch = reader.next().unwrap().unwrap();
            assert_eq!(batch.schema(), schemas::GET_INFO_SCHEMA.clone());
            assert_eq!(batch.num_rows(), 1);
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            assert_eq!(names.value(0), u32::from(&code));
            let union = batch
                .column(1)
                .as_any()
                .downcast_ref::<UnionArray>()
                .unwrap();
            assert_eq!(union.type_id(0), 0);
            let value = union.value(0);
            assert_eq!(
                value
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                expected
            );
        }

        for (code, expected) in [
            (InfoCode::VendorSql, true),
            (InfoCode::VendorSubstrait, false),
        ] {
            let mut reader = registry.get_info(Some(HashSet::from([code]))).build();
            let batch = reader.next().unwrap().unwrap();
            assert_eq!(batch.num_rows(), 1);
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            assert_eq!(names.value(0), u32::from(&code));
            let union = batch
                .column(1)
                .as_any()
                .downcast_ref::<UnionArray>()
                .unwrap();
            assert_eq!(union.type_id(0), 1);
            let value = union.value(0);
            assert_eq!(
                value
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(0),
                expected
            );
        }

        let code = InfoCode::DriverAdbcVersion;
        let mut reader = registry.get_info(Some(HashSet::from([code]))).build();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(names.value(0), u32::from(&code));
        let union = batch
            .column(1)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        assert_eq!(union.type_id(0), 2);
        let value = union.value(0);
        assert_eq!(
            value
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            constants::ADBC_VERSION_1_1_0 as i64
        );
    }

    #[test]
    fn info_registry_ignores_unsupported_codes() {
        use arrow_array::UInt32Array;

        let registry = info_registry("10.1.2");
        let unsupported = InfoCode::VendorArrowVersion;
        let mut reader = registry
            .get_info(Some(HashSet::from([unsupported])))
            .build();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.schema(), schemas::GET_INFO_SCHEMA.clone());
        assert_eq!(batch.num_rows(), 0);

        let mut reader = registry
            .get_info(Some(HashSet::from([unsupported, InfoCode::VendorSql])))
            .build();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(names.value(0), u32::from(&InfoCode::VendorSql));
    }

    #[test]
    fn connection_cleanup_waits_for_close_before_release_after_close_panic() {
        use std::sync::Mutex;

        let calls = Arc::new(Mutex::new(Vec::new()));
        let close_calls = Arc::clone(&calls);
        let release_calls = Arc::clone(&calls);
        attempt_connection_cleanup(
            move || {
                close_calls.lock().unwrap().push("close");
                panic!("simulated close failure");
            },
            move || release_calls.lock().unwrap().push("release"),
            |close| std::thread::Builder::new().spawn(close),
        );

        assert_eq!(&*calls.lock().unwrap(), &["close", "release"]);
    }

    #[test]
    fn connection_cleanup_releases_when_thread_spawn_fails() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let released = Arc::new(AtomicBool::new(false));
        let released_by_cleanup = Arc::clone(&released);
        attempt_connection_cleanup(
            || panic!("close must not run without its dedicated thread"),
            move || released_by_cleanup.store(true, Ordering::SeqCst),
            |_close| Err(std::io::Error::other("simulated spawn failure")),
        );

        assert!(released.load(Ordering::SeqCst));
    }

    #[test]
    fn session_identifiers_are_quoted_without_trimming() {
        assert_eq!(
            session_identifier_sql(SessionIdentifierKind::Database, " db\" name "),
            "USE DATABASE \" db\"\" name \""
        );
        assert_eq!(
            session_identifier_sql(SessionIdentifierKind::Schema, " schema "),
            "USE SCHEMA \" schema \""
        );
        assert_eq!(
            session_identifier_sql(SessionIdentifierKind::Database, "normal_name"),
            "USE DATABASE \"normal_name\""
        );
    }

    #[test]
    fn get_options_return_unknown_key_for_missing_values() {
        let conn = fake_connection();
        let key = OptionConnection::Other("unknown".into());
        for error in [
            conn.get_option_string(key.clone()).unwrap_err(),
            conn.get_option_bytes(key.clone()).unwrap_err(),
            conn.get_option_int(key.clone()).unwrap_err(),
            conn.get_option_double(key).unwrap_err(),
        ] {
            assert_eq!(error.status, Status::NotFound);
            assert_eq!(error.message, "[Snowflake] unknown option \"unknown\"");
        }
    }

    #[test]
    fn trace_parent_option_validates_and_clears() {
        let mut conn = fake_connection();
        let key = OptionConnection::Other(TRACE_PARENT.into());
        let parent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        assert_eq!(conn.get_option_string(key.clone()).unwrap(), "");
        conn.set_option(key.clone(), OptionValue::String(parent.into()))
            .unwrap();
        assert_eq!(conn.get_option_string(key.clone()).unwrap(), parent);
        for value in [OptionValue::String("invalid".into()), OptionValue::Int(1)] {
            assert_eq!(
                conn.set_option(key.clone(), value).unwrap_err().status,
                Status::InvalidArguments,
            );
            assert_eq!(conn.get_option_string(key.clone()).unwrap(), parent);
        }
        conn.set_option(key.clone(), OptionValue::String(String::new()))
            .unwrap();
        assert_eq!(conn.get_option_string(key).unwrap(), "");
    }

    #[test]
    fn connection_cancel_is_scoped_to_current_operation() {
        use adbc_core::Connection as _;
        let conn = fake_connection();
        let handle = conn.get_cancel_handle();
        handle.try_cancel().unwrap();
        let old = conn.begin_operation();
        let statement_operation = OperationCtx::with_own_token();
        let (started, waiting) = std::sync::mpsc::channel();
        let mut started = Some(started);
        let cancelled = std::thread::scope(|scope| {
            let cancel = &handle;
            let cancelling = scope.spawn(move || {
                waiting.recv().unwrap();
                cancel.try_cancel().unwrap();
            });
            let result = conn.inner.block_on(old.run(
                "connection_cancel_unit",
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
        assert!(!statement_operation.token().is_cancelled());
        let next = conn.begin_operation();
        assert!(!next.token().is_cancelled());
        assert!(
            conn.inner
                .block_on(next.run(
                    "connection_cancel_unit",
                    std::future::ready(Ok::<(), sf_core::apis::database_driver_v1::ApiError>(())),
                ))
                .is_ok()
        );
        handle.try_cancel().unwrap();
    }

    #[test]
    fn connection_geo_options_validate_before_alter_session() {
        let mut conn = fake_connection();
        let key = OptionConnection::Other(GEOGRAPHY_OUTPUT_FORMAT.into());
        let error = conn
            .set_option(key.clone(), OptionValue::String("WKT".into()))
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert_eq!(conn.get_option_string(key).unwrap(), "GEOJSON");
        let error = conn
            .set_option(
                OptionConnection::Other("unknown.option".into()),
                OptionValue::String("ignored".into()),
            )
            .unwrap_err();
        assert_eq!(error.status, Status::NotImplemented);
        assert_eq!(
            error.message,
            "[Snowflake] unknown option \"unknown.option\""
        );
    }

    #[test]
    fn autocommit_accepts_only_explicit_boolean_spellings() {
        for (value, expected) in [
            ("true", true),
            ("1", true),
            ("enabled", true),
            ("false", false),
            ("0", false),
            ("disabled", false),
        ] {
            assert_eq!(
                parse_autocommit(&OptionValue::String(value.into())).unwrap(),
                expected,
                "{value}"
            );
        }
        for value in [
            OptionValue::String("yes".into()),
            OptionValue::String("TRUE ".into()),
            OptionValue::Int(1),
        ] {
            assert_eq!(
                parse_autocommit(&value).unwrap_err().status,
                Status::InvalidArguments
            );
        }
    }

    #[test]
    fn match_exactly_accepts_only_true_and_false() {
        let mut conn = fake_connection();
        let key = OptionConnection::Other(MATCH_EXACTLY.into());
        assert_eq!(conn.get_option_string(key.clone()).unwrap(), "false");
        conn.set_option(key.clone(), OptionValue::String("true".into()))
            .unwrap();
        assert_eq!(conn.get_option_string(key.clone()).unwrap(), "true");
        for value in ["enabled", "TRUE", "", "true "] {
            let error = conn
                .set_option(key.clone(), OptionValue::String(value.into()))
                .unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            assert!(error.message.contains("must be 'true' or 'false'"));
            assert_eq!(conn.get_option_string(key.clone()).unwrap(), "true");
        }
        let error = conn
            .set_option(key.clone(), OptionValue::Int(1))
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(error.message.contains("must be a string"));
        assert_eq!(conn.get_option_string(key.clone()).unwrap(), "true");
        conn.set_option(key.clone(), OptionValue::String("false".into()))
            .unwrap();
        assert_eq!(conn.get_option_string(key).unwrap(), "false");
    }

    #[test]
    fn desc_table_rows_supply_field_metadata_without_losing_extensions() {
        let rows = [
            vec!["ID", "PAYLOAD", "UUID_COL", "LOCATION"],
            vec!["NUMBER(10,0)", "ARRAY", "UUID", "GEOGRAPHY"],
            vec!["COLUMN", "COLUMN", "COLUMN", "COLUMN"],
            vec!["N", "Y", "Y", "Y"],
            vec!["", "", "", ""],
            vec!["Y", "N", "N", "N"],
            vec!["N", "N", "N", "N"],
            vec!["", "", "", ""],
            vec!["", "", "", ""],
        ];
        let mut data: Vec<ArrayRef> = rows
            .into_iter()
            .map(|row| Arc::new(StringArray::from(row)) as ArrayRef)
            .collect();
        data.push(Arc::new(StringArray::from(vec![
            Some("identifier"),
            None,
            Some(""),
            None,
        ])));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(
                (0..10)
                    .map(|index| Field::new(format!("column_{index}"), DataType::Utf8, true))
                    .collect::<Vec<_>>(),
            )),
            data,
        )
        .unwrap();
        let mut reader = single_batch_reader(batch);
        let desc = desc_columns(reader.as_mut()).unwrap();
        assert_eq!(
            desc["ID"],
            DescColumn {
                data_type: "NUMBER(10,0)".into(),
                primary_key: "Y".into(),
                comment: Some("identifier".into()),
            }
        );
        let fields = [
            Field::new("ID", DataType::Int64, false)
                .with_metadata(HashMap::from([("logicalType".into(), "FIXED".into())])),
            Field::new("PAYLOAD", DataType::Utf8, true).with_metadata(HashMap::from([(
                "ARROW:extension:name".into(),
                "arrow.json".into(),
            )])),
            Field::new("UUID_COL", DataType::FixedSizeBinary(16), true).with_metadata(
                HashMap::from([("ARROW:extension:name".into(), "arrow.uuid".into())]),
            ),
            Field::new("LOCATION", DataType::Binary, true).with_metadata(HashMap::from([(
                "ARROW:extension:name".into(),
                "geoarrow.wkb".into(),
            )])),
        ];
        let result = apply_desc_metadata(&Schema::new(fields.to_vec()), &desc).unwrap();
        assert_eq!(
            result.field(0).metadata(),
            &HashMap::from([
                ("DATA_TYPE".into(), "NUMBER(10,0)".into()),
                ("PRIMARY_KEY".into(), "Y".into()),
                ("COMMENT".into(), "identifier".into()),
            ]),
        );
        assert_eq!(
            result.field(1).metadata(),
            &HashMap::from([("ARROW:extension:name".into(), "arrow.json".into())]),
        );
        assert_eq!(
            result
                .field(2)
                .metadata()
                .get("COMMENT")
                .map(String::as_str),
            Some("")
        );
        assert_eq!(
            result
                .field(2)
                .metadata()
                .get("DATA_TYPE")
                .map(String::as_str),
            Some("UUID")
        );
        assert_eq!(
            result
                .field(2)
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some("arrow.uuid")
        );
        assert_eq!(
            result.field(3).metadata(),
            &HashMap::from([("ARROW:extension:name".into(), "geoarrow.wkb".into())]),
        );
    }

    #[test]
    fn table_identifier_quoting_preserves_all_components() {
        assert_eq!(
            qualified_table_identifier(Some("db\"name"), Some("schema"), "table"),
            "\"db\"\"name\".\"schema\".\"table\""
        );
        assert_eq!(
            qualified_table_identifier(Some("db"), None, "table"),
            "\"db\"..\"table\""
        );
        assert_eq!(
            qualified_table_identifier(None, Some("schema"), "table"),
            "\"schema\".\"table\""
        );
        assert_eq!(
            qualified_table_identifier(None, None, " table "),
            "\" table \""
        );
    }

    #[test]
    fn get_table_types_returns_table_and_view() {
        use adbc_core::Connection as _;
        use arrow_array::cast::AsArray;
        let conn = fake_connection();
        let mut reader = conn.get_table_types().unwrap();
        let batch = reader.next().unwrap().unwrap();
        let types: Vec<&str> = batch
            .column(0)
            .as_string::<i32>()
            .iter()
            .flatten()
            .collect();
        assert_eq!(types, vec!["TABLE", "VIEW"]);
    }
}
