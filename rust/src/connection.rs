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
/// Must be kept in sync with the `arrow-array` dependency version in Cargo.toml.
const ARROW_VERSION: &str = "v58.1.0";

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use adbc_core::{
    Optionable, constants,
    error::{Error, Result, Status},
    options::{InfoCode, ObjectDepth, OptionConnection, OptionValue},
    schemas,
};
use arrow_array::{
    ArrayRef, BooleanArray, Int64Array, RecordBatch, RecordBatchIterator, RecordBatchReader,
    StringArray, UInt32Array, UnionArray,
};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field, Schema};
use sf_core::apis::database_driver_v1::{ConnectionInfo, Handle};
use sf_core::apis::operation_ctx::OperationCtx;

use crate::database::{GEOGRAPHY_OUTPUT_FORMAT, GEOMETRY_OUTPUT_FORMAT, GeoOutputFormat};
use crate::driver::{Inner, TimestampPrecision};
use crate::statement::{Statement, adjust_schema};

pub struct Connection {
    pub(crate) inner: Arc<Inner>,
    pub(crate) conn_handle: Handle,
    pub(crate) autocommit: bool,
    pub(crate) timestamp_precision: TimestampPrecision,
    pub(crate) geography_output_format: GeoOutputFormat,
    pub(crate) geometry_output_format: GeoOutputFormat,
    /// Only connection-level operations use this slot; statements retain
    /// separate contexts and are never cancelled by Connection::cancel().
    pub(crate) operation: Mutex<Arc<OperationCtx>>,
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

pub(crate) fn cleanup_connection_handle(inner: &Arc<Inner>, conn_handle: Handle) {
    let close_inner = Arc::clone(inner);
    attempt_connection_cleanup(
        move || {
            let _ = close_inner
                .runtime
                .block_on(close_inner.sf.connection_close(conn_handle));
        },
        || {
            let _ = inner.sf.connection_release(conn_handle);
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
        cleanup_connection_handle(&self.inner, self.conn_handle);
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

impl Connection {
    pub(crate) fn begin_operation(&self) -> Arc<OperationCtx> {
        let operation = Arc::new(OperationCtx::with_own_token());
        *self.operation.lock().unwrap_or_else(|e| e.into_inner()) = Arc::clone(&operation);
        operation
    }

    fn context_names(&self) -> Result<(Option<String>, Option<String>)> {
        // Deliberately extract only cached context names. ConnectionInfo also carries
        // sensitive tokens, which must never be surfaced through ADBC options.
        let operation = self.begin_operation();
        let ConnectionInfo {
            database, schema, ..
        } = self
            .inner
            .runtime
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
            return Err(Error::with_message_and_status(
                "session identifier must not be empty",
                Status::InvalidArguments,
            ));
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
            other => Err(Error::with_message_and_status(
                format!(
                    "invalid autocommit value {other:?}: expected true/false, 1/0, or enabled/disabled"
                ),
                Status::InvalidArguments,
            )),
        },
        _ => Err(Error::with_message_and_status(
            "autocommit value must be a string",
            Status::InvalidArguments,
        )),
    }
}

impl Optionable for Connection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key {
            OptionConnection::AutoCommit => {
                let enabled = parse_autocommit(&value)?;
                self.set_autocommit(enabled)
            }
            OptionConnection::CurrentCatalog => {
                if let OptionValue::String(s) = &value {
                    self.set_current_identifier(SessionIdentifierKind::Database, s)
                } else {
                    Err(Error::with_message_and_status(
                        "current_catalog value must be a string",
                        Status::InvalidArguments,
                    ))
                }
            }
            OptionConnection::CurrentSchema => {
                if let OptionValue::String(s) = &value {
                    self.set_current_identifier(SessionIdentifierKind::Schema, s)
                } else {
                    Err(Error::with_message_and_status(
                        "current_schema value must be a string",
                        Status::InvalidArguments,
                    ))
                }
            }
            OptionConnection::Other(ref k)
                if k == GEOGRAPHY_OUTPUT_FORMAT || k == GEOMETRY_OUTPUT_FORMAT =>
            {
                self.set_geo_output_format(k, &value)
            }
            _ => Err(Error::with_message_and_status(
                format!("unrecognized connection option '{}'", key.as_ref()),
                Status::NotImplemented,
            )),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionConnection::AutoCommit => {
                Ok(if self.autocommit { "true" } else { "false" }.to_string())
            }
            OptionConnection::CurrentCatalog => self.context_names()?.0.ok_or_else(|| {
                Error::with_message_and_status("current catalog is not set", Status::NotFound)
            }),
            OptionConnection::CurrentSchema => self.context_names()?.1.ok_or_else(|| {
                Error::with_message_and_status("current schema is not set", Status::NotFound)
            }),
            OptionConnection::Other(ref k) if k == GEOGRAPHY_OUTPUT_FORMAT => {
                Ok(self.geography_output_format.as_str().to_string())
            }
            OptionConnection::Other(ref k) if k == GEOMETRY_OUTPUT_FORMAT => {
                Ok(self.geometry_output_format.as_str().to_string())
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
            query: None,
            target_table: None,
            ingest_catalog: None,
            ingest_schema: None,
            ingest_mode: None,
            query_tag: None,
            query_timeout_seconds: None,
            timestamp_precision: self.timestamp_precision,
            ingest_options: crate::ingest::IngestOptions::default(),
            bound: crate::statement::Bound::Empty,
            last_query_id: None,
            prepared_parameter_schema: None,
            prepared_result_schema: None,
            operation: std::sync::Mutex::new(std::sync::Arc::new(
                sf_core::apis::operation_ctx::OperationCtx::with_own_token(),
            )),
        })
    }

    fn cancel(&mut self) -> Result<()> {
        self.operation
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .cancel();
        Ok(())
    }

    #[allow(refining_impl_trait)]
    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let need_vendor_version = codes
            .as_ref()
            .is_none_or(|s| s.contains(&InfoCode::VendorVersion));
        let vendor_version = if need_vendor_version {
            let operation = self.begin_operation();
            self.inner
                .runtime
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

        // (InfoCode, type_id, offset_within_arm_array)
        let all_entries: &[(InfoCode, i8, i32)] = &[
            (InfoCode::VendorName, 0, 0),
            (InfoCode::VendorSql, 1, 0),
            (InfoCode::VendorSubstrait, 1, 1),
            (InfoCode::DriverName, 0, 1),
            (InfoCode::DriverVersion, 0, 2),
            (InfoCode::DriverAdbcVersion, 2, 0),
            (InfoCode::VendorVersion, 0, 3),
            (InfoCode::DriverArrowVersion, 0, 4),
        ];

        let selected: Vec<_> = match &codes {
            None => all_entries.iter().collect(),
            Some(set) => all_entries
                .iter()
                .filter(|(c, _, _)| set.contains(c))
                .collect(),
        };

        if selected.is_empty() {
            let batch = RecordBatch::new_empty(schemas::GET_INFO_SCHEMA.clone());
            return Ok(single_batch_reader(batch));
        }

        let name_vals: Vec<u32> = selected.iter().map(|(c, _, _)| u32::from(c)).collect();
        let type_ids: Vec<i8> = selected.iter().map(|(_, t, _)| *t).collect();
        let offsets: Vec<i32> = selected.iter().map(|(_, _, o)| *o).collect();

        use arrow_schema::UnionFields;

        let string_values = Arc::new(StringArray::from(vec![
            "Snowflake",
            "ADBC Snowflake Driver (Rust)",
            env!("CARGO_PKG_VERSION"),
            vendor_version.as_str(),
            ARROW_VERSION,
        ])) as ArrayRef;
        let bool_values = Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef;
        let int64_values =
            Arc::new(Int64Array::from(vec![constants::ADBC_VERSION_1_1_0 as i64])) as ArrayRef;
        let int32_values = Arc::new(arrow_array::Int32Array::from(vec![0i32])) as ArrayRef;
        let list_values = Arc::new(arrow_array::ListArray::new_null(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            0,
        )) as ArrayRef;
        // arm 5: int32_to_int32_list_map — proper empty MapArray to satisfy schema type check
        // (This arm is never selected, but must have the right type for RecordBatch::try_new)
        let empty_int32_list_inner = arrow_array::Int32Array::from(Vec::<i32>::new());
        let empty_int32_list = arrow_array::ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            arrow_buffer::OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(vec![0i32])),
            Arc::new(empty_int32_list_inner),
            None,
        );
        let empty_entries = arrow_array::StructArray::new(
            arrow_schema::Fields::from(vec![
                Field::new("key", DataType::Int32, false),
                Field::new_list("value", Field::new_list_field(DataType::Int32, true), true),
            ]),
            vec![
                Arc::new(arrow_array::Int32Array::from(Vec::<i32>::new())) as ArrayRef,
                Arc::new(empty_int32_list) as ArrayRef,
            ],
            None,
        );
        let map_values = Arc::new(
            arrow_array::MapArray::try_new(
                Arc::new(Field::new_struct(
                    "entries",
                    vec![
                        Field::new("key", DataType::Int32, false),
                        Field::new_list(
                            "value",
                            Field::new_list_field(DataType::Int32, true),
                            true,
                        ),
                    ],
                    false,
                )),
                arrow_buffer::OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(vec![0i32])),
                empty_entries,
                None,
                false,
            )
            .map_err(|e| Error::with_message_and_status(e.to_string(), Status::Internal))?,
        ) as ArrayRef;

        let union_array = UnionArray::try_new(
            #[allow(deprecated)]
            UnionFields::new(
                [0i8, 1, 2, 3, 4, 5],
                [
                    Field::new("string_value", DataType::Utf8, true),
                    Field::new("bool_value", DataType::Boolean, true),
                    Field::new("int64_value", DataType::Int64, true),
                    Field::new("int32_bitmask", DataType::Int32, true),
                    Field::new_list(
                        "string_list",
                        Field::new_list_field(DataType::Utf8, true),
                        true,
                    ),
                    Field::new_map(
                        "int32_to_int32_list_map",
                        "entries",
                        Field::new("key", DataType::Int32, false),
                        Field::new_list(
                            "value",
                            Field::new_list_field(DataType::Int32, true),
                            true,
                        ),
                        false,
                        true,
                    ),
                ],
            ),
            type_ids.into_iter().collect::<ScalarBuffer<i8>>(),
            Some(offsets.into_iter().collect::<ScalarBuffer<i32>>()),
            vec![
                string_values,
                bool_values,
                int64_values,
                int32_values,
                list_values,
                map_values,
            ],
        )
        .map_err(|e| Error::with_message_and_status(e.to_string(), Status::Internal))?;

        let batch = RecordBatch::try_new(
            schemas::GET_INFO_SCHEMA.clone(),
            vec![
                Arc::new(UInt32Array::from(name_vals)) as ArrayRef,
                Arc::new(union_array) as ArrayRef,
            ],
        )
        .map_err(|e| Error::with_message_and_status(e.to_string(), Status::Internal))?;

        Ok(single_batch_reader(batch))
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
        crate::get_objects::execute_get_objects(
            self,
            &depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
        )
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
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
                return Err(Error::with_message_and_status(
                    format!("sf_core prepared an empty schema for table {qualified}"),
                    Status::Internal,
                ));
            }
            let timezone = self.inner.session_timezone(self.conn_handle)?;
            Ok(adjust_schema(
                &schema,
                self.timestamp_precision.time_unit(),
                &timezone,
                None,
            )
            .as_ref()
            .clone())
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
    }

    #[allow(refining_impl_trait)]
    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let array = Arc::new(StringArray::from(vec!["TABLE", "VIEW"]));
        let batch = RecordBatch::try_new(schemas::GET_TABLE_TYPES_SCHEMA.clone(), vec![array])
            .map_err(|e| Error::with_message_and_status(e.to_string(), Status::Internal))?;
        Ok(single_batch_reader(batch))
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
        crate::statistics::get_statistics(self, catalog, db_schema, table_name, approximate)
    }

    fn commit(&mut self) -> Result<()> {
        if self.autocommit {
            return Err(Error::with_message_and_status(
                "cannot commit: autocommit is enabled",
                Status::InvalidState,
            ));
        }
        let operation = self.begin_operation();
        self.inner
            .execute_temporary_statement(Some(&operation), self.conn_handle, "COMMIT", None)
    }

    fn rollback(&mut self) -> Result<()> {
        if self.autocommit {
            return Err(Error::with_message_and_status(
                "cannot rollback: autocommit is enabled",
                Status::InvalidState,
            ));
        }
        let operation = self.begin_operation();
        self.inner
            .execute_temporary_statement(Some(&operation), self.conn_handle, "ROLLBACK", None)
    }

    #[allow(refining_impl_trait)]
    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(crate::error::not_implemented("read_partition"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_connection() -> Connection {
        let driver = crate::driver::Driver::default();
        Connection {
            inner: driver.inner.clone(),
            conn_handle: Handle { id: 0, magic: 0 },
            autocommit: true,
            timestamp_precision: TimestampPrecision::Nanoseconds,
            geography_output_format: GeoOutputFormat::GeoJson,
            geometry_output_format: GeoOutputFormat::GeoJson,
            operation: Mutex::new(Arc::new(OperationCtx::with_own_token())),
        }
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
    fn get_option_string_returns_not_found_for_unknown_key() {
        let conn = fake_connection();
        let result = conn.get_option_string(OptionConnection::Other("unknown".into()));
        assert_eq!(result.unwrap_err().status, Status::NotFound);
    }

    #[test]
    fn connection_cancel_is_scoped_to_current_operation() {
        use adbc_core::Connection as _;
        let mut conn = fake_connection();
        conn.cancel().unwrap();
        let old = conn.begin_operation();
        let statement_operation = OperationCtx::with_own_token();
        conn.cancel().unwrap();
        let cancelled = conn.inner.runtime.block_on(old.run(
            "connection_cancel_unit",
            std::future::ready(Ok::<(), sf_core::apis::database_driver_v1::ApiError>(())),
        ));
        assert_eq!(
            crate::error::api_error_to_adbc_error(cancelled.unwrap_err()).status,
            Status::Cancelled
        );
        assert!(!statement_operation.token().is_cancelled());
        let next = conn.begin_operation();
        assert!(!next.token().is_cancelled());
        assert!(
            conn.inner
                .runtime
                .block_on(next.run(
                    "connection_cancel_unit",
                    std::future::ready(Ok::<(), sf_core::apis::database_driver_v1::ApiError>(())),
                ))
                .is_ok()
        );
        conn.cancel().unwrap();
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
            "unrecognized connection option 'unknown.option'"
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
