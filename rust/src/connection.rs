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
use std::sync::Arc;

use adbc_core::{
    Optionable, constants,
    error::{Error, Result, Status},
    options::{InfoCode, ObjectDepth, OptionConnection, OptionValue},
    schemas,
};
use arrow_array::{
    ArrayRef, BooleanArray, Int64Array, RecordBatch, RecordBatchReader, StringArray, UInt32Array,
    UnionArray,
};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field, Schema};
use sf_core::apis::database_driver_v1::{ConnectionInfo, Handle};

use crate::driver::{Inner, TimestampPrecision};
use crate::statement::{Statement, adjust_schema};

pub struct Connection {
    pub(crate) inner: Arc<Inner>,
    pub(crate) conn_handle: Handle,
    pub(crate) autocommit: bool,
    pub(crate) use_high_precision: bool,
    pub(crate) timestamp_precision: TimestampPrecision,
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

pub(crate) struct SingleBatchReader {
    batch: Option<RecordBatch>,
    schema: std::sync::Arc<Schema>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionIdentifierKind {
    Database,
    Schema,
}

/// sf_core f9175f07 trims identifiers before quoting them. Keep the standard
/// core path unless trimming would change the identifier, in which case an
/// exactly quoted USE statement is required to preserve Snowflake semantics.
fn exact_identifier_sql(kind: SessionIdentifierKind, name: &str) -> Option<String> {
    if name == name.trim() {
        return None;
    }
    let keyword = match kind {
        SessionIdentifierKind::Database => "DATABASE",
        SessionIdentifierKind::Schema => "SCHEMA",
    };
    Some(format!("USE {keyword} \"{}\"", name.replace('"', "\"\"")))
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn qualified_table_identifier(
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

impl SingleBatchReader {
    pub(crate) fn new(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        Self {
            batch: Some(batch),
            schema,
        }
    }
}

impl Iterator for SingleBatchReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        Ok(self.batch.take()).transpose()
    }
}

impl RecordBatchReader for SingleBatchReader {
    fn schema(&self) -> std::sync::Arc<Schema> {
        self.schema.clone()
    }
}

impl Connection {
    fn context_names(&self) -> Result<(Option<String>, Option<String>)> {
        // Deliberately extract only cached context names. ConnectionInfo also carries
        // sensitive tokens, which must never be surfaced through ADBC options.
        let ConnectionInfo {
            database, schema, ..
        } = self
            .inner
            .runtime
            .block_on(self.inner.sf.connection_get_info(self.conn_handle))
            .map_err(crate::error::api_error_to_adbc_error)?;
        Ok((database, schema))
    }

    pub(crate) fn set_autocommit(&mut self, enabled: bool) -> Result<()> {
        self.inner
            .runtime
            .block_on(
                self.inner
                    .sf
                    .connection_set_autocommit(self.conn_handle, enabled),
            )
            .map_err(crate::error::api_error_to_adbc_error)?;
        self.autocommit = enabled;
        Ok(())
    }

    fn set_current_identifier(&self, kind: SessionIdentifierKind, name: &str) -> Result<()> {
        if let Some(sql) = exact_identifier_sql(kind, name) {
            return self.execute_session_sql(sql);
        }
        let result = match kind {
            SessionIdentifierKind::Database => self.inner.runtime.block_on(
                self.inner
                    .sf
                    .connection_use_database(self.conn_handle, name),
            ),
            SessionIdentifierKind::Schema => self
                .inner
                .runtime
                .block_on(self.inner.sf.connection_use_schema(self.conn_handle, name)),
        };
        result.map_err(crate::error::api_error_to_adbc_error)
    }

    fn execute_session_sql(&self, sql: String) -> Result<()> {
        let statement = self
            .inner
            .sf
            .statement_new(self.conn_handle)
            .map_err(crate::error::api_error_to_adbc_error)?;
        let result = self.inner.execute_query(statement, sql, None);
        let release = self.inner.sf.statement_release(statement);
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
}

impl Optionable for Connection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key {
            OptionConnection::AutoCommit => {
                let enabled = match &value {
                    OptionValue::String(s) => s == "true" || s == "1",
                    _ => {
                        return Err(Error::with_message_and_status(
                            "autocommit value must be a string",
                            Status::InvalidArguments,
                        ));
                    }
                };
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
            _ => Err(Error::with_message_and_status(
                format!("unsupported connection option: {}", key.as_ref()),
                Status::NotFound,
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
            use_high_precision: self.use_high_precision,
            timestamp_precision: self.timestamp_precision,
            bound_batches: vec![],
            binding_supplied: false,
            last_query_id: None,
            prepared_parameter_schema: None,
        })
    }

    fn cancel(&mut self) -> Result<()> {
        Err(crate::error::not_implemented("cancel"))
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
            self.inner
                .runtime
                .block_on(
                    self.inner
                        .sf
                        .connection_get_server_version(self.conn_handle),
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
            return Ok(Box::new(SingleBatchReader::new(batch)));
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

        Ok(Box::new(SingleBatchReader::new(batch)))
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
        let qualified = qualified_table_identifier(catalog, db_schema, table_name);
        let stmt_handle = self
            .inner
            .sf
            .statement_new(self.conn_handle)
            .map_err(crate::error::api_error_to_adbc_error)?;

        let result = (|| {
            let prepared = self
                .inner
                .prepare_statement(stmt_handle, format!("SELECT * FROM {qualified}"))?;
            let schema = prepared.reader.schema();
            if schema.fields().is_empty() {
                return Err(Error::with_message_and_status(
                    format!("sf_core prepared an empty schema for table {qualified}"),
                    Status::Internal,
                ));
            }
            Ok(adjust_schema(
                &schema,
                self.use_high_precision,
                self.timestamp_precision.time_unit(),
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
        Ok(Box::new(SingleBatchReader::new(batch)))
    }

    #[allow(refining_impl_trait)]
    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(crate::error::not_implemented("get_statistic_names"))
    }

    #[allow(refining_impl_trait)]
    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(crate::error::not_implemented("get_statistics"))
    }

    fn commit(&mut self) -> Result<()> {
        if self.autocommit {
            return Err(Error::with_message_and_status(
                "cannot commit: autocommit is enabled",
                Status::InvalidState,
            ));
        }
        self.inner
            .runtime
            .block_on(self.inner.sf.connection_commit(self.conn_handle))
            .map_err(crate::error::api_error_to_adbc_error)
    }

    fn rollback(&mut self) -> Result<()> {
        if self.autocommit {
            return Err(Error::with_message_and_status(
                "cannot rollback: autocommit is enabled",
                Status::InvalidState,
            ));
        }
        self.inner
            .runtime
            .block_on(self.inner.sf.connection_rollback(self.conn_handle))
            .map_err(crate::error::api_error_to_adbc_error)
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
    fn exact_identifier_fallback_is_narrow_and_quotes_without_trimming() {
        assert_eq!(
            exact_identifier_sql(SessionIdentifierKind::Database, " db\" name "),
            Some("USE DATABASE \" db\"\" name \"".into())
        );
        assert_eq!(
            exact_identifier_sql(SessionIdentifierKind::Schema, " schema "),
            Some("USE SCHEMA \" schema \"".into())
        );
        assert_eq!(
            exact_identifier_sql(SessionIdentifierKind::Database, "normal_name"),
            None
        );
    }

    #[test]
    fn get_option_string_returns_not_found_for_unknown_key() {
        let driver = crate::driver::Driver::default();
        let conn = Connection {
            inner: driver.inner.clone(),
            conn_handle: sf_core::apis::database_driver_v1::Handle { id: 0, magic: 0 },
            autocommit: true,
            use_high_precision: true,
            timestamp_precision: TimestampPrecision::Nanoseconds,
        };
        let result = conn.get_option_string(OptionConnection::Other("unknown".into()));
        assert_eq!(result.unwrap_err().status, Status::NotFound);
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
        let driver = crate::driver::Driver::default();
        let conn = Connection {
            inner: driver.inner.clone(),
            conn_handle: sf_core::apis::database_driver_v1::Handle { id: 0, magic: 0 },
            autocommit: true,
            use_high_precision: true,
            timestamp_precision: TimestampPrecision::Nanoseconds,
        };
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
