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
use std::sync::Arc;

use adbc_core::{
    error::{Error, Result, Status},
    options::{OptionDatabase, OptionValue},
    Optionable,
};
use arrow_array::RecordBatchReader;
use arrow_schema::TimeUnit;
use sf_core::apis::database_driver_v1::{
    ApiError, BindingType, ColumnMetadata, DatabaseDriverV1, ExecuteQueryResult, Handle,
    ResultSetDescriptor, ResultSetInfo,
};
use tokio::runtime::Runtime;

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

pub(crate) struct Inner {
    pub runtime: Runtime,
    pub sf: DatabaseDriverV1,
}

pub(crate) struct QueryResult {
    pub reader: Box<dyn RecordBatchReader + Send>,
    pub descriptor: ResultSetDescriptor,
}

pub(crate) struct PreparedResult {
    pub reader: Box<dyn RecordBatchReader + Send>,
    pub query_id: String,
    pub number_of_binds: i32,
    pub binds: Vec<ColumnMetadata>,
}

/// Bridge sf_core's Arrow 56 reader to the Arrow 58 types exposed by this
/// driver through the stable Arrow C Stream interface.
pub(crate) fn bridge_arrow_reader(
    reader: Box<dyn arrow_sf_core::array::RecordBatchReader + Send>,
) -> Result<Box<dyn RecordBatchReader + Send>> {
    let stream = Box::new(arrow_sf_core::ffi_stream::FFI_ArrowArrayStream::new(reader));
    let raw = Box::into_raw(stream) as *mut arrow_array::ffi_stream::FFI_ArrowArrayStream;
    let reader = unsafe { arrow_array::ffi_stream::ArrowArrayStreamReader::from_raw(raw) }
        .map_err(|error| {
            // from_raw does not invoke release on failure, so reclaim the stream.
            drop(unsafe { Box::from_raw(raw) });
            Error::with_message_and_status(error.to_string(), Status::IO)
        })?;
    Ok(Box::new(reader))
}

impl Inner {
    fn new() -> Result<Self> {
        let runtime = Runtime::new().map_err(|e| {
            Error::with_message_and_status(
                format!("Failed to create tokio runtime: {e}"),
                Status::IO,
            )
        })?;
        Ok(Self {
            runtime,
            sf: DatabaseDriverV1::new(),
        })
    }

    /// Execute SQL using a temporary sf_core statement and release the
    /// statement on every execution path. Execution errors take precedence over
    /// cleanup errors; a release error is returned only after successful execution.
    pub(crate) fn execute_temporary_statement(
        &self,
        connection: Handle,
        query: impl Into<String>,
        timeout_seconds: Option<u32>,
    ) -> Result<()> {
        let statement = self
            .sf
            .statement_new(connection)
            .map_err(crate::error::api_error_to_adbc_error)?;
        let result =
            self.execute_query_with_timeout(statement, query.into(), None, timeout_seconds);
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
        statement: Handle,
        query: String,
        bindings: Option<BindingType<'a>>,
        timeout_seconds: Option<u32>,
    ) -> Result<QueryResult> {
        let result = self
            .execute_query_raw_with_timeout(statement, query, bindings, timeout_seconds)
            .map_err(crate::error::api_error_to_adbc_error)?;
        self.acquire_execute_result(result)
    }

    /// Execute through sf_core without erasing its typed error. Callers must
    /// pass successful results to [`Self::acquire_execute_result`] so result
    /// handles are released on every stream-acquisition path.
    pub(crate) fn execute_query_raw_with_timeout<'a>(
        &self,
        statement: Handle,
        query: String,
        bindings: Option<BindingType<'a>>,
        timeout_seconds: Option<u32>,
    ) -> std::result::Result<ExecuteQueryResult, ApiError> {
        self.runtime
            .block_on(self.sf.statement_set_sql_query(statement, query))?;
        self.runtime.block_on(
            self.sf
                .statement_execute_query(statement, bindings, timeout_seconds),
        )
    }

    pub(crate) fn acquire_execute_result(&self, result: ExecuteQueryResult) -> Result<QueryResult> {
        let ExecuteQueryResult::Single(result) = result else {
            return Err(Error::with_message_and_status(
                "multi-statement query results are not supported",
                Status::NotImplemented,
            ));
        };
        self.acquire_result(result)
    }

    /// Synchronize SQL to sf_core and issue its describe-only prepare request.
    pub(crate) fn prepare_statement(
        &self,
        statement: Handle,
        query: String,
    ) -> Result<PreparedResult> {
        self.runtime
            .block_on(self.sf.statement_set_sql_query(statement, query))
            .map_err(crate::error::api_error_to_adbc_error)?;
        let result = self
            .runtime
            .block_on(self.sf.statement_prepare(statement))
            .map_err(crate::error::api_error_to_adbc_error)?;
        let reader = bridge_arrow_reader(result.stream)?;
        Ok(PreparedResult {
            reader,
            query_id: result.query_id,
            number_of_binds: result.number_of_binds,
            binds: result.binds,
        })
    }

    /// Acquire a core result stream, release its handle on every path, and bridge
    /// sf_core's Arrow version to the ADBC-facing Arrow version.
    pub fn acquire_result(&self, result: ResultSetInfo) -> Result<QueryResult> {
        let stream = self
            .runtime
            .block_on(self.sf.result_set_get_stream(result.handle));
        let release = self.sf.result_set_release(result.handle);

        let reader = match stream {
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

        let reader = bridge_arrow_reader(reader)?;

        Ok(QueryResult {
            reader,
            descriptor: result.descriptor,
        })
    }
}

/// Snowflake ADBC Driver.
pub struct Driver {
    pub(crate) inner: Arc<Inner>,
}

impl Default for Driver {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner::new().expect("failed to initialize driver")),
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
            use_high_precision: true,
            timestamp_precision: TimestampPrecision::default(),
        };
        for (key, value) in opts {
            db.set_option(key, value)?;
        }
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

    #[test]
    fn arrow_reader_bridge_preserves_schema_and_owns_stream() {
        let schema = Arc::new(arrow_sf_core::datatypes::Schema::new(vec![
            arrow_sf_core::datatypes::Field::new(
                "value",
                arrow_sf_core::datatypes::DataType::Int64,
                true,
            ),
        ]));
        let batch = arrow_sf_core::record_batch::RecordBatch::new_empty(schema.clone());
        let reader = Box::new(arrow_sf_core::record_batch::RecordBatchIterator::new(
            std::iter::once(Ok(batch)),
            schema,
        )) as Box<dyn arrow_sf_core::array::RecordBatchReader + Send>;
        let reader = bridge_arrow_reader(reader).unwrap();

        assert_eq!(reader.schema().field(0).name(), "value");
        assert_eq!(
            reader.schema().field(0).data_type(),
            &arrow_schema::DataType::Int64
        );
    }
}
