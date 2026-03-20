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
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use sf_core::apis::database_driver_v1::Handle;

use crate::driver::Inner;

pub struct Statement {
    pub(crate) inner: Arc<Inner>,
    pub(crate) stmt_handle: Handle,
    pub(crate) conn_handle: Handle,
    pub(crate) query: Option<String>,
    pub(crate) target_table: Option<String>,
    pub(crate) ingest_mode: Option<String>,
    pub(crate) query_tag: Option<String>,
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
    fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
        Err(crate::error::not_implemented("bind"))
    }

    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        Err(crate::error::not_implemented("bind_stream"))
    }

    #[allow(refining_impl_trait)]
    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        if self.target_table.is_some() {
            return Err(crate::error::not_implemented(
                "bulk ingestion (target_table) is not yet implemented",
            ));
        }
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
        Ok(Box::new(reader))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        if self.target_table.is_some() {
            return Err(crate::error::not_implemented(
                "bulk ingestion (target_table) is not yet implemented",
            ));
        }
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

        Ok(result.rows_affected)
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        Err(crate::error::not_implemented("execute_schema"))
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
            ingest_mode: None,
            query_tag: None,
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
    fn execute_with_target_table_returns_not_implemented() {
        let driver = crate::driver::Driver::default();
        let mut stmt = Statement {
            inner: driver.inner.clone(),
            stmt_handle: sf_core::apis::database_driver_v1::Handle { id: 0, magic: 0 },
            conn_handle: sf_core::apis::database_driver_v1::Handle { id: 0, magic: 0 },
            query: None,
            target_table: Some("mytable".into()),
            ingest_mode: None,
            query_tag: None,
        };
        match stmt.execute() {
            Err(err) => assert_eq!(err.status, adbc_core::error::Status::NotImplemented),
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
            ingest_mode: None,
            query_tag: None,
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
}
