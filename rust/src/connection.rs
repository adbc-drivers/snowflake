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

// src/connection.rs

/// Arrow library version reported via get_info(DriverArrowVersion).
/// Must be kept in sync with the `arrow-array` dependency version in Cargo.toml.
const ARROW_VERSION: &str = "v57.3.0";

use std::collections::HashSet;
use std::sync::Arc;

use adbc_core::{
    Optionable, constants,
    error::{Error, Result, Status},
    options::{InfoCode, ObjectDepth, OptionConnection, OptionValue},
    schemas,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, RecordBatchReader, StringArray,
    UInt32Array, UnionArray,
};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field, Schema};
use sf_core::apis::database_driver_v1::Handle;

use crate::driver::{Inner, TimestampPrecision};
use crate::statement::Statement;

pub struct Connection {
    pub(crate) inner: Arc<Inner>,
    pub(crate) conn_handle: Handle,
    pub(crate) autocommit: bool,
    pub(crate) active_transaction: bool,
    pub(crate) use_high_precision: bool,
    pub(crate) timestamp_precision: TimestampPrecision,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let _ = self.inner.sf.connection_release(self.conn_handle);
    }
}

pub(crate) struct SingleBatchReader {
    batch: Option<RecordBatch>,
    schema: std::sync::Arc<Schema>,
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
    pub(crate) fn execute_simple(&self, sql: &str) -> Result<()> {
        let stmt_handle = self
            .inner
            .sf
            .statement_new(self.conn_handle)
            .map_err(crate::error::api_error_to_adbc_error)?;
        let result = self.inner.runtime.block_on(async {
            self.inner
                .sf
                .statement_set_sql_query(stmt_handle, sql.to_string())
                .await?;
            self.inner
                .sf
                .statement_execute_query(stmt_handle, None)
                .await
        });
        let _ = self.inner.sf.statement_release(stmt_handle);
        result
            .map(|_| ())
            .map_err(crate::error::api_error_to_adbc_error)
    }

    fn query_scalar(&self, sql: &str) -> Result<String> {
        let stmt_handle = self
            .inner
            .sf
            .statement_new(self.conn_handle)
            .map_err(crate::error::api_error_to_adbc_error)?;
        let result = self.inner.runtime.block_on(async {
            self.inner
                .sf
                .statement_set_sql_query(stmt_handle, sql.to_string())
                .await?;
            self.inner
                .sf
                .statement_execute_query(stmt_handle, None)
                .await
        });
        let _ = self.inner.sf.statement_release(stmt_handle);
        let exec_result = result.map_err(crate::error::api_error_to_adbc_error)?;

        let raw =
            Box::into_raw(exec_result.stream) as *mut arrow_array::ffi_stream::FFI_ArrowArrayStream;
        let mut reader = unsafe { arrow_array::ffi_stream::ArrowArrayStreamReader::from_raw(raw) }
            .map_err(|e| Error::with_message_and_status(e.to_string(), Status::IO))?;

        use arrow_array::cast::AsArray;
        let batch = reader
            .next()
            .ok_or_else(|| {
                Error::with_message_and_status("empty result from scalar query", Status::IO)
            })?
            .map_err(|e| Error::with_message_and_status(e.to_string(), Status::IO))?;
        Ok(batch.column(0).as_string::<i32>().value(0).to_string())
    }

    pub(crate) fn set_autocommit(&mut self, enabled: bool) -> Result<()> {
        if enabled {
            if self.active_transaction {
                self.execute_simple("COMMIT")?;
                self.active_transaction = false;
            }
            self.execute_simple("ALTER SESSION SET AUTOCOMMIT = true")?;
            self.autocommit = true;
        } else {
            self.execute_simple("ALTER SESSION SET AUTOCOMMIT = false")?;
            if !self.active_transaction {
                self.execute_simple("BEGIN")?;
                self.active_transaction = true;
            }
            self.autocommit = false;
        }
        Ok(())
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
                    self.execute_simple(&format!(r#"USE DATABASE "{}""#, s.replace('"', "\"\"")))
                } else {
                    Err(Error::with_message_and_status(
                        "current_catalog value must be a string",
                        Status::InvalidArguments,
                    ))
                }
            }
            OptionConnection::CurrentSchema => {
                if let OptionValue::String(s) = &value {
                    self.execute_simple(&format!(r#"USE SCHEMA "{}""#, s.replace('"', "\"\"")))
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
            OptionConnection::CurrentCatalog => self.query_scalar("SELECT CURRENT_DATABASE()"),
            OptionConnection::CurrentSchema => self.query_scalar("SELECT CURRENT_SCHEMA()"),
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
            use_high_precision: self.use_high_precision,
            timestamp_precision: self.timestamp_precision,
            bound_batches: vec![],
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
            self.query_scalar("SELECT CURRENT_VERSION()")?
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
        let quoted = |s: &str| format!(r#""{}""#, s.replace('"', "\"\""));
        let qualified = match (catalog, db_schema) {
            (Some(c), Some(s)) => {
                format!("{}.{}.{}", quoted(c), quoted(s), quoted(table_name))
            }
            (None, Some(s)) => format!("{}.{}", quoted(s), quoted(table_name)),
            (Some(c), None) => format!("{}.{}", quoted(c), quoted(table_name)),
            (None, None) => quoted(table_name),
        };
        let sql = format!("DESC TABLE {qualified}");
        let stmt_handle = self
            .inner
            .sf
            .statement_new(self.conn_handle)
            .map_err(crate::error::api_error_to_adbc_error)?;
        let result = self.inner.runtime.block_on(async {
            self.inner
                .sf
                .statement_set_sql_query(stmt_handle, sql)
                .await?;
            self.inner
                .sf
                .statement_execute_query(stmt_handle, None)
                .await
        });
        let _ = self.inner.sf.statement_release(stmt_handle);
        let exec_result = result.map_err(crate::error::api_error_to_adbc_error)?;

        // Safety: exec_result.stream is a valid FFI stream from sf_core. We take ownership
        // via Box::into_raw and transfer it to ArrowArrayStreamReader. The C ABI layout is
        // stable across arrow versions per the Arrow C Data Interface specification.
        let raw =
            Box::into_raw(exec_result.stream) as *mut arrow_array::ffi_stream::FFI_ArrowArrayStream;
        let reader = unsafe { arrow_array::ffi_stream::ArrowArrayStreamReader::from_raw(raw) }
            .map_err(|e| Error::with_message_and_status(e.to_string(), Status::IO))?;

        let mut fields: Vec<Field> = Vec::new();
        for batch in reader {
            let batch =
                batch.map_err(|e| Error::with_message_and_status(e.to_string(), Status::IO))?;
            use arrow_array::cast::AsArray;

            // Resolve column indices by name (case-insensitive) so a future
            // reordering of DESC TABLE columns doesn't silently shift the mapping.
            // Known positional defaults from the current Snowflake DESC TABLE schema:
            //   0=name, 1=type, 2=kind, 3=null?, 4=default, 5=primary key,
            //   6=unique key, 7=check, 8=expression, 9=comment, …
            let schema = batch.schema();
            let find = |name: &str, fallback: usize| {
                schema
                    .fields()
                    .iter()
                    .position(|f| f.name().eq_ignore_ascii_case(name))
                    .unwrap_or(fallback)
            };
            let name_col = find("name", 0);
            let type_col = find("type", 1);
            let null_col = find("null?", 3);
            let pk_col = find("primary key", 5);
            let comment_col = find("comment", 9);

            if batch.num_columns() <= name_col
                || batch.num_columns() <= type_col
                || batch.num_columns() <= null_col
            {
                continue;
            }
            let names = batch.column(name_col).as_string::<i32>();
            let types = batch.column(type_col).as_string::<i32>();
            let nullables = batch.column(null_col).as_string::<i32>();
            // primary_key and comment are present only when the result has enough columns.
            let primary_keys =
                (batch.num_columns() > pk_col).then(|| batch.column(pk_col).as_string::<i32>());
            let comments = (batch.num_columns() > comment_col)
                .then(|| batch.column(comment_col).as_string::<i32>());
            for i in 0..batch.num_rows() {
                let type_str = types.value(i);
                let arrow_type = snowflake_type_to_arrow(
                    type_str,
                    self.use_high_precision,
                    self.timestamp_precision.time_unit(),
                );
                let mut md = std::collections::HashMap::new();
                md.insert("DATA_TYPE".to_string(), type_str.to_string());
                if let Some(pk) = &primary_keys {
                    md.insert("PRIMARY_KEY".to_string(), pk.value(i).to_string());
                }
                if let Some(cm) = &comments
                    && !cm.is_null(i)
                {
                    md.insert("COMMENT".to_string(), cm.value(i).to_string());
                }
                fields.push(
                    Field::new(names.value(i), arrow_type, nullables.value(i) == "Y")
                        .with_metadata(md),
                );
            }
        }
        Ok(Schema::new(fields))
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
        self.execute_simple("COMMIT")?;
        self.active_transaction = false;
        self.execute_simple("BEGIN")?;
        self.active_transaction = true;
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        if self.autocommit {
            return Err(Error::with_message_and_status(
                "cannot rollback: autocommit is enabled",
                Status::InvalidState,
            ));
        }
        self.execute_simple("ROLLBACK")?;
        self.active_transaction = false;
        self.execute_simple("BEGIN")?;
        self.active_transaction = true;
        Ok(())
    }

    #[allow(refining_impl_trait)]
    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(crate::error::not_implemented("read_partition"))
    }
}

fn ts_scale_to_unit(scale: u32) -> arrow_schema::TimeUnit {
    match scale {
        0 => arrow_schema::TimeUnit::Second,
        1..=3 => arrow_schema::TimeUnit::Millisecond,
        4..=6 => arrow_schema::TimeUnit::Microsecond,
        _ => arrow_schema::TimeUnit::Nanosecond,
    }
}

fn min_time_unit(a: arrow_schema::TimeUnit, b: arrow_schema::TimeUnit) -> arrow_schema::TimeUnit {
    use arrow_schema::TimeUnit::*;
    let rank = |u| match u {
        Second => 0u8,
        Millisecond => 1,
        Microsecond => 2,
        Nanosecond => 3,
    };
    if rank(a) <= rank(b) { a } else { b }
}

fn snowflake_type_to_arrow(
    type_str: &str,
    high_precision: bool,
    ts_unit: arrow_schema::TimeUnit,
) -> DataType {
    let upper = type_str.to_uppercase();
    let base = upper.split('(').next().unwrap_or(&upper).trim();
    match base {
        "FLOAT" | "DOUBLE" | "REAL" | "FLOAT4" | "FLOAT8" => DataType::Float64,
        "BOOLEAN" => DataType::Boolean,
        "DATE" => DataType::Date32,
        "TIME" => DataType::Time64(arrow_schema::TimeUnit::Nanosecond),
        "TEXT" | "STRING" | "VARCHAR" | "CHAR" | "CHARACTER" | "NCHAR" | "NVARCHAR"
        | "NVARCHAR2" | "CHAR VARYING" | "NCHAR VARYING" => DataType::Utf8,
        "BINARY" | "VARBINARY" => DataType::Binary,
        "ARRAY" | "OBJECT" | "VARIANT" | "GEOGRAPHY" | "GEOMETRY" => DataType::Utf8,
        "NUMBER" | "NUMERIC" | "DECIMAL" | "INT" | "INTEGER" | "BIGINT" | "SMALLINT"
        | "TINYINT" | "BYTEINT" => {
            if let Some(inner) = type_str
                .find('(')
                .and_then(|s| type_str.rfind(')').map(|e| &type_str[s + 1..e]))
            {
                let mut parts = inner.split(',');
                let precision = parts
                    .next()
                    .and_then(|s| s.trim().parse::<u8>().ok())
                    .unwrap_or(38);
                let scale = parts
                    .next()
                    .and_then(|s| s.trim().parse::<i8>().ok())
                    .unwrap_or(0);
                if scale == 0 {
                    DataType::Int64
                } else if high_precision {
                    DataType::Decimal128(precision, scale)
                } else {
                    DataType::Float64
                }
            } else {
                DataType::Int64
            }
        }
        "TIMESTAMP" | "TIMESTAMP_NTZ" | "DATETIME" => {
            let scale = type_str
                .find('(')
                .and_then(|s| type_str.rfind(')').map(|e| &type_str[s + 1..e]))
                .and_then(|s| s.trim().parse::<u32>().ok())
                .unwrap_or(9);
            let natural = ts_scale_to_unit(scale);
            let unit = min_time_unit(natural, ts_unit);
            DataType::Timestamp(unit, None)
        }
        "TIMESTAMP_LTZ" | "TIMESTAMP_TZ" => {
            let scale = type_str
                .find('(')
                .and_then(|s| type_str.rfind(')').map(|e| &type_str[s + 1..e]))
                .and_then(|s| s.trim().parse::<u32>().ok())
                .unwrap_or(9);
            let natural = ts_scale_to_unit(scale);
            let unit = min_time_unit(natural, ts_unit);
            DataType::Timestamp(unit, Some("UTC".into()))
        }
        _ => DataType::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_option_string_returns_not_found_for_unknown_key() {
        let driver = crate::driver::Driver::default();
        let conn = Connection {
            inner: driver.inner.clone(),
            conn_handle: sf_core::apis::database_driver_v1::Handle { id: 0, magic: 0 },
            autocommit: true,
            active_transaction: false,
            use_high_precision: true,
            timestamp_precision: TimestampPrecision::Nanoseconds,
        };
        let result = conn.get_option_string(OptionConnection::Other("unknown".into()));
        assert_eq!(result.unwrap_err().status, Status::NotFound);
    }

    #[test]
    fn snowflake_type_number_no_scale_is_int64() {
        assert_eq!(
            snowflake_type_to_arrow("NUMBER(38,0)", true, arrow_schema::TimeUnit::Nanosecond),
            DataType::Int64
        );
    }

    #[test]
    fn snowflake_type_number_with_scale_high_precision_is_decimal128() {
        assert_eq!(
            snowflake_type_to_arrow("NUMBER(10,2)", true, arrow_schema::TimeUnit::Nanosecond),
            DataType::Decimal128(10, 2)
        );
    }

    #[test]
    fn snowflake_type_number_with_scale_low_precision_is_float64() {
        assert_eq!(
            snowflake_type_to_arrow("NUMBER(10,2)", false, arrow_schema::TimeUnit::Nanosecond),
            DataType::Float64
        );
    }

    #[test]
    fn snowflake_type_text_is_utf8() {
        assert_eq!(
            snowflake_type_to_arrow("TEXT", true, arrow_schema::TimeUnit::Nanosecond),
            DataType::Utf8
        );
        assert_eq!(
            snowflake_type_to_arrow(
                "VARCHAR(16777216)",
                true,
                arrow_schema::TimeUnit::Nanosecond
            ),
            DataType::Utf8
        );
    }

    #[test]
    fn snowflake_type_boolean_is_boolean() {
        assert_eq!(
            snowflake_type_to_arrow("BOOLEAN", true, arrow_schema::TimeUnit::Nanosecond),
            DataType::Boolean
        );
    }

    #[test]
    fn snowflake_type_timestamp_ntz_nanosecond() {
        assert_eq!(
            snowflake_type_to_arrow("TIMESTAMP_NTZ(9)", true, arrow_schema::TimeUnit::Nanosecond),
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn snowflake_type_timestamp_ntz_microsecond() {
        assert_eq!(
            snowflake_type_to_arrow(
                "TIMESTAMP_NTZ(6)",
                true,
                arrow_schema::TimeUnit::Microsecond
            ),
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn snowflake_type_timestamp_ntz_scale6_with_ns_unit_returns_us() {
        assert_eq!(
            snowflake_type_to_arrow(
                "TIMESTAMP_NTZ(6)",
                true,
                arrow_schema::TimeUnit::Nanosecond
            ),
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn snowflake_type_timestamp_ntz_scale9_capped_by_us_unit() {
        assert_eq!(
            snowflake_type_to_arrow(
                "TIMESTAMP_NTZ(9)",
                true,
                arrow_schema::TimeUnit::Microsecond
            ),
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn snowflake_type_timestamp_ltz_scale6_with_ns_unit_returns_us() {
        assert_eq!(
            snowflake_type_to_arrow(
                "TIMESTAMP_LTZ(6)",
                true,
                arrow_schema::TimeUnit::Nanosecond
            ),
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
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
            active_transaction: false,
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

    #[test]
    fn snowflake_type_timestamp_tz_scale6_with_ns_unit_returns_us() {
        assert_eq!(
            snowflake_type_to_arrow("TIMESTAMP_TZ(6)", true, arrow_schema::TimeUnit::Nanosecond),
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn snowflake_type_timestamp_ntz_no_parens_defaults_to_ns() {
        assert_eq!(
            snowflake_type_to_arrow("TIMESTAMP_NTZ", true, arrow_schema::TimeUnit::Nanosecond),
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
        );
    }
}
