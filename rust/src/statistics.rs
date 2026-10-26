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

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::{
    constants,
    error::{Error, Result, Status},
    schemas,
};
use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, RecordBatchReader, StringArray,
    StructArray, UnionArray,
    builder::{
        ArrayBuilder, BooleanBuilder, Float64Builder, Int16Builder, Int64Builder, StringBuilder,
    },
    new_empty_array, new_null_array,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::DataType;

use crate::connection::{Connection, quote_identifier, single_batch_reader};

const BYTES: i16 = 1024;
const RETENTION_DAYS: i16 = 1025;
const ACTIVE_BYTES: i16 = 1026;
const TIME_TRAVEL_BYTES: i16 = 1027;
const FAILSAFE_BYTES: i16 = 1028;
const CLUSTERING_DEPTH: i16 = 1029;
const MAX_EXACT_TABLES: usize = 1000;

const STATISTIC_NAMES: [(&str, i16); 6] = [
    ("snowflake.table.bytes", BYTES),
    ("snowflake.table.retention.time_days", RETENTION_DAYS),
    ("snowflake.table.bytes.active", ACTIVE_BYTES),
    ("snowflake.table.bytes.time_travel", TIME_TRAVEL_BYTES),
    ("snowflake.table.bytes.failsafe", FAILSAFE_BYTES),
    ("snowflake.table.clustering.depth", CLUSTERING_DEPTH),
];

pub(crate) fn get_statistic_names() -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let mut names = StringBuilder::new();
    let mut keys = Int16Builder::new();
    for &(name, key) in &STATISTIC_NAMES {
        names.append_value(name);
        keys.append_value(key);
    }
    let batch = RecordBatch::try_new(
        schemas::GET_STATISTIC_NAMES_SCHEMA.clone(),
        vec![Arc::new(names.finish()), Arc::new(keys.finish())],
    )
    .map_err(internal_error)?;
    Ok(single_batch_reader(batch))
}

struct TableInfo {
    schema: String,
    name: String,
    row_count: i64,
    bytes: i64,
    retention_days: i64,
    clustering_key: Option<String>,
}

struct CatalogTables {
    name: String,
    tables: Vec<TableInfo>,
}

struct StorageMetric {
    active_bytes: i64,
    time_travel_bytes: i64,
    failsafe_bytes: i64,
}

type StorageMetrics = HashMap<String, HashMap<String, StorageMetric>>;

pub(crate) fn get_statistics(
    connection: &Connection,
    catalog: Option<&str>,
    db_schema: Option<&str>,
    table_name: Option<&str>,
    approximate: bool,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    if [catalog, db_schema, table_name]
        .into_iter()
        .flatten()
        .any(str::is_empty)
    {
        return Ok(empty_statistics());
    }

    let catalogs = matching_databases(connection, catalog)?;
    let mut tables_by_catalog: Vec<CatalogTables> = Vec::new();
    let mut catalog_indices = HashMap::<String, usize>::new();
    let mut total_tables = 0;
    for database in catalogs {
        let (actual_catalog, mut tables) = query_tables(
            connection,
            &database,
            db_schema.unwrap_or("%"),
            table_name.unwrap_or("%"),
        )?;
        if tables.is_empty() {
            continue;
        }
        total_tables += tables.len();
        check_exact_limit(total_tables, approximate)?;
        let actual_catalog = actual_catalog.expect("nonempty table results have a catalog");
        if let Some(&index) = catalog_indices.get(&actual_catalog) {
            tables_by_catalog[index].tables.append(&mut tables);
        } else {
            catalog_indices.insert(actual_catalog.clone(), tables_by_catalog.len());
            tables_by_catalog.push(CatalogTables {
                name: actual_catalog,
                tables,
            });
        }
    }

    if tables_by_catalog.is_empty() {
        return Ok(empty_statistics());
    }

    let mut builder = StatisticsBuilder::default();
    for catalog in &tables_by_catalog {
        // Storage metrics are optional: most roles cannot read TABLE_STORAGE_METRICS.
        let storage = if approximate {
            None
        } else {
            query_storage_metrics(connection, &catalog.name, &catalog.tables).ok()
        };
        let mut schemas: Vec<(&str, Vec<&TableInfo>)> = Vec::new();
        let mut schema_indices = HashMap::<&str, usize>::new();
        for table in &catalog.tables {
            match schema_indices.get(table.schema.as_str()) {
                Some(&index) => schemas[index].1.push(table),
                None => {
                    schema_indices.insert(&table.schema, schemas.len());
                    schemas.push((&table.schema, vec![table]));
                }
            }
        }

        for (schema, tables) in schemas {
            for table in tables {
                let metric = storage.as_ref().and_then(|metrics| {
                    metrics
                        .get(table.schema.as_str())
                        .and_then(|schema| schema.get(table.name.as_str()))
                });
                let depth = if !approximate
                    && table
                        .clustering_key
                        .as_deref()
                        .is_some_and(|key| !key.is_empty())
                {
                    query_clustering_depth(connection, &catalog.name, table).ok()
                } else {
                    None
                };
                builder.append_table(table, metric, depth)?;
            }
            builder.finish_schema(schema)?;
        }
        builder.finish_catalog(&catalog.name)?;
    }
    Ok(single_batch_reader(builder.finish()?))
}

fn check_exact_limit(total_tables: usize, approximate: bool) -> Result<()> {
    if !approximate && total_tables > MAX_EXACT_TABLES {
        Err(Error::with_message_and_status(
            format!(
                "exact statistics requested for more than {MAX_EXACT_TABLES} tables; use approximate=true or narrower catalog/schema/table filters"
            ),
            Status::NotImplemented,
        ))
    } else {
        Ok(())
    }
}

fn empty_statistics() -> Box<dyn RecordBatchReader + Send + 'static> {
    single_batch_reader(RecordBatch::new_empty(
        schemas::GET_STATISTICS_SCHEMA.clone(),
    ))
}

// Snowflake SQL string literals process backslashes; SHOW LIKE processes
// another layer of escaping in its pattern.
fn append_sql_literal(sql: &mut String, value: &str, escaped_backslash: &str) {
    sql.push('\'');
    for character in value.chars() {
        match character {
            '\\' => sql.push_str(escaped_backslash),
            '\'' => sql.push_str("''"),
            _ => sql.push(character),
        }
    }
    sql.push('\'');
}

fn databases_sql(catalog: Option<&str>) -> String {
    match catalog {
        None => "SHOW DATABASES".to_owned(),
        Some(pattern) => {
            let mut sql = String::from("SHOW DATABASES LIKE ");
            append_sql_literal(&mut sql, pattern, "\\\\\\\\");
            sql
        }
    }
}

fn tables_sql(database: &str, schema_filter: &str, table_filter: &str) -> String {
    let mut sql = format!(
        "SELECT table_catalog, table_schema, table_name, \
         COALESCE(row_count, 0), COALESCE(bytes, 0), COALESCE(retention_time, 1), clustering_key \
         FROM {}.information_schema.tables WHERE table_schema ILIKE ",
        quote_identifier(database),
    );
    append_sql_literal(&mut sql, schema_filter, "\\\\");
    sql.push_str(" AND table_name ILIKE ");
    append_sql_literal(&mut sql, table_filter, "\\\\");
    sql.push_str(" AND table_type = 'BASE TABLE' ORDER BY table_schema, table_name");
    sql
}

fn storage_sql(catalog: &str, tables: &[TableInfo]) -> String {
    let mut sql = format!(
        "SELECT table_schema, table_name, COALESCE(active_bytes, 0), \
         COALESCE(time_travel_bytes, 0), COALESCE(failsafe_bytes, 0) \
         FROM {}.information_schema.table_storage_metrics \
         WHERE (table_schema, table_name) IN (",
        quote_identifier(catalog),
    );
    for (index, table) in tables.iter().enumerate() {
        if index != 0 {
            sql.push_str(", ");
        }
        sql.push('(');
        append_sql_literal(&mut sql, &table.schema, "\\\\");
        sql.push_str(", ");
        append_sql_literal(&mut sql, &table.name, "\\\\");
        sql.push(')');
    }
    sql.push_str(") ORDER BY table_schema, table_name");
    sql
}

fn clustering_sql(catalog: &str, schema: &str, table: &str) -> String {
    let qualified = format!(
        "{}.{}.{}",
        quote_identifier(catalog),
        quote_identifier(schema),
        quote_identifier(table),
    );
    // Snowflake returns JSON text. Parse the depth on the server rather than
    // adding a JSON dependency for the single field returned by this function.
    let mut sql =
        String::from("SELECT COALESCE(TRY_TO_DOUBLE((PARSE_JSON(SYSTEM$CLUSTERING_INFORMATION(");
    append_sql_literal(&mut sql, &qualified, "\\\\");
    sql.push_str(")):average_depth)::STRING), 0)");
    sql
}

fn with_query_reader<T>(
    connection: &Connection,
    query: String,
    consume: impl FnOnce(&mut dyn RecordBatchReader) -> Result<T>,
) -> Result<T> {
    let stmt_handle = connection
        .inner
        .sf
        .statement_new(connection.conn_handle)
        .map_err(crate::error::api_error_to_adbc_error)?;
    let result = (|| {
        let mut result =
            connection
                .inner
                .execute_query_with_timeout(None, stmt_handle, query, None, None)?;
        consume(result.reader.as_mut())
    })();
    let release = connection.inner.sf.statement_release(stmt_handle);
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

fn matching_databases(connection: &Connection, catalog: Option<&str>) -> Result<Vec<String>> {
    if let Some(name) = catalog.filter(|name| !name.contains(['%', '_'])) {
        return Ok(vec![name.to_owned()]);
    }
    with_query_reader(connection, databases_sql(catalog), |reader| {
        let mut databases = Vec::new();
        for batch in reader {
            let batch = batch.map_err(io_error)?;
            if batch.num_columns() < 2 {
                return Err(Error::with_message_and_status(
                    format!(
                        "SHOW DATABASES returned {} columns, expected at least 2",
                        batch.num_columns()
                    ),
                    Status::Internal,
                ));
            }
            let names = string_column(&batch, 1, "database name")?;
            for row in 0..batch.num_rows() {
                if !names.is_null(row) {
                    databases.push(names.value(row).to_owned());
                }
            }
        }
        Ok(databases)
    })
}

fn query_tables(
    connection: &Connection,
    catalog: &str,
    schema_filter: &str,
    table_filter: &str,
) -> Result<(Option<String>, Vec<TableInfo>)> {
    with_query_reader(
        connection,
        tables_sql(catalog, schema_filter, table_filter),
        |reader| {
            let mut actual_catalog = None;
            let mut tables = Vec::new();
            for batch in reader {
                let batch = batch.map_err(io_error)?;
                if batch.num_columns() != 7 {
                    return Err(unexpected_columns("INFORMATION_SCHEMA.TABLES", 7, &batch));
                }
                let catalogs = string_column(&batch, 0, "table_catalog")?;
                let schemas = string_column(&batch, 1, "table_schema")?;
                let names = string_column(&batch, 2, "table_name")?;
                let rows = int64_column(batch.column(3))?;
                let bytes = int64_column(batch.column(4))?;
                let retention = int64_column(batch.column(5))?;
                let rows = rows.as_any().downcast_ref::<Int64Array>().unwrap();
                let bytes = bytes.as_any().downcast_ref::<Int64Array>().unwrap();
                let retention = retention.as_any().downcast_ref::<Int64Array>().unwrap();
                let clustering = string_column(&batch, 6, "clustering_key")?;
                for row in 0..batch.num_rows() {
                    if actual_catalog.is_none() {
                        actual_catalog =
                            Some(required_string(catalogs, row, "table_catalog")?.to_owned());
                    }
                    tables.push(TableInfo {
                        schema: required_string(schemas, row, "table_schema")?.to_owned(),
                        name: required_string(names, row, "table_name")?.to_owned(),
                        row_count: numeric_value(rows, row, 0),
                        bytes: numeric_value(bytes, row, 0),
                        retention_days: numeric_value(retention, row, 1),
                        clustering_key: (!clustering.is_null(row))
                            .then(|| clustering.value(row).to_owned()),
                    });
                }
            }
            Ok((actual_catalog, tables))
        },
    )
}

fn query_storage_metrics(
    connection: &Connection,
    catalog: &str,
    tables: &[TableInfo],
) -> Result<StorageMetrics> {
    with_query_reader(connection, storage_sql(catalog, tables), |reader| {
        let mut metrics = StorageMetrics::new();
        for batch in reader {
            let batch = batch.map_err(io_error)?;
            if batch.num_columns() != 5 {
                return Err(unexpected_columns("TABLE_STORAGE_METRICS", 5, &batch));
            }
            let schemas = string_column(&batch, 0, "table_schema")?;
            let names = string_column(&batch, 1, "table_name")?;
            let active = int64_column(batch.column(2))?;
            let travel = int64_column(batch.column(3))?;
            let failsafe = int64_column(batch.column(4))?;
            let active = active.as_any().downcast_ref::<Int64Array>().unwrap();
            let travel = travel.as_any().downcast_ref::<Int64Array>().unwrap();
            let failsafe = failsafe.as_any().downcast_ref::<Int64Array>().unwrap();
            for row in 0..batch.num_rows() {
                metrics
                    .entry(required_string(schemas, row, "table_schema")?.to_owned())
                    .or_default()
                    .insert(
                        required_string(names, row, "table_name")?.to_owned(),
                        StorageMetric {
                            active_bytes: numeric_value(active, row, 0),
                            time_travel_bytes: numeric_value(travel, row, 0),
                            failsafe_bytes: numeric_value(failsafe, row, 0),
                        },
                    );
            }
        }
        Ok(metrics)
    })
}

fn query_clustering_depth(
    connection: &Connection,
    catalog: &str,
    table: &TableInfo,
) -> Result<f64> {
    with_query_reader(
        connection,
        clustering_sql(catalog, &table.schema, &table.name),
        |reader| {
            for batch in reader {
                let batch = batch.map_err(io_error)?;
                if batch.num_columns() != 1 {
                    return Err(unexpected_columns(
                        "SYSTEM$CLUSTERING_INFORMATION",
                        1,
                        &batch,
                    ));
                }
                if batch.num_rows() == 0 {
                    continue;
                }
                let depths =
                    arrow_cast::cast(batch.column(0), &DataType::Float64).map_err(io_error)?;
                let depths = depths
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        Error::with_message_and_status("invalid clustering depth", Status::IO)
                    })?;
                if !depths.is_null(0) {
                    return Ok(depths.value(0));
                }
            }
            Err(Error::with_message_and_status(
                "null clustering depth",
                Status::IO,
            ))
        },
    )
}

fn string_column<'a>(batch: &'a RecordBatch, index: usize, field: &str) -> Result<&'a StringArray> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            Error::with_message_and_status(format!("{field} must be a string"), Status::IO)
        })
}

fn required_string<'a>(array: &'a StringArray, index: usize, field: &str) -> Result<&'a str> {
    if array.is_null(index) {
        Err(Error::with_message_and_status(
            format!("{field} is null"),
            Status::IO,
        ))
    } else {
        Ok(array.value(index))
    }
}

fn int64_column(array: &ArrayRef) -> Result<ArrayRef> {
    if array.data_type() == &DataType::Int64 {
        Ok(Arc::clone(array))
    } else {
        arrow_cast::cast(array.as_ref(), &DataType::Int64).map_err(io_error)
    }
}

fn numeric_value(array: &Int64Array, row: usize, default: i64) -> i64 {
    if array.is_null(row) {
        default
    } else {
        array.value(row)
    }
}

fn unexpected_columns(source: &str, expected: usize, batch: &RecordBatch) -> Error {
    Error::with_message_and_status(
        format!(
            "{source} returned {} columns; expected {expected}",
            batch.num_columns()
        ),
        Status::Internal,
    )
}

fn io_error(error: impl std::fmt::Display) -> Error {
    Error::with_message_and_status(error.to_string(), Status::IO)
}

fn internal_error(error: impl std::fmt::Display) -> Error {
    Error::with_message_and_status(error.to_string(), Status::Internal)
}

#[derive(Default)]
struct StatisticsBuilder {
    catalog_names: StringBuilder,
    catalog_offsets: Vec<i32>,
    schema_names: StringBuilder,
    schema_offsets: Vec<i32>,
    table_names: StringBuilder,
    keys: Int16Builder,
    approximate: BooleanBuilder,
    type_ids: Vec<i8>,
    value_offsets: Vec<i32>,
    ints: Int64Builder,
    floats: Float64Builder,
}

impl StatisticsBuilder {
    fn append_int(&mut self, table: &str, key: i16, value: i64, approximate: bool) -> Result<()> {
        let offset = i32::try_from(self.ints.len()).map_err(internal_error)?;
        self.ints.append_value(value);
        self.append_stat(table, key, approximate, 0, offset);
        Ok(())
    }

    fn append_float(&mut self, table: &str, key: i16, value: f64, approximate: bool) -> Result<()> {
        let offset = i32::try_from(self.floats.len()).map_err(internal_error)?;
        self.floats.append_value(value);
        self.append_stat(table, key, approximate, 2, offset);
        Ok(())
    }

    fn append_stat(&mut self, table: &str, key: i16, approximate: bool, type_id: i8, offset: i32) {
        self.table_names.append_value(table);
        self.keys.append_value(key);
        self.approximate.append_value(approximate);
        self.type_ids.push(type_id);
        self.value_offsets.push(offset);
    }

    fn append_table(
        &mut self,
        table: &TableInfo,
        storage: Option<&StorageMetric>,
        clustering_depth: Option<f64>,
    ) -> Result<()> {
        self.append_float(
            &table.name,
            constants::ADBC_STATISTIC_ROW_COUNT_KEY,
            table.row_count as f64,
            true,
        )?;
        self.append_int(&table.name, BYTES, table.bytes, true)?;
        self.append_int(&table.name, RETENTION_DAYS, table.retention_days, true)?;
        if let Some(storage) = storage {
            self.append_int(&table.name, ACTIVE_BYTES, storage.active_bytes, false)?;
            self.append_int(
                &table.name,
                TIME_TRAVEL_BYTES,
                storage.time_travel_bytes,
                false,
            )?;
            self.append_int(&table.name, FAILSAFE_BYTES, storage.failsafe_bytes, false)?;
        }
        if let Some(depth) = clustering_depth {
            self.append_float(&table.name, CLUSTERING_DEPTH, depth, false)?;
        }
        Ok(())
    }

    fn finish_schema(&mut self, schema: &str) -> Result<()> {
        self.schema_names.append_value(schema);
        self.schema_offsets
            .push(i32::try_from(self.type_ids.len()).map_err(internal_error)?);
        Ok(())
    }

    fn finish_catalog(&mut self, catalog: &str) -> Result<()> {
        self.catalog_names.append_value(catalog);
        self.catalog_offsets
            .push(i32::try_from(self.schema_offsets.len()).map_err(internal_error)?);
        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        let DataType::Union(value_fields, _) = &*schemas::STATISTIC_VALUE_SCHEMA else {
            unreachable!("ADBC statistic values use a union")
        };
        let values = UnionArray::try_new(
            value_fields.clone(),
            self.type_ids.into_iter().collect::<ScalarBuffer<i8>>(),
            Some(
                self.value_offsets
                    .into_iter()
                    .collect::<ScalarBuffer<i32>>(),
            ),
            vec![
                Arc::new(self.ints.finish()),
                new_empty_array(&DataType::UInt64),
                Arc::new(self.floats.finish()),
                new_empty_array(&DataType::Binary),
            ],
        )
        .map_err(internal_error)?;
        let DataType::Struct(stat_fields) = &*schemas::STATISTICS_SCHEMA else {
            unreachable!("ADBC statistics use a struct")
        };
        let statistics = StructArray::try_new(
            stat_fields.clone(),
            vec![
                Arc::new(self.table_names.finish()),
                new_null_array(&DataType::Utf8, self.keys.len()),
                Arc::new(self.keys.finish()),
                Arc::new(values),
                Arc::new(self.approximate.finish()),
            ],
            None,
        )
        .map_err(internal_error)?;
        let DataType::Struct(schema_fields) = &*schemas::STATISTICS_DB_SCHEMA_SCHEMA else {
            unreachable!("ADBC statistic schemas use a struct")
        };
        let DataType::List(stat_item) = schema_fields[1].data_type() else {
            unreachable!("ADBC statistics use a list")
        };
        let stats_list = arrow_array::ListArray::try_new(
            Arc::clone(stat_item),
            OffsetBuffer::new(ScalarBuffer::from(
                std::iter::once(0)
                    .chain(self.schema_offsets)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(statistics),
            None,
        )
        .map_err(internal_error)?;
        let schemas_struct = StructArray::try_new(
            schema_fields.clone(),
            vec![Arc::new(self.schema_names.finish()), Arc::new(stats_list)],
            None,
        )
        .map_err(internal_error)?;
        let DataType::List(schema_item) = schemas::GET_STATISTICS_SCHEMA.field(1).data_type()
        else {
            unreachable!("ADBC statistic catalogs use a list")
        };
        let schema_list = arrow_array::ListArray::try_new(
            Arc::clone(schema_item),
            OffsetBuffer::new(ScalarBuffer::from(
                std::iter::once(0)
                    .chain(self.catalog_offsets)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(schemas_struct),
            None,
        )
        .map_err(internal_error)?;
        RecordBatch::try_new(
            schemas::GET_STATISTICS_SCHEMA.clone(),
            vec![Arc::new(self.catalog_names.finish()), Arc::new(schema_list)],
        )
        .map_err(internal_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Int16Array, ListArray};

    fn table(schema: &str, name: &str) -> TableInfo {
        TableInfo {
            schema: schema.to_owned(),
            name: name.to_owned(),
            row_count: 42,
            bytes: 128,
            retention_days: 7,
            clustering_key: Some("LINEAR(a)".to_owned()),
        }
    }

    #[test]
    fn names_match_snowflake_keys_and_adbc_schema() {
        let mut reader = get_statistic_names().unwrap();
        assert_eq!(reader.schema(), *schemas::GET_STATISTIC_NAMES_SCHEMA);
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 6);
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let keys = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        for (index, &(name, key)) in STATISTIC_NAMES.iter().enumerate() {
            assert_eq!((names.value(index), keys.value(index)), (name, key));
        }
        assert!(reader.next().is_none());
    }

    #[test]
    fn exact_mode_rejects_more_than_one_thousand_tables_only() {
        assert!(check_exact_limit(0, false).is_ok());
        assert!(check_exact_limit(MAX_EXACT_TABLES, false).is_ok());
        let error = check_exact_limit(MAX_EXACT_TABLES + 1, false).unwrap_err();
        assert_eq!(error.status, Status::NotImplemented);
        assert!(check_exact_limit(MAX_EXACT_TABLES + 1, true).is_ok());
    }

    #[test]
    fn pattern_queries_escape_quotes_and_backslashes_without_escaping_wildcards() {
        assert_eq!(databases_sql(None), "SHOW DATABASES");
        assert_eq!(
            databases_sql(Some("db'\\_%")),
            r"SHOW DATABASES LIKE 'db''\\\\_%'"
        );
        let query = tables_sql("Db\"Name", "a'\\_%", "t'\\%_");
        assert!(query.contains("FROM \"Db\"\"Name\".information_schema.tables"));
        assert!(query.contains(r"table_schema ILIKE 'a''\\_%'"));
        assert!(query.contains(r"table_name ILIKE 't''\\%_'"));
        assert!(query.contains("table_type = 'BASE TABLE'"));
        assert_eq!(
            clustering_sql("Db'\\", "S\"", "T'"),
            r#"SELECT COALESCE(TRY_TO_DOUBLE((PARSE_JSON(SYSTEM$CLUSTERING_INFORMATION('"Db''\\"."S"""."T''"')):average_depth)::STRING), 0)"#
        );
    }

    #[test]
    fn storage_query_quotes_every_candidate_identifier() {
        let sql = storage_sql("D\"b", &[table("s'\\", "t'"), table("s", "other")]);
        assert!(sql.contains("FROM \"D\"\"b\".information_schema.table_storage_metrics"));
        assert!(
            sql.contains(r"WHERE (table_schema, table_name) IN (('s''\\', 't'''), ('s', 'other'))")
        );
    }

    #[test]
    fn statistics_assembly_uses_adbc_schema_and_correct_union_arms() {
        let mut builder = StatisticsBuilder::default();
        builder.append_table(&table("s1", "t"), None, None).unwrap();
        builder.finish_schema("s1").unwrap();
        builder
            .append_table(
                &table("s2", "u"),
                Some(&StorageMetric {
                    active_bytes: 3,
                    time_travel_bytes: 4,
                    failsafe_bytes: 5,
                }),
                Some(1.25),
            )
            .unwrap();
        builder.finish_schema("s2").unwrap();
        builder.finish_catalog("db").unwrap();
        let batch = builder.finish().unwrap();
        assert_eq!(batch.schema(), *schemas::GET_STATISTICS_SCHEMA);
        assert_eq!(batch.num_rows(), 1);
        let catalogs = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(catalogs.value_offsets(), &[0, 2]);
        let schemas = catalogs.values();
        let schemas = schemas.as_any().downcast_ref::<StructArray>().unwrap();
        let stats_list = schemas
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(stats_list.value_offsets(), &[0, 3, 10]);
        let stats = stats_list.values();
        let stats = stats.as_any().downcast_ref::<StructArray>().unwrap();
        let keys = stats
            .column(2)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert_eq!(
            keys.values(),
            &[6, 1024, 1025, 6, 1024, 1025, 1026, 1027, 1028, 1029]
        );
        let approximate = stats
            .column(4)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!((0..3).all(|index| approximate.value(index)));
        assert!((3..6).all(|index| approximate.value(index)));
        assert!((6..10).all(|index| !approximate.value(index)));
        let values = stats
            .column(3)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        assert_eq!(values.type_id(0), 2);
        assert_eq!(values.type_id(1), 0);
        assert_eq!(values.type_id(9), 2);
        let counts = values
            .child(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(counts.values(), &[42.0, 42.0, 1.25]);
        let ints = values
            .child(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ints.values(), &[128, 7, 128, 7, 3, 4, 5]);
    }

    #[test]
    fn empty_statistics_preserve_schema() {
        let mut reader = empty_statistics();
        assert_eq!(reader.schema(), *schemas::GET_STATISTICS_SCHEMA);
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 0);
        assert!(reader.next().is_none());
    }
}
