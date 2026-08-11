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

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use adbc_core::{
    error::{Error, Result as AdbcResult, Status},
    options::ObjectDepth,
    schemas,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int16Array, Int32Array, Int64Array, LargeListArray, ListArray,
    RecordBatch, RecordBatchReader, StringArray, StructArray, new_empty_array, new_null_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, Field, Fields};
use sf_core::apis::database_driver_v1::{
    DEPTH_CATALOGS, DEPTH_COLUMNS, DEPTH_DB_SCHEMAS, DEPTH_TABLES, GetObjectsRequest,
};

use crate::connection::{Connection, SingleBatchReader};

#[derive(Default)]
struct ObjectTree {
    catalogs: BTreeMap<String, CatalogEntry>,
}

#[derive(Default)]
struct CatalogEntry {
    schemas: BTreeMap<String, SchemaEntry>,
}

#[derive(Default)]
struct SchemaEntry {
    tables: BTreeMap<String, TableEntry>,
}

#[derive(Default)]
struct TableEntry {
    table_type: String,
    columns: Vec<ColumnEntry>,
}

struct ColumnEntry {
    name: String,
    ordinal_position: i32,
    remarks: Option<String>,
    xdbc_type_name: Option<String>,
    xdbc_column_size: Option<i32>,
    xdbc_decimal_digits: Option<i16>,
    xdbc_num_prec_radix: Option<i16>,
    xdbc_nullable: Option<i16>,
    column_default: Option<String>,
    xdbc_char_octet_length: Option<i32>,
    xdbc_is_nullable: Option<String>,
}

pub(crate) fn execute_get_objects(
    connection: &Connection,
    depth: &ObjectDepth,
    catalog: Option<&str>,
    db_schema: Option<&str>,
    table_name: Option<&str>,
    table_type: Option<Vec<&str>>,
    column_name: Option<&str>,
) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
    let table_types = table_type
        .unwrap_or_default()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let mut objects = ObjectTree::default();

    let catalogs = fetch_core_objects(connection, DEPTH_CATALOGS, catalog, None, None, &[], None)?;
    merge_catalogs(&mut objects, catalogs);

    if !matches!(depth, ObjectDepth::Catalogs) {
        let db_schemas = fetch_core_objects(
            connection,
            DEPTH_DB_SCHEMAS,
            catalog,
            db_schema,
            None,
            &[],
            None,
        )?;
        merge_schemas(&mut objects, db_schemas);
    }

    if matches!(
        depth,
        ObjectDepth::Tables | ObjectDepth::Columns | ObjectDepth::All
    ) {
        // DEPTH_TABLES is the authoritative source for table identity and type.
        // In sf_core f9175f07 DEPTH_COLUMNS neither applies table_type nor fills
        // table_type, so columns are only merged into this filtered table set.
        let tables = fetch_core_objects(
            connection,
            DEPTH_TABLES,
            catalog,
            db_schema,
            table_name,
            &table_types,
            None,
        )?;
        merge_tables(&mut objects, tables);
    }

    if matches!(depth, ObjectDepth::Columns | ObjectDepth::All) {
        let columns = fetch_core_objects(
            connection,
            DEPTH_COLUMNS,
            catalog,
            db_schema,
            table_name,
            &[],
            column_name,
        )?;
        merge_columns(&mut objects, columns);
    }

    let batch = build_batch(&objects, depth).map_err(arrow_to_adbc_error)?;
    Ok(Box::new(SingleBatchReader::new(batch)))
}

#[allow(clippy::too_many_arguments)]
fn fetch_core_objects(
    connection: &Connection,
    depth: i32,
    catalog: Option<&str>,
    db_schema: Option<&str>,
    table_name: Option<&str>,
    table_type: &[String],
    column_name: Option<&str>,
) -> AdbcResult<ObjectTree> {
    let request = GetObjectsRequest {
        conn_handle: connection.conn_handle,
        depth,
        catalog: catalog.map(str::to_owned),
        db_schema: db_schema.map(str::to_owned),
        table_name: table_name.map(str::to_owned),
        table_type: table_type.to_vec(),
        column_name: column_name.map(str::to_owned),
    };
    let result = connection
        .inner
        .runtime
        .block_on(connection.inner.sf.connection_get_objects(request))
        .map_err(crate::error::api_error_to_adbc_error)?;
    let mut reader = connection.inner.acquire_result(result)?.reader;
    let mut objects = ObjectTree::default();
    for batch in &mut reader {
        let batch = batch.map_err(arrow_to_adbc_error)?;
        merge_all(
            &mut objects,
            parse_core_batch(&batch).map_err(arrow_to_adbc_error)?,
        );
    }
    Ok(objects)
}

fn merge_catalogs(target: &mut ObjectTree, source: ObjectTree) {
    for name in source.catalogs.into_keys() {
        target.catalogs.entry(name).or_default();
    }
}

fn merge_schemas(target: &mut ObjectTree, source: ObjectTree) {
    for (catalog_name, catalog) in source.catalogs {
        let target_catalog = target.catalogs.entry(catalog_name).or_default();
        for schema_name in catalog.schemas.into_keys() {
            target_catalog.schemas.entry(schema_name).or_default();
        }
    }
}

fn merge_tables(target: &mut ObjectTree, source: ObjectTree) {
    for (catalog_name, catalog) in source.catalogs {
        let target_catalog = target.catalogs.entry(catalog_name).or_default();
        for (schema_name, schema) in catalog.schemas {
            let target_schema = target_catalog.schemas.entry(schema_name).or_default();
            for (table_name, table) in schema.tables {
                target_schema.tables.insert(
                    table_name,
                    TableEntry {
                        table_type: table.table_type,
                        columns: Vec::new(),
                    },
                );
            }
        }
    }
}

fn merge_columns(target: &mut ObjectTree, source: ObjectTree) {
    for (catalog_name, catalog) in source.catalogs {
        let Some(target_catalog) = target.catalogs.get_mut(&catalog_name) else {
            continue;
        };
        for (schema_name, schema) in catalog.schemas {
            let Some(target_schema) = target_catalog.schemas.get_mut(&schema_name) else {
                continue;
            };
            for (table_name, table) in schema.tables {
                if let Some(target_table) = target_schema.tables.get_mut(&table_name) {
                    target_table.columns = table.columns;
                }
            }
        }
    }
}

fn merge_all(target: &mut ObjectTree, source: ObjectTree) {
    for (catalog_name, catalog) in source.catalogs {
        let target_catalog = target.catalogs.entry(catalog_name).or_default();
        for (schema_name, schema) in catalog.schemas {
            let target_schema = target_catalog.schemas.entry(schema_name).or_default();
            for (table_name, table) in schema.tables {
                target_schema.tables.insert(table_name, table);
            }
        }
    }
}

fn parse_core_batch(batch: &RecordBatch) -> Result<ObjectTree, ArrowError> {
    if batch.num_columns() != 2 {
        return Err(schema_error(format!(
            "sf_core get_objects returned {} top-level columns; expected 2",
            batch.num_columns()
        )));
    }
    let catalog_names = expect_array::<StringArray>(batch.column(0), "catalog_name")?;
    let schema_lists = expect_array::<LargeListArray>(batch.column(1), "catalog_db_schemas")?;
    let schema_values = expect_array::<StructArray>(schema_lists.values(), "schema list values")?;
    if schema_values.num_columns() != 2 {
        return Err(schema_error("sf_core schema struct must have 2 fields"));
    }
    let schema_names = expect_array::<StringArray>(schema_values.column(0), "db_schema_name")?;
    let table_lists = expect_array::<LargeListArray>(schema_values.column(1), "db_schema_tables")?;
    let table_values = expect_array::<StructArray>(table_lists.values(), "table list values")?;
    if table_values.num_columns() != 4 {
        return Err(schema_error("sf_core table struct must have 4 fields"));
    }
    let table_names = expect_array::<StringArray>(table_values.column(0), "table_name")?;
    let table_types = expect_array::<StringArray>(table_values.column(1), "table_type")?;
    let column_lists = expect_array::<LargeListArray>(table_values.column(2), "table_columns")?;
    let column_values = expect_array::<StructArray>(column_lists.values(), "column list values")?;
    let column_arrays = CoreColumnArrays::try_new(column_values)?;

    let mut objects = ObjectTree::default();
    for catalog_index in 0..batch.num_rows() {
        let catalog_name = required_string(catalog_names, catalog_index, "catalog_name")?;
        let catalog = objects.catalogs.entry(catalog_name.to_owned()).or_default();
        let Some(schema_range) = list_range(schema_lists, catalog_index, "catalog_db_schemas")?
        else {
            continue;
        };
        for schema_index in schema_range {
            let schema_name = required_string(schema_names, schema_index, "db_schema_name")?;
            let schema = catalog.schemas.entry(schema_name.to_owned()).or_default();
            let Some(table_range) = list_range(table_lists, schema_index, "db_schema_tables")?
            else {
                continue;
            };
            for table_index in table_range {
                let table_name = required_string(table_names, table_index, "table_name")?;
                let table_type = if table_types.is_null(table_index) {
                    String::new()
                } else {
                    table_types.value(table_index).to_owned()
                };
                let mut table = TableEntry {
                    table_type,
                    columns: Vec::new(),
                };
                if let Some(column_range) = list_range(column_lists, table_index, "table_columns")?
                {
                    table.columns.reserve(column_range.len());
                    for column_index in column_range {
                        table.columns.push(column_arrays.column(column_index)?);
                    }
                }
                schema.tables.insert(table_name.to_owned(), table);
            }
        }
    }
    Ok(objects)
}

struct CoreColumnArrays<'a> {
    names: &'a StringArray,
    ordinals: &'a Int32Array,
    logical_types: &'a StringArray,
    precisions: &'a Int32Array,
    scales: &'a Int32Array,
    char_lengths: &'a Int64Array,
    byte_lengths: &'a Int64Array,
    nullables: &'a BooleanArray,
    defaults: &'a StringArray,
    remarks: &'a StringArray,
}

impl<'a> CoreColumnArrays<'a> {
    fn try_new(values: &'a StructArray) -> Result<Self, ArrowError> {
        if values.num_columns() != 10 {
            return Err(schema_error("sf_core column struct must have 10 fields"));
        }
        Ok(Self {
            names: expect_array::<StringArray>(values.column(0), "column_name")?,
            ordinals: expect_array::<Int32Array>(values.column(1), "ordinal_position")?,
            logical_types: expect_array::<StringArray>(values.column(2), "logical_type")?,
            precisions: expect_array::<Int32Array>(values.column(3), "precision")?,
            scales: expect_array::<Int32Array>(values.column(4), "scale")?,
            char_lengths: expect_array::<Int64Array>(values.column(5), "char_length")?,
            byte_lengths: expect_array::<Int64Array>(values.column(6), "byte_length")?,
            nullables: expect_array::<BooleanArray>(values.column(7), "nullable")?,
            defaults: expect_array::<StringArray>(values.column(8), "column_def")?,
            remarks: expect_array::<StringArray>(values.column(9), "remarks")?,
        })
    }

    fn column(&self, index: usize) -> Result<ColumnEntry, ArrowError> {
        let logical_type = optional_string(self.logical_types, index);
        let nullable = (!self.nullables.is_null(index)).then(|| self.nullables.value(index));
        Ok(ColumnEntry {
            name: required_string(self.names, index, "column_name")?.to_owned(),
            ordinal_position: if self.ordinals.is_null(index) {
                return Err(schema_error("sf_core ordinal_position is null"));
            } else {
                self.ordinals.value(index)
            },
            remarks: optional_string(self.remarks, index).map(str::to_owned),
            xdbc_type_name: logical_type.map(public_snowflake_type_name),
            xdbc_column_size: if !self.char_lengths.is_null(index) {
                Some(checked_i64_to_i32(
                    self.char_lengths.value(index),
                    "char_length",
                )?)
            } else {
                optional_i32(self.precisions, index)
            },
            xdbc_decimal_digits: xdbc_decimal_digits(
                logical_type,
                optional_i32(self.scales, index),
            )?,
            xdbc_num_prec_radix: logical_type.and_then(|logical_type| match logical_type {
                "FIXED" => Some(10),
                "REAL" => Some(2),
                _ => None,
            }),
            xdbc_nullable: nullable.map(|value| if value { 1 } else { 0 }),
            column_default: optional_string(self.defaults, index).map(str::to_owned),
            xdbc_char_octet_length: optional_i64(self.byte_lengths, index)
                .map(|value| checked_i64_to_i32(value, "byte_length"))
                .transpose()?,
            xdbc_is_nullable: nullable.map(|value| if value { "YES" } else { "NO" }.to_owned()),
        })
    }
}

fn xdbc_decimal_digits(
    logical_type: Option<&str>,
    scale: Option<i32>,
) -> Result<Option<i16>, ArrowError> {
    if !matches!(logical_type, Some("FIXED" | "REAL")) {
        return Ok(None);
    }

    scale
        .map(|value| {
            i16::try_from(value).map_err(|_| schema_error("sf_core scale does not fit in Int16"))
        })
        .transpose()
}

fn public_snowflake_type_name(logical_type: &str) -> String {
    match logical_type.to_ascii_uppercase().as_str() {
        "FIXED" => "NUMBER".to_owned(),
        "REAL" => "DOUBLE".to_owned(),
        "TEXT" => "VARCHAR".to_owned(),
        "BINARY" => "BINARY".to_owned(),
        "BOOLEAN" => "BOOLEAN".to_owned(),
        "DATE" => "DATE".to_owned(),
        "TIME" => "TIME".to_owned(),
        "TIMESTAMP_NTZ" => "TIMESTAMP_NTZ".to_owned(),
        "TIMESTAMP_LTZ" => "TIMESTAMP_LTZ".to_owned(),
        "TIMESTAMP_TZ" => "TIMESTAMP_TZ".to_owned(),
        "TIMESTAMP" => "TIMESTAMP_NTZ".to_owned(),
        _ => logical_type.to_owned(),
    }
}

fn build_batch(objects: &ObjectTree, depth: &ObjectDepth) -> Result<RecordBatch, ArrowError> {
    let schemas_null = matches!(depth, ObjectDepth::Catalogs);
    let tables_null = matches!(depth, ObjectDepth::Catalogs | ObjectDepth::Schemas);
    let columns_null = !matches!(depth, ObjectDepth::Columns | ObjectDepth::All);

    let catalogs = objects.catalogs.iter().collect::<Vec<_>>();
    let db_schemas = catalogs
        .iter()
        .flat_map(|(_, catalog)| catalog.schemas.iter())
        .collect::<Vec<_>>();
    let tables = db_schemas
        .iter()
        .flat_map(|(_, schema)| schema.tables.iter())
        .collect::<Vec<_>>();
    let columns = tables
        .iter()
        .flat_map(|(_, table)| table.columns.iter())
        .collect::<Vec<_>>();

    let column_fields = struct_fields(&schemas::COLUMN_SCHEMA)?;
    let column_values = StructArray::try_new(
        column_fields.clone(),
        vec![
            Arc::new(StringArray::from(
                columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int32Array::from(
                columns
                    .iter()
                    .map(|column| column.ordinal_position)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                columns
                    .iter()
                    .map(|column| column.remarks.as_deref())
                    .collect::<Vec<_>>(),
            )),
            new_null_array(column_fields[3].data_type(), columns.len()),
            Arc::new(StringArray::from(
                columns
                    .iter()
                    .map(|column| column.xdbc_type_name.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                columns
                    .iter()
                    .map(|column| column.xdbc_column_size)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int16Array::from(
                columns
                    .iter()
                    .map(|column| column.xdbc_decimal_digits)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int16Array::from(
                columns
                    .iter()
                    .map(|column| column.xdbc_num_prec_radix)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int16Array::from(
                columns
                    .iter()
                    .map(|column| column.xdbc_nullable)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                columns
                    .iter()
                    .map(|column| column.column_default.as_deref())
                    .collect::<Vec<_>>(),
            )),
            new_null_array(column_fields[10].data_type(), columns.len()),
            new_null_array(column_fields[11].data_type(), columns.len()),
            Arc::new(Int32Array::from(
                columns
                    .iter()
                    .map(|column| column.xdbc_char_octet_length)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                columns
                    .iter()
                    .map(|column| column.xdbc_is_nullable.as_deref())
                    .collect::<Vec<_>>(),
            )),
            new_null_array(column_fields[14].data_type(), columns.len()),
            new_null_array(column_fields[15].data_type(), columns.len()),
            new_null_array(column_fields[16].data_type(), columns.len()),
            new_null_array(column_fields[17].data_type(), columns.len()),
            new_null_array(column_fields[18].data_type(), columns.len()),
        ],
        None,
    )?;

    let column_item = list_item_from_type(&schemas::TABLE_SCHEMA, 2)?;
    let column_list = if columns_null {
        all_null_list(column_item, tables.len())
    } else {
        let offsets = offsets_from_counts(
            tables.iter().map(|(_, table)| table.columns.len()),
            "table_columns",
        )?;
        ListArray::try_new(
            column_item,
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Arc::new(column_values),
            None,
        )?
    };
    let constraint_list = all_null_list(
        list_item_from_type(&schemas::TABLE_SCHEMA, 3)?,
        tables.len(),
    );

    let table_values = StructArray::try_new(
        struct_fields(&schemas::TABLE_SCHEMA)?,
        vec![
            Arc::new(StringArray::from(
                tables
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                tables
                    .iter()
                    .map(|(_, table)| table.table_type.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(column_list),
            Arc::new(constraint_list),
        ],
        None,
    )?;

    let table_item = list_item_from_type(&schemas::OBJECTS_DB_SCHEMA_SCHEMA, 1)?;
    let table_list = if tables_null {
        all_null_list(table_item, db_schemas.len())
    } else {
        let offsets = offsets_from_counts(
            db_schemas.iter().map(|(_, schema)| schema.tables.len()),
            "db_schema_tables",
        )?;
        ListArray::try_new(
            table_item,
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Arc::new(table_values),
            None,
        )?
    };

    let schema_values = StructArray::try_new(
        struct_fields(&schemas::OBJECTS_DB_SCHEMA_SCHEMA)?,
        vec![
            Arc::new(StringArray::from(
                db_schemas
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(table_list),
        ],
        None,
    )?;

    let schema_item = list_item(schemas::GET_OBJECTS_SCHEMA.field(1).data_type())?;
    let schema_list = if schemas_null {
        all_null_list(schema_item, catalogs.len())
    } else {
        let offsets = offsets_from_counts(
            catalogs.iter().map(|(_, catalog)| catalog.schemas.len()),
            "catalog_db_schemas",
        )?;
        ListArray::try_new(
            schema_item,
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Arc::new(schema_values),
            None,
        )?
    };

    RecordBatch::try_new(
        schemas::GET_OBJECTS_SCHEMA.clone(),
        vec![
            Arc::new(StringArray::from(
                catalogs
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(schema_list),
        ],
    )
}

fn offsets_from_counts(
    counts: impl IntoIterator<Item = usize>,
    field: &str,
) -> Result<Vec<i32>, ArrowError> {
    let mut offsets = vec![0i32];
    for count in counts {
        let count = i32::try_from(count).map_err(|_| {
            schema_error(format!(
                "get_objects {field} count {count} exceeds ADBC List capacity"
            ))
        })?;
        let next = offsets
            .last()
            .copied()
            .and_then(|offset| offset.checked_add(count))
            .ok_or_else(|| schema_error(format!("get_objects {field} offset overflow")))?;
        offsets.push(next);
    }
    Ok(offsets)
}

fn list_range(
    list: &LargeListArray,
    index: usize,
    field: &str,
) -> Result<Option<Range<usize>>, ArrowError> {
    if list.is_null(index) {
        return Ok(None);
    }
    let offsets = list.value_offsets();
    let start = usize::try_from(offsets[index])
        .map_err(|_| schema_error(format!("sf_core {field} has a negative offset")))?;
    let end = usize::try_from(offsets[index + 1])
        .map_err(|_| schema_error(format!("sf_core {field} has a negative offset")))?;
    if start > end || end > list.values().len() {
        return Err(schema_error(format!(
            "sf_core {field} offsets are out of bounds"
        )));
    }
    Ok(Some(start..end))
}

fn all_null_list(item: Arc<Field>, len: usize) -> ListArray {
    ListArray::new(
        item.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32; len + 1])),
        new_empty_array(item.data_type()),
        (len > 0).then(|| NullBuffer::new_null(len)),
    )
}

fn required_string<'a>(
    array: &'a StringArray,
    index: usize,
    field: &str,
) -> Result<&'a str, ArrowError> {
    if array.is_null(index) {
        Err(schema_error(format!("sf_core {field} is null")))
    } else {
        Ok(array.value(index))
    }
}

fn optional_string(array: &StringArray, index: usize) -> Option<&str> {
    (!array.is_null(index)).then(|| array.value(index))
}

fn optional_i32(array: &Int32Array, index: usize) -> Option<i32> {
    (!array.is_null(index)).then(|| array.value(index))
}

fn optional_i64(array: &Int64Array, index: usize) -> Option<i64> {
    (!array.is_null(index)).then(|| array.value(index))
}

fn checked_i64_to_i32(value: i64, field: &str) -> Result<i32, ArrowError> {
    i32::try_from(value)
        .map_err(|_| schema_error(format!("sf_core {field} value {value} exceeds Int32")))
}

fn expect_array<'a, T: 'static>(array: &'a dyn Array, field: &str) -> Result<&'a T, ArrowError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| schema_error(format!("sf_core get_objects field {field} has wrong type")))
}

fn struct_fields(data_type: &DataType) -> Result<Fields, ArrowError> {
    match data_type {
        DataType::Struct(fields) => Ok(fields.clone()),
        _ => Err(schema_error("expected ADBC struct type")),
    }
}

fn list_item(data_type: &DataType) -> Result<Arc<Field>, ArrowError> {
    match data_type {
        DataType::List(item) => Ok(item.clone()),
        _ => Err(schema_error("expected ADBC List type")),
    }
}

fn list_item_from_type(data_type: &DataType, field_index: usize) -> Result<Arc<Field>, ArrowError> {
    let fields = struct_fields(data_type)?;
    list_item(fields[field_index].data_type())
}

fn schema_error(message: impl Into<String>) -> ArrowError {
    ArrowError::SchemaError(message.into())
}

fn arrow_to_adbc_error(error: ArrowError) -> Error {
    Error::with_message_and_status(error.to_string(), Status::Internal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;

    fn parent_tree() -> ObjectTree {
        let mut objects = ObjectTree::default();
        objects
            .catalogs
            .entry("database".into())
            .or_default()
            .schemas
            .entry("public".into())
            .or_default();
        objects
    }

    fn column(name: &str, logical_type: &str) -> ColumnEntry {
        ColumnEntry {
            name: name.into(),
            ordinal_position: 1,
            remarks: None,
            xdbc_type_name: Some(public_snowflake_type_name(logical_type)),
            xdbc_column_size: Some(38),
            xdbc_decimal_digits: Some(0),
            xdbc_num_prec_radix: Some(10),
            xdbc_nullable: Some(1),
            column_default: None,
            xdbc_char_octet_length: None,
            xdbc_is_nullable: Some("YES".into()),
        }
    }

    #[test]
    fn public_xdbc_type_names_use_snowflake_sql_names() {
        assert_eq!(public_snowflake_type_name("FIXED"), "NUMBER");
        assert_eq!(public_snowflake_type_name("TEXT"), "VARCHAR");
        assert_eq!(public_snowflake_type_name("REAL"), "DOUBLE");
        assert_eq!(public_snowflake_type_name("TIMESTAMP_LTZ"), "TIMESTAMP_LTZ");
    }

    #[test]
    fn decimal_digits_only_use_numeric_scales() {
        assert_eq!(
            xdbc_decimal_digits(Some("FIXED"), Some(2)).unwrap(),
            Some(2)
        );
        assert_eq!(xdbc_decimal_digits(Some("REAL"), Some(7)).unwrap(), Some(7));
        assert_eq!(xdbc_decimal_digits(Some("TIME"), Some(9)).unwrap(), None);
        assert_eq!(
            xdbc_decimal_digits(Some("TIMESTAMP_NTZ"), Some(9)).unwrap(),
            None
        );
        assert_eq!(xdbc_decimal_digits(Some("TEXT"), Some(0)).unwrap(), None);
    }

    #[test]
    fn column_depth_keeps_filtered_table_types_and_ignores_other_column_tables() {
        let mut objects = parent_tree();
        let mut filtered_tables = parent_tree();
        filtered_tables
            .catalogs
            .get_mut("database")
            .unwrap()
            .schemas
            .get_mut("public")
            .unwrap()
            .tables
            .insert(
                "wanted_view".into(),
                TableEntry {
                    table_type: "VIEW".into(),
                    columns: Vec::new(),
                },
            );
        merge_tables(&mut objects, filtered_tables);

        let mut column_results = parent_tree();
        let tables = &mut column_results
            .catalogs
            .get_mut("database")
            .unwrap()
            .schemas
            .get_mut("public")
            .unwrap()
            .tables;
        tables.insert(
            "wanted_view".into(),
            TableEntry {
                table_type: String::new(),
                columns: vec![column("amount", "FIXED"), column("label", "TEXT")],
            },
        );
        tables.insert(
            "filtered_out_table".into(),
            TableEntry {
                table_type: String::new(),
                columns: vec![column("hidden", "FIXED")],
            },
        );
        merge_columns(&mut objects, column_results);

        let batch = build_batch(&objects, &ObjectDepth::Columns).unwrap();
        assert_eq!(batch.schema(), schemas::GET_OBJECTS_SCHEMA.clone());
        let schemas = batch.column(1).as_list::<i32>();
        let schema_values = schemas.values().as_struct();
        let table_lists = schema_values.column(1).as_list::<i32>();
        assert_eq!(table_lists.value_length(0), 1);
        let table_values = table_lists.values().as_struct();
        assert_eq!(
            table_values.column(0).as_string::<i32>().value(0),
            "wanted_view"
        );
        assert_eq!(table_values.column(1).as_string::<i32>().value(0), "VIEW");
        let column_values = table_values.column(2).as_list::<i32>().values().as_struct();
        let type_names = column_values.column(4).as_string::<i32>();
        assert_eq!(type_names.value(0), "NUMBER");
        assert_eq!(type_names.value(1), "VARCHAR");
        assert!(table_values.column(3).as_list::<i32>().is_null(0));
    }

    #[test]
    fn deeper_filter_with_no_matches_retains_catalog_and_schema_parents() {
        let objects = parent_tree();
        let batch = build_batch(&objects, &ObjectDepth::Columns).unwrap();
        let schema_lists = batch.column(1).as_list::<i32>();
        assert!(!schema_lists.is_null(0));
        assert_eq!(schema_lists.value_length(0), 1);
        let schema_values = schema_lists.values().as_struct();
        assert_eq!(
            schema_values.column(0).as_string::<i32>().value(0),
            "public"
        );
        let table_lists = schema_values.column(1).as_list::<i32>();
        assert!(!table_lists.is_null(0));
        assert_eq!(table_lists.value_length(0), 0);
    }

    #[test]
    fn shallower_depths_use_null_child_lists() {
        let objects = parent_tree();
        let catalogs = build_batch(&objects, &ObjectDepth::Catalogs).unwrap();
        assert!(catalogs.column(1).as_list::<i32>().is_null(0));

        let db_schemas = build_batch(&objects, &ObjectDepth::Schemas).unwrap();
        let schema_values = db_schemas.column(1).as_list::<i32>().values().as_struct();
        assert!(schema_values.column(1).as_list::<i32>().is_null(0));
    }

    #[test]
    fn list_offset_overflow_is_rejected() {
        let error = offsets_from_counts([i32::MAX as usize, 1], "test").unwrap_err();
        assert!(error.to_string().contains("offset overflow"));
    }
}
