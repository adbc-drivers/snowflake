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

// src/get_objects.rs

use std::sync::Arc;

use adbc_core::{
    error::{Error, Result, Status},
    options::ObjectDepth,
    schemas,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int16Array, Int32Array, ListArray, RecordBatch,
    RecordBatchReader, StringArray, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields};

use crate::connection::{Connection, SingleBatchReader};

// ── SQL helpers ───────────────────────────────────────────────────────────────

fn sql_esc(s: &str) -> String {
    s.replace('\'', "''")
}

/// Converts an optional filter into a SQL ILIKE pattern. None → match-all.
fn ilike(p: Option<&str>) -> String {
    match p {
        None | Some("") | Some("%") | Some(".*") => "%".to_string(),
        Some(s) => sql_esc(s),
    }
}

/// Returns an `information_schema.` prefix, optionally qualified by a specific
/// catalog when the filter is an exact name (not a wildcard).
fn info_prefix(catalog_filter: Option<&str>) -> String {
    match catalog_filter {
        Some(c) if !c.is_empty() && !c.contains('%') && !c.contains('_') => {
            format!("\"{}\".information_schema.", sql_esc(c))
        }
        _ => "information_schema.".to_string(),
    }
}

/// Builds a table-type WHERE fragment, mapping ADBC 'TABLE' → Snowflake 'BASE TABLE'.
fn type_clause(filter: &Option<Vec<&str>>) -> String {
    let Some(types) = filter else {
        return String::new();
    };
    if types.is_empty() {
        return String::new();
    }
    let quoted: Vec<String> = types
        .iter()
        .map(|t| {
            let up = t.to_uppercase();
            let sf = if up == "TABLE" { "BASE TABLE" } else { t };
            format!("'{}'", sql_esc(sf))
        })
        .collect();
    format!(" AND table_type IN ({})", quoted.join(", "))
}

// ── query execution ───────────────────────────────────────────────────────────

/// Runs a SQL query and returns all rows as `Vec<Vec<Option<String>>>`.
fn run_query(conn: &Connection, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
    let stmt = conn
        .inner
        .sf
        .statement_new(conn.conn_handle)
        .map_err(crate::error::api_error_to_adbc_error)?;

    let result = conn.inner.runtime.block_on(async {
        conn.inner
            .sf
            .statement_set_sql_query(stmt, sql.to_string())
            .await?;
        conn.inner.sf.statement_execute_query(stmt, None).await
    });
    let _ = conn.inner.sf.statement_release(stmt);
    let exec = result.map_err(crate::error::api_error_to_adbc_error)?;

    let raw = Box::into_raw(exec.stream) as *mut arrow_array::ffi_stream::FFI_ArrowArrayStream;
    let reader = unsafe { arrow_array::ffi_stream::ArrowArrayStreamReader::from_raw(raw) }
        .map_err(|e| Error::with_message_and_status(e.to_string(), Status::IO))?;

    let mut rows = Vec::new();
    for batch_res in reader {
        let batch =
            batch_res.map_err(|e| Error::with_message_and_status(e.to_string(), Status::IO))?;
        let ncols = batch.num_columns();
        for row_idx in 0..batch.num_rows() {
            let row = (0..ncols)
                .map(|c| cell_to_string(batch.column(c).as_ref(), row_idx))
                .collect();
            rows.push(row);
        }
    }
    Ok(rows)
}

fn cell_to_string(arr: &dyn Array, i: usize) -> Option<String> {
    if arr.is_null(i) {
        return None;
    }
    if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
        return Some(s.value(i).to_string());
    }
    if let Some(n) = arr.as_any().downcast_ref::<arrow_array::Int64Array>() {
        return Some(n.value(i).to_string());
    }
    if let Some(n) = arr.as_any().downcast_ref::<arrow_array::Int32Array>() {
        return Some(n.value(i).to_string());
    }
    None
}

// ── data holders ─────────────────────────────────────────────────────────────

struct ColEntry {
    name: String,
    ordinal_position: i32,
    remarks: Option<String>,
    xdbc_type_name: Option<String>,
    xdbc_column_size: Option<i32>,
    xdbc_char_octet_length: Option<i32>,
    xdbc_decimal_digits: Option<i16>,
    xdbc_num_prec_radix: Option<i16>,
    xdbc_nullable: Option<i16>,
    xdbc_is_nullable: Option<String>,
    xdbc_datetime_sub: Option<i16>,
}

struct TableEntry {
    name: String,
    table_type: String,
    columns: Vec<ColEntry>,
}

struct SchemaEntry {
    name: String,
    tables: Vec<TableEntry>,
}

struct CatalogEntry {
    name: String,
    schemas: Vec<SchemaEntry>,
}

// ── data collection ───────────────────────────────────────────────────────────

pub(crate) fn execute_get_objects(
    conn: &Connection,
    depth: &ObjectDepth,
    catalog_filter: Option<&str>,
    db_schema_filter: Option<&str>,
    table_name_filter: Option<&str>,
    table_type_filter: Option<Vec<&str>>,
    column_name_filter: Option<&str>,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    let entries = collect(
        conn,
        depth,
        catalog_filter,
        db_schema_filter,
        table_name_filter,
        table_type_filter,
        column_name_filter,
    )?;
    let batch = build_batch(&entries, &depth)?;
    Ok(Box::new(SingleBatchReader::new(batch)))
}

fn collect(
    conn: &Connection,
    depth: &ObjectDepth,
    catalog_filter: Option<&str>,
    db_schema_filter: Option<&str>,
    table_name_filter: Option<&str>,
    table_type_filter: Option<Vec<&str>>,
    column_name_filter: Option<&str>,
) -> Result<Vec<CatalogEntry>> {
    let cat_pat = ilike(catalog_filter);
    let sch_pat = ilike(db_schema_filter);
    let tbl_pat = ilike(table_name_filter);
    let col_pat = ilike(column_name_filter);

    // 1. Catalogs — always from information_schema.databases (account-wide)
    let cat_rows = run_query(
        conn,
        &format!(
            "SELECT database_name FROM information_schema.databases \
             WHERE database_name ILIKE '{}' ORDER BY database_name",
            cat_pat
        ),
    )?;
    let catalog_names: Vec<String> = cat_rows
        .into_iter()
        .filter_map(|r| r.into_iter().next().flatten())
        .collect();

    if catalog_names.is_empty() || matches!(depth, ObjectDepth::Catalogs) {
        return Ok(catalog_names
            .into_iter()
            .map(|name| CatalogEntry {
                name,
                schemas: vec![],
            })
            .collect());
    }

    // 2. Schemas — scoped to the catalog(s) in view
    // When catalog_filter is a specific database we qualify the prefix; otherwise
    // information_schema resolves to the current database.
    let prefix = info_prefix(catalog_filter);
    let sch_rows = run_query(
        conn,
        &format!(
            "SELECT catalog_name, schema_name FROM {}schemata \
             WHERE catalog_name ILIKE '{}' AND schema_name ILIKE '{}' \
             ORDER BY catalog_name, schema_name",
            prefix, cat_pat, sch_pat
        ),
    )?;

    // Group schemas by catalog
    let mut schemas_by_cat: std::collections::BTreeMap<String, Vec<String>> =
        catalog_names.iter().map(|n| (n.clone(), vec![])).collect();
    for row in &sch_rows {
        if row.len() < 2 {
            continue;
        }
        if let (Some(cat), Some(sch)) = (&row[0], &row[1]) {
            schemas_by_cat
                .entry(cat.clone())
                .or_default()
                .push(sch.clone());
        }
    }

    if matches!(depth, ObjectDepth::Schemas) {
        return Ok(catalog_names
            .into_iter()
            .map(|cat| {
                let schemas = schemas_by_cat
                    .remove(&cat)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|sch| SchemaEntry {
                        name: sch,
                        tables: vec![],
                    })
                    .collect();
                CatalogEntry { name: cat, schemas }
            })
            .collect());
    }

    // 3. Tables
    let tc = type_clause(&table_type_filter);
    let tbl_rows = run_query(
        conn,
        &format!(
            "SELECT table_catalog, table_schema, table_name, \
             CASE table_type WHEN 'BASE TABLE' THEN 'TABLE' ELSE table_type END \
             FROM {}tables \
             WHERE table_catalog ILIKE '{}' AND table_schema ILIKE '{}' \
             AND table_name ILIKE '{}'{} \
             ORDER BY table_catalog, table_schema, table_name",
            prefix, cat_pat, sch_pat, tbl_pat, tc
        ),
    )?;

    // Group tables by (catalog, schema)
    let mut tables_by_key: std::collections::BTreeMap<(String, String), Vec<TableEntry>> =
        std::collections::BTreeMap::new();
    for row in &tbl_rows {
        if row.len() < 4 {
            continue;
        }
        if let (Some(cat), Some(sch), Some(tbl), Some(tt)) = (&row[0], &row[1], &row[2], &row[3]) {
            tables_by_key
                .entry((cat.clone(), sch.clone()))
                .or_default()
                .push(TableEntry {
                    name: tbl.clone(),
                    table_type: tt.clone(),
                    columns: vec![],
                });
        }
    }

    if matches!(depth, ObjectDepth::Tables) {
        return Ok(assemble(catalog_names, schemas_by_cat, tables_by_key));
    }

    // 4. Columns (All / Columns depth)
    let col_rows = run_query(
        conn,
        &format!(
            "SELECT table_catalog, table_schema, table_name, column_name, \
             ordinal_position::INTEGER, comment, data_type, is_nullable, \
             COALESCE(character_maximum_length, numeric_precision)::INTEGER, \
             character_octet_length::INTEGER, numeric_scale::INTEGER, \
             numeric_precision_radix::INTEGER, datetime_precision::INTEGER \
             FROM {}columns \
             WHERE table_catalog ILIKE '{}' AND table_schema ILIKE '{}' \
             AND table_name ILIKE '{}' AND column_name ILIKE '{}' \
             ORDER BY table_catalog, table_schema, table_name, ordinal_position",
            prefix, cat_pat, sch_pat, tbl_pat, col_pat
        ),
    )?;

    for row in &col_rows {
        if row.len() < 13 {
            continue;
        }
        let (Some(cat), Some(sch), Some(tbl), Some(col_name)) =
            (&row[0], &row[1], &row[2], &row[3])
        else {
            continue;
        };
        let key = (cat.clone(), sch.clone());
        if let Some(tables) = tables_by_key.get_mut(&key) {
            if let Some(table) = tables.iter_mut().find(|t| &t.name == tbl) {
                let nullable_str = row[7].clone();
                let nullable_int = nullable_str.as_deref().map(|s| {
                    if s.eq_ignore_ascii_case("YES") {
                        1i16
                    } else {
                        0
                    }
                });
                let ordinal = row[4]
                    .as_deref()
                    .and_then(|s| s.parse::<i32>().ok())
                    .unwrap_or(0);
                table.columns.push(ColEntry {
                    name: col_name.clone(),
                    ordinal_position: ordinal,
                    remarks: row[5].clone(),
                    xdbc_type_name: row[6].clone(),
                    xdbc_column_size: row[8].as_deref().and_then(|s| s.parse().ok()),
                    xdbc_char_octet_length: row[9].as_deref().and_then(|s| s.parse().ok()),
                    xdbc_decimal_digits: row[10].as_deref().and_then(|s| s.parse().ok()),
                    xdbc_num_prec_radix: row[11].as_deref().and_then(|s| s.parse().ok()),
                    xdbc_nullable: nullable_int,
                    xdbc_is_nullable: nullable_str,
                    xdbc_datetime_sub: row[12].as_deref().and_then(|s| s.parse().ok()),
                });
            }
        }
    }

    Ok(assemble(catalog_names, schemas_by_cat, tables_by_key))
}

fn assemble(
    catalog_names: Vec<String>,
    mut schemas_by_cat: std::collections::BTreeMap<String, Vec<String>>,
    mut tables_by_key: std::collections::BTreeMap<(String, String), Vec<TableEntry>>,
) -> Vec<CatalogEntry> {
    catalog_names
        .into_iter()
        .map(|cat| {
            let schema_names = schemas_by_cat.remove(&cat).unwrap_or_default();
            let schemas = schema_names
                .into_iter()
                .map(|sch| {
                    let tables = tables_by_key
                        .remove(&(cat.clone(), sch.clone()))
                        .unwrap_or_default();
                    SchemaEntry { name: sch, tables }
                })
                .collect();
            CatalogEntry { name: cat, schemas }
        })
        .collect()
}

// ── Arrow output construction ─────────────────────────────────────────────────

fn build_batch(entries: &[CatalogEntry], depth: &ObjectDepth) -> Result<RecordBatch> {
    let schemas_null = matches!(depth, ObjectDepth::Catalogs);
    let tables_null = matches!(depth, ObjectDepth::Catalogs | ObjectDepth::Schemas);
    let cols_null = !matches!(depth, ObjectDepth::All | ObjectDepth::Columns);

    // Flatten counts
    let n_cats = entries.len();
    let n_schemas: usize = entries.iter().map(|c| c.schemas.len()).sum();
    let n_tables: usize = entries
        .iter()
        .flat_map(|c| &c.schemas)
        .map(|s| s.tables.len())
        .sum();
    let n_cols: usize = entries
        .iter()
        .flat_map(|c| &c.schemas)
        .flat_map(|s| &s.tables)
        .map(|t| t.columns.len())
        .sum();

    // ── columns ───────────────────────────────────────────────────────────────
    let all_cols: Vec<&ColEntry> = entries
        .iter()
        .flat_map(|c| &c.schemas)
        .flat_map(|s| &s.tables)
        .flat_map(|t| &t.columns)
        .collect();
    let col_struct = build_col_struct(&all_cols);

    // col offsets per table
    let (col_offsets, col_null_buf) = if cols_null {
        (
            vec![0i32; n_tables + 1],
            Some(NullBuffer::new_null(n_tables)),
        )
    } else {
        let mut offs = Vec::with_capacity(n_tables + 1);
        offs.push(0i32);
        for c in entries
            .iter()
            .flat_map(|c| &c.schemas)
            .flat_map(|s| &s.tables)
        {
            let last = *offs.last().unwrap();
            offs.push(last + c.columns.len() as i32);
        }
        (offs, None)
    };
    let col_item_field = Arc::new(item_field_of(&schemas::COLUMN_SCHEMA));
    let col_list = make_list(
        col_item_field,
        col_offsets,
        Arc::new(col_struct),
        col_null_buf,
    )?;

    // ── table_constraints (always null in our impl) ───────────────────────────
    let constraint_item_field = Arc::new(item_field_of(&schemas::CONSTRAINT_SCHEMA));
    let constraint_list = make_all_null_list(constraint_item_field, n_tables)?;

    // ── table struct ──────────────────────────────────────────────────────────
    let tbl_struct = build_table_struct(entries, col_list, constraint_list)?;

    // table offsets per schema
    let (tbl_offsets, tbl_null_buf) = if tables_null {
        (
            vec![0i32; n_schemas + 1],
            Some(NullBuffer::new_null(n_schemas)),
        )
    } else {
        let mut offs = Vec::with_capacity(n_schemas + 1);
        offs.push(0i32);
        for s in entries.iter().flat_map(|c| &c.schemas) {
            let last = *offs.last().unwrap();
            offs.push(last + s.tables.len() as i32);
        }
        (offs, None)
    };
    let tbl_item_field = Arc::new(item_field_of(&schemas::TABLE_SCHEMA));
    let tbl_list = make_list(
        tbl_item_field,
        tbl_offsets,
        Arc::new(tbl_struct),
        tbl_null_buf,
    )?;

    // ── schema struct ─────────────────────────────────────────────────────────
    let sch_struct = build_schema_struct(entries, tbl_list)?;

    // schema offsets per catalog
    let (sch_offsets, sch_null_buf) = if schemas_null {
        (vec![0i32; n_cats + 1], Some(NullBuffer::new_null(n_cats)))
    } else {
        let mut offs = Vec::with_capacity(n_cats + 1);
        offs.push(0i32);
        for c in entries {
            let last = *offs.last().unwrap();
            offs.push(last + c.schemas.len() as i32);
        }
        (offs, None)
    };
    let sch_item_field = Arc::new(item_field_of(&schemas::OBJECTS_DB_SCHEMA_SCHEMA));
    let sch_list = make_list(
        sch_item_field,
        sch_offsets,
        Arc::new(sch_struct),
        sch_null_buf,
    )?;

    // ── top-level batch ───────────────────────────────────────────────────────
    let cat_names: Vec<Option<&str>> = entries.iter().map(|c| Some(c.name.as_str())).collect();
    RecordBatch::try_new(
        schemas::GET_OBJECTS_SCHEMA.clone(),
        vec![
            Arc::new(StringArray::from(cat_names)) as ArrayRef,
            Arc::new(sch_list) as ArrayRef,
        ],
    )
    .map_err(|e| Error::with_message_and_status(e.to_string(), Status::Internal))
}

// ── struct array builders ─────────────────────────────────────────────────────

fn build_col_struct(cols: &[&ColEntry]) -> StructArray {
    let n = cols.len();
    let mut col_name = StringBuilder::with_capacity(n, n * 16);
    let mut ordinal = Int32Builder::with_capacity(n);
    let mut remarks = StringBuilder::with_capacity(n, n * 8);
    let mut xdbc_data_type = Int16Builder::with_capacity(n);
    let mut xdbc_type_name = StringBuilder::with_capacity(n, n * 8);
    let mut xdbc_col_size = Int32Builder::with_capacity(n);
    let mut xdbc_dec_digits = Int16Builder::with_capacity(n);
    let mut xdbc_radix = Int16Builder::with_capacity(n);
    let mut xdbc_nullable = Int16Builder::with_capacity(n);
    let mut xdbc_col_def = StringBuilder::with_capacity(n, 0);
    let mut xdbc_sql_dt = Int16Builder::with_capacity(n);
    let mut xdbc_dt_sub = Int16Builder::with_capacity(n);
    let mut xdbc_octet_len = Int32Builder::with_capacity(n);
    let mut xdbc_is_nullable = StringBuilder::with_capacity(n, n * 4);
    let mut xdbc_scope_cat = StringBuilder::with_capacity(n, 0);
    let mut xdbc_scope_sch = StringBuilder::with_capacity(n, 0);
    let mut xdbc_scope_tbl = StringBuilder::with_capacity(n, 0);
    let mut xdbc_auto_inc = BooleanBuilder::with_capacity(n);
    let mut xdbc_gen_col = BooleanBuilder::with_capacity(n);

    for c in cols {
        col_name.append_value(&c.name);
        ordinal.append_value(c.ordinal_position);
        append_opt_str(&mut remarks, c.remarks.as_deref());
        xdbc_data_type.append_null(); // computed from Arrow type — not available here
        append_opt_str(&mut xdbc_type_name, c.xdbc_type_name.as_deref());
        append_opt_i32(&mut xdbc_col_size, c.xdbc_column_size);
        append_opt_i16(&mut xdbc_dec_digits, c.xdbc_decimal_digits);
        append_opt_i16(&mut xdbc_radix, c.xdbc_num_prec_radix);
        append_opt_i16(&mut xdbc_nullable, c.xdbc_nullable);
        xdbc_col_def.append_null();
        xdbc_sql_dt.append_null();
        append_opt_i16(&mut xdbc_dt_sub, c.xdbc_datetime_sub);
        append_opt_i32(&mut xdbc_octet_len, c.xdbc_char_octet_length);
        append_opt_str(&mut xdbc_is_nullable, c.xdbc_is_nullable.as_deref());
        xdbc_scope_cat.append_null();
        xdbc_scope_sch.append_null();
        xdbc_scope_tbl.append_null();
        xdbc_auto_inc.append_null();
        xdbc_gen_col.append_null();
    }

    let fields = struct_fields(&schemas::COLUMN_SCHEMA);
    StructArray::try_new(
        fields,
        vec![
            Arc::new(col_name.finish()) as ArrayRef,
            Arc::new(ordinal.finish()),
            Arc::new(remarks.finish()),
            Arc::new(xdbc_data_type.finish()),
            Arc::new(xdbc_type_name.finish()),
            Arc::new(xdbc_col_size.finish()),
            Arc::new(xdbc_dec_digits.finish()),
            Arc::new(xdbc_radix.finish()),
            Arc::new(xdbc_nullable.finish()),
            Arc::new(xdbc_col_def.finish()),
            Arc::new(xdbc_sql_dt.finish()),
            Arc::new(xdbc_dt_sub.finish()),
            Arc::new(xdbc_octet_len.finish()),
            Arc::new(xdbc_is_nullable.finish()),
            Arc::new(xdbc_scope_cat.finish()),
            Arc::new(xdbc_scope_sch.finish()),
            Arc::new(xdbc_scope_tbl.finish()),
            Arc::new(xdbc_auto_inc.finish()),
            Arc::new(xdbc_gen_col.finish()),
        ],
        None,
    )
    .expect("column StructArray construction")
}

fn build_table_struct(
    entries: &[CatalogEntry],
    col_list: ListArray,
    constraint_list: ListArray,
) -> Result<StructArray> {
    let n_tables: usize = entries
        .iter()
        .flat_map(|c| &c.schemas)
        .map(|s| s.tables.len())
        .sum();

    let mut tbl_name = StringBuilder::with_capacity(n_tables, n_tables * 16);
    let mut tbl_type = StringBuilder::with_capacity(n_tables, n_tables * 8);

    for t in entries
        .iter()
        .flat_map(|c| &c.schemas)
        .flat_map(|s| &s.tables)
    {
        tbl_name.append_value(&t.name);
        tbl_type.append_value(&t.table_type);
    }

    let fields = struct_fields(&schemas::TABLE_SCHEMA);
    StructArray::try_new(
        fields,
        vec![
            Arc::new(tbl_name.finish()) as ArrayRef,
            Arc::new(tbl_type.finish()),
            Arc::new(col_list),
            Arc::new(constraint_list),
        ],
        None,
    )
    .map_err(|e| Error::with_message_and_status(e.to_string(), Status::Internal))
}

fn build_schema_struct(entries: &[CatalogEntry], tbl_list: ListArray) -> Result<StructArray> {
    let n_schemas: usize = entries.iter().map(|c| c.schemas.len()).sum();
    let mut sch_name = StringBuilder::with_capacity(n_schemas, n_schemas * 16);

    for s in entries.iter().flat_map(|c| &c.schemas) {
        sch_name.append_value(&s.name);
    }

    let fields = struct_fields(&schemas::OBJECTS_DB_SCHEMA_SCHEMA);
    StructArray::try_new(
        fields,
        vec![Arc::new(sch_name.finish()) as ArrayRef, Arc::new(tbl_list)],
        None,
    )
    .map_err(|e| Error::with_message_and_status(e.to_string(), Status::Internal))
}

// ── list array helpers ────────────────────────────────────────────────────────

fn make_list(
    item_field: Arc<Field>,
    offsets: Vec<i32>,
    values: ArrayRef,
    null_buf: Option<NullBuffer>,
) -> Result<ListArray> {
    let offs = OffsetBuffer::new(ScalarBuffer::from(offsets));
    ListArray::try_new(item_field, offs, values, null_buf)
        .map_err(|e| Error::with_message_and_status(e.to_string(), Status::Internal))
}

/// Creates a ListArray where every entry is null (for table_constraints).
fn make_all_null_list(item_field: Arc<Field>, n: usize) -> Result<ListArray> {
    let null_buf = if n > 0 {
        Some(NullBuffer::new_null(n))
    } else {
        None
    };
    let offsets = vec![0i32; n + 1];
    // Empty values StructArray matching the item field's struct type
    let values: ArrayRef = match item_field.data_type() {
        DataType::Struct(fields) => {
            let empty_arrays: Vec<ArrayRef> = fields
                .iter()
                .map(|f| empty_array_for(f.data_type()))
                .collect();
            Arc::new(
                StructArray::try_new(fields.clone(), empty_arrays, None)
                    .map_err(|e| Error::with_message_and_status(e.to_string(), Status::Internal))?,
            )
        }
        _ => {
            return Err(Error::with_message_and_status(
                "expected struct item type",
                Status::Internal,
            ));
        }
    };
    make_list(item_field, offsets, values, null_buf)
}

// ── type extraction helpers ───────────────────────────────────────────────────

/// Extracts the `Fields` from a DataType::Struct stored in an adbc_core schema static.
fn struct_fields(dt: &DataType) -> Fields {
    match dt {
        DataType::Struct(f) => f.clone(),
        _ => panic!("expected DataType::Struct"),
    }
}

/// Returns a `Field { name: "item", data_type: <struct fields>, nullable: true }`
/// that matches what `DataType::new_list(dt, nullable)` produces internally.
fn item_field_of(dt: &DataType) -> Field {
    Field::new("item", dt.clone(), true)
}

/// Returns an empty (zero-length) array for the given DataType, used when
/// building all-null list arrays that still need a correctly-typed values array.
fn empty_array_for(dt: &DataType) -> ArrayRef {
    match dt {
        DataType::Utf8 => Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        DataType::Int16 => Arc::new(Int16Array::from(Vec::<Option<i16>>::new())),
        DataType::Int32 => Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
        DataType::Boolean => Arc::new(BooleanArray::from(Vec::<Option<bool>>::new())),
        DataType::Struct(fields) => {
            let children: Vec<ArrayRef> = fields
                .iter()
                .map(|f| empty_array_for(f.data_type()))
                .collect();
            Arc::new(
                StructArray::try_new(fields.clone(), children, None).expect("empty struct array"),
            )
        }
        DataType::List(f) => {
            let values = empty_array_for(f.data_type());
            let offs = OffsetBuffer::<i32>::new(ScalarBuffer::from(vec![0i32]));
            Arc::new(ListArray::try_new(f.clone(), offs, values, None).expect("empty list array"))
        }
        _ => Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
    }
}

// ── builder shorthand ─────────────────────────────────────────────────────────

use arrow_array::builder::{BooleanBuilder, Int16Builder, Int32Builder, StringBuilder};

fn append_opt_str(b: &mut StringBuilder, v: Option<&str>) {
    match v {
        Some(s) => b.append_value(s),
        None => b.append_null(),
    }
}

fn append_opt_i16(b: &mut Int16Builder, v: Option<i16>) {
    match v {
        Some(n) => b.append_value(n),
        None => b.append_null(),
    }
}

fn append_opt_i32(b: &mut Int32Builder, v: Option<i32>) {
    match v {
        Some(n) => b.append_value(n),
        None => b.append_null(),
    }
}
