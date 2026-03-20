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

// tests/integration.rs
use adbc_core::{
    Connection as _, Database as _, Driver as _, Optionable, Statement as _,
    options::{OptionConnection, OptionDatabase, OptionValue},
};
use adbc_snowflake::Driver;
use arrow_array::cast::AsArray;

fn get_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

fn make_connection() -> Option<adbc_snowflake::Connection> {
    let uri = get_env("SNOWFLAKE_URI")?;
    let mut driver = Driver::default();
    let mut db = driver.new_database().expect("new_database");
    db.set_option(OptionDatabase::Uri, OptionValue::String(uri))
        .expect("set uri");
    if let Some(database) = get_env("SNOWFLAKE_DATABASE") {
        db.set_option(
            OptionDatabase::Other("adbc.snowflake.sql.db".into()),
            OptionValue::String(database),
        )
        .expect("set database");
    }
    if let Some(schema) = get_env("SNOWFLAKE_SCHEMA") {
        db.set_option(
            OptionDatabase::Other("adbc.snowflake.sql.schema".into()),
            OptionValue::String(schema),
        )
        .expect("set schema");
    }
    Some(db.new_connection().expect("new_connection"))
}

#[test]
fn test_private_key_simple_query() {
    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };

    let mut stmt = conn.new_statement().expect("new_statement");
    stmt.set_sql_query("SELECT CURRENT_USER(), CURRENT_WAREHOUSE(), CURRENT_ROLE()")
        .expect("set_sql_query");
    let mut reader = stmt.execute().expect("execute");
    let batch = reader.next().expect("no batch").expect("batch error");

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 3);
    assert!(!batch.column(0).as_string::<i32>().value(0).is_empty(), "CURRENT_USER() is empty");
    assert!(!batch.column(1).as_string::<i32>().value(0).is_empty(), "CURRENT_WAREHOUSE() is empty");
    assert!(!batch.column(2).as_string::<i32>().value(0).is_empty(), "CURRENT_ROLE() is empty");
}

#[test]
fn test_select_one() {
    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let mut stmt = conn.new_statement().expect("new_statement");
    stmt.set_sql_query("SELECT 1 AS n").expect("set_sql_query");
    let mut reader = stmt.execute().expect("execute");
    let batch = reader.next().expect("no batch").expect("batch error");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
}

#[test]
fn test_get_table_types() {
    let Some(conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let mut reader = conn.get_table_types().expect("get_table_types");
    let batch = reader.next().expect("no batch").expect("batch error");
    let types: Vec<&str> = batch
        .column(0)
        .as_string::<i32>()
        .iter()
        .flatten()
        .collect();
    assert!(types.contains(&"TABLE"));
    assert!(types.contains(&"VIEW"));
}

#[test]
fn test_get_info_no_codes() {
    let Some(conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let mut reader = conn.get_info(None).expect("get_info");
    let batch = reader.next().expect("no batch").expect("batch error");
    assert!(batch.num_rows() > 0, "expected at least one info row");
}

#[test]
fn test_get_info_vendor_version() {
    use adbc_core::options::InfoCode;
    use std::collections::HashSet;

    let Some(conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };

    let mut reader = conn
        .get_info(Some(HashSet::from([InfoCode::VendorVersion])))
        .expect("get_info");
    let batch = reader.next().expect("no batch").expect("batch error");
    assert_eq!(batch.num_rows(), 1);

    // VendorVersion is a string value — type_id 0 in the union
    use arrow_array::cast::AsArray;
    let type_ids = batch.column(1).as_any().downcast_ref::<arrow_array::UnionArray>().unwrap();
    assert_eq!(type_ids.type_id(0), 0, "VendorVersion should be a string union arm");
    let version_str = type_ids.value(0);
    let v = version_str.as_string::<i32>().value(0);
    assert!(!v.is_empty(), "VendorVersion should not be empty");
    // Snowflake versions look like "8.x.x" — just check it contains a dot
    assert!(v.contains('.'), "VendorVersion should look like a version string, got: {v}");
}

#[test]
fn test_get_option_string_current_catalog_and_schema() {
    let Some(conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };

    let catalog = conn
        .get_option_string(OptionConnection::CurrentCatalog)
        .expect("get CurrentCatalog");
    let schema = conn
        .get_option_string(OptionConnection::CurrentSchema)
        .expect("get CurrentSchema");

    assert!(!catalog.is_empty(), "current catalog should not be empty");
    assert!(!schema.is_empty(), "current schema should not be empty");
}

#[test]
fn test_get_option_string_autocommit() {
    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };

    let ac = conn
        .get_option_string(OptionConnection::AutoCommit)
        .expect("get AutoCommit");
    assert_eq!(ac, "true", "default autocommit should be true");

    conn.set_option(
        OptionConnection::AutoCommit,
        OptionValue::String("false".into()),
    )
    .expect("disable autocommit");
    let ac = conn
        .get_option_string(OptionConnection::AutoCommit)
        .expect("get AutoCommit after disable");
    assert_eq!(ac, "false");
}

#[test]
fn test_execute_ddl_and_dml() {
    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };

    {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query("CREATE OR REPLACE TEMP TABLE adbc_rust_test (id INTEGER, name TEXT)")
            .unwrap();
        stmt.execute_update().expect("create table");
    }

    {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query("INSERT INTO adbc_rust_test VALUES (1, 'hello')")
            .unwrap();
        let rows = stmt.execute_update().expect("insert");
        assert_eq!(rows, Some(1));
    }

    {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query("SELECT * FROM adbc_rust_test").unwrap();
        let mut reader = stmt.execute().expect("select");
        let batch = reader.next().expect("no batch").expect("batch error");
        assert_eq!(batch.num_rows(), 1);
    }

    {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query("DROP TABLE IF EXISTS adbc_rust_test")
            .unwrap();
        stmt.execute_update().expect("drop table");
    }
}
