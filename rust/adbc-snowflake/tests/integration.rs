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
    options::{OptionDatabase, OptionValue},
};
use adbc_snowflake::Driver;
use arrow_array::cast::AsArray;

fn get_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

fn make_connection() -> Option<adbc_snowflake::Connection> {
    let account = get_env("SNOWFLAKE_TEST_ACCOUNT")?;
    let user = get_env("SNOWFLAKE_TEST_USER")?;    

    let mut driver = Driver::default();
    let mut db = driver.new_database().expect("new_database");
    db.set_option(
        OptionDatabase::Other("adbc.snowflake.sql.account".into()),
        OptionValue::String(account),
    )
    .expect("set account");
    db.set_option(OptionDatabase::Username, OptionValue::String(user))
        .expect("set user");    

    if let Some(wh) = get_env("SNOWFLAKE_TEST_WAREHOUSE") {
        db.set_option(
            OptionDatabase::Other("adbc.snowflake.sql.warehouse".into()),
            OptionValue::String(wh),
        )
        .expect("set warehouse");
    }
    if let Some(db_name) = get_env("SNOWFLAKE_TEST_DATABASE") {
        db.set_option(
            OptionDatabase::Other("adbc.snowflake.sql.db".into()),
            OptionValue::String(db_name),
        )
        .expect("set database");
    }
    if let Some(schema) = get_env("SNOWFLAKE_TEST_SCHEMA") {
        db.set_option(
            OptionDatabase::Other("adbc.snowflake.sql.schema".into()),
            OptionValue::String(schema),
        )
        .expect("set schema");
    }

    Some(db.new_connection().expect("new_connection"))
}

fn make_private_key_connection() -> Option<adbc_snowflake::Connection> {
    let account = get_env("SNOWFLAKE_TEST_ACCOUNT")?;
    let user = get_env("SNOWFLAKE_TEST_USER")?;
    let private_key_file = get_env("SNOWFLAKE_TEST_PRIVATE_KEY_FILE")?;

    let mut driver = Driver::default();
    let mut db = driver.new_database().expect("new_database");
    db.set_option(
        OptionDatabase::Other("adbc.snowflake.sql.account".into()),
        OptionValue::String(account),
    )
    .expect("set account");
    db.set_option(OptionDatabase::Username, OptionValue::String(user))
        .expect("set user");
    db.set_option(
        OptionDatabase::Other("adbc.snowflake.sql.auth_type".into()),
        OptionValue::String("SNOWFLAKE_JWT".into()),
    )
    .expect("set auth_type");
    db.set_option(
        OptionDatabase::Other("adbc.snowflake.sql.client_option.jwt_private_key".into()),
        OptionValue::String(private_key_file),
    )
    .expect("set private_key_file");
    if let Some(wh) = get_env("SNOWFLAKE_TEST_WAREHOUSE") {
        db.set_option(
            OptionDatabase::Other("adbc.snowflake.sql.warehouse".into()),
            OptionValue::String(wh),
        )
        .expect("set warehouse");
    }
    if let Some(role) = get_env("SNOWFLAKE_TEST_ROLE") {
        db.set_option(
            OptionDatabase::Other("adbc.snowflake.sql.role".into()),
            OptionValue::String(role),
        )
        .expect("set role");
    }

    Some(db.new_connection().expect("new_connection"))
}

#[test]
fn test_private_key_simple_query() {
    let Some(mut conn) = make_private_key_connection() else {
        eprintln!("Skipping: SNOWFLAKE_TEST_ACCOUNT/USER/PRIVATE_KEY_FILE not set");
        return;
    };

    let expected_user = get_env("SNOWFLAKE_TEST_USER").unwrap();
    let expected_warehouse = get_env("SNOWFLAKE_TEST_WAREHOUSE");
    let expected_role = get_env("SNOWFLAKE_TEST_ROLE");

    let mut stmt = conn.new_statement().expect("new_statement");
    stmt.set_sql_query("SELECT CURRENT_USER(), CURRENT_WAREHOUSE(), CURRENT_ROLE()")
        .expect("set_sql_query");
    let mut reader = stmt.execute().expect("execute");
    let batch = reader.next().expect("no batch").expect("batch error");

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 3);

    let actual_user = batch.column(0).as_string::<i32>().value(0);
    assert_eq!(
        actual_user.to_uppercase(),
        expected_user.to_uppercase(),
        "CURRENT_USER() mismatch"
    );

    if let Some(wh) = expected_warehouse {
        let actual_wh = batch.column(1).as_string::<i32>().value(0);
        assert_eq!(
            actual_wh.to_uppercase(),
            wh.to_uppercase(),
            "CURRENT_WAREHOUSE() mismatch"
        );
    }

    if let Some(role) = expected_role {
        let actual_role = batch.column(2).as_string::<i32>().value(0);
        assert_eq!(
            actual_role.to_uppercase(),
            role.to_uppercase(),
            "CURRENT_ROLE() mismatch"
        );
    }
}

#[test]
fn test_select_one() {
    let Some(mut conn) = make_private_key_connection() else {
        eprintln!("Skipping: SNOWFLAKE_TEST_ACCOUNT/USER/PASSWORD not set");
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
        eprintln!("Skipping: SNOWFLAKE_TEST_ACCOUNT/USER/PASSWORD not set");
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
        eprintln!("Skipping: SNOWFLAKE_TEST_ACCOUNT/USER/PASSWORD not set");
        return;
    };
    let mut reader = conn.get_info(None).expect("get_info");
    let batch = reader.next().expect("no batch").expect("batch error");
    assert!(batch.num_rows() > 0, "expected at least one info row");
}

#[test]
fn test_execute_ddl_and_dml() {
    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_TEST_ACCOUNT/USER/PASSWORD not set");
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
