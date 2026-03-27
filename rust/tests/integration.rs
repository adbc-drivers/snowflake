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
    options::{OptionConnection, OptionDatabase, OptionValue},
    Connection as _, Database as _, Driver as _, Optionable, Statement as _,
};
use adbc_driver_snowflake::{Database, Driver};
use arrow_array::cast::AsArray;
use arrow_array::Array;
use arrow_schema::{DataType, TimeUnit};

fn get_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

/// Build a configured Database without opening a connection.
/// Callers can set extra options before calling `.new_connection()`.
fn make_db() -> Option<Database> {
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
    Some(db)
}

fn make_connection() -> Option<adbc_driver_snowflake::Connection> {
    Some(make_db()?.new_connection().expect("new_connection"))
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
    assert!(
        !batch.column(0).as_string::<i32>().value(0).is_empty(),
        "CURRENT_USER() is empty"
    );
    assert!(
        !batch.column(1).as_string::<i32>().value(0).is_empty(),
        "CURRENT_WAREHOUSE() is empty"
    );
    assert!(
        !batch.column(2).as_string::<i32>().value(0).is_empty(),
        "CURRENT_ROLE() is empty"
    );
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
    let type_ids = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::UnionArray>()
        .unwrap();
    assert_eq!(
        type_ids.type_id(0),
        0,
        "VendorVersion should be a string union arm"
    );
    let version_str = type_ids.value(0);
    let v = version_str.as_string::<i32>().value(0);
    assert!(!v.is_empty(), "VendorVersion should not be empty");
    // Snowflake versions look like "8.x.x" — just check it contains a dot
    assert!(
        v.contains('.'),
        "VendorVersion should look like a version string, got: {v}"
    );
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

// ── precision option tests ────────────────────────────────────────────────────

/// Verify that the precision options round-trip correctly on Database
/// (no live query required — purely option set/get).
#[test]
fn test_precision_options_defaults_and_round_trip() {
    let Some(mut db) = make_db() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };

    // Defaults
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(
            "adbc.snowflake.sql.client_option.use_high_precision".into()
        ))
        .unwrap(),
        "enabled"
    );
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(
            "adbc.snowflake.sql.client_option.max_timestamp_precision".into()
        ))
        .unwrap(),
        "nanoseconds"
    );

    // Round-trip: disable high precision
    db.set_option(
        OptionDatabase::Other("adbc.snowflake.sql.client_option.use_high_precision".into()),
        OptionValue::String("disabled".into()),
    )
    .unwrap();
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(
            "adbc.snowflake.sql.client_option.use_high_precision".into()
        ))
        .unwrap(),
        "disabled"
    );

    // Round-trip: microsecond timestamps
    db.set_option(
        OptionDatabase::Other("adbc.snowflake.sql.client_option.max_timestamp_precision".into()),
        OptionValue::String("microseconds".into()),
    )
    .unwrap();
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(
            "adbc.snowflake.sql.client_option.max_timestamp_precision".into()
        ))
        .unwrap(),
        "microseconds"
    );

    // Round-trip: nanoseconds_error_on_overflow
    db.set_option(
        OptionDatabase::Other("adbc.snowflake.sql.client_option.max_timestamp_precision".into()),
        OptionValue::String("nanoseconds_error_on_overflow".into()),
    )
    .unwrap();
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(
            "adbc.snowflake.sql.client_option.max_timestamp_precision".into()
        ))
        .unwrap(),
        "nanoseconds_error_on_overflow"
    );
}

/// Mirrors Go's TestUseHighPrecision / TestSchemaWithLowPrecision.
///
/// With high precision (default): NUMBER(10,0) → Int64, NUMBER(15,2) → Decimal128(15,2).
/// With high precision disabled:  NUMBER(10,0) → Int64, NUMBER(15,2) → Float64.
#[test]
fn test_high_precision_get_table_schema() {
    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };

    // Create a permanent table so a second connection can also DESC it.
    {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query(
            "CREATE OR REPLACE TABLE adbc_rust_precision_test \
             (INT_COL NUMBER(10,0), DEC_COL NUMBER(15,2))",
        )
        .unwrap();
        stmt.execute_update().expect("create precision test table");
    }

    // Snowflake folds unquoted identifiers to uppercase.
    // ── high precision (default: enabled) ────────────────────────────────────
    let schema_hp = conn
        .get_table_schema(None, None, "ADBC_RUST_PRECISION_TEST")
        .expect("get_table_schema high precision");

    assert_eq!(
        schema_hp.field_with_name("INT_COL").unwrap().data_type(),
        &DataType::Int64,
        "INT_COL: expected Int64 (scale=0)"
    );
    assert_eq!(
        schema_hp.field_with_name("DEC_COL").unwrap().data_type(),
        &DataType::Decimal128(15, 2),
        "DEC_COL: expected Decimal128(15,2) with high precision"
    );

    // ── low precision (disabled) ──────────────────────────────────────────────
    let Some(mut db_lp) = make_db() else { return };
    db_lp
        .set_option(
            OptionDatabase::Other("adbc.snowflake.sql.client_option.use_high_precision".into()),
            OptionValue::String("disabled".into()),
        )
        .unwrap();
    let conn_lp = db_lp.new_connection().expect("low-precision connection");

    let schema_lp = conn_lp
        .get_table_schema(None, None, "ADBC_RUST_PRECISION_TEST")
        .expect("get_table_schema low precision");

    assert_eq!(
        schema_lp.field_with_name("INT_COL").unwrap().data_type(),
        &DataType::Int64,
        "INT_COL: expected Int64 (scale=0)"
    );
    assert_eq!(
        schema_lp.field_with_name("DEC_COL").unwrap().data_type(),
        &DataType::Float64,
        "DEC_COL: expected Float64 with low precision"
    );

    // Cleanup
    {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query("DROP TABLE IF EXISTS adbc_rust_precision_test")
            .unwrap();
        stmt.execute_update().expect("drop precision test table");
    }
}

/// Mirrors Go's TestTimestampPrecision.
///
/// With nanoseconds (default): TIMESTAMP_NTZ → Timestamp(Nanosecond, None),
///                              TIMESTAMP_TZ  → Timestamp(Nanosecond, UTC).
/// With microseconds:           TIMESTAMP_NTZ → Timestamp(Microsecond, None),
///                              TIMESTAMP_TZ  → Timestamp(Microsecond, UTC).
#[test]
fn test_timestamp_precision_get_table_schema() {
    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };

    {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query(
            "CREATE OR REPLACE TABLE adbc_rust_ts_precision_test \
             (NTZ_COL TIMESTAMP_NTZ, TZ_COL TIMESTAMP_TZ)",
        )
        .unwrap();
        stmt.execute_update()
            .expect("create ts precision test table");
    }

    // Snowflake folds unquoted identifiers to uppercase.
    // ── nanoseconds (default) ─────────────────────────────────────────────────
    let schema_ns = conn
        .get_table_schema(None, None, "ADBC_RUST_TS_PRECISION_TEST")
        .expect("get_table_schema nanoseconds");

    assert_eq!(
        schema_ns.field_with_name("NTZ_COL").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None),
        "NTZ_COL: expected Timestamp(Nanosecond, None)"
    );
    assert_eq!(
        schema_ns.field_with_name("TZ_COL").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        "TZ_COL: expected Timestamp(Nanosecond, UTC)"
    );

    // ── microseconds ──────────────────────────────────────────────────────────
    let Some(mut db_us) = make_db() else { return };
    db_us
        .set_option(
            OptionDatabase::Other(
                "adbc.snowflake.sql.client_option.max_timestamp_precision".into(),
            ),
            OptionValue::String("microseconds".into()),
        )
        .unwrap();
    let conn_us = db_us.new_connection().expect("microsecond connection");

    let schema_us = conn_us
        .get_table_schema(None, None, "ADBC_RUST_TS_PRECISION_TEST")
        .expect("get_table_schema microseconds");

    assert_eq!(
        schema_us.field_with_name("NTZ_COL").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None),
        "NTZ_COL: expected Timestamp(Microsecond, None)"
    );
    assert_eq!(
        schema_us.field_with_name("TZ_COL").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "TZ_COL: expected Timestamp(Microsecond, UTC)"
    );

    // Cleanup
    {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query("DROP TABLE IF EXISTS adbc_rust_ts_precision_test")
            .unwrap();
        stmt.execute_update().expect("drop ts precision test table");
    }
}

// ── missing database options ──────────────────────────────────────────────────

/// Verify simple 1:1 option round-trips through the public Database API.
/// No live connection required — make_db() only needs SNOWFLAKE_URI for the env
/// var check; no actual network call is made until new_connection().
#[test]
fn test_database_options_round_trip() {
    let Some(mut db) = make_db() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };

    let cases: &[(&str, &str)] = &[
        ("adbc.snowflake.sql.region", "us-east-1"),
        ("adbc.snowflake.sql.client_option.login_timeout", "30s"),
        ("adbc.snowflake.sql.client_option.request_timeout", "60s"),
        ("adbc.snowflake.sql.client_option.jwt_expire_timeout", "90s"),
        ("adbc.snowflake.sql.client_option.client_timeout", "120s"),
        (
            "adbc.snowflake.sql.client_option.keep_session_alive",
            "enabled",
        ),
        (
            "adbc.snowflake.sql.client_option.disable_telemetry",
            "enabled",
        ),
        (
            "adbc.snowflake.sql.client_option.cache_mfa_token",
            "enabled",
        ),
        (
            "adbc.snowflake.sql.client_option.store_temp_creds",
            "enabled",
        ),
        ("adbc.snowflake.sql.client_option.tracing", "debug"),
        (
            "adbc.snowflake.sql.client_option.identity_provider",
            "azure",
        ),
    ];

    for (key, val) in cases {
        db.set_option(
            OptionDatabase::Other((*key).into()),
            OptionValue::String((*val).into()),
        )
        .unwrap_or_else(|e| panic!("set_option({key}) failed: {e}"));

        let got = db
            .get_option_string(OptionDatabase::Other((*key).into()))
            .unwrap_or_else(|e| panic!("get_option_string({key}) failed: {e}"));

        assert_eq!(got, *val, "round-trip mismatch for {key}");
    }
}

/// Verify tls_skip_verify round-trips and that the compound TLS flags are
/// readable back through their own option keys.
#[test]
fn test_tls_skip_verify_option() {
    let Some(mut db) = make_db() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };

    // enabled → skip verification
    db.set_option(
        OptionDatabase::Other("adbc.snowflake.sql.client_option.tls_skip_verify".into()),
        OptionValue::String("enabled".into()),
    )
    .expect("set tls_skip_verify enabled");
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(
            "adbc.snowflake.sql.client_option.tls_skip_verify".into()
        ))
        .unwrap(),
        "enabled"
    );

    // disabled → restore verification
    db.set_option(
        OptionDatabase::Other("adbc.snowflake.sql.client_option.tls_skip_verify".into()),
        OptionValue::String("disabled".into()),
    )
    .expect("set tls_skip_verify disabled");
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(
            "adbc.snowflake.sql.client_option.tls_skip_verify".into()
        ))
        .unwrap(),
        "disabled"
    );
}

/// Verify ocsp_fail_open_mode round-trips correctly.
#[test]
fn test_ocsp_fail_open_mode_option() {
    let Some(mut db) = make_db() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };

    db.set_option(
        OptionDatabase::Other("adbc.snowflake.sql.client_option.ocsp_fail_open_mode".into()),
        OptionValue::String("enabled".into()),
    )
    .expect("set ocsp_fail_open_mode enabled");
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(
            "adbc.snowflake.sql.client_option.ocsp_fail_open_mode".into()
        ))
        .unwrap(),
        "enabled"
    );

    db.set_option(
        OptionDatabase::Other("adbc.snowflake.sql.client_option.ocsp_fail_open_mode".into()),
        OptionValue::String("disabled".into()),
    )
    .expect("set ocsp_fail_open_mode disabled");
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(
            "adbc.snowflake.sql.client_option.ocsp_fail_open_mode".into()
        ))
        .unwrap(),
        "disabled"
    );
}

#[test]
fn test_execute_schema() {
    use arrow_schema::{DataType, TimeUnit};

    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };

    // Schema from a plain SELECT
    {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query("SELECT 1::INTEGER AS n, 'hello'::VARCHAR AS s")
            .unwrap();
        let schema = stmt.execute_schema().expect("execute_schema");
        assert_eq!(schema.fields().len(), 2);
    }

    // Schema from a table select matches the table definition
    {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query(
            "CREATE OR REPLACE TEMP TABLE adbc_rust_schema_test \
             (id NUMBER(10,0), val VARCHAR, ts TIMESTAMP_NTZ)",
        )
        .unwrap();
        stmt.execute_update().expect("create table");

        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query("SELECT * FROM ADBC_RUST_SCHEMA_TEST")
            .unwrap();
        let schema = stmt.execute_schema().expect("execute_schema on table");
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "ID");
        assert_eq!(schema.field(1).name(), "VAL");
        assert_eq!(schema.field(2).name(), "TS");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }
}

#[test]
fn test_float64_select_precision() {
    let Some(mut conn) = make_connection() else {
        return;
    };
    let mut stmt = conn.new_statement().expect("new_statement");

    stmt.set_sql_query(
        "SELECT 3.14159265358979::FLOAT as pi, \
         0.0::FLOAT as zero, \
         -1.7976931348623157e308::FLOAT as neg_max, \
         1.7976931348623157e308::FLOAT as pos_max, \
         2.2250738585072014e-308::FLOAT as min_pos, \
         NULL::FLOAT as null_val",
    )
    .expect("set_sql_query");

    let mut reader = stmt.execute().expect("execute");
    let batch = reader.next().expect("batch").expect("ok");

    let check = |col_idx: usize, name: &str, expected_bits: u64| {
        let arr = batch
            .column(col_idx)
            .as_primitive::<arrow_array::types::Float64Type>();
        let v = arr.value(0);
        assert_eq!(
            v.to_bits(),
            expected_bits,
            "{name}: got {v} (bits={:#018x}), expected bits={:#018x}",
            v.to_bits(),
            expected_bits
        );
    };

    check(0, "pi", 0x400921fb54442d11);
    check(1, "zero", 0x0000000000000000);
    check(2, "neg_max", 0xffefffffffffffff);
    check(3, "pos_max", 0x7fefffffffffffff);
    check(4, "min_pos", 0x0010000000000000);

    let null_arr = batch
        .column(5)
        .as_primitive::<arrow_array::types::Float64Type>();
    assert!(null_arr.is_null(0), "null_val should be null");
}
