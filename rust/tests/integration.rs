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

// tests/integration.rs
use adbc_core::{
    Connection as _, Database as _, Driver as _, Optionable, Statement as _,
    options::{ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue},
};
use adbc_driver_snowflake::{Database, Driver};
use arrow_array::Array;
use arrow_array::cast::AsArray;
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

/// A rejected login keeps Snowflake's vendor code and Go's SQLSTATE 08004.
#[test]
fn test_login_failure_diagnostics() {
    let Some(mut db) = make_db() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    db.set_option(
        OptionDatabase::Other("adbc.snowflake.sql.auth_type".into()),
        OptionValue::String("auth_snowflake".into()),
    )
    .unwrap();
    db.set_option(
        OptionDatabase::Password,
        OptionValue::String(format!("not-the-password-{}", uuid::Uuid::new_v4())),
    )
    .unwrap();
    let error = db.new_connection().err().expect("login must fail");
    assert_eq!(
        error.status,
        adbc_core::error::Status::Unauthorized,
        "{error:?}"
    );
    assert_ne!(error.vendor_code, 0, "{error:?}");
    assert!(error.message.starts_with("[snowflake] "), "{error:?}");
    assert_eq!(error.sqlstate.map(|c| c as u8), *b"08004", "{error:?}");
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
fn test_statement_cancel_during_execution() {
    use adbc_core::error::Status;

    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    conn.get_cancel_handle().try_cancel().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    let cancel = stmt.get_cancel_handle();
    let error = cancel.try_cancel().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert_eq!(
        error.message,
        "AdbcStatementCancel: no active query to cancel"
    );
    stmt.set_sql_query("SELECT SYSTEM$WAIT(30)").unwrap();
    std::thread::scope(|scope| {
        let cancelling = scope.spawn(|| {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                match cancel.try_cancel() {
                    Ok(()) => break,
                    Err(error) => assert_eq!(error.status, Status::InvalidState),
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "execution never became active"
                );
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        });
        let error = stmt.execute().err().expect("query must be cancelled");
        cancelling.join().unwrap();
        assert_eq!(error.status, Status::Cancelled, "{error:?}");
    });
    assert_eq!(
        cancel.try_cancel().unwrap_err().status,
        Status::InvalidState
    );
    stmt.set_sql_query("SELECT 1").unwrap();
    let mut reader = stmt.execute().unwrap();
    assert!(reader.next().unwrap().is_ok());
    assert!(reader.next().is_none());
    assert_eq!(
        cancel.try_cancel().unwrap_err().status,
        Status::InvalidState
    );
}

#[test]
fn test_statement_cancel_during_stream_consumption() {
    use adbc_core::error::{Error, Status};

    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let mut stmt = conn.new_statement().unwrap();
    let cancel = stmt.get_cancel_handle();
    stmt.set_sql_query("SELECT SEQ4() FROM TABLE(GENERATOR(ROWCOUNT => 100000))")
        .unwrap();
    let mut reader = stmt.execute().unwrap();
    assert!(reader.next().unwrap().unwrap().num_rows() > 0);
    cancel.try_cancel().unwrap();
    let error = reader.next().unwrap().unwrap_err();
    let arrow_schema::ArrowError::ExternalError(error) = error else {
        panic!("expected an ADBC cancellation error");
    };
    let error = error.downcast_ref::<Error>().unwrap();
    assert_eq!(error.status, Status::Cancelled);
    assert!(error.message.starts_with("[Snowflake] "));
    assert!(reader.next().is_none());
    assert_eq!(
        cancel.try_cancel().unwrap_err().status,
        Status::InvalidState
    );
    drop(reader);

    stmt.set_sql_query("SELECT 1").unwrap();
    let reader = stmt.execute().unwrap();
    drop(reader);
    assert_eq!(
        cancel.try_cancel().unwrap_err().status,
        Status::InvalidState
    );
}

#[test]
fn test_server_query_error_uses_lowercase_prefix() {
    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT FROM").unwrap();
    let error = stmt.execute().err().expect("SQL must be rejected");
    assert_eq!(error.status, adbc_core::error::Status::InvalidArguments);
    assert!(error.message.starts_with("[snowflake] "), "{error:?}");
    assert!(error.message.contains("(query ID: "), "{error:?}");
    assert_eq!(
        stmt.get_cancel_handle().try_cancel().unwrap_err().status,
        adbc_core::error::Status::InvalidState
    );
}

/// Verify that the database options export the driver and sf_core spans in the
/// same remote trace without configuring a process-global telemetry provider.
#[test]
fn test_adbcfile_trace_parent_and_sf_core_children() {
    let Some(mut db) = make_db() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let folder = std::env::temp_dir().join(format!("adbc-snowflake-otel-{}", uuid::Uuid::new_v4()));
    let trace_parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    db.set_option(
        OptionDatabase::Other("adbc.telemetry.traces_exporter".into()),
        OptionValue::String("adbcfile".into()),
    )
    .expect("set traces exporter");
    db.set_option(
        OptionDatabase::Other("adbc.telemetry.traces_folder_path".into()),
        OptionValue::String(folder.to_string_lossy().into_owned()),
    )
    .expect("set traces folder path");
    db.set_option(
        OptionDatabase::Other("adbc.telemetry.trace_parent".into()),
        OptionValue::String(trace_parent.into()),
    )
    .expect("set trace parent");

    let mut conn = db.new_connection().expect("new_connection");
    let mut stmt = conn.new_statement().expect("new_statement");
    stmt.set_sql_query("SELECT 1").expect("set_sql_query");
    let mut reader = stmt.execute().expect("execute");
    assert_eq!(
        reader
            .next()
            .expect("no batch")
            .expect("batch error")
            .num_rows(),
        1
    );
    drop(reader);
    drop(stmt);
    drop(conn);
    drop(db); // the last Inner drops and synchronously flushes its batch exporter

    let spans: Vec<serde_json::Value> = std::fs::read_dir(&folder)
        .expect("tracing folder")
        .flat_map(|file| {
            std::fs::read_to_string(file.expect("trace file").path())
                .expect("read trace file")
                .lines()
                .map(|line| serde_json::from_str(line).expect("valid span JSON"))
                .collect::<Vec<_>>()
        })
        .collect();
    let trace_id = "4bf92f3577b34da6a3ce929d0e0e4736";
    // The exporter is process-wide, so concurrently running tests' spans can
    // land in this folder too; only this test's remote trace counts.
    let spans: Vec<_> = spans
        .into_iter()
        .filter(|span| span["SpanContext"]["TraceID"] == trace_id)
        .collect();
    let open = spans
        .iter()
        .find(|span| span["Name"] == "databaseImpl.Open")
        .expect("Open span in the remote trace");
    let execute = spans
        .iter()
        .find(|span| span["Name"] == "statement.ExecuteQuery")
        .expect("ExecuteQuery span in the remote trace");
    assert!(
        spans.iter().any(|span| {
            let name = span["Name"].as_str().unwrap_or_default();
            name != "databaseImpl.Open"
                && name != "statement.ExecuteQuery"
                && span["SpanContext"]["TraceID"] == trace_id
                && (span["Parent"]["SpanID"] == open["SpanContext"]["SpanID"]
                    || span["Parent"]["SpanID"] == execute["SpanContext"]["SpanID"])
        }),
        "expected an sf_core span nested below Open or ExecuteQuery"
    );
    if std::env::var_os("ADBC_TRACE_REPORT_TREE").is_some() {
        fn print_subtree(root: &serde_json::Value, spans: &[serde_json::Value], depth: usize) {
            let start = chrono::DateTime::parse_from_rfc3339(root["StartTime"].as_str().unwrap())
                .expect("span start");
            let end = chrono::DateTime::parse_from_rfc3339(root["EndTime"].as_str().unwrap())
                .expect("span end");
            let duration_ms = (end - start).num_microseconds().unwrap_or_default() as f64 / 1000.0;
            eprintln!(
                "{}{} {:.3} ms",
                "  ".repeat(depth),
                root["Name"].as_str().unwrap_or_default(),
                duration_ms,
            );
            for child in spans.iter().filter(|child| {
                child["SpanContext"]["TraceID"] == root["SpanContext"]["TraceID"]
                    && child["Parent"]["SpanID"] == root["SpanContext"]["SpanID"]
            }) {
                print_subtree(child, spans, depth + 1);
            }
        }
        eprintln!("SELECT 1 trace (trace_id={trace_id}):");
        print_subtree(open, &spans, 0);
        print_subtree(execute, &spans, 0);
    }
    std::fs::remove_dir_all(folder).expect("remove trace folder");
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
        let rows = stmt.execute_update().expect("create table");
        assert_eq!(rows, None, "sf_core should classify DDL as no-result");
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
        let rows = stmt.execute_update().expect("drop table");
        assert_eq!(rows, None, "sf_core should classify DDL as no-result");
    }
}

// ── precision option tests ────────────────────────────────────────────────────

/// Verify that the timestamp precision option round-trips correctly on
/// Database (no live query required — purely option set/get).
#[test]
fn test_precision_options_defaults_and_round_trip() {
    let Some(mut db) = make_db() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };

    // Defaults
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(
            "adbc.snowflake.sql.client_option.max_timestamp_precision".into()
        ))
        .unwrap(),
        "nanoseconds"
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

/// NUMBER columns keep the Arrow types chosen by the server: scale 0 is a
/// signed integer and scale > 0 is an exact Decimal128 of the declared
/// precision and scale.
#[test]
fn test_get_table_schema_uses_server_numeric_types() {
    let table_name = format!(
        "ADBC_RUST_PRECISION_TEST_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros()
    );
    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };

    {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query(format!(
            "CREATE OR REPLACE TABLE {} (INT_COL NUMBER(10,0), DEC_COL NUMBER(15,2))",
            table_name
        ))
        .unwrap();
        stmt.execute_update().expect("create precision test table");
    }

    // Snowflake folds unquoted identifiers to uppercase.
    let schema = conn
        .get_table_schema(None, None, &table_name)
        .expect("get_table_schema");

    let int_type = schema.field_with_name("INT_COL").unwrap().data_type();
    assert!(
        matches!(
            int_type,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        ),
        "INT_COL: expected a signed integer type, got {int_type:?}"
    );
    assert_eq!(
        schema.field_with_name("DEC_COL").unwrap().data_type(),
        &DataType::Decimal128(15, 2),
        "DEC_COL: expected Decimal128(15,2)"
    );

    // Cleanup
    {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query(format!("DROP TABLE IF EXISTS {}", table_name))
            .unwrap();
        stmt.execute_update().expect("drop precision test table");
    }
}

#[test]
fn test_get_objects_constraints_and_table_schema_metadata() {
    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let catalog = conn
        .get_option_string(OptionConnection::CurrentCatalog)
        .unwrap();
    let schema_name = conn
        .get_option_string(OptionConnection::CurrentSchema)
        .unwrap();
    let suffix = format!(
        "{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros()
    );
    let parent = format!("ADBC_RUST_PARENT_{suffix}");
    let child = format!("ADBC_RUST_CHILD_{suffix}");
    for sql in [
        format!(
            "CREATE TEMPORARY TABLE \"{parent}\" (\
             \"id_a\" NUMBER(10,0), \"id_b\" NUMBER(10,0), \
             \"label\" VARCHAR(20) COMMENT 'display label', \
             CONSTRAINT \"pk_parent\" PRIMARY KEY (\"id_a\", \"id_b\"), \
             CONSTRAINT \"uq_parent\" UNIQUE (\"label\"))"
        ),
        format!(
            "CREATE TEMPORARY TABLE \"{child}\" (\
             \"ref_a\" NUMBER(10,0), \"ref_b\" NUMBER(10,0), \
             CONSTRAINT \"fk_child\" FOREIGN KEY (\"ref_b\", \"ref_a\") \
             REFERENCES \"{parent}\" (\"id_a\", \"id_b\"))"
        ),
    ] {
        let mut statement = conn.new_statement().unwrap();
        statement.set_sql_query(sql).unwrap();
        statement.execute_update().unwrap();
    }

    let schema = conn.get_table_schema(None, None, &parent).unwrap();
    assert_eq!(
        schema.field_with_name("id_a").unwrap().metadata()["PRIMARY_KEY"],
        "Y"
    );
    assert_eq!(
        schema.field_with_name("id_a").unwrap().metadata()["DATA_TYPE"],
        "NUMBER(10,0)"
    );
    let label = schema.field_with_name("label").unwrap();
    assert_eq!(label.metadata()["PRIMARY_KEY"], "N");
    assert_eq!(label.metadata()["COMMENT"], "display label");

    conn.set_option(
        OptionConnection::Other("adbc.connection.get_objects.match_exactly".into()),
        OptionValue::String("true".into()),
    )
    .unwrap();

    for (table, expected_names, expected_types) in [
        (
            &parent,
            vec!["pk_parent", "uq_parent"],
            vec!["PRIMARY KEY", "UNIQUE"],
        ),
        (&child, vec!["fk_child"], vec!["FOREIGN KEY"]),
    ] {
        let mut reader = conn
            .get_objects(
                ObjectDepth::All,
                Some(&catalog),
                Some(&schema_name),
                Some(table),
                None,
                None,
            )
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let db_schemas = batch.column(1).as_list::<i32>().values().as_struct();
        let tables = db_schemas.column(1).as_list::<i32>().values().as_struct();
        let table_names = tables.column(0).as_string::<i32>();
        let table_index = (0..tables.len())
            .find(|&index| table_names.value(index) == table)
            .expect("created table in GetObjects");
        let constraints_list = tables.column(3).as_list::<i32>();
        let constraints = constraints_list.value(table_index);
        let constraints = constraints.as_struct();
        let names = constraints.column(0).as_string::<i32>();
        let types = constraints.column(1).as_string::<i32>();
        assert_eq!(
            (0..constraints.len())
                .map(|index| names.value(index))
                .collect::<Vec<_>>(),
            expected_names
        );
        assert_eq!(
            (0..constraints.len())
                .map(|index| types.value(index))
                .collect::<Vec<_>>(),
            expected_types
        );
        let columns = constraints.column(2).as_list::<i32>();
        let usages = constraints.column(3).as_list::<i32>();
        if table == &parent {
            let pk = columns.value(0);
            let pk = pk.as_string::<i32>();
            assert_eq!(pk.value(0), "id_a");
            assert_eq!(pk.value(1), "id_b");
            assert!(usages.is_null(0));
            assert!(usages.is_null(1));
        } else {
            let fk = columns.value(0);
            let fk = fk.as_string::<i32>();
            assert_eq!(fk.value(0), "ref_b");
            assert_eq!(fk.value(1), "ref_a");
            let referenced = usages.value(0);
            let referenced = referenced.as_struct();
            assert_eq!(referenced.column(0).as_string::<i32>().value(0), catalog);
            assert_eq!(
                referenced.column(1).as_string::<i32>().value(0),
                schema_name
            );
            assert_eq!(referenced.column(2).as_string::<i32>().value(0), parent);
            assert_eq!(referenced.column(3).as_string::<i32>().value(0), "id_a");
            assert_eq!(referenced.column(3).as_string::<i32>().value(1), "id_b");
        }
    }
    for filter in [
        parent.to_ascii_lowercase(),
        format!("{parent}%"),
        "%".into(),
        "_".into(),
        String::new(),
    ] {
        let mut reader = conn
            .get_objects(
                ObjectDepth::Columns,
                Some(&catalog),
                Some(&schema_name),
                Some(&filter),
                None,
                None,
            )
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let schemas = batch.column(1).as_list::<i32>().values().as_struct();
        let tables = schemas.column(1).as_list::<i32>().values().as_struct();
        assert_eq!(tables.len(), 0, "{filter:?} must not match literally");
    }

    let mut reader = conn
        .get_objects(
            ObjectDepth::Columns,
            Some(&catalog),
            Some(&schema_name),
            Some(&parent),
            None,
            Some(""),
        )
        .unwrap();
    let batch = reader.next().unwrap().unwrap();
    let schemas = batch.column(1).as_list::<i32>().values().as_struct();
    let tables = schemas.column(1).as_list::<i32>().values().as_struct();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables.column(2).as_list::<i32>().value_length(0), 0);
    assert_eq!(tables.column(3).as_list::<i32>().value_length(0), 2);

    // SHOW COLUMNS renumbers a filtered result; the original third-column
    // ordinal must survive both a LIKE column filter and a case-insensitive
    // table filter, without losing that table's constraints.
    conn.set_option(
        OptionConnection::Other("adbc.connection.get_objects.match_exactly".into()),
        OptionValue::String("false".into()),
    )
    .unwrap();
    let lowercase_parent = parent.to_ascii_lowercase();
    let mut reader = conn
        .get_objects(
            ObjectDepth::Columns,
            Some(&catalog),
            Some(&schema_name),
            Some(&lowercase_parent),
            None,
            Some("lab_l"),
        )
        .unwrap();
    let (batch, catalog_index) = reader
        .find_map(|batch| {
            let batch = batch.unwrap();
            let names = batch.column(0).as_string::<i32>();
            let index = (0..batch.num_rows()).find(|&index| names.value(index) == catalog);
            index.map(|index| (batch, index))
        })
        .expect("GetObjects must include the current catalog");
    let schema_values = batch.column(1).as_list::<i32>().value(catalog_index);
    let schemas = schema_values.as_struct();
    let schema_names = schemas.column(0).as_string::<i32>();
    let schema_index = (0..schemas.len())
        .find(|&index| schema_names.value(index) == schema_name)
        .expect("GetObjects must include the current schema");
    let table_values = schemas.column(1).as_list::<i32>().value(schema_index);
    let tables = table_values.as_struct();
    let table_names = tables.column(0).as_string::<i32>();
    let table_index = (0..tables.len())
        .find(|&index| table_names.value(index) == parent)
        .expect("case-insensitive table filter must include created table");
    let columns = tables.column(2).as_list::<i32>().value(table_index);
    let columns = columns.as_struct();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns.column(0).as_string::<i32>().value(0), "label");
    assert_eq!(
        columns
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>()
            .value(0),
        3
    );
    assert_eq!(
        tables.column(3).as_list::<i32>().value_length(table_index),
        2
    );
}

/// Mirrors Go's TestTimestampPrecision.
///
/// With nanoseconds (default): TIMESTAMP_NTZ → Timestamp(Nanosecond, None),
///                              TIMESTAMP_TZ  → Timestamp(Nanosecond, UTC).
/// With microseconds:           TIMESTAMP_NTZ → Timestamp(Microsecond, None),
///                              TIMESTAMP_TZ  → Timestamp(Microsecond, UTC).
#[test]
fn test_timestamp_precision_get_table_schema() {
    let table_name = format!(
        "ADBC_RUST_TS_PRECISION_TEST_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros()
    );
    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };

    {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query(format!(
            "CREATE OR REPLACE TABLE {} (NTZ_COL TIMESTAMP_NTZ, TZ_COL TIMESTAMP_TZ)",
            table_name
        ))
        .unwrap();
        stmt.execute_update()
            .expect("create ts precision test table");
    }

    // Snowflake folds unquoted identifiers to uppercase.
    // ── nanoseconds (default) ─────────────────────────────────────────────────
    let schema_ns = conn
        .get_table_schema(None, None, &table_name)
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
        .get_table_schema(None, None, &table_name)
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
        stmt.set_sql_query(format!("DROP TABLE IF EXISTS {}", table_name))
            .unwrap();
        stmt.execute_update().expect("drop ts precision test table");
    }
}

// ── database options ──────────────────────────────────────────────────────────

/// Verify simple 1:1 option round-trips through the public Database API.
/// No live connection required — make_db() only needs SNOWFLAKE_URI for the env
/// var check; no actual network call is made until new_connection().
#[test]
fn test_database_options_round_trip() {
    let Some(mut db) = make_db() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };

    // Boolean options accept enabled/disabled but read back as true/false, as in Go.
    let cases: &[(&str, &str, &str)] = &[
        (
            "adbc.snowflake.sql.client_option.login_timeout",
            "30s",
            "30s",
        ),
        (
            "adbc.snowflake.sql.client_option.request_timeout",
            "60s",
            "60s",
        ),
        (
            "adbc.snowflake.sql.client_option.client_timeout",
            "120s",
            "120s",
        ),
        (
            "adbc.snowflake.sql.client_option.keep_session_alive",
            "enabled",
            "true",
        ),
        (
            "adbc.snowflake.sql.client_option.disable_telemetry",
            "true",
            "true",
        ),
        (
            "adbc.snowflake.sql.client_option.cache_mfa_token",
            "enabled",
            "true",
        ),
        (
            "adbc.snowflake.sql.client_option.store_temp_creds",
            "false",
            "false",
        ),
        (
            "adbc.snowflake.sql.client_option.identity_provider",
            "azure",
            "azure",
        ),
    ];

    for (key, val, expected) in cases {
        db.set_option(
            OptionDatabase::Other((*key).into()),
            OptionValue::String((*val).into()),
        )
        .unwrap_or_else(|e| panic!("set_option({key}) failed: {e}"));

        let got = db
            .get_option_string(OptionDatabase::Other((*key).into()))
            .unwrap_or_else(|e| panic!("get_option_string({key}) failed: {e}"));

        assert_eq!(got, *expected, "round-trip mismatch for {key}");
    }
}

/// tls_skip_verify only disables revocation checks (as in Go; the unit tests
/// assert it never sets sf_core's TLS_SKIP_VERIFY) and reads back as true/false.
#[test]
fn test_tls_skip_verify_option() {
    let Some(mut db) = make_db() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };

    let key = || OptionDatabase::Other("adbc.snowflake.sql.client_option.tls_skip_verify".into());
    assert_eq!(db.get_option_string(key()).unwrap(), "false");
    db.set_option(key(), OptionValue::String("enabled".into()))
        .expect("set tls_skip_verify enabled");
    assert_eq!(db.get_option_string(key()).unwrap(), "true");
    let mut conn = db.new_connection().expect("connect with tls_skip_verify");
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT 1").unwrap();
    assert_eq!(
        stmt.execute().unwrap().next().unwrap().unwrap().num_rows(),
        1
    );

    db.set_option(key(), OptionValue::String("disabled".into()))
        .expect("set tls_skip_verify disabled");
    assert_eq!(db.get_option_string(key()).unwrap(), "false");
}

/// Unsupported Go-only options cannot be silently accepted by sf_core; the
/// getter reports Go's unset value.
#[test]
fn test_ocsp_fail_open_mode_is_not_implemented() {
    let Some(mut db) = make_db() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let option =
        OptionDatabase::Other("adbc.snowflake.sql.client_option.ocsp_fail_open_mode".into());
    let error = db
        .set_option(option.clone(), OptionValue::String("enabled".into()))
        .unwrap_err();
    assert_eq!(error.status, adbc_core::error::Status::NotImplemented);
    assert_eq!(db.get_option_string(option).unwrap(), "0");
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
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }
}

#[test]
fn test_result_field_metadata_matches_go_paths() {
    use std::collections::HashMap;

    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let table = format!(
        "ADBC_RUST_RESULT_METADATA_{}",
        uuid::Uuid::new_v4().simple()
    );
    let mut statement = conn.new_statement().unwrap();
    statement
        .set_sql_query(format!(
            "CREATE TEMPORARY TABLE {table} \
             (N NUMBER(10,2), S VARCHAR(12), TS TIMESTAMP_NTZ(9), A ARRAY, V VARIANT)"
        ))
        .unwrap();
    // DDL status rows use JSON, not Arrow IPC.
    let mut created = statement.execute().unwrap();
    assert_eq!(
        created.schema().field(0).metadata(),
        &HashMap::from([("SNOWFLAKE_TYPE".into(), "text".into())]),
    );
    assert_eq!(created.next().unwrap().unwrap().num_rows(), 1);
    drop(created);

    statement
        .set_sql_query(format!(
            "INSERT INTO {table} SELECT 12.34, 'hello', \
             '2020-01-02 03:04:05.123456789'::TIMESTAMP_NTZ(9), \
             ARRAY_CONSTRUCT(1, 'two'), PARSE_JSON('{{\"k\":1}}')"
        ))
        .unwrap();
    statement.execute_update().unwrap();

    statement
        .set_sql_query(format!("SELECT * FROM {table}"))
        .unwrap();
    let described = statement.execute_schema().unwrap();
    let expected_row_types = ["fixed", "text", "timestamp_ntz", "array", "variant"];
    for (field, snowflake_type) in described.fields().iter().zip(expected_row_types) {
        let expected = if snowflake_type == "array" {
            HashMap::from([
                ("ARROW:extension:name".into(), "arrow.json".into()),
                ("ARROW:extension:metadata".into(), String::new()),
            ])
        } else {
            HashMap::from([("SNOWFLAKE_TYPE".into(), snowflake_type.into())])
        };
        assert_eq!(field.metadata(), &expected, "describe {}", field.name());
    }

    let mut reader = statement.execute().unwrap();
    let schema = reader.schema();
    for (field, logical_type) in
        schema
            .fields()
            .iter()
            .zip(["FIXED", "TEXT", "TIMESTAMP_NTZ", "ARRAY", "VARIANT"])
    {
        if logical_type == "ARRAY" {
            assert_eq!(
                field.metadata(),
                &HashMap::from([
                    ("ARROW:extension:name".into(), "arrow.json".into()),
                    ("ARROW:extension:metadata".into(), String::new()),
                ]),
            );
        } else {
            // Go preserves the server's IPC keys, rather than adding the JSON key.
            assert_eq!(field.metadata()["logicalType"], logical_type);
            assert!(!field.metadata().contains_key("SNOWFLAKE_TYPE"));
        }
    }
    assert_eq!(schema.field(0).metadata()["precision"], "10");
    assert_eq!(schema.field(0).metadata()["scale"], "2");
    assert_eq!(schema.field(2).metadata()["scale"], "9");
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.schema(), schema);
    assert_eq!(batch.num_rows(), 1);
    assert!(reader.next().is_none());
    drop(reader);

    // With no IPC payload, Go builds the schema from rowTypes even for SELECT.
    statement
        .set_sql_query(format!("SELECT * FROM {table} WHERE FALSE"))
        .unwrap();
    let empty = statement.execute().unwrap();
    for (field, expected) in empty.schema().fields().iter().zip(described.fields()) {
        assert_eq!(
            field.metadata(),
            expected.metadata(),
            "empty {}",
            field.name()
        );
    }
    assert_eq!(
        empty.map(|batch| batch.unwrap().num_rows()).sum::<usize>(),
        0
    );
}

#[test]
fn test_mixed_geometry_srids_are_stripped_without_column_crs() {
    use std::collections::HashMap;

    let Some(mut db) = make_db() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    db.set_option(
        OptionDatabase::Other("adbc.snowflake.sql.client_option.geometry_output_format".into()),
        OptionValue::String("ewkb".into()),
    )
    .unwrap();
    let mut conn = db.new_connection().unwrap();
    let mut statement = conn.new_statement().unwrap();
    statement
        .set_sql_query(
            "SELECT TO_GEOMETRY(column1) AS GEO FROM VALUES \
             ('SRID=3857;POINT(1 2)'), ('SRID=4326;POINT(1 2)')",
        )
        .unwrap();
    let reader = statement.execute().unwrap();
    assert_eq!(
        reader.schema().field(0).metadata(),
        &HashMap::from([("ARROW:extension:name".into(), "geoarrow.wkb".into())]),
    );
    let mut rows = 0;
    for batch in reader {
        let batch = batch.unwrap();
        let values = batch.column(0).as_binary::<i32>();
        for value in values.iter().flatten() {
            assert_eq!(
                value,
                &[
                    1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xf0, 0x3f, 0, 0, 0, 0, 0, 0, 0, 0x40
                ]
            );
            rows += 1;
        }
    }
    assert_eq!(rows, 2);
}

#[test]
fn test_prepare_parameter_schema_and_describe_only_execution() {
    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let table_name = format!("ADBC_RUST_PREPARE_TEST_{}", std::process::id());
    let mut ddl = conn.new_statement().unwrap();
    ddl.set_sql_query(format!(
        "CREATE OR REPLACE TEMP TABLE {table_name} (VALUE NUMBER(10,2))"
    ))
    .unwrap();
    ddl.execute_update().expect("create prepare test table");

    // A typed target column gives Snowflake a fully describable statement.
    // Snowflake still truthfully reports JSON bind transport as TEXT rather
    // than inventing the target column's logical NUMBER type.
    let mut parameterized = conn.new_statement().unwrap();
    parameterized
        .set_sql_query(format!("INSERT INTO {table_name} (VALUE) VALUES (?)"))
        .unwrap();
    parameterized
        .prepare()
        .expect("prepare parameterized query");
    let parameter_schema = parameterized
        .get_parameter_schema()
        .expect("parameter metadata");
    assert_eq!(parameter_schema.fields().len(), 1);
    assert_eq!(parameter_schema.field(0).data_type(), &DataType::Utf8);

    let mut insert = conn.new_statement().unwrap();
    insert
        .set_sql_query(format!("INSERT INTO {table_name} VALUES (1.25)"))
        .unwrap();
    let schema = insert.execute_schema().expect("describe insert");
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "number of rows inserted");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);

    let mut count = conn.new_statement().unwrap();
    count
        .set_sql_query(format!("SELECT COUNT(*) FROM {table_name}"))
        .unwrap();
    let batch = count
        .execute()
        .expect("count rows")
        .next()
        .expect("count batch")
        .expect("count result");
    let rows = arrow_cast::cast(batch.column(0), &DataType::Int64).unwrap();
    assert_eq!(
        rows.as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        0,
        "execute_schema must not execute the INSERT"
    );
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

#[test]
fn test_float_bindings_preserve_nonfinite_values() {
    use arrow_array::{Float64Array, Int64Array, RecordBatch};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    for data_type in [DataType::Float16, DataType::Float32, DataType::Float64] {
        let finite = if data_type == DataType::Float64 {
            1.2345678901234567
        } else {
            1.5
        };
        let expected = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, finite];
        let values = arrow_cast::cast(&Float64Array::from(expected.to_vec()), &data_type).unwrap();
        let value_schema = Arc::new(Schema::new(vec![Field::new("V", data_type.clone(), false)]));
        let bound_values = RecordBatch::try_new(value_schema, vec![values.clone()]).unwrap();
        let insert_schema = Arc::new(Schema::new(vec![
            Field::new("ID", DataType::Int64, false),
            Field::new("V", data_type, false),
        ]));
        let bound_insert = RecordBatch::try_new(
            insert_schema,
            vec![Arc::new(Int64Array::from(vec![0, 1, 2, 3])), values],
        )
        .unwrap();
        let assert_values = |batches: Vec<RecordBatch>| {
            let mut row = 0;
            for batch in batches {
                let values = batch
                    .column(0)
                    .as_primitive::<arrow_array::types::Float64Type>();
                for index in 0..values.len() {
                    assert!(row < expected.len(), "unexpected extra row");
                    assert!(!values.is_null(index), "bound float became NULL");
                    let actual = values.value(index);
                    let expected = expected[row];
                    if expected.is_nan() {
                        assert!(actual.is_nan(), "expected NaN, got {actual}");
                    } else if expected.is_infinite() {
                        assert!(actual.is_infinite(), "expected infinity, got {actual}");
                        assert_eq!(actual.is_sign_negative(), expected.is_sign_negative());
                    } else {
                        assert_eq!(actual.to_bits(), expected.to_bits());
                    }
                    row += 1;
                }
            }
            assert_eq!(row, expected.len());
        };

        let table = format!("ADBC_RUST_FLOAT_BIND_{}", uuid::Uuid::new_v4().simple());
        let mut ddl = conn.new_statement().unwrap();
        ddl.set_sql_query(format!(
            "CREATE TEMPORARY TABLE \"{table}\" (ID INTEGER, V FLOAT)"
        ))
        .unwrap();
        ddl.execute_update().expect("create float bind table");
        let mut insert = conn.new_statement().unwrap();
        insert
            .set_sql_query(format!("INSERT INTO \"{table}\" VALUES (?, ?)"))
            .unwrap();
        insert.bind(bound_insert).unwrap();
        assert_eq!(insert.execute_update().expect("array bind floats"), Some(4));

        let mut query = conn.new_statement().unwrap();
        query
            .set_sql_query(format!("SELECT V FROM \"{table}\" ORDER BY ID"))
            .unwrap();
        assert_values(
            query
                .execute()
                .expect("read array-bound floats")
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
        );
        query.set_sql_query("SELECT ?::FLOAT").unwrap();
        query.bind(bound_values).unwrap();
        assert_values(
            query
                .execute()
                .expect("query bound floats")
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
        );
        ddl.set_sql_query(format!("DROP TABLE \"{table}\""))
            .unwrap();
        ddl.execute_update().expect("drop float bind table");
    }
}

#[test]
fn test_ingest_stream_parquet_split_and_append_schema_validation() {
    use adbc_core::{error::Status, options::OptionStatement};
    use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let table = format!("ADBC_RUST_PARQUET_INGEST_{}", std::process::id());
    let schema = Arc::new(Schema::new(vec![Field::new("ID", DataType::Int64, false)]));
    let batches = [
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap(),
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![4, 5]))]).unwrap(),
    ];
    let mut ingest = conn.new_statement().unwrap();
    ingest
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String(table.clone()),
        )
        .unwrap();
    ingest
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("adbc.ingest.mode.replace".into()),
        )
        .unwrap();
    for (name, value) in [
        ("ingest_target_file_size", 1),
        ("ingest_writer_concurrency", 2),
        ("ingest_upload_concurrency", 2),
        ("ingest_copy_concurrency", 2),
    ] {
        ingest
            .set_option(
                OptionStatement::Other(format!("adbc.snowflake.statement.{name}")),
                OptionValue::Int(value),
            )
            .unwrap();
    }
    ingest
        .bind_stream(Box::new(RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema,
        )))
        .unwrap();
    assert_eq!(
        ingest.execute_update().expect("Parquet stream ingest"),
        Some(5)
    );

    let mut query = conn.new_statement().unwrap();
    query
        .set_sql_query(format!("SELECT COUNT(*) FROM \"{table}\""))
        .unwrap();
    let result = query.execute().unwrap().next().unwrap().unwrap();
    // Snowflake picks the narrowest integer type for the result set.
    let count = arrow_cast::cast(result.column(0), &DataType::Int64).unwrap();
    assert_eq!(
        count
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        5
    );

    let mut append = conn.new_statement().unwrap();
    append
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String(table.clone()),
        )
        .unwrap();
    append
        .set_option(
            OptionStatement::IngestMode,
            OptionValue::String("adbc.ingest.mode.append".into()),
        )
        .unwrap();
    let incompatible_schema = Arc::new(Schema::new(vec![Field::new(
        "OTHER",
        DataType::Int64,
        false,
    )]));
    append
        .bind(
            RecordBatch::try_new(
                incompatible_schema,
                vec![Arc::new(Int64Array::from(vec![6]))],
            )
            .unwrap(),
        )
        .unwrap();
    let error = append.execute_update().unwrap_err();
    assert_eq!(error.status, Status::AlreadyExists);

    let mut cleanup = conn.new_statement().unwrap();
    cleanup
        .set_sql_query(format!("DROP TABLE IF EXISTS \"{table}\""))
        .unwrap();
    cleanup.execute_update().expect("drop ingest test table");
}

/// Empty ingest streams succeed with zero rows in every mode (as in Go), and
/// the statement query tag reaches every ingest SQL statement and the PUT.
#[test]
fn test_ingest_empty_stream_and_query_tag() {
    use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let table = format!("ADBC_RUST_EMPTY_INGEST_{}", std::process::id());
    let tag = format!("adbc-rust-ingest-tag-{}", uuid::Uuid::new_v4());
    let schema = Arc::new(Schema::new(vec![Field::new("ID", DataType::Int64, false)]));
    let ingest = |conn: &mut adbc_driver_snowflake::Connection, mode: &str, rows: Vec<i64>| {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_option(
            OptionStatement::TargetTable,
            OptionValue::String(table.clone()),
        )
        .unwrap();
        stmt.set_option(
            OptionStatement::IngestMode,
            OptionValue::String(format!("adbc.ingest.mode.{mode}")),
        )
        .unwrap();
        stmt.set_option(
            OptionStatement::Other("adbc.snowflake.statement.query_tag".into()),
            OptionValue::String(tag.clone()),
        )
        .unwrap();
        let batches = if rows.is_empty() {
            Vec::new()
        } else {
            vec![
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(rows))])
                    .unwrap(),
            ]
        };
        stmt.bind_stream(Box::new(RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema.clone(),
        )))
        .unwrap();
        stmt.execute_update()
    };

    assert_eq!(
        ingest(&mut conn, "create", vec![]).expect("empty create"),
        Some(0)
    );
    assert_eq!(count_rows(&mut conn, &format!("\"{table}\"")), 0);
    assert_eq!(
        ingest(&mut conn, "append", vec![]).expect("empty append"),
        Some(0)
    );
    assert_eq!(
        ingest(&mut conn, "create_append", vec![]).expect("empty create_append"),
        Some(0)
    );
    assert_eq!(
        ingest(&mut conn, "append", vec![1, 2]).expect("append"),
        Some(2)
    );
    assert_eq!(count_rows(&mut conn, &format!("\"{table}\"")), 2);
    assert_eq!(
        ingest(&mut conn, "replace", vec![]).expect("empty replace"),
        Some(0)
    );
    assert_eq!(count_rows(&mut conn, &format!("\"{table}\"")), 0);

    let mut history = conn.new_statement().unwrap();
    history
        .set_sql_query(format!(
            "SELECT QUERY_TEXT FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 1000)) \
             WHERE QUERY_TAG = '{tag}'"
        ))
        .unwrap();
    let mut texts = Vec::new();
    for batch in history.execute().unwrap() {
        let batch = batch.unwrap();
        let column = batch.column(0).as_string::<i32>();
        texts.extend((0..column.len()).map(|i| column.value(i).to_uppercase()));
    }
    for expected in ["CREATE TABLE", "PUT ", "COPY INTO", "SWAP WITH"] {
        assert!(
            texts.iter().any(|text| text.contains(expected)),
            "no {expected:?} statement carried the query tag; tagged statements: {texts:#?}"
        );
    }

    let mut cleanup = conn.new_statement().unwrap();
    cleanup
        .set_sql_query(format!("DROP TABLE IF EXISTS \"{table}\""))
        .unwrap();
    cleanup.execute_update().expect("drop ingest test table");
}

#[test]
fn test_geo_formats_and_session_timezone_follow_session_state() {
    let Some(mut db) = make_db() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    for key in [
        "adbc.snowflake.sql.client_option.geography_output_format",
        "adbc.snowflake.sql.client_option.geometry_output_format",
    ] {
        db.set_option(
            OptionDatabase::Other(key.into()),
            OptionValue::String("ewkb".into()),
        )
        .unwrap();
    }
    let mut conn = db.new_connection().unwrap();
    let mut query = conn.new_statement().unwrap();
    query
        .set_sql_query(
            "SELECT TO_GEOGRAPHY('POINT(1 2)') AS GEO, TO_GEOMETRY('POINT(1 2)') AS GEOM",
        )
        .unwrap();
    let mut reader = query.execute().unwrap();
    for field in reader.schema().fields() {
        assert_eq!(field.data_type(), &DataType::Binary);
        assert_eq!(
            field
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some("geoarrow.wkb")
        );
    }
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .schema()
            .field(0)
            .metadata()
            .get("ARROW:extension:metadata")
            .map(String::as_str),
        Some("{\"crs\":\"EPSG:4326\",\"crs_type\":\"authority_code\"}")
    );

    let mut alter = conn.new_statement().unwrap();
    alter
        .set_sql_query("ALTER SESSION SET TIMEZONE = 'America/New_York'")
        .unwrap();
    alter.execute_update().unwrap();
    let mut timestamps = conn.new_statement().unwrap();
    timestamps
        .set_sql_query("SELECT CURRENT_TIMESTAMP()::TIMESTAMP_LTZ AS TS")
        .unwrap();
    let expected = &DataType::Timestamp(TimeUnit::Nanosecond, Some("America/New_York".into()));
    assert_eq!(
        timestamps.execute_schema().unwrap().field(0).data_type(),
        expected
    );
    assert_eq!(
        timestamps.execute().unwrap().schema().field(0).data_type(),
        expected
    );

    let table = format!("ADBC_RUST_LTZ_GEO_{}", std::process::id());
    let mut create = conn.new_statement().unwrap();
    create
        .set_sql_query(format!(
            "CREATE OR REPLACE TEMP TABLE \"{table}\" (TS TIMESTAMP_LTZ, GEO GEOGRAPHY)"
        ))
        .unwrap();
    create.execute_update().unwrap();
    let schema = conn.get_table_schema(None, None, &table).unwrap();
    assert_eq!(schema.field(0).data_type(), expected);
    assert_eq!(
        schema
            .field(1)
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some("geoarrow.wkb")
    );

    // A connection option updates the same session rather than only local metadata.
    conn.set_option(
        OptionConnection::Other("adbc.snowflake.sql.client_option.geography_output_format".into()),
        OptionValue::String("geojson".into()),
    )
    .unwrap();
    let mut geojson = conn.new_statement().unwrap();
    geojson
        .set_sql_query("SELECT TO_GEOGRAPHY('POINT(1 2)') AS GEO")
        .unwrap();
    assert_eq!(
        geojson.execute().unwrap().schema().field(0).data_type(),
        &DataType::Utf8
    );
    let schema = conn.get_table_schema(None, None, &table).unwrap();
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
}

#[test]
fn test_result_fetch_tuning_options() {
    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let prefetch = OptionStatement::Other("adbc.snowflake.rpc.prefetch_concurrency".into());
    let queue = OptionStatement::Other("adbc.rpc.result_queue_size".into());
    let mut stmt = conn.new_statement().expect("new_statement");
    assert!(
        stmt.get_option_int(prefetch.clone())
            .expect("session prefetch")
            > 0
    );
    assert_eq!(
        stmt.get_option_int(queue.clone()).unwrap_err().status,
        adbc_core::error::Status::NotFound
    );
    for key in [&prefetch, &queue] {
        let error = stmt
            .set_option(key.clone(), OptionValue::Int(0))
            .unwrap_err();
        assert_eq!(error.status, adbc_core::error::Status::InvalidArguments);
    }
    stmt.set_option(prefetch.clone(), OptionValue::String("1".into()))
        .expect("set prefetch");
    stmt.set_option(queue.clone(), OptionValue::Int(1))
        .expect("set queue");
    assert_eq!(stmt.get_option_int(prefetch).unwrap(), 1);
    assert_eq!(stmt.get_option_string(queue).unwrap(), "1");

    // Large enough for several result chunks, each with more than one batch.
    let rows = 300_000;
    stmt.set_sql_query(format!(
        "SELECT SEQ8() AS n, RANDSTR(64, RANDOM()) AS s FROM TABLE(GENERATOR(ROWCOUNT => {rows}))"
    ))
    .expect("set_sql_query");
    let total: usize = stmt
        .execute()
        .expect("execute")
        .map(|batch| batch.expect("batch").num_rows())
        .sum();
    assert_eq!(total, rows);
}

fn count_rows(conn: &mut adbc_driver_snowflake::Connection, table: &str) -> i64 {
    let mut stmt = conn.new_statement().expect("new_statement");
    stmt.set_sql_query(format!("SELECT COUNT(*) FROM {table}"))
        .expect("set_sql_query");
    let batch = stmt.execute().expect("execute").next().unwrap().unwrap();
    let column = arrow_cast::cast(batch.column(0), &DataType::Int64).unwrap();
    column
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

/// Manual transactions without Go's explicit BEGIN: Snowflake starts one
/// implicitly, and re-enabling autocommit commits it.
#[test]
fn test_manual_transactions() {
    let Some(mut observer) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let table = format!("TXN_TEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let mut setup = observer.new_statement().unwrap();
    setup
        .set_sql_query(format!("CREATE TABLE {table} (v INT)"))
        .unwrap();
    setup.execute_update().unwrap();

    let mut conn = make_connection().unwrap();
    let insert = |conn: &mut adbc_driver_snowflake::Connection, value: i32| {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query(format!("INSERT INTO {table} VALUES ({value})"))
            .unwrap();
        stmt.execute_update().unwrap();
    };
    conn.set_option(
        OptionConnection::AutoCommit,
        OptionValue::String("false".into()),
    )
    .unwrap();
    insert(&mut conn, 1);
    assert_eq!(count_rows(&mut observer, &table), 0, "uncommitted insert");
    conn.rollback().unwrap();
    insert(&mut conn, 2);
    conn.commit().unwrap();
    assert_eq!(count_rows(&mut observer, &table), 1, "rollback then commit");
    insert(&mut conn, 3);
    conn.set_option(
        OptionConnection::AutoCommit,
        OptionValue::String("true".into()),
    )
    .unwrap();
    assert_eq!(count_rows(&mut observer, &table), 2, "re-enabling commits");
    assert_eq!(
        conn.commit().unwrap_err().status,
        adbc_core::error::Status::InvalidState
    );
    insert(&mut conn, 4);
    assert_eq!(count_rows(&mut observer, &table), 3, "autocommit");

    setup.set_sql_query(format!("DROP TABLE {table}")).unwrap();
    setup.execute_update().unwrap();
}

const LAST_QUERY_ID: &str = "snowflake.query.last_query_id";
const QUERY_ID_METADATA: &str = "SNOWFLAKE:query_id";

fn last_query_id(stmt: &adbc_driver_snowflake::Statement) -> String {
    stmt.get_option_string(OptionStatement::Other(LAST_QUERY_ID.into()))
        .expect("last query id")
}

/// Mirrors the Go driver's TestQueryID contract.
#[test]
fn test_query_id() {
    use arrow_array::RecordBatchReader;
    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let mut stmt = conn.new_statement().unwrap();
    assert_eq!(last_query_id(&stmt), "");
    assert_eq!(
        stmt.set_option(
            OptionStatement::Other(LAST_QUERY_ID.into()),
            OptionValue::String("x".into())
        )
        .unwrap_err()
        .status,
        adbc_core::error::Status::InvalidArguments
    );
    let mut previous = String::new();
    for query in [
        "SELECT 1",
        "SELECT 1 WHERE FALSE",
        "SHOW PARAMETERS LIKE 'TIMEZONE' IN SESSION",
    ] {
        stmt.set_sql_query(query).unwrap();
        let reader = stmt.execute().unwrap();
        let id = last_query_id(&stmt);
        assert!(!id.is_empty() && id != previous, "{query}");
        let schema = reader.schema();
        assert_eq!(schema.metadata().get(QUERY_ID_METADATA), Some(&id));
        for batch in reader {
            assert_eq!(batch.unwrap().schema().metadata(), schema.metadata());
        }
        previous = id;
    }
    stmt.set_sql_query("SELECT 1").unwrap();
    let schema = stmt.execute_schema().unwrap();
    assert!(!schema.metadata().contains_key(QUERY_ID_METADATA));
    assert_eq!(last_query_id(&stmt), previous);

    stmt.set_sql_query("SELECT * FROM table_that_does_not_exist_for_query_id")
        .unwrap();
    assert!(stmt.execute().is_err());
    let failed = last_query_id(&stmt);
    assert!(
        !failed.is_empty() && failed != previous,
        "failed query's ID"
    );

    // Reusing the statement for bulk ingestion clears the ID, as in Go.
    use arrow_array::{Int64Array, RecordBatch};
    use std::sync::Arc;
    let table = format!("QUERY_ID_INGEST_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String(table.clone()),
    )
    .unwrap();
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "ID",
        DataType::Int64,
        false,
    )]));
    stmt.bind(RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap())
        .unwrap();
    assert_eq!(stmt.execute_update().unwrap(), Some(1));
    assert_eq!(last_query_id(&stmt), "", "ingest reports no query ID");
    let mut cleanup = conn.new_statement().unwrap();
    cleanup
        .set_sql_query(format!("DROP TABLE {table}"))
        .unwrap();
    cleanup.execute_update().unwrap();
}

/// Mirrors the Go driver's TestQueryIDBound contract.
#[test]
fn test_query_id_bound() {
    use arrow_array::{Int64Array, RecordBatch, RecordBatchReader};
    use std::sync::Arc;
    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT ?").unwrap();
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "v",
        DataType::Int64,
        false,
    )]));
    stmt.bind(RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap())
        .unwrap();
    let reader = stmt.execute().unwrap();
    let first = reader
        .schema()
        .metadata()
        .get(QUERY_ID_METADATA)
        .cloned()
        .expect("query id metadata");
    let mut count = 0;
    for batch in reader {
        assert_eq!(
            batch.unwrap().schema().metadata().get(QUERY_ID_METADATA),
            Some(&first)
        );
        count += 1;
    }
    assert_eq!(count, 2);
    let last = last_query_id(&stmt);
    assert!(!last.is_empty() && last != first);
}

fn ingest_temporary_rows(
    conn: &mut adbc_driver_snowflake::Connection,
    table: &str,
    mode: &str,
    rows: Vec<i64>,
) -> adbc_core::error::Result<Option<i64>> {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    let mut stmt = conn.new_statement()?;
    stmt.set_option(OptionStatement::TargetTable, table.into())?;
    stmt.set_option(
        OptionStatement::IngestMode,
        format!("adbc.ingest.mode.{mode}").into(),
    )?;
    stmt.set_option(OptionStatement::Temporary, "enabled".into())?;
    assert_eq!(stmt.get_option_string(OptionStatement::Temporary)?, "true");
    let schema = Arc::new(Schema::new(vec![Field::new("ID", DataType::Int64, false)]));
    stmt.bind(RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(rows))]).unwrap())?;
    stmt.execute_update()
}

#[test]
fn test_temporary_ingest_create_append_and_isolation() {
    let Some(db) = make_db() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let mut conn = db.new_connection().unwrap();
    let mut observer = db.new_connection().unwrap();
    let table = format!("ADBC_TEMP_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let quoted = format!("\"{table}\"");
    assert_eq!(
        ingest_temporary_rows(&mut conn, &table, "create", vec![1, 2]).unwrap(),
        Some(2)
    );
    assert_eq!(
        ingest_temporary_rows(&mut conn, &table, "append", vec![3]).unwrap(),
        Some(1)
    );
    assert_eq!(
        ingest_temporary_rows(&mut conn, &table, "create_append", vec![4]).unwrap(),
        Some(1)
    );
    assert_eq!(count_rows(&mut conn, &quoted), 4);
    let mut query = observer.new_statement().unwrap();
    query
        .set_sql_query(format!("SELECT * FROM {quoted}"))
        .unwrap();
    let error = query
        .execute()
        .err()
        .expect("temporary table must be private");
    assert_eq!(error.status, adbc_core::error::Status::NotFound, "{error}");

    let mut drop_table = conn.new_statement().unwrap();
    drop_table
        .set_sql_query(format!("DROP TABLE {quoted}"))
        .unwrap();
    drop_table.execute_update().unwrap();
    assert_eq!(
        ingest_temporary_rows(&mut conn, &table, "create_append", vec![5]).unwrap(),
        Some(1)
    );
    assert_eq!(count_rows(&mut conn, &quoted), 1);
}

#[test]
fn test_temporary_ingest_replace_and_shadowed_permanent_table() {
    use arrow_array::{RecordBatch, RecordBatchIterator};
    use arrow_schema::{ArrowError, Field, Schema};
    use std::sync::Arc;

    let Some(db) = make_db() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let mut conn = db.new_connection().unwrap();
    let mut observer = db.new_connection().unwrap();
    let table = format!("ADBC_TEMP_SWAP_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let quoted = format!("\"{table}\"");
    let mut ddl = observer.new_statement().unwrap();
    ddl.set_sql_query(format!(
        "CREATE TABLE {quoted} AS SELECT 'permanent' AS PERMANENT_VALUE"
    ))
    .unwrap();
    ddl.execute_update().unwrap();

    assert_eq!(
        ingest_temporary_rows(&mut conn, &table, "create", vec![1, 2]).unwrap(),
        Some(2)
    );
    // Preparing SELECT * must see the temporary ID column, not PERMANENT_VALUE.
    assert_eq!(
        ingest_temporary_rows(&mut conn, &table, "append", vec![3]).unwrap(),
        Some(1)
    );
    assert_eq!(
        ingest_temporary_rows(&mut conn, &table, "create_append", vec![4]).unwrap(),
        Some(1)
    );
    assert_eq!(count_rows(&mut conn, &quoted), 4);
    assert_eq!(
        ingest_temporary_rows(&mut conn, &table, "replace", vec![8, 9]).unwrap(),
        Some(2)
    );
    assert_eq!(count_rows(&mut conn, &quoted), 2);
    assert_eq!(count_rows(&mut observer, &quoted), 1);

    let schema = Arc::new(Schema::new(vec![Field::new("ID", DataType::Int64, false)]));
    let mut failing = conn.new_statement().unwrap();
    failing
        .set_option(OptionStatement::TargetTable, table.clone().into())
        .unwrap();
    failing
        .set_option(OptionStatement::Temporary, "true".into())
        .unwrap();
    failing
        .set_option(
            OptionStatement::IngestMode,
            "adbc.ingest.mode.replace".into(),
        )
        .unwrap();
    let failure: Result<RecordBatch, ArrowError> = Err(ArrowError::ParseError(
        "intentional replacement failure".into(),
    ));
    failing
        .bind_stream(Box::new(RecordBatchIterator::new([failure], schema)))
        .unwrap();
    let error = failing.execute_update().unwrap_err();
    assert!(error.message.contains("intentional replacement failure"));
    assert!(error.message.contains("target table was not replaced"));
    assert!(
        error
            .details
            .as_ref()
            .unwrap()
            .iter()
            .any(|(key, value)| { key == "adbc.snowflake.ingest.rows_loaded" && value == b"0" })
    );
    assert_eq!(count_rows(&mut conn, &quoted), 2);
    assert_eq!(count_rows(&mut observer, &quoted), 1);

    // The second session drops only the permanent table, never the temporary one.
    ddl.set_sql_query(format!("DROP TABLE {quoted}")).unwrap();
    ddl.execute_update().unwrap();
    assert_eq!(count_rows(&mut conn, &quoted), 2);
}

#[test]
fn test_temporary_ingest_rejects_explicit_catalog_or_schema() {
    use arrow_array::RecordBatch;
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    let Some(mut conn) = make_connection() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let catalog = conn
        .get_option_string(OptionConnection::CurrentCatalog)
        .unwrap();
    let schema_name = conn
        .get_option_string(OptionConnection::CurrentSchema)
        .unwrap();
    let table = format!("ADBC_TEMP_NAMESPACE_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let schema = Arc::new(Schema::new(vec![Field::new("ID", DataType::Int64, false)]));
    for (key, value) in [
        (OptionStatement::TargetCatalog, catalog),
        (OptionStatement::TargetDbSchema, schema_name),
    ] {
        for mode in ["create", "append", "create_append", "replace"] {
            for namespace_first in [false, true] {
                let mut stmt = conn.new_statement().unwrap();
                stmt.set_option(OptionStatement::TargetTable, table.clone().into())
                    .unwrap();
                stmt.set_option(
                    OptionStatement::IngestMode,
                    format!("adbc.ingest.mode.{mode}").into(),
                )
                .unwrap();
                if namespace_first {
                    stmt.set_option(key.clone(), value.clone().into()).unwrap();
                }
                stmt.set_option(OptionStatement::Temporary, "true".into())
                    .unwrap();
                if !namespace_first {
                    stmt.set_option(key.clone(), value.clone().into()).unwrap();
                }
                stmt.bind(RecordBatch::new_empty(Arc::clone(&schema)))
                    .unwrap();
                let error = stmt.execute_update().unwrap_err();
                assert_eq!(error.status, adbc_core::error::Status::InvalidArguments);
                assert!(error.message.contains("target_catalog"));
                assert!(error.message.contains("target_db_schema"));
            }
        }
    }
    let mut query = conn.new_statement().unwrap();
    query
        .set_sql_query(format!("SELECT * FROM \"{table}\""))
        .unwrap();
    let error = query
        .execute()
        .err()
        .expect("invalid options must not create a table");
    assert_eq!(error.status, adbc_core::error::Status::NotFound);
}

#[test]
fn test_failed_ingest_recounts_partial_rows_with_query_tag() {
    use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{ArrowError, Field, Schema};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    let Some(db) = make_db() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let mut conn = db.new_connection().unwrap();
    let mut observer = db.new_connection().unwrap();
    let table = format!("ADBC_PARTIAL_{}", uuid::Uuid::new_v4().simple()).to_uppercase();
    let quoted = format!("\"{table}\"");
    let tag = format!("adbc-partial-{}", uuid::Uuid::new_v4());
    let mut ddl = conn.new_statement().unwrap();
    ddl.set_sql_query(format!("CREATE TABLE {quoted} AS SELECT 0::INTEGER AS ID"))
        .unwrap();
    ddl.execute_update().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("ID", DataType::Int64, false)]));
    let first = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let observed_table = quoted.clone();
    let fail_after_copy = std::iter::once_with(move || {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if count_rows(&mut observer, &observed_table) == 4 {
                return Err(ArrowError::ParseError("intentional stream failure".into()));
            }
            if Instant::now() >= deadline {
                return Err(ArrowError::ParseError(
                    "COPY did not finish before deadline".into(),
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    });
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_option(OptionStatement::TargetTable, table.into())
        .unwrap();
    stmt.set_option(
        OptionStatement::IngestMode,
        "adbc.ingest.mode.append".into(),
    )
    .unwrap();
    stmt.set_option(
        OptionStatement::Other("adbc.snowflake.statement.query_tag".into()),
        tag.clone().into(),
    )
    .unwrap();
    for (name, value) in [
        ("ingest_target_file_size", 0),
        ("ingest_writer_concurrency", 1),
        ("ingest_upload_concurrency", 1),
        ("ingest_copy_concurrency", 1),
    ] {
        stmt.set_option(
            OptionStatement::Other(format!("adbc.snowflake.statement.{name}")),
            OptionValue::Int(value),
        )
        .unwrap();
    }
    stmt.bind_stream(Box::new(RecordBatchIterator::new(
        std::iter::once(Ok(first)).chain(fail_after_copy),
        schema,
    )))
    .unwrap();
    // Reading the failure cancels the pipeline token before the recount.
    let error = stmt.execute_update().unwrap_err();
    assert_eq!(error.status, adbc_core::error::Status::InvalidArguments);
    assert!(
        error.message.contains("intentional stream failure"),
        "{error}"
    );
    assert!(error.message.contains("3 rows were loaded"), "{error}");
    assert!(
        error
            .details
            .as_ref()
            .unwrap()
            .iter()
            .any(|(key, value)| { key == "adbc.snowflake.ingest.rows_loaded" && value == b"3" })
    );
    assert_eq!(count_rows(&mut conn, &quoted), 4);

    let mut history = conn.new_statement().unwrap();
    history
        .set_sql_query(format!(
            "SELECT QUERY_TEXT FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(RESULT_LIMIT => 1000)) \
             WHERE QUERY_TAG = '{tag}'"
        ))
        .unwrap();
    let mut counts = 0;
    let count_sql = format!("SELECT COUNT(*) FROM {quoted}");
    for batch in history.execute().unwrap() {
        let batch = batch.unwrap();
        let texts = batch.column(0).as_string::<i32>();
        counts += (0..texts.len())
            .filter(|&row| texts.value(row) == count_sql)
            .count();
    }
    assert_eq!(
        counts, 2,
        "initial count and failure recount must carry the tag"
    );
    ddl.set_sql_query(format!("DROP TABLE {quoted}")).unwrap();
    ddl.execute_update().unwrap();
}

#[test]
fn test_jwt_key_replacement_uses_last_parsed_key_and_does_not_reopen_file() {
    use aws_lc_rs::{encoding::AsDer, signature::RsaKeyPair};
    use base64::{
        Engine as _,
        engine::general_purpose::{STANDARD, URL_SAFE},
    };
    use pkcs8::der::Decode;
    use std::io::Write as _;

    let Some(uri) = get_env("SNOWFLAKE_URI") else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let decode = |value: &str| {
        let bytes = value.as_bytes();
        let mut output = Vec::with_capacity(bytes.len());
        let mut index = 0;
        while index < bytes.len() {
            match bytes[index] {
                b'%' => {
                    let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap();
                    output.push(u8::from_str_radix(hex, 16).unwrap());
                    index += 3;
                }
                b'+' => {
                    output.push(b' ');
                    index += 1;
                }
                byte => {
                    output.push(byte);
                    index += 1;
                }
            }
        }
        String::from_utf8(output).unwrap()
    };
    let key_source = uri.split_once('?').and_then(|(_, query)| {
        query
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .find_map(|(name, value)| {
                let name = decode(name);
                matches!(
                    name.as_str(),
                    "privateKey" | "private_key" | "private_key_file"
                )
                .then(|| (name, decode(value)))
            })
    });
    let Some((source, value)) = key_source else {
        eprintln!("Skipping: SNOWFLAKE_URI does not configure a private key");
        return;
    };
    let encoded = if source == "private_key_file" {
        std::fs::read(value).unwrap()
    } else if value.contains("-----BEGIN ") {
        value.into_bytes()
    } else if source == "privateKey" {
        URL_SAFE.decode(value).unwrap()
    } else {
        STANDARD.decode(value).unwrap()
    };
    let der = if encoded.starts_with(b"-----BEGIN ") {
        let body = std::str::from_utf8(&encoded)
            .unwrap()
            .lines()
            .filter(|line| !line.starts_with("-----"))
            .collect::<String>();
        STANDARD.decode(body).unwrap()
    } else {
        encoded
    };
    let key = RsaKeyPair::from_pkcs8(&der)
        .or_else(|_| RsaKeyPair::from_der(&der))
        .unwrap();
    let valid_der = key.as_der().unwrap();
    let pem = |label: &str, bytes: &[u8]| {
        format!(
            "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
            STANDARD.encode(bytes)
        )
    };
    let valid_pem = pem("PRIVATE KEY", valid_der.as_ref());
    let other = RsaKeyPair::generate(aws_lc_rs::rsa::KeySize::Rsa2048)
        .unwrap()
        .as_der()
        .unwrap();
    let other_pem = pem("PRIVATE KEY", other.as_ref());
    let encrypted = pkcs8::PrivateKeyInfo::from_der(valid_der.as_ref())
        .unwrap()
        .encrypt_with_params(
            pkcs8::pkcs5::pbes2::Parameters::pbkdf2_sha256_aes256cbc(
                100,
                b"integration test salt",
                &[9; 16],
            )
            .unwrap(),
            b"integration test password",
        )
        .unwrap();
    let encrypted_pem = pem("ENCRYPTED PRIVATE KEY", encrypted.as_bytes());
    struct RemoveKeyFile(std::path::PathBuf);
    impl Drop for RemoveKeyFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    for file_last in [false, true] {
        let mut db = make_db().unwrap();
        db.set_option(
            OptionDatabase::Other("adbc.snowflake.sql.auth_type".into()),
            OptionValue::String("auth_jwt".into()),
        )
        .unwrap();
        let path = RemoveKeyFile(
            std::env::temp_dir().join(format!("adbc-jwt-{}.pem", uuid::Uuid::new_v4())),
        );
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options
            .open(&path.0)
            .unwrap()
            .write_all(if file_last {
                valid_pem.as_bytes()
            } else {
                other_pem.as_bytes()
            })
            .unwrap();
        let file_option =
            OptionDatabase::Other("adbc.snowflake.sql.client_option.jwt_private_key".into());
        let value_option = OptionDatabase::Other(
            "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value".into(),
        );
        if file_last {
            db.set_option(value_option, OptionValue::String(other_pem.clone()))
                .unwrap();
            db.set_option(
                file_option,
                OptionValue::String(path.0.to_str().unwrap().into()),
            )
            .unwrap();
        } else {
            db.set_option(
                file_option,
                OptionValue::String(path.0.to_str().unwrap().into()),
            )
            .unwrap();
            db.set_option(
                OptionDatabase::Other(
                    "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_password".into(),
                ),
                OptionValue::String("integration test password".into()),
            )
            .unwrap();
            db.set_option(value_option, OptionValue::String(encrypted_pem.clone()))
                .unwrap();
        }
        std::fs::remove_file(&path.0).unwrap();
        let mut connection = db
            .new_connection()
            .expect("last parsed key must authenticate");
        let mut statement = connection.new_statement().unwrap();
        statement.set_sql_query("SELECT 1").unwrap();
        let rows = statement
            .execute()
            .unwrap()
            .map(|batch| batch.unwrap().num_rows())
            .sum::<usize>();
        assert_eq!(rows, 1);
    }
}

fn temporary_ingest_preserves_permanent_table(mode: &str) {
    use adbc_core::error::Status;

    let Some(db) = make_db() else {
        eprintln!("Skipping: SNOWFLAKE_URI not set");
        return;
    };
    let mut conn = db.new_connection().unwrap();
    let mut observer = db.new_connection().unwrap();
    // Exercise literal matching, SQL quoting, and case-sensitive identifiers.
    let table = format!("ADBC_TEMP_{}_mixed_%_'_\\", uuid::Uuid::new_v4().simple());
    let quoted = format!("\"{table}\"");
    let execute = |conn: &mut adbc_driver_snowflake::Connection, sql: String| {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query(sql).unwrap();
        stmt.execute_update().unwrap();
    };
    let ids = |conn: &mut adbc_driver_snowflake::Connection| {
        let mut stmt = conn.new_statement().unwrap();
        stmt.set_sql_query(format!("SELECT ID FROM {quoted} ORDER BY ID"))
            .unwrap();
        stmt.execute()
            .unwrap()
            .flat_map(|batch| {
                let batch = batch.unwrap();
                (0..batch.num_rows())
                    .map(|row| {
                        arrow_cast::display::array_value_to_string(batch.column(0), row).unwrap()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    };
    execute(
        &mut observer,
        format!("CREATE TABLE {quoted} AS SELECT 99 AS ID"),
    );
    let first = ingest_temporary_rows(&mut conn, &table, mode, vec![1, 2]);
    let first_permanent_rows = ids(&mut observer);
    if mode == "append" {
        // The permanent table has a compatible schema, so schema validation
        // alone would incorrectly permit COPY into it.
        execute(
            &mut conn,
            format!("CREATE TEMPORARY TABLE {quoted} AS SELECT 0 AS ID"),
        );
    }
    let first_temporary_rows = ids(&mut conn);
    let second = ingest_temporary_rows(&mut conn, &table, mode, vec![3]);
    let second_temporary_rows = ids(&mut conn);
    let second_permanent_rows = ids(&mut observer);
    // Clean up before asserting so a regression does not leak a permanent table.
    execute(&mut conn, format!("DROP TABLE {quoted}"));
    execute(&mut observer, format!("DROP TABLE {quoted}"));

    assert_eq!(first_permanent_rows, ["99"], "{mode}");
    assert_eq!(second_permanent_rows, ["99"], "{mode}");
    match mode {
        "append" => {
            assert_eq!(first.unwrap_err().status, Status::NotFound);
            assert_eq!(first_temporary_rows, ["0"]);
            assert_eq!(second.unwrap(), Some(1));
            assert_eq!(second_temporary_rows, ["0", "3"]);
        }
        "create" => {
            assert_eq!(first.unwrap(), Some(2));
            assert_eq!(first_temporary_rows, ["1", "2"]);
            assert_eq!(second.unwrap_err().status, Status::AlreadyExists);
            assert_eq!(second_temporary_rows, ["1", "2"]);
        }
        "create_append" => {
            assert_eq!(first.unwrap(), Some(2));
            assert_eq!(first_temporary_rows, ["1", "2"]);
            assert_eq!(second.unwrap(), Some(1));
            assert_eq!(second_temporary_rows, ["1", "2", "3"]);
        }
        "replace" => {
            assert_eq!(first.unwrap(), Some(2));
            assert_eq!(first_temporary_rows, ["1", "2"]);
            assert_eq!(second.unwrap(), Some(1));
            assert_eq!(second_temporary_rows, ["3"]);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_temporary_ingest_append_preserves_permanent_table() {
    temporary_ingest_preserves_permanent_table("append");
}

#[test]
fn test_temporary_ingest_create_preserves_permanent_table() {
    temporary_ingest_preserves_permanent_table("create");
}

#[test]
fn test_temporary_ingest_create_append_preserves_permanent_table() {
    temporary_ingest_preserves_permanent_table("create_append");
}

#[test]
fn test_temporary_ingest_replace_preserves_permanent_table() {
    temporary_ingest_preserves_permanent_table("replace");
}
