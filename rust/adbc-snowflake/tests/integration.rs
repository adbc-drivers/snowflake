// tests/integration.rs
use adbc_core::{
    Connection as _, Database as _, Driver as _, Optionable, Statement as _,
    options::{OptionDatabase, OptionValue},
};
use adbc_snowflake::Driver;

fn get_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

fn make_connection() -> Option<adbc_snowflake::Connection> {
    let account = get_env("SNOWFLAKE_TEST_ACCOUNT")?;
    let user = get_env("SNOWFLAKE_TEST_USER")?;
    let password = get_env("SNOWFLAKE_TEST_PASSWORD")?;

    let mut driver = Driver::default();
    let mut db = driver.new_database().expect("new_database");
    db.set_option(
        OptionDatabase::Other("adbc.snowflake.sql.account".into()),
        OptionValue::String(account),
    )
    .expect("set account");
    db.set_option(OptionDatabase::Username, OptionValue::String(user))
        .expect("set user");
    db.set_option(OptionDatabase::Password, OptionValue::String(password))
        .expect("set password");

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

#[test]
fn test_select_one() {
    let Some(mut conn) = make_connection() else {
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
    use arrow_array::cast::AsArray;
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
