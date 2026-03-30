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

// src/database.rs
use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::{
    Optionable,
    error::{Error, Result, Status},
    options::{OptionConnection, OptionDatabase, OptionValue},
};
use sf_core::apis::database_driver_v1::Handle;
use sf_core::config::param_registry::param_names;
use sf_core::config::settings::Setting;

use crate::connection::Connection;
use crate::driver::{Inner, TimestampPrecision};

use percent_encoding::percent_decode_str;

/// Convert an ADBC OptionDatabase key + OptionValue into an sf_core (param_name, Setting) pair.
/// Returns None for the "uri" key (handled by apply_uri separately).
/// Returns Err for keys with invalid values (e.g. non-numeric port).
fn adbc_db_opt_to_sf(key: &str, value: &OptionValue) -> Result<Option<(String, Setting)>> {
    let setting = match value {
        OptionValue::String(s) => Setting::String(s.clone()),
        OptionValue::Int(i) => Setting::Int(*i),
        OptionValue::Double(d) => Setting::Double(*d),
        OptionValue::Bytes(b) => Setting::Bytes(b.clone()),
        _ => {
            return Err(Error::with_message_and_status(
                "unsupported option value type",
                Status::InvalidArguments,
            ));
        }
    };

    let param: String = match key {
        "username" => param_names::USER.into(),
        "password" => param_names::PASSWORD.into(),
        "adbc.snowflake.sql.account" => param_names::ACCOUNT.into(),
        "adbc.snowflake.sql.db" => param_names::DATABASE.into(),
        "adbc.snowflake.sql.schema" => param_names::SCHEMA.into(),
        "adbc.snowflake.sql.warehouse" => param_names::WAREHOUSE.into(),
        "adbc.snowflake.sql.role" => param_names::ROLE.into(),
        "adbc.snowflake.sql.uri.host" => param_names::HOST.into(),
        "adbc.snowflake.sql.uri.protocol" => param_names::PROTOCOL.into(),
        "adbc.snowflake.sql.auth_type" => param_names::AUTHENTICATOR.into(),
        "adbc.snowflake.sql.client_option.application" => "client_app_id".to_string(),
        "adbc.snowflake.sql.client_option.auth_token" => param_names::TOKEN.into(),
        "adbc.snowflake.sql.client_option.jwt_private_key" => param_names::PRIVATE_KEY_FILE.into(),
        "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value" => {
            param_names::PRIVATE_KEY.into()
        }
        "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_password" => {
            param_names::PRIVATE_KEY_PASSWORD.into()
        }
        // Account geography
        "adbc.snowflake.sql.region" => "region".to_string(),
        // Auth extras
        // The Okta authenticator URL is the authenticator value in sf_core.
        "adbc.snowflake.sql.client_option.okta_url" => param_names::AUTHENTICATOR.into(),
        "adbc.snowflake.sql.client_option.identity_provider" => "identity_provider".to_string(),
        // Connection timeouts (stored as-is; sf_core will use them once supported)
        "adbc.snowflake.sql.client_option.login_timeout" => "login_timeout".to_string(),
        "adbc.snowflake.sql.client_option.request_timeout" => "request_timeout".to_string(),
        "adbc.snowflake.sql.client_option.jwt_expire_timeout" => "jwt_expire_timeout".to_string(),
        "adbc.snowflake.sql.client_option.client_timeout" => "client_timeout".to_string(),
        // TLS — tls_skip_verify compound effect is applied separately in set_option
        "adbc.snowflake.sql.client_option.tls_skip_verify" => "tls_skip_verify".to_string(),
        "adbc.snowflake.sql.client_option.tls_root_cert" => {
            param_names::CUSTOM_ROOT_STORE_PATH.into()
        }
        // OCSP — ocsp_fail_open_mode compound effect is applied separately in set_option
        "adbc.snowflake.sql.client_option.ocsp_fail_open_mode" => "ocsp_fail_open_mode".to_string(),
        // Session behaviour
        "adbc.snowflake.sql.client_option.keep_session_alive" => "keep_session_alive".to_string(),
        "adbc.snowflake.sql.client_option.disable_telemetry" => "disable_telemetry".to_string(),
        "adbc.snowflake.sql.client_option.cache_mfa_token" => "cache_mfa_token".to_string(),
        "adbc.snowflake.sql.client_option.store_temp_creds" => "store_temp_creds".to_string(),
        // Config / logging
        "adbc.snowflake.sql.client_option.config_file" => "config_file".to_string(),
        "adbc.snowflake.sql.client_option.tracing" => "log_level".to_string(),
        "adbc.snowflake.sql.uri.port" => {
            let port = match value {
                OptionValue::String(s) => s.parse::<i64>().map_err(|_| {
                    Error::with_message_and_status(
                        format!("invalid port value: {s}"),
                        Status::InvalidArguments,
                    )
                })?,
                OptionValue::Int(i) => *i,
                _ => {
                    return Err(Error::with_message_and_status(
                        "port must be a string or int",
                        Status::InvalidArguments,
                    ));
                }
            };
            return Ok(Some((param_names::PORT.into(), Setting::Int(port))));
        }
        "uri" => return Ok(None),
        other => other.to_string(),
    };

    Ok(Some((param, setting)))
}

pub struct Database {
    pub(crate) inner: Arc<Inner>,
    pub(crate) db_handle: Handle,
    /// Local copy of sf_core settings keyed by canonical param name.
    /// Propagated to each new connection before connection_init.
    pub(crate) sf_settings: HashMap<String, Setting>,
    /// Map NUMBER(p,s) with s>0 to Decimal128 instead of Float64.
    pub(crate) use_high_precision: bool,
    /// Arrow time unit used for TIMESTAMP columns.
    pub(crate) timestamp_precision: TimestampPrecision,
}

impl Drop for Database {
    fn drop(&mut self) {
        let _ = self.inner.sf.database_release(self.db_handle);
    }
}

impl Optionable for Database {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        let key_str = key.as_ref();
        if key_str == "uri" {
            if let OptionValue::String(uri) = &value {
                return self.apply_uri(uri.clone());
            }
            return Err(Error::with_message_and_status(
                "uri option must be a string",
                Status::InvalidArguments,
            ));
        }
        if key_str == "adbc.snowflake.sql.client_option.use_high_precision" {
            if let OptionValue::String(s) = &value {
                self.use_high_precision = s == "enabled" || s == "true";
            }
            return Ok(());
        }
        if key_str == "adbc.snowflake.sql.client_option.max_timestamp_precision" {
            if let OptionValue::String(s) = &value {
                self.timestamp_precision = match s.as_str() {
                    "microseconds" => TimestampPrecision::Microseconds,
                    "nanoseconds_error_on_overflow" => {
                        TimestampPrecision::NanosecondsErrorOnOverflow
                    }
                    _ => TimestampPrecision::Nanoseconds,
                };
            }
            return Ok(());
        }
        if let Some((param, setting)) = adbc_db_opt_to_sf(key_str, &value)? {
            self.sf_settings.insert(param.clone(), setting.clone());
            self.inner
                .runtime
                .block_on(
                    self.inner
                        .sf
                        .database_set_option(self.db_handle, param, setting),
                )
                .map_err(crate::error::api_error_to_adbc_error)?;
        }

        // tls_skip_verify: also drive the underlying verify_certificates / verify_hostname
        // params so sf_core skips certificate and hostname checks when enabled.
        if key_str == "adbc.snowflake.sql.client_option.tls_skip_verify" {
            let skip = matches!(&value, OptionValue::String(s) if s == "enabled");
            let verify = Setting::Bool(!skip);
            self.sf_settings.insert(
                param_names::VERIFY_CERTIFICATES.as_str().to_string(),
                verify.clone(),
            );
            self.sf_settings.insert(
                param_names::VERIFY_HOSTNAME.as_str().to_string(),
                verify.clone(),
            );
            self.inner
                .runtime
                .block_on(async {
                    self.inner
                        .sf
                        .database_set_option(
                            self.db_handle,
                            param_names::VERIFY_CERTIFICATES.into(),
                            verify.clone(),
                        )
                        .await?;
                    self.inner
                        .sf
                        .database_set_option(
                            self.db_handle,
                            param_names::VERIFY_HOSTNAME.into(),
                            verify,
                        )
                        .await
                })
                .map_err(crate::error::api_error_to_adbc_error)?;
        }

        // ocsp_fail_open_mode: map to sf_core's crl_check_mode
        // enabled (fail-open / advisory) → ADVISORY; disabled (strict) → ENABLED.
        if key_str == "adbc.snowflake.sql.client_option.ocsp_fail_open_mode" {
            let fail_open = matches!(&value, OptionValue::String(s) if s == "enabled");
            let mode = Setting::String(if fail_open { "ADVISORY" } else { "ENABLED" }.to_string());
            self.sf_settings.insert(
                param_names::CRL_CHECK_MODE.as_str().to_string(),
                mode.clone(),
            );
            self.inner
                .runtime
                .block_on(self.inner.sf.database_set_option(
                    self.db_handle,
                    param_names::CRL_CHECK_MODE.into(),
                    mode,
                ))
                .map_err(crate::error::api_error_to_adbc_error)?;
        }

        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let key_str = key.as_ref();
        if key_str == "adbc.snowflake.sql.client_option.use_high_precision" {
            return Ok(if self.use_high_precision {
                "enabled".to_string()
            } else {
                "disabled".to_string()
            });
        }
        if key_str == "adbc.snowflake.sql.client_option.max_timestamp_precision" {
            return Ok(match self.timestamp_precision {
                TimestampPrecision::Microseconds => "microseconds",
                TimestampPrecision::NanosecondsErrorOnOverflow => "nanoseconds_error_on_overflow",
                TimestampPrecision::Nanoseconds => "nanoseconds",
            }
            .to_string());
        }
        if let Ok(Some((param, _))) =
            adbc_db_opt_to_sf(key_str, &OptionValue::String(String::new()))
            && let Some(Setting::String(s)) = self.sf_settings.get(&param)
        {
            return Ok(s.clone());
        }
        Err(Error::with_message_and_status(
            format!("option not found: {key_str}"),
            Status::NotFound,
        ))
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        let key_str = key.as_ref();
        if let Ok(Some((param, _))) = adbc_db_opt_to_sf(key_str, &OptionValue::Bytes(vec![]))
            && let Some(Setting::Bytes(b)) = self.sf_settings.get(&param)
        {
            return Ok(b.clone());
        }
        Err(Error::with_message_and_status(
            format!("option not found: {key_str}"),
            Status::NotFound,
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        let key_str = key.as_ref();
        if let Ok(Some((param, _))) = adbc_db_opt_to_sf(key_str, &OptionValue::Int(0))
            && let Some(Setting::Int(i)) = self.sf_settings.get(&param)
        {
            return Ok(*i);
        }
        Err(Error::with_message_and_status(
            format!("option not found: {key_str}"),
            Status::NotFound,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        let key_str = key.as_ref();
        if let Ok(Some((param, _))) = adbc_db_opt_to_sf(key_str, &OptionValue::Double(0.0))
            && let Some(Setting::Double(d)) = self.sf_settings.get(&param)
        {
            return Ok(*d);
        }
        Err(Error::with_message_and_status(
            format!("option not found: {key_str}"),
            Status::NotFound,
        ))
    }
}

impl Database {
    /// Parse a Snowflake URI and apply each component as an individual option.
    /// Format: snowflake://[user[:password]@]account[/database[/schema]][?param=value&...]
    /// Recognized query params: warehouse, role, host, port, protocol, authenticator
    ///
    /// Limitations: passwords containing `@` are not supported; use `set_option` for
    /// Username/Password directly when credentials contain special characters.
    /// Query parameter values are not URL-decoded.
    fn apply_uri(&mut self, uri: String) -> Result<()> {
        let stripped = uri.strip_prefix("snowflake://").unwrap_or(&uri).to_string();

        let (user_info, rest) = if let Some(at) = stripped.find('@') {
            (
                Some(stripped[..at].to_string()),
                stripped[at + 1..].to_string(),
            )
        } else {
            (None, stripped)
        };

        if let Some(info) = user_info {
            if let Some(colon) = info.find(':') {
                let user = percent_decode_str(&info[..colon]).decode_utf8_lossy();
                let pass = percent_decode_str(&info[colon + 1..]).decode_utf8_lossy();
                if !user.is_empty() {
                    self.set_option(
                        OptionDatabase::Username,
                        OptionValue::String(user.into_owned()),
                    )?;
                }
                self.set_option(
                    OptionDatabase::Password,
                    OptionValue::String(pass.into_owned()),
                )?;
            } else if !info.is_empty() {
                let user = percent_decode_str(&info).decode_utf8_lossy();
                self.set_option(
                    OptionDatabase::Username, 
                    OptionValue::String(user.into_owned())
                )?;
            }
        }

        let (path, query) = if let Some(q) = rest.find('?') {
            (rest[..q].to_string(), Some(rest[q + 1..].to_string()))
        } else {
            (rest, None)
        };

        let parts: Vec<&str> = path.splitn(3, '/').collect();
        if let Some(account) = parts.first().filter(|s| !s.is_empty()) {
            self.set_option(
                OptionDatabase::Other("adbc.snowflake.sql.account".into()),
                OptionValue::String(account.to_string()),
            )?;
        }
        if let Some(database) = parts.get(1).filter(|s| !s.is_empty()) {
            self.set_option(
                OptionDatabase::Other("adbc.snowflake.sql.db".into()),
                OptionValue::String(database.to_string()),
            )?;
        }
        if let Some(schema) = parts.get(2).filter(|s| !s.is_empty()) {
            self.set_option(
                OptionDatabase::Other("adbc.snowflake.sql.schema".into()),
                OptionValue::String(schema.to_string()),
            )?;
        }

        if let Some(q) = query {
            for pair in q.split('&') {
                if let Some(eq) = pair.find('=') {
                    let k = &pair[..eq];
                    let v = &pair[eq + 1..];
                    let adbc_key = match k {
                        "warehouse" => "adbc.snowflake.sql.warehouse",
                        "role" => "adbc.snowflake.sql.role",
                        "host" => "adbc.snowflake.sql.uri.host",
                        "port" => "adbc.snowflake.sql.uri.port",
                        "protocol" => "adbc.snowflake.sql.uri.protocol",
                        "authenticator" => "adbc.snowflake.sql.auth_type",
                        "private_key_file" => "adbc.snowflake.sql.client_option.jwt_private_key",
                        "private_key" => {
                            "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value"
                        }
                        _ => continue,
                    };
                    self.set_option(
                        OptionDatabase::Other(adbc_key.into()),
                        OptionValue::String(v.to_string()),
                    )?;
                }
            }
        }
        Ok(())
    }
}

impl adbc_core::Database for Database {
    type ConnectionType = Connection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        self.new_connection_with_opts(std::iter::empty())
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let conn_handle = self.inner.sf.connection_new();

        // Propagate all database-level settings to the connection
        for (param, setting) in &self.sf_settings {
            self.inner
                .runtime
                .block_on(self.inner.sf.connection_set_option(
                    conn_handle,
                    param.clone(),
                    setting.clone(),
                ))
                .map_err(crate::error::api_error_to_adbc_error)?;
        }

        let mut post_autocommit: Option<bool> = None;
        let mut post_catalog: Option<String> = None;
        let mut post_schema: Option<String> = None;

        for (key, value) in opts {
            match &key {
                OptionConnection::AutoCommit => {
                    if let OptionValue::String(s) = &value {
                        post_autocommit = Some(s == "true" || s == "1");
                    }
                }
                OptionConnection::CurrentCatalog => {
                    if let OptionValue::String(s) = &value {
                        post_catalog = Some(s.clone());
                    }
                }
                OptionConnection::CurrentSchema => {
                    if let OptionValue::String(s) = &value {
                        post_schema = Some(s.clone());
                    }
                }
                OptionConnection::Other(k) => {
                    let sf_setting = match &value {
                        OptionValue::String(s) => Setting::String(s.clone()),
                        OptionValue::Int(i) => Setting::Int(*i),
                        OptionValue::Double(d) => Setting::Double(*d),
                        OptionValue::Bytes(b) => Setting::Bytes(b.clone()),
                        _ => {
                            return Err(Error::with_message_and_status(
                                "unsupported option value type",
                                Status::InvalidArguments,
                            ));
                        }
                    };
                    self.inner
                        .runtime
                        .block_on(self.inner.sf.connection_set_option(
                            conn_handle,
                            k.clone(),
                            sf_setting,
                        ))
                        .map_err(crate::error::api_error_to_adbc_error)?;
                }
                _ => {}
            }
        }

        // If neither host nor server_url was provided, derive host from account.
        if !self.sf_settings.contains_key(param_names::HOST.as_str())
            && !self
                .sf_settings
                .contains_key(param_names::SERVER_URL.as_str())
            && let Some(Setting::String(account)) =
                self.sf_settings.get(param_names::ACCOUNT.as_str())
        {
            let host = format!("{}.snowflakecomputing.com", account);
            self.inner
                .runtime
                .block_on(self.inner.sf.connection_set_option(
                    conn_handle,
                    param_names::HOST.into(),
                    Setting::String(host),
                ))
                .map_err(crate::error::api_error_to_adbc_error)?;
        }

        // Authenticate
        self.inner
            .runtime
            .block_on(self.inner.sf.connection_init(conn_handle, self.db_handle))
            .map_err(crate::error::api_error_to_adbc_error)?;

        let mut conn = Connection {
            inner: self.inner.clone(),
            conn_handle,
            autocommit: true,
            active_transaction: false,
            use_high_precision: self.use_high_precision,
            timestamp_precision: self.timestamp_precision,
        };

        if let Some(ac) = post_autocommit {
            conn.set_autocommit(ac)?;
        }
        if let Some(cat) = post_catalog {
            conn.execute_simple(&format!(r#"USE DATABASE "{}""#, cat.replace('"', "\"\"")))?;
        }
        if let Some(sch) = post_schema {
            conn.execute_simple(&format!(r#"USE SCHEMA "{}""#, sch.replace('"', "\"\"")))?;
        }

        Ok(conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::{
        Driver as _,
        options::{OptionDatabase, OptionValue},
    };
    use sf_core::config::param_registry::param_names;

    fn make_db() -> Database {
        let mut driver = crate::driver::Driver::default();
        driver.new_database().unwrap()
    }

    #[test]
    fn set_and_get_account_option() {
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Other("adbc.snowflake.sql.account".into()),
            OptionValue::String("myaccount".into()),
        )
        .unwrap();
        assert_eq!(
            db.get_option_string(OptionDatabase::Other("adbc.snowflake.sql.account".into()))
                .unwrap(),
            "myaccount"
        );
    }

    #[test]
    fn set_port_option_as_string_converts_to_int() {
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Other("adbc.snowflake.sql.uri.port".into()),
            OptionValue::String("443".into()),
        )
        .unwrap();
        let setting = db.sf_settings.get(param_names::PORT.as_str()).unwrap();
        assert_eq!(*setting, sf_core::config::settings::Setting::Int(443));
    }

    #[test]
    fn username_maps_to_user_param() {
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Username,
            OptionValue::String("alice".into()),
        )
        .unwrap();
        let setting = db.sf_settings.get(param_names::USER.as_str()).unwrap();
        assert_eq!(
            *setting,
            sf_core::config::settings::Setting::String("alice".into())
        );
    }

    #[test]
    fn tls_skip_verify_enabled_clears_verify_flags() {
        use sf_core::config::settings::Setting;
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Other("adbc.snowflake.sql.client_option.tls_skip_verify".into()),
            OptionValue::String("enabled".into()),
        )
        .unwrap();
        // Round-trip
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(
                "adbc.snowflake.sql.client_option.tls_skip_verify".into()
            ))
            .unwrap(),
            "enabled"
        );
        // Compound: verify_certificates and verify_hostname must be false
        assert_eq!(
            db.sf_settings
                .get(param_names::VERIFY_CERTIFICATES.as_str()),
            Some(&Setting::Bool(false))
        );
        assert_eq!(
            db.sf_settings.get(param_names::VERIFY_HOSTNAME.as_str()),
            Some(&Setting::Bool(false))
        );
    }

    #[test]
    fn tls_skip_verify_disabled_restores_verify_flags() {
        use sf_core::config::settings::Setting;
        let mut db = make_db();
        // First enable, then disable
        db.set_option(
            OptionDatabase::Other("adbc.snowflake.sql.client_option.tls_skip_verify".into()),
            OptionValue::String("enabled".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Other("adbc.snowflake.sql.client_option.tls_skip_verify".into()),
            OptionValue::String("disabled".into()),
        )
        .unwrap();
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(
                "adbc.snowflake.sql.client_option.tls_skip_verify".into()
            ))
            .unwrap(),
            "disabled"
        );
        assert_eq!(
            db.sf_settings
                .get(param_names::VERIFY_CERTIFICATES.as_str()),
            Some(&Setting::Bool(true))
        );
        assert_eq!(
            db.sf_settings.get(param_names::VERIFY_HOSTNAME.as_str()),
            Some(&Setting::Bool(true))
        );
    }

    #[test]
    fn ocsp_fail_open_mode_enabled_maps_to_crl_advisory() {
        use sf_core::config::settings::Setting;
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Other("adbc.snowflake.sql.client_option.ocsp_fail_open_mode".into()),
            OptionValue::String("enabled".into()),
        )
        .unwrap();
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(
                "adbc.snowflake.sql.client_option.ocsp_fail_open_mode".into()
            ))
            .unwrap(),
            "enabled"
        );
        assert_eq!(
            db.sf_settings.get(param_names::CRL_CHECK_MODE.as_str()),
            Some(&Setting::String("ADVISORY".into()))
        );
    }

    #[test]
    fn ocsp_fail_open_mode_disabled_maps_to_crl_enabled() {
        use sf_core::config::settings::Setting;
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Other("adbc.snowflake.sql.client_option.ocsp_fail_open_mode".into()),
            OptionValue::String("disabled".into()),
        )
        .unwrap();
        assert_eq!(
            db.sf_settings.get(param_names::CRL_CHECK_MODE.as_str()),
            Some(&Setting::String("ENABLED".into()))
        );
    }

    #[test]
    fn simple_option_round_trips() {
        let mut db = make_db();
        let cases = [
            ("adbc.snowflake.sql.region", "us-east-1"),
            ("adbc.snowflake.sql.client_option.login_timeout", "30s"),
            ("adbc.snowflake.sql.client_option.request_timeout", "60s"),
            (
                "adbc.snowflake.sql.client_option.keep_session_alive",
                "enabled",
            ),
            (
                "adbc.snowflake.sql.client_option.disable_telemetry",
                "enabled",
            ),
            ("adbc.snowflake.sql.client_option.tracing", "debug"),
            (
                "adbc.snowflake.sql.client_option.config_file",
                "/home/user/.snowflake/config.toml",
            ),
        ];
        for (key, val) in cases {
            db.set_option(
                OptionDatabase::Other(key.into()),
                OptionValue::String(val.into()),
            )
            .unwrap_or_else(|e| panic!("set_option({key}) failed: {e}"));
            let got = db
                .get_option_string(OptionDatabase::Other(key.into()))
                .unwrap_or_else(|e| panic!("get_option_string({key}) failed: {e}"));
            assert_eq!(got, val, "round-trip failed for {key}");
        }
    }

    #[test]
    fn uri_parses_account_user_database() {
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("snowflake://alice:secret@myaccount/mydb/myschema".into()),
        )
        .unwrap();
        assert_eq!(
            db.sf_settings.get(param_names::ACCOUNT.as_str()).unwrap(),
            &sf_core::config::settings::Setting::String("myaccount".into())
        );
        assert_eq!(
            db.sf_settings.get(param_names::USER.as_str()).unwrap(),
            &sf_core::config::settings::Setting::String("alice".into())
        );
        assert_eq!(
            db.sf_settings.get(param_names::DATABASE.as_str()).unwrap(),
            &sf_core::config::settings::Setting::String("mydb".into())
        );
    }
}
