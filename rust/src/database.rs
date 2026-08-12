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

// src/database.rs
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use adbc_core::{
    Optionable,
    error::{Error, Result, Status},
    options::{OptionConnection, OptionDatabase, OptionValue},
};
use sf_core::apis::database_driver_v1::{
    Handle, ValidationIssue, ValidationSeverity, connection::WrapperIdentity,
};
use sf_core::config::param_registry::{param_names, registry};
use sf_core::config::settings::Setting;

use crate::connection::{Connection, cleanup_connection_handle};
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
        "adbc.snowflake.sql.client_option.application" => "application".to_string(),
        "adbc.snowflake.sql.client_option.auth_token" => param_names::TOKEN.into(),
        "adbc.snowflake.sql.client_option.jwt_private_key" => param_names::PRIVATE_KEY_FILE.into(),
        "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value" => {
            param_names::PRIVATE_KEY.into()
        }
        "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_password" => {
            param_names::PRIVATE_KEY_PASSWORD.into()
        }
        // Auth extras
        // The Okta authenticator URL is the authenticator value in sf_core.
        "adbc.snowflake.sql.client_option.okta_url" => param_names::AUTHENTICATOR.into(),
        "adbc.snowflake.sql.client_option.identity_provider" => {
            param_names::WORKLOAD_IDENTITY_PROVIDER.into()
        }
        // Connection timeouts (normalized to sf_core integer seconds below)
        "adbc.snowflake.sql.client_option.login_timeout" => "login_timeout".to_string(),
        "adbc.snowflake.sql.client_option.request_timeout" => "request_timeout".to_string(),
        "adbc.snowflake.sql.client_option.jwt_expire_timeout" => "jwt_expire_timeout".to_string(),
        "adbc.snowflake.sql.client_option.client_timeout" => "client_timeout".to_string(),
        // TLS
        "adbc.snowflake.sql.client_option.tls_skip_verify" => param_names::TLS_SKIP_VERIFY.into(),
        "adbc.snowflake.sql.client_option.tls_root_cert" => {
            param_names::CUSTOM_ROOT_STORE_PATH.into()
        }
        // Session behaviour
        "adbc.snowflake.sql.client_option.keep_session_alive" => {
            param_names::CLIENT_SESSION_KEEP_ALIVE.into()
        }
        "adbc.snowflake.sql.client_option.disable_telemetry" => {
            param_names::CLIENT_TELEMETRY_ENABLED.into()
        }
        // sf_core's temporary-credential setting explicitly controls MFA-token
        // caching. The Go/ADBC store_temp_creds option instead controls ID-token
        // storage, so retain that raw name and let sf_core report it as unknown.
        "adbc.snowflake.sql.client_option.cache_mfa_token" => {
            param_names::CLIENT_STORE_TEMPORARY_CREDENTIAL.into()
        }
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

    let setting = match key {
        "adbc.snowflake.sql.client_option.login_timeout"
        | "adbc.snowflake.sql.client_option.request_timeout" => {
            let seconds = match value {
                OptionValue::Int(value) => *value,
                OptionValue::String(value) => {
                    let value = value.strip_suffix('s').unwrap_or(value);
                    if value.is_empty() {
                        0
                    } else {
                        value.parse::<i64>().map_err(|_| {
                            Error::with_message_and_status(
                                format!("invalid timeout value: {value}"),
                                Status::InvalidArguments,
                            )
                        })?
                    }
                }
                _ => {
                    return Err(Error::with_message_and_status(
                        "timeout must be a string or int",
                        Status::InvalidArguments,
                    ));
                }
            };
            Setting::Int(seconds)
        }
        "adbc.snowflake.sql.client_option.tls_skip_verify"
        | "adbc.snowflake.sql.client_option.keep_session_alive"
        | "adbc.snowflake.sql.client_option.cache_mfa_token"
        | "adbc.snowflake.sql.client_option.store_temp_creds" => {
            Setting::Bool(adbc_option_enabled(value)?)
        }
        "adbc.snowflake.sql.client_option.disable_telemetry" => {
            Setting::Bool(!adbc_option_enabled(value)?)
        }
        _ => setting,
    };

    Ok(Some((param, setting)))
}

fn adbc_option_enabled(value: &OptionValue) -> Result<bool> {
    match value {
        OptionValue::String(value) => match value.to_ascii_lowercase().as_str() {
            "enabled" | "true" | "1" => Ok(true),
            "disabled" | "false" | "0" => Ok(false),
            _ => Err(Error::with_message_and_status(
                "boolean option must be enabled, true, 1, disabled, false, or 0",
                Status::InvalidArguments,
            )),
        },
        OptionValue::Int(value) => Ok(*value != 0),
        _ => Err(Error::with_message_and_status(
            "boolean option must be a string or int",
            Status::InvalidArguments,
        )),
    }
}

pub struct Database {
    pub(crate) inner: Arc<Inner>,
    pub(crate) db_handle: Handle,
    /// Local copy of sf_core settings, using canonical parameter names when an
    /// equivalent exists and preserving original raw names otherwise.
    /// Propagated to each new connection before connection_init.
    pub(crate) sf_settings: HashMap<String, Setting>,
    /// Database-level warning messages already surfaced to the application logger.
    pub(crate) surfaced_warnings: HashSet<String>,
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

impl Database {
    fn set_sf_options(&mut self, options: HashMap<String, Setting>) -> Result<()> {
        let warnings = self
            .inner
            .runtime
            .block_on(self.inner.sf.database_set_options(self.db_handle, options))
            .map_err(crate::error::api_error_to_adbc_error)?;
        for warning in warnings {
            let rendered = warning.to_string();
            if self.surfaced_warnings.insert(rendered.clone()) {
                log::warn!("Snowflake database option warning: {rendered}");
            }
        }
        Ok(())
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
            self.set_sf_options(HashMap::from([(param.clone(), setting.clone())]))?;
            self.sf_settings.insert(param, setting);
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
        let lookup_value = if matches!(
            key_str,
            "adbc.snowflake.sql.client_option.tls_skip_verify"
                | "adbc.snowflake.sql.client_option.keep_session_alive"
                | "adbc.snowflake.sql.client_option.cache_mfa_token"
                | "adbc.snowflake.sql.client_option.store_temp_creds"
                | "adbc.snowflake.sql.client_option.disable_telemetry"
        ) {
            OptionValue::String("disabled".into())
        } else {
            OptionValue::String(String::new())
        };
        if let Ok(Some((param, _))) = adbc_db_opt_to_sf(key_str, &lookup_value)
            && let Some(setting) = self.sf_settings.get(&param)
        {
            return match setting {
                Setting::String(value) => Ok(value.clone()),
                Setting::Int(value)
                    if matches!(
                        key_str,
                        "adbc.snowflake.sql.client_option.login_timeout"
                            | "adbc.snowflake.sql.client_option.request_timeout"
                    ) =>
                {
                    Ok(format!("{value}s"))
                }
                Setting::Int(value) => Ok(value.to_string()),
                Setting::Double(value) => Ok(value.to_string()),
                Setting::Bool(value)
                    if key_str == "adbc.snowflake.sql.client_option.disable_telemetry" =>
                {
                    Ok(if *value { "disabled" } else { "enabled" }.into())
                }
                Setting::Bool(value) => Ok(if *value { "enabled" } else { "disabled" }.into()),
                Setting::Bytes(_) => Err(Error::with_message_and_status(
                    format!("option is not a string: {key_str}"),
                    Status::InvalidArguments,
                )),
            };
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
    /// Format: snowflake://[user[:password]@]host[/database[/schema]][?param=value&...]
    /// Recognized query params: account, warehouse, role, host, port, protocol, authenticator
    ///
    /// Limitations: passwords containing `@` are not supported; use `set_option` for
    /// Username/Password directly when credentials contain special characters.
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
                let user = percent_decode_str(&info[..colon])
                    .decode_utf8()
                    .map_err(|e| {
                        Error::with_message_and_status(
                            format!("invalid UTF-8 in URI username: {e}"),
                            Status::InvalidArguments,
                        )
                    })?;
                let pass = percent_decode_str(&info[colon + 1..])
                    .decode_utf8()
                    .map_err(|e| {
                        Error::with_message_and_status(
                            format!("invalid UTF-8 in URI password: {e}"),
                            Status::InvalidArguments,
                        )
                    })?;
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
                let user = percent_decode_str(&info).decode_utf8().map_err(|e| {
                    Error::with_message_and_status(
                        format!("invalid UTF-8 in URI username: {e}"),
                        Status::InvalidArguments,
                    )
                })?;
                self.set_option(
                    OptionDatabase::Username,
                    OptionValue::String(user.into_owned()),
                )?;
            }
        }

        let (path, query) = if let Some(q) = rest.find('?') {
            (&rest[..q], Some(&rest[q + 1..]))
        } else {
            (rest.as_str(), None)
        };
        let explicit_account = query.is_some_and(|query| {
            query.split('&').any(|pair| {
                pair.split_once('=')
                    .is_some_and(|(key, _)| key == "account")
            })
        });

        let parts: Vec<&str> = path.splitn(3, '/').collect();
        if let Some(authority) = parts.first().filter(|s| !s.is_empty()) {
            if explicit_account {
                let (host, port) = authority
                    .rsplit_once(':')
                    .map_or((*authority, None), |(host, port)| (host, Some(port)));
                self.set_option(
                    OptionDatabase::Other("adbc.snowflake.sql.uri.host".into()),
                    OptionValue::String(host.to_string()),
                )?;
                if let Some(port) = port {
                    self.set_option(
                        OptionDatabase::Other("adbc.snowflake.sql.uri.port".into()),
                        OptionValue::String(port.to_string()),
                    )?;
                }
            } else {
                self.set_option(
                    OptionDatabase::Other("adbc.snowflake.sql.account".into()),
                    OptionValue::String(authority.to_string()),
                )?;
            }
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
                if let Some((key, encoded_value)) = pair.split_once('=') {
                    let value = percent_decode_str(encoded_value)
                        .decode_utf8()
                        .map_err(|e| {
                            Error::with_message_and_status(
                                format!("invalid UTF-8 in URI query parameter: {e}"),
                                Status::InvalidArguments,
                            )
                        })?;
                    let adbc_key = match key {
                        "account" => "adbc.snowflake.sql.account",
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
                        OptionValue::String(value.into_owned()),
                    )?;
                }
            }
        }
        Ok(())
    }
}

fn connection_option_to_setting(value: &OptionValue) -> Result<Setting> {
    match value {
        OptionValue::String(value) => Ok(Setting::String(value.clone())),
        OptionValue::Int(value) => Ok(Setting::Int(*value)),
        OptionValue::Double(value) => Ok(Setting::Double(*value)),
        OptionValue::Bytes(value) => Ok(Setting::Bytes(value.clone())),
        _ => Err(Error::with_message_and_status(
            "unsupported option value type",
            Status::InvalidArguments,
        )),
    }
}

struct AccumulatedConnectionOptions {
    sf_options: HashMap<String, Setting>,
    post_connect_options: Vec<(OptionConnection, OptionValue)>,
    no_connection_details: bool,
}

fn accumulate_connection_options(
    database_options: &HashMap<String, Setting>,
    opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
) -> Result<AccumulatedConnectionOptions> {
    let mut sf_options = database_options.clone();
    // This mirrors sf_core peers' raw-kwargs contract: only a call with no
    // database settings and no raw connection options is a bare connection.
    let mut no_connection_details = database_options.is_empty();
    let mut post_connect_options = Vec::new();

    for (key, value) in opts {
        no_connection_details = false;
        if let OptionConnection::Other(name) = &key {
            // Canonicalize only known registry parameters so a connection-level
            // alias/case variant replaces the database-level canonical value.
            // Unknown wrapper/vendor options retain their original spelling.
            let name = registry()
                .resolve(name)
                .map(|param| param.canonical_name.to_owned())
                .unwrap_or_else(|| name.clone());
            sf_options.insert(name, connection_option_to_setting(&value)?);
        } else {
            post_connect_options.push((key, value));
        }
    }

    Ok(AccumulatedConnectionOptions {
        sf_options,
        post_connect_options: order_post_connect_options(post_connect_options),
        no_connection_details,
    })
}

fn wrapper_identity() -> WrapperIdentity {
    WrapperIdentity {
        driver_name: "ADBC Snowflake Driver (Rust)".into(),
        driver_version: env!("CARGO_PKG_VERSION").into(),
        language_runtime: "Rust".into(),
        language_version: String::new(),
        language_compiler: None,
        release_type: None,
    }
}

fn process_validation_issues(
    issues: impl IntoIterator<Item = ValidationIssue>,
    seen_warnings: &mut HashSet<String>,
    database_warnings: &HashSet<String>,
) -> Result<()> {
    let mut errors = Vec::new();
    for issue in issues {
        match issue.severity {
            ValidationSeverity::Error => errors.push(issue.to_string()),
            ValidationSeverity::Warning => {
                let rendered = issue.to_string();
                if seen_warnings.insert(rendered.clone()) && !database_warnings.contains(&rendered)
                {
                    log::warn!("Snowflake connection option warning: {rendered}");
                }
            }
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(Error::with_message_and_status(
            format!(
                "invalid Snowflake connection options: {}",
                errors.join("; ")
            ),
            Status::InvalidArguments,
        ))
    }
}

fn order_post_connect_options(
    mut options: Vec<(OptionConnection, OptionValue)>,
) -> Vec<(OptionConnection, OptionValue)> {
    // Stable sorting preserves caller order among duplicate options while making
    // the database/schema dependency deterministic. Autocommit and any future
    // standard options are still applied exactly once after both context names.
    options.sort_by_key(|(key, _)| match key {
        OptionConnection::CurrentCatalog => 0,
        OptionConnection::CurrentSchema => 1,
        _ => 2,
    });
    options
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
        let AccumulatedConnectionOptions {
            mut sf_options,
            post_connect_options,
            no_connection_details,
        } = accumulate_connection_options(&self.sf_settings, opts)?;
        // Snowflake currently gates Arrow result transport on this protocol ID.
        // Keep the visible application and wrapper telemetry driver-specific.
        sf_options.insert(
            "client_app_id".to_string(),
            Setting::String("PythonConnector".to_string()),
        );
        sf_options
            .entry("application".to_string())
            .or_insert_with(|| Setting::String("ADBC Snowflake Driver (Rust)".to_string()));
        let conn_handle = self.inner.sf.connection_new();

        let setup = (|| -> Result<()> {
            // Batch all pre-connect options in one validation/resolution pass.
            let warnings = self
                .inner
                .runtime
                .block_on(self.inner.sf.connection_set_options(
                    conn_handle,
                    sf_options,
                    no_connection_details,
                ))
                .map_err(crate::error::api_error_to_adbc_error)?;
            let mut seen_warnings = HashSet::new();
            process_validation_issues(warnings, &mut seen_warnings, &self.surfaced_warnings)?;

            self.inner
                .runtime
                .block_on(
                    self.inner
                        .sf
                        .set_wrapper_identity(conn_handle, wrapper_identity()),
                )
                .map_err(crate::error::api_error_to_adbc_error)?;

            // Seed Arrow format in the login payload; the backend requires this in
            // addition to the post-login ARROW_FORCE switch below.
            self.inner
                .runtime
                .block_on(self.inner.sf.connection_set_session_parameters(
                    conn_handle,
                    HashMap::from([(
                        "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT".to_string(),
                        "ARROW".to_string(),
                    )]),
                ))
                .map_err(crate::error::api_error_to_adbc_error)?;

            let validation_issues = self
                .inner
                .runtime
                .block_on(self.inner.sf.connection_validate_options(conn_handle))
                .map_err(crate::error::api_error_to_adbc_error)?;
            process_validation_issues(
                validation_issues,
                &mut seen_warnings,
                &self.surfaced_warnings,
            )?;

            self.inner
                .runtime
                .block_on(
                    self.inner
                        .sf
                        .connection_init(None, conn_handle, self.db_handle),
                )
                .map_err(crate::error::api_error_to_adbc_error)?;

            // Complete the backend's Arrow handshake after login. Without the
            // connector-specific forced value, query responses remain JSON.
            self.inner.execute_temporary_statement(
                conn_handle,
                "ALTER SESSION SET PYTHON_CONNECTOR_QUERY_RESULT_FORMAT = 'ARROW_FORCE'",
                None,
            )
        })();

        if let Err(error) = setup {
            // Initialization may have established a partial session. Attempt
            // close before release, while still guaranteeing release on error.
            cleanup_connection_handle(&self.inner, conn_handle);
            return Err(error);
        }

        let mut conn = Connection {
            inner: self.inner.clone(),
            conn_handle,
            autocommit: true,
            use_high_precision: self.use_high_precision,
            timestamp_precision: self.timestamp_precision,
        };
        // From this point Connection::drop owns cleanup on every error path.
        for (key, value) in post_connect_options {
            conn.set_option(key, value)?;
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
    fn connection_options_accumulate_once_with_connection_values_winning() {
        let database_options = HashMap::from([
            ("account".to_string(), Setting::String("db-account".into())),
            (
                "warehouse".to_string(),
                Setting::String("db-warehouse".into()),
            ),
        ]);
        let accumulated = accumulate_connection_options(
            &database_options,
            [
                (
                    OptionConnection::Other("account".into()),
                    OptionValue::String("connection-account".into()),
                ),
                (
                    OptionConnection::CurrentSchema,
                    OptionValue::String("schema".into()),
                ),
                (
                    OptionConnection::AutoCommit,
                    OptionValue::String("false".into()),
                ),
            ],
        )
        .unwrap();

        assert_eq!(
            accumulated.sf_options.get("account"),
            Some(&Setting::String("connection-account".into()))
        );
        assert_eq!(
            accumulated.sf_options.get("warehouse"),
            Some(&Setting::String("db-warehouse".into()))
        );
        assert!(!accumulated.no_connection_details);
        assert!(matches!(
            accumulated.post_connect_options.as_slice(),
            [
                (OptionConnection::CurrentSchema, _),
                (OptionConnection::AutoCommit, _)
            ]
        ));
    }

    #[test]
    fn only_no_database_settings_and_no_raw_options_is_bare() {
        let bare = accumulate_connection_options(&HashMap::new(), []).unwrap();
        assert!(bare.no_connection_details);

        let database_options = HashMap::from([(
            param_names::ACCOUNT.to_string(),
            Setting::String("account".into()),
        )]);
        let with_database_setting = accumulate_connection_options(&database_options, []).unwrap();
        assert!(!with_database_setting.no_connection_details);
    }

    #[test]
    fn any_raw_option_makes_the_connection_non_bare() {
        let accumulated = accumulate_connection_options(
            &HashMap::new(),
            [(
                OptionConnection::AutoCommit,
                OptionValue::String("false".into()),
            )],
        )
        .unwrap();
        assert!(!accumulated.no_connection_details);
        assert!(accumulated.sf_options.is_empty());
    }

    #[test]
    fn known_connection_alias_overrides_database_canonical_option() {
        let database_options = HashMap::from([(
            param_names::USER.to_string(),
            Setting::String("database-user".into()),
        )]);
        let accumulated = accumulate_connection_options(
            &database_options,
            [(
                OptionConnection::Other("uId".into()),
                OptionValue::String("connection-user".into()),
            )],
        )
        .unwrap();

        assert_eq!(
            accumulated.sf_options,
            HashMap::from([(
                param_names::USER.to_string(),
                Setting::String("connection-user".into()),
            )])
        );
    }

    #[test]
    fn unknown_connection_option_preserves_original_key() {
        let accumulated = accumulate_connection_options(
            &HashMap::new(),
            [(
                OptionConnection::Other("Vendor.CustomOption".into()),
                OptionValue::String("value".into()),
            )],
        )
        .unwrap();

        assert_eq!(
            accumulated.sf_options.get("Vendor.CustomOption"),
            Some(&Setting::String("value".into()))
        );
    }

    #[test]
    fn post_connect_options_always_apply_catalog_before_schema() {
        let options = vec![
            (
                OptionConnection::CurrentSchema,
                OptionValue::String("schema_one".into()),
            ),
            (
                OptionConnection::AutoCommit,
                OptionValue::String("false".into()),
            ),
            (
                OptionConnection::CurrentCatalog,
                OptionValue::String("database".into()),
            ),
            (
                OptionConnection::CurrentSchema,
                OptionValue::String("schema_two".into()),
            ),
        ];
        let ordered = order_post_connect_options(options);
        let keys = ordered
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                OptionConnection::CurrentCatalog,
                OptionConnection::CurrentSchema,
                OptionConnection::CurrentSchema,
                OptionConnection::AutoCommit,
            ]
        );
        assert!(matches!(
            &ordered[3].1,
            OptionValue::String(value) if value == "false"
        ));
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
    fn tls_skip_verify_passes_only_the_canonical_bool() {
        for (value, expected) in [
            (OptionValue::String("enabled".into()), true),
            (OptionValue::String("disabled".into()), false),
            (OptionValue::Int(-1), true),
        ] {
            let mut db = make_db();
            db.set_option(
                OptionDatabase::Other("adbc.snowflake.sql.client_option.tls_skip_verify".into()),
                value,
            )
            .unwrap();
            assert_eq!(
                db.sf_settings,
                HashMap::from([(
                    param_names::TLS_SKIP_VERIFY.to_string(),
                    Setting::Bool(expected),
                )])
            );
        }
    }

    #[test]
    fn boolean_option_parser_accepts_documented_strings_and_integers() {
        for (value, expected) in [
            (OptionValue::String("enabled".into()), true),
            (OptionValue::String("TRUE".into()), true),
            (OptionValue::String("1".into()), true),
            (OptionValue::String("DISABLED".into()), false),
            (OptionValue::String("false".into()), false),
            (OptionValue::String("0".into()), false),
            (OptionValue::Int(-1), true),
            (OptionValue::Int(0), false),
            (OptionValue::Int(2), true),
        ] {
            assert_eq!(adbc_option_enabled(&value).unwrap(), expected);
        }
    }

    #[test]
    fn boolean_options_reject_invalid_values_without_changing_state() {
        for (key, param, enabled_setting) in [
            (
                "adbc.snowflake.sql.client_option.tls_skip_verify",
                param_names::TLS_SKIP_VERIFY.as_str(),
                true,
            ),
            (
                "adbc.snowflake.sql.client_option.keep_session_alive",
                param_names::CLIENT_SESSION_KEEP_ALIVE.as_str(),
                true,
            ),
            (
                "adbc.snowflake.sql.client_option.cache_mfa_token",
                param_names::CLIENT_STORE_TEMPORARY_CREDENTIAL.as_str(),
                true,
            ),
            (
                "adbc.snowflake.sql.client_option.store_temp_creds",
                "store_temp_creds",
                true,
            ),
            (
                "adbc.snowflake.sql.client_option.disable_telemetry",
                param_names::CLIENT_TELEMETRY_ENABLED.as_str(),
                false,
            ),
        ] {
            let mut db = make_db();
            let option = OptionDatabase::Other(key.into());
            db.set_option(option.clone(), OptionValue::String("enabled".into()))
                .unwrap();

            for invalid in [
                OptionValue::String("ambiguous".into()),
                OptionValue::Double(1.0),
                OptionValue::Bytes(vec![1]),
            ] {
                let error = db.set_option(option.clone(), invalid).unwrap_err();
                assert_eq!(error.status, Status::InvalidArguments);
                assert_eq!(
                    db.sf_settings.get(param),
                    Some(&Setting::Bool(enabled_setting)),
                    "invalid value changed {key}"
                );
            }
        }
    }

    #[test]
    fn legacy_options_use_equivalent_core_parameters_or_original_raw_names() {
        let cases = [
            (
                "adbc.snowflake.sql.client_option.keep_session_alive",
                param_names::CLIENT_SESSION_KEEP_ALIVE.as_str(),
                Setting::Bool(true),
            ),
            (
                "adbc.snowflake.sql.client_option.cache_mfa_token",
                param_names::CLIENT_STORE_TEMPORARY_CREDENTIAL.as_str(),
                Setting::Bool(true),
            ),
            // sf_core has no ID-token-storage equivalent. Preserving the Go
            // option name keeps it distinct and produces an explicit unknown-
            // parameter validation warning rather than changing its meaning.
            (
                "adbc.snowflake.sql.client_option.store_temp_creds",
                "store_temp_creds",
                Setting::Bool(true),
            ),
            (
                "adbc.snowflake.sql.client_option.identity_provider",
                param_names::WORKLOAD_IDENTITY_PROVIDER.as_str(),
                Setting::String("AWS".into()),
            ),
            (
                "adbc.snowflake.sql.client_option.disable_telemetry",
                param_names::CLIENT_TELEMETRY_ENABLED.as_str(),
                Setting::Bool(false),
            ),
        ];
        for (key, expected_name, expected_setting) in cases {
            let value = if key.ends_with("identity_provider") {
                OptionValue::String("AWS".into())
            } else {
                OptionValue::String("enabled".into())
            };
            let (name, setting) = adbc_db_opt_to_sf(key, &value).unwrap().unwrap();
            assert_eq!(name, expected_name);
            assert_eq!(setting, expected_setting);
        }
    }

    #[test]
    fn cache_mfa_token_and_store_temp_creds_are_independent() {
        let cache_mfa_token =
            OptionDatabase::Other("adbc.snowflake.sql.client_option.cache_mfa_token".into());
        let store_temp_creds =
            OptionDatabase::Other("adbc.snowflake.sql.client_option.store_temp_creds".into());
        let mut db = make_db();

        db.set_option(
            cache_mfa_token.clone(),
            OptionValue::String("enabled".into()),
        )
        .unwrap();
        db.set_option(
            store_temp_creds.clone(),
            OptionValue::String("disabled".into()),
        )
        .unwrap();

        assert_eq!(
            db.sf_settings,
            HashMap::from([
                (
                    param_names::CLIENT_STORE_TEMPORARY_CREDENTIAL.to_string(),
                    Setting::Bool(true),
                ),
                ("store_temp_creds".to_string(), Setting::Bool(false)),
            ])
        );
        assert_eq!(
            db.get_option_string(cache_mfa_token.clone()).unwrap(),
            "enabled"
        );
        assert_eq!(
            db.get_option_string(store_temp_creds.clone()).unwrap(),
            "disabled"
        );

        db.set_option(cache_mfa_token.clone(), OptionValue::Int(0))
            .unwrap();
        db.set_option(store_temp_creds.clone(), OptionValue::Int(1))
            .unwrap();

        assert_eq!(
            db.sf_settings
                .get(param_names::CLIENT_STORE_TEMPORARY_CREDENTIAL.as_str()),
            Some(&Setting::Bool(false))
        );
        assert_eq!(
            db.sf_settings.get("store_temp_creds"),
            Some(&Setting::Bool(true))
        );
        assert_eq!(db.get_option_string(cache_mfa_token).unwrap(), "disabled");
        assert_eq!(db.get_option_string(store_temp_creds).unwrap(), "enabled");
    }

    #[test]
    fn region_and_ocsp_are_not_translated_to_non_equivalent_core_options() {
        for key in [
            "adbc.snowflake.sql.region",
            "adbc.snowflake.sql.client_option.ocsp_fail_open_mode",
        ] {
            let (name, _) = adbc_db_opt_to_sf(key, &OptionValue::String("enabled".into()))
                .unwrap()
                .unwrap();
            assert_eq!(name, key);
        }
    }

    #[test]
    fn wrapper_identity_is_stable_and_does_not_claim_a_rust_version() {
        let identity = wrapper_identity();
        assert_eq!(identity.driver_name, "ADBC Snowflake Driver (Rust)");
        assert_eq!(identity.driver_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(identity.language_runtime, "Rust");
        assert!(identity.language_version.is_empty());
        assert!(identity.language_compiler.is_none());
    }

    #[test]
    fn validation_errors_are_invalid_arguments_and_warnings_are_deduplicated() {
        use sf_core::apis::database_driver_v1::ValidationCode;

        let warning = ValidationIssue {
            severity: ValidationSeverity::Warning,
            parameter: "unknown".into(),
            message: "warning".into(),
            code: ValidationCode::UnknownParameter,
        };
        let mut seen = HashSet::new();
        process_validation_issues([warning.clone(), warning], &mut seen, &HashSet::new()).unwrap();
        assert_eq!(seen.len(), 1);

        let error = process_validation_issues(
            [ValidationIssue {
                severity: ValidationSeverity::Error,
                parameter: "account".into(),
                message: "missing".into(),
                code: ValidationCode::MissingRequired,
            }],
            &mut seen,
            &HashSet::new(),
        )
        .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(error.message.contains("account"));
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
    fn uri_full_hostname_parses_host_port_and_explicit_account() {
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String(
                "snowflake://sys_admin@private.network.com:443/OPS_MONITOR/DBA?account=vpc-id-1234"
                    .into(),
            ),
        )
        .unwrap();
        assert_eq!(
            db.sf_settings.get(param_names::HOST.as_str()).unwrap(),
            &Setting::String("private.network.com".into())
        );
        assert_eq!(
            db.sf_settings.get(param_names::PORT.as_str()).unwrap(),
            &Setting::Int(443)
        );
        assert_eq!(
            db.sf_settings.get(param_names::ACCOUNT.as_str()).unwrap(),
            &Setting::String("vpc-id-1234".into())
        );
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
