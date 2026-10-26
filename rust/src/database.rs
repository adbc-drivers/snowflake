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
use sf_core::config::param_registry::{Wrapper, param_names, registry};
use sf_core::config::settings::Setting;

use crate::connection::{Connection, cleanup_connection_handle};
use crate::driver::{Inner, TimestampPrecision};

use percent_encoding::percent_decode_str;

const AUTH_TYPE: &str = "adbc.snowflake.sql.auth_type";
const OKTA_URL: &str = "adbc.snowflake.sql.client_option.okta_url";
const MAX_RETRY_COUNT: &str = "adbc.snowflake.sql.client_option.max_retry_count";
const CACHE_MFA_TOKEN: &str = "adbc.snowflake.sql.client_option.cache_mfa_token";
const STORE_TEMP_CREDS: &str = "adbc.snowflake.sql.client_option.store_temp_creds";
const MAX_TIMESTAMP_PRECISION: &str = "adbc.snowflake.sql.client_option.max_timestamp_precision";
pub(crate) const GEOGRAPHY_OUTPUT_FORMAT: &str =
    "adbc.snowflake.sql.client_option.geography_output_format";
pub(crate) const GEOMETRY_OUTPUT_FORMAT: &str =
    "adbc.snowflake.sql.client_option.geometry_output_format";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum GeoOutputFormat {
    Ewkb,
    #[default]
    GeoJson,
}

impl GeoOutputFormat {
    pub(crate) fn parse(key: &str, value: &OptionValue) -> Result<Self> {
        let OptionValue::String(value) = value else {
            return Err(invalid_typed_option_value(key, value));
        };
        if value.eq_ignore_ascii_case("EWKB") {
            Ok(Self::Ewkb)
        } else if value.eq_ignore_ascii_case("GEOJSON") {
            Ok(Self::GeoJson)
        } else {
            Err(Error::with_message_and_status(
                format!(
                    "[snowflake] invalid value for database option '{key}': '{value}' (must be 'EWKB' or 'GeoJSON')"
                ),
                Status::InvalidArguments,
            ))
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Ewkb => "EWKB",
            Self::GeoJson => "GEOJSON",
        }
    }
}

// None means that sf_core needs the separately configured Okta URL as authenticator.
const AUTH_TYPES: [(&str, Option<&str>); 8] = [
    ("auth_snowflake", Some("SNOWFLAKE")),
    ("auth_oauth", Some("OAUTH")),
    ("auth_ext_browser", Some("EXTERNALBROWSER")),
    ("auth_okta", None),
    ("auth_jwt", Some("SNOWFLAKE_JWT")),
    ("auth_mfa", Some("USERNAME_PASSWORD_MFA")),
    ("auth_pat", Some("PROGRAMMATIC_ACCESS_TOKEN")),
    ("auth_wif", Some("WORKLOAD_IDENTITY")),
];

fn invalid_option_value(key: &str, value: &str) -> Error {
    Error::with_message_and_status(
        format!("Invalid value for database option '{key}': '{value}'"),
        Status::InvalidArguments,
    )
}

fn invalid_typed_option_value(key: &str, value: &OptionValue) -> Error {
    match value {
        OptionValue::String(value) => invalid_option_value(key, value),
        OptionValue::Int(value) => invalid_option_value(key, &value.to_string()),
        OptionValue::Double(value) => invalid_option_value(key, &value.to_string()),
        OptionValue::Bytes(value) => invalid_option_value(key, &format!("{value:?}")),
        _ => invalid_option_value(key, &format!("{value:?}")),
    }
}

fn unrecognized_option(key: &str) -> Error {
    Error::with_message_and_status(
        format!("unrecognized database option '{key}'"),
        Status::NotImplemented,
    )
}

fn option_to_setting(value: &OptionValue) -> Result<Setting> {
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

// Go's ParseDuration accepts signed composite/fractional durations; sf_core
// accepts integral seconds. Round positive subsecond amounts up rather than
// silently converting a timeout to zero (which sf_core treats as unlimited).
fn go_duration_seconds(value: &str) -> Option<i64> {
    let (negative, mut rest) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    if rest == "0" {
        return Some(0);
    }
    let mut nanos = 0_u128;
    let mut components = 0;
    while !rest.is_empty() {
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        let whole = if digits == 0 {
            Some(0)
        } else {
            rest[..digits].parse::<u128>().ok()
        };
        rest = &rest[digits..];
        let mut fraction = 0_u128;
        let mut divisor = 1_u128;
        let mut fraction_digits = 0;
        if let Some(after_dot) = rest.strip_prefix('.') {
            let count = after_dot.bytes().take_while(u8::is_ascii_digit).count();
            for &digit in after_dot.as_bytes().iter().take(count) {
                // More than 19 decimal places cannot change an integral
                // nanosecond for the supported units.
                if fraction_digits < 19 {
                    fraction = fraction * 10 + u128::from(digit - b'0');
                    divisor *= 10;
                    fraction_digits += 1;
                }
            }
            rest = &after_dot[count..];
            if digits == 0 && count == 0 {
                return None;
            }
        } else if digits == 0 {
            return None;
        }
        let (unit, after_unit) = [
            ("ns", 1_u128),
            ("us", 1_000),
            ("µs", 1_000),
            ("μs", 1_000),
            ("ms", 1_000_000),
            ("s", 1_000_000_000),
            ("m", 60_000_000_000),
            ("h", 3_600_000_000_000),
        ]
        .iter()
        .find_map(|&(unit, nanos)| rest.strip_prefix(unit).map(|remaining| (nanos, remaining)))?;
        let whole_nanos = whole?.checked_mul(unit)?;
        let fractional_nanos = fraction.checked_mul(unit)? / divisor;
        nanos = nanos
            .checked_add(whole_nanos)?
            .checked_add(fractional_nanos)?;
        rest = after_unit;
        components += 1;
    }
    if components == 0 || nanos > (i64::MAX as u128) + u128::from(negative) {
        return None;
    }
    i64::try_from(nanos.div_ceil(1_000_000_000)).ok()
}

fn duration_setting(key: &str, value: &OptionValue) -> Result<Setting> {
    let seconds = match value {
        OptionValue::String(duration) => {
            go_duration_seconds(duration).ok_or_else(|| invalid_option_value(key, duration))?
        }
        OptionValue::Int(seconds) => seconds
            .checked_abs()
            .ok_or_else(|| invalid_option_value(key, &seconds.to_string()))?,
        _ => return Err(invalid_typed_option_value(key, value)),
    };
    Ok(Setting::Int(seconds))
}

fn non_negative_int(key: &str, value: &OptionValue) -> Result<i64> {
    let number = match value {
        OptionValue::String(number) => number
            .parse::<i64>()
            .map_err(|_| invalid_option_value(key, number))?,
        OptionValue::Int(number) => *number,
        _ => return Err(invalid_typed_option_value(key, value)),
    };
    if number < 0 {
        return Err(invalid_option_value(key, &number.to_string()));
    }
    Ok(number)
}

fn adbc_option_enabled(value: &OptionValue) -> Result<bool> {
    match value {
        OptionValue::String(value)
            if value.eq_ignore_ascii_case("enabled")
                || value.eq_ignore_ascii_case("true")
                || value == "1" =>
        {
            Ok(true)
        }
        OptionValue::String(value)
            if value.eq_ignore_ascii_case("disabled")
                || value.eq_ignore_ascii_case("false")
                || value == "0" =>
        {
            Ok(false)
        }
        OptionValue::String(_) => Err(Error::with_message_and_status(
            "boolean option must be enabled, true, 1, disabled, false, or 0",
            Status::InvalidArguments,
        )),
        OptionValue::Int(value) => Ok(*value != 0),
        _ => Err(Error::with_message_and_status(
            "boolean option must be a string or int",
            Status::InvalidArguments,
        )),
    }
}

/// Resolve a Go/ADBC option or a registry-recognized raw sf_core name.
fn adbc_db_param(key: &str) -> Result<&'static str> {
    let param = match key {
        "username" => param_names::USER,
        "password" => param_names::PASSWORD,
        "adbc.snowflake.sql.account" => param_names::ACCOUNT,
        "adbc.snowflake.sql.db" => param_names::DATABASE,
        "adbc.snowflake.sql.schema" => param_names::SCHEMA,
        "adbc.snowflake.sql.warehouse" => param_names::WAREHOUSE,
        "adbc.snowflake.sql.role" => param_names::ROLE,
        "adbc.snowflake.sql.uri.host" => param_names::HOST,
        "adbc.snowflake.sql.uri.protocol" => param_names::PROTOCOL,
        "adbc.snowflake.sql.uri.port" => param_names::PORT,
        "adbc.snowflake.sql.client_option.app_name"
        | "adbc.snowflake.sql.client_option.application" => param_names::APPLICATION,
        "adbc.snowflake.sql.client_option.auth_token" => param_names::TOKEN,
        "adbc.snowflake.sql.client_option.jwt_private_key" => param_names::PRIVATE_KEY_FILE,
        "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value" => param_names::PRIVATE_KEY,
        "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_password" => {
            param_names::PRIVATE_KEY_PASSWORD
        }
        "adbc.snowflake.sql.client_option.identity_provider" => {
            param_names::WORKLOAD_IDENTITY_PROVIDER
        }
        "adbc.snowflake.sql.client_option.login_timeout" => param_names::LOGIN_TIMEOUT,
        "adbc.snowflake.sql.client_option.request_timeout" => param_names::REQUEST_TIMEOUT,
        "adbc.snowflake.sql.client_option.client_timeout" => param_names::RETRY_TIMEOUT,
        MAX_RETRY_COUNT => param_names::RETRY_MAX_ATTEMPTS,
        "adbc.snowflake.sql.client_option.tls_skip_verify" => param_names::TLS_SKIP_VERIFY,
        "adbc.snowflake.sql.client_option.tls_root_cert" => param_names::CUSTOM_ROOT_STORE_PATH,
        "adbc.snowflake.sql.client_option.keep_session_alive" => {
            param_names::SERVER_SESSION_KEEP_ALIVE
        }
        "adbc.snowflake.sql.client_option.disable_telemetry" => {
            param_names::CLIENT_TELEMETRY_ENABLED
        }
        CACHE_MFA_TOKEN | STORE_TEMP_CREDS => param_names::CLIENT_STORE_TEMPORARY_CREDENTIAL,
        "adbc.snowflake.sql.client_option.validate_default_parameters" => {
            param_names::VALIDATE_DEFAULT_PARAMETERS
        }
        "adbc.snowflake.sql.client_option.proxy_host" => param_names::PROXY_HOST,
        "adbc.snowflake.sql.client_option.proxy_port" => param_names::PROXY_PORT,
        "adbc.snowflake.sql.client_option.proxy_user" => param_names::PROXY_USER,
        "adbc.snowflake.sql.client_option.proxy_password" => param_names::PROXY_PASSWORD,
        "adbc.snowflake.sql.client_option.proxy_protocol" => param_names::PROXY_SCHEME,
        other => {
            return registry()
                .resolve_for(Wrapper::Python, other)
                .map(|definition| definition.canonical_name)
                .ok_or_else(|| unrecognized_option(other));
        }
    };
    Ok(param.as_str())
}

/// Convert a validated ADBC database option to an sf_core setting.
fn adbc_db_opt_to_sf(key: &str, value: &OptionValue) -> Result<Option<(&'static str, Setting)>> {
    if key == "uri" {
        return Ok(None);
    }
    let param = adbc_db_param(key)?;
    let setting = match key {
        "adbc.snowflake.sql.uri.port" | "adbc.snowflake.sql.client_option.proxy_port" => {
            let port = non_negative_int(key, value)?;
            if key.ends_with("proxy_port") && !(1..=65535).contains(&port) {
                return Err(invalid_option_value(key, &port.to_string()));
            }
            Setting::Int(port)
        }
        "adbc.snowflake.sql.client_option.login_timeout"
        | "adbc.snowflake.sql.client_option.request_timeout"
        | "adbc.snowflake.sql.client_option.client_timeout" => duration_setting(key, value)?,
        "adbc.snowflake.sql.client_option.tls_skip_verify"
        | "adbc.snowflake.sql.client_option.keep_session_alive"
        | CACHE_MFA_TOKEN
        | STORE_TEMP_CREDS
        | "adbc.snowflake.sql.client_option.validate_default_parameters" => {
            Setting::Bool(adbc_option_enabled(value)?)
        }
        "adbc.snowflake.sql.client_option.disable_telemetry" => {
            Setting::Bool(!adbc_option_enabled(value)?)
        }
        "adbc.snowflake.sql.client_option.proxy_protocol" => match value {
            OptionValue::String(protocol)
                if protocol.eq_ignore_ascii_case("http")
                    || protocol.eq_ignore_ascii_case("https") =>
            {
                Setting::String(protocol.to_ascii_lowercase())
            }
            OptionValue::String(protocol) => return Err(invalid_option_value(key, protocol)),
            _ => return Err(invalid_typed_option_value(key, value)),
        },
        _ => option_to_setting(value)?,
    };
    Ok(Some((param, setting)))
}

pub struct Database {
    pub(crate) inner: Arc<Inner>,
    pub(crate) db_handle: Handle,
    /// Canonical sf_core settings already stored on the database handle. Used
    /// only for local getters and the bare-connection signal, never re-seeded.
    pub(crate) sf_settings: HashMap<String, Setting>,
    pub(crate) auth_type: Option<&'static str>,
    pub(crate) okta_url: Option<String>,
    pub(crate) max_retry_count: Option<i64>,
    pub(crate) duration_options: HashMap<&'static str, String>,
    pub(crate) credential_options: HashMap<&'static str, bool>,
    /// Database-level warning messages already surfaced to the application logger.
    pub(crate) surfaced_warnings: HashSet<String>,
    /// Arrow time unit used for TIMESTAMP columns.
    pub(crate) timestamp_precision: TimestampPrecision,
    pub(crate) geography_output_format: GeoOutputFormat,
    pub(crate) geometry_output_format: GeoOutputFormat,
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

    fn set_sf_option(&mut self, param: &'static str, setting: Setting) -> Result<()> {
        self.set_sf_options(HashMap::from([(param.into(), setting.clone())]))?;
        self.sf_settings.insert(param.into(), setting);
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
        if key_str == MAX_TIMESTAMP_PRECISION {
            let OptionValue::String(precision) = &value else {
                return Err(invalid_typed_option_value(key_str, &value));
            };
            self.timestamp_precision = match precision.as_str() {
                "microseconds" => TimestampPrecision::Microseconds,
                "nanoseconds_error_on_overflow" => TimestampPrecision::NanosecondsErrorOnOverflow,
                "nanoseconds" => TimestampPrecision::Nanoseconds,
                _ => return Err(invalid_option_value(key_str, precision)),
            };
            return Ok(());
        }
        if key_str == GEOGRAPHY_OUTPUT_FORMAT {
            self.geography_output_format = GeoOutputFormat::parse(key_str, &value)?;
            return Ok(());
        }
        if key_str == GEOMETRY_OUTPUT_FORMAT {
            self.geometry_output_format = GeoOutputFormat::parse(key_str, &value)?;
            return Ok(());
        }
        if key_str == AUTH_TYPE {
            let OptionValue::String(auth) = &value else {
                return Err(invalid_typed_option_value(key_str, &value));
            };
            let &(go_value, core_value) = AUTH_TYPES
                .iter()
                .find(|&&(go_value, _)| go_value == auth.as_str())
                .ok_or_else(|| invalid_option_value(key_str, auth))?;
            if let Some(authenticator) = core_value.or(self.okta_url.as_deref()) {
                self.set_sf_option(
                    param_names::AUTHENTICATOR.as_str(),
                    Setting::String(authenticator.to_string()),
                )?;
            }
            self.auth_type = Some(go_value);
            return Ok(());
        }
        if key_str == OKTA_URL {
            let OptionValue::String(url) = &value else {
                return Err(invalid_typed_option_value(key_str, &value));
            };
            if self.auth_type == Some("auth_okta") {
                self.set_sf_option(
                    param_names::AUTHENTICATOR.as_str(),
                    Setting::String(url.clone()),
                )?;
            }
            self.okta_url = Some(url.clone());
            return Ok(());
        }
        if key_str == MAX_RETRY_COUNT {
            let retries = non_negative_int(key_str, &value)?;
            let attempts = if retries == 0 {
                8
            } else {
                retries
                    .checked_add(1)
                    .ok_or_else(|| invalid_option_value(key_str, &retries.to_string()))?
            };
            if attempts > u32::MAX as i64 {
                return Err(invalid_option_value(key_str, &retries.to_string()));
            }
            self.set_sf_option(
                param_names::RETRY_MAX_ATTEMPTS.as_str(),
                Setting::Int(attempts),
            )?;
            self.max_retry_count = Some(retries);
            return Ok(());
        }
        if let Some((param, setting)) = adbc_db_opt_to_sf(key_str, &value)? {
            let credential_enabled = match &setting {
                Setting::Bool(enabled) => Some(*enabled),
                _ => None,
            };
            self.set_sf_option(param, setting)?;
            if key_str.eq_ignore_ascii_case(param_names::AUTHENTICATOR.as_str()) {
                self.auth_type = None;
            }
            if param == param_names::RETRY_MAX_ATTEMPTS.as_str() {
                self.max_retry_count = None;
            }
            if param == param_names::CLIENT_STORE_TEMPORARY_CREDENTIAL.as_str() {
                if key_str == CACHE_MFA_TOKEN || key_str == STORE_TEMP_CREDS {
                    if let Some(enabled) = credential_enabled {
                        self.credential_options.insert(
                            if key_str == CACHE_MFA_TOKEN {
                                CACHE_MFA_TOKEN
                            } else {
                                STORE_TEMP_CREDS
                            },
                            enabled,
                        );
                    }
                } else {
                    self.credential_options.clear();
                }
            }
            self.duration_options.remove(param);
            if let OptionValue::String(duration) = &value
                && matches!(
                    key_str,
                    "adbc.snowflake.sql.client_option.login_timeout"
                        | "adbc.snowflake.sql.client_option.request_timeout"
                        | "adbc.snowflake.sql.client_option.client_timeout"
                )
            {
                self.duration_options.insert(
                    match key_str {
                        "adbc.snowflake.sql.client_option.login_timeout" => {
                            param_names::LOGIN_TIMEOUT.as_str()
                        }
                        "adbc.snowflake.sql.client_option.request_timeout" => {
                            param_names::REQUEST_TIMEOUT.as_str()
                        }
                        _ => param_names::RETRY_TIMEOUT.as_str(),
                    },
                    duration.clone(),
                );
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let key_str = key.as_ref();
        if key_str == MAX_TIMESTAMP_PRECISION {
            return Ok(match self.timestamp_precision {
                TimestampPrecision::Microseconds => "microseconds",
                TimestampPrecision::NanosecondsErrorOnOverflow => "nanoseconds_error_on_overflow",
                TimestampPrecision::Nanoseconds => "nanoseconds",
            }
            .to_string());
        }
        if key_str == GEOGRAPHY_OUTPUT_FORMAT {
            return Ok(self.geography_output_format.as_str().to_string());
        }
        if key_str == GEOMETRY_OUTPUT_FORMAT {
            return Ok(self.geometry_output_format.as_str().to_string());
        }
        if key_str == AUTH_TYPE {
            if let Some(auth) = self.auth_type {
                return Ok(auth.to_string());
            }
            if let Some(Setting::String(auth)) =
                self.sf_settings.get(param_names::AUTHENTICATOR.as_str())
            {
                return Ok(AUTH_TYPES
                    .iter()
                    .find(|&&(_, core)| core.is_some_and(|core| core.eq_ignore_ascii_case(auth)))
                    .map(|&(go, _)| go)
                    .or_else(|| {
                        auth.eq_ignore_ascii_case("SNOWFLAKE_PASSWORD")
                            .then_some("auth_snowflake")
                    })
                    .or_else(|| {
                        auth.get(..8)
                            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
                            .then_some("auth_okta")
                    })
                    .unwrap_or(auth)
                    .to_string());
            }
        }
        if key_str == OKTA_URL {
            return self.okta_url.clone().ok_or_else(|| {
                Error::with_message_and_status(
                    format!("option not found: {key_str}"),
                    Status::NotFound,
                )
            });
        }
        if key_str == MAX_RETRY_COUNT
            && let Some(count) = self.max_retry_count
        {
            return Ok(count.to_string());
        }
        if let Some(&enabled) = self.credential_options.get(key_str) {
            return Ok(if enabled { "enabled" } else { "disabled" }.to_string());
        }
        let param = adbc_db_param(key_str).ok();
        if let Some(value) = param.and_then(|param| self.duration_options.get(param)) {
            return Ok(value.clone());
        }
        if let Some(setting) = param.and_then(|param| self.sf_settings.get(param)) {
            return match setting {
                Setting::String(value) => Ok(value.clone()),
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
        if let Ok(param) = adbc_db_param(key_str)
            && let Some(Setting::Bytes(b)) = self.sf_settings.get(param)
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
        if key_str == MAX_RETRY_COUNT
            && let Some(count) = self.max_retry_count
        {
            return Ok(count);
        }
        if let Ok(param) = adbc_db_param(key_str)
            && let Some(Setting::Int(i)) = self.sf_settings.get(param)
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
        if let Ok(param) = adbc_db_param(key_str)
            && let Some(Setting::Double(d)) = self.sf_settings.get(param)
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
            let authority = decode_uri_component(authority, "account/host")?;
            if explicit_account {
                let (host, port) = authority
                    .rsplit_once(':')
                    .map_or((authority.as_str(), None), |(host, port)| {
                        (host, Some(port))
                    });
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
                    OptionValue::String(authority),
                )?;
            }
        }
        if let Some(database) = parts.get(1).filter(|s| !s.is_empty()) {
            self.set_option(
                OptionDatabase::Other("adbc.snowflake.sql.db".into()),
                OptionValue::String(decode_uri_component(database, "database")?),
            )?;
        }
        if let Some(schema) = parts.get(2).filter(|s| !s.is_empty()) {
            self.set_option(
                OptionDatabase::Other("adbc.snowflake.sql.schema".into()),
                OptionValue::String(decode_uri_component(schema, "schema")?),
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
                        "authenticator" => "authenticator",
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

/// Percent-decode one URI component, rejecting invalid UTF-8.
fn decode_uri_component(value: &str, what: &str) -> Result<String> {
    percent_decode_str(value)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|e| {
            Error::with_message_and_status(
                format!("invalid UTF-8 in URI {what}: {e}"),
                Status::InvalidArguments,
            )
        })
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
    let mut sf_options = HashMap::new();
    // This mirrors sf_core peers' raw-kwargs contract: only a call with no
    // database settings and no raw connection options is a bare connection.
    let mut no_connection_details = database_options.is_empty();
    let mut post_connect_options = Vec::new();

    for (key, value) in opts {
        no_connection_details = false;
        if let OptionConnection::Other(name) = &key {
            if name == GEOGRAPHY_OUTPUT_FORMAT || name == GEOMETRY_OUTPUT_FORMAT {
                post_connect_options.push((key, value));
                continue;
            }
            match adbc_db_opt_to_sf(name, &value) {
                Ok(Some((param, setting))) => {
                    sf_options.insert(param.to_string(), setting);
                }
                Ok(None) => {
                    return Err(Error::with_message_and_status(
                        format!("unrecognized connection option '{name}'"),
                        Status::NotImplemented,
                    ));
                }
                Err(error) if error.status == Status::NotImplemented => {
                    return Err(Error::with_message_and_status(
                        format!("unrecognized connection option '{name}'"),
                        Status::NotImplemented,
                    ));
                }
                Err(error) => return Err(error),
            }
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
        application_path: None,
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
        if !self
            .sf_settings
            .contains_key(param_names::APPLICATION.as_str())
            && !sf_options
                .keys()
                .any(|key| key.eq_ignore_ascii_case(param_names::APPLICATION.as_str()))
        {
            sf_options.insert(
                param_names::APPLICATION.into(),
                Setting::String("ADBC Snowflake Driver (Rust)".to_string()),
            );
        }
        if self.auth_type == Some("auth_okta") && self.okta_url.as_deref().unwrap_or("").is_empty()
        {
            return Err(Error::with_message_and_status(
                format!("database option '{OKTA_URL}' is required for '{AUTH_TYPE}=auth_okta'"),
                Status::InvalidArguments,
            ));
        }
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
                    None,
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
                    HashMap::from([
                        (
                            "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT".to_string(),
                            "ARROW".to_string(),
                        ),
                        (
                            "GEOGRAPHY_OUTPUT_FORMAT".to_string(),
                            self.geography_output_format.as_str().to_string(),
                        ),
                        (
                            "GEOMETRY_OUTPUT_FORMAT".to_string(),
                            self.geometry_output_format.as_str().to_string(),
                        ),
                    ]),
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
                None,
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
            timestamp_precision: self.timestamp_precision,
            geography_output_format: self.geography_output_format,
            geometry_output_format: self.geometry_output_format,
            operation: std::sync::Mutex::new(std::sync::Arc::new(
                sf_core::apis::operation_ctx::OperationCtx::with_own_token(),
            )),
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
    fn geo_formats_validate_and_round_trip_without_changing_on_error() {
        let mut db = make_db();
        for key in [GEOGRAPHY_OUTPUT_FORMAT, GEOMETRY_OUTPUT_FORMAT] {
            let option = OptionDatabase::Other(key.into());
            assert_eq!(db.get_option_string(option.clone()).unwrap(), "GEOJSON");
            db.set_option(option.clone(), OptionValue::String("ewKb".into()))
                .unwrap();
            assert_eq!(db.get_option_string(option.clone()).unwrap(), "EWKB");
            for value in [OptionValue::String("WKT".into()), OptionValue::Int(1)] {
                let err = db.set_option(option.clone(), value).unwrap_err();
                assert_eq!(err.status, Status::InvalidArguments);
                assert_eq!(db.get_option_string(option.clone()).unwrap(), "EWKB");
            }
            db.set_option(option.clone(), OptionValue::String("geojson".into()))
                .unwrap();
            assert_eq!(db.get_option_string(option).unwrap(), "GEOJSON");
        }
    }

    #[test]
    fn connection_options_reject_unknown_before_opening() {
        let error = accumulate_connection_options(
            &HashMap::new(),
            [(
                OptionConnection::Other("adbc.fake".into()),
                OptionValue::Int(1),
            )],
        )
        .err()
        .expect("unknown option must fail");
        assert_eq!(error.status, Status::NotImplemented);
        assert_eq!(error.message, "unrecognized connection option 'adbc.fake'");
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
                param_names::SERVER_SESSION_KEEP_ALIVE.as_str(),
                true,
            ),
            (
                "adbc.snowflake.sql.client_option.cache_mfa_token",
                param_names::CLIENT_STORE_TEMPORARY_CREDENTIAL.as_str(),
                true,
            ),
            (
                "adbc.snowflake.sql.client_option.store_temp_creds",
                param_names::CLIENT_STORE_TEMPORARY_CREDENTIAL.as_str(),
                true,
            ),
            (
                "adbc.snowflake.sql.client_option.validate_default_parameters",
                param_names::VALIDATE_DEFAULT_PARAMETERS.as_str(),
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
    fn go_options_use_consumed_core_settings() {
        let cases = [
            (
                "adbc.snowflake.sql.client_option.keep_session_alive",
                param_names::SERVER_SESSION_KEEP_ALIVE.as_str(),
                Setting::Bool(true),
            ),
            (
                CACHE_MFA_TOKEN,
                param_names::CLIENT_STORE_TEMPORARY_CREDENTIAL.as_str(),
                Setting::Bool(true),
            ),
            (
                STORE_TEMP_CREDS,
                param_names::CLIENT_STORE_TEMPORARY_CREDENTIAL.as_str(),
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
            (
                "adbc.snowflake.sql.client_option.validate_default_parameters",
                param_names::VALIDATE_DEFAULT_PARAMETERS.as_str(),
                Setting::Bool(true),
            ),
            (
                "adbc.snowflake.sql.client_option.proxy_protocol",
                param_names::PROXY_SCHEME.as_str(),
                Setting::String("https".into()),
            ),
        ];
        for (key, expected_name, expected_setting) in cases {
            let value = if key.ends_with("identity_provider") {
                "AWS"
            } else if key.ends_with("proxy_protocol") {
                "HTTPS"
            } else {
                "enabled"
            };
            let (name, setting) = adbc_db_opt_to_sf(key, &OptionValue::String(value.into()))
                .unwrap()
                .unwrap();
            assert_eq!(name, expected_name);
            assert_eq!(setting, expected_setting);
        }
    }

    #[test]
    fn credential_cache_options_round_trip_even_though_core_combines_them() {
        let mut db = make_db();
        for (key, value) in [(CACHE_MFA_TOKEN, "enabled"), (STORE_TEMP_CREDS, "disabled")] {
            db.set_option(
                OptionDatabase::Other(key.into()),
                OptionValue::String(value.into()),
            )
            .unwrap();
            assert_eq!(
                db.get_option_string(OptionDatabase::Other(key.into()))
                    .unwrap(),
                value
            );
        }
        assert_eq!(
            db.sf_settings
                .get(param_names::CLIENT_STORE_TEMPORARY_CREDENTIAL.as_str()),
            Some(&Setting::Bool(false))
        );
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(CACHE_MFA_TOKEN.into()))
                .unwrap(),
            "enabled"
        );
    }

    #[test]
    fn unsupported_go_and_unknown_options_are_not_stored() {
        let mut db = make_db();
        for key in [
            "adbc.snowflake.sql.region",
            "adbc.snowflake.sql.client_option.ocsp_fail_open_mode",
            "adbc.snowflake.sql.client_option.jwt_expire_timeout",
            "adbc.snowflake.sql.client_option.tracing",
            "adbc.snowflake.sql.client_option.config_file",
            "adbc.snowflake.sql.client_option.use_high_precision",
            "adbc.snowflake.sql.client_option.stream_retry_enabled",
            "adbc.connection.get_objects.match_exactly",
            "some.unknown.database.option",
        ] {
            let error = db
                .set_option(
                    OptionDatabase::Other(key.into()),
                    OptionValue::String("value".into()),
                )
                .unwrap_err();
            assert_eq!(error.status, Status::NotImplemented, "{key}");
            assert_eq!(
                error.message,
                format!("unrecognized database option '{key}'")
            );
            assert!(!db.sf_settings.contains_key(key));
            assert_eq!(
                db.get_option_string(OptionDatabase::Other(key.into()))
                    .unwrap_err()
                    .status,
                Status::NotFound
            );
        }
        assert!(db.sf_settings.is_empty());
    }

    #[test]
    fn recognized_raw_core_alias_is_accepted_and_gettable() {
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Other("USER".into()),
            OptionValue::String("raw-user".into()),
        )
        .unwrap();
        assert_eq!(
            db.sf_settings.get(param_names::USER.as_str()),
            Some(&Setting::String("raw-user".into()))
        );
        assert_eq!(
            db.get_option_string(OptionDatabase::Other("USER".into()))
                .unwrap(),
            "raw-user"
        );
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
            ("adbc.snowflake.sql.client_option.app_name", "my-app"),
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
            ("adbc.snowflake.sql.client_option.client_timeout", "120s"),
            (
                "adbc.snowflake.sql.client_option.identity_provider",
                "AZURE",
            ),
            (
                "adbc.snowflake.sql.client_option.proxy_host",
                "proxy.example",
            ),
            ("adbc.snowflake.sql.client_option.proxy_user", "proxy-user"),
            (
                "adbc.snowflake.sql.client_option.proxy_password",
                "proxy-pass",
            ),
            ("adbc.snowflake.sql.client_option.proxy_protocol", "https"),
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
    fn auth_type_maps_all_go_values_and_round_trips() {
        for &(go, core) in &AUTH_TYPES {
            if let Some(core) = core {
                let mut db = make_db();
                let option = OptionDatabase::Other(AUTH_TYPE.into());
                db.set_option(option.clone(), OptionValue::String(go.into()))
                    .unwrap();
                assert_eq!(
                    db.sf_settings.get(param_names::AUTHENTICATOR.as_str()),
                    Some(&Setting::String(core.into()))
                );
                assert_eq!(db.get_option_string(option).unwrap(), go);
            }
        }
    }

    #[test]
    fn native_authenticator_values_map_back_to_go_auth_type() {
        for &(go, core) in &AUTH_TYPES {
            if let Some(core) = core {
                let mut db = make_db();
                db.set_option(
                    OptionDatabase::Other("authenticator".into()),
                    OptionValue::String(core.to_ascii_lowercase()),
                )
                .unwrap();
                assert_eq!(
                    db.get_option_string(OptionDatabase::Other(AUTH_TYPE.into()))
                        .unwrap(),
                    go
                );
            }
        }
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Other("authenticator".into()),
            OptionValue::String("SNOWFLAKE_PASSWORD".into()),
        )
        .unwrap();
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(AUTH_TYPE.into()))
                .unwrap(),
            "auth_snowflake"
        );
    }

    #[test]
    fn okta_url_only_selects_authenticator_when_auth_okta_is_selected() {
        for url_first in [true, false] {
            let mut db = make_db();
            let auth = OptionDatabase::Other(AUTH_TYPE.into());
            let okta = OptionDatabase::Other(OKTA_URL.into());
            if url_first {
                db.set_option(
                    okta.clone(),
                    OptionValue::String("https://okta.test".into()),
                )
                .unwrap();
                assert!(
                    !db.sf_settings
                        .contains_key(param_names::AUTHENTICATOR.as_str())
                );
                db.set_option(auth.clone(), OptionValue::String("auth_okta".into()))
                    .unwrap();
            } else {
                db.set_option(auth.clone(), OptionValue::String("auth_okta".into()))
                    .unwrap();
                db.set_option(
                    okta.clone(),
                    OptionValue::String("https://okta.test".into()),
                )
                .unwrap();
            }
            assert_eq!(
                db.sf_settings.get(param_names::AUTHENTICATOR.as_str()),
                Some(&Setting::String("https://okta.test".into()))
            );
            assert_eq!(db.get_option_string(auth.clone()).unwrap(), "auth_okta");
            assert_eq!(
                db.get_option_string(okta.clone()).unwrap(),
                "https://okta.test"
            );
            db.set_option(auth, OptionValue::String("auth_oauth".into()))
                .unwrap();
            assert_eq!(
                db.sf_settings.get(param_names::AUTHENTICATOR.as_str()),
                Some(&Setting::String("OAUTH".into()))
            );
            assert_eq!(db.get_option_string(okta).unwrap(), "https://okta.test");
        }
    }

    #[test]
    fn invalid_auth_type_does_not_change_setting() {
        let mut db = make_db();
        let auth = OptionDatabase::Other(AUTH_TYPE.into());
        db.set_option(auth.clone(), OptionValue::String("auth_snowflake".into()))
            .unwrap();
        for invalid in ["SNOWFLAKE", "oauth", "auth_OKTA", "auth_unknown"] {
            let error = db
                .set_option(auth.clone(), OptionValue::String(invalid.into()))
                .unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            assert_eq!(
                db.sf_settings.get(param_names::AUTHENTICATOR.as_str()),
                Some(&Setting::String("SNOWFLAKE".into()))
            );
            assert_eq!(
                db.get_option_string(auth.clone()).unwrap(),
                "auth_snowflake"
            );
        }
    }

    #[test]
    fn uri_authenticator_uses_native_sf_core_values() {
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("snowflake://myaccount?authenticator=externalbrowser".into()),
        )
        .unwrap();
        assert_eq!(
            db.sf_settings.get(param_names::AUTHENTICATOR.as_str()),
            Some(&Setting::String("externalbrowser".into()))
        );
        assert_eq!(
            db.get_option_string(OptionDatabase::Other(AUTH_TYPE.into()))
                .unwrap(),
            "auth_ext_browser"
        );
    }

    #[test]
    fn timestamp_precision_rejects_invalid_values_without_changing_state() {
        let mut db = make_db();
        let option = OptionDatabase::Other(MAX_TIMESTAMP_PRECISION.into());
        for valid in [
            "nanoseconds",
            "nanoseconds_error_on_overflow",
            "microseconds",
        ] {
            db.set_option(option.clone(), OptionValue::String(valid.into()))
                .unwrap();
            assert_eq!(db.get_option_string(option.clone()).unwrap(), valid);
        }
        for invalid in [
            OptionValue::String("millis".into()),
            OptionValue::String("".into()),
            OptionValue::Int(0),
        ] {
            let displayed = match &invalid {
                OptionValue::String(value) => value.clone(),
                OptionValue::Int(value) => value.to_string(),
                _ => unreachable!(),
            };
            let error = db.set_option(option.clone(), invalid).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            assert_eq!(
                error.message,
                format!(
                    "Invalid value for database option '{MAX_TIMESTAMP_PRECISION}': '{displayed}'"
                )
            );
            assert_eq!(
                db.get_option_string(option.clone()).unwrap(),
                "microseconds"
            );
        }
    }

    #[test]
    fn go_duration_converts_fractional_composite_and_extreme_values() {
        for (duration, seconds) in [
            ("0", 0),
            ("300ms", 1),
            ("1.5s", 2),
            (".5s", 1),
            ("1m30s", 90),
            ("1h.5m", 3630),
            ("-1m30s", 90),
            ("1ns", 1),
            ("-9223372036.854775808s", 9223372037),
            ("9223372036.854775807s", 9223372037),
        ] {
            assert_eq!(go_duration_seconds(duration), Some(seconds), "{duration}");
        }
        for invalid in ["", "30", "1w", "1.5", "1ss", "9223372036.854775808s"] {
            assert_eq!(go_duration_seconds(invalid), None, "{invalid}");
        }
        let mut db = make_db();
        let option =
            OptionDatabase::Other("adbc.snowflake.sql.client_option.client_timeout".into());
        db.set_option(option.clone(), OptionValue::String("300ms".into()))
            .unwrap();
        assert_eq!(
            db.sf_settings.get(param_names::RETRY_TIMEOUT.as_str()),
            Some(&Setting::Int(1))
        );
        assert_eq!(db.get_option_string(option.clone()).unwrap(), "300ms");
        let error = db
            .set_option(option.clone(), OptionValue::String("30".into()))
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert_eq!(db.get_option_string(option).unwrap(), "300ms");
    }

    #[test]
    fn retry_count_converts_retries_to_attempts_and_preserves_zero() {
        let mut db = make_db();
        let option = OptionDatabase::Other(MAX_RETRY_COUNT.into());
        for (input, attempts) in [("0", 8), ("1", 2), ("7", 8)] {
            db.set_option(option.clone(), OptionValue::String(input.into()))
                .unwrap();
            assert_eq!(
                db.sf_settings.get(param_names::RETRY_MAX_ATTEMPTS.as_str()),
                Some(&Setting::Int(attempts))
            );
            assert_eq!(db.get_option_string(option.clone()).unwrap(), input);
        }
        for invalid in ["-1", "4294967295", "bogus"] {
            let error = db
                .set_option(option.clone(), OptionValue::String(invalid.into()))
                .unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            assert_eq!(db.get_option_int(option.clone()).unwrap(), 7);
        }
    }

    #[test]
    fn proxy_port_validates_go_bounds() {
        let mut db = make_db();
        let option = OptionDatabase::Other("adbc.snowflake.sql.client_option.proxy_port".into());
        for port in ["1", "65535"] {
            db.set_option(option.clone(), OptionValue::String(port.into()))
                .unwrap();
            assert_eq!(db.get_option_string(option.clone()).unwrap(), port);
        }
        for invalid in ["0", "65536", "-1", "abc"] {
            assert_eq!(
                db.set_option(option.clone(), OptionValue::String(invalid.into()))
                    .unwrap_err()
                    .status,
                Status::InvalidArguments
            );
            assert_eq!(db.get_option_int(option.clone()).unwrap(), 65535);
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

    #[test]
    fn uri_percent_decodes_database_and_schema() {
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("snowflake://alice@myaccount/MY%20DB/S%2FX".into()),
        )
        .unwrap();
        assert_eq!(
            db.sf_settings.get(param_names::DATABASE.as_str()).unwrap(),
            &sf_core::config::settings::Setting::String("MY DB".into())
        );
        assert_eq!(
            db.sf_settings.get(param_names::SCHEMA.as_str()).unwrap(),
            &sf_core::config::settings::Setting::String("S/X".into())
        );

        let err = make_db()
            .set_option(
                OptionDatabase::Uri,
                OptionValue::String("snowflake://myaccount/%FF".into()),
            )
            .unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
    }
}
