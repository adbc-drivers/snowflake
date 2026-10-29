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
use driverbase::error::ErrorHelper;
use sf_core::apis::database_driver_v1::{
    Handle, ValidationIssue, ValidationSeverity, connection::WrapperIdentity,
};
use sf_core::config::param_registry::{Wrapper, param_names, registry};
use sf_core::config::rest_parameters::DEFAULT_JWT_TIMEOUT_SECS;
use sf_core::config::settings::Setting;

use crate::connection::{Connection, cleanup_connection_handle};
use crate::driver::{Inner, TimestampPrecision};
use crate::error::SnowflakeError;
use driverbase::telemetry::{
    Exporter, TRACE_PARENT, TRACES_EXPORTER, TRACES_FOLDER_PATH, validate_trace_parent,
};

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
        let value = database_option_as_string(key, value)?;
        if value.eq_ignore_ascii_case("EWKB") {
            Ok(Self::Ewkb)
        } else if value.eq_ignore_ascii_case("GEOJSON") {
            Ok(Self::GeoJson)
        } else {
            Err(SnowflakeError::invalid_argument()
                .context("set database option")
                .format(format_args!(
                    "option '{key}': invalid value '{value}' (must be 'EWKB' or 'GeoJSON')"
                ))
                .into())
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

fn invalid_option_value(key: &str, value: impl std::fmt::Display) -> Error {
    SnowflakeError::invalid_argument()
        .context("set database option")
        .format(format_args!("option '{key}': invalid value '{value}'"))
        .into()
}

fn database_option_as_string<'v>(key: &str, value: &'v OptionValue) -> Result<&'v str> {
    SnowflakeError::option_as_string(&key, value)
        .map_err(|error| error.context("set database option").into())
}

fn unrecognized_option(key: &str) -> Error {
    SnowflakeError::set_unknown_option(&key)
        .context("set database option")
        .into()
}

fn unsupported_uri_config(key: &str) -> Error {
    SnowflakeError::not_implemented()
        .context("set URI database option")
        .format(format_args!(
            "unsupported Snowflake URI configuration parameter '{key}'"
        ))
        .into()
}

fn option_to_setting(key: &str, value: &OptionValue) -> Result<Setting> {
    match value {
        OptionValue::String(value) => Ok(Setting::String(value.clone())),
        OptionValue::Int(value) => Ok(Setting::Int(*value)),
        OptionValue::Double(value) => Ok(Setting::Double(*value)),
        OptionValue::Bytes(value) => Ok(Setting::Bytes(value.clone())),
        _ => Err(SnowflakeError::set_invalid_option(&key, value)
            .context("set database option")
            .message("unsupported option value type")
            .into()),
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
            .ok_or_else(|| invalid_option_value(key, seconds))?,
        _ => {
            return Err(SnowflakeError::set_invalid_option(&key, value)
                .context("set database option")
                .into());
        }
    };
    Ok(Setting::Int(seconds))
}

fn non_negative_int(key: &str, value: &OptionValue) -> Result<i64> {
    let number = SnowflakeError::option_as_int(&key, value)
        .map_err(|error| error.context("set database option"))?;
    if number < 0 {
        return Err(invalid_option_value(key, number));
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
        OptionValue::String(_) => Err(SnowflakeError::invalid_argument()
            .context("set boolean database option")
            .message("must be enabled, true, 1, disabled, false, or 0")
            .into()),
        OptionValue::Int(value) => Ok(*value != 0),
        _ => Err(SnowflakeError::invalid_argument()
            .context("set boolean database option")
            .message("must be a string or int")
            .into()),
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
        "adbc.snowflake.sql.client_option.jwt_expire_timeout" => param_names::JWT_TIMEOUT,
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

/// Go's max_retry_count counts retries (0 = driver default); sf_core counts
/// attempts, so N retries are N + 1 attempts and 0 maps to the default of 8.
fn retry_attempts(key: &str, value: &OptionValue) -> Result<(i64, i64)> {
    let retries = non_negative_int(key, value)?;
    let attempts = if retries == 0 {
        8
    } else {
        retries
            .checked_add(1)
            .filter(|&attempts| attempts <= i64::from(u32::MAX))
            .ok_or_else(|| invalid_option_value(key, retries))?
    };
    Ok((retries, attempts))
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
                return Err(invalid_option_value(key, port));
            }
            Setting::Int(port)
        }
        "adbc.snowflake.sql.client_option.login_timeout"
        | "adbc.snowflake.sql.client_option.request_timeout"
        | "adbc.snowflake.sql.client_option.client_timeout" => duration_setting(key, value)?,
        // Like gosnowflake, a zero lifetime selects the default.
        "adbc.snowflake.sql.client_option.jwt_expire_timeout" => {
            match duration_setting(key, value)? {
                Setting::Int(0) => Setting::Int(DEFAULT_JWT_TIMEOUT_SECS),
                setting => setting,
            }
        }
        "adbc.snowflake.sql.client_option.tls_skip_verify"
        | "adbc.snowflake.sql.client_option.keep_session_alive"
        | CACHE_MFA_TOKEN
        | STORE_TEMP_CREDS
        | "adbc.snowflake.sql.client_option.validate_default_parameters" => {
            Setting::Bool(adbc_option_enabled(value)?)
        }
        "passcodeInPassword"
        | "oauth_enable_single_use_refresh_tokens"
        | "workload_identity_aws_use_outbound_token"
        | "log_query_text"
        | "log_query_parameters"
        | "disable_query_context_cache"
        | "include_retry_reason"
        | "disable_saml_url_check"
        | "enable_connection_diag"
        | "crl_allow_certificates_without_crl_url" => Setting::Bool(adbc_option_enabled(value)?),
        "crl_enable_memory_caching" | "crl_enable_disk_caching" => {
            Setting::Bool(adbc_option_enabled(value)?)
        }
        "crl_max_download_size" | "crl_http_timeout" => Setting::Int(non_negative_int(key, value)?),
        "adbc.snowflake.sql.client_option.disable_telemetry" => {
            Setting::Bool(!adbc_option_enabled(value)?)
        }
        "adbc.snowflake.sql.client_option.proxy_protocol" => {
            let protocol = database_option_as_string(key, value)?;
            if !protocol.eq_ignore_ascii_case("http") && !protocol.eq_ignore_ascii_case("https") {
                return Err(invalid_option_value(key, protocol));
            }
            Setting::String(protocol.to_ascii_lowercase())
        }
        MAX_RETRY_COUNT => Setting::Int(retry_attempts(key, value)?.1),
        _ => option_to_setting(key, value)?,
    };
    Ok(Some((param, setting)))
}

pub struct Database {
    pub(crate) inner: Arc<Inner>,
    pub(crate) db_handle: Handle,
    /// Canonical sf_core settings already stored on the database handle. Used
    /// only for local getters and the bare-connection signal, never re-seeded.
    pub(crate) sf_settings: HashMap<String, Setting>,
    /// Additional login session parameters; names are stored as supplied for GetOption.
    pub(crate) session_parameters: HashMap<String, String>,
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
    pub(crate) trace_parent: Option<String>,
    pub(crate) traces_exporter: Option<Exporter>,
    pub(crate) traces_folder_path: Option<String>,
}

impl Drop for Database {
    fn drop(&mut self) {
        let _ = self.inner.sf.database_release(self.db_handle);
        // The batch exporter owns its own thread, so this does not enter the
        // Tokio runtime even when a database is dropped on a runtime worker.
        self.inner.telemetry.flush();
    }
}

impl Database {
    fn set_sf_options(&mut self, options: HashMap<String, Setting>) -> Result<()> {
        let warnings = self
            .inner
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
        if key_str == TRACE_PARENT {
            let parent = database_option_as_string(key_str, &value)?;
            self.trace_parent = validate_trace_parent(parent)?;
            return Ok(());
        }
        if key_str == TRACES_EXPORTER {
            let exporter = database_option_as_string(key_str, &value)?;
            self.traces_exporter = Exporter::parse_option(exporter)?;
            return Ok(());
        }
        if key_str == TRACES_FOLDER_PATH {
            let folder = database_option_as_string(key_str, &value)?;
            self.traces_folder_path = (!folder.is_empty()).then(|| folder.to_owned());
            return Ok(());
        }
        if key_str == "uri" {
            let uri = SnowflakeError::option_as_string(&key_str, &value)
                .map_err(|error| error.context("set URI database option"))?;
            return self.apply_uri(uri);
        }
        if key_str == MAX_TIMESTAMP_PRECISION {
            let precision = database_option_as_string(key_str, &value)?;
            self.timestamp_precision = match precision {
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
            let auth = database_option_as_string(key_str, &value)?;
            let &(go_value, core_value) = AUTH_TYPES
                .iter()
                .find(|&&(go_value, _)| go_value == auth)
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
            let url = database_option_as_string(key_str, &value)?;
            if self.auth_type == Some("auth_okta") {
                self.set_sf_option(
                    param_names::AUTHENTICATOR.as_str(),
                    Setting::String(url.to_owned()),
                )?;
            }
            self.okta_url = Some(url.to_owned());
            return Ok(());
        }
        if key_str == MAX_RETRY_COUNT {
            let (retries, attempts) = retry_attempts(key_str, &value)?;
            self.set_sf_option(
                param_names::RETRY_MAX_ATTEMPTS.as_str(),
                Setting::Int(attempts),
            )?;
            self.max_retry_count = Some(retries);
            return Ok(());
        }
        let mapped = match adbc_db_opt_to_sf(key_str, &value) {
            Ok(mapped) => mapped,
            Err(error)
                if error.status == Status::NotImplemented
                    && !key_str
                        .get(..5)
                        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("adbc.")) =>
            {
                let session_value = database_option_as_string(key_str, &value)?;
                self.set_session_parameter(key_str.to_owned(), session_value.to_owned());
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if let Some((param, setting)) = mapped {
            let credential_enabled = match &setting {
                Setting::Bool(enabled) => Some(*enabled),
                _ => None,
            };
            self.set_sf_option(param, setting)?;
            self.session_parameters
                .retain(|key, _| !key.eq_ignore_ascii_case(param));
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
                        | "adbc.snowflake.sql.client_option.jwt_expire_timeout"
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
                        "adbc.snowflake.sql.client_option.client_timeout" => {
                            param_names::RETRY_TIMEOUT.as_str()
                        }
                        _ => param_names::JWT_TIMEOUT.as_str(),
                    },
                    duration.clone(),
                );
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let key_str = key.as_ref();
        if key_str == TRACE_PARENT {
            return Ok(self.trace_parent.clone().unwrap_or_default());
        }
        if key_str == TRACES_EXPORTER {
            return Ok(self
                .traces_exporter
                .map(Exporter::as_str)
                .unwrap_or_default()
                .to_owned());
        }
        if key_str == TRACES_FOLDER_PATH {
            return Ok(self.traces_folder_path.clone().unwrap_or_default());
        }
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
                SnowflakeError::get_unknown_option(&key_str)
                    .context("get database option")
                    .into()
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
        if let Some((_, value)) = self
            .session_parameters
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(key_str))
        {
            return Ok(value.clone());
        }
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
                Setting::Bytes(_) => Err(SnowflakeError::invalid_argument()
                    .context("get database option as string")
                    .format(format_args!("option '{key_str}' is bytes"))
                    .into()),
            };
        }
        Err(SnowflakeError::get_unknown_option(&key_str)
            .context("get database option")
            .into())
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        let key_str = key.as_ref();
        if let Ok(param) = adbc_db_param(key_str)
            && let Some(Setting::Bytes(b)) = self.sf_settings.get(param)
        {
            return Ok(b.clone());
        }
        Err(SnowflakeError::get_unknown_option(&key_str)
            .context("get database option")
            .into())
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
        Err(SnowflakeError::get_unknown_option(&key_str)
            .context("get database option")
            .into())
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        let key_str = key.as_ref();
        if let Ok(param) = adbc_db_param(key_str)
            && let Some(Setting::Double(d)) = self.sf_settings.get(param)
        {
            return Ok(*d);
        }
        Err(SnowflakeError::get_unknown_option(&key_str)
            .context("get database option")
            .into())
    }
}

impl Database {
    /// Snowflake parameter names are case-insensitive: the last spelling set wins.
    fn set_session_parameter(&mut self, key: String, value: String) {
        self.session_parameters
            .retain(|existing, _| !existing.eq_ignore_ascii_case(&key));
        self.session_parameters.insert(key, value);
    }

    /// Build the single login map. Go normalizes parameter names to uppercase
    /// when sending them; driver-owned result/geo options always take precedence.
    fn login_session_parameters(&self) -> HashMap<String, String> {
        let mut parameters = self
            .session_parameters
            .iter()
            .map(|(key, value)| (key.to_uppercase(), value.clone()))
            .collect::<HashMap<_, _>>();
        parameters.insert(
            "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT".into(),
            "ARROW_FORCE".into(),
        );
        parameters.insert(
            "GEOGRAPHY_OUTPUT_FORMAT".into(),
            self.geography_output_format.as_str().into(),
        );
        parameters.insert(
            "GEOMETRY_OUTPUT_FORMAT".into(),
            self.geometry_output_format.as_str().into(),
        );
        parameters
    }

    /// Parse a Snowflake URI and apply each component as an individual option.
    /// Format: snowflake://[user[:password]@]host[/database[/schema]][?param=value&...]
    /// Other query params are Snowflake login session parameters.
    ///
    /// Limitations: passwords containing `@` are not supported; use `set_option` for
    /// Username/Password directly when credentials contain special characters.
    fn apply_uri(&mut self, uri: &str) -> Result<()> {
        let stripped = uri.strip_prefix("snowflake://").unwrap_or(uri);
        let (user_info, rest) = stripped
            .split_once('@')
            .map_or((None, stripped), |(info, rest)| (Some(info), rest));

        if let Some(info) = user_info {
            if let Some((encoded_user, encoded_pass)) = info.split_once(':') {
                let user = decode_uri_component(encoded_user, "username")?;
                let pass = decode_uri_component(encoded_pass, "password")?;
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
                let user = decode_uri_component(info, "username")?;
                self.set_option(
                    OptionDatabase::Username,
                    OptionValue::String(user.into_owned()),
                )?;
            }
        }

        let (path, query) = if let Some(q) = rest.find('?') {
            (&rest[..q], Some(&rest[q + 1..]))
        } else {
            (rest, None)
        };
        let explicit_account = query.is_some_and(|query| {
            query.split('&').any(|pair| {
                pair.split_once('=').is_some_and(|(key, _)| {
                    decode_uri_component(key, "query parameter name")
                        .is_ok_and(|key| key == "account")
                })
            })
        });

        let mut parts = path.splitn(3, '/');
        if let Some(authority) = parts.next().filter(|s| !s.is_empty()) {
            let authority = decode_uri_component(authority, "account/host")?;
            // The documented form is host[:port], with or without account=.
            let (host, port) = match authority.rsplit_once(':') {
                Some((host, port)) => (host.to_owned(), Some(port.to_owned())),
                None => (authority.into_owned(), None),
            };
            // Like gosnowflake's ParseDSN: an authority containing the
            // Snowflake domain is a full hostname whose first label is the
            // account (unless account= is given); otherwise it is the account.
            let is_hostname = host.to_ascii_lowercase().contains(".snowflakecomputing.");
            if explicit_account || is_hostname {
                if is_hostname && !explicit_account {
                    let account = host.split('.').next().unwrap_or_default();
                    self.set_option(
                        OptionDatabase::Other("adbc.snowflake.sql.account".into()),
                        OptionValue::String(account.to_owned()),
                    )?;
                }
                self.set_option(
                    OptionDatabase::Other("adbc.snowflake.sql.uri.host".into()),
                    OptionValue::String(host),
                )?;
            } else {
                self.set_option(
                    OptionDatabase::Other("adbc.snowflake.sql.account".into()),
                    OptionValue::String(host),
                )?;
            }
            if let Some(port) = port {
                self.set_option(
                    OptionDatabase::Other("adbc.snowflake.sql.uri.port".into()),
                    OptionValue::String(port),
                )?;
            }
        }
        if let Some(database) = parts.next().filter(|s| !s.is_empty()) {
            self.set_option(
                OptionDatabase::Other("adbc.snowflake.sql.db".into()),
                OptionValue::String(decode_uri_component(database, "database")?.into_owned()),
            )?;
        }
        if let Some(schema) = parts.next().filter(|s| !s.is_empty()) {
            self.set_option(
                OptionDatabase::Other("adbc.snowflake.sql.schema".into()),
                OptionValue::String(decode_uri_component(schema, "schema")?.into_owned()),
            )?;
        }

        if let Some(q) = query {
            for pair in q.split('&') {
                if let Some((key, encoded_value)) = pair.split_once('=') {
                    let key = decode_uri_component(key, "query parameter name")?;
                    let value = decode_uri_component(encoded_value, "query parameter")?;
                    if let Some(option) = uri_query_option(&key)? {
                        let value = match key.as_ref() {
                            // gosnowflake's timeout DSN values are integral seconds.
                            "loginTimeout"
                            | "requestTimeout"
                            | "clientTimeout"
                            | "jwtTimeout"
                            | "crlDownloadMaxSize"
                            | "crlHttpClientTimeout" => OptionValue::Int(
                                value
                                    .parse::<i64>()
                                    .map_err(|_| invalid_option_value(&key, &value))?,
                            ),
                            "crlInMemoryCacheDisabled" | "crlOnDiskCacheDisabled" => {
                                OptionValue::String(
                                    (!adbc_option_enabled(&OptionValue::String(
                                        value.into_owned(),
                                    ))?)
                                    .to_string(),
                                )
                            }
                            // Go's DSN uses URL-safe base64; sf_core accepts standard base64.
                            "privateKey" => {
                                OptionValue::String(value.replace('-', "+").replace('_', "/"))
                            }
                            "certRevocationCheckMode"
                                if !matches!(
                                    value.to_ascii_lowercase().as_str(),
                                    "disabled" | "enabled" | "advisory"
                                ) =>
                            {
                                return Err(invalid_option_value(&key, &value));
                            }
                            _ => OptionValue::String(value.into_owned()),
                        };
                        self.set_option(OptionDatabase::Other(option.into()), value)
                            .map_err(|error| {
                                if error.status == Status::NotImplemented {
                                    unsupported_uri_config(&key)
                                } else {
                                    error
                                }
                            })?;
                    } else if key
                        .get(..5)
                        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("adbc."))
                    {
                        return Err(unrecognized_option(&key));
                    } else {
                        self.set_session_parameter(key.into_owned(), value.into_owned());
                    }
                }
            }
        }
        Ok(())
    }
}

/// Match gosnowflake v2's parseDSNParams config keys. Non-config query keys
/// (including Snowflake session names) are passed to the login payload instead.
fn uri_query_option(key: &str) -> Result<Option<&'static str>> {
    let option = match key {
        "account" => "adbc.snowflake.sql.account",
        "warehouse" => "adbc.snowflake.sql.warehouse",
        "database" => "adbc.snowflake.sql.db",
        "schema" => "adbc.snowflake.sql.schema",
        "role" => "adbc.snowflake.sql.role",
        "host" => "adbc.snowflake.sql.uri.host",
        "port" => "adbc.snowflake.sql.uri.port",
        "protocol" => "adbc.snowflake.sql.uri.protocol",
        "authenticator" => "authenticator",
        "passcode" => param_names::PASSCODE.as_str(),
        "passcodeInPassword" => param_names::PASSCODE_IN_PASSWORD.as_str(),
        "oauthClientId" => param_names::OAUTH_CLIENT_ID.as_str(),
        "oauthClientSecret" => param_names::OAUTH_CLIENT_SECRET.as_str(),
        "oauthAuthorizationUrl" => param_names::OAUTH_AUTHORIZATION_URL.as_str(),
        "oauthTokenRequestUrl" => param_names::OAUTH_TOKEN_REQUEST_URL.as_str(),
        "oauthRedirectUri" => param_names::OAUTH_REDIRECT_URI.as_str(),
        "oauthScope" => param_names::OAUTH_SCOPE.as_str(),
        "enableSingleUseRefreshTokens" => {
            param_names::OAUTH_ENABLE_SINGLE_USE_REFRESH_TOKENS.as_str()
        }
        "loginTimeout" => "adbc.snowflake.sql.client_option.login_timeout",
        "requestTimeout" => "adbc.snowflake.sql.client_option.request_timeout",
        "clientTimeout" => "adbc.snowflake.sql.client_option.client_timeout",
        "jwtTimeout" => "adbc.snowflake.sql.client_option.jwt_expire_timeout",
        "maxRetryCount" => MAX_RETRY_COUNT,
        "serverSessionKeepAlive" => "adbc.snowflake.sql.client_option.keep_session_alive",
        "application" => "adbc.snowflake.sql.client_option.application",
        "token" => "adbc.snowflake.sql.client_option.auth_token",
        "tokenFilePath" => param_names::TOKEN_FILE_PATH.as_str(),
        "privateKey" | "private_key" => {
            "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value"
        }
        "private_key_file" => "adbc.snowflake.sql.client_option.jwt_private_key",
        "workloadIdentityProvider" => "adbc.snowflake.sql.client_option.identity_provider",
        "workloadIdentityEntraResource" => param_names::WORKLOAD_IDENTITY_ENTRA_RESOURCE.as_str(),
        "workloadIdentityImpersonationPath" => {
            param_names::WORKLOAD_IDENTITY_IMPERSONATION_PATH.as_str()
        }
        "workloadIdentityAwsUseOutboundToken" => {
            param_names::WORKLOAD_IDENTITY_AWS_USE_OUTBOUND_TOKEN.as_str()
        }
        "workloadIdentityHost" => param_names::WIF_HOST.as_str(),
        "validateDefaultParameters" => {
            "adbc.snowflake.sql.client_option.validate_default_parameters"
        }
        "clientStoreTemporaryCredential" => STORE_TEMP_CREDS,
        "logQueryText" => param_names::LOG_QUERY_TEXT.as_str(),
        "logQueryParameters" => param_names::LOG_QUERY_PARAMETERS.as_str(),
        "disableQueryContextCache" => param_names::DISABLE_QUERY_CONTEXT_CACHE.as_str(),
        "includeRetryReason" => param_names::INCLUDE_RETRY_REASON.as_str(),
        "disableSamlURLCheck" => param_names::DISABLE_SAML_URL_CHECK.as_str(),
        "certRevocationCheckMode" => param_names::CRL_CHECK_MODE.as_str(),
        "crlAllowCertificatesWithoutCrlURL" => {
            param_names::CRL_ALLOW_CERTIFICATES_WITHOUT_CRL_URL.as_str()
        }
        "crlInMemoryCacheDisabled" => param_names::CRL_ENABLE_MEMORY_CACHING.as_str(),
        "crlOnDiskCacheDisabled" => param_names::CRL_ENABLE_DISK_CACHING.as_str(),
        "crlDownloadMaxSize" => param_names::CRL_MAX_DOWNLOAD_SIZE.as_str(),
        "crlHttpClientTimeout" => param_names::CRL_HTTP_TIMEOUT.as_str(),
        "connectionDiagnosticsEnabled" => param_names::ENABLE_CONNECTION_DIAG.as_str(),
        "connectionDiagnosticsAllowlistFile" => {
            param_names::CONNECTION_DIAG_ALLOWLIST_PATH.as_str()
        }
        "proxyHost" => "adbc.snowflake.sql.client_option.proxy_host",
        "proxyPort" => "adbc.snowflake.sql.client_option.proxy_port",
        "proxyUser" => "adbc.snowflake.sql.client_option.proxy_user",
        "proxyPassword" => "adbc.snowflake.sql.client_option.proxy_password",
        "proxyProtocol" => "adbc.snowflake.sql.client_option.proxy_protocol",
        "noProxy" => param_names::NO_PROXY.as_str(),
        "singleAuthenticationPrompt"
        | "region"
        | "jwtClientTimeout"
        | "externalBrowserTimeout"
        | "cloudStorageTimeout"
        | "cleanupTimeout"
        | "disableOCSPChecks"
        | "ocspFailOpen"
        | "tlsConfigName"
        | "clientRequestMfaToken"
        | "tracing"
        | "tmpDirPath"
        | "clientConfigFile"
        | "disableConsoleLogin" => return Err(unsupported_uri_config(key)),
        _ => return Ok(None),
    };
    Ok(Some(option))
}

/// Decode a DSN component with Go's url.QueryUnescape semantics, rejecting
/// malformed percent escapes and invalid UTF-8.
fn decode_uri_component<'a>(value: &'a str, what: &str) -> Result<std::borrow::Cow<'a, str>> {
    let bytes = value.as_bytes();
    if !bytes.iter().any(|&byte| matches!(byte, b'+' | b'%')) {
        return Ok(std::borrow::Cow::Borrowed(value));
    }
    let invalid_escape = || {
        SnowflakeError::invalid_argument()
            .context("decode URI component")
            .format(format_args!("invalid percent escape in {what}"))
            .to_adbc()
    };
    let hex = |byte: u8| match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    };
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => decoded.push(b' '),
            b'%' => {
                let high = bytes.get(index + 1).and_then(|&byte| hex(byte));
                let low = bytes.get(index + 2).and_then(|&byte| hex(byte));
                decoded.push(
                    (high.ok_or_else(invalid_escape)? << 4) | low.ok_or_else(invalid_escape)?,
                );
                index += 2;
            }
            byte => decoded.push(byte),
        }
        index += 1;
    }
    String::from_utf8(decoded)
        .map(std::borrow::Cow::Owned)
        .map_err(|e| {
            SnowflakeError::invalid_argument()
                .context("decode URI component")
                .format(format_args!("invalid UTF-8 in {what}: {e}"))
                .into()
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
            if name == TRACE_PARENT {
                let parent = database_option_as_string(name, &value)?;
                validate_trace_parent(parent)?;
                post_connect_options.push((key, value));
                continue;
            }
            match adbc_db_opt_to_sf(name, &value) {
                Ok(Some((param, setting))) => {
                    sf_options.insert(param.to_string(), setting);
                }
                Ok(None)
                | Err(Error {
                    status: Status::NotImplemented,
                    ..
                }) => {
                    return Err(SnowflakeError::set_unknown_option(&name)
                        .context("set connection option")
                        .into());
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
        Err(SnowflakeError::invalid_argument()
            .context("validate connection options")
            .message(errors.join("; "))
            .into())
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
        self.inner
            .configure_tracing(self.traces_exporter, self.traces_folder_path.as_deref())?;
        self.inner
            .traced("databaseImpl.Open", self.trace_parent.as_deref(), |_| {
                let AccumulatedConnectionOptions {
                    mut sf_options,
                    post_connect_options,
                    mut no_connection_details,
                } = accumulate_connection_options(&self.sf_settings, opts)?;
                no_connection_details &= self.session_parameters.is_empty();
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
                if self.auth_type == Some("auth_okta")
                    && self.okta_url.as_deref().unwrap_or("").is_empty()
                {
                    return Err(SnowflakeError::invalid_argument()
                        .context("open connection")
                        .format(format_args!(
                            "database option '{OKTA_URL}' is required for '{AUTH_TYPE}=auth_okta'"
                        ))
                        .into());
                }
                let conn_handle = self.inner.sf.connection_new();

                let setup = (|| -> Result<()> {
                    // Batch all pre-connect options in one validation/resolution pass.
                    let warnings = self
                        .inner
                        .block_on(self.inner.sf.connection_set_options(
                            conn_handle,
                            sf_options,
                            no_connection_details,
                            None,
                        ))
                        .map_err(crate::error::api_error_to_adbc_error)?;
                    let mut seen_warnings = HashSet::new();
                    process_validation_issues(
                        warnings,
                        &mut seen_warnings,
                        &self.surfaced_warnings,
                    )?;

                    self.inner
                        .block_on(
                            self.inner
                                .sf
                                .set_wrapper_identity(conn_handle, wrapper_identity()),
                        )
                        .map_err(crate::error::api_error_to_adbc_error)?;

                    // Request Arrow results in the login payload. ARROW_FORCE (rather
                    // than ARROW) is what makes the backend answer this connector ID
                    // with Arrow instead of JSON, so no post-login ALTER SESSION
                    // round trip is needed.
                    self.inner
                        .block_on(self.inner.sf.connection_set_session_parameters(
                            conn_handle,
                            self.login_session_parameters(),
                        ))
                        .map_err(crate::error::api_error_to_adbc_error)?;

                    self.inner
                        .block_on(
                            self.inner
                                .sf
                                .connection_init(None, conn_handle, self.db_handle),
                        )
                        .map_err(crate::error::api_error_to_adbc_error)
                })();

                if let Err(error) = setup {
                    // Initialization may have established a partial session. Attempt
                    // close before release, while still guaranteeing release on error.
                    cleanup_connection_handle(
                        &self.inner,
                        conn_handle,
                        self.trace_parent.as_deref(),
                    );
                    return Err(error);
                }

                let mut conn = Connection {
                    inner: self.inner.clone(),
                    conn_handle,
                    autocommit: true,
                    trace_parent: self.trace_parent.clone(),
                    match_exactly: false,
                    timestamp_precision: self.timestamp_precision,
                    geography_output_format: self.geography_output_format,
                    geometry_output_format: self.geometry_output_format,
                    operation: std::sync::Arc::new(std::sync::Mutex::new(std::sync::Arc::new(
                        sf_core::apis::operation_ctx::OperationCtx::with_own_token(),
                    ))),
                };
                // From this point Connection::drop owns cleanup on every error path.
                for (key, value) in post_connect_options {
                    conn.set_option(key, value)?;
                }

                Ok(conn)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::{
        Connection as _, Database as _, Driver as _, Statement as _,
        options::{OptionDatabase, OptionValue},
    };
    use arrow_array::cast::AsArray;
    use sf_core::config::param_registry::param_names;

    fn make_db() -> Database {
        let mut driver = crate::driver::Driver::default();
        driver.new_database().unwrap()
    }

    #[test]
    fn telemetry_options_round_trip_and_reject_invalid_values() {
        let mut db = make_db();
        let parent_key = OptionDatabase::Other(TRACE_PARENT.into());
        let exporter_key = OptionDatabase::Other(TRACES_EXPORTER.into());
        let folder_key = OptionDatabase::Other(TRACES_FOLDER_PATH.into());
        let parent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";

        assert_eq!(db.get_option_string(parent_key.clone()).unwrap(), "");
        db.set_option(parent_key.clone(), OptionValue::String(parent.into()))
            .unwrap();
        assert_eq!(db.get_option_string(parent_key.clone()).unwrap(), parent);
        for value in [OptionValue::String("invalid".into()), OptionValue::Int(1)] {
            assert_eq!(
                db.set_option(parent_key.clone(), value).unwrap_err().status,
                Status::InvalidArguments,
            );
            assert_eq!(db.get_option_string(parent_key.clone()).unwrap(), parent);
        }
        db.set_option(parent_key.clone(), OptionValue::String(String::new()))
            .unwrap();
        assert_eq!(db.get_option_string(parent_key).unwrap(), "");

        assert_eq!(db.get_option_string(exporter_key.clone()).unwrap(), "");
        db.set_option(exporter_key.clone(), OptionValue::String("adbcfile".into()))
            .unwrap();
        assert_eq!(
            db.get_option_string(exporter_key.clone()).unwrap(),
            "adbcfile"
        );
        for value in [OptionValue::String("invalid".into()), OptionValue::Int(1)] {
            assert_eq!(
                db.set_option(exporter_key.clone(), value)
                    .unwrap_err()
                    .status,
                Status::InvalidArguments,
            );
            assert_eq!(
                db.get_option_string(exporter_key.clone()).unwrap(),
                "adbcfile"
            );
        }
        // A blank exporter unsets the option so OTEL_TRACES_EXPORTER applies.
        db.set_option(exporter_key.clone(), OptionValue::String("  ".into()))
            .unwrap();
        assert_eq!(db.traces_exporter, None);
        assert_eq!(db.get_option_string(exporter_key.clone()).unwrap(), "");

        db.set_option(
            folder_key.clone(),
            OptionValue::String("/tmp/traces".into()),
        )
        .unwrap();
        assert_eq!(
            db.get_option_string(folder_key.clone()).unwrap(),
            "/tmp/traces"
        );
        assert_eq!(
            db.set_option(folder_key.clone(), OptionValue::Int(1))
                .unwrap_err()
                .status,
            Status::InvalidArguments
        );
        db.set_option(folder_key.clone(), OptionValue::String(String::new()))
            .unwrap();
        assert_eq!(db.get_option_string(folder_key).unwrap(), "");
        assert!(db.sf_settings.is_empty());
    }

    #[test]
    fn trace_parent_connection_option_is_validated_before_connect() {
        let parent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        let option = OptionConnection::Other(TRACE_PARENT.into());
        let accumulated = accumulate_connection_options(
            &HashMap::new(),
            [(option.clone(), OptionValue::String(parent.into()))],
        )
        .unwrap();
        assert!(accumulated.sf_options.is_empty());
        assert!(matches!(
            &accumulated.post_connect_options[..],
            [(OptionConnection::Other(key), OptionValue::String(value))]
                if key == TRACE_PARENT && value == parent
        ));
        for value in [OptionValue::String("invalid".into()), OptionValue::Int(1)] {
            let err = accumulate_connection_options(&HashMap::new(), [(option.clone(), value)])
                .err()
                .expect("invalid trace_parent");
            assert_eq!(err.status, Status::InvalidArguments);
        }
    }

    #[test]
    fn missing_required_option_fails_connect_with_invalid_arguments() {
        use adbc_core::Database as _;
        // connection_init validates the merged database + connection settings
        // before any network call, so this fails without a Snowflake account.
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Username,
            OptionValue::String("alice".into()),
        )
        .unwrap();
        db.set_option(
            OptionDatabase::Password,
            OptionValue::String("secret".into()),
        )
        .unwrap();
        let error = db.new_connection().err().expect("account is required");
        assert_eq!(error.status, Status::InvalidArguments, "{error:?}");
        assert!(error.message.contains("account"), "{error:?}");
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
                let expected = match &value {
                    OptionValue::String(value) => format!(
                        "[Snowflake] could not set database option: option '{key}': invalid value '{value}' (must be 'EWKB' or 'GeoJSON')"
                    ),
                    other => format!(
                        "[Snowflake] could not set database option: could not invalid option \"{key}\"={other:?}: must be a string"
                    ),
                };
                let err = db.set_option(option.clone(), value).unwrap_err();
                assert_eq!(err.status, Status::InvalidArguments);
                assert_eq!(err.message, expected);
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
        assert_eq!(
            error.message,
            "[Snowflake] could not set connection option: unknown option \"adbc.fake\""
        );
    }

    #[test]
    fn connection_max_retry_count_uses_database_retry_conversion() {
        for (retries, attempts) in [("0", 8), ("2", 3)] {
            let accumulated = accumulate_connection_options(
                &HashMap::new(),
                [(
                    OptionConnection::Other(MAX_RETRY_COUNT.into()),
                    OptionValue::String(retries.into()),
                )],
            )
            .unwrap();
            assert_eq!(
                accumulated
                    .sf_options
                    .get(param_names::RETRY_MAX_ATTEMPTS.as_str()),
                Some(&Setting::Int(attempts)),
                "{retries}"
            );
        }
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
    fn unsupported_go_and_adbc_options_are_not_stored() {
        let mut db = make_db();
        for key in [
            "adbc.snowflake.sql.region",
            "adbc.snowflake.sql.client_option.ocsp_fail_open_mode",
            "adbc.snowflake.sql.client_option.tracing",
            "adbc.snowflake.sql.client_option.config_file",
            "adbc.snowflake.sql.client_option.use_high_precision",
            "adbc.snowflake.sql.client_option.stream_retry_enabled",
            "adbc.connection.get_objects.match_exactly",
            "adbc.some.unknown.database.option",
            "ADBC.some.unknown.database.option",
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
                format!("[Snowflake] could not set database option: unknown option \"{key}\"")
            );
            assert!(!db.sf_settings.contains_key(key));
            for missing in [
                db.get_option_string(OptionDatabase::Other(key.into()))
                    .unwrap_err(),
                db.get_option_bytes(OptionDatabase::Other(key.into()))
                    .unwrap_err(),
                db.get_option_int(OptionDatabase::Other(key.into()))
                    .unwrap_err(),
                db.get_option_double(OptionDatabase::Other(key.into()))
                    .unwrap_err(),
            ] {
                assert_eq!(missing.status, Status::NotFound);
                assert_eq!(
                    missing.message,
                    format!("[Snowflake] could not get database option: unknown option \"{key}\"")
                );
            }
        }
        assert!(db.sf_settings.is_empty());
    }

    #[test]
    fn session_parameters_round_trip_and_reject_non_string_values() {
        let mut db = make_db();
        let option = OptionDatabase::Other("WEEK_START".into());
        assert_eq!(
            db.get_option_string(option.clone()).unwrap_err().status,
            Status::NotFound
        );
        db.set_option(option.clone(), OptionValue::String("3".into()))
            .unwrap();
        assert_eq!(db.get_option_string(option.clone()).unwrap(), "3");
        assert_eq!(
            db.session_parameters.get("WEEK_START").map(String::as_str),
            Some("3")
        );
        assert!(db.sf_settings.is_empty());
        for value in [
            OptionValue::Int(2),
            OptionValue::Double(2.0),
            OptionValue::Bytes(vec![2]),
        ] {
            assert_eq!(
                db.set_option(option.clone(), value).unwrap_err().status,
                Status::InvalidArguments
            );
            assert_eq!(db.get_option_string(option.clone()).unwrap(), "3");
        }
        db.set_option(option.clone(), OptionValue::String("1".into()))
            .unwrap();
        assert_eq!(db.get_option_string(option).unwrap(), "1");
    }

    #[test]
    fn driver_owned_login_parameters_override_user_values_case_insensitively() {
        let mut db = make_db();
        for (name, value) in [
            ("python_connector_query_result_format", "JSON"),
            ("Geography_Output_Format", "EWKB"),
            ("geometry_output_format", "EWKB"),
            ("WEEK_START", "3"),
        ] {
            db.set_option(
                OptionDatabase::Other(name.into()),
                OptionValue::String(value.into()),
            )
            .unwrap();
        }
        db.set_option(
            OptionDatabase::Other(GEOMETRY_OUTPUT_FORMAT.into()),
            OptionValue::String("EWKB".into()),
        )
        .unwrap();

        let params = db.login_session_parameters();
        assert_eq!(
            params["PYTHON_CONNECTOR_QUERY_RESULT_FORMAT"],
            "ARROW_FORCE"
        );
        assert_eq!(params["GEOGRAPHY_OUTPUT_FORMAT"], "GEOJSON");
        assert_eq!(params["GEOMETRY_OUTPUT_FORMAT"], "EWKB");
        assert_eq!(params["WEEK_START"], "3");
        assert_eq!(params.len(), 4);
        assert_eq!(
            db.get_option_string(OptionDatabase::Other("Geography_Output_Format".into()))
                .unwrap(),
            "EWKB"
        );
    }

    #[test]
    fn later_database_option_overrides_same_uri_session_parameter() {
        let mut db = make_db();
        let option = OptionDatabase::Other("QUERY_TAG".into());
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("snowflake://myaccount?QUERY_TAG=from-uri".into()),
        )
        .unwrap();
        assert_eq!(db.get_option_string(option.clone()).unwrap(), "from-uri");
        db.set_option(option.clone(), OptionValue::String("from-option".into()))
            .unwrap();
        assert_eq!(db.get_option_string(option).unwrap(), "from-option");
        assert!(!db.session_parameters.contains_key("QUERY_TAG"));
        assert_eq!(
            db.sf_settings.get(param_names::QUERY_TAG.as_str()),
            Some(&Setting::String("from-option".into()))
        );
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
    fn jwt_expire_timeout_sets_whole_seconds_and_zero_selects_default() {
        let key = "adbc.snowflake.sql.client_option.jwt_expire_timeout";
        for (value, seconds) in [
            ("1m30s", 90),
            ("1500ms", 2),
            ("0s", DEFAULT_JWT_TIMEOUT_SECS),
        ] {
            let mut db = make_db();
            db.set_option(
                OptionDatabase::Other(key.into()),
                OptionValue::String(value.into()),
            )
            .unwrap();
            assert_eq!(
                db.sf_settings.get(param_names::JWT_TIMEOUT.as_str()),
                Some(&Setting::Int(seconds)),
                "{value}"
            );
            assert_eq!(
                db.get_option_string(OptionDatabase::Other(key.into()))
                    .unwrap(),
                value
            );
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
            let expected = match &invalid {
                OptionValue::String(value) => format!(
                    "[Snowflake] could not set database option: option '{MAX_TIMESTAMP_PRECISION}': invalid value '{value}'"
                ),
                other => format!(
                    "[Snowflake] could not set database option: could not invalid option \"{MAX_TIMESTAMP_PRECISION}\"={other:?}: must be a string"
                ),
            };
            let error = db.set_option(option.clone(), invalid).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
            assert_eq!(error.message, expected);
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
    fn non_negative_integer_options_accept_strings_and_ints() {
        let key = "adbc.snowflake.sql.uri.port";
        for (value, expected) in [
            (OptionValue::String("443".into()), 443),
            (OptionValue::String("+443".into()), 443),
            (OptionValue::Int(443), 443),
        ] {
            assert_eq!(non_negative_int(key, &value).unwrap(), expected);
        }
        for value in [
            OptionValue::String("abc".into()),
            OptionValue::Int(-1),
            OptionValue::Double(443.0),
        ] {
            assert_eq!(
                non_negative_int(key, &value).unwrap_err().status,
                Status::InvalidArguments
            );
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
    fn uri_account_with_port_splits_port_without_explicit_account() {
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("snowflake://user:pass@myorg-account1:443/db".into()),
        )
        .unwrap();
        assert_eq!(
            db.sf_settings.get(param_names::ACCOUNT.as_str()).unwrap(),
            &Setting::String("myorg-account1".into())
        );
        assert_eq!(
            db.sf_settings.get(param_names::PORT.as_str()).unwrap(),
            &Setting::Int(443)
        );
        assert!(!db.sf_settings.contains_key(param_names::HOST.as_str()));
    }

    #[test]
    fn uri_snowflake_hostname_sets_host_and_account_from_first_label() {
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("user:pass@lgyanbp-im41116.snowflakecomputing.com/db/sc".into()),
        )
        .unwrap();
        assert_eq!(
            db.sf_settings.get(param_names::HOST.as_str()),
            Some(&Setting::String(
                "lgyanbp-im41116.snowflakecomputing.com".into()
            ))
        );
        assert_eq!(
            db.sf_settings.get(param_names::ACCOUNT.as_str()),
            Some(&Setting::String("lgyanbp-im41116".into()))
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
    fn uri_query_unescapes_user_password_path_and_query_values() {
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String(
                "snowflake://a+lice:p+ss%2Bword@myaccount/MY+DB/S%2B+X?%77arehouse=WH+1%2B2".into(),
            ),
        )
        .unwrap();
        for (key, expected) in [
            (param_names::USER.as_str(), "a lice"),
            (param_names::PASSWORD.as_str(), "p ss+word"),
            (param_names::DATABASE.as_str(), "MY DB"),
            (param_names::SCHEMA.as_str(), "S+ X"),
            (param_names::WAREHOUSE.as_str(), "WH 1+2"),
        ] {
            assert_eq!(
                db.sf_settings.get(key),
                Some(&Setting::String(expected.to_owned())),
                "{key}"
            );
        }
        let error = make_db()
            .set_option(
                OptionDatabase::Uri,
                OptionValue::String("snowflake://alice:bad%2@myaccount".into()),
            )
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
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

    #[test]
    fn uri_query_splits_go_config_and_session_parameters() {
        let mut db = make_db();
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String(
                "snowflake://myaccount/PATH_DB?database=QUERY_DB&schema=SC&warehouse=WH\
                 &application=ADBC_APP&loginTimeout=15&requestTimeout=30&maxRetryCount=2&jwtTimeout=45\
                 &serverSessionKeepAlive=true&validateDefaultParameters=false\
                 &clientStoreTemporaryCredential=true&proxyHost=proxy.example\
                 &noProxy=localhost&workloadIdentityProvider=AWS&oauthClientId=client-id\
                 &passcodeInPassword=true&crlInMemoryCacheDisabled=true\
                 &certRevocationCheckMode=advisory&TIMEZONE=America%2FLos_Angeles\
                 &WEEK_START=3&%71uery_tag=hello+world"
                    .into(),
            ),
        )
        .unwrap();
        for (key, expected) in [
            (
                param_names::DATABASE.as_str(),
                Setting::String("QUERY_DB".into()),
            ),
            (param_names::SCHEMA.as_str(), Setting::String("SC".into())),
            (
                param_names::WAREHOUSE.as_str(),
                Setting::String("WH".into()),
            ),
            (
                param_names::APPLICATION.as_str(),
                Setting::String("ADBC_APP".into()),
            ),
            (param_names::LOGIN_TIMEOUT.as_str(), Setting::Int(15)),
            (param_names::REQUEST_TIMEOUT.as_str(), Setting::Int(30)),
            (param_names::JWT_TIMEOUT.as_str(), Setting::Int(45)),
            (param_names::RETRY_MAX_ATTEMPTS.as_str(), Setting::Int(3)),
            (
                param_names::SERVER_SESSION_KEEP_ALIVE.as_str(),
                Setting::Bool(true),
            ),
            (
                param_names::VALIDATE_DEFAULT_PARAMETERS.as_str(),
                Setting::Bool(false),
            ),
            (
                param_names::CLIENT_STORE_TEMPORARY_CREDENTIAL.as_str(),
                Setting::Bool(true),
            ),
            (
                param_names::PROXY_HOST.as_str(),
                Setting::String("proxy.example".into()),
            ),
            (
                param_names::NO_PROXY.as_str(),
                Setting::String("localhost".into()),
            ),
            (
                param_names::WORKLOAD_IDENTITY_PROVIDER.as_str(),
                Setting::String("AWS".into()),
            ),
            (
                param_names::OAUTH_CLIENT_ID.as_str(),
                Setting::String("client-id".into()),
            ),
            (
                param_names::PASSCODE_IN_PASSWORD.as_str(),
                Setting::Bool(true),
            ),
            (
                param_names::CRL_ENABLE_MEMORY_CACHING.as_str(),
                Setting::Bool(false),
            ),
            (
                param_names::CRL_CHECK_MODE.as_str(),
                Setting::String("advisory".into()),
            ),
        ] {
            assert_eq!(db.sf_settings.get(key), Some(&expected), "{key}");
        }
        assert_eq!(
            db.session_parameters,
            HashMap::from([
                ("TIMEZONE".into(), "America/Los_Angeles".into()),
                ("WEEK_START".into(), "3".into()),
                ("query_tag".into(), "hello world".into()),
            ])
        );
        assert_eq!(
            db.get_option_string(OptionDatabase::Other("TIMEZONE".into()))
                .unwrap(),
            "America/Los_Angeles"
        );
        assert_eq!(db.login_session_parameters()["QUERY_TAG"], "hello world");
        assert_eq!(
            db.get_option_string(OptionDatabase::Other("query_tag".into()))
                .unwrap(),
            "hello world"
        );
    }

    #[test]
    fn session_parameter_names_are_case_insensitive_and_last_set_wins() {
        let mut db = make_db();
        for (key, value) in [
            ("MY_SESSION_PARAM", "first"),
            ("my_session_param", "second"),
        ] {
            db.set_option(
                OptionDatabase::Other(key.into()),
                OptionValue::String(value.into()),
            )
            .unwrap();
        }
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String("snowflake://myaccount?week_start=1&WEEK_START=3".into()),
        )
        .unwrap();
        let login = db.login_session_parameters();
        assert_eq!(login["MY_SESSION_PARAM"], "second");
        assert_eq!(login["WEEK_START"], "3");
        assert_eq!(db.session_parameters.len(), 2);
        assert_eq!(
            db.get_option_string(OptionDatabase::Other("My_Session_Param".into()))
                .unwrap(),
            "second"
        );
    }

    #[test]
    fn uri_rejects_unsupported_go_configuration_without_forwarding_it() {
        for key in [
            "region",
            "singleAuthenticationPrompt",
            "jwtClientTimeout",
            "disableOCSPChecks",
            "ocspFailOpen",
            "clientRequestMfaToken",
            "tracing",
            "clientConfigFile",
        ] {
            let mut db = make_db();
            let error = db
                .set_option(
                    OptionDatabase::Uri,
                    OptionValue::String(format!("snowflake://myaccount?{key}=true")),
                )
                .unwrap_err();
            assert_eq!(error.status, Status::NotImplemented, "{key}: {error:?}");
            assert!(error.message.contains(key), "{key}: {error:?}");
            assert!(db.session_parameters.is_empty(), "{key}");
        }
        let mut db = make_db();
        let error = db
            .set_option(
                OptionDatabase::Uri,
                OptionValue::String("snowflake://myaccount?adbc.some.option=value".into()),
            )
            .unwrap_err();
        assert_eq!(error.status, Status::NotImplemented);
        assert!(db.session_parameters.is_empty());
        let error = db
            .set_option(
                OptionDatabase::Uri,
                OptionValue::String("snowflake://myaccount?SOME_PARAM=%GG".into()),
            )
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }

    fn live_db() -> Option<Database> {
        let uri = std::env::var("SNOWFLAKE_URI")
            .ok()
            .filter(|uri| !uri.is_empty())?;
        let mut db = make_db();
        db.set_option(OptionDatabase::Uri, OptionValue::String(uri))
            .unwrap();
        for (env_key, db_key) in [
            ("SNOWFLAKE_DATABASE", "adbc.snowflake.sql.db"),
            ("SNOWFLAKE_SCHEMA", "adbc.snowflake.sql.schema"),
        ] {
            if let Some(value) = std::env::var(env_key)
                .ok()
                .filter(|value| !value.is_empty())
            {
                db.set_option(
                    OptionDatabase::Other(db_key.into()),
                    OptionValue::String(value),
                )
                .unwrap();
            }
        }
        Some(db)
    }

    fn show_session_parameter(conn: &mut Connection, name: &str) -> Option<String> {
        let mut statement = conn.new_statement().unwrap();
        statement
            .set_sql_query(format!("SHOW PARAMETERS LIKE '{name}' IN SESSION"))
            .unwrap();
        for batch in statement.execute().unwrap() {
            let batch = batch.unwrap();
            let keys = batch.column(0).as_string::<i32>();
            let values = batch.column(1).as_string::<i32>();
            for index in 0..batch.num_rows() {
                if keys.value(index) == name {
                    return Some(values.value(index).into());
                }
            }
        }
        None
    }

    #[test]
    fn live_database_session_parameters_apply_at_login() {
        let Some(mut db) = live_db() else {
            eprintln!("Skipping: SNOWFLAKE_URI not set");
            return;
        };
        for (key, value) in [
            ("WEEK_START", "3"),
            ("QUERY_TAG", "adbc-rust-session-test"),
            ("TIMEZONE", "America/Los_Angeles"),
        ] {
            db.set_option(
                OptionDatabase::Other(key.into()),
                OptionValue::String(value.into()),
            )
            .unwrap();
        }
        let mut conn = db.new_connection().unwrap();
        assert_eq!(
            show_session_parameter(&mut conn, "WEEK_START").as_deref(),
            Some("3")
        );
        assert_eq!(
            show_session_parameter(&mut conn, "QUERY_TAG").as_deref(),
            Some("adbc-rust-session-test")
        );
        assert_eq!(
            show_session_parameter(&mut conn, "TIMEZONE").as_deref(),
            Some("America/Los_Angeles")
        );
    }

    #[test]
    fn live_uri_session_parameter_applies_at_login() {
        let Some(mut db) = live_db() else {
            eprintln!("Skipping: SNOWFLAKE_URI not set");
            return;
        };
        let uri = std::env::var("SNOWFLAKE_URI").unwrap();
        let separator = if uri.contains('?') { '&' } else { '?' };
        db.set_option(
            OptionDatabase::Uri,
            OptionValue::String(format!("{uri}{separator}TIMEZONE=America%2FChicago")),
        )
        .unwrap();
        let mut conn = db.new_connection().unwrap();
        assert_eq!(
            show_session_parameter(&mut conn, "TIMEZONE").as_deref(),
            Some("America/Chicago")
        );
    }

    #[test]
    fn live_database_option_set_after_connect_only_affects_future_connections() {
        let Some(mut db) = live_db() else {
            eprintln!("Skipping: SNOWFLAKE_URI not set");
            return;
        };
        let mut first = db.new_connection().unwrap();
        let original = show_session_parameter(&mut first, "DATE_OUTPUT_FORMAT");
        db.set_option(
            OptionDatabase::Other("DATE_OUTPUT_FORMAT".into()),
            OptionValue::String("DD/MM/YYYY".into()),
        )
        .unwrap();
        assert_eq!(
            show_session_parameter(&mut first, "DATE_OUTPUT_FORMAT"),
            original
        );
        let mut second = db.new_connection().unwrap();
        assert_eq!(
            show_session_parameter(&mut second, "DATE_OUTPUT_FORMAT").as_deref(),
            Some("DD/MM/YYYY")
        );
    }
}
