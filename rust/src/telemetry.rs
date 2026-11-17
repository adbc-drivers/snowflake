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

//! Bridge sf_core's in-band Snowflake spans to driverbase's local trace exporter.
//! sf_core's OpenTelemetry layer and our driverbase layer cannot both insert
//! `OtelData` into the same tracing span. Only sf_core's layer observes its own
//! spans; this mirror exports a separate copy of each completed builder.

use std::fmt::{self, Write as _};
use std::fs::{File, OpenOptions};
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use adbc_core::error::{Error, Result};
use driverbase::error::ErrorHelper;
use driverbase::telemetry::Telemetry;
use opentelemetry::KeyValue;
use opentelemetry_sdk::trace::SdkTracer;
use tracing::span::Id;
use tracing_opentelemetry::{OtelData, PreSampledTracer};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::{Context as LayerContext, Layer};
use tracing_subscriber::registry::Registry;
use tracing_subscriber::reload;

use crate::error::SnowflakeError;

pub(crate) struct LogLevelHandle {
    filter: reload::Handle<LevelFilter, Registry>,
    output: Arc<Mutex<Option<Arc<File>>>>,
    // Go permits an initial discovery attempt and then one explicit config.
    last_config_input: Mutex<Option<String>>,
}

impl LogLevelHandle {
    fn reload(&self, filter: LevelFilter) -> Result<()> {
        self.filter.reload(filter).map_err(|error| {
            SnowflakeError::internal_no_location()
                .format(format_args!(
                    "could not update Snowflake log level: {error}"
                ))
                .into()
        })
    }

    fn configure_level(&self, level: Option<&str>) -> Result<()> {
        // Every Go ADBC database starts with fatal. An explicitly empty tracing
        // string is different: gosnowflake skips SetLogLevel for that connection.
        let level = level.unwrap_or("fatal");
        if level.is_empty() {
            return Ok(());
        }
        let filter = log_level_filter(level).ok_or_else(|| {
            SnowflakeError::io()
                .format(format_args!(
                    "error while setting log level. unknown log level: {}",
                    level.to_uppercase(),
                ))
                .to_adbc()
        })?;
        self.reload(filter)
    }

    pub(crate) fn configure(&self, level: Option<&str>, client_config_file: &str) -> Result<()> {
        self.configure_level(level)?;
        let mut previous = self
            .last_config_input
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if previous
            .as_ref()
            .is_some_and(|input| !input.is_empty() || client_config_file.is_empty())
        {
            return Ok(());
        }
        let Some(config_path) = find_client_config(client_config_file)? else {
            *previous = Some(client_config_file.to_owned());
            return Ok(());
        };
        let (filter, folder) = read_client_logging_config(&config_path)?;
        let mut directories = std::fs::DirBuilder::new();
        directories.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            directories.mode(0o700);
        }
        directories.create(&folder).map_err(client_config_error)?;

        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o640);
        }
        // Go consumes this attempt even if opening the output file fails.
        *previous = Some(client_config_file.to_owned());
        let file = options
            .open(folder.join("snowflake.log"))
            .map_err(|error| {
                SnowflakeError::io()
                    .format(format_args!("{error}"))
                    .to_adbc()
            })?;
        *self.output.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(file));
        self.reload(filter)
    }
}

fn client_config_error(error: impl fmt::Display) -> Error {
    SnowflakeError::io()
        .vendor_code(260012)
        .format(format_args!("client configuration failed: {error}"))
        .into()
}

fn home_directory() -> Option<PathBuf> {
    std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn find_client_config(input: &str) -> Result<Option<PathBuf>> {
    if !input.is_empty() {
        return Ok(Some(PathBuf::from(input)));
    }
    if let Some(path) = std::env::var_os("SF_CLIENT_CONFIG_FILE").filter(|value| !value.is_empty())
    {
        return Ok(Some(path.into()));
    }
    let executable_dir = std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(Path::to_path_buf));
    for directory in [executable_dir, home_directory()].into_iter().flatten() {
        let file = directory.join("sf_client_config.json");
        match file.try_exists() {
            Ok(true) => return Ok(Some(file)),
            Ok(false) => {}
            Err(error) => return Err(client_config_error(error)),
        }
    }
    Ok(None)
}

fn read_client_logging_config(path: &Path) -> Result<(LevelFilter, PathBuf)> {
    let parse_error = |error: &dyn fmt::Display| {
        client_config_error(format_args!("parsing client config failed: {error}"))
    };
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path).map_err(|error| parse_error(&error))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = file.metadata().map_err(|error| parse_error(&error))?;
        if metadata.mode() & 0o022 != 0 {
            return Err(parse_error(&format_args!(
                "incorrect permissions of {}",
                path.display()
            )));
        }
        // SAFETY: getuid takes no pointers and only reads this process's real UID.
        if metadata.uid() != unsafe { libc::getuid() } {
            return Err(parse_error(&format_args!(
                "incorrect owner of {}",
                path.display()
            )));
        }
    }
    let mut contents = Vec::new();
    file.read_to_end(&mut contents)
        .map_err(|error| parse_error(&error))?;
    let config: serde_json::Value =
        serde_json::from_slice(&contents).map_err(|error| parse_error(&error))?;
    let common = config
        .as_object()
        .and_then(|object| {
            object
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case("common"))
        })
        .and_then(|(_, common)| common.as_object())
        .ok_or_else(|| parse_error(&"common section in client config not found"))?;
    let string_value = |name: &str| -> Result<&str> {
        match common
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value)
        {
            None | Some(serde_json::Value::Null) => Ok(""),
            Some(serde_json::Value::String(value)) => Ok(value.as_str()),
            Some(_) => Err(parse_error(&format_args!("{name} must be a string"))),
        }
    };
    let level = string_value("log_level")?;
    // Easy logging accepts OFF but not FATAL, unlike the tracing option.
    let filter = if level.is_empty() {
        LevelFilter::OFF
    } else {
        log_level_filter(level)
            .filter(|_| !level.eq_ignore_ascii_case("fatal"))
            .ok_or_else(|| parse_error(&format_args!("unknown log level: {level}")))?
    };
    let configured_path = string_value("log_path")?;
    let folder = if configured_path.is_empty() {
        home_directory()
            .ok_or_else(|| client_config_error("user home directory is not accessible"))?
    } else {
        PathBuf::from(configured_path)
    };
    // Go always appends "go", including for the literal log_path "STDOUT".
    Ok((filter, folder.join("go")))
}

struct LogWriter<W> {
    fallback: W,
    output: Arc<Mutex<Option<Arc<File>>>>,
}

enum LogWriterGuard<W> {
    Fallback(W),
    File(Arc<File>),
}

impl<W: std::io::Write> std::io::Write for LogWriterGuard<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Fallback(writer) => writer.write(bytes),
            Self::File(file) => (&**file).write(bytes),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Fallback(writer) => writer.flush(),
            Self::File(file) => (&**file).flush(),
        }
    }
}

impl<'a, W: tracing_subscriber::fmt::MakeWriter<'a>> tracing_subscriber::fmt::MakeWriter<'a>
    for LogWriter<W>
{
    type Writer = LogWriterGuard<W::Writer>;

    fn make_writer(&'a self) -> Self::Writer {
        let output = self.output.lock().unwrap_or_else(|e| e.into_inner());
        match output.as_ref() {
            Some(file) => LogWriterGuard::File(Arc::clone(file)),
            None => LogWriterGuard::Fallback(self.fallback.make_writer()),
        }
    }
}

struct GoLogFormat;

struct Escaped<'a, W>(&'a mut W);

impl<W: fmt::Write> fmt::Write for Escaped<'_, W> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        for character in text.chars() {
            match character {
                '"' => self.0.write_str("\\\"")?,
                '\\' => self.0.write_str("\\\\")?,
                '\n' => self.0.write_str("\\n")?,
                '\r' => self.0.write_str("\\r")?,
                '\t' => self.0.write_str("\\t")?,
                c if c.is_control() => write!(self.0, "\\u{:04x}", c as u32)?,
                c => self.0.write_char(c)?,
            }
        }
        Ok(())
    }
}

struct LogFields<'a, 'writer> {
    writer: &'a mut tracing_subscriber::fmt::format::Writer<'writer>,
    message: bool,
    written: bool,
    result: fmt::Result,
}

impl LogFields<'_, '_> {
    fn record(&mut self, field: &tracing::field::Field, value: fmt::Arguments<'_>, quoted: bool) {
        if (field.name() == "message") != self.message || self.result.is_err() {
            return;
        }
        self.written = true;
        self.result = (|| {
            if !self.message {
                write!(self.writer, " {}=", field.name())?;
            }
            if quoted {
                self.writer.write_char('"')?;
                Escaped(self.writer).write_fmt(value)?;
                self.writer.write_char('"')
            } else {
                self.writer.write_fmt(value)
            }
        })();
    }
}

impl tracing::field::Visit for LogFields<'_, '_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        self.record(field, format_args!("{value:?}"), true);
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.record(field, format_args!("{value}"), true);
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.record(field, format_args!("{value}"), false);
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.record(field, format_args!("{value}"), false);
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.record(field, format_args!("{value}"), false);
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.record(field, format_args!("{value}"), false);
    }
}

impl<N> tracing_subscriber::fmt::FormatEvent<Registry, N> for GoLogFormat
where
    N: for<'a> tracing_subscriber::fmt::FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &tracing_subscriber::fmt::FmtContext<'_, Registry, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> fmt::Result {
        let metadata = event.metadata();
        // slog prints its nonstandard TRACE (-8) as DEBUG-4.
        let level = if *metadata.level() == tracing::Level::TRACE {
            "DEBUG-4"
        } else {
            metadata.level().as_str()
        };
        let source = metadata
            .file()
            .and_then(|file| Path::new(file).file_name())
            .and_then(|name| name.to_str())
            .unwrap_or("unknown");
        write!(
            writer,
            "time={} level={level} source={source}:{} msg=",
            chrono::Local::now().format("%+"),
            metadata.line().unwrap_or(0),
        )?;
        let mut fields = LogFields {
            writer: &mut writer,
            message: true,
            written: false,
            result: Ok(()),
        };
        event.record(&mut fields);
        fields.result?;
        if !fields.written {
            fields.writer.write_str("\"\"")?;
        }
        fields.message = false;
        event.record(&mut fields);
        fields.result?;
        writeln!(writer)
    }
}

pub(crate) fn log_level_filter(level: &str) -> Option<LevelFilter> {
    [
        ("trace", LevelFilter::TRACE),
        ("debug", LevelFilter::DEBUG),
        ("info", LevelFilter::INFO),
        ("warn", LevelFilter::WARN),
        ("error", LevelFilter::ERROR),
        // tracing has no fatal events; ERROR is below Go's FATAL threshold.
        ("fatal", LevelFilter::OFF),
        ("off", LevelFilter::OFF),
    ]
    .into_iter()
    .find_map(|(name, filter)| level.eq_ignore_ascii_case(name).then_some(filter))
}

fn log_layer<W>(writer: W) -> (impl Layer<Registry> + Send + Sync + 'static, LogLevelHandle)
where
    W: for<'a> tracing_subscriber::fmt::MakeWriter<'a> + Send + Sync + 'static,
{
    // The Go ADBC driver's init overrides gosnowflake's package default to WARN.
    let (filter, handle) = reload::Layer::new(LevelFilter::WARN);
    let output = Arc::new(Mutex::new(None));
    let layer = tracing_subscriber::fmt::layer()
        .event_format(GoLogFormat)
        .with_ansi(false)
        .with_writer(LogWriter {
            fallback: writer,
            output: Arc::clone(&output),
        })
        .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
            matches!(metadata.target(), "sf_core" | "adbc_driver_snowflake")
                || metadata.target().starts_with("sf_core::")
                || metadata.target().starts_with("adbc_driver_snowflake::")
        }))
        .with_filter(filter);
    (
        layer,
        LogLevelHandle {
            filter: handle,
            output,
            last_config_input: Mutex::new(None),
        },
    )
}

struct CoreSpanMirror {
    tracer: SdkTracer,
    telemetry: Arc<Telemetry>,
}

struct CoreParentSet;

impl Layer<Registry> for CoreSpanMirror {
    fn on_enter(&self, id: &Id, ctx: LayerContext<'_, Registry>) {
        if !self.telemetry.is_enabled() {
            return;
        }
        let Some(span) = ctx.span(id) else {
            return;
        };
        if !span.metadata().target().starts_with("sf_core")
            || span.extensions().get::<CoreParentSet>().is_some()
        {
            return;
        }
        // sf_core's filtered OpenTelemetry layer cannot look up an ADBC parent
        // span. Bridge that parent via the unfiltered registry before the core
        // future is polled; the remote trace ID then reaches nested sf_core spans.
        let parent = span
            .parent()
            .and_then(|parent| parent.extensions().get::<OtelData>().cloned())
            .map(|mut data| self.tracer.sampled_context(&mut data));
        let mut extensions = span.extensions_mut();
        if let Some(parent) = parent
            && let Some(data) = extensions.get_mut::<OtelData>()
        {
            data.parent_cx = parent;
            data.builder.trace_id = None;
            data.builder.sampling_result = None;
        }
        extensions.insert(CoreParentSet);
    }

    fn on_close(&self, id: Id, ctx: LayerContext<'_, Registry>) {
        if !self.telemetry.is_enabled() {
            return;
        }
        let Some(span) = ctx.span(&id) else {
            return;
        };
        if !span.metadata().target().starts_with("sf_core") {
            return;
        }
        let data = span.extensions().get::<OtelData>().cloned();
        if let Some(mut data) = data {
            let operation = data.builder.name.to_string();
            let attributes = data.builder.attributes.get_or_insert_with(Vec::new);
            attributes.retain(|attribute| {
                let key = attribute.key.as_str();
                (key.eq_ignore_ascii_case("query_id")
                    || key.eq_ignore_ascii_case("snowflake.query.id"))
                    || !key.split(['.', '_']).any(|part| {
                        part.eq_ignore_ascii_case("sql") || part.eq_ignore_ascii_case("query")
                    })
            });
            attributes.push(KeyValue::new("db.system.name", "snowflake"));
            attributes.push(KeyValue::new("db.operation.name", operation));
            // sf_core events can contain server-provided SQL. Local traces only
            // need timing and safe metadata; leave its in-band lane untouched.
            data.builder.events = None;
            data.builder
                .start_with_context(&self.tracer, &data.parent_cx);
        }
    }
}

pub(crate) fn app_layer(
    telemetry: Arc<Telemetry>,
) -> (impl Layer<Registry> + Send + Sync + 'static, LogLevelHandle) {
    let app = telemetry.app_layer();
    let tracer = telemetry.tracer();
    let (logs, level) = log_layer(std::io::stderr);
    // Filter only fmt output. The mirror must see the unfiltered registry to
    // propagate ADBC parents, even when logging is off or below the span level.
    (
        app.and_then(CoreSpanMirror { tracer, telemetry })
            .and_then(logs),
        level,
    )
}

#[cfg(test)]
mod tests {
    use crate::driver::Inner;
    use driverbase::telemetry::Exporter;

    #[derive(Clone, Default)]
    struct CapturedLogs(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn initial_log_level_warn_includes_driver_and_core_targets() {
        use tracing_subscriber::prelude::*;

        let output = CapturedLogs::default();
        let (layer, _) = super::log_layer(output.clone());
        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            tracing::warn!(target: "sf_core", "initial core warning");
            tracing::warn!(target: "adbc_driver_snowflake", "initial driver warning");
            tracing::warn!("initial driver module warning");
            tracing::info!(target: "sf_core", "initial core info suppressed");
            tracing::info!("initial driver info suppressed");
            tracing::warn!(target: "adbc_driver_snowflake_other", "unrelated driver suppressed");
        });
        let bytes = output.0.lock().unwrap();
        let logs = std::str::from_utf8(&bytes).unwrap();
        assert!(logs.contains("initial core warning"));
        assert!(logs.contains("initial driver warning"));
        assert!(logs.contains("initial driver module warning"));
        assert!(!logs.contains("suppressed"));
        assert_eq!(logs.lines().count(), 3);
    }

    #[test]
    fn stderr_filter_reload_only_changes_log_output() {
        use tracing_subscriber::prelude::*;

        let output = CapturedLogs::default();
        let (layer, level) = super::log_layer(output.clone());
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            level.configure_level(None).unwrap();
            tracing::error!(target: "sf_core", "default fatal suppresses error");
            level.configure_level(Some("DEBUG")).unwrap();
            tracing::debug!(target: "sf_core::connection", query_id = "abc", "core debug is visible");
            tracing::info!("driver info is visible");
            tracing::error!(target: "driverbase", "ADBC error is suppressed");
            tracing::info!(target: "sf_core_other", "unrelated prefix is suppressed");
            tracing::trace!(target: "sf_core", "trace is suppressed");
            level.configure_level(Some("off")).unwrap();
            tracing::error!(target: "sf_core", "off suppresses error");
        });
        let bytes = output.0.lock().unwrap();
        let logs = std::str::from_utf8(&bytes).unwrap();
        assert!(logs.contains("core debug is visible"));
        assert!(logs.contains("driver info is visible"));
        assert!(!logs.contains("ADBC error"));
        assert!(!logs.contains("unrelated prefix"));
        assert!(logs.starts_with("time="));
        assert!(logs.contains(" level=DEBUG source=telemetry.rs:"));
        assert!(logs.contains(" msg=\"core debug is visible\" query_id=\"abc\""));
        assert_eq!(logs.lines().count(), 2);
        assert!(!logs.contains("suppresses"));
        assert!(!logs.contains("trace is suppressed"));
    }

    #[test]
    fn default_reapplies_fatal_empty_preserves_level_and_invalid_fails_at_connect() {
        use tracing_subscriber::prelude::*;
        let output = CapturedLogs::default();
        let (layer, level) = super::log_layer(output.clone());
        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            level.configure_level(Some("trace")).unwrap();
            level.configure_level(Some("")).unwrap();
            tracing::trace!(target: "sf_core", "trace before reset");
            let error = level.configure_level(Some("warning")).unwrap_err();
            assert_eq!(error.status, adbc_core::error::Status::IO);
            assert!(
                error
                    .message
                    .contains("error while setting log level. unknown log level: WARNING")
            );
            tracing::debug!(target: "sf_core", "invalid level leaves existing filter");
            level.configure_level(None).unwrap();
            tracing::error!(target: "sf_core", "default fatal suppresses errors");
        });
        let bytes = output.0.lock().unwrap();
        let logs = std::str::from_utf8(&bytes).unwrap();
        assert!(logs.contains("level=DEBUG-4"));
        assert!(logs.contains("trace before reset"));
        assert!(logs.contains("invalid level leaves existing filter"));
        assert!(!logs.contains("default fatal"));
        for level in ["warning", "panic", " info ", ""] {
            assert!(super::log_level_filter(level).is_none());
        }
    }

    #[test]
    fn easy_logging_overrides_first_connect_then_keeps_file_but_reapplies_tracing() {
        use tracing_subscriber::prelude::*;
        let folder = std::env::temp_dir().join(format!("adbc-logging-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&folder).unwrap();
        let config = folder.join("client.json");
        std::fs::write(
            &config,
            serde_json::json!({"common": {"log_level": "DEBUG", "log_path": folder}}).to_string(),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let output = CapturedLogs::default();
        let (layer, level) = super::log_layer(output.clone());
        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            level.configure(None, config.to_str().unwrap()).unwrap();
            tracing::debug!(target: "sf_core", "easy logging overrides fatal once");
            level.configure(None, config.to_str().unwrap()).unwrap();
            tracing::error!(target: "sf_core", "next connect restores fatal");
            level.configure(Some("info"), "").unwrap();
            tracing::info!(target: "sf_core", "file destination survives default connection");
            // A different explicit file is ignored after an explicit file was tried.
            level
                .configure(Some("debug"), "/does-not-exist/client.json")
                .unwrap();
            tracing::debug!(target: "sf_core", "second file is ignored");
        });
        let logs = std::fs::read_to_string(folder.join("go/snowflake.log")).unwrap();
        assert!(logs.contains("easy logging overrides fatal once"));
        assert!(logs.contains("file destination survives default connection"));
        assert!(logs.contains("second file is ignored"));
        assert!(!logs.contains("next connect restores fatal"));
        assert!(output.0.lock().unwrap().is_empty());
        drop(level);
        std::fs::remove_dir_all(folder).unwrap();
    }

    #[test]
    fn easy_logging_rejects_bad_config_and_does_not_consume_retry() {
        use tracing_subscriber::prelude::*;
        let folder =
            std::env::temp_dir().join(format!("adbc-logging-errors-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&folder).unwrap();
        let config = folder.join("client.json");
        let output = CapturedLogs::default();
        let (layer, level) = super::log_layer(output);
        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            for contents in ["not JSON", "{}", r#"{"common":{"log_level":"fatal"}}"#] {
                std::fs::write(&config, contents).unwrap();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600))
                        .unwrap();
                }
                let error = level.configure(None, config.to_str().unwrap()).unwrap_err();
                assert_eq!(error.status, adbc_core::error::Status::IO);
                assert_eq!(error.vendor_code, 260012);
                assert!(error.message.contains("client configuration failed:"));
            }
            std::fs::write(
                &config,
                serde_json::json!({"common": {"log_path": folder}}).to_string(),
            )
            .unwrap();
            level
                .configure(Some("debug"), config.to_str().unwrap())
                .unwrap();
            tracing::error!(target: "sf_core", "missing easy log level selects off");
        });
        assert!(
            std::fs::read_to_string(folder.join("go/snowflake.log"))
                .unwrap()
                .is_empty()
        );
        drop(level);
        std::fs::remove_dir_all(folder).unwrap();
    }

    #[test]
    fn easy_logging_discovers_environment_then_allows_one_explicit_config() {
        use tracing_subscriber::prelude::*;
        const CHILD: &str = "ADBC_SNOWFLAKE_EASY_LOGGING_ENV_TEST";
        if std::env::var_os(CHILD).is_none() {
            let folder =
                std::env::temp_dir().join(format!("adbc-log-env-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&folder).unwrap();
            for name in ["environment", "explicit"] {
                std::fs::write(
                    folder.join(format!("{name}.json")),
                    serde_json::json!({"common": {"log_level": "DEBUG", "log_path": folder.join(name)}}).to_string(),
                ).unwrap();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(
                        folder.join(format!("{name}.json")),
                        std::fs::Permissions::from_mode(0o600),
                    )
                    .unwrap();
                }
            }
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "telemetry::tests::easy_logging_discovers_environment_then_allows_one_explicit_config", "--nocapture"])
                .env(CHILD, &folder)
                .env("SF_CLIENT_CONFIG_FILE", folder.join("environment.json"))
                .output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let first =
                std::fs::read_to_string(folder.join("environment/go/snowflake.log")).unwrap();
            let second = std::fs::read_to_string(folder.join("explicit/go/snowflake.log")).unwrap();
            assert!(first.contains("environment configuration"));
            assert!(!first.contains("explicit configuration"));
            assert!(second.contains("explicit configuration"));
            assert!(!second.contains("later default resets fatal"));
            std::fs::remove_dir_all(folder).unwrap();
            return;
        }
        let folder = std::path::PathBuf::from(std::env::var_os(CHILD).unwrap());
        let explicit = folder.join("explicit.json");
        assert_eq!(
            super::find_client_config(explicit.to_str().unwrap()).unwrap(),
            Some(explicit.clone()),
        );
        let (layer, level) = super::log_layer(CapturedLogs::default());
        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            level.configure(None, "").unwrap();
            tracing::debug!(target: "sf_core", "environment configuration");
            level.configure(None, explicit.to_str().unwrap()).unwrap();
            tracing::debug!(target: "sf_core", "explicit configuration");
            level.configure(None, "").unwrap();
            tracing::error!(target: "sf_core", "later default resets fatal");
        });
    }

    #[cfg(unix)]
    #[test]
    fn easy_logging_rejects_writable_and_symlinked_client_configs() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let folder = std::env::temp_dir().join(format!("adbc-log-perms-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&folder).unwrap();
        let config = folder.join("client.json");
        std::fs::write(&config, r#"{"common":{}}"#).unwrap();
        std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o666)).unwrap();
        let error = super::read_client_logging_config(&config).unwrap_err();
        assert_eq!(error.vendor_code, 260012);
        assert!(error.message.contains("incorrect permissions"));
        std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600)).unwrap();
        let link = folder.join("symlink.json");
        symlink(&config, &link).unwrap();
        assert_eq!(
            super::read_client_logging_config(&link)
                .unwrap_err()
                .vendor_code,
            260012
        );
        std::fs::remove_dir_all(folder).unwrap();
    }

    #[test]
    fn log_reload_preserves_callsites_with_only_driver_dispatcher() {
        const CHILD: &str = "ADBC_SNOWFLAKE_TEST_SINGLE_DISPATCHER";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "telemetry::tests::log_reload_preserves_callsites_with_only_driver_dispatcher",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
            return;
        }
        static INNER: std::sync::LazyLock<Inner> =
            std::sync::LazyLock::new(|| Inner::new().unwrap());
        let inner = &*INNER;
        let span_enabled = || {
            tracing::dispatcher::with_default(&inner.dispatch, || {
                !tracing::info_span!(target: "sf_core", "reload_regression").is_disabled()
            })
        };
        assert!(span_enabled());
        inner
            .configure_tracing(None, None, Some("off"), "")
            .unwrap();
        assert!(span_enabled());
    }

    #[test]
    fn sf_core_span_inherits_adbc_remote_parent_without_exporting_query_text() {
        let folder =
            std::env::temp_dir().join(format!("adbc-parent-test-{}", uuid::Uuid::new_v4()));
        // A dedicated instance keeps this test's exporter configuration away
        // from the shared driver. Like the shared one it lives for the whole
        // process: dropping a dispatcher can deadlock tracing-core (see
        // SHARED_INNER).
        static INNER: std::sync::LazyLock<Inner> =
            std::sync::LazyLock::new(|| Inner::new().unwrap());
        let inner = &*INNER;
        inner
            .configure_tracing(
                Some(Exporter::AdbcFile),
                Some(folder.to_str().unwrap()),
                Some("off"),
                "",
            )
            .unwrap();
        // An unrelated database choosing only logging must leave this exporter
        // and its span pipeline intact.
        inner
            .configure_tracing(None, None, Some("fatal"), "")
            .unwrap();
        inner.configure_tracing(None, None, None, "").unwrap();
        assert!(inner.telemetry.is_enabled());
        let parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        inner
            .traced("statement.ExecuteQuery", Some(parent), |_| {
                tracing::info_span!(target: "sf_core", "core_child", query = "SELECT sensitive")
                    .in_scope(|| {});
                Ok(())
            })
            .unwrap();
        inner.telemetry.flush();
        let spans: Vec<serde_json::Value> = std::fs::read_dir(&folder)
            .unwrap()
            .flat_map(|entry| {
                std::fs::read_to_string(entry.unwrap().path())
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect();
        let root = spans
            .iter()
            .find(|span| span["Name"] == "statement.ExecuteQuery")
            .unwrap();
        let child = spans
            .iter()
            .find(|span| span["Name"] == "core_child")
            .unwrap();
        assert_eq!(
            root["SpanContext"]["TraceID"],
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(root["Parent"]["SpanID"], "00f067aa0ba902b7");
        assert_eq!(child["Parent"]["SpanID"], root["SpanContext"]["SpanID"]);
        assert_eq!(
            child["SpanContext"]["TraceID"],
            root["SpanContext"]["TraceID"]
        );
        assert!(!child.to_string().contains("SELECT sensitive"));
        std::fs::remove_dir_all(folder).unwrap();
    }
}
