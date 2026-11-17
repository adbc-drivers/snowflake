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

use std::sync::Arc;

use driverbase::telemetry::Telemetry;
use opentelemetry::KeyValue;
use opentelemetry_sdk::trace::SdkTracer;
use tracing::span::Id;
use tracing_opentelemetry::{OtelData, PreSampledTracer};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::{Context as LayerContext, Layer};
use tracing_subscriber::registry::Registry;
use tracing_subscriber::reload;

pub(crate) type LogLevelHandle = reload::Handle<LevelFilter, Registry>;

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
    let (filter, handle) = reload::Layer::new(LevelFilter::OFF);
    let layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(writer)
        .with_filter(filter);
    (layer, handle)
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
    fn stderr_filter_reload_only_changes_log_output() {
        use tracing_subscriber::prelude::*;

        let output = CapturedLogs::default();
        let (layer, level) = super::log_layer(output.clone());
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::error!("default fatal suppresses error");
            level
                .reload(super::log_level_filter("DEBUG").unwrap())
                .unwrap();
            tracing::debug!(target: "sf_core", "core debug is visible");
            tracing::info!("driver info is visible");
            tracing::trace!("trace is suppressed");
            level
                .reload(super::log_level_filter("off").unwrap())
                .unwrap();
            tracing::error!("off suppresses error");
        });
        let bytes = output.0.lock().unwrap();
        let logs = std::str::from_utf8(&bytes).unwrap();
        assert!(logs.contains("core debug is visible"));
        assert!(logs.contains("driver info is visible"));
        assert!(!logs.contains("suppresses"));
        assert!(!logs.contains("trace is suppressed"));
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
        inner.configure_tracing(None, None, Some("off")).unwrap();
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
            )
            .unwrap();
        // An unrelated database choosing only logging must leave this exporter
        // and its span pipeline intact.
        inner.configure_tracing(None, None, Some("fatal")).unwrap();
        inner.configure_tracing(None, None, None).unwrap();
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
