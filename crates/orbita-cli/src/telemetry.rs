//! OpenTelemetry and logging setup.
//!
//! The settings come from the same layered configuration as everything else,
//! so an operator sets a collector endpoint the same way they set a listen
//! address, rather than learning a second mechanism.
//!
//! Logs go to stderr always. Traces and metrics are exported only when a
//! collector endpoint is configured, because a node that cannot reach a
//! collector must still start and must still be debuggable. Exporting is an
//! enhancement, not a dependency.
//!
//! The exporter itself is installed by [`install_node`], which the node process
//! calls once it is inside its Tokio runtime. Keeping it out of
//! [`install_logging`] is what keeps the heavyweight SDK — and a background
//! export task that needs a runtime to spawn on — out of every `orbita get` a
//! script runs in a loop.
//!
//! # Push, not pull
//!
//! Metrics and traces are pushed to an OTLP collector rather than scraped from
//! the node. The config surface already resolves an OTLP endpoint, so a push
//! pipeline reuses the setting an operator already sets instead of inventing a
//! second one. It also adds no inbound listener: ADR 0004 makes the node's port
//! surface deliberate — a client port and a private peer port — and a metrics
//! port to scrape would be a third thing to bind, secure, and keep off the
//! public network. The collector fans out to Prometheus, Tempo, or whatever
//! else and owns retention and cardinality limiting, which leaves the node a
//! thin producer. Head-based trace sampling is decided here, at the node that
//! starts the trace, which is a push concept and matches [`Settings`].

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::{Sampler, TracerProvider};
use opentelemetry_sdk::{runtime, Resource};
use tonic::transport::ClientTlsConfig;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::prelude::*;

use crate::config::Config;

/// Everything the exporter needs, resolved.
///
/// Resource attributes follow the OpenTelemetry semantic conventions, so they
/// are strings on both sides and this type does not try to be clever about
/// them.
#[derive(Debug, Clone, PartialEq)]
pub struct Settings {
    /// The OTLP collector to export to. `None` disables export.
    pub otlp_endpoint: Option<String>,
    /// The fraction of traces to sample, between 0 and 1. Sampling is head
    /// based, so this is decided once per trace at the node that starts it.
    pub trace_sample_ratio: f64,
    /// `service.name`, which is the attribute every backend groups by first.
    pub service_name: String,
    /// Everything else, such as `deployment.environment` or `cloud.region`.
    /// The node adds its own id and role on top of these.
    pub resource_attributes: BTreeMap<String, String>,
}

impl Settings {
    /// Pulls the telemetry settings out of a resolved configuration.
    ///
    /// The node's identity is added here rather than left to the caller,
    /// because a trace that cannot be attributed to a node is a trace nobody
    /// can act on.
    #[must_use]
    pub fn from_config(config: &Config) -> Self {
        let mut resource_attributes = config.telemetry.resource_attributes.clone();
        resource_attributes.insert("service.version".to_owned(), crate::VERSION.to_owned());
        resource_attributes.insert("orbita.node.id".to_owned(), config.node.id.to_string());
        resource_attributes.insert("orbita.node.role".to_owned(), config.node.role.to_string());
        resource_attributes.insert("orbita.cluster".to_owned(), config.cluster.name.clone());

        Self {
            otlp_endpoint: config.telemetry.otlp_endpoint.clone(),
            trace_sample_ratio: config.telemetry.trace_sample_ratio,
            service_name: config.telemetry.service_name.clone(),
            resource_attributes,
        }
    }

    /// Whether traces and metrics will actually leave the process.
    #[must_use]
    pub fn export_enabled(&self) -> bool {
        self.otlp_endpoint.is_some()
    }
}

/// Builds the log filter, naming a fix when the configured one does not parse.
fn log_filter(config: &Config) -> Result<EnvFilter> {
    EnvFilter::try_new(&config.telemetry.log_level).with_context(|| {
        format!(
            "{:?} is not a valid log filter. Try info, or orbita=debug",
            config.telemetry.log_level
        )
    })
}

/// Installs the log subscriber.
///
/// It writes to stderr so that stdout stays clean for command output. A script
/// running `orbita get ... --output json | jq` must not have a log line land in
/// the middle of the document.
///
/// This is the client path. A node uses [`install_node`], which adds the OTLP
/// exporters on top of the same stderr logging.
pub fn install_logging(config: &Config) -> Result<()> {
    // A second install is not an error worth failing a command over: it only
    // happens in tests that share a process.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(log_filter(config)?)
        .with_writer(std::io::stderr)
        .try_init();
    Ok(())
}

/// Keeps the exporters alive and flushes them on shutdown.
///
/// A batch exporter holds spans and metrics it has not sent yet, so dropping the
/// providers without a flush loses the last window — exactly the window that
/// covers a crash, which is the one an operator most wants. Holding this for the
/// life of the node and letting `Drop` shut the providers down is what turns a
/// clean exit into a clean flush.
#[must_use = "dropping the guard flushes and stops the exporters"]
pub struct TelemetryGuard {
    tracer_provider: Option<TracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
}

impl TelemetryGuard {
    /// The guard a node with no collector configured holds: nothing to flush,
    /// because nothing was exported.
    fn inert() -> Self {
        Self {
            tracer_provider: None,
            meter_provider: None,
        }
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        // Best effort: a node on its way out cannot do anything with a flush
        // that fails, and turning shutdown into an error would only obscure the
        // reason it was shutting down.
        if let Some(provider) = self.tracer_provider.take() {
            let _ = provider.shutdown();
        }
        if let Some(provider) = self.meter_provider.take() {
            let _ = provider.shutdown();
        }
    }
}

/// Installs logging and, when a collector is configured, the OTLP trace and
/// metric exporters.
///
/// This is the node path, and it is deliberately not called until the node is
/// inside its Tokio runtime: the OTLP batch and periodic exporters spawn
/// background tasks, so building them anywhere else would panic for want of a
/// runtime. Logging is installed either way, so a node with no collector still
/// writes to stderr and is still debuggable.
///
/// The returned guard has to outlive serving. Dropping it flushes whatever the
/// exporters are still holding.
pub fn install_node(config: &Config) -> Result<TelemetryGuard> {
    let filter = log_filter(config)?;
    let settings = Settings::from_config(config);

    let (otel_layer, guard) = if let Some(endpoint) = settings.otlp_endpoint.clone() {
        let resource = resource(&settings);

        // Traces. Head-based sampling is decided here because this node is where
        // the trace starts; a forwarded hop inherits the decision rather than
        // re-rolling it.
        let span_exporter = configure_otlp_tls(
            opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint.clone()),
            &endpoint,
        )
        .build()
        .context("building the OTLP span exporter")?;
        let tracer_provider = TracerProvider::builder()
            .with_batch_exporter(span_exporter, runtime::Tokio)
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                settings.trace_sample_ratio,
            ))))
            .with_resource(resource.clone())
            .build();
        let tracer = tracer_provider.tracer("orbita");
        global::set_tracer_provider(tracer_provider.clone());

        // Metrics. A periodic reader pushes on its own cadence; the node emits
        // into the meter and never blocks a request on an export.
        let metric_exporter = configure_otlp_tls(
            opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint.clone()),
            &endpoint,
        )
        .build()
        .context("building the OTLP metric exporter")?;
        let reader = PeriodicReader::builder(metric_exporter, runtime::Tokio).build();
        let meter_provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource)
            .build();
        global::set_meter_provider(meter_provider.clone());

        (
            Some(tracing_opentelemetry::layer().with_tracer(tracer)),
            TelemetryGuard {
                tracer_provider: Some(tracer_provider),
                meter_provider: Some(meter_provider),
            },
        )
    } else {
        (None, TelemetryGuard::inert())
    };

    // One subscriber, built once, with the OTLP layer folded in only when it
    // exists — `Option<Layer>` is itself a `Layer`, so the client path and the
    // exporting path share a single init. A second install is ignored rather
    // than fatal, which is what a test sharing the process relies on.
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(otel_layer)
        .try_init();

    Ok(guard)
}

fn configure_otlp_tls<T: opentelemetry_otlp::WithTonicConfig>(builder: T, endpoint: &str) -> T {
    if otlp_uses_tls(endpoint) {
        builder.with_tls_config(ClientTlsConfig::new().with_webpki_roots())
    } else {
        builder
    }
}

fn otlp_uses_tls(endpoint: &str) -> bool {
    endpoint.starts_with("https://")
}

/// The OpenTelemetry resource every span and metric is attributed to.
///
/// `service.name` is set explicitly because it is the attribute every backend
/// groups by first; the rest are whatever the operator configured plus the node
/// identity [`Settings::from_config`] already folded in.
fn resource(settings: &Settings) -> Resource {
    let mut attributes = vec![KeyValue::new("service.name", settings.service_name.clone())];
    for (key, value) in &settings.resource_attributes {
        attributes.push(KeyValue::new(key.clone(), value.clone()));
    }
    Resource::new(attributes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Layer, NodeLayer, Role, TelemetryLayer};

    fn config(telemetry: TelemetryLayer) -> Config {
        Layer {
            node: NodeLayer {
                id: Some(7),
                role: Some(Role::Worker),
                ..NodeLayer::default()
            },
            telemetry,
            ..Layer::default()
        }
        .resolve()
        .unwrap()
    }

    #[test]
    fn export_is_off_until_a_collector_endpoint_is_configured() {
        let settings = Settings::from_config(&config(TelemetryLayer::default()));
        assert!(!settings.export_enabled());
        assert_eq!(settings.service_name, "orbita");
    }

    #[test]
    fn a_configured_collector_turns_export_on() {
        let settings = Settings::from_config(&config(TelemetryLayer {
            otlp_endpoint: Some("http://collector:4317".to_owned()),
            ..TelemetryLayer::default()
        }));
        assert!(settings.export_enabled());
    }

    #[test]
    fn https_collectors_use_tls_while_local_http_collectors_do_not() {
        assert!(otlp_uses_tls("https://ingress.example.com:4317"));
        assert!(!otlp_uses_tls("http://collector:4317"));
    }

    #[test]
    fn the_node_identity_is_attached_to_every_exported_span() {
        let settings = Settings::from_config(&config(TelemetryLayer::default()));
        assert_eq!(settings.resource_attributes["orbita.node.id"], "7");
        assert_eq!(settings.resource_attributes["orbita.node.role"], "worker");
        assert_eq!(
            settings.resource_attributes["service.version"],
            crate::VERSION
        );
    }

    #[test]
    fn configured_resource_attributes_survive_alongside_the_node_identity() {
        let mut attributes = BTreeMap::new();
        attributes.insert("deployment.environment".to_owned(), "staging".to_owned());
        let settings = Settings::from_config(&config(TelemetryLayer {
            resource_attributes: Some(attributes),
            ..TelemetryLayer::default()
        }));
        assert_eq!(
            settings.resource_attributes["deployment.environment"],
            "staging"
        );
        assert_eq!(settings.resource_attributes["orbita.node.id"], "7");
    }

    #[test]
    fn the_resource_carries_the_service_name_and_the_node_identity() {
        // service.name is what a backend groups by first, so it is set
        // explicitly, and the node identity folded in by `from_config` has to
        // survive into the resource or a span cannot be attributed to a node.
        let settings = Settings::from_config(&config(TelemetryLayer {
            service_name: Some("orbita".to_owned()),
            ..TelemetryLayer::default()
        }));
        let resource = resource(&settings);
        assert_eq!(
            resource
                .get(opentelemetry::Key::from_static_str("service.name"))
                .map(|v| v.to_string()),
            Some("orbita".to_owned())
        );
        assert_eq!(
            resource
                .get(opentelemetry::Key::from_static_str("orbita.node.id"))
                .map(|v| v.to_string()),
            Some("7".to_owned())
        );
    }

    #[test]
    fn a_node_without_a_collector_still_installs_and_yields_an_inert_guard() {
        // The export path needs a runtime to spawn its exporters on; the no
        // collector path does not, so it can be proven here without one. It has
        // to succeed and leave a guard with nothing to flush, because a node
        // that cannot reach a collector must still start and still log.
        let guard = install_node(&config(TelemetryLayer::default())).expect("install");
        assert!(guard.tracer_provider.is_none());
        assert!(guard.meter_provider.is_none());
    }

    #[test]
    fn an_invalid_log_filter_is_reported_with_an_example_of_a_valid_one() {
        let config = config(TelemetryLayer {
            log_level: Some("orbita=shouty".to_owned()),
            ..TelemetryLayer::default()
        });
        let err = install_logging(&config).unwrap_err();
        assert!(format!("{err:#}").contains("orbita=debug"), "{err:#}");
    }
}
