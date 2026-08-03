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
//! The exporter itself is installed by the node process, not here. This module
//! resolves and validates the settings and hands them over, which keeps the
//! heavyweight SDK out of every `orbita get` a script runs in a loop.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use tracing_subscriber::filter::EnvFilter;

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

/// Installs the log subscriber.
///
/// It writes to stderr so that stdout stays clean for command output. A script
/// running `orbita get ... --output json | jq` must not have a log line land in
/// the middle of the document.
pub fn install_logging(config: &Config) -> Result<()> {
    let filter = EnvFilter::try_new(&config.telemetry.log_level).with_context(|| {
        format!(
            "{:?} is not a valid log filter. Try info, or orbita=debug",
            config.telemetry.log_level
        )
    })?;
    // A second install is not an error worth failing a command over: it only
    // happens in tests that share a process.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
    Ok(())
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
    fn an_invalid_log_filter_is_reported_with_an_example_of_a_valid_one() {
        let config = config(TelemetryLayer {
            log_level: Some("orbita=shouty".to_owned()),
            ..TelemetryLayer::default()
        });
        let err = install_logging(&config).unwrap_err();
        assert!(format!("{err:#}").contains("orbita=debug"), "{err:#}");
    }
}
