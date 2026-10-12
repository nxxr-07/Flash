use serde::{Deserialize, Serialize};

pub mod logging {
    use std::{collections::HashMap, env};

    use anyhow::{Context, anyhow};
    use opentelemetry::KeyValue;
    use opentelemetry_appender_tracing::layer::{
        OpenTelemetryTracingBridge, TracingSpanAttributes,
    };
    use opentelemetry_otlp::{LogExporter, Protocol, WithExportConfig, WithHttpConfig};
    use opentelemetry_sdk::{Resource, logs::SdkLoggerProvider};
    use tracing_subscriber::{
        EnvFilter, filter::LevelFilter, layer::SubscriberExt, util::SubscriberInitExt,
    };

    const DEFAULT_NEW_RELIC_ENDPOINT: &str = "https://otlp.nr-data.net/v1/logs";

    /// Initializes stdout logging and optionally exports tracing events to New Relic.
    pub fn init_tracing(service_name: &str) -> anyhow::Result<Option<SdkLoggerProvider>> {
        match dotenvy::dotenv() {
            Ok(_) => {}
            Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("loading .env file"),
        }

        let filter = EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .from_env()
            .map_err(|error| anyhow!("invalid RUST_LOG filter: {error}"))?;

        let license_key = match env::var("NEW_RELIC_LICENSE_KEY") {
            Ok(key) if !key.trim().is_empty() => Some(key),
            Ok(_) => return Err(anyhow!("NEW_RELIC_LICENSE_KEY must not be empty")),
            Err(env::VarError::NotPresent) => None,
            Err(error) => return Err(error).context("reading NEW_RELIC_LICENSE_KEY"),
        };

        let Some(license_key) = license_key else {
            tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer())
                .try_init()
                .map_err(|error| anyhow!("failed to initialize tracing subscriber: {error}"))?;
            tracing::info!(
                service_name,
                "New Relic export disabled; NEW_RELIC_LICENSE_KEY is not set"
            );
            return Ok(None);
        };

        let endpoint = match env::var("NEW_RELIC_OTLP_ENDPOINT") {
            Ok(endpoint) if !endpoint.trim().is_empty() => endpoint,
            Ok(_) => return Err(anyhow!("NEW_RELIC_OTLP_ENDPOINT must not be empty")),
            Err(env::VarError::NotPresent) => DEFAULT_NEW_RELIC_ENDPOINT.to_string(),
            Err(error) => return Err(error).context("reading NEW_RELIC_OTLP_ENDPOINT"),
        };
        let exporter = LogExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(endpoint)
            .with_headers(HashMap::from([("api-key".to_string(), license_key)]))
            .build()
            .context("building New Relic OTLP log exporter")?;
        let provider = SdkLoggerProvider::builder()
            .with_resource(
                Resource::builder()
                    .with_attributes([KeyValue::new("service.name", service_name.to_string())])
                    .build(),
            )
            .with_batch_exporter(exporter)
            .build();
        let otel_layer = OpenTelemetryTracingBridge::builder(&provider)
            .with_tracing_span_attributes(TracingSpanAttributes::all())
            .build();

        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer())
            .with(otel_layer)
            .try_init()
            .map_err(|error| anyhow!("failed to initialize tracing subscriber: {error}"))?;

        tracing::info!(service_name, "New Relic OTLP log export enabled");
        Ok(Some(provider))
    }

    /// Flushes pending log records and shuts down the optional exporter.
    pub fn shutdown_tracing(provider: Option<SdkLoggerProvider>) -> anyhow::Result<()> {
        if let Some(provider) = provider {
            provider
                .shutdown()
                .map_err(|error| anyhow!("failed to shut down OpenTelemetry logger: {error}"))?;
        }
        Ok(())
    }
}

/// Commands sent from Client to Server
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Command {
    /// Publish a message to a topic
    Publish { topic: String, payload: Vec<u8> },
    /// Subscribe to a topic to receive messages
    Subscribe { topic: String },
}

/// Responses sent from Server to Client
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    /// Operation successful
    Ok,
    /// Operation failed with error message
    Error(String),
    /// A message from a subscribed topic
    Message { topic: String, payload: Vec<u8> },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_round_trips_through_json() {
        let command = Command::Publish {
            topic: "notifications".to_string(),
            payload: b"hello".to_vec(),
        };

        let encoded = serde_json::to_string(&command).unwrap();
        let decoded: Command = serde_json::from_str(&encoded).unwrap();

        match decoded {
            Command::Publish { topic, payload } => {
                assert_eq!(topic, "notifications");
                assert_eq!(payload, b"hello");
            }
            Command::Subscribe { .. } => panic!("decoded a different command variant"),
        }
    }
}
