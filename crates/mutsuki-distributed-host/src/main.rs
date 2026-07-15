use mutsuki_distributed_contracts::{
    DistributedError, DistributedErrorKind, DistributionMode, NodeId, WorkerAdvertisement,
};
use mutsuki_distributed_host_adapter::ServiceHostAdapter;
use mutsuki_distributed_runtime::{
    ControllerProcess, LinkResourceLocalizer, Sidecar, WorkerConnectionConfig, WorkerProcess,
};
use serde::Deserialize;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case", deny_unknown_fields)]
#[allow(clippy::large_enum_variant)]
enum ClusterDeployment {
    Controller {
        node_id: String,
        management_address: String,
        management_client_node: String,
        cluster_secret_env: String,
        service_endpoint: String,
        service_token_env: String,
        workers: Vec<WorkerConfig>,
        #[serde(default = "default_max_tasks")]
        max_tasks: usize,
        #[serde(default = "default_request_timeout_ms")]
        request_timeout_ms: u64,
        #[serde(default = "default_pulse_interval_ms")]
        pulse_interval_ms: u64,
    },
    Worker {
        node_id: String,
        controller_node: String,
        listen_address: String,
        cluster_secret_env: String,
        service_endpoint: String,
        service_token_env: String,
        content_directory: String,
        #[serde(default = "default_max_content_bytes")]
        max_content_bytes: u64,
        advertisement: WorkerAdvertisement,
        #[serde(default = "default_request_timeout_ms")]
        request_timeout_ms: u64,
    },
    HighAvailability,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerConfig {
    node_id: String,
    address: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = parse_mode(std::env::args().nth(1).as_deref())?;
    match mode {
        DistributionMode::Disabled => {
            let sidecar = Sidecar::disabled();
            debug_assert_eq!(sidecar.background_tasks(), 0);
            debug_assert!(!sidecar.opens_network_on_construction());
            println!("MutsukiDistributedHost is disabled");
        }
        DistributionMode::LocalObservable => {
            let endpoint = required_env("MUTSUKI_SERVICE_ENDPOINT")?;
            let token = required_env("MUTSUKI_CONTROL_TOKEN")?;
            let sidecar = Sidecar::local_observable(Arc::new(ServiceHostAdapter::local_socket(
                endpoint, token,
            )));
            let health = sidecar.health().await?;
            println!("local Host health: {health}");
        }
        DistributionMode::Clustered => {
            let config_path = std::env::args().nth(2).ok_or_else(|| {
                DistributedError::new(
                    DistributedErrorKind::InvalidConfig,
                    "clustered mode requires a deployment JSON path",
                )
            })?;
            run_clustered(&config_path).await?;
        }
    }
    Ok(())
}

async fn run_clustered(path: &str) -> Result<(), DistributedError> {
    let config = load_deployment(Path::new(path))?;
    match config {
        ClusterDeployment::Controller {
            node_id,
            management_address,
            management_client_node,
            cluster_secret_env,
            service_endpoint,
            service_token_env,
            workers,
            max_tasks,
            request_timeout_ms,
            pulse_interval_ms,
        } => {
            let secret = secret_from_env(&cluster_secret_env)?;
            let token = required_env(&service_token_env)?;
            let host = Arc::new(ServiceHostAdapter::local_socket(service_endpoint, token));
            let controller = Arc::new(
                ControllerProcess::connect(
                    NodeId(node_id),
                    host,
                    workers
                        .into_iter()
                        .map(|worker| WorkerConnectionConfig {
                            node_id: NodeId(worker.node_id),
                            address: worker.address,
                        })
                        .collect(),
                    secret.clone(),
                    max_tasks,
                    Duration::from_millis(request_timeout_ms),
                )
                .await?,
            );
            controller
                .serve_management(
                    NodeId(management_client_node),
                    management_address,
                    secret,
                    Duration::from_millis(pulse_interval_ms),
                    Duration::from_millis(request_timeout_ms),
                )
                .await
        }
        ClusterDeployment::Worker {
            node_id,
            controller_node,
            listen_address,
            cluster_secret_env,
            service_endpoint,
            service_token_env,
            content_directory,
            max_content_bytes,
            advertisement,
            request_timeout_ms,
        } => {
            let secret = secret_from_env(&cluster_secret_env)?;
            let token = required_env(&service_token_env)?;
            let host = Arc::new(ServiceHostAdapter::local_socket(service_endpoint, token));
            let node_id = NodeId(node_id);
            let localizer = Arc::new(LinkResourceLocalizer::new(
                node_id.clone(),
                secret.clone(),
                content_directory,
                max_content_bytes,
                Duration::from_millis(request_timeout_ms),
            )?);
            WorkerProcess::new(
                node_id,
                NodeId(controller_node),
                listen_address,
                secret,
                advertisement,
                host,
                localizer,
                Duration::from_millis(request_timeout_ms),
            )?
            .run()
            .await
        }
        ClusterDeployment::HighAvailability => Err(DistributedError::new(
            DistributedErrorKind::ExperimentalUnavailable,
            "HA process deployment is unavailable; ReferenceCftModel is conformance-only",
        )),
    }
}

fn load_deployment(path: &Path) -> Result<ClusterDeployment, DistributedError> {
    let bytes = std::fs::read(path).map_err(|_| {
        DistributedError::new(
            DistributedErrorKind::InvalidConfig,
            "cluster deployment file could not be read",
        )
    })?;
    serde_json::from_slice(&bytes).map_err(|_| {
        DistributedError::new(
            DistributedErrorKind::InvalidConfig,
            "cluster deployment JSON is invalid",
        )
    })
}

fn secret_from_env(name: &str) -> Result<Arc<[u8]>, DistributedError> {
    let secret = required_env(name)?;
    if secret.len() < 32 {
        return Err(DistributedError::new(
            DistributedErrorKind::InvalidConfig,
            "cluster secret must contain at least 32 bytes",
        ));
    }
    Ok(Arc::from(secret.into_bytes()))
}

fn required_env(name: &str) -> Result<String, DistributedError> {
    std::env::var(name).map_err(|_| {
        DistributedError::new(
            DistributedErrorKind::InvalidConfig,
            "required deployment secret environment variable is missing",
        )
    })
}

const fn default_max_tasks() -> usize {
    1024
}

const fn default_request_timeout_ms() -> u64 {
    5_000
}

const fn default_max_content_bytes() -> u64 {
    64 * 1024 * 1024 * 1024
}

const fn default_pulse_interval_ms() -> u64 {
    1_000
}

fn parse_mode(value: Option<&str>) -> Result<DistributionMode, &'static str> {
    match value.unwrap_or("disabled") {
        "disabled" => Ok(DistributionMode::Disabled),
        "local-observable" => Ok(DistributionMode::LocalObservable),
        "clustered" => Ok(DistributionMode::Clustered),
        _ => Err("mode must be disabled, local-observable, or clustered"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_defaults_to_disabled_and_rejects_unknown_values() {
        assert_eq!(parse_mode(None).unwrap(), DistributionMode::Disabled);
        assert_eq!(
            parse_mode(Some("local-observable")).unwrap(),
            DistributionMode::LocalObservable
        );
        assert!(parse_mode(Some("anonymous-cluster")).is_err());
    }

    #[test]
    fn ha_deployment_is_a_distinct_unavailable_role() {
        let deployment: ClusterDeployment =
            serde_json::from_str(r#"{"role":"high_availability"}"#).unwrap();
        assert!(matches!(deployment, ClusterDeployment::HighAvailability));
    }
}
