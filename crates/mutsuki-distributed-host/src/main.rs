use mutsuki_distributed_contracts::DistributionMode;
use mutsuki_distributed_host_adapter::ServiceHostAdapter;
use mutsuki_distributed_runtime::Sidecar;
use std::sync::Arc;

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
            let endpoint = std::env::var("MUTSUKI_SERVICE_ENDPOINT")
                .map_err(|_| "MUTSUKI_SERVICE_ENDPOINT is required")?;
            let token = std::env::var("MUTSUKI_CONTROL_TOKEN")
                .map_err(|_| "MUTSUKI_CONTROL_TOKEN is required")?;
            let sidecar = Sidecar::local_observable(Arc::new(ServiceHostAdapter::local_socket(
                endpoint, token,
            )));
            let health = sidecar.health().await?;
            println!("local Host health: {health}");
        }
        DistributionMode::Clustered => {
            return Err("clustered mode requires explicit authenticated Link sessions, Worker registry, and resource localizer assembly".into());
        }
    }
    Ok(())
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
}
