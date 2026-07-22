//! Adapter from the existing local `ServiceHost` control API to the external
//! distributed sidecar. It does not add a distributed execution path to Host.

#![forbid(unsafe_code)]
#![allow(clippy::missing_errors_doc, clippy::must_use_candidate)]

use mutsuki_distributed_contracts::{
    DistributedError, DistributedErrorKind, LocalTaskOutcome, LocalTaskSnapshot,
};
use mutsuki_runtime_contracts::{RuntimeEvent, TaskBatch, TaskHandle};
use mutsuki_service_control::{
    ControlMethod, ControlResponse, CoreDrainResponse, CoreStatus, HealthReport, IdParam,
    TaskEventPage, TaskEventsAfterParam, TaskOutcomeView, TaskSnapshot, TaskSubmitBatchParam,
    TaskSubmitBatchResponse,
};
use mutsuki_service_ipc::{ControlClient, ControlClientConfig, IpcTransport};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;

pub type HostFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, DistributedError>> + Send + 'a>>;

pub trait HostAdapter: Send + Sync {
    fn submit_batch(&self, batch: TaskBatch) -> HostFuture<'_, Vec<TaskHandle>>;
    fn cancel(&self, handle: &TaskHandle) -> HostFuture<'_, ()>;
    fn snapshots(&self) -> HostFuture<'_, Vec<LocalTaskSnapshot>>;
    fn outcome(&self, handle: &TaskHandle) -> HostFuture<'_, Option<LocalTaskOutcome>>;
    fn events_after(&self, sequence: u64, limit: usize) -> HostFuture<'_, Vec<RuntimeEvent>>;
    fn begin_drain(&self) -> HostFuture<'_, ()>;
    fn health(&self) -> HostFuture<'_, String>;
}

#[derive(Clone)]
pub struct ServiceHostAdapter {
    client: ControlClient,
}

impl std::fmt::Debug for ServiceHostAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceHostAdapter").finish_non_exhaustive()
    }
}

impl ServiceHostAdapter {
    pub fn new(config: ControlClientConfig) -> Self {
        Self {
            client: ControlClient::new(config),
        }
    }

    pub fn local_socket(
        endpoint: impl Into<String>,
        token: impl Into<String>,
    ) -> ServiceHostAdapter {
        #[cfg(windows)]
        let transport = IpcTransport::NamedPipe;
        #[cfg(unix)]
        let transport = IpcTransport::UnixSocket;
        Self::new(ControlClientConfig::new(transport, endpoint, token))
    }

    async fn request<T: DeserializeOwned>(
        &self,
        method: ControlMethod,
        params: Value,
    ) -> Result<T, DistributedError> {
        let response = self
            .client
            .request(method, params)
            .await
            .map_err(|_| host_unavailable())?;
        decode_response(response)
    }
}

impl HostAdapter for ServiceHostAdapter {
    fn submit_batch(&self, batch: TaskBatch) -> HostFuture<'_, Vec<TaskHandle>> {
        Box::pin(async move {
            let params = serde_json::to_value(TaskSubmitBatchParam { batch })
                .map_err(|_| protocol_error())?;
            let response: TaskSubmitBatchResponse =
                self.request(ControlMethod::TaskSubmitBatch, params).await?;
            Ok(response.handles)
        })
    }

    fn cancel(&self, handle: &TaskHandle) -> HostFuture<'_, ()> {
        let id = handle.task_id.clone();
        Box::pin(async move {
            let params = serde_json::to_value(IdParam { id }).map_err(|_| protocol_error())?;
            let _: Value = self.request(ControlMethod::TaskCancel, params).await?;
            Ok(())
        })
    }

    fn snapshots(&self) -> HostFuture<'_, Vec<LocalTaskSnapshot>> {
        Box::pin(async move {
            let snapshots: Vec<TaskSnapshot> =
                self.request(ControlMethod::TaskList, Value::Null).await?;
            Ok(snapshots.into_iter().map(map_snapshot).collect())
        })
    }

    fn outcome(&self, handle: &TaskHandle) -> HostFuture<'_, Option<LocalTaskOutcome>> {
        let id = handle.task_id.clone();
        Box::pin(async move {
            let params = serde_json::to_value(IdParam { id }).map_err(|_| protocol_error())?;
            let outcome: TaskOutcomeView = self.request(ControlMethod::TaskOutcome, params).await?;
            Ok(Some(LocalTaskOutcome {
                task_id: outcome.task_id,
                status: outcome.status,
                output_ref: outcome.output_ref,
                reason: outcome.reason,
                error_code: outcome.error_code,
            }))
        })
    }

    fn events_after(&self, sequence: u64, limit: usize) -> HostFuture<'_, Vec<RuntimeEvent>> {
        Box::pin(async move {
            let params = serde_json::to_value(TaskEventsAfterParam { sequence, limit })
                .map_err(|_| protocol_error())?;
            let page: TaskEventPage = self.request(ControlMethod::TaskEventsAfter, params).await?;
            Ok(page.events)
        })
    }

    fn begin_drain(&self) -> HostFuture<'_, ()> {
        Box::pin(async move {
            let response: CoreDrainResponse = self
                .request(ControlMethod::CoreBeginDrain, Value::Null)
                .await?;
            if response.state != "draining" {
                return Err(DistributedError::new(
                    DistributedErrorKind::HostUnavailable,
                    "local Host did not enter draining state",
                ));
            }
            Ok(())
        })
    }

    fn health(&self) -> HostFuture<'_, String> {
        Box::pin(async move {
            let core: CoreStatus = self.request(ControlMethod::CoreStatus, Value::Null).await?;
            if !core.running {
                return Err(host_unavailable());
            }
            let health: HealthReport = self
                .request(ControlMethod::HealthCheck, Value::Null)
                .await?;
            Ok(health.core)
        })
    }
}

fn decode_response<T: DeserializeOwned>(response: ControlResponse) -> Result<T, DistributedError> {
    if !response.ok {
        let kind = if response
            .error
            .as_ref()
            .is_some_and(|error| error.code == "unsupported")
        {
            DistributedErrorKind::Incompatible
        } else {
            DistributedErrorKind::HostUnavailable
        };
        return Err(DistributedError::new(
            kind,
            "local Host rejected the control request",
        ));
    }
    serde_json::from_value(response.result.unwrap_or(Value::Null)).map_err(|_| protocol_error())
}

fn map_snapshot(snapshot: TaskSnapshot) -> LocalTaskSnapshot {
    LocalTaskSnapshot {
        task_id: snapshot.task_id,
        protocol_id: snapshot.protocol_id,
        status: snapshot.status,
        registry_generation: snapshot.registry_generation,
        runner_id: snapshot.claimed_by.or(snapshot.owner_runner),
        lease_id: snapshot.lease_id,
    }
}

const fn host_unavailable() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::HostUnavailable,
        "local Host is unavailable",
    )
}

const fn protocol_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Protocol,
        "local Host control response is invalid",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use mutsuki_service_control::{ControlError, ControlResponse};

    #[test]
    fn unsupported_control_surface_is_reported_as_incompatible() {
        let response = ControlResponse::err(ControlError::Unsupported("task_submit_batch".into()));
        assert_eq!(
            decode_response::<Value>(response).unwrap_err().kind,
            DistributedErrorKind::Incompatible
        );
    }

    #[test]
    fn malformed_success_response_is_a_protocol_error() {
        let response = ControlResponse::ok("not-a-core-status");
        assert_eq!(
            decode_response::<CoreStatus>(response).unwrap_err().kind,
            DistributedErrorKind::Protocol
        );
    }
}
