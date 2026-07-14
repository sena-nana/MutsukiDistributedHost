//! Single-controller remote execution MVP. Consensus, durable registries,
//! checkpoint migration, and trust policy belong to later phases.

#![forbid(unsafe_code)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::too_many_arguments
)]

use mutsuki_distributed_contracts::{
    AttemptRecord, DISTRIBUTED_PROTOCOL_ID, DISTRIBUTED_PROTOCOL_MAJOR, DirectDataRef,
    DistributedError, DistributedErrorKind, DistributionMode, GlobalTaskId, GlobalTaskRecord,
    LocalTaskOutcome, LocalTaskSnapshot, NodeId, PlacementKind, RemoteAccepted, RemoteResult,
    RemoteTaskEnvelope, TaskPlacement, WorkerAdvertisement, WorkerCommand, WorkerFailure,
    WorkerHealth, WorkerPulse, WorkerReply, WorkerReplyBody, WorkerRequest, can_restart_from_input,
    decode_control, encode_control,
};
use mutsuki_distributed_host_adapter::{HostAdapter, HostFuture};
use mutsuki_link_core::{
    AuthenticatedSession, ChannelMode, ConnectionQuality, PeerId, ProtocolChannel,
    ProtocolDescriptor, ProtocolId, ProtocolVersion, SecurityLevel, VersionRange,
};
use mutsuki_runtime_contracts::{
    ExecutionMobility, PortableTask, RequirementSet, RuntimeEvent, TaskBatch, TaskHandle,
};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

mod content_store;
mod durable_registry;
mod ha_control;
mod recovery;
mod scheduler;
pub use content_store::*;
pub use durable_registry::*;
pub use ha_control::*;
pub use recovery::*;
pub use scheduler::*;

pub fn distributed_protocol_descriptor() -> ProtocolDescriptor {
    ProtocolDescriptor {
        id: ProtocolId::new(DISTRIBUTED_PROTOCOL_ID).expect("static distributed protocol id"),
        versions: VersionRange::new(ProtocolVersion::new(1, 0), ProtocolVersion::new(1, 0)),
        channels: vec![
            protocol_channel("control", ChannelMode::RequestResponse, 0, 64 * 1024, None),
            protocol_channel(
                "resource",
                ChannelMode::Stream,
                80,
                1024 * 1024,
                Some(64 * 1024 * 1024 * 1024),
            ),
            protocol_channel(
                "result",
                ChannelMode::Stream,
                40,
                1024 * 1024,
                Some(64 * 1024 * 1024 * 1024),
            ),
        ],
    }
}

fn protocol_channel(
    name: &str,
    mode: ChannelMode,
    priority: u8,
    max_frame_bytes: usize,
    max_stream_bytes: Option<u64>,
) -> ProtocolChannel {
    ProtocolChannel {
        name: name.to_owned(),
        mode,
        priority,
        max_frame_bytes,
        max_stream_bytes,
        max_in_flight_frames: 32,
        discardable: false,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LinkSessionBinding {
    pub peer_id: PeerId,
    pub quality: ConnectionQuality,
    pub security_level: SecurityLevel,
}

impl LinkSessionBinding {
    pub fn from_authenticated(session: AuthenticatedSession<'_>) -> Result<Self, DistributedError> {
        if !session
            .info()
            .protocols
            .iter()
            .any(|protocol| protocol.namespace == DISTRIBUTED_PROTOCOL_ID)
        {
            return Err(DistributedError::new(
                DistributedErrorKind::Incompatible,
                "authenticated Link session did not negotiate the distributed protocol",
            ));
        }
        Ok(Self {
            peer_id: session.info().peer_id,
            quality: session.info().quality,
            security_level: session.security().security_level,
        })
    }
}

#[derive(Debug)]
pub struct WorkerRegistry {
    max_workers: usize,
    workers: BTreeMap<NodeId, WorkerAdvertisement>,
    pulses: BTreeMap<NodeId, WorkerPulse>,
}

impl WorkerRegistry {
    pub fn new(max_workers: usize) -> Result<Self, DistributedError> {
        if max_workers == 0 {
            return Err(invalid_config());
        }
        Ok(Self {
            max_workers,
            workers: BTreeMap::new(),
            pulses: BTreeMap::new(),
        })
    }

    pub fn register(&mut self, advertisement: WorkerAdvertisement) -> Result<(), DistributedError> {
        if advertisement.protocol_major != DISTRIBUTED_PROTOCOL_MAJOR {
            return Err(DistributedError::new(
                DistributedErrorKind::Incompatible,
                "Worker distributed protocol is incompatible",
            ));
        }
        if advertisement.snapshot_version == 0
            || advertisement
                .runners
                .iter()
                .any(|runner| runner.runner_generation == 0 || runner.plugin_generation == 0)
        {
            return Err(DistributedError::new(
                DistributedErrorKind::Incompatible,
                "Worker capability generation must be versioned",
            ));
        }
        if !self.workers.contains_key(&advertisement.node_id)
            && self.workers.len() >= self.max_workers
        {
            return Err(DistributedError::new(
                DistributedErrorKind::CapacityExceeded,
                "Worker registry capacity exceeded",
            ));
        }
        if self
            .workers
            .get(&advertisement.node_id)
            .is_some_and(|current| current.snapshot_version > advertisement.snapshot_version)
        {
            return Err(DistributedError::new(
                DistributedErrorKind::AttemptStale,
                "Worker capability snapshot is stale",
            ));
        }
        self.workers
            .insert(advertisement.node_id.clone(), advertisement);
        Ok(())
    }

    pub fn pulse(&mut self, pulse: WorkerPulse) -> Result<(), DistributedError> {
        let worker = self.workers.get_mut(&pulse.node_id).ok_or_else(|| {
            DistributedError::new(
                DistributedErrorKind::WorkerUnavailable,
                "Worker pulse has no registered capability snapshot",
            )
        })?;
        if worker.snapshot_version != pulse.snapshot_version {
            worker.health = WorkerHealth::Incompatible;
            return Err(DistributedError::new(
                DistributedErrorKind::Incompatible,
                "Worker pulse references an unknown capability version",
            ));
        }
        worker.health = pulse.health;
        self.pulses.insert(pulse.node_id.clone(), pulse);
        Ok(())
    }

    pub fn candidates(
        &self,
        portable: &PortableTask,
        requirements: &RequirementSet,
        excluded: &BTreeSet<NodeId>,
    ) -> Vec<NodeId> {
        self.workers
            .values()
            .filter(|worker| !excluded.contains(&worker.node_id))
            .filter(|worker| remote_eligible(worker, portable, requirements))
            .map(|worker| worker.node_id.clone())
            .collect()
    }

    pub fn get(&self, node_id: &NodeId) -> Option<&WorkerAdvertisement> {
        self.workers.get(node_id)
    }

    pub fn latest_pulse(&self, node_id: &NodeId) -> Option<&WorkerPulse> {
        self.pulses.get(node_id)
    }
}

pub fn remote_eligible(
    worker: &WorkerAdvertisement,
    portable: &PortableTask,
    requirements: &RequirementSet,
) -> bool {
    if worker.health != WorkerHealth::Ready
        || !portable.has_supported_envelope()
        || portable.capability.mobility == ExecutionMobility::LocalOnly
        || !requirements.is_satisfied_by(&worker.capabilities)
    {
        return false;
    }
    let capability = worker
        .portability
        .tasks
        .iter()
        .find(|descriptor| descriptor.protocol_id == portable.task.protocol_id);
    let task_compatible = capability.is_some_and(|descriptor| {
        descriptor.task_schema == portable.task_schema
            && descriptor.capability.mobility != ExecutionMobility::LocalOnly
    });
    let runner_compatible = portable
        .task
        .runner_hint
        .as_ref()
        .is_none_or(|runner_hint| {
            worker
                .runners
                .iter()
                .any(|runner| runner.runner_id == *runner_hint && runner.runner_generation > 0)
        });
    let resources_compatible = portable.resources.iter().all(|required| {
        worker.portability.resources.iter().any(|available| {
            available.resource_kind == required.resource_kind && available.schema == required.schema
        })
    });
    task_compatible && runner_compatible && resources_compatible
}

pub type WorkerFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, DistributedError>> + Send + 'a>>;

pub trait RemoteWorker: Send + Sync {
    fn submit(&self, envelope: RemoteTaskEnvelope) -> WorkerFuture<'_, RemoteAccepted>;
    fn cancel(&self, handle: &TaskHandle) -> WorkerFuture<'_, ()>;
    fn outcome(&self, handle: &TaskHandle) -> WorkerFuture<'_, Option<LocalTaskOutcome>>;
}

pub trait ResourceLocalizer: Send + Sync {
    fn localize<'a>(&'a self, resources: &'a [DirectDataRef]) -> WorkerFuture<'a, ()>;
}

/// A bounded request/reply carrier. Production implementations bind this to
/// the authenticated Mutsuki Link `control` channel; resource and result bytes
/// use their dedicated stream channels instead.
pub trait WorkerTransport: Send + Sync {
    fn round_trip(&self, request: Vec<u8>) -> WorkerFuture<'_, Vec<u8>>;
}

pub struct WireRemoteWorker {
    transport: Arc<dyn WorkerTransport>,
    next_request_id: AtomicU64,
}

impl WireRemoteWorker {
    pub fn new(transport: Arc<dyn WorkerTransport>) -> Self {
        Self {
            transport,
            next_request_id: AtomicU64::new(1),
        }
    }

    async fn request(&self, command: WorkerCommand) -> Result<WorkerReplyBody, DistributedError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let request = encode_control(&WorkerRequest {
            request_id,
            command,
        })?;
        let reply: WorkerReply = decode_control(&self.transport.round_trip(request).await?)?;
        if reply.request_id != request_id {
            return Err(DistributedError::new(
                DistributedErrorKind::Protocol,
                "Worker reply request id does not match",
            ));
        }
        reply.result.map_err(|failure| {
            DistributedError::new(failure.kind, "Worker rejected the distributed request")
        })
    }
}

impl RemoteWorker for WireRemoteWorker {
    fn submit(&self, envelope: RemoteTaskEnvelope) -> WorkerFuture<'_, RemoteAccepted> {
        Box::pin(async move {
            match self
                .request(WorkerCommand::Submit(Box::new(envelope)))
                .await?
            {
                WorkerReplyBody::Accepted(accepted) => Ok(accepted),
                _ => Err(unexpected_reply()),
            }
        })
    }

    fn cancel(&self, handle: &TaskHandle) -> WorkerFuture<'_, ()> {
        let handle = handle.clone();
        Box::pin(async move {
            match self.request(WorkerCommand::Cancel(handle)).await? {
                WorkerReplyBody::Cancelled => Ok(()),
                _ => Err(unexpected_reply()),
            }
        })
    }

    fn outcome(&self, handle: &TaskHandle) -> WorkerFuture<'_, Option<LocalTaskOutcome>> {
        let handle = handle.clone();
        Box::pin(async move {
            match self.request(WorkerCommand::Outcome(handle)).await? {
                WorkerReplyBody::Outcome(outcome) => Ok(outcome),
                _ => Err(unexpected_reply()),
            }
        })
    }
}

pub struct WorkerRequestDispatcher {
    worker: Arc<dyn RemoteWorker>,
}

impl WorkerRequestDispatcher {
    pub fn new(worker: Arc<dyn RemoteWorker>) -> Self {
        Self { worker }
    }

    pub async fn dispatch(&self, request: &[u8]) -> Result<Vec<u8>, DistributedError> {
        let request: WorkerRequest = decode_control(request)?;
        let result = match request.command {
            WorkerCommand::Submit(envelope) => self
                .worker
                .submit(*envelope)
                .await
                .map(WorkerReplyBody::Accepted),
            WorkerCommand::Cancel(handle) => self
                .worker
                .cancel(&handle)
                .await
                .map(|()| WorkerReplyBody::Cancelled),
            WorkerCommand::Outcome(handle) => self
                .worker
                .outcome(&handle)
                .await
                .map(WorkerReplyBody::Outcome),
        };
        encode_control(&WorkerReply {
            request_id: request.request_id,
            result: result.map_err(|error| WorkerFailure::from(&error)),
        })
    }
}

pub struct WorkerEndpoint {
    node_id: NodeId,
    host: Arc<dyn HostAdapter>,
    localizer: Arc<dyn ResourceLocalizer>,
    state: Mutex<WorkerEndpointState>,
}

#[derive(Debug)]
struct WorkerEndpointState {
    connected: bool,
    reject_next: bool,
}

impl WorkerEndpoint {
    pub fn new(
        node_id: NodeId,
        host: Arc<dyn HostAdapter>,
        localizer: Arc<dyn ResourceLocalizer>,
    ) -> Self {
        Self {
            node_id,
            host,
            localizer,
            state: Mutex::new(WorkerEndpointState {
                connected: true,
                reject_next: false,
            }),
        }
    }

    pub fn set_connected(&self, connected: bool) {
        self.state.lock().expect("worker endpoint mutex").connected = connected;
    }

    pub fn reject_next(&self) {
        self.state
            .lock()
            .expect("worker endpoint mutex")
            .reject_next = true;
    }

    fn admit(&self) -> Result<(), DistributedError> {
        let mut state = self.state.lock().expect("worker endpoint mutex");
        if !state.connected {
            return Err(DistributedError::new(
                DistributedErrorKind::TransportClosed,
                "Worker Link session is disconnected",
            ));
        }
        if state.reject_next {
            state.reject_next = false;
            return Err(DistributedError::new(
                DistributedErrorKind::WorkerRejected,
                "Worker rejected remote admission",
            ));
        }
        Ok(())
    }
}

impl RemoteWorker for WorkerEndpoint {
    fn submit(&self, envelope: RemoteTaskEnvelope) -> WorkerFuture<'_, RemoteAccepted> {
        Box::pin(async move {
            self.admit()?;
            envelope.portable.validate_envelope().map_err(|_| {
                DistributedError::new(
                    DistributedErrorKind::Incompatible,
                    "portable task envelope is incompatible",
                )
            })?;
            self.localizer.localize(&envelope.direct_inputs).await?;
            let mut local_task = envelope.portable.into_local_task();
            local_task.task_id =
                format!("{}:attempt:{}", envelope.global_task_id.0, envelope.attempt);
            let handles = self
                .host
                .submit_batch(TaskBatch::one(
                    format!("{}:batch:{}", envelope.global_task_id.0, envelope.attempt),
                    local_task,
                ))
                .await?;
            let local_handle = handles.into_iter().next().ok_or_else(|| {
                DistributedError::new(
                    DistributedErrorKind::HostUnavailable,
                    "Worker Host returned no TaskHandle",
                )
            })?;
            Ok(RemoteAccepted {
                global_task_id: envelope.global_task_id,
                attempt: envelope.attempt,
                worker_node: self.node_id.clone(),
                local_handle,
            })
        })
    }

    fn cancel(&self, handle: &TaskHandle) -> WorkerFuture<'_, ()> {
        let handle = handle.clone();
        Box::pin(async move {
            self.admit()?;
            self.host.cancel(&handle).await
        })
    }

    fn outcome(&self, handle: &TaskHandle) -> WorkerFuture<'_, Option<LocalTaskOutcome>> {
        let handle = handle.clone();
        Box::pin(async move {
            self.admit()?;
            self.host.outcome(&handle).await
        })
    }
}

pub struct Coordinator {
    origin_node: NodeId,
    origin_host: Arc<dyn HostAdapter>,
    registry: Arc<Mutex<WorkerRegistry>>,
    workers: BTreeMap<NodeId, Arc<dyn RemoteWorker>>,
    records: Mutex<BTreeMap<GlobalTaskId, GlobalTaskRecord>>,
    max_tasks: usize,
    max_fallback_workers: usize,
}

impl Coordinator {
    pub fn new(
        origin_node: NodeId,
        origin_host: Arc<dyn HostAdapter>,
        registry: Arc<Mutex<WorkerRegistry>>,
        workers: BTreeMap<NodeId, Arc<dyn RemoteWorker>>,
        max_tasks: usize,
        max_fallback_workers: usize,
    ) -> Result<Self, DistributedError> {
        let invalid_fallback_limit = if workers.is_empty() {
            max_fallback_workers != 0
        } else {
            max_fallback_workers >= workers.len()
        };
        if max_tasks == 0 || invalid_fallback_limit {
            return Err(invalid_config());
        }
        Ok(Self {
            origin_node,
            origin_host,
            registry,
            workers,
            records: Mutex::new(BTreeMap::new()),
            max_tasks,
            max_fallback_workers,
        })
    }

    pub async fn submit(
        &self,
        global_task_id: GlobalTaskId,
        portable: PortableTask,
        requirements: RequirementSet,
        direct_inputs: Vec<DirectDataRef>,
    ) -> Result<TaskPlacement, DistributedError> {
        {
            let records = self.records.lock().expect("global task records mutex");
            if records.contains_key(&global_task_id) {
                return Err(DistributedError::new(
                    DistributedErrorKind::AttemptStale,
                    "global task id is already registered",
                ));
            }
            if records.len() >= self.max_tasks {
                return Err(DistributedError::new(
                    DistributedErrorKind::CapacityExceeded,
                    "global task registry capacity exceeded",
                ));
            }
        }

        let remote = if portable.capability.mobility == ExecutionMobility::LocalOnly {
            None
        } else {
            self.try_remote(
                &global_task_id,
                1,
                &portable,
                &requirements,
                &direct_inputs,
                &BTreeSet::new(),
            )
            .await?
        };
        let placement = match remote {
            Some(accepted) => TaskPlacement {
                kind: PlacementKind::Remote,
                global_task_id: global_task_id.clone(),
                attempt: 1,
                node_id: accepted.worker_node,
                local_handle: accepted.local_handle,
            },
            None => {
                self.submit_local(&global_task_id, 1, portable.clone())
                    .await?
            }
        };
        self.records
            .lock()
            .expect("global task records mutex")
            .insert(
                global_task_id.clone(),
                GlobalTaskRecord {
                    global_task_id,
                    portable,
                    requirements,
                    direct_inputs,
                    attempts: vec![AttemptRecord {
                        attempt: placement.attempt,
                        node_id: placement.node_id.clone(),
                        local_handle: placement.local_handle.clone(),
                        active: true,
                    }],
                },
            );
        Ok(placement)
    }

    async fn submit_local(
        &self,
        global_task_id: &GlobalTaskId,
        attempt: u32,
        portable: PortableTask,
    ) -> Result<TaskPlacement, DistributedError> {
        let mut task = portable.into_local_task();
        task.task_id = format!("{}:attempt:{attempt}", global_task_id.0);
        let handles = self
            .origin_host
            .submit_batch(TaskBatch::one(
                format!("{}:batch:{attempt}", global_task_id.0),
                task,
            ))
            .await?;
        let local_handle = handles.into_iter().next().ok_or_else(|| {
            DistributedError::new(
                DistributedErrorKind::HostUnavailable,
                "origin Host returned no TaskHandle",
            )
        })?;
        Ok(TaskPlacement {
            kind: PlacementKind::Local,
            global_task_id: global_task_id.clone(),
            attempt,
            node_id: self.origin_node.clone(),
            local_handle,
        })
    }

    async fn try_remote(
        &self,
        global_task_id: &GlobalTaskId,
        attempt: u32,
        portable: &PortableTask,
        requirements: &RequirementSet,
        direct_inputs: &[DirectDataRef],
        excluded: &BTreeSet<NodeId>,
    ) -> Result<Option<RemoteAccepted>, DistributedError> {
        let candidates = self
            .registry
            .lock()
            .expect("worker registry mutex")
            .candidates(portable, requirements, excluded);
        for node_id in candidates
            .into_iter()
            .take(self.max_fallback_workers.saturating_add(1))
        {
            let Some(worker) = self.workers.get(&node_id) else {
                continue;
            };
            let envelope = RemoteTaskEnvelope {
                global_task_id: global_task_id.clone(),
                attempt,
                origin_node: self.origin_node.clone(),
                requirements: requirements.clone(),
                portable: portable.clone(),
                direct_inputs: direct_inputs.to_vec(),
            };
            match worker.submit(envelope).await {
                Ok(accepted) => return Ok(Some(accepted)),
                Err(error)
                    if matches!(
                        error.kind,
                        DistributedErrorKind::WorkerRejected
                            | DistributedErrorKind::WorkerUnavailable
                            | DistributedErrorKind::TransportClosed
                    ) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }

    pub async fn cancel(&self, global_task_id: &GlobalTaskId) -> Result<(), DistributedError> {
        let active = self.active_attempt(global_task_id)?;
        if active.node_id == self.origin_node {
            self.origin_host.cancel(&active.local_handle).await
        } else {
            self.workers
                .get(&active.node_id)
                .ok_or_else(worker_unavailable)?
                .cancel(&active.local_handle)
                .await
        }
    }

    pub async fn outcome(
        &self,
        global_task_id: &GlobalTaskId,
    ) -> Result<Option<LocalTaskOutcome>, DistributedError> {
        let active = self.active_attempt(global_task_id)?;
        if active.node_id == self.origin_node {
            self.origin_host.outcome(&active.local_handle).await
        } else {
            self.workers
                .get(&active.node_id)
                .ok_or_else(worker_unavailable)?
                .outcome(&active.local_handle)
                .await
        }
    }

    pub async fn restart_after_disconnect(
        &self,
        global_task_id: &GlobalTaskId,
    ) -> Result<TaskPlacement, DistributedError> {
        let record = self
            .records
            .lock()
            .expect("global task records mutex")
            .get(global_task_id)
            .cloned()
            .ok_or_else(task_unknown)?;
        if !can_restart_from_input(&record.portable) {
            return Err(DistributedError::new(
                DistributedErrorKind::RetryUnsafe,
                "task cannot be safely restarted after Worker disconnect",
            ));
        }
        let attempt = record
            .attempts
            .last()
            .map_or(1, |attempt| attempt.attempt.saturating_add(1));
        let excluded = record
            .attempts
            .iter()
            .map(|attempt| attempt.node_id.clone())
            .collect();
        let remote = self
            .try_remote(
                global_task_id,
                attempt,
                &record.portable,
                &record.requirements,
                &record.direct_inputs,
                &excluded,
            )
            .await?;
        let placement = match remote {
            Some(accepted) => TaskPlacement {
                kind: PlacementKind::Remote,
                global_task_id: global_task_id.clone(),
                attempt,
                node_id: accepted.worker_node,
                local_handle: accepted.local_handle,
            },
            None => {
                self.submit_local(global_task_id, attempt, record.portable.clone())
                    .await?
            }
        };
        let mut records = self.records.lock().expect("global task records mutex");
        let current = records.get_mut(global_task_id).ok_or_else(task_unknown)?;
        for previous in &mut current.attempts {
            previous.active = false;
        }
        current.attempts.push(AttemptRecord {
            attempt,
            node_id: placement.node_id.clone(),
            local_handle: placement.local_handle.clone(),
            active: true,
        });
        Ok(placement)
    }

    pub fn accept_result(&self, result: RemoteResult) -> Result<RemoteResult, DistributedError> {
        let active = self.active_attempt(&result.global_task_id)?;
        if active.attempt != result.attempt || active.node_id != result.worker_node {
            return Err(DistributedError::new(
                DistributedErrorKind::AttemptStale,
                "remote result belongs to a stale attempt",
            ));
        }
        Ok(result)
    }

    pub fn record(&self, global_task_id: &GlobalTaskId) -> Option<GlobalTaskRecord> {
        self.records
            .lock()
            .expect("global task records mutex")
            .get(global_task_id)
            .cloned()
    }

    fn active_attempt(
        &self,
        global_task_id: &GlobalTaskId,
    ) -> Result<AttemptRecord, DistributedError> {
        self.records
            .lock()
            .expect("global task records mutex")
            .get(global_task_id)
            .and_then(GlobalTaskRecord::active_attempt)
            .cloned()
            .ok_or_else(task_unknown)
    }
}

pub struct Sidecar {
    mode: DistributionMode,
    host: Option<Arc<dyn HostAdapter>>,
    coordinator: Option<Arc<Coordinator>>,
}

impl Sidecar {
    pub const fn disabled() -> Self {
        Self {
            mode: DistributionMode::Disabled,
            host: None,
            coordinator: None,
        }
    }

    pub fn local_observable(host: Arc<dyn HostAdapter>) -> Self {
        Self {
            mode: DistributionMode::LocalObservable,
            host: Some(host),
            coordinator: None,
        }
    }

    pub fn clustered(host: Arc<dyn HostAdapter>, coordinator: Arc<Coordinator>) -> Self {
        Self {
            mode: DistributionMode::Clustered,
            host: Some(host),
            coordinator: Some(coordinator),
        }
    }

    pub const fn mode(&self) -> DistributionMode {
        self.mode
    }

    /// Construction never spawns sampling or network tasks. Link/session and
    /// polling drivers are owned explicitly by the process executor.
    pub const fn background_tasks(&self) -> usize {
        0
    }

    pub const fn opens_network_on_construction(&self) -> bool {
        false
    }

    pub async fn health(&self) -> Result<String, DistributedError> {
        match &self.host {
            Some(host) => host.health().await,
            None => Err(DistributedError::new(
                DistributedErrorKind::Disabled,
                "DistributedHost is disabled",
            )),
        }
    }

    pub fn coordinator(&self) -> Result<&Arc<Coordinator>, DistributedError> {
        self.coordinator.as_ref().ok_or_else(|| {
            DistributedError::new(
                DistributedErrorKind::Disabled,
                "clustered DistributedHost is not enabled",
            )
        })
    }

    pub fn poll_events(&self, sequence: u64, limit: usize) -> HostFuture<'_, Vec<RuntimeEvent>> {
        match &self.host {
            Some(host) => host.events_after(sequence, limit),
            None => Box::pin(async {
                Err(DistributedError::new(
                    DistributedErrorKind::Disabled,
                    "DistributedHost is disabled",
                ))
            }),
        }
    }

    pub fn task_snapshots(&self) -> HostFuture<'_, Vec<LocalTaskSnapshot>> {
        match &self.host {
            Some(host) => host.snapshots(),
            None => Box::pin(async { Err(disabled()) }),
        }
    }

    /// Explicit operator action only. Dropping a Sidecar never drains or stops
    /// the independently owned local Host.
    pub fn begin_local_drain(&self) -> HostFuture<'_, ()> {
        match &self.host {
            Some(host) => host.begin_drain(),
            None => Box::pin(async { Err(disabled()) }),
        }
    }
}

const fn invalid_config() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::InvalidConfig,
        "DistributedHost limits must be positive and bounded",
    )
}

const fn worker_unavailable() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::WorkerUnavailable,
        "Worker is unavailable",
    )
}

const fn task_unknown() -> DistributedError {
    DistributedError::new(DistributedErrorKind::TaskUnknown, "global task is unknown")
}

const fn unexpected_reply() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Protocol,
        "Worker returned an unexpected reply type",
    )
}

const fn disabled() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Disabled,
        "DistributedHost is disabled",
    )
}

#[cfg(test)]
mod durable_tests;
#[cfg(test)]
mod ha_tests;
#[cfg(test)]
mod recovery_tests;
#[cfg(test)]
mod scheduler_tests;
#[cfg(test)]
mod tests;
