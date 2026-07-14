use mutsuki_distributed_contracts::{
    ClusterAvailability, CommittedControlRecord, ControlLease, ControlNodeKind, ControlOperation,
    ControlRecord, ControlRecordKind, ControlRole, DistributedError, DistributedErrorKind,
    ExecutedUncommittedResult, ExecutionGrant, GlobalTaskId, MemberHealth, MemberPulseSummary,
    NodeId, ReconciliationDecision,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::MetadataReplica;

const MAX_CONSENSUS_RECORD_BYTES: usize = 64 * 1024;
const MAX_UNCOMMITTED_RESULTS: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlNodeSpec {
    pub node_id: NodeId,
    pub kind: ControlNodeKind,
    pub storage_path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ControlNodeSnapshot {
    node_id: NodeId,
    kind: ControlNodeKind,
    role: ControlRole,
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<CommittedControlRecord>,
}

struct ControlNode {
    snapshot: ControlNodeSnapshot,
    path: PathBuf,
    alive: bool,
}

impl ControlNode {
    fn open(spec: ControlNodeSpec) -> Result<Self, DistributedError> {
        let snapshot = if spec.storage_path.exists() {
            let bytes = fs::read(&spec.storage_path).map_err(|_| control_storage_error())?;
            let snapshot: ControlNodeSnapshot =
                serde_json::from_slice(&bytes).map_err(|_| control_corrupt_error())?;
            if snapshot.node_id != spec.node_id || snapshot.kind != spec.kind {
                return Err(control_corrupt_error());
            }
            snapshot
        } else {
            ControlNodeSnapshot {
                node_id: spec.node_id,
                kind: spec.kind,
                role: match spec.kind {
                    ControlNodeKind::Full => ControlRole::Follower,
                    ControlNodeKind::Witness => ControlRole::Witness,
                },
                current_term: 0,
                voted_for: None,
                log: Vec::new(),
            }
        };
        let node = Self {
            snapshot,
            path: spec.storage_path,
            alive: true,
        };
        node.persist()?;
        Ok(node)
    }

    fn persist(&self) -> Result<(), DistributedError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|_| control_storage_error())?;
        }
        let bytes = serde_json::to_vec(&self.snapshot).map_err(|_| control_corrupt_error())?;
        let temporary = self.path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|_| control_storage_error())?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| control_storage_error())?;
        fs::rename(temporary, &self.path).map_err(|_| control_storage_error())
    }

    fn last_index(&self) -> u64 {
        self.snapshot.log.last().map_or(0, |entry| entry.index)
    }
}

pub trait CftControlBackend {
    fn leader(&self) -> Option<&NodeId>;
    fn propose(
        &mut self,
        leader: &NodeId,
        record: ControlRecord,
    ) -> Result<CommittedControlRecord, DistributedError>;
    fn committed_records(
        &self,
        node: &NodeId,
    ) -> Result<Vec<CommittedControlRecord>, DistributedError>;
}

pub struct ReplicatedControlPlane {
    nodes: BTreeMap<NodeId, ControlNode>,
    links: BTreeMap<(NodeId, NodeId), bool>,
    leader: Option<NodeId>,
    control_lease: Option<ControlLease>,
    current_epoch: u64,
    grants: BTreeMap<GlobalTaskId, ExecutionGrant>,
    uncommitted_results: Vec<ExecutedUncommittedResult>,
}

impl ReplicatedControlPlane {
    pub fn open(specs: Vec<ControlNodeSpec>) -> Result<Self, DistributedError> {
        let mut nodes = BTreeMap::new();
        for spec in specs {
            if nodes.contains_key(&spec.node_id) {
                return Err(control_config_error());
            }
            nodes.insert(spec.node_id.clone(), ControlNode::open(spec)?);
        }
        let full = nodes
            .values()
            .filter(|node| node.snapshot.kind == ControlNodeKind::Full)
            .count();
        let witnesses = nodes
            .values()
            .filter(|node| node.snapshot.kind == ControlNodeKind::Witness)
            .count();
        if !((full >= 3) || (full >= 2 && witnesses >= 1)) {
            return Err(control_config_error());
        }
        for node in nodes.values_mut() {
            node.snapshot.role = match node.snapshot.kind {
                ControlNodeKind::Full => ControlRole::Follower,
                ControlNodeKind::Witness => ControlRole::Witness,
            };
            node.persist()?;
        }
        let current_epoch = nodes
            .values()
            .flat_map(|node| node.snapshot.log.iter())
            .map(|entry| entry.epoch)
            .max()
            .unwrap_or(0);
        let grants = rebuild_grants(&nodes)?;
        Ok(Self {
            nodes,
            links: BTreeMap::new(),
            leader: None,
            control_lease: None,
            current_epoch,
            grants,
            uncommitted_results: Vec::new(),
        })
    }

    pub fn quorum_size(&self) -> usize {
        self.voter_count() / 2 + 1
    }

    pub fn elect(&mut self, candidate: &NodeId) -> Result<u64, DistributedError> {
        let candidate_node = self.nodes.get(candidate).ok_or_else(node_unknown)?;
        if !candidate_node.alive || candidate_node.snapshot.kind != ControlNodeKind::Full {
            return Err(control_unavailable());
        }
        let term = self
            .nodes
            .values()
            .map(|node| node.snapshot.current_term)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let candidate_last_index = candidate_node.last_index();
        let voter_ids: Vec<_> = self
            .nodes
            .iter()
            .filter(|(_, node)| node.alive)
            .filter(|(id, _)| self.connected(candidate, id))
            .filter(|(_, node)| node.last_index() <= candidate_last_index)
            .map(|(id, _)| id.clone())
            .collect();
        if voter_ids.len() < self.quorum_size() {
            self.leader = None;
            return Err(quorum_lost());
        }
        for node in self.nodes.values_mut() {
            if node.alive && voter_ids.contains(&node.snapshot.node_id) {
                node.snapshot.current_term = term;
                node.snapshot.voted_for = Some(candidate.clone());
                node.snapshot.role = match node.snapshot.kind {
                    ControlNodeKind::Full if node.snapshot.node_id == *candidate => {
                        ControlRole::Leader
                    }
                    ControlNodeKind::Full => ControlRole::Follower,
                    ControlNodeKind::Witness => ControlRole::Witness,
                };
                node.persist()?;
            }
        }
        self.leader = Some(candidate.clone());
        self.control_lease = None;
        Ok(term)
    }

    pub fn set_alive(&mut self, node_id: &NodeId, alive: bool) -> Result<(), DistributedError> {
        let node = self.nodes.get_mut(node_id).ok_or_else(node_unknown)?;
        node.alive = alive;
        if !alive && self.leader.as_ref() == Some(node_id) {
            self.leader = None;
            self.control_lease = None;
        }
        Ok(())
    }

    pub fn isolate(&mut self, node_id: &NodeId, isolated: bool) -> Result<(), DistributedError> {
        if !self.nodes.contains_key(node_id) {
            return Err(node_unknown());
        }
        let ids: Vec<_> = self.nodes.keys().cloned().collect();
        for other in ids {
            if other != *node_id {
                self.links.insert(link_key(node_id, &other), !isolated);
            }
        }
        if isolated && self.leader.as_ref() == Some(node_id) {
            self.leader = None;
            self.control_lease = None;
        }
        Ok(())
    }

    pub fn recover(&mut self, node_id: &NodeId) -> Result<(), DistributedError> {
        self.set_alive(node_id, true)?;
        let Some(leader_id) = self.leader.clone() else {
            return Ok(());
        };
        if !self.connected(node_id, &leader_id) {
            return Ok(());
        }
        let leader = self.nodes.get(&leader_id).ok_or_else(node_unknown)?;
        let log = leader.snapshot.log.clone();
        let term = leader.snapshot.current_term;
        let node = self.nodes.get_mut(node_id).ok_or_else(node_unknown)?;
        node.snapshot.log = log;
        node.snapshot.current_term = term;
        node.snapshot.voted_for = Some(leader_id);
        node.snapshot.role = match node.snapshot.kind {
            ControlNodeKind::Full => ControlRole::Follower,
            ControlNodeKind::Witness => ControlRole::Witness,
        };
        node.persist()
    }

    pub fn node_role(&self, node_id: &NodeId) -> Result<ControlRole, DistributedError> {
        self.nodes
            .get(node_id)
            .map(|node| node.snapshot.role)
            .ok_or_else(node_unknown)
    }

    pub fn current_term(&self) -> u64 {
        self.leader
            .as_ref()
            .and_then(|leader| self.nodes.get(leader))
            .map_or(0, |node| node.snapshot.current_term)
    }

    pub fn availability(&self, observer: &NodeId, tick: u64) -> ClusterAvailability {
        let Some(node) = self.nodes.get(observer) else {
            return ClusterAvailability::SafeStop;
        };
        if !node.alive {
            return ClusterAvailability::SafeStop;
        }
        let reachable = self.reachable_voters(observer);
        if reachable < self.quorum_size() {
            if self.leader.as_ref() == Some(observer) {
                return ClusterAvailability::QuorumLost;
            }
            return if self.has_valid_grant_for(observer, tick) {
                ClusterAvailability::Isolated
            } else {
                ClusterAvailability::SafeStop
            };
        }
        let Some(leader) = &self.leader else {
            return ClusterAvailability::Degraded;
        };
        if !self.connected(observer, leader) {
            return ClusterAvailability::Isolated;
        }
        if reachable == self.voter_count() {
            ClusterAvailability::Healthy
        } else if reachable == self.quorum_size() {
            ClusterAvailability::Degraded
        } else {
            ClusterAvailability::Impaired
        }
    }

    pub fn authorize(
        &self,
        observer: &NodeId,
        operation: ControlOperation,
        grant: Option<&ExecutionGrant>,
        tick: u64,
    ) -> Result<(), DistributedError> {
        match operation {
            ControlOperation::Query | ControlOperation::LocalWork => Ok(()),
            ControlOperation::ContinueGranted => {
                let grant = grant.ok_or_else(grant_expired)?;
                self.validate_grant(observer, grant, tick)
            }
            ControlOperation::DurableWrite
            | ControlOperation::MembershipChange
            | ControlOperation::GenerationSwitch
            | ControlOperation::IrreversibleEffect => {
                if !self.has_leader_quorum(observer) {
                    return Err(quorum_lost());
                }
                if self.control_lease.as_ref().is_some_and(|lease| {
                    lease.leader_node == *observer
                        && lease.term == self.current_term()
                        && lease.is_valid_at(tick)
                }) {
                    Ok(())
                } else {
                    Err(control_lease_expired())
                }
            }
        }
    }

    pub fn renew_control_lease(
        &mut self,
        leader: &NodeId,
        issued_tick: u64,
        valid_for_ticks: u64,
    ) -> Result<ControlLease, DistributedError> {
        if valid_for_ticks == 0 || !self.has_leader_quorum(leader) {
            return Err(quorum_lost());
        }
        let lease = ControlLease {
            leader_node: leader.clone(),
            term: self.current_term(),
            issued_tick,
            valid_until_tick: issued_tick.saturating_add(valid_for_ticks),
        };
        self.control_lease = Some(lease.clone());
        Ok(lease)
    }

    pub fn control_lease(&self) -> Option<&ControlLease> {
        self.control_lease.as_ref()
    }

    pub fn propose_from(
        &mut self,
        ingress: &NodeId,
        record: ControlRecord,
        tick: u64,
    ) -> Result<CommittedControlRecord, DistributedError> {
        let leader = self.leader.clone().ok_or_else(not_leader)?;
        if !self.connected(ingress, &leader)
            || !self.nodes.get(ingress).is_some_and(|node| node.alive)
        {
            return Err(not_leader());
        }
        self.authorize(&leader, ControlOperation::DurableWrite, None, tick)?;
        self.propose(&leader, record)
    }

    pub fn issue_grant(
        &mut self,
        leader: &NodeId,
        global_task_id: GlobalTaskId,
        attempt: u32,
        worker_node: NodeId,
        issued_tick: u64,
        valid_for_ticks: u64,
        irreversible_effects: bool,
    ) -> Result<ExecutionGrant, DistributedError> {
        if attempt == 0 || valid_for_ticks == 0 {
            return Err(control_config_error());
        }
        self.authorize(
            leader,
            if irreversible_effects {
                ControlOperation::IrreversibleEffect
            } else {
                ControlOperation::DurableWrite
            },
            None,
            issued_tick,
        )?;
        let grant = ExecutionGrant {
            global_task_id: global_task_id.clone(),
            attempt,
            worker_node,
            term: self.current_term(),
            epoch: self.current_epoch.saturating_add(1),
            issued_tick,
            valid_until_tick: issued_tick.saturating_add(valid_for_ticks),
            irreversible_effects,
        };
        let mut metadata = BTreeMap::new();
        metadata.insert("global_task_id".into(), global_task_id.0.clone());
        metadata.insert("attempt".into(), attempt.to_string());
        metadata.insert("worker_node".into(), grant.worker_node.0.clone());
        metadata.insert("issued_tick".into(), issued_tick.to_string());
        metadata.insert(
            "valid_until_tick".into(),
            grant.valid_until_tick.to_string(),
        );
        metadata.insert(
            "irreversible_effects".into(),
            irreversible_effects.to_string(),
        );
        self.propose(
            leader,
            ControlRecord {
                record_id: format!("grant:{}:{}", global_task_id.0, grant.epoch),
                kind: ControlRecordKind::ExecutionGrant,
                metadata,
            },
        )?;
        self.grants.insert(global_task_id, grant.clone());
        Ok(grant)
    }

    pub fn validate_result(
        &self,
        grant: &ExecutionGrant,
        tick: u64,
    ) -> Result<(), DistributedError> {
        if !grant.is_valid_at(tick) {
            return Err(grant_expired());
        }
        if self.grants.get(&grant.global_task_id) != Some(grant) {
            return Err(fenced());
        }
        Ok(())
    }

    pub fn record_uncommitted_result(
        &mut self,
        observer: &NodeId,
        result: ExecutedUncommittedResult,
        tick: u64,
    ) -> Result<(), DistributedError> {
        if self.has_leader_quorum(observer)
            || self.uncommitted_results.len() >= MAX_UNCOMMITTED_RESULTS
        {
            return Err(control_config_error());
        }
        let grant = self.grants.get(&result.global_task_id).ok_or_else(fenced)?;
        if grant.attempt != result.attempt
            || grant.worker_node != result.worker_node
            || grant.term != result.grant_term
            || grant.epoch != result.grant_epoch
        {
            return Err(fenced());
        }
        self.validate_grant(observer, grant, tick)?;
        self.uncommitted_results.push(result);
        Ok(())
    }

    pub fn reconcile(
        &mut self,
        leader: &NodeId,
        global_task_id: &GlobalTaskId,
        decision: ReconciliationDecision,
        tick: u64,
    ) -> Result<ExecutedUncommittedResult, DistributedError> {
        self.authorize(leader, ControlOperation::DurableWrite, None, tick)?;
        let index = self
            .uncommitted_results
            .iter()
            .position(|result| &result.global_task_id == global_task_id)
            .ok_or_else(|| {
                DistributedError::new(
                    DistributedErrorKind::TaskUnknown,
                    "executed-uncommitted result is unknown",
                )
            })?;
        let result = self.uncommitted_results[index].clone();
        let mut metadata = BTreeMap::new();
        metadata.insert("global_task_id".into(), global_task_id.0.clone());
        metadata.insert("decision".into(), format!("{decision:?}"));
        self.propose(
            leader,
            ControlRecord {
                record_id: format!("reconcile:{}:{}", global_task_id.0, self.current_epoch + 1),
                kind: ControlRecordKind::Reconciliation,
                metadata,
            },
        )?;
        self.uncommitted_results.remove(index);
        Ok(result)
    }

    fn validate_grant(
        &self,
        observer: &NodeId,
        grant: &ExecutionGrant,
        tick: u64,
    ) -> Result<(), DistributedError> {
        if &grant.worker_node != observer || !grant.is_valid_at(tick) {
            return Err(grant_expired());
        }
        if self.grants.get(&grant.global_task_id) != Some(grant) {
            return Err(fenced());
        }
        Ok(())
    }

    fn has_valid_grant_for(&self, node_id: &NodeId, tick: u64) -> bool {
        self.grants
            .values()
            .any(|grant| &grant.worker_node == node_id && grant.is_valid_at(tick))
    }

    fn has_leader_quorum(&self, observer: &NodeId) -> bool {
        self.leader.as_ref().is_some_and(|leader| {
            leader == observer
                && self.nodes.get(leader).is_some_and(|node| node.alive)
                && self.reachable_voters(leader) >= self.quorum_size()
        })
    }

    fn voter_count(&self) -> usize {
        self.nodes.len()
    }

    fn reachable_voters(&self, observer: &NodeId) -> usize {
        self.nodes
            .iter()
            .filter(|(_, node)| node.alive)
            .filter(|(node_id, _)| self.connected(observer, node_id))
            .count()
    }

    fn connected(&self, left: &NodeId, right: &NodeId) -> bool {
        left == right
            || self
                .links
                .get(&link_key(left, right))
                .copied()
                .unwrap_or(true)
    }
}

impl CftControlBackend for ReplicatedControlPlane {
    fn leader(&self) -> Option<&NodeId> {
        self.leader.as_ref()
    }

    fn propose(
        &mut self,
        leader: &NodeId,
        record: ControlRecord,
    ) -> Result<CommittedControlRecord, DistributedError> {
        if self.leader.as_ref() != Some(leader) {
            return Err(not_leader());
        }
        if !self.has_leader_quorum(leader) {
            return Err(quorum_lost());
        }
        let encoded = serde_json::to_vec(&record).map_err(|_| control_corrupt_error())?;
        if encoded.len() > MAX_CONSENSUS_RECORD_BYTES {
            return Err(DistributedError::new(
                DistributedErrorKind::CapacityExceeded,
                "control record exceeds the bounded consensus limit",
            ));
        }
        let term = self.current_term();
        let epoch = self.current_epoch.saturating_add(1);
        let index = self
            .nodes
            .get(leader)
            .ok_or_else(node_unknown)?
            .last_index()
            .saturating_add(1);
        let committed = CommittedControlRecord {
            index,
            term,
            epoch,
            record,
        };
        let targets: Vec<_> = self
            .nodes
            .iter()
            .filter(|(_, node)| node.alive)
            .filter(|(node_id, _)| self.connected(leader, node_id))
            .map(|(node_id, _)| node_id.clone())
            .collect();
        let mut persisted = Vec::new();
        for node_id in targets {
            let node = self.nodes.get_mut(&node_id).ok_or_else(node_unknown)?;
            node.snapshot.log.push(committed.clone());
            if node.persist().is_ok() {
                persisted.push(node_id);
            } else {
                node.snapshot.log.pop();
            }
        }
        if persisted.len() < self.quorum_size() {
            for node_id in persisted {
                if let Some(node) = self.nodes.get_mut(&node_id) {
                    node.snapshot.log.pop();
                    let _ = node.persist();
                }
            }
            return Err(quorum_lost());
        }
        self.current_epoch = epoch;
        Ok(committed)
    }

    fn committed_records(
        &self,
        node: &NodeId,
    ) -> Result<Vec<CommittedControlRecord>, DistributedError> {
        self.nodes
            .get(node)
            .map(|node| node.snapshot.log.clone())
            .ok_or_else(node_unknown)
    }
}

/// Bridges the Phase 4 registry WAL into the bounded CFT metadata log. The
/// CFT backend fsyncs each accepted record on a quorum, so `sync` does not
/// change behavior here.
pub struct CftRegistryReplica {
    control: Arc<Mutex<ReplicatedControlPlane>>,
    ingress: NodeId,
    next_tick: AtomicU64,
    sequence: AtomicU64,
}

impl CftRegistryReplica {
    pub fn new(
        control: Arc<Mutex<ReplicatedControlPlane>>,
        ingress: NodeId,
        first_tick: u64,
    ) -> Self {
        Self {
            control,
            ingress,
            next_tick: AtomicU64::new(first_tick),
            sequence: AtomicU64::new(1),
        }
    }

    pub fn restore_registry_wal(
        control: &ReplicatedControlPlane,
        node_id: &NodeId,
        destination: impl Into<PathBuf>,
    ) -> Result<(), DistributedError> {
        let destination = destination.into();
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|_| control_storage_error())?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(destination)
            .map_err(|_| control_storage_error())?;
        for entry in control.committed_records(node_id)? {
            if let Some(registry_record) = entry.record.metadata.get("registry_record") {
                file.write_all(registry_record.as_bytes())
                    .and_then(|()| file.write_all(b"\n"))
                    .map_err(|_| control_storage_error())?;
            }
        }
        file.sync_all().map_err(|_| control_storage_error())
    }
}

impl MetadataReplica for CftRegistryReplica {
    fn append(&self, record: &[u8], _sync: bool) -> Result<(), DistributedError> {
        let record = std::str::from_utf8(record).map_err(|_| control_corrupt_error())?;
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let tick = self.next_tick.fetch_add(1, Ordering::Relaxed);
        self.control
            .lock()
            .expect("CFT registry replica mutex")
            .propose_from(
                &self.ingress,
                ControlRecord {
                    record_id: format!("registry:{sequence}"),
                    kind: ControlRecordKind::GlobalTask,
                    metadata: BTreeMap::from([("registry_record".into(), record.to_owned())]),
                },
                tick,
            )?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PulseDisposition {
    Accepted,
    FullSnapshotRequired,
}

#[derive(Clone, Debug)]
struct MemberObservation {
    summary: MemberPulseSummary,
    last_tick: u64,
}

pub struct FailureDetector {
    members: BTreeMap<NodeId, MemberObservation>,
    max_members: usize,
    suspect_after_ticks: u64,
    dead_after_ticks: u64,
    active_interval_ticks: u64,
    stable_interval_ticks: u64,
}

impl FailureDetector {
    pub fn new(
        max_members: usize,
        suspect_after_ticks: u64,
        dead_after_ticks: u64,
        active_interval_ticks: u64,
        stable_interval_ticks: u64,
    ) -> Result<Self, DistributedError> {
        if max_members == 0
            || suspect_after_ticks == 0
            || dead_after_ticks <= suspect_after_ticks
            || active_interval_ticks == 0
            || stable_interval_ticks < active_interval_ticks
        {
            return Err(control_config_error());
        }
        Ok(Self {
            members: BTreeMap::new(),
            max_members,
            suspect_after_ticks,
            dead_after_ticks,
            active_interval_ticks,
            stable_interval_ticks,
        })
    }

    pub fn register_full(
        &mut self,
        summary: MemberPulseSummary,
        tick: u64,
    ) -> Result<(), DistributedError> {
        if summary.capability_version == 0 || summary.resource_version == 0 {
            return Err(control_config_error());
        }
        if !self.members.contains_key(&summary.node_id) && self.members.len() >= self.max_members {
            return Err(DistributedError::new(
                DistributedErrorKind::CapacityExceeded,
                "member detector capacity exceeded",
            ));
        }
        self.members.insert(
            summary.node_id.clone(),
            MemberObservation {
                summary,
                last_tick: tick,
            },
        );
        Ok(())
    }

    pub fn pulse(
        &mut self,
        pulse: &MemberPulseSummary,
        tick: u64,
    ) -> Result<PulseDisposition, DistributedError> {
        let member = self
            .members
            .get_mut(&pulse.node_id)
            .ok_or_else(node_unknown)?;
        if member.summary.capability_version != pulse.capability_version
            || member.summary.resource_version != pulse.resource_version
        {
            member.summary.health = MemberHealth::Incompatible;
            return Ok(PulseDisposition::FullSnapshotRequired);
        }
        member.summary.pressure_bucket = pulse.pressure_bucket;
        member.summary.health = pulse.health;
        member.last_tick = tick;
        Ok(PulseDisposition::Accepted)
    }

    pub fn advance(&mut self, tick: u64) {
        for member in self.members.values_mut() {
            let silence = tick.saturating_sub(member.last_tick);
            if silence >= self.dead_after_ticks {
                member.summary.health = MemberHealth::Dead;
            } else if silence >= self.suspect_after_ticks {
                member.summary.health = MemberHealth::Suspect;
            }
        }
    }

    pub fn health(&self, node_id: &NodeId) -> Option<MemberHealth> {
        self.members
            .get(node_id)
            .map(|member| member.summary.health)
    }

    pub fn next_pulse_after(&self, node_id: &NodeId) -> Option<u64> {
        self.members.get(node_id).map(|member| {
            if (matches!(member.summary.health, MemberHealth::Healthy)
                && member.summary.pressure_bucket == 0)
                || matches!(
                    member.summary.health,
                    MemberHealth::Overloaded | MemberHealth::Draining
                )
            {
                self.stable_interval_ticks
            } else {
                self.active_interval_ticks
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LeaderMetrics {
    pub p95_network_ms: f64,
    pub storage_healthy: bool,
    pub sleep_risk: f64,
    pub control_capacity: f64,
    pub trusted: bool,
}

impl LeaderMetrics {
    fn score(self) -> Option<f64> {
        if !self.storage_healthy || !self.trusted {
            return None;
        }
        Some(self.control_capacity - self.p95_network_ms - self.sleep_risk * 100.0)
    }
}

pub struct LeadershipPreference {
    margin: f64,
    dwell_ticks: u64,
    preferred: Option<(NodeId, u64)>,
}

impl LeadershipPreference {
    pub fn new(margin: f64, dwell_ticks: u64) -> Result<Self, DistributedError> {
        if !margin.is_finite() || margin <= 0.0 || dwell_ticks == 0 {
            return Err(control_config_error());
        }
        Ok(Self {
            margin,
            dwell_ticks,
            preferred: None,
        })
    }

    pub fn should_transfer(
        &mut self,
        current: (&NodeId, LeaderMetrics),
        candidate: (&NodeId, LeaderMetrics),
        tick: u64,
    ) -> bool {
        let Some(current_score) = current.1.score() else {
            return candidate.1.score().is_some();
        };
        let Some(candidate_score) = candidate.1.score() else {
            self.preferred = None;
            return false;
        };
        if candidate_score < current_score + self.margin {
            self.preferred = None;
            return false;
        }
        match &self.preferred {
            Some((node_id, since)) if node_id == candidate.0 => {
                tick.saturating_sub(*since) >= self.dwell_ticks
            }
            _ => {
                self.preferred = Some((candidate.0.clone(), tick));
                false
            }
        }
    }
}

fn rebuild_grants(
    nodes: &BTreeMap<NodeId, ControlNode>,
) -> Result<BTreeMap<GlobalTaskId, ExecutionGrant>, DistributedError> {
    let Some(source) = nodes.values().max_by_key(|node| node.last_index()) else {
        return Ok(BTreeMap::new());
    };
    let mut grants = BTreeMap::new();
    for entry in source
        .snapshot
        .log
        .iter()
        .filter(|entry| entry.record.kind == ControlRecordKind::ExecutionGrant)
    {
        let metadata = &entry.record.metadata;
        let global_task_id = GlobalTaskId(
            metadata
                .get("global_task_id")
                .cloned()
                .ok_or_else(control_corrupt_error)?,
        );
        let grant = ExecutionGrant {
            global_task_id: global_task_id.clone(),
            attempt: parse_metadata(metadata, "attempt")?,
            worker_node: NodeId(
                metadata
                    .get("worker_node")
                    .cloned()
                    .ok_or_else(control_corrupt_error)?,
            ),
            term: entry.term,
            epoch: entry.epoch,
            issued_tick: parse_metadata(metadata, "issued_tick")?,
            valid_until_tick: parse_metadata(metadata, "valid_until_tick")?,
            irreversible_effects: parse_metadata(metadata, "irreversible_effects")?,
        };
        grants.insert(global_task_id, grant);
    }
    Ok(grants)
}

fn parse_metadata<T: std::str::FromStr>(
    metadata: &BTreeMap<String, String>,
    key: &str,
) -> Result<T, DistributedError> {
    metadata
        .get(key)
        .and_then(|value| value.parse().ok())
        .ok_or_else(control_corrupt_error)
}

fn link_key(left: &NodeId, right: &NodeId) -> (NodeId, NodeId) {
    if left <= right {
        (left.clone(), right.clone())
    } else {
        (right.clone(), left.clone())
    }
}

const fn control_storage_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Storage,
        "control plane storage operation failed",
    )
}

const fn control_corrupt_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Corrupt,
        "control plane state is corrupt",
    )
}

const fn control_config_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::InvalidConfig,
        "control plane configuration is invalid",
    )
}

const fn control_unavailable() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::WorkerUnavailable,
        "control node is unavailable",
    )
}

const fn quorum_lost() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::QuorumLost,
        "control plane quorum is unavailable",
    )
}

const fn node_unknown() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::WorkerUnavailable,
        "control node is unknown",
    )
}

const fn not_leader() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::NotLeader,
        "control write must be submitted to the current Leader",
    )
}

const fn fenced() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Fenced,
        "term, epoch, or attempt has been fenced",
    )
}

const fn grant_expired() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::GrantExpired,
        "execution grant is absent or expired",
    )
}

const fn control_lease_expired() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::ControlLeaseExpired,
        "control lease is absent, expired, or belongs to another term",
    )
}
