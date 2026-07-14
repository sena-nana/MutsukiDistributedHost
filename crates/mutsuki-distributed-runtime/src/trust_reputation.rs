use mutsuki_distributed_contracts::{
    AnomalyKind, CompromiseImpact, DistributedError, DistributedErrorKind, GlobalTaskId, NodeId,
    ReputationSnapshot,
};
use mutsuki_runtime_contracts::ContentId;
use std::collections::{BTreeMap, BTreeSet};

use crate::{NodeIdentityRegistry, ResourceAuthorizer};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ReputationKey {
    node_id: NodeId,
    capability: String,
    plugin_generation: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReputationObservationFlags(pub u8);

impl ReputationObservationFlags {
    pub const SUCCEEDED: Self = Self(1 << 0);
    pub const TIMED_OUT: Self = Self(1 << 1);
    pub const RESULT_MISMATCH: Self = Self(1 << 2);
    pub const RESOURCE_CORRUPTION: Self = Self(1 << 3);
    pub const CANCEL_REFUSED: Self = Self(1 << 4);
    pub const UNAUTHORIZED_ACCESS: Self = Self(1 << 5);
    pub const FORGED_CAPABILITY: Self = Self(1 << 6);

    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub const fn contains(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReputationObservation {
    pub flags: ReputationObservationFlags,
    pub latency_ratio: f64,
}

pub struct ReputationModel {
    max_buckets: usize,
    minimum_samples: u32,
    buckets: BTreeMap<ReputationKey, ReputationSnapshot>,
}

impl ReputationModel {
    pub fn new(max_buckets: usize, minimum_samples: u32) -> Result<Self, DistributedError> {
        if max_buckets == 0 || minimum_samples == 0 {
            return Err(DistributedError::new(
                DistributedErrorKind::InvalidConfig,
                "reputation model must be bounded and require samples",
            ));
        }
        Ok(Self {
            max_buckets,
            minimum_samples,
            buckets: BTreeMap::new(),
        })
    }

    pub fn record(
        &mut self,
        node_id: NodeId,
        capability: String,
        plugin_generation: u64,
        observation: ReputationObservation,
    ) -> Result<ReputationSnapshot, DistributedError> {
        if plugin_generation == 0
            || !observation.latency_ratio.is_finite()
            || observation.latency_ratio < 0.0
        {
            return Err(DistributedError::new(
                DistributedErrorKind::Incompatible,
                "reputation observation is invalid",
            ));
        }
        let key = ReputationKey {
            node_id: node_id.clone(),
            capability: capability.clone(),
            plugin_generation,
        };
        if !self.buckets.contains_key(&key) && self.buckets.len() >= self.max_buckets {
            let evict = self.buckets.keys().next().cloned().ok_or_else(|| {
                DistributedError::new(
                    DistributedErrorKind::InvalidConfig,
                    "reputation model has no eviction candidate",
                )
            })?;
            self.buckets.remove(&evict);
        }
        let snapshot = self.buckets.entry(key).or_insert(ReputationSnapshot {
            node_id,
            capability,
            plugin_generation,
            samples: 0,
            reliability_ewma: 0.5,
            timeout_ewma: 0.0,
            mismatch_ewma: 0.0,
            corruption_ewma: 0.0,
            uncertainty_penalty: 1.0,
            anomalies: BTreeSet::new(),
        });
        snapshot.samples = snapshot.samples.saturating_add(1);
        snapshot.reliability_ewma = slow_ewma(
            snapshot.reliability_ewma,
            observation
                .flags
                .contains(ReputationObservationFlags::SUCCEEDED),
        );
        snapshot.timeout_ewma = slow_ewma(
            snapshot.timeout_ewma,
            observation
                .flags
                .contains(ReputationObservationFlags::TIMED_OUT),
        );
        snapshot.mismatch_ewma = slow_ewma(
            snapshot.mismatch_ewma,
            observation
                .flags
                .contains(ReputationObservationFlags::RESULT_MISMATCH),
        );
        snapshot.corruption_ewma = slow_ewma(
            snapshot.corruption_ewma,
            observation
                .flags
                .contains(ReputationObservationFlags::RESOURCE_CORRUPTION),
        );
        let missing = self.minimum_samples.saturating_sub(snapshot.samples);
        snapshot.uncertainty_penalty = f64::from(missing) / f64::from(self.minimum_samples.max(1));
        if snapshot.samples >= self.minimum_samples {
            if observation
                .flags
                .contains(ReputationObservationFlags::FORGED_CAPABILITY)
            {
                snapshot.anomalies.insert(AnomalyKind::ForgedCapability);
            }
            if snapshot.mismatch_ewma >= 0.2 {
                snapshot.anomalies.insert(AnomalyKind::ResultMismatch);
            }
            if snapshot.corruption_ewma >= 0.2 {
                snapshot.anomalies.insert(AnomalyKind::ResourceCorruption);
            }
            if observation.latency_ratio >= 4.0 {
                snapshot.anomalies.insert(AnomalyKind::AbnormalLatency);
            }
            if observation
                .flags
                .contains(ReputationObservationFlags::CANCEL_REFUSED)
            {
                snapshot.anomalies.insert(AnomalyKind::CancelRefused);
            }
            if observation
                .flags
                .contains(ReputationObservationFlags::UNAUTHORIZED_ACCESS)
            {
                snapshot.anomalies.insert(AnomalyKind::UnauthorizedAccess);
            }
        }
        Ok(snapshot.clone())
    }

    pub fn decay(&mut self) {
        for snapshot in self.buckets.values_mut() {
            snapshot.reliability_ewma = snapshot.reliability_ewma.mul_add(0.99, 0.005);
            snapshot.timeout_ewma *= 0.99;
            snapshot.mismatch_ewma *= 0.99;
            snapshot.corruption_ewma *= 0.99;
        }
    }

    pub fn scheduling_risk(
        &self,
        node_id: &NodeId,
        capability: &str,
        plugin_generation: u64,
    ) -> f64 {
        self.buckets
            .get(&ReputationKey {
                node_id: node_id.clone(),
                capability: capability.into(),
                plugin_generation,
            })
            .map_or(1.0, |snapshot| {
                (1.0 - snapshot.reliability_ewma)
                    + snapshot.timeout_ewma
                    + snapshot.mismatch_ewma * 2.0
                    + snapshot.corruption_ewma * 2.0
                    + snapshot.uncertainty_penalty
            })
    }

    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }
}

#[derive(Default)]
pub struct CompromiseTracker {
    content_by_node: BTreeMap<NodeId, Vec<ContentId>>,
    tasks_by_node: BTreeMap<NodeId, BTreeSet<GlobalTaskId>>,
    fencing_epoch: u64,
}

impl CompromiseTracker {
    pub fn new(fencing_epoch: u64) -> Result<Self, DistributedError> {
        if fencing_epoch == 0 {
            return Err(DistributedError::new(
                DistributedErrorKind::InvalidConfig,
                "compromise tracker requires a fencing epoch",
            ));
        }
        Ok(Self {
            content_by_node: BTreeMap::new(),
            tasks_by_node: BTreeMap::new(),
            fencing_epoch,
        })
    }

    pub fn record_output(
        &mut self,
        node_id: NodeId,
        global_task_id: GlobalTaskId,
        content_id: ContentId,
    ) {
        self.content_by_node
            .entry(node_id.clone())
            .or_default()
            .push(content_id);
        self.tasks_by_node
            .entry(node_id)
            .or_default()
            .insert(global_task_id);
    }

    pub fn isolate(
        &mut self,
        registry: &mut NodeIdentityRegistry,
        authorizer: &mut ResourceAuthorizer,
        node_id: &NodeId,
    ) -> Result<CompromiseImpact, DistributedError> {
        registry.quarantine(node_id)?;
        self.fencing_epoch = self.fencing_epoch.saturating_add(1);
        let revoked_authorizations = authorizer.revoke_for_node(node_id);
        let quarantined_content = self.content_by_node.remove(node_id).unwrap_or_default();
        let affected_tasks = self.tasks_by_node.remove(node_id).unwrap_or_default();
        let mut required_actions = vec![
            "verify_or_recompute_committed_results".into(),
            "repair_from_trusted_replica".into(),
            "rotate_affected_access_keys".into(),
            "preserve_audit_evidence".into(),
        ];
        if !affected_tasks.is_empty() {
            required_actions.push("rollback_compensate_or_manual_review".into());
        }
        Ok(CompromiseImpact {
            node_id: node_id.clone(),
            new_fencing_epoch: self.fencing_epoch,
            revoked_authorizations,
            quarantined_content,
            affected_tasks,
            required_actions,
        })
    }

    pub const fn fencing_epoch(&self) -> u64 {
        self.fencing_epoch
    }
}

fn slow_ewma(previous: f64, observed: bool) -> f64 {
    previous.mul_add(0.95, f64::from(u8::from(observed)) * 0.05)
}
