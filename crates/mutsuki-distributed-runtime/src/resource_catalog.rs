use crate::ContentStore;
use mutsuki_distributed_contracts::{
    AcceptanceMode, DistributedError, DistributedErrorKind, ReplicaHealth, ReplicaRecord,
    ReplicaTarget, ResourceCatalogRecord, ResourcePolicy,
};
use mutsuki_runtime_contracts::ContentId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairJob {
    pub content_id: ContentId,
    pub missing_copies: usize,
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CatalogSnapshot {
    records: BTreeMap<String, ResourceCatalogRecord>,
}

pub struct ResourceCatalog {
    path: PathBuf,
    records: BTreeMap<String, ResourceCatalogRecord>,
    max_records: usize,
    max_repair_jobs: usize,
}

impl ResourceCatalog {
    pub fn open(
        path: impl Into<PathBuf>,
        max_records: usize,
        max_repair_jobs: usize,
    ) -> Result<Self, DistributedError> {
        if max_records == 0 || max_repair_jobs == 0 {
            return Err(registry_config_error());
        }
        let path = path.into();
        let records = if path.exists() {
            let bytes = fs::read(&path).map_err(|_| registry_storage_error())?;
            let snapshot: CatalogSnapshot =
                serde_json::from_slice(&bytes).map_err(|_| registry_corrupt_error())?;
            snapshot.records
        } else {
            BTreeMap::new()
        };
        if records.len() > max_records {
            return Err(registry_capacity_error());
        }
        Ok(Self {
            path,
            records,
            max_records,
            max_repair_jobs,
        })
    }

    pub fn register(
        &mut self,
        manifest: mutsuki_distributed_contracts::ContentManifest,
        replicas: Vec<ReplicaRecord>,
        reference_count: u64,
        retain_until_epoch: u64,
    ) -> Result<(), DistributedError> {
        if !self.records.contains_key(&manifest.content_id.digest)
            && self.records.len() >= self.max_records
        {
            return Err(registry_capacity_error());
        }
        let mut record = ResourceCatalogRecord {
            manifest,
            replicas,
            reference_count,
            retain_until_epoch,
            repair_required: false,
        };
        record.repair_required = needs_repair(&record);
        self.records
            .insert(record.manifest.content_id.digest.clone(), record);
        self.persist()
    }

    pub fn record_replica(
        &mut self,
        content_id: &ContentId,
        target: ReplicaTarget,
        health: ReplicaHealth,
        verified_at_epoch: u64,
    ) -> Result<(), DistributedError> {
        let record = self
            .records
            .get_mut(&content_id.digest)
            .ok_or_else(durability_unavailable)?;
        if let Some(replica) = record
            .replicas
            .iter_mut()
            .find(|replica| replica.target == target)
        {
            replica.health = health;
            replica.verified_at_epoch = verified_at_epoch;
        } else {
            record.replicas.push(ReplicaRecord {
                target,
                health,
                verified_at_epoch,
            });
        }
        record.repair_required = needs_repair(record);
        self.persist()
    }

    pub fn add_reference(&mut self, content_id: &ContentId) -> Result<(), DistributedError> {
        let record = self
            .records
            .get_mut(&content_id.digest)
            .ok_or_else(durability_unavailable)?;
        record.reference_count = record
            .reference_count
            .checked_add(1)
            .ok_or_else(registry_capacity_error)?;
        self.persist()
    }

    pub fn release_reference(
        &mut self,
        content_id: &ContentId,
        retain_until_epoch: u64,
    ) -> Result<(), DistributedError> {
        let record = self
            .records
            .get_mut(&content_id.digest)
            .ok_or_else(durability_unavailable)?;
        record.reference_count = record.reference_count.saturating_sub(1);
        record.retain_until_epoch = record.retain_until_epoch.max(retain_until_epoch);
        self.persist()
    }

    pub fn get(&self, content_id: &ContentId) -> Option<&ResourceCatalogRecord> {
        self.records.get(&content_id.digest)
    }

    pub fn inputs_recoverable(&self, inputs: &[ContentId]) -> bool {
        inputs.iter().all(|input| {
            self.get(input)
                .is_some_and(ResourceCatalogRecord::is_recoverable)
        })
    }

    pub fn minimum_healthy_copies(&self, inputs: &[ContentId]) -> Result<usize, DistributedError> {
        if inputs.is_empty() {
            return Ok(0);
        }
        let mut minimum = usize::MAX;
        for input in inputs {
            minimum = minimum.min(
                self.get(input)
                    .map(ResourceCatalogRecord::healthy_copies)
                    .ok_or_else(durability_unavailable)?,
            );
        }
        Ok(minimum)
    }

    pub fn is_commit_ready(&self, content_id: &ContentId, mode: AcceptanceMode) -> bool {
        let Some(record) = self.get(content_id) else {
            return false;
        };
        match mode {
            AcceptanceMode::Fast => record.healthy_copies() >= 1,
            AcceptanceMode::Durable => record.is_recoverable() && record.healthy_copies() >= 1,
            AcceptanceMode::Critical { minimum_replicas } => {
                record.is_recoverable() && record.healthy_copies() >= usize::from(minimum_replicas)
            }
        }
    }

    pub fn plan_repairs(&self, max_bytes: u64) -> Vec<RepairJob> {
        let mut bytes = 0_u64;
        self.records
            .values()
            .filter(|record| record.repair_required)
            .filter_map(|record| {
                let missing_copies = missing_replica_count(record);
                let estimated = record
                    .manifest
                    .content_id
                    .size
                    .saturating_mul(missing_copies as u64);
                if missing_copies == 0
                    || bytes.saturating_add(estimated) > max_bytes
                    || estimated == 0
                {
                    return None;
                }
                bytes = bytes.saturating_add(estimated);
                Some(RepairJob {
                    content_id: record.manifest.content_id.clone(),
                    missing_copies,
                    estimated_bytes: estimated,
                })
            })
            .take(self.max_repair_jobs)
            .collect()
    }

    pub fn collect_garbage(
        &mut self,
        now_epoch: u64,
        store: &ContentStore,
        max_items: usize,
    ) -> Result<Vec<ContentId>, DistributedError> {
        let candidates: Vec<_> = self
            .records
            .values()
            .filter(|record| record.reference_count == 0 && record.retain_until_epoch <= now_epoch)
            .take(max_items)
            .map(|record| record.manifest.content_id.clone())
            .collect();
        for content_id in &candidates {
            store.remove_content(content_id)?;
            self.records.remove(&content_id.digest);
        }
        if !candidates.is_empty() {
            self.persist()?;
        }
        Ok(candidates)
    }

    fn persist(&self) -> Result<(), DistributedError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|_| registry_storage_error())?;
        }
        let temporary = self.path.with_extension("tmp");
        let bytes = serde_json::to_vec(&CatalogSnapshot {
            records: self.records.clone(),
        })
        .map_err(|_| registry_corrupt_error())?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|_| registry_storage_error())?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| registry_storage_error())?;
        fs::rename(temporary, &self.path).map_err(|_| registry_storage_error())
    }
}

fn needs_repair(record: &ResourceCatalogRecord) -> bool {
    match record.manifest.policy {
        ResourcePolicy::Ephemeral | ResourcePolicy::Reconstructible => false,
        ResourcePolicy::Replicated { minimum_replicas } => {
            record.healthy_copies() < usize::from(minimum_replicas)
        }
        ResourcePolicy::ExternalDurable => !record.replicas.iter().any(|replica| {
            matches!(replica.target, ReplicaTarget::External(_))
                && replica.health == ReplicaHealth::Healthy
        }),
    }
}

fn required_replica_count(record: &ResourceCatalogRecord) -> usize {
    match record.manifest.policy {
        ResourcePolicy::Ephemeral | ResourcePolicy::Reconstructible => 0,
        ResourcePolicy::Replicated { minimum_replicas } => usize::from(minimum_replicas),
        ResourcePolicy::ExternalDurable => 1,
    }
}

fn missing_replica_count(record: &ResourceCatalogRecord) -> usize {
    match record.manifest.policy {
        ResourcePolicy::ExternalDurable => usize::from(!record.replicas.iter().any(|replica| {
            matches!(replica.target, ReplicaTarget::External(_))
                && replica.health == ReplicaHealth::Healthy
        })),
        _ => required_replica_count(record).saturating_sub(record.healthy_copies()),
    }
}

const fn registry_storage_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Storage,
        "persistent registry storage operation failed",
    )
}

const fn registry_corrupt_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Corrupt,
        "persistent registry data is corrupt",
    )
}

const fn registry_config_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::InvalidConfig,
        "persistent registry limits are invalid",
    )
}

const fn registry_capacity_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::CapacityExceeded,
        "persistent registry capacity exceeded",
    )
}

const fn durability_unavailable() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::DurabilityUnavailable,
        "requested durability cannot be proven",
    )
}
