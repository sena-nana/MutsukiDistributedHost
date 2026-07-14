use mutsuki_distributed_contracts::{
    AuditEvent, AuditEventKind, AuditInclusionProof, AuditSegment, DistributedError,
    DistributedErrorKind, GlobalTaskId, NodeId, TrustPlaneBudget,
};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use crate::digest_hex;

pub struct PersistentAuditLog {
    path: PathBuf,
    budget: TrustPlaneBudget,
    events: Vec<AuditEvent>,
    bytes_this_tick: u64,
}

impl PersistentAuditLog {
    pub fn open(
        path: impl AsRef<Path>,
        budget: TrustPlaneBudget,
    ) -> Result<Self, DistributedError> {
        if budget.max_audit_events_per_segment == 0
            || budget.max_audit_metadata_entries == 0
            || budget.max_audit_bytes_per_tick == 0
        {
            return Err(audit_config_error());
        }
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|_| audit_storage_error())?;
        }
        let mut events = Vec::new();
        if path.exists() {
            let file = File::open(&path).map_err(|_| audit_storage_error())?;
            for line in BufReader::new(file).lines() {
                let line = line.map_err(|_| audit_storage_error())?;
                if line.trim().is_empty() {
                    continue;
                }
                let event: AuditEvent =
                    serde_json::from_str(&line).map_err(|_| audit_corrupt_error())?;
                validate_next_event(events.last(), &event)?;
                events.push(event);
            }
        }
        if events.len() > bounded_event_capacity(budget) {
            return Err(DistributedError::new(
                DistributedErrorKind::CapacityExceeded,
                "audit index exceeds its bounded capacity and requires archival",
            ));
        }
        Ok(Self {
            path,
            budget,
            events,
            bytes_this_tick: 0,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &mut self,
        tick: u64,
        kind: AuditEventKind,
        global_task_id: Option<GlobalTaskId>,
        attempt: Option<u32>,
        node_id: Option<NodeId>,
        metadata: BTreeMap<String, String>,
    ) -> Result<AuditEvent, DistributedError> {
        validate_metadata(&metadata, self.budget.max_audit_metadata_entries)?;
        if self.events.len() >= bounded_event_capacity(self.budget) {
            return Err(DistributedError::new(
                DistributedErrorKind::CapacityExceeded,
                "audit index is full and requires archival",
            ));
        }
        let sequence = self
            .events
            .last()
            .map_or(1, |event| event.sequence.saturating_add(1));
        let previous_hash = self.events.last().map_or_else(
            || digest_hex(b"mutsuki-audit-genesis-v1"),
            |event| event.event_hash.clone(),
        );
        let mut event = AuditEvent {
            sequence,
            tick,
            kind,
            global_task_id,
            attempt,
            node_id,
            metadata,
            previous_hash,
            event_hash: String::new(),
        };
        event.event_hash = hash_event(&event)?;
        let mut encoded = serde_json::to_vec(&event).map_err(|_| audit_corrupt_error())?;
        encoded.push(b'\n');
        let encoded_bytes = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
        if self.bytes_this_tick.saturating_add(encoded_bytes) > self.budget.max_audit_bytes_per_tick
        {
            return Err(DistributedError::new(
                DistributedErrorKind::CapacityExceeded,
                "audit write budget exhausted",
            ));
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|_| audit_storage_error())?;
        file.write_all(&encoded)
            .and_then(|()| file.sync_data())
            .map_err(|_| audit_storage_error())?;
        self.bytes_this_tick = self.bytes_this_tick.saturating_add(encoded_bytes);
        self.events.push(event.clone());
        Ok(event)
    }

    pub fn next_tick(&mut self) {
        self.bytes_this_tick = 0;
    }

    pub fn build_segment(
        &self,
        segment_id: u64,
        start_sequence: u64,
        previous_segment_root: Option<String>,
    ) -> Result<AuditSegment, DistributedError> {
        let events = self
            .events
            .iter()
            .filter(|event| event.sequence >= start_sequence)
            .take(self.budget.max_audit_events_per_segment)
            .collect::<Vec<_>>();
        let first = events.first().ok_or_else(audit_unknown_error)?;
        let last = events.last().ok_or_else(audit_unknown_error)?;
        let leaf_hashes = events
            .iter()
            .map(|event| event.event_hash.clone())
            .collect::<Vec<_>>();
        Ok(AuditSegment {
            segment_id,
            first_sequence: first.sequence,
            last_sequence: last.sequence,
            previous_segment_root,
            merkle_root: merkle_root(&leaf_hashes),
            leaf_hashes,
        })
    }

    pub fn inclusion_proof(
        segment: &AuditSegment,
        leaf_index: u32,
    ) -> Result<AuditInclusionProof, DistributedError> {
        let index = usize::try_from(leaf_index).map_err(|_| audit_unknown_error())?;
        let leaf_hash = segment
            .leaf_hashes
            .get(index)
            .cloned()
            .ok_or_else(audit_unknown_error)?;
        let mut level = segment.leaf_hashes.clone();
        let mut current = index;
        let mut siblings = Vec::new();
        while level.len() > 1 {
            let sibling_index = if current.is_multiple_of(2) {
                (current + 1).min(level.len() - 1)
            } else {
                current - 1
            };
            siblings.push((level[sibling_index].clone(), sibling_index < current));
            level = next_merkle_level(&level);
            current /= 2;
        }
        Ok(AuditInclusionProof {
            segment_id: segment.segment_id,
            leaf_index,
            leaf_hash,
            siblings,
            merkle_root: segment.merkle_root.clone(),
        })
    }

    pub fn verify_inclusion(proof: &AuditInclusionProof) -> bool {
        let root =
            proof
                .siblings
                .iter()
                .fold(proof.leaf_hash.clone(), |current, (sibling, left)| {
                    if *left {
                        hash_pair(sibling, &current)
                    } else {
                        hash_pair(&current, sibling)
                    }
                });
        root == proof.merkle_root
    }

    pub fn verify_segment_consistency(previous: &AuditSegment, current: &AuditSegment) -> bool {
        current.segment_id == previous.segment_id.saturating_add(1)
            && current.first_sequence == previous.last_sequence.saturating_add(1)
            && current.previous_segment_root.as_deref() == Some(previous.merkle_root.as_str())
    }

    pub fn trace_task(&self, global_task_id: &GlobalTaskId) -> Vec<&AuditEvent> {
        self.events
            .iter()
            .filter(|event| event.global_task_id.as_ref() == Some(global_task_id))
            .collect()
    }

    pub fn trace_attempt(&self, global_task_id: &GlobalTaskId, attempt: u32) -> Vec<&AuditEvent> {
        self.events
            .iter()
            .filter(|event| {
                event.global_task_id.as_ref() == Some(global_task_id)
                    && event.attempt == Some(attempt)
            })
            .collect()
    }

    pub fn trace_node(&self, node_id: &NodeId) -> Vec<&AuditEvent> {
        self.events
            .iter()
            .filter(|event| event.node_id.as_ref() == Some(node_id))
            .collect()
    }

    pub fn event_count(&self) -> usize {
        self.events.len()
    }
}

fn validate_next_event(
    previous: Option<&AuditEvent>,
    event: &AuditEvent,
) -> Result<(), DistributedError> {
    let expected_sequence = previous.map_or(1, |event| event.sequence.saturating_add(1));
    let expected_previous = previous.map_or_else(
        || digest_hex(b"mutsuki-audit-genesis-v1"),
        |event| event.event_hash.clone(),
    );
    if event.sequence != expected_sequence
        || event.previous_hash != expected_previous
        || event.event_hash != hash_event(event)?
    {
        return Err(audit_corrupt_error());
    }
    Ok(())
}

fn hash_event(event: &AuditEvent) -> Result<String, DistributedError> {
    let mut unsigned = event.clone();
    unsigned.event_hash.clear();
    serde_json::to_vec(&unsigned)
        .map(|bytes| digest_hex(&bytes))
        .map_err(|_| audit_corrupt_error())
}

fn validate_metadata(
    metadata: &BTreeMap<String, String>,
    max_entries: usize,
) -> Result<(), DistributedError> {
    const FORBIDDEN: [&str; 6] = [
        "secret",
        "token",
        "password",
        "credential",
        "input",
        "output",
    ];
    if metadata.len() > max_entries
        || metadata.iter().any(|(key, value)| {
            let normalized = key.to_ascii_lowercase();
            key.len() > 64
                || value.len() > 256
                || FORBIDDEN.iter().any(|forbidden| {
                    normalized == *forbidden || normalized.ends_with(&format!("_{forbidden}"))
                })
        })
    {
        return Err(DistributedError::new(
            DistributedErrorKind::WorkerRejected,
            "audit metadata exceeds bounds or may contain sensitive payload",
        ));
    }
    Ok(())
}

fn merkle_root(leaves: &[String]) -> String {
    if leaves.is_empty() {
        return digest_hex(b"mutsuki-empty-merkle-v1");
    }
    let mut level = leaves.to_vec();
    while level.len() > 1 {
        level = next_merkle_level(&level);
    }
    level[0].clone()
}

fn next_merkle_level(level: &[String]) -> Vec<String> {
    level
        .chunks(2)
        .map(|pair| {
            if pair.len() == 2 {
                hash_pair(&pair[0], &pair[1])
            } else {
                hash_pair(&pair[0], &pair[0])
            }
        })
        .collect()
}

fn hash_pair(left: &str, right: &str) -> String {
    digest_hex(format!("{left}:{right}").as_bytes())
}

const fn bounded_event_capacity(budget: TrustPlaneBudget) -> usize {
    budget.max_audit_events_per_segment.saturating_mul(64)
}

const fn audit_config_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::InvalidConfig,
        "audit log budget must be positive and bounded",
    )
}

const fn audit_storage_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Storage,
        "audit log could not persist an append-only event",
    )
}

const fn audit_corrupt_error() -> DistributedError {
    DistributedError::new(DistributedErrorKind::Corrupt, "audit hash chain is corrupt")
}

const fn audit_unknown_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::TaskUnknown,
        "audit event or segment is unavailable",
    )
}
