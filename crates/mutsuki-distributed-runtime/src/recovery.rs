use crate::{ContentStore, ResourceCatalog, remote_eligible};
use mutsuki_distributed_contracts::{
    CheckpointArtifactManifest, DistributedError, DistributedErrorKind, DurableTaskRecord,
    EffectRecoveryAction, EffectRecoveryCapability, FailureConfirmation, GlobalTaskId,
    MigrationEstimate, MirrorBudget, NodeId, RealtimeSession, RecoveryAction, RecoveryPolicy,
    RecoveryTarget, RecoveryTier, ResourcePolicy, SessionMigrationDecision, WorkerAdvertisement,
};
use mutsuki_runtime_contracts::{RetrySafety, TaskCheckpoint};
use std::collections::BTreeMap;
use std::io::Cursor;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointBudget {
    pub max_checkpoint_bytes: u64,
    pub max_uploaded_bytes: u64,
    pub max_operations: usize,
}

pub struct CheckpointManager {
    store: ContentStore,
    budget: CheckpointBudget,
    uploaded_bytes: u64,
    operations: usize,
}

impl CheckpointManager {
    pub fn new(store: ContentStore, budget: CheckpointBudget) -> Result<Self, DistributedError> {
        if budget.max_checkpoint_bytes == 0
            || budget.max_uploaded_bytes == 0
            || budget.max_operations == 0
        {
            return Err(recovery_config_error());
        }
        Ok(Self {
            store,
            budget,
            uploaded_bytes: 0,
            operations: 0,
        })
    }

    pub fn persist(
        &mut self,
        global_task_id: GlobalTaskId,
        source_attempt: u32,
        checkpoint: &TaskCheckpoint,
        plugin_generation: u64,
        previous: Option<&CheckpointArtifactManifest>,
        policy: ResourcePolicy,
    ) -> Result<CheckpointArtifactManifest, DistributedError> {
        if source_attempt == 0 || plugin_generation == 0 || !checkpoint.is_self_consistent() {
            return Err(checkpoint_incompatible());
        }
        if self.operations >= self.budget.max_operations {
            return Err(recovery_budget_exceeded());
        }
        let mut encoded = Vec::new();
        checkpoint
            .write_json(&mut encoded)
            .map_err(|_| checkpoint_incompatible())?;
        if encoded.len() as u64 > self.budget.max_checkpoint_bytes {
            return Err(recovery_budget_exceeded());
        }
        let planned = self.store.build_manifest(
            &encoded,
            "application/vnd.mutsuki.task-checkpoint+json",
            policy,
        )?;
        let expected_upload = self.store.missing_upload_bytes(&planned)?;
        if self.uploaded_bytes.saturating_add(expected_upload) > self.budget.max_uploaded_bytes {
            return Err(recovery_budget_exceeded());
        }
        let (content, stats) = self.store.put_bytes(
            &encoded,
            "application/vnd.mutsuki.task-checkpoint+json",
            policy,
        )?;
        let next_uploaded = self
            .uploaded_bytes
            .checked_add(stats.bytes_uploaded)
            .ok_or_else(recovery_budget_exceeded)?;
        if next_uploaded > self.budget.max_uploaded_bytes {
            return Err(recovery_budget_exceeded());
        }
        let (baseline_content_id, previous_content_id, complete_baseline, changed_chunks) =
            if let Some(previous) = previous {
                validate_checkpoint_lineage(
                    previous,
                    &global_task_id,
                    checkpoint,
                    plugin_generation,
                )?;
                self.store.manifest(&previous.baseline_content_id)?;
                let previous_manifest = self.store.manifest(&previous.checkpoint_content_id)?;
                (
                    previous.baseline_content_id.clone(),
                    Some(previous.checkpoint_content_id.clone()),
                    false,
                    changed_chunk_indices(&previous_manifest, &content),
                )
            } else {
                (
                    content.content_id.clone(),
                    None,
                    true,
                    content.chunks.iter().map(|chunk| chunk.index).collect(),
                )
            };
        self.uploaded_bytes = next_uploaded;
        self.operations += 1;
        Ok(CheckpointArtifactManifest {
            global_task_id,
            source_attempt,
            sequence: checkpoint.sequence,
            checkpoint_schema: checkpoint.checkpoint_schema.clone(),
            task_schema: checkpoint.task_schema.clone(),
            plugin_generation,
            input_content_id: checkpoint.input_content_id.clone(),
            checkpoint_content_id: content.content_id,
            baseline_content_id,
            previous_content_id,
            changed_chunks,
            complete_baseline,
        })
    }

    pub fn restore(
        &self,
        artifact: &CheckpointArtifactManifest,
        task_schema: &mutsuki_runtime_contracts::SchemaIdentity,
        plugin_generation: u64,
        input_content_id: &mutsuki_runtime_contracts::ContentId,
    ) -> Result<TaskCheckpoint, DistributedError> {
        if plugin_generation != artifact.plugin_generation
            || task_schema != &artifact.task_schema
            || input_content_id != &artifact.input_content_id
        {
            return Err(checkpoint_incompatible());
        }
        let bytes = self.store.read_content(&artifact.checkpoint_content_id)?;
        let checkpoint =
            TaskCheckpoint::read_json(Cursor::new(bytes)).map_err(|_| checkpoint_incompatible())?;
        checkpoint
            .validate_restore(task_schema, plugin_generation, input_content_id)
            .map_err(|_| checkpoint_incompatible())?;
        Ok(checkpoint)
    }

    pub fn adaptive_interval(
        checkpoint_cost_ticks: u64,
        lost_work_per_tick: u64,
        deadline_remaining_ticks: u64,
        network_pressure: f64,
    ) -> Option<u64> {
        if checkpoint_cost_ticks == 0
            || lost_work_per_tick == 0
            || deadline_remaining_ticks == 0
            || !network_pressure.is_finite()
            || network_pressure >= 0.8
        {
            return None;
        }
        let pressure_multiplier = if network_pressure >= 0.5 { 4 } else { 2 };
        Some(
            checkpoint_cost_ticks
                .saturating_mul(pressure_multiplier)
                .max(1)
                .min(deadline_remaining_ticks),
        )
    }

    pub const fn uploaded_bytes(&self) -> u64 {
        self.uploaded_bytes
    }
}

pub struct RecoveryPlanner;

impl RecoveryPlanner {
    #[allow(clippy::too_many_arguments)]
    pub fn plan(
        record: &DurableTaskRecord,
        policy: &RecoveryPolicy,
        failure: FailureConfirmation,
        advertisement: &WorkerAdvertisement,
        target: RecoveryTarget,
        resources: &ResourceCatalog,
        checkpoint: Option<&CheckpointArtifactManifest>,
        standby: Option<NodeId>,
        now_tick: u64,
    ) -> RecoveryAction {
        if failure == FailureConfirmation::Suspect && !policy.allow_speculative {
            return RecoveryAction::Wait;
        }
        if failure == FailureConfirmation::ExplicitSpeculative && !policy.allow_speculative {
            return RecoveryAction::Wait;
        }
        if policy
            .deadline_tick
            .is_some_and(|deadline| now_tick >= deadline)
        {
            return fail("recovery deadline expired");
        }
        if record.attempts.len() >= policy.max_attempts as usize {
            return fail("recovery attempt budget exhausted");
        }
        if target.quality < policy.minimum_quality
            || target.node_id != advertisement.node_id
            || !remote_eligible(
                advertisement,
                &record.spec.portable,
                &record.spec.requirements,
            )
            || !resources.inputs_recoverable(&record.spec.required_inputs)
        {
            return fail("no compatible target with recoverable inputs");
        }
        let Some(runner) = advertisement.runners.iter().find(|runner| {
            runner.runner_generation == target.runner_generation
                && runner.plugin_generation == target.plugin_generation
        }) else {
            return fail("target Runner or plugin generation is incompatible");
        };
        if record
            .spec
            .portable
            .task
            .runner_hint
            .as_ref()
            .is_some_and(|hint| hint != &runner.runner_id)
        {
            return fail("target Runner hint is incompatible");
        }
        let attempt = record
            .attempts
            .last()
            .map_or(1, |attempt| attempt.attempt.saturating_add(1));
        let not_before_tick = now_tick.saturating_add(backoff(policy, attempt));
        match policy.tier {
            RecoveryTier::Ephemeral => fail("ephemeral task expires with its Worker"),
            RecoveryTier::NonRecoverable => required("task requires explicit recovery review"),
            RecoveryTier::Restartable => {
                if mutsuki_distributed_contracts::can_restart_from_input(&record.spec.portable) {
                    RecoveryAction::Restart {
                        attempt,
                        not_before_tick,
                        target,
                    }
                } else {
                    required("task cannot be safely restarted from input")
                }
            }
            RecoveryTier::Checkpointed => {
                let Some(checkpoint) = checkpoint.cloned() else {
                    return required("no valid checkpoint is available");
                };
                if checkpoint.global_task_id != record.spec.global_task_id
                    || checkpoint.input_content_id != record.spec.portable.input_content_id
                    || checkpoint.plugin_generation != target.plugin_generation
                    || checkpoint.task_schema != record.spec.portable.task_schema
                    || !resources
                        .get(&checkpoint.checkpoint_content_id)
                        .is_some_and(
                            mutsuki_distributed_contracts::ResourceCatalogRecord::is_recoverable,
                        )
                {
                    return required("checkpoint is stale or incompatible");
                }
                RecoveryAction::RestoreCheckpoint {
                    attempt,
                    not_before_tick,
                    target,
                    checkpoint: Box::new(checkpoint),
                }
            }
            RecoveryTier::Mirrored => standby.map_or_else(
                || required("mirrored task has no admitted standby"),
                |node_id| RecoveryAction::PromoteStandby { node_id },
            ),
        }
    }
}

pub fn effect_recovery_action(
    capability: &EffectRecoveryCapability,
    quorum_available: bool,
) -> EffectRecoveryAction {
    if !quorum_available {
        return EffectRecoveryAction::RejectWhileQuorumLost;
    }
    if capability.transactional_outbox {
        return EffectRecoveryAction::TransactionalOutbox;
    }
    match capability.retry_safety {
        RetrySafety::Idempotent if capability.idempotency_key.is_some() => {
            EffectRecoveryAction::RetryWithIdempotencyKey
        }
        RetrySafety::Verifiable if capability.external_verifier => {
            EffectRecoveryAction::VerifyExternalState
        }
        RetrySafety::Compensatable if capability.compensation_hook => {
            EffectRecoveryAction::CompensateThenRetry
        }
        _ => EffectRecoveryAction::RecoveryRequired,
    }
}

pub fn decide_session_migration(
    session: &RealtimeSession,
    target: NodeId,
    estimate: MigrationEstimate,
    leader_only_change: bool,
    worker_dead: bool,
) -> SessionMigrationDecision {
    if leader_only_change {
        return SessionMigrationDecision::Stay;
    }
    if worker_dead {
        if let Some(standby) = &session.standby_node {
            return SessionMigrationDecision::PromoteStandby {
                target: standby.clone(),
            };
        }
        return if session.last_checkpoint.is_some() {
            SessionMigrationDecision::MigrateWholeSession { target }
        } else {
            SessionMigrationDecision::Degrade {
                reason: "session has no recoverable checkpoint or standby".into(),
            }
        };
    }
    let cost = estimate.state_transfer_cost
        + estimate.cold_start_cost
        + estimate.interruption_cost
        + estimate.failure_risk_cost
        + estimate.safety_margin;
    if estimate.future_benefit > cost {
        SessionMigrationDecision::MigrateWholeSession { target }
    } else {
        SessionMigrationDecision::Stay
    }
}

pub struct MirrorAdmission {
    budget: MirrorBudget,
    sessions: BTreeMap<String, (u64, u64, u64)>,
}

impl MirrorAdmission {
    pub fn new(budget: MirrorBudget) -> Result<Self, DistributedError> {
        if budget.max_sessions == 0
            || budget.max_compute_units == 0
            || budget.max_memory_bytes == 0
            || budget.max_network_bytes == 0
        {
            return Err(recovery_config_error());
        }
        Ok(Self {
            budget,
            sessions: BTreeMap::new(),
        })
    }

    pub fn admit(
        &mut self,
        session_id: impl Into<String>,
        critical: bool,
        compute_units: u64,
        memory_bytes: u64,
        network_bytes: u64,
    ) -> Result<(), DistributedError> {
        let session_id = session_id.into();
        if !critical || self.sessions.contains_key(&session_id) {
            return Err(recovery_config_error());
        }
        let used = self
            .sessions
            .values()
            .fold((0_u64, 0_u64, 0_u64), |used, item| {
                (
                    used.0.saturating_add(item.0),
                    used.1.saturating_add(item.1),
                    used.2.saturating_add(item.2),
                )
            });
        if self.sessions.len() >= self.budget.max_sessions
            || used.0.saturating_add(compute_units) > self.budget.max_compute_units
            || used.1.saturating_add(memory_bytes) > self.budget.max_memory_bytes
            || used.2.saturating_add(network_bytes) > self.budget.max_network_bytes
        {
            return Err(recovery_budget_exceeded());
        }
        self.sessions
            .insert(session_id, (compute_units, memory_bytes, network_bytes));
        Ok(())
    }

    pub fn release(&mut self, session_id: &str) -> bool {
        self.sessions.remove(session_id).is_some()
    }
}

fn changed_chunk_indices(
    previous: &mutsuki_distributed_contracts::ContentManifest,
    current: &mutsuki_distributed_contracts::ContentManifest,
) -> Vec<u32> {
    current
        .chunks
        .iter()
        .filter(|chunk| {
            previous
                .chunks
                .get(chunk.index as usize)
                .is_none_or(|old| old.digest != chunk.digest || old.size != chunk.size)
        })
        .map(|chunk| chunk.index)
        .collect()
}

fn validate_checkpoint_lineage(
    previous: &CheckpointArtifactManifest,
    global_task_id: &GlobalTaskId,
    checkpoint: &TaskCheckpoint,
    plugin_generation: u64,
) -> Result<(), DistributedError> {
    if previous.global_task_id != *global_task_id
        || previous.sequence >= checkpoint.sequence
        || previous.checkpoint_schema != checkpoint.checkpoint_schema
        || previous.task_schema != checkpoint.task_schema
        || previous.plugin_generation != plugin_generation
        || previous.input_content_id != checkpoint.input_content_id
        || (!previous.complete_baseline && previous.previous_content_id.is_none())
    {
        return Err(checkpoint_incompatible());
    }
    Ok(())
}

fn backoff(policy: &RecoveryPolicy, attempt: u32) -> u64 {
    let shift = attempt.saturating_sub(1).min(63);
    policy
        .base_backoff_ticks
        .saturating_mul(1_u64 << shift)
        .min(policy.max_backoff_ticks)
}

fn fail(reason: &str) -> RecoveryAction {
    RecoveryAction::Fail {
        reason: reason.into(),
    }
}

fn required(reason: &str) -> RecoveryAction {
    RecoveryAction::RecoveryRequired {
        reason: reason.into(),
    }
}

const fn recovery_config_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::InvalidConfig,
        "recovery policy or budget is invalid",
    )
}

const fn recovery_budget_exceeded() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::CapacityExceeded,
        "recovery resource budget is exhausted",
    )
}

const fn checkpoint_incompatible() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Incompatible,
        "checkpoint schema, generation, input, or lineage is incompatible",
    )
}
