use mutsuki_distributed_contracts::{
    AdmissionOutcome, CapabilityBits, CombinedPulse, DistributedError, DistributedErrorKind,
    LatencyClass, LexicographicPriority, LocalResourceBudget, MemberHealth, NetworkBudget,
    NetworkDegradation, NodeId, PlacementCandidate, PlacementFlags, PlacementPlan, QualityPolicy,
    RemoteLoadAction, ReservationRequest, SchedulingEvent, SchedulingNodeSnapshot,
    TaskPlacementRequest, TelemetryClass, WorkOrigin,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PerformanceKey {
    task_type: String,
    variant_id: String,
    input_bucket: u8,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PerformanceObservation {
    pub latency_ticks: f64,
    pub peak_memory_bytes: u64,
    pub failed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PerformancePrediction {
    pub samples: u32,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub ewma: f64,
    pub peak_memory_bytes: u64,
    pub failure_probability: f64,
    pub throughput_per_tick: f64,
    pub uncertainty_penalty: f64,
}

#[derive(Clone, Debug)]
struct BoundedProfile {
    histogram: [u32; 32],
    samples: u32,
    failures: u32,
    ewma: f64,
    peak_memory_bytes: u64,
}

impl BoundedProfile {
    fn new(observation: PerformanceObservation) -> Self {
        let mut profile = Self {
            histogram: [0; 32],
            samples: 0,
            failures: 0,
            ewma: observation.latency_ticks,
            peak_memory_bytes: 0,
        };
        profile.record(observation);
        profile
    }

    fn record(&mut self, observation: PerformanceObservation) {
        let bucket = latency_bucket(observation.latency_ticks);
        self.histogram[bucket] = self.histogram[bucket].saturating_add(1);
        self.samples = self.samples.saturating_add(1);
        self.failures = self.failures.saturating_add(u32::from(observation.failed));
        self.ewma = if self.samples == 1 {
            observation.latency_ticks
        } else {
            self.ewma.mul_add(0.8, observation.latency_ticks * 0.2)
        };
        self.peak_memory_bytes = self.peak_memory_bytes.max(observation.peak_memory_bytes);
    }

    fn quantile(&self, numerator: u32, denominator: u32) -> f64 {
        let target = self
            .samples
            .saturating_mul(numerator)
            .div_ceil(denominator)
            .max(1);
        let mut cumulative = 0_u32;
        for (index, count) in self.histogram.iter().enumerate() {
            cumulative = cumulative.saturating_add(*count);
            if cumulative >= target {
                return bucket_upper_bound(index);
            }
        }
        bucket_upper_bound(self.histogram.len() - 1)
    }

    fn prediction(&self, minimum_samples: u32) -> PerformancePrediction {
        let missing = minimum_samples.saturating_sub(self.samples);
        PerformancePrediction {
            samples: self.samples,
            p50: self.quantile(50, 100),
            p95: self.quantile(95, 100),
            p99: self.quantile(99, 100),
            ewma: self.ewma,
            peak_memory_bytes: self.peak_memory_bytes,
            failure_probability: f64::from(self.failures) / f64::from(self.samples.max(1)),
            throughput_per_tick: 1.0 / self.ewma.max(f64::EPSILON),
            uncertainty_penalty: f64::from(missing) / f64::from(minimum_samples.max(1)),
        }
    }
}

pub struct PerformanceModel {
    profiles: BTreeMap<PerformanceKey, BoundedProfile>,
    max_profiles: usize,
    minimum_samples: u32,
}

impl PerformanceModel {
    pub fn new(max_profiles: usize, minimum_samples: u32) -> Result<Self, DistributedError> {
        if max_profiles == 0 || minimum_samples == 0 {
            return Err(scheduler_config_error());
        }
        Ok(Self {
            profiles: BTreeMap::new(),
            max_profiles,
            minimum_samples,
        })
    }

    pub fn record(
        &mut self,
        task_type: &str,
        variant_id: &str,
        input_bucket: u8,
        observation: PerformanceObservation,
    ) -> Result<(), DistributedError> {
        if !observation.latency_ticks.is_finite() || observation.latency_ticks <= 0.0 {
            return Err(scheduler_input_error());
        }
        let key = PerformanceKey {
            task_type: task_type.to_owned(),
            variant_id: variant_id.to_owned(),
            input_bucket,
        };
        if let Some(profile) = self.profiles.get_mut(&key) {
            profile.record(observation);
            return Ok(());
        }
        if self.profiles.len() >= self.max_profiles {
            let oldest_key = self
                .profiles
                .keys()
                .next()
                .cloned()
                .ok_or_else(scheduler_config_error)?;
            self.profiles.remove(&oldest_key);
        }
        self.profiles.insert(key, BoundedProfile::new(observation));
        Ok(())
    }

    pub fn predict(
        &self,
        task_type: &str,
        variant_id: &str,
        input_bucket: u8,
    ) -> Option<PerformancePrediction> {
        self.profiles
            .get(&PerformanceKey {
                task_type: task_type.to_owned(),
                variant_id: variant_id.to_owned(),
                input_bucket,
            })
            .map(|profile| profile.prediction(self.minimum_samples))
    }

    pub fn profile_count(&self) -> usize {
        self.profiles.len()
    }
}

pub struct CapabilityIndex {
    nodes: BTreeMap<NodeId, SchedulingNodeSnapshot>,
    by_capability_bit: BTreeMap<u8, BTreeSet<NodeId>>,
    by_plugin_generation: BTreeMap<(String, u64), BTreeSet<NodeId>>,
}

impl CapabilityIndex {
    pub fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
            by_capability_bit: BTreeMap::new(),
            by_plugin_generation: BTreeMap::new(),
        }
    }

    pub fn upsert(&mut self, snapshot: SchedulingNodeSnapshot) -> Result<(), DistributedError> {
        if snapshot.capability_version == 0
            || snapshot.resource_version == 0
            || snapshot.pressure_bucket > 100
            || snapshot.variants.iter().any(|variant| {
                variant.runner_generation == 0
                    || variant.plugin_generation == 0
                    || !variant.quality.is_finite()
                    || !(0.0..=1.0).contains(&variant.failure_probability)
            })
        {
            return Err(scheduler_input_error());
        }
        if self.nodes.get(&snapshot.node_id).is_some_and(|current| {
            current.capability_version > snapshot.capability_version
                || current.resource_version > snapshot.resource_version
        }) {
            return Err(DistributedError::new(
                DistributedErrorKind::AttemptStale,
                "scheduling node snapshot is stale",
            ));
        }
        self.remove(&snapshot.node_id);
        for bit in set_bits(snapshot.capabilities) {
            self.by_capability_bit
                .entry(bit)
                .or_default()
                .insert(snapshot.node_id.clone());
        }
        for variant in &snapshot.variants {
            self.by_plugin_generation
                .entry((variant.plugin_id.clone(), variant.plugin_generation))
                .or_default()
                .insert(snapshot.node_id.clone());
        }
        self.nodes.insert(snapshot.node_id.clone(), snapshot);
        Ok(())
    }

    pub fn remove(&mut self, node_id: &NodeId) {
        if let Some(previous) = self.nodes.remove(node_id) {
            for bit in set_bits(previous.capabilities) {
                if let Some(nodes) = self.by_capability_bit.get_mut(&bit) {
                    nodes.remove(node_id);
                }
            }
            for variant in previous.variants {
                if let Some(nodes) = self
                    .by_plugin_generation
                    .get_mut(&(variant.plugin_id.clone(), variant.plugin_generation))
                {
                    nodes.remove(node_id);
                }
            }
            self.by_capability_bit.retain(|_, nodes| !nodes.is_empty());
            self.by_plugin_generation
                .retain(|_, nodes| !nodes.is_empty());
        }
    }

    pub fn candidate_ids(&self, required: CapabilityBits) -> BTreeSet<NodeId> {
        let mut required_bits = set_bits(required).into_iter();
        let Some(first) = required_bits.next() else {
            return self.nodes.keys().cloned().collect();
        };
        let mut result = self
            .by_capability_bit
            .get(&first)
            .cloned()
            .unwrap_or_default();
        for bit in required_bits {
            let Some(nodes) = self.by_capability_bit.get(&bit) else {
                return BTreeSet::new();
            };
            result.retain(|node| nodes.contains(node));
        }
        result
    }

    pub fn candidate_ids_for(
        &self,
        required: CapabilityBits,
        plugin: Option<&(String, u64)>,
    ) -> BTreeSet<NodeId> {
        let mut result = self.candidate_ids(required);
        if let Some(plugin) = plugin {
            let Some(nodes) = self.by_plugin_generation.get(plugin) else {
                return BTreeSet::new();
            };
            result.retain(|node| nodes.contains(node));
        }
        result
    }

    pub fn get(&self, node_id: &NodeId) -> Option<&SchedulingNodeSnapshot> {
        self.nodes.get(node_id)
    }

    fn capability_supply(&self, bit: u8) -> usize {
        self.by_capability_bit.get(&bit).map_or(0, BTreeSet::len)
    }
}

impl Default for CapabilityIndex {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TaskPriorityQueue<T> {
    max_tasks: usize,
    tasks: Vec<(LexicographicPriority, T)>,
}

impl<T> TaskPriorityQueue<T> {
    pub fn new(max_tasks: usize) -> Result<Self, DistributedError> {
        if max_tasks == 0 {
            return Err(scheduler_config_error());
        }
        Ok(Self {
            max_tasks,
            tasks: Vec::new(),
        })
    }

    pub fn push(
        &mut self,
        priority: LexicographicPriority,
        task: T,
    ) -> Result<(), DistributedError> {
        if self.tasks.len() >= self.max_tasks {
            return Err(DistributedError::new(
                DistributedErrorKind::CapacityExceeded,
                "scheduler task queue is full",
            ));
        }
        self.tasks.push((priority, task));
        Ok(())
    }

    pub fn pop(&mut self) -> Option<T> {
        let index = self
            .tasks
            .iter()
            .enumerate()
            .max_by_key(|(_, (priority, _))| priority.scheduling_key())
            .map(|(index, _)| index)?;
        Some(self.tasks.swap_remove(index).1)
    }
}

pub struct PlacementScheduler {
    index: CapabilityIndex,
    performance: PerformanceModel,
    top_k: usize,
    max_scheduler_operations: usize,
}

impl PlacementScheduler {
    pub fn new(
        top_k: usize,
        max_scheduler_operations: usize,
        max_profiles: usize,
        minimum_samples: u32,
    ) -> Result<Self, DistributedError> {
        if top_k == 0 || max_scheduler_operations == 0 {
            return Err(scheduler_config_error());
        }
        Ok(Self {
            index: CapabilityIndex::new(),
            performance: PerformanceModel::new(max_profiles, minimum_samples)?,
            top_k,
            max_scheduler_operations,
        })
    }

    pub fn update_node(
        &mut self,
        event: SchedulingEvent,
        snapshot: SchedulingNodeSnapshot,
    ) -> Result<(), DistributedError> {
        if !matches!(
            event,
            SchedulingEvent::NodeStateChanged | SchedulingEvent::CapabilityChanged
        ) {
            return Err(scheduler_input_error());
        }
        self.index.upsert(snapshot)
    }

    pub fn performance_model_mut(&mut self) -> &mut PerformanceModel {
        &mut self.performance
    }

    // The placement pipeline stays linear here so its hard-filter/SLO/score order is auditable.
    #[allow(clippy::too_many_lines)]
    pub fn schedule(
        &self,
        event: SchedulingEvent,
        request: &TaskPlacementRequest,
    ) -> Result<PlacementPlan, DistributedError> {
        if !matches!(
            event,
            SchedulingEvent::NewTask
                | SchedulingEvent::AdmissionRejected
                | SchedulingEvent::SessionMigrationRequested
        ) {
            return Err(scheduler_input_error());
        }
        validate_request(request)?;
        let default_local = request.flags.contains(PlacementFlags::LOCAL_ONLY)
            || request.flags.contains(PlacementFlags::FRAME_BOUND)
            || request.flags.contains(PlacementFlags::LOCAL_DEVICE_BOUND)
            || request.local_estimated_cost <= request.small_task_threshold
            || request.priority.latency_class == LatencyClass::HardRealtime;
        let mut coarse = self
            .index
            .candidate_ids_for(
                request.required_capabilities,
                request.required_plugin.as_ref(),
            )
            .into_iter()
            .filter_map(|node_id| {
                let node = self.index.get(&node_id)?;
                if !hard_node_filter(node, request) {
                    return None;
                }
                let localized = request
                    .required_content
                    .iter()
                    .filter(|content| node.localized_content.contains(*content))
                    .count();
                let localized = u32::try_from(localized).unwrap_or(u32::MAX);
                let coarse_cost = f64::from(node.pressure_bucket) - f64::from(localized) * 5.0;
                Some((coarse_cost, node_id))
            })
            .collect::<Vec<_>>();
        coarse.sort_by(|left, right| float_order(left.0, right.0));
        coarse.truncate(self.top_k.min(self.max_scheduler_operations));

        let mut evaluated = 0_usize;
        let mut candidates = Vec::new();
        for (_, node_id) in coarse {
            let Some(node) = self.index.get(&node_id) else {
                continue;
            };
            for variant in &node.variants {
                if evaluated >= self.max_scheduler_operations {
                    break;
                }
                evaluated += 1;
                if !hard_variant_filter(node, variant, request) {
                    continue;
                }
                let is_local = node.node_id == request.local_node;
                if default_local && !is_local {
                    continue;
                }
                let prediction = self.performance.predict(
                    &request.task_type,
                    &variant.variant_id,
                    request.input_bucket,
                );
                let predicted_p50 = prediction.map_or(variant.base_cost.execution, |p| p.p50);
                let predicted_p95 = prediction.map_or(
                    variant.base_cost.execution + variant.base_cost.jitter,
                    |p| p.p95,
                );
                let predicted_p99 = prediction.map_or(
                    variant.base_cost.execution + variant.base_cost.jitter * 2.0,
                    |p| p.p99,
                );
                let failure_probability = prediction
                    .map_or(variant.failure_probability, |p| p.failure_probability)
                    .max(variant.failure_probability);
                let uncertainty = prediction.map_or(1.0, |p| p.uncertainty_penalty);
                if !satisfies_slo(
                    request,
                    variant,
                    predicted_p95,
                    predicted_p99,
                    failure_probability,
                ) {
                    continue;
                }
                let transfer_cost = if is_local {
                    0.0
                } else {
                    variant.base_cost.input_transfer + variant.base_cost.output_transfer
                };
                let session_cost = request.session_node.as_ref().map_or(0.0, |session_node| {
                    if session_node == &node.node_id {
                        -request.migration_cost.min(variant.base_cost.total() * 0.25)
                    } else {
                        request.migration_cost
                    }
                });
                let dag_cost = if request.dag_cross_node_cost > request.dag_parallel_benefit {
                    request.dag_cross_node_cost - request.dag_parallel_benefit
                } else {
                    0.0
                };
                let scarcity_penalty = scarcity_penalty(&self.index, node, request);
                let recovery_risk = failure_probability * variant.base_cost.recovery;
                let remote_cost = variant.base_cost.queue
                    + variant.base_cost.rtt
                    + transfer_cost
                    + variant.base_cost.prewarm
                    + predicted_p50
                    + variant.base_cost.commit
                    + variant.base_cost.jitter
                    + recovery_risk
                    + session_cost
                    + dag_cost;
                if !is_remote_profitable(
                    request.local_estimated_cost,
                    remote_cost,
                    request.safety_margin,
                    default_local,
                    is_local,
                ) {
                    continue;
                }
                let quality_penalty = (1.0 - variant.quality).max(0.0) * 1000.0;
                let stability_bias = if matches!(
                    request.priority.latency_class,
                    LatencyClass::ClusterSafety
                        | LatencyClass::HardRealtime
                        | LatencyClass::SoftRealtime
                ) {
                    uncertainty * 50.0
                } else {
                    uncertainty * 10.0
                };
                let risk_adjusted_cost = remote_cost
                    + quality_penalty
                    + scarcity_penalty
                    + stability_bias
                    + variant.base_cost.energy;
                candidates.push(PlacementCandidate {
                    node_id: node.node_id.clone(),
                    variant_id: variant.variant_id.clone(),
                    capability_version: node.capability_version,
                    resource_version: node.resource_version,
                    predicted_p50,
                    predicted_p95,
                    predicted_p99,
                    risk_adjusted_cost,
                    remote_cost,
                });
            }
        }
        candidates.sort_by(|left, right| {
            float_order(left.risk_adjusted_cost, right.risk_adjusted_cost)
                .then_with(|| left.node_id.cmp(&right.node_id))
        });
        let selected = candidates.first().cloned().ok_or_else(|| {
            DistributedError::new(
                DistributedErrorKind::WorkerUnavailable,
                "no placement satisfies hard constraints, SLO, and profitability",
            )
        })?;
        let fallbacks = candidates.into_iter().skip(1).take(self.top_k).collect();
        let profitability_margin =
            request.local_estimated_cost - selected.remote_cost - request.safety_margin;
        Ok(PlacementPlan {
            selected,
            fallbacks,
            evaluated_candidates: evaluated,
            profitability_margin,
        })
    }
}

#[derive(Clone, Debug)]
struct ActiveReservation {
    request: ReservationRequest,
}

pub struct LocalAdmissionController {
    budget: LocalResourceBudget,
    capability_version: u64,
    pressure_bucket: u8,
    reservations: BTreeMap<String, ActiveReservation>,
}

impl LocalAdmissionController {
    pub fn new(
        budget: LocalResourceBudget,
        capability_version: u64,
    ) -> Result<Self, DistributedError> {
        if capability_version == 0
            || budget.total_cpu_units == 0
            || budget.total_memory_bytes == 0
            || budget.total_threads == 0
            || budget.reserved_local_cpu_units > budget.total_cpu_units
            || budget.reserved_local_memory_bytes > budget.total_memory_bytes
            || budget.reserved_local_vram_bytes > budget.total_vram_bytes
            || budget.reserved_local_threads > budget.total_threads
            || budget.max_remote_pressure_bucket > 100
        {
            return Err(scheduler_config_error());
        }
        Ok(Self {
            budget,
            capability_version,
            pressure_bucket: 0,
            reservations: BTreeMap::new(),
        })
    }

    pub fn update_local_state(&mut self, capability_version: u64, pressure_bucket: u8) {
        self.capability_version = capability_version;
        self.pressure_bucket = pressure_bucket.min(100);
    }

    pub fn admit(&mut self, request: ReservationRequest, now_tick: u64) -> AdmissionOutcome {
        self.expire(now_tick);
        if request.capability_version != self.capability_version {
            return AdmissionOutcome::CapabilityChanged {
                current_version: self.capability_version,
            };
        }
        if request.valid_until_tick <= now_tick {
            return AdmissionOutcome::RetryAfter {
                tick: now_tick.saturating_add(1),
            };
        }
        if request.origin == WorkOrigin::Remote
            && self.pressure_bucket >= self.budget.max_remote_pressure_bucket
        {
            return AdmissionOutcome::Overloaded;
        }
        let (cpu, memory, vram, threads) = self.reserved_totals();
        let remote = request.origin == WorkOrigin::Remote;
        let cpu_limit = self.budget.total_cpu_units.saturating_sub(if remote {
            self.budget.reserved_local_cpu_units
        } else {
            0
        });
        let memory_limit = self.budget.total_memory_bytes.saturating_sub(if remote {
            self.budget.reserved_local_memory_bytes
        } else {
            0
        });
        let vram_limit = self.budget.total_vram_bytes.saturating_sub(if remote {
            self.budget.reserved_local_vram_bytes
        } else {
            0
        });
        let thread_limit = self.budget.total_threads.saturating_sub(if remote {
            self.budget.reserved_local_threads
        } else {
            0
        });
        if memory.saturating_add(request.memory_bytes) > memory_limit
            || vram.saturating_add(request.vram_bytes) > vram_limit
        {
            return AdmissionOutcome::InsufficientMemory;
        }
        if cpu.saturating_add(request.cpu_units) > cpu_limit
            || threads.saturating_add(request.threads) > thread_limit
        {
            return AdmissionOutcome::Overloaded;
        }
        let reservation_id = request.reservation_id.clone();
        self.reservations
            .insert(reservation_id.clone(), ActiveReservation { request });
        AdmissionOutcome::Accept { reservation_id }
    }

    pub fn release(&mut self, reservation_id: &str) -> bool {
        self.reservations.remove(reservation_id).is_some()
    }

    pub fn expire(&mut self, now_tick: u64) {
        self.reservations
            .retain(|_, active| active.request.valid_until_tick > now_tick);
    }

    pub fn remote_load_action(&self) -> RemoteLoadAction {
        let threshold = self.budget.max_remote_pressure_bucket;
        if self.pressure_bucket < threshold.saturating_sub(20) {
            RemoteLoadAction::Continue
        } else if self.pressure_bucket < threshold {
            RemoteLoadAction::ReduceConcurrency
        } else if self.pressure_bucket < threshold.saturating_add(10).min(100) {
            RemoteLoadAction::PauseCheckpointableBatch
        } else {
            RemoteLoadAction::CancelRemoteBackground
        }
    }

    fn reserved_totals(&self) -> (u32, u64, u64, u32) {
        self.reservations.values().fold(
            (0_u32, 0_u64, 0_u64, 0_u32),
            |(cpu, memory, vram, threads), active| {
                (
                    cpu.saturating_add(active.request.cpu_units),
                    memory.saturating_add(active.request.memory_bytes),
                    vram.saturating_add(active.request.vram_bytes),
                    threads.saturating_add(active.request.threads),
                )
            },
        )
    }
}

pub struct TelemetrySampler {
    node_id: NodeId,
    max_events_per_tick: u32,
    base_interval_ticks: u64,
    max_interval_ticks: u64,
    accepted_this_tick: u32,
    stable_rounds: u8,
    pressure_ewma_scaled: u32,
    events: BTreeMap<String, u64>,
}

impl TelemetrySampler {
    pub fn new(
        node_id: NodeId,
        max_events_per_tick: u32,
        base_interval_ticks: u64,
        max_interval_ticks: u64,
    ) -> Result<Self, DistributedError> {
        if max_events_per_tick == 0
            || base_interval_ticks == 0
            || max_interval_ticks < base_interval_ticks
        {
            return Err(scheduler_config_error());
        }
        Ok(Self {
            node_id,
            max_events_per_tick,
            base_interval_ticks,
            max_interval_ticks,
            accepted_this_tick: 0,
            stable_rounds: 0,
            pressure_ewma_scaled: 0,
            events: BTreeMap::new(),
        })
    }

    pub fn record(
        &mut self,
        class: TelemetryClass,
        name: &str,
        count: u64,
        pressure_bucket: u8,
    ) -> bool {
        let budget_available = self.accepted_this_tick < self.max_events_per_tick;
        let accepted = match class {
            TelemetryClass::Correctness => true,
            TelemetryClass::SchedulingSummary => budget_available && pressure_bucket < 90,
            TelemetryClass::Discardable => budget_available && pressure_bucket < 60,
        };
        if accepted {
            *self.events.entry(name.to_owned()).or_default() = self
                .events
                .get(name)
                .copied()
                .unwrap_or_default()
                .saturating_add(count);
            self.accepted_this_tick = self.accepted_this_tick.saturating_add(1);
        }
        accepted
    }

    pub fn pulse(
        &mut self,
        capability_version: u64,
        resource_version: u64,
        pressure_bucket: u8,
        health: MemberHealth,
        state_changed: bool,
    ) -> CombinedPulse {
        self.pressure_ewma_scaled = self
            .pressure_ewma_scaled
            .saturating_mul(8)
            .saturating_add(u32::from(pressure_bucket).saturating_mul(200))
            / 10;
        self.stable_rounds = if state_changed || pressure_bucket >= 70 {
            0
        } else {
            self.stable_rounds.saturating_add(1).min(16)
        };
        let multiplier = 1_u64 << self.stable_rounds.min(6);
        let next_sample_after_ticks = self
            .base_interval_ticks
            .saturating_mul(multiplier)
            .min(self.max_interval_ticks);
        self.accepted_this_tick = 0;
        CombinedPulse {
            node_id: self.node_id.clone(),
            capability_version,
            resource_version,
            pressure_bucket: u8::try_from((self.pressure_ewma_scaled + 50) / 100)
                .unwrap_or(100)
                .min(100),
            health,
            next_sample_after_ticks,
            accepted_events: std::mem::take(&mut self.events),
        }
    }
}

pub struct DistributedBudgetMeter {
    budget: mutsuki_distributed_contracts::DistributedResourceBudget,
    hash_bytes: u64,
    disk_bytes: u64,
    scheduler_operations: u32,
    telemetry_events: u32,
}

impl DistributedBudgetMeter {
    pub fn new(
        budget: mutsuki_distributed_contracts::DistributedResourceBudget,
    ) -> Result<Self, DistributedError> {
        if budget.max_cpu_share_percent == 0
            || budget.max_cpu_share_percent > 100
            || budget.max_memory_bytes == 0
            || budget.max_hash_bytes_per_tick == 0
            || budget.max_disk_bytes_per_tick == 0
            || budget.max_scheduler_operations_per_tick == 0
            || budget.max_telemetry_events_per_tick == 0
        {
            return Err(scheduler_config_error());
        }
        Ok(Self {
            budget,
            hash_bytes: 0,
            disk_bytes: 0,
            scheduler_operations: 0,
            telemetry_events: 0,
        })
    }

    pub fn admit_hash_and_disk(&mut self, hash_bytes: u64, disk_bytes: u64) -> bool {
        let next_hash = self.hash_bytes.saturating_add(hash_bytes);
        let next_disk = self.disk_bytes.saturating_add(disk_bytes);
        if next_hash > self.budget.max_hash_bytes_per_tick
            || next_disk > self.budget.max_disk_bytes_per_tick
        {
            return false;
        }
        self.hash_bytes = next_hash;
        self.disk_bytes = next_disk;
        true
    }

    pub fn admit_scheduler_operation(&mut self) -> bool {
        if self.scheduler_operations >= self.budget.max_scheduler_operations_per_tick {
            return false;
        }
        self.scheduler_operations += 1;
        true
    }

    pub fn admit_telemetry_event(&mut self) -> bool {
        if self.telemetry_events >= self.budget.max_telemetry_events_per_tick {
            return false;
        }
        self.telemetry_events += 1;
        true
    }

    pub fn next_tick(&mut self) {
        self.hash_bytes = 0;
        self.disk_bytes = 0;
        self.scheduler_operations = 0;
        self.telemetry_events = 0;
    }
}

pub struct NetworkBudgetController {
    budget: NetworkBudget,
    data_bytes_this_tick: u64,
    control_bytes_this_tick: u64,
    active_transfers: usize,
    queued_bytes: u64,
    pressure_bucket: u8,
    reconnecting: bool,
}

impl NetworkBudgetController {
    pub fn new(budget: NetworkBudget) -> Result<Self, DistributedError> {
        if budget.max_bytes_per_tick == 0
            || budget.max_concurrent_transfers == 0
            || budget.max_queued_bytes == 0
            || budget.control_reserve_bytes_per_tick == 0
            || budget.control_reserve_bytes_per_tick >= budget.max_bytes_per_tick
        {
            return Err(scheduler_config_error());
        }
        Ok(Self {
            budget,
            data_bytes_this_tick: 0,
            control_bytes_this_tick: 0,
            active_transfers: 0,
            queued_bytes: 0,
            pressure_bucket: 0,
            reconnecting: false,
        })
    }

    pub fn update_pressure(&mut self, pressure_bucket: u8, reconnecting: bool) {
        self.pressure_bucket = pressure_bucket.min(100);
        self.reconnecting = reconnecting;
    }

    pub fn admit_control(&mut self, bytes: u64) -> bool {
        let next = self.control_bytes_this_tick.saturating_add(bytes);
        if next > self.budget.control_reserve_bytes_per_tick {
            return false;
        }
        self.control_bytes_this_tick = next;
        true
    }

    pub fn enqueue_data(&mut self, bytes: u64, leader_forwarding: bool) -> bool {
        if leader_forwarding
            || self.degradation() == NetworkDegradation::ControlOnly
            || self.queued_bytes.saturating_add(bytes) > self.budget.max_queued_bytes
        {
            return false;
        }
        self.queued_bytes = self.queued_bytes.saturating_add(bytes);
        true
    }

    pub fn start_data(&mut self, bytes: u64) -> bool {
        let data_limit = self
            .budget
            .max_bytes_per_tick
            .saturating_sub(self.budget.control_reserve_bytes_per_tick);
        if self.active_transfers >= self.budget.max_concurrent_transfers
            || bytes > self.queued_bytes
            || self.data_bytes_this_tick.saturating_add(bytes) > data_limit
        {
            return false;
        }
        self.queued_bytes -= bytes;
        self.data_bytes_this_tick = self.data_bytes_this_tick.saturating_add(bytes);
        self.active_transfers += 1;
        true
    }

    pub fn complete_data(&mut self) -> bool {
        if self.active_transfers == 0 {
            return false;
        }
        self.active_transfers -= 1;
        true
    }

    pub fn next_tick(&mut self) {
        self.data_bytes_this_tick = 0;
        self.control_bytes_this_tick = 0;
    }

    pub fn degradation(&self) -> NetworkDegradation {
        let queue_percent = self
            .queued_bytes
            .saturating_mul(100)
            .checked_div(self.budget.max_queued_bytes)
            .unwrap_or(100);
        let pressure = u64::from(self.pressure_bucket).max(queue_percent);
        if self.reconnecting && pressure >= 90 {
            NetworkDegradation::ControlOnly
        } else if pressure >= 90 {
            NetworkDegradation::RejectLargeRemote
        } else if pressure >= 75 {
            NetworkDegradation::PauseRemoteBatch
        } else if pressure >= 60 {
            NetworkDegradation::ReduceCheckpointing
        } else if pressure >= 40 {
            NetworkDegradation::StopPreReplication
        } else {
            NetworkDegradation::Normal
        }
    }
}

fn validate_request(request: &TaskPlacementRequest) -> Result<(), DistributedError> {
    let finite = [
        request.local_estimated_cost,
        request.safety_margin,
        request.small_task_threshold,
        request.migration_cost,
        request.dag_cross_node_cost,
        request.dag_parallel_benefit,
        request.slo.deadline_ticks,
        request.slo.max_p95_ticks,
        request.slo.max_p99_ticks,
        request.slo.max_jitter_ticks,
        request.slo.max_failure_probability,
        request.slo.minimum_quality,
    ]
    .into_iter()
    .all(f64::is_finite);
    if !finite
        || request.local_estimated_cost < 0.0
        || request.safety_margin < 0.0
        || request.slo.minimum_quality < 0.0
        || request.slo.max_failure_probability < 0.0
    {
        return Err(scheduler_input_error());
    }
    Ok(())
}

pub fn is_remote_profitable(
    local_estimated_cost: f64,
    remote_cost: f64,
    safety_margin: f64,
    default_local: bool,
    is_local_candidate: bool,
) -> bool {
    is_local_candidate
        || (!default_local
            && local_estimated_cost.is_finite()
            && remote_cost.is_finite()
            && safety_margin.is_finite()
            && local_estimated_cost > remote_cost + safety_margin)
}

fn hard_node_filter(node: &SchedulingNodeSnapshot, request: &TaskPlacementRequest) -> bool {
    node.health == MemberHealth::Healthy
        && node.capabilities.contains(request.required_capabilities)
        && node.trust_level >= request.minimum_trust
        && request.required_os.as_ref().is_none_or(|os| os == &node.os)
        && request
            .required_abi
            .as_ref()
            .is_none_or(|abi| abi == &node.abi)
        && node.available_memory_bytes >= request.required_memory_bytes
        && node.available_vram_bytes >= request.required_vram_bytes
        && (!request.flags.contains(PlacementFlags::LOCAL_ONLY)
            || node.node_id == request.local_node)
}

fn hard_variant_filter(
    node: &SchedulingNodeSnapshot,
    variant: &mutsuki_distributed_contracts::ExecutionVariant,
    request: &TaskPlacementRequest,
) -> bool {
    node.capabilities.contains(variant.required_capabilities)
        && variant.peak_memory_bytes <= node.available_memory_bytes
        && variant.peak_vram_bytes <= node.available_vram_bytes
        && request
            .required_plugin
            .as_ref()
            .is_none_or(|(plugin, generation)| {
                variant.plugin_id == *plugin && variant.plugin_generation == *generation
            })
        && variant.quality >= request.slo.minimum_quality
        && (request.quality_policy == QualityPolicy::AllowDegraded || variant.quality >= 1.0)
}

fn satisfies_slo(
    request: &TaskPlacementRequest,
    variant: &mutsuki_distributed_contracts::ExecutionVariant,
    p95: f64,
    p99: f64,
    failure_probability: f64,
) -> bool {
    let cost = &variant.base_cost;
    let total = cost.total();
    total <= request.slo.deadline_ticks
        && p95 <= request.slo.max_p95_ticks
        && p99 <= request.slo.max_p99_ticks
        && cost.jitter <= request.slo.max_jitter_ticks
        && failure_probability <= request.slo.max_failure_probability
        && (!request.slo.streaming
            || (request
                .slo
                .max_ttft_ticks
                .is_none_or(|max| cost.ttft.is_some_and(|ttft| ttft <= max))
                && request
                    .slo
                    .max_steady_latency_ticks
                    .is_none_or(|max| cost.steady_latency.is_some_and(|steady| steady <= max))))
}

fn scarcity_penalty(
    index: &CapabilityIndex,
    node: &SchedulingNodeSnapshot,
    request: &TaskPlacementRequest,
) -> f64 {
    let scarce_count = set_bits(node.capabilities)
        .into_iter()
        .filter(|bit| {
            request.required_capabilities.0 & (1_u128 << bit) == 0
                && index.capability_supply(*bit) <= 2
        })
        .count();
    f64::from(u32::try_from(scarce_count).unwrap_or(u32::MAX)) * 25.0
}

fn set_bits(bits: CapabilityBits) -> Vec<u8> {
    let mut value = bits.0;
    let mut result = Vec::new();
    while value != 0 {
        let bit = u8::try_from(value.trailing_zeros()).unwrap_or(127);
        result.push(bit);
        value &= value - 1;
    }
    result
}

fn latency_bucket(value: f64) -> usize {
    if value <= 1.0 {
        return 0;
    }
    let mut scaled = value;
    let mut bucket = 0_usize;
    while scaled > 1.0 && bucket < 31 {
        scaled /= 2.0;
        bucket += 1;
    }
    bucket
}

fn bucket_upper_bound(index: usize) -> f64 {
    f64::from(1_u32 << index.min(31))
}

fn float_order(left: f64, right: f64) -> Ordering {
    left.partial_cmp(&right).unwrap_or(Ordering::Equal)
}

const fn scheduler_config_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::InvalidConfig,
        "scheduler budget must be positive and bounded",
    )
}

const fn scheduler_input_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::Incompatible,
        "scheduler input is invalid or incompatible",
    )
}
