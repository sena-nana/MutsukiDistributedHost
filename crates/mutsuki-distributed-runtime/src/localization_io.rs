use mutsuki_distributed_contracts::{DistributedError, DistributedErrorKind};
use serde::{Deserialize, Serialize};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[cfg(feature = "localization-testkit")]
use std::time::Duration;

pub const LOCALIZATION_CHUNK_BYTES: usize = 256 * 1024;
const HISTOGRAM_BUCKETS: usize = 64;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocalizationIoBudget {
    pub max_active_reads: usize,
    pub max_active_writes: usize,
    pub max_active_hash_jobs: usize,
    pub max_queued_jobs: usize,
    pub max_buffered_bytes: usize,
    pub max_content_bytes: u64,
}

impl LocalizationIoBudget {
    pub fn validate(self) -> Result<Self, DistributedError> {
        if self.max_active_reads == 0
            || self.max_active_writes == 0
            || self.max_active_hash_jobs == 0
            || self.max_queued_jobs == 0
            || self.max_buffered_bytes < LOCALIZATION_CHUNK_BYTES
            || self.max_content_bytes == 0
            || self.max_buffered_bytes > Semaphore::MAX_PERMITS
        {
            return Err(DistributedError::new(
                DistributedErrorKind::InvalidConfig,
                "localization I/O budget must be positive and bounded",
            ));
        }
        self.max_active_reads
            .checked_add(self.max_active_writes)
            .and_then(|active| active.checked_add(self.max_active_hash_jobs))
            .and_then(|active| active.checked_add(self.max_queued_jobs))
            .filter(|permits| *permits <= Semaphore::MAX_PERMITS)
            .ok_or_else(|| {
                DistributedError::new(
                    DistributedErrorKind::InvalidConfig,
                    "localization I/O admission budget exceeds the supported limit",
                )
            })?;
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BlockingRequirements {
    pub read: bool,
    pub write: bool,
    pub hash: bool,
}

impl BlockingRequirements {
    pub const READ: Self = Self {
        read: true,
        write: false,
        hash: false,
    };
    pub const WRITE: Self = Self {
        read: false,
        write: true,
        hash: false,
    };
    pub const READ_HASH: Self = Self {
        read: true,
        write: false,
        hash: true,
    };
    pub const WRITE_HASH: Self = Self {
        read: false,
        write: true,
        hash: true,
    };
    pub const READ_WRITE_HASH: Self = Self {
        read: true,
        write: true,
        hash: true,
    };
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct LocalizationLatencyHistogram {
    pub buckets: Vec<u64>,
    pub count: u64,
    pub total_ns: u128,
    pub max_ns: u64,
}

impl LocalizationLatencyHistogram {
    fn record(&mut self, elapsed_ns: u64) {
        if self.buckets.is_empty() {
            self.buckets.resize(HISTOGRAM_BUCKETS, 0);
        }
        let bucket = if elapsed_ns == 0 {
            0
        } else {
            usize::try_from(u64::BITS - elapsed_ns.leading_zeros())
                .unwrap_or(HISTOGRAM_BUCKETS - 1)
                .min(HISTOGRAM_BUCKETS - 1)
        };
        self.buckets[bucket] = self.buckets[bucket].saturating_add(1);
        self.count = self.count.saturating_add(1);
        self.total_ns = self.total_ns.saturating_add(u128::from(elapsed_ns));
        self.max_ns = self.max_ns.max(elapsed_ns);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct LocalizationIoMetricsSnapshot {
    pub queued_jobs: usize,
    pub active_reads: usize,
    pub active_writes: usize,
    pub active_hash_jobs: usize,
    pub buffered_bytes: usize,
    pub peak_buffered_bytes: usize,
    pub started_jobs: u64,
    pub completed_jobs: u64,
    pub failed_jobs: u64,
    pub cancelled_jobs: u64,
    pub panicked_jobs: u64,
    pub processed_bytes: u64,
    pub physical_source_reads: u64,
    pub physical_validation_reads: u64,
    pub physical_downloads: u64,
    pub queue_time_ns: LocalizationLatencyHistogram,
    pub execution_time_ns: LocalizationLatencyHistogram,
}

#[derive(Default)]
struct MetricsState {
    peak_buffered_bytes: usize,
    started_jobs: u64,
    completed_jobs: u64,
    failed_jobs: u64,
    cancelled_jobs: u64,
    panicked_jobs: u64,
    processed_bytes: u64,
    queue_time_ns: LocalizationLatencyHistogram,
    execution_time_ns: LocalizationLatencyHistogram,
}

struct RuntimeInner {
    budget: LocalizationIoBudget,
    admission: Arc<Semaphore>,
    reads: Arc<Semaphore>,
    writes: Arc<Semaphore>,
    hashes: Arc<Semaphore>,
    buffers: Arc<Semaphore>,
    shutdown: AtomicBool,
    queued_jobs: AtomicUsize,
    active_reads: AtomicUsize,
    active_writes: AtomicUsize,
    active_hashes: AtomicUsize,
    buffered_bytes: AtomicUsize,
    physical_source_reads: AtomicU64,
    physical_validation_reads: AtomicU64,
    physical_downloads: AtomicU64,
    metrics: Mutex<MetricsState>,
    #[cfg(feature = "localization-testkit")]
    testkit: LocalizationIoTestkit,
}

#[cfg(feature = "localization-testkit")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalizationIoInjectedFault {
    DiskFull,
    PermissionDenied,
}

#[cfg(feature = "localization-testkit")]
#[derive(Default)]
struct TestkitState {
    read_delay_ns: AtomicU64,
    write_delay_ns: AtomicU64,
    hash_delay_ns: AtomicU64,
    network_delay_ns: AtomicU64,
    next_write_fault: AtomicUsize,
    panic_next_job: AtomicBool,
}

#[cfg(feature = "localization-testkit")]
#[derive(Clone, Default)]
pub struct LocalizationIoTestkit {
    inner: Arc<TestkitState>,
}

#[cfg(feature = "localization-testkit")]
impl LocalizationIoTestkit {
    pub fn set_stage_delays(&self, read: Duration, write: Duration, hash: Duration) {
        self.inner
            .read_delay_ns
            .store(duration_ns(read.as_nanos()), Ordering::Release);
        self.inner
            .write_delay_ns
            .store(duration_ns(write.as_nanos()), Ordering::Release);
        self.inner
            .hash_delay_ns
            .store(duration_ns(hash.as_nanos()), Ordering::Release);
    }

    pub fn set_network_delay(&self, delay: Duration) {
        self.inner
            .network_delay_ns
            .store(duration_ns(delay.as_nanos()), Ordering::Release);
    }

    pub fn inject_next_write_fault(&self, fault: LocalizationIoInjectedFault) {
        let value = match fault {
            LocalizationIoInjectedFault::DiskFull => 1,
            LocalizationIoInjectedFault::PermissionDenied => 2,
        };
        self.inner.next_write_fault.store(value, Ordering::Release);
    }

    pub fn panic_next_blocking_job(&self) {
        self.inner.panic_next_job.store(true, Ordering::Release);
    }

    fn before_blocking(&self, requirements: BlockingRequirements) -> Result<(), DistributedError> {
        assert!(
            !self.inner.panic_next_job.swap(false, Ordering::AcqRel),
            "injected localization blocking panic"
        );
        if requirements.write {
            match self.inner.next_write_fault.swap(0, Ordering::AcqRel) {
                1 => {
                    return Err(DistributedError::new(
                        DistributedErrorKind::Storage,
                        "injected localization disk-full fault",
                    ));
                }
                2 => {
                    return Err(DistributedError::new(
                        DistributedErrorKind::Storage,
                        "injected localization permission-denied fault",
                    ));
                }
                _ => {}
            }
        }
        let delay_ns = [
            requirements
                .read
                .then(|| self.inner.read_delay_ns.load(Ordering::Acquire)),
            requirements
                .write
                .then(|| self.inner.write_delay_ns.load(Ordering::Acquire)),
            requirements
                .hash
                .then(|| self.inner.hash_delay_ns.load(Ordering::Acquire)),
        ]
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(0);
        if delay_ns > 0 {
            std::thread::sleep(Duration::from_nanos(delay_ns));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct LocalizationIoRuntime {
    inner: Arc<RuntimeInner>,
}

impl LocalizationIoRuntime {
    pub fn new(budget: LocalizationIoBudget) -> Result<Self, DistributedError> {
        let budget = budget.validate()?;
        Ok(Self {
            inner: Arc::new(RuntimeInner {
                budget,
                admission: Arc::new(Semaphore::new(budget.max_queued_jobs)),
                reads: Arc::new(Semaphore::new(budget.max_active_reads)),
                writes: Arc::new(Semaphore::new(budget.max_active_writes)),
                hashes: Arc::new(Semaphore::new(budget.max_active_hash_jobs)),
                buffers: Arc::new(Semaphore::new(budget.max_buffered_bytes)),
                shutdown: AtomicBool::new(false),
                queued_jobs: AtomicUsize::new(0),
                active_reads: AtomicUsize::new(0),
                active_writes: AtomicUsize::new(0),
                active_hashes: AtomicUsize::new(0),
                buffered_bytes: AtomicUsize::new(0),
                physical_source_reads: AtomicU64::new(0),
                physical_validation_reads: AtomicU64::new(0),
                physical_downloads: AtomicU64::new(0),
                metrics: Mutex::new(MetricsState::default()),
                #[cfg(feature = "localization-testkit")]
                testkit: LocalizationIoTestkit::default(),
            }),
        })
    }

    pub fn budget(&self) -> LocalizationIoBudget {
        self.inner.budget
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.inner.shutdown.load(Ordering::Acquire)
    }

    #[cfg(feature = "localization-testkit")]
    pub fn testkit(&self) -> LocalizationIoTestkit {
        self.inner.testkit.clone()
    }

    pub(crate) async fn shape_network(&self) {
        #[cfg(feature = "localization-testkit")]
        {
            let delay_ns = self
                .inner
                .testkit
                .inner
                .network_delay_ns
                .load(Ordering::Acquire);
            if delay_ns > 0 {
                tokio::time::sleep(Duration::from_nanos(delay_ns)).await;
            }
        }
    }

    pub fn shutdown(&self) {
        if !self.inner.shutdown.swap(true, Ordering::AcqRel) {
            self.inner.admission.close();
            self.inner.reads.close();
            self.inner.writes.close();
            self.inner.hashes.close();
            self.inner.buffers.close();
        }
    }

    pub fn metrics(&self) -> LocalizationIoMetricsSnapshot {
        let metrics = self
            .inner
            .metrics
            .lock()
            .expect("localization metrics mutex");
        LocalizationIoMetricsSnapshot {
            queued_jobs: self.inner.queued_jobs.load(Ordering::Acquire),
            active_reads: self.inner.active_reads.load(Ordering::Acquire),
            active_writes: self.inner.active_writes.load(Ordering::Acquire),
            active_hash_jobs: self.inner.active_hashes.load(Ordering::Acquire),
            buffered_bytes: self.inner.buffered_bytes.load(Ordering::Acquire),
            peak_buffered_bytes: metrics.peak_buffered_bytes,
            started_jobs: metrics.started_jobs,
            completed_jobs: metrics.completed_jobs,
            failed_jobs: metrics.failed_jobs,
            cancelled_jobs: metrics.cancelled_jobs,
            panicked_jobs: metrics.panicked_jobs,
            processed_bytes: metrics.processed_bytes,
            physical_source_reads: self.inner.physical_source_reads.load(Ordering::Acquire),
            physical_validation_reads: self.inner.physical_validation_reads.load(Ordering::Acquire),
            physical_downloads: self.inner.physical_downloads.load(Ordering::Acquire),
            queue_time_ns: metrics.queue_time_ns.clone(),
            execution_time_ns: metrics.execution_time_ns.clone(),
        }
    }

    pub(crate) fn record_source_read(&self) {
        self.inner
            .physical_source_reads
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn record_validation_read(&self) {
        self.inner
            .physical_validation_reads
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn record_download(&self) {
        self.inner.physical_downloads.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) async fn acquire_buffer(
        &self,
        bytes: usize,
    ) -> Result<BufferedBytes, DistributedError> {
        let permits = u32::try_from(bytes).map_err(|_| capacity_error())?;
        let permit = self
            .inner
            .buffers
            .clone()
            .acquire_many_owned(permits)
            .await
            .map_err(|_| cancelled_error())?;
        let current = self
            .inner
            .buffered_bytes
            .fetch_add(bytes, Ordering::AcqRel)
            .saturating_add(bytes);
        let mut metrics = self
            .inner
            .metrics
            .lock()
            .expect("localization metrics mutex");
        metrics.peak_buffered_bytes = metrics.peak_buffered_bytes.max(current);
        drop(metrics);
        Ok(BufferedBytes {
            bytes: Vec::with_capacity(bytes),
            accounted_bytes: bytes,
            permit: Some(permit),
            inner: self.inner.clone(),
        })
    }

    pub(crate) async fn run_blocking<T, F>(
        &self,
        requirements: BlockingRequirements,
        expected_bytes: u64,
        cancelled: Arc<AtomicBool>,
        work: F,
    ) -> Result<T, DistributedError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, DistributedError> + Send + 'static,
    {
        if self.is_shutdown() || cancelled.load(Ordering::Acquire) {
            return Err(cancelled_error());
        }
        let queued_at = Instant::now();
        let admission = self
            .inner
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| capacity_error())?;
        let queued = QueuedJobGuard::new(self.inner.clone());
        let read = acquire_if(requirements.read, &self.inner.reads).await?;
        let write = acquire_if(requirements.write, &self.inner.writes).await?;
        let hash = acquire_if(requirements.hash, &self.inner.hashes).await?;
        if self.is_shutdown() || cancelled.load(Ordering::Acquire) {
            return Err(cancelled_error());
        }
        drop(queued);
        drop(admission);
        let queue_ns = duration_ns(queued_at.elapsed().as_nanos());
        let active = ActiveJobGuard::new(self.inner.clone(), requirements);
        {
            let mut metrics = self
                .inner
                .metrics
                .lock()
                .expect("localization metrics mutex");
            metrics.started_jobs = metrics.started_jobs.saturating_add(1);
            metrics.queue_time_ns.record(queue_ns);
        }
        let inner = self.inner.clone();
        #[cfg(feature = "localization-testkit")]
        let testkit = self.inner.testkit.clone();
        let joined = tokio::task::spawn_blocking(move || {
            let _read = read;
            let _write = write;
            let _hash = hash;
            let _active = active;
            let started = Instant::now();
            let result = catch_unwind(AssertUnwindSafe(|| {
                #[cfg(feature = "localization-testkit")]
                testkit.before_blocking(requirements)?;
                work()
            }));
            let execution_ns = duration_ns(started.elapsed().as_nanos());
            let mut metrics = inner.metrics.lock().expect("localization metrics mutex");
            metrics.execution_time_ns.record(execution_ns);
            metrics.processed_bytes = metrics.processed_bytes.saturating_add(expected_bytes);
            match result {
                Ok(Ok(value)) => {
                    metrics.completed_jobs = metrics.completed_jobs.saturating_add(1);
                    Ok(value)
                }
                Ok(Err(error)) if error.kind == DistributedErrorKind::WorkerUnavailable => {
                    metrics.cancelled_jobs = metrics.cancelled_jobs.saturating_add(1);
                    Err(error)
                }
                Ok(Err(error)) => {
                    metrics.failed_jobs = metrics.failed_jobs.saturating_add(1);
                    Err(error)
                }
                Err(_) => {
                    metrics.panicked_jobs = metrics.panicked_jobs.saturating_add(1);
                    Err(DistributedError::new(
                        DistributedErrorKind::LocalizationFailed,
                        "localization blocking worker failed",
                    ))
                }
            }
        })
        .await;
        joined.map_err(|_| {
            DistributedError::new(
                DistributedErrorKind::LocalizationFailed,
                "localization blocking worker could not be joined",
            )
        })?
    }
}

struct ActiveJobGuard {
    inner: Arc<RuntimeInner>,
    requirements: BlockingRequirements,
}

impl ActiveJobGuard {
    fn new(inner: Arc<RuntimeInner>, requirements: BlockingRequirements) -> Self {
        bump_active(&inner, requirements, true);
        Self {
            inner,
            requirements,
        }
    }
}

impl Drop for ActiveJobGuard {
    fn drop(&mut self) {
        bump_active(&self.inner, self.requirements, false);
    }
}

fn bump_active(inner: &RuntimeInner, requirements: BlockingRequirements, active: bool) {
    let apply = |enabled: bool, counter: &AtomicUsize| {
        if !enabled {
            return;
        }
        if active {
            counter.fetch_add(1, Ordering::AcqRel);
        } else {
            counter.fetch_sub(1, Ordering::AcqRel);
        }
    };
    apply(requirements.read, &inner.active_reads);
    apply(requirements.write, &inner.active_writes);
    apply(requirements.hash, &inner.active_hashes);
}

struct QueuedJobGuard {
    inner: Arc<RuntimeInner>,
}

impl QueuedJobGuard {
    fn new(inner: Arc<RuntimeInner>) -> Self {
        inner.queued_jobs.fetch_add(1, Ordering::AcqRel);
        Self { inner }
    }
}

impl Drop for QueuedJobGuard {
    fn drop(&mut self) {
        self.inner.queued_jobs.fetch_sub(1, Ordering::AcqRel);
    }
}

async fn acquire_if(
    required: bool,
    semaphore: &Arc<Semaphore>,
) -> Result<Option<OwnedSemaphorePermit>, DistributedError> {
    if required {
        semaphore
            .clone()
            .acquire_owned()
            .await
            .map(Some)
            .map_err(|_| cancelled_error())
    } else {
        Ok(None)
    }
}

pub(crate) struct BufferedBytes {
    bytes: Vec<u8>,
    accounted_bytes: usize,
    permit: Option<OwnedSemaphorePermit>,
    inner: Arc<RuntimeInner>,
}

impl BufferedBytes {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn bytes_mut(&mut self) -> &mut Vec<u8> {
        &mut self.bytes
    }

    pub(crate) fn replace(&mut self, bytes: Vec<u8>) {
        self.bytes = bytes;
    }
}

impl Drop for BufferedBytes {
    fn drop(&mut self) {
        self.inner
            .buffered_bytes
            .fetch_sub(self.accounted_bytes, Ordering::AcqRel);
        self.permit.take();
    }
}

pub(crate) struct CancellationGuard {
    cancelled: Arc<AtomicBool>,
    armed: bool,
}

impl CancellationGuard {
    pub(crate) fn new(cancelled: Arc<AtomicBool>) -> Self {
        Self {
            cancelled,
            armed: true,
        }
    }

    pub(crate) fn complete(mut self) {
        self.armed = false;
    }
}

impl Drop for CancellationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.cancelled.store(true, Ordering::Release);
        }
    }
}

pub(crate) fn cancellation_requested(
    runtime: &LocalizationIoRuntime,
    cancelled: &AtomicBool,
) -> bool {
    runtime.is_shutdown() || cancelled.load(Ordering::Acquire)
}

pub(crate) fn cancelled_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::WorkerUnavailable,
        "localization I/O operation was cancelled",
    )
}

pub(crate) fn capacity_error() -> DistributedError {
    DistributedError::new(
        DistributedErrorKind::CapacityExceeded,
        "localization I/O capacity is exhausted",
    )
}

fn duration_ns(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::time::Duration;

    fn budget() -> LocalizationIoBudget {
        LocalizationIoBudget {
            max_active_reads: 1,
            max_active_writes: 1,
            max_active_hash_jobs: 1,
            max_queued_jobs: 1,
            max_buffered_bytes: LOCALIZATION_CHUNK_BYTES,
            max_content_bytes: 1024 * 1024,
        }
    }

    #[test]
    fn budget_rejects_zero_and_sub_chunk_buffer_limits() {
        let mut invalid = budget();
        invalid.max_active_reads = 0;
        assert_eq!(
            invalid.validate().unwrap_err().kind,
            DistributedErrorKind::InvalidConfig
        );
        invalid = budget();
        invalid.max_buffered_bytes = LOCALIZATION_CHUNK_BYTES - 1;
        assert_eq!(
            invalid.validate().unwrap_err().kind,
            DistributedErrorKind::InvalidConfig
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admission_rejects_only_after_the_waiting_queue_is_full() {
        let io = LocalizationIoRuntime::new(budget()).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let worker_barrier = barrier.clone();
        let active_io = io.clone();
        let active = tokio::spawn(async move {
            active_io
                .run_blocking(
                    BlockingRequirements::READ,
                    1,
                    Arc::new(AtomicBool::new(false)),
                    move || {
                        worker_barrier.wait();
                        Ok(())
                    },
                )
                .await
        });
        while io.metrics().active_reads != 1 {
            tokio::task::yield_now().await;
        }
        let queued_io = io.clone();
        let queued = tokio::spawn(async move {
            queued_io
                .run_blocking(
                    BlockingRequirements::READ,
                    1,
                    Arc::new(AtomicBool::new(false)),
                    || Ok(()),
                )
                .await
        });
        while io.metrics().queued_jobs != 1 {
            tokio::task::yield_now().await;
        }
        let rejected = io
            .run_blocking(
                BlockingRequirements::READ,
                1,
                Arc::new(AtomicBool::new(false)),
                || Ok(()),
            )
            .await
            .unwrap_err();
        assert_eq!(rejected.kind, DistributedErrorKind::CapacityExceeded);
        barrier.wait();
        active.await.unwrap().unwrap();
        queued.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn buffered_bytes_are_a_hard_global_limit() {
        let io = LocalizationIoRuntime::new(budget()).unwrap();
        let first = io.acquire_buffer(LOCALIZATION_CHUNK_BYTES).await.unwrap();
        assert_eq!(io.metrics().buffered_bytes, LOCALIZATION_CHUNK_BYTES);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), io.acquire_buffer(1))
                .await
                .is_err()
        );
        drop(first);
        let second = io.acquire_buffer(1).await.unwrap();
        assert_eq!(io.metrics().peak_buffered_bytes, LOCALIZATION_CHUNK_BYTES);
        drop(second);
        assert_eq!(io.metrics().buffered_bytes, 0);
    }

    #[tokio::test]
    async fn blocking_panics_are_structured_and_counted() {
        let io = LocalizationIoRuntime::new(budget()).unwrap();
        let error = io
            .run_blocking(
                BlockingRequirements::WRITE,
                0,
                Arc::new(AtomicBool::new(false)),
                || -> Result<(), DistributedError> { panic!("injected blocking panic") },
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, DistributedErrorKind::LocalizationFailed);
        let metrics = io.metrics();
        assert_eq!(metrics.panicked_jobs, 1);
        assert_eq!(metrics.active_writes, 0);
        assert_eq!(metrics.queued_jobs, 0);
    }
}
