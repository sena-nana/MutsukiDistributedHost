# Issue #1 验收映射

| 验收项 | 实现与自动证据 |
| --- | --- |
| 分布式无关 Host adapter | `HostAdapter` 与 `ServiceHostAdapter`；公开 submit/cancel/snapshot/outcome/events/drain/health。 |
| Sidecar 不侵入本地 Host | `disabled_sidecar_is_inert_and_does_not_own_local_host_lifecycle`。 |
| 三种运行等级及 Disabled 零开销 | `Sidecar::{disabled,local_observable,clustered}`；Disabled 构造不启动线程或网络。 |
| Worker 版本能力与紧凑 pulse | `registry_uses_full_snapshots_and_compact_versioned_pulses`。 |
| portable/resource/runner compatibility | `incompatible_runner_or_resource_keeps_execution_local`。 |
| envelope 编码、发送、解码与普通 Host 提交 | e2e fixture 经 `WireRemoteWorker → WorkerRequestDispatcher → WorkerEndpoint → HostAdapter`。 |
| 同一插件本地/远程路径 | `submit_cancel_result_and_worker_rejection_use_the_same_local_task_path` 校验 protocol、runner_hint 和 payload 不变。 |
| 资源先本地化 | `localization_failure_is_structured_and_never_reaches_local_host`。 |
| 大数据不经过控制节点 | `large_data_never_enters_control_envelope` 与独立 resource/result stream descriptor。 |
| 有限 fallback | `fallback_attempts_are_strictly_bounded`。 |
| cancel/result/reject | `submit_cancel_result_and_worker_rejection_use_the_same_local_task_path`。 |
| 断网后从输入重试并拒绝旧结果 | `disconnect_restarts_safe_work_with_new_attempt_and_rejects_stale_result`。 |
| 不安全任务不自动重试 | `unsafe_remote_work_never_restarts_automatically`。 |

发布前固定执行：

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo metadata --locked --format-version 1
./scripts/check-boundaries.sh
```
