# Issue #8 验收映射

| 验收项 | 自动证据 |
| --- | --- |
| Suspect 不过早双跑、Dead 后新 Attempt | `recovery_waits_for_dead_applies_backoff_and_refuses_unsafe_retry`。 |
| retry 次数、退避、deadline、质量与兼容性 | `RecoveryPlanner` 的统一硬过滤与同测试。 |
| 完整 baseline、增量/COW、hash 与恢复 | `checkpoint_requires_full_baseline_then_deduplicates_and_restores` 使用真实 ContentStore。 |
| 损坏/旧 generation/输入拒绝 | checkpoint restore 校验；Phase 4 已覆盖损坏 chunk。 |
| checkpoint 预算与自适应降频 | `CheckpointBudget`、actual uploaded bytes 与 `adaptive_interval` 断言。 |
| Leader 切换会话不迁移、Worker 故障按会话迁移 | `effects_sessions_and_mirroring_are_explicit_and_budgeted`。 |
| Mirrored 非默认且受硬预算 | 同测试只允许 explicit Critical，并拒绝超额第二会话。 |
| Unsafe 不自动重试 | planner 与 effect matrix 均返回 RecoveryRequired。 |
| quorum 丢失拒绝不可逆副作用 | effect matrix 返回 RejectWhileQuorumLost；Phase 5 同时拒绝 control write。 |

恢复最终仍生成普通本地 Task/Attempt；没有 Node、checkpoint 上传或 failover callback 进入插件 ABI。
