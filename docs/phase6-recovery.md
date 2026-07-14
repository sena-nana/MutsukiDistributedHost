# Phase 6：恢复策略

恢复只在任务描述、输入、副本、目标 Runner/插件 generation、质量和 retry safety 同时满足时发生。
Suspect 默认等待；只有 Dead、lease expired 或显式 speculative policy 才创建递增的新 Attempt。重试受
指数退避、最大次数和 deadline 限制，旧 Attempt 仍由 Phase 4/5 fencing 拒绝。

| 等级 | 行为 |
| --- | --- |
| Ephemeral | Worker 失联即失败。 |
| Restartable | Idempotent/Verifiable/Compensatable 从持久输入创建普通本地 Task。 |
| Checkpointed | 校验 schema、plugin generation、input digest 和 lineage 后恢复。 |
| Mirrored | 仅显式 Critical 且通过 compute/memory/network/session 预算时提升 standby。 |
| NonRecoverable | 进入 RecoveryRequired，不自动重试。 |

Checkpoint 通过 Core 通用 `TaskCheckpoint` JSON 写入 Phase 4 ContentStore。第一个 artifact 必须是完整
baseline；后续完整状态利用相同 chunk hash 自动去重，只上传变化 chunk，manifest 记录 baseline、previous、
sequence 和 changed index。损坏、旧 generation、错误输入或缺失 baseline 都被拒绝。大小、累计上传量、
操作数和自适应间隔均有预算；高网络压力直接暂停 checkpoint。

实时任务迁移整个 session，而不是逐帧恢复。Leader 切换不迁移 primary；Worker Dead 时优先提升已准入
standby，否则从低频 checkpoint 重建。主动迁移仅在未来收益大于传输、冷启动、中断、失败风险和安全余量
之和时发生。

副作用按 Idempotent key、外部验证、补偿或 transactional outbox 处理。Unsafe/能力不完整进入
RecoveryRequired；quorum 丢失时拒绝启动新的不可逆副作用。插件 ABI、Runner 接口和 ServiceHost 不增加
failover callback。
