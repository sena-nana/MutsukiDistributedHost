# MutsukiDistributedHost

Mutsuki 的外置分布式 Sidecar。它通过分布式无关的本地 Host control API 观察和提交普通
`TaskBatch`，通过认证后的 Mutsuki Link 会话承载远程控制与点对点数据流；它不修改 Core、
ServiceHost、Runner 或插件执行路径。

## Phase 3 能力

- `mutsuki-distributed-host-adapter`：本地 Host 的 submit/cancel/snapshot/event/drain/health
  适配；当前提供 ServiceHost IPC 实现。
- `mutsuki-distributed-contracts`：Worker 能力快照、紧凑 pulse、远程 envelope、Attempt
  映射和有界 wire frame。
- `mutsuki-distributed-runtime`：Disabled / LocalObservable / Clustered 组合、兼容性过滤、
  有限 fallback、Worker 资源本地化和从输入重建 Attempt。
- `mutsuki-distributed-host`：安全的独立进程入口；默认 `disabled`，不会启动网络或后台任务。

```bash
cargo run -p mutsuki-distributed-host -- disabled
cargo test --workspace --all-targets
```

Clustered 进程由部署层显式提供本地 Host endpoint/token、认证 Link session、Worker 集合和
资源本地化器。仓库不会用匿名明文网络或生产 fallback 猜测这些值。

架构与失败语义见 [docs/phase3-architecture.md](docs/phase3-architecture.md)，Issue #1 的验收
证据见 [docs/acceptance/issue-1.md](docs/acceptance/issue-1.md)。

## Phase 4 能力

- 小型、追加式 `GlobalTaskRegistry` WAL，持久保存 Task/Attempt/ContentId 状态并复制到管理副本。
- Fast、Durable、Critical 明确回执；无法证明所请求持久性时结构化拒绝。
- SHA-256 ContentId、分块 manifest、去重、断点续传、真实副本目录和有预算的数据传输队列。
- OutputStaged → Committed 两阶段输出，旧 Attempt 输出进入冲突记录，不覆盖正式结果。
- 副本修复计划、引用计数、保留期和安全 GC。

数据分层和持久性语义见 [docs/phase4-durable-registry.md](docs/phase4-durable-registry.md)，Issue #4
验收证据见 [docs/acceptance/issue-4.md](docs/acceptance/issue-4.md)。

## Phase 5 能力

- 可替换 `CftControlBackend` 与文件持久化参考实现；支持 3 个完整投票节点或 2 个完整节点 + Witness。
- 多数派选举、term/epoch fencing、Follower 查询/转发、旧 Leader 恢复降级。
- 短 ControlLease 与长 ExecutionGrant 分离；选举期间纯计算继续，新授权 fence 旧结果。
- Healthy / Impaired / Degraded / QuorumLost / Isolated / SafeStop 明确降级。
- 版本化紧凑 pulse、Healthy → Suspect → Dead 检测，以及有滞回的 Leader 偏好转移。

架构与降级矩阵见 [docs/phase5-ha-control.md](docs/phase5-ha-control.md)，Issue #6 验收证据见
[docs/acceptance/issue-6.md](docs/acceptance/issue-6.md)。
