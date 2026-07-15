# MutsukiDistributedHost

Mutsuki 的外置分布式 Sidecar。它通过分布式无关的本地 Host control API 观察和提交普通
`TaskBatch`，通过认证后的 Mutsuki Link 会话承载远程控制与点对点数据流；它不修改 Core、
ServiceHost、Runner 或插件执行路径。

## Capability Matrix

本表是公开能力契约；“有类型/有测试”不等于可部署，未列为 Deployable 的能力在 binary
中必须结构化拒绝。

| 能力 | Contracts | Reference Model | In-process Test | Deployable | Production-ready |
| --- | --- | --- | --- | --- | --- |
| Disabled / LocalObservable | ✅ | — | ✅ | ✅ | 否 |
| 单 Controller + Worker Clustered | ✅ | — | ✅ | ✅ | 否（MVP） |
| 认证 Link control、capability/pulse、submit/query/cancel | ✅ | — | ✅ | ✅ | 否（仅 local transport） |
| 点对点 resource/result content stream | ✅ | — | ✅（独立进程） | ✅ | 否（仅 local transport） |
| PersistentRegistry / Durable | ✅ | ✅ | ✅ | 否 | 否 |
| Critical durability | ✅ | ✅ | ✅ | 否 | 否 |
| `ReferenceCftModel` | ✅ | ✅ | ✅ | 否 | 否 |
| 多 Controller HA | ✅ | ✅ | ✅（conformance only） | 否 | 否 |
| Recovery / trust policy | ✅ | ✅ | ✅ | 否 | 否 |

当前没有任何能力标记为 Production-ready。`DistributedCapability::maturity()` 提供同一份
机器可读边界；HA 配置返回 `ExperimentalUnavailable`，不会把参考状态机当作集群。

## Phase 3 能力

- `mutsuki-distributed-host-adapter`：本地 Host 的 submit/cancel/snapshot/event/drain/health
  适配；当前提供 ServiceHost IPC 实现。
- `mutsuki-distributed-contracts`：Worker 能力快照、紧凑 pulse、远程 envelope、Attempt
  映射和有界 wire frame。
- `mutsuki-distributed-runtime`：Disabled / LocalObservable、单 Controller Clustered 进程驱动、
  有限 fallback、Worker 资源本地化和从输入重建 Attempt。
- `mutsuki-distributed-host`：安全的独立进程入口；默认 `disabled`。Clustered 只接受显式
  deployment JSON，secret 和 ServiceHost token 只通过配置引用的环境变量注入。

```bash
cargo run -p mutsuki-distributed-host -- disabled
cargo test --workspace --all-targets
```

Clustered 进程使用：

```text
mutsuki-distributed-host clustered /absolute/path/to/deployment.json
```

deployment 的 `role` 只能是 `controller`、`worker` 或 `high_availability`。Controller 配置
声明 Worker 的 node/address、管理 client node、本地 ServiceHost endpoint，以及 secret/token
环境变量名；Worker 配置另声明 capability advertisement 与 content directory。配置文件不保存
secret。`high_availability` 在真实多进程 CFT backend 完成前始终结构化拒绝。

当前可部署 transport 是 MutsukiLink local IPC：连接执行 HMAC 双向身份校验、OS peer credential
校验和分布式 protocol negotiation。Controller control frame 保持 64 KiB 上限；大内容由 origin
进程的 `FileContentServer` 直接流向 Worker 的 `LinkResourceLocalizer`，按 256 KiB 分块并在原子
发布前验证 size 和 SHA-256。断线会关闭 session，后续调用重新认证连接；安全重试由 Attempt
fencing 决定，不能安全重试的任务结构化失败。

`mutsuki-distributed-control-client` 是产品启动门控使用的瘦客户端。它通过同一认证管理端点读取
`SidecarCapabilityProof` 和 health，不链接 scheduler/recovery 实现。证明固定包含 capability schema、
distributed protocol major、release、构建 Git revision，以及 LocalObservation、Clustered、Durable、Critical、HA、
checkpoint、trust 的逐项 maturity。产品必须同时校验 release/revision 和所需 feature maturity；
连接失败、旧 revision 或仅有 reference/in-process 证据都不能被解释为可部署能力。

架构与失败语义见 [docs/phase3-architecture.md](docs/phase3-architecture.md)，Issue #1 的验收
证据见 [docs/acceptance/issue-1.md](docs/acceptance/issue-1.md)。

## Phase 4 能力

- 线性化 `GlobalTaskRegistry` transaction、checksum framed WAL 与原子 snapshot/compaction，持久保存 Task/Attempt/ContentId 状态并复制到管理副本。
- Fast、Durable、Critical 明确回执；无法证明所请求持久性时结构化拒绝。
- SHA-256 ContentId、分块 manifest、去重、断点续传、真实副本目录和有预算的数据传输队列。
- OutputStaged → Committed 两阶段输出，旧 Attempt 输出进入冲突记录，不覆盖正式结果。
- 副本修复计划、引用计数、保留期和安全 GC。

数据分层和持久性语义见 [docs/phase4-durable-registry.md](docs/phase4-durable-registry.md)，Issue #4
验收证据见 [docs/acceptance/issue-4.md](docs/acceptance/issue-4.md)。

## Phase 5 能力

- 可替换 `CftControlBackend` 与文件持久化 `ReferenceCftModel`；支持在进程内 conformance test
  模拟 3 个完整投票节点或 2 个完整节点 + Witness，但不是跨进程共识 backend。
- 多数派选举、term/epoch fencing、Follower 查询/转发、旧 Leader 恢复降级。
- 短 ControlLease 与长 ExecutionGrant 分离；选举期间纯计算继续，新授权 fence 旧结果。
- Healthy / Impaired / Degraded / QuorumLost / Isolated / SafeStop 明确降级。
- 版本化紧凑 pulse、Healthy → Suspect → Dead 检测，以及有滞回的 Leader 偏好转移。

架构与降级矩阵见 [docs/phase5-ha-control.md](docs/phase5-ha-control.md)，Issue #6 验收证据见
[docs/acceptance/issue-6.md](docs/acceptance/issue-6.md)。

## Phase 6 能力

- Dead/lease-expired 后按资格、deadline、退避和 Attempt 预算从输入重启。
- Core `TaskCheckpoint` 的内容寻址基线、chunk COW 去重、lineage/实现校验与预算。
- 会话级迁移/standby 提升、显式 Mirrored 资源准入和外部副作用恢复矩阵。

详见 [docs/phase6-recovery.md](docs/phase6-recovery.md) 与
[docs/acceptance/issue-8.md](docs/acceptance/issue-8.md)。

## Phase 7 能力

- 字典序任务优先级、节点本地工作优先和控制/实时资源硬保留。
- capability/plugin generation 索引、硬约束过滤、事件触发 Top-K 与预计算 fallback。
- queue/RTT/传输/预热/执行/回传/提交/jitter/恢复的端到端成本，流式 TTFT/steady SLO，
  以及有界实测 histogram/EWMA 性能模型。
- 节点本地短时 Reservation 和最终 Admission；过期 capability 快照、过载及内存不足结构化拒绝。
- telemetry、hash/disk、调度操作和独立数据通道硬预算；拥塞时保留控制通道并按固定次序降级。
- `LocalEstimatedCost > RemoteCost + SafetyMargin` 的分发收益门槛；帧级、本地设备依赖和短任务默认本地。

设计与预算语义见 [docs/phase7-scheduling.md](docs/phase7-scheduling.md)，Issue #10 验收证据见
[docs/acceptance/issue-10.md](docs/acceptance/issue-10.md)。

## Phase 8 能力

- TrustedLan / AuditedLan / RestrictedWorkers 显式模式；ByzantineResistant 只保留可替换边界，不默认启用。
- 稳定 NodeId、审批、HMAC 身份、密钥轮换/吊销/隔离与 Mutsuki Link 双向认证加密基线。
- 敏感度/信任级别/缓存/attestation/验证策略硬过滤，以及 Controller 签发的最小资源授权。
- 制品 allowlist、ContentId/generation 完整性、可选 attestation verifier、term/epoch/Attempt 收据与提交证明。
- 确定性 replay、显式浮点容差、领域 verifier、N-of-M/spot-check/proof/manual-review 计划；验证失败隔离。
- 真实落盘追加式哈希链审计、Merkle inclusion/segment consistency、按 Task/Attempt/Node 追踪。
- 慢变有界信誉、失陷演练/fencing/授权撤销/制品隔离，以及独立 trust CPU/网络/存储预算。

安全边界与失败语义见 [docs/phase8-trust-plane.md](docs/phase8-trust-plane.md)，Issue #12 验收证据见
[docs/acceptance/issue-12.md](docs/acceptance/issue-12.md)。
