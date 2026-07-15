# Phase 4：持久任务与内容资源

## 数据分层

| 层 | 内容 | 持久化路径 |
| --- | --- | --- |
| 权威控制元数据 | GlobalTaskId、状态、Attempt、TaskHandle、Runner generation、ContentId | 64 KiB/record 上限的 framed registry WAL + 原子 snapshot；按 acceptance mode 两阶段提交、fsync/复制。 |
| 内容元数据 | Content manifest、chunk hash、资源策略、位置、健康、引用和保留期 | 独立 resource catalog snapshot。 |
| 大型数据 | 输入、输出、checkpoint 的 chunk bytes | 入口/资源节点与 Worker 的内容存储及 Link resource/result lane。 |
| 临时数据 | 帧、GPU 中间状态、可重建缓存 | 本地 ephemeral，不进入 WAL。 |

Registry transaction 携带单调 log index、transaction id、previous transaction 和 task version
expectation；只有 prepare/commit 配对且副本回执达标后才发布内存状态。WAL 使用 length/version/index/
payload/SHA-256 framing，尾部半写安全截断，完整损坏按 byte offset 报告。record 只引用 ContentId，
不包含 chunk manifest 或内容字节。`DataTransferQueue` 仅存在于数据
端点，受最大 chunk、排队字节和并发数约束；控制 request/reply 不与它共享 payload 或队列。

## 接受等级

| 模式 | 返回条件 | 代价与故障语义 |
| --- | --- | --- |
| Fast | 本地追加完成，不 fsync、不等待副本；输入可以是 Ephemeral。 | 最低延时，进程/磁盘故障可丢失；receipt 明确为 `Fast/Submitted`。 |
| Durable | Task 描述已 fsync 到两个管理副本，必要输入声明为可恢复。 | 入口节点丢失后另一个管理节点可按 GlobalTaskId 重放查询。 |
| Critical | 元数据、输入和输出均达到显式 `minimum_replicas >= 2`。 | 等待更多真实副本；不足时拒绝，不静默降级。 |

receipt 同时报告 requested/actual、状态、transaction/log index、逐副本 commit proof、已确认元数据
副本数和最小输入副本数。当前阶段仍是单控制端，
没有 Leader 自动选举；管理副本用于持久读取和显式接管，自动共识属于 Phase 5。

## GlobalTask 状态机

```text
Submitted ──persist──> Persisted ──assign──> Assigned ──start──> Running
                                                │                  │
                                                └────stage─────────┘
                                                         │
                                                         v
                                                  OutputStaged
                                                         │ verify + commit
                                                         v
                                                    Committed

non-terminal ──> Failed | Cancelled | RecoveryRequired
RecoveryRequired ──new fenced Attempt──> Assigned
```

每次重新分配都创建递增 Attempt 并停用旧 Attempt。Worker 输出先确认内容副本并进入 `OutputStaged`；
只有当前 Attempt 的已持久输出才能进入 `Committed`。旧 Attempt 或节点不匹配的输出保留为冲突候选，
不按完成时间自动覆盖。

Worker 在 staged 后失联时，其他管理节点可重放 WAL、验证 resource catalog 中的持久副本并完成 commit，
无需重复完整计算。

## 内容存储

- ContentId 与每个 chunk 均使用 SHA-256；manifest 限制 chunk 大小、数量、顺序和总大小。
- 接收端先请求缺失 chunk index；已有且哈希正确的 chunk 直接复用，进程重启后继续缺失部分。
- 完成时按顺序流式重算整体 ContentId，之后原子发布并 fsync manifest。
- `Replicated` 按真实 Healthy 位置计数；`ExternalDurable` 必须存在健康外部位置；目录声明不能代替副本。
- 损坏、缺失或副本不足生成受 job 数和 bytes 双重限制的 repair plan。
- 只有引用计数为零且保留期结束的内容才能 GC；共享 chunk 在仍被其他 manifest 引用时保留。

Phase 4 不实现 quorum、自动 Leader 接管、checkpoint 迁移或 exactly-once 外部副作用。

## 压缩与恢复

`RegistryOptions` 以 committed transaction 数和 WAL bytes 两个阈值触发 snapshot。snapshot 临时文件
写入并 fsync 后原子 rename；Unix 再 fsync 父目录，Windows 没有可用的目录 fsync API，因此依赖已同步的
snapshot 文件和同目录原子 rename。只有 snapshot 发布成功后才在现有句柄上 truncate WAL 并
`sync_all`，避免 Windows 无法覆盖已打开文件，同时保持任一 truncate crash window 可恢复。snapshot
记录 `last_included_log_index`，因此任一 crash window 都可选择旧 snapshot + 旧 WAL，或新 snapshot +
旧/新 WAL 恢复，不会重复应用 mutation。详细故障矩阵与长期运行证据见
[Issue #19 验收](acceptance/issue-19.md)。
