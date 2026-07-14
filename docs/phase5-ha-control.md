# Phase 5：高可用控制面

## CFT 后端与节点角色

`CftControlBackend` 是可替换边界；仓库内的 `ReplicatedControlPlane` 是文件持久化的小集群 CFT 参考
实现。它只复制有界权威元数据，默认接受以下拓扑：

- 3 个或更多 Full 投票节点；或
- 2 个 Full 投票节点 + 1 个不能成为 Leader 的轻量 Witness。

每次管理写入都带递增 log index、term 和 epoch。Follower 可本地读取已提交副本，也可把写入路由到
可达 Leader。单 record 最大 64 KiB；输入、输出、chunk manifest 和流式数据仍只走 Phase 4 数据面。
`CftRegistryReplica` 把 Phase 4 的真实 registry mutation 写入该有界日志，并能从任意管理副本恢复
registry WAL；因此 HA 日志不是与 GlobalTaskRegistry 平行的示例状态机。

候选只有在日志不落后且获得多数票时才能成为 Leader。隔离节点无法靠自己的日志长度或任务数量
自选；旧 Leader 恢复后先同步当前 term/log，再以 Follower 身份运行，旧 term 写入被拒绝。

## 授权分层

| 授权 | 生命周期 | quorum 丢失时 |
| --- | --- | --- |
| ControlLease | 短，绑定当前 Leader + term + 逻辑 tick。 | 新 Durable 写、成员变更、generation 切换和不可逆副作用全部停止。 |
| ExecutionGrant | 较长，绑定 GlobalTaskId + Attempt + Worker + term + epoch。 | 有效期内的纯计算/实时任务可继续，结果标为 executed-uncommitted。 |

租约只使用单调逻辑 tick，不比较节点墙钟。新 Leader 可确认并续发同一 Attempt 的新 grant；epoch
递增后，旧 grant/result 即使来自原 Worker 也被 fencing。

## 成员与故障检测

完整 capability/resource 只在加入或版本变化时同步。正常 pulse 只携带两个 version、health 和 pressure
bucket；版本不匹配返回 FullSnapshotRequired。无 pulse 时先 Healthy → Suspect，再到 Dead，不因一次网络
抖动立即重跑任务。稳定或 Overloaded/Draining 时拉长采样间隔，避免与本地实时负载竞争。

| 故障 | 行为 |
| --- | --- |
| Leader 失联 | 多数派选新 Leader；有效 ExecutionGrant 继续。 |
| Worker/本地 ServiceHost 失联 | 仅该节点执行能力变为 Suspect/Dead；恢复策略由后续阶段决定。DistributedHost 不接管 ServiceHost 生命周期。 |
| 用户入口失联 | 任意管理节点按 GlobalTaskId/已提交日志继续查询，Follower 可转发写。 |
| quorum 丢失 | 禁止权威新写；允许本地工作和有效 grant，结果暂存为 executed-uncommitted。 |
| 隔离恢复 | 由 Accept/Reexecute/Compensate/Reject/ManualReview 显式协调，不按最快完成自动提交。 |

## 可用性状态

- Healthy：所有投票节点可达。
- Impaired：存在故障但可达票数高于最小 quorum。
- Degraded：仅剩最小 quorum。
- QuorumLost：Leader 仍存活但无法获得多数；权威写停止。
- Isolated：节点与多数派隔离，但仍持有有效 ExecutionGrant。
- SafeStop：隔离/失联且没有可继续授权的任务。

Leader 安全由 CFT 选举决定。网络 P95、存储健康、休眠风险、控制余量与信任只用于候选偏好或受控
leadership transfer，并要求持续 margin + dwell hysteresis；GPU 算力不参与 Leader 安全判断。

Phase 5 不实现 Byzantine 容错、跨节点墙钟租约或大型数据共识复制。
