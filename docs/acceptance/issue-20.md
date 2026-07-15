# Issue #20 验收映射

## 公开边界

README Capability Matrix 与 `DistributedCapability::maturity()` 同时标记 Contract、ReferenceModel、
InProcessTest、Deployable、ProductionReady 和 Unavailable。`ReplicatedControlPlane` 已更名为
`ReferenceCftModel`；HA deployment 结构化返回 `ExperimentalUnavailable`。Durable/Critical 仍是
参考与进程内验证能力，不进入当前 production assembly。

## Clustered MVP

`process::tests::independent_process_worker_submits_queries_cancels_pulses_and_drains` 启动三个独立
OS 进程：管理/Controller 测试进程、Worker 子进程和 content origin 子进程。该测试验证：

- MutsukiLink local session 经过 HMAC 双向身份与 OS peer credential 校验；
- Worker capability describe、周期 pulse 和真实 health；
- management client 经 Controller 提交、查询、取消普通 `PortableTask`；
- Worker 使用同一 ServiceHost `HostAdapter` 路径提交普通 `TaskBatch`；
- 2 MiB+17 bytes 输入从 content origin 直接分块流向 Worker，Controller 只携带 `ContentId` 和
  endpoint descriptor；Worker 原子发布前验证长度和 SHA-256；
- shutdown 先进入 drain，再停止 Worker session 和进程；
- transport 关闭返回结构化错误，后续请求会重新建立并认证 session；Coordinator 仅对满足
  portability/retry-safety 的任务创建新 Attempt，旧结果继续由 attempt fencing 拒绝。

## HA gate

当前 HA 没有启用。既有选举、隔离、恢复、term/epoch fencing 测试只覆盖
`ReferenceCftModel`，不能作为 deployable HA 证据。binary 明确拒绝 `high_availability`；因此
“启用 HA 前必须通过三节点多进程故障测试”作为发布 gate 保持 fail-closed，不会因参考测试而
误标已完成。完成真实独立节点通信和持久日志 backend 后，才允许把 Capability Matrix 的 HA
Deployable 列改为是。
