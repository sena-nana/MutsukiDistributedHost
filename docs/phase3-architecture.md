# Phase 3：零侵入远程执行

## 边界

```text
origin client
  ├─ small control envelope ──> DistributedHost coordinator
  │                              └─ authenticated Link control ──> WorkerEndpoint
  └─ resource/result streams ────────────────────────────────────> ResourceLocalizer
                                                                  └─ ordinary TaskBatch
                                                                     └─ local HostRuntime
```

DistributedHost 只持有 `GlobalTaskId ↔ Attempt ↔ local TaskHandle` 映射。Worker 先校验 portable
envelope，再通过独立数据通道本地化 `DirectDataRef`，最后把清除旧运行时状态的普通 Task 交给
本地 Host。插件只能看到本地 `Task`、`ExecutionContext` 和 `ResourceRef`，不会看到节点、远程
位置或 fallback 分支。

本地适配器只消费 ServiceHost 的公开 control API：批量提交、取消、snapshot、outcome、增量
event page、drain 和 health。后续 Host 只需实现同一 `HostAdapter`，不需要复制插件路径。

## 运行等级

| 等级 | 行为 |
| --- | --- |
| Disabled | 无 Host 连接、无网络、无后台任务；构造与销毁都是惰性的。 |
| LocalObservable | 只通过本地 IPC 查询 health/snapshot/event；不接管 Host 生命周期。 |
| Clustered | 在上层显式注入认证 Link transport、Worker registry 和 localizer 后允许远程执行。 |

Sidecar 不启动或停止 ServiceHost。IPC 失败、版本不兼容或 Sidecar 崩溃只会让分布式控制调用结构化
失败；已经在本地 Host 内运行的任务继续由 HostRuntime 管理。

## 控制面与数据面

- `control` 是有界的 request/response channel，单 frame 最大 64 KiB。
- `resource` 与 `result` 是独立 Link stream；控制 envelope 只携带 ContentId、大小和 endpoint
  descriptor，大型字节不经过 coordinator。
- Worker 完整能力只在加入或版本变化时更新；pulse 只携带 snapshot version、健康和队列摘要。
- 候选必须同时满足 envelope、Task schema、requirements、Runner generation 与 resource schema。

## Phase 3 失败语义

| 能力 | Worker 断开后的行为 |
| --- | --- |
| `LocalOnly` 或旧插件缺少 portability 描述 | 始终保留 origin 本地执行。 |
| `Portable` + `Unsafe` | 可首次远程执行，但状态不确定时不自动重试，返回 `RetryUnsafe`。 |
| `Restartable/Checkpointable` + Idempotent/Verifiable/Compensatable | 从原输入创建递增的新 Attempt；旧 Attempt 结果被 fencing 拒绝。 |
| 不兼容 Runner/resource/capability | 不向插件暴露远程错误，自动保留本地执行。 |
| Worker admission 拒绝或 transport 断开 | 最多尝试配置的有限 fallback，然后本地执行。 |
| 资源本地化失败 | 在进入 HostRuntime 前返回 `LocalizationFailed`，插件不会收到远程资源。 |

Phase 3 不承诺 exactly-once 外部副作用、持久任务账本、Leader 选举或 checkpoint 迁移；这些属于
后续阶段。
