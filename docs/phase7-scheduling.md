# Phase 7：低干扰异构调度

调度只响应 `NewTask`、节点/能力版本变化、Admission 拒绝和会话迁移请求，不启动高频全局扫描线程。
任务按安全/恢复、LatencyClass、本地来源、deadline 风险、DAG 关键性与解锁收益、业务优先级、aging 和
fair-share 的字典序出队。安全/恢复和本地实时工作始终先于可退让的远程工作。

## Placement 管线

```text
capability/plugin 索引
  -> 健康、OS/ABI、信任、内存/显存、数据策略和 generation 硬过滤
  -> 粗压力 Top-K
  -> 最低质量、deadline、P95/P99、jitter、流式 TTFT/steady 和失败风险过滤
  -> 风险调整成本排序
  -> 目标节点最新状态 Admission + 短时 Reservation
```

调度单位是 `Node × ExecutionVariant`。成本包含 queue、RTT、输入传输、预热、执行、输出、提交、jitter、
失败概率乘恢复成本、能耗、DAG 中间数据、会话迁移和稀缺能力惩罚；同节点数据/会话位置作为奖励。通用任务
不会无代价占用只有少数节点具备的 GPU 或可信执行能力。会话迁移成本未被未来收益显著覆盖时保持粘性，
DAG 跨节点成本高于并行收益时计入 placement。

性能画像按 `task type × variant × input bucket` 保存固定 32 bucket histogram、EWMA、峰值内存、失败率和
吞吐估计，不保存原始样本。样本不足带 uncertainty penalty；实时/关键任务的 penalty 高于 Batch，后者可在
硬 SLO 内有限探索。首选节点拒绝时直接使用已经完整评分的 fallback，不重新扫描集群。

## 本地最终准入

全局状态是近似值，目标节点用 capability version 和最新压力再次判断。Reservation 有明确到期 tick，避免
并发超额承诺；返回 Accept、RetryAfter、Overloaded、InsufficientMemory 或 CapabilityChanged。远程可用
CPU、内存、显存和线程上限会扣除本地硬保留。压力升高依次降低远程并发、暂停可检查点 Batch、取消远程
Background；本地工作仍可使用保留预算。

## 低干扰与网络预算

telemetry 在显式 pulse 时合并健康、capability/resource version、压力 EWMA 和有界事件计数。稳定时指数
降低采样频率；过载时先丢弃精细 telemetry，再停止可过期调度摘要，但 correctness 状态不丢弃。
`DistributedBudgetMeter` 对调度/telemetry 操作和 hash/disk 字节计数；`NetworkBudgetController` 将小型控制
消息保留在独立额度中，数据受带宽、排队字节和并发数限制，且控制 Leader 不能转发大数据。

拥塞降级固定为：停止预复制 → 降低 checkpoint → 暂停远程 Batch → 拒绝大型远程任务 → 仅控制通道。
重连高压时直接 ControlOnly，因此先恢复成员/授权/活跃任务正确性，再由预算允许后台资源修复，不会同步风暴。

## 分发收益门槛

远程候选只有在 `LocalEstimatedCost > RemoteCost + SafetyMargin` 时保留。RemoteCost 已含所有调度、网络、
预热、执行、输出、提交、干扰和恢复风险。HardRealtime、帧级、强本地设备依赖、毫秒级短任务默认本地；
低质量变体只有请求显式允许质量损失且仍满足最低质量时才可参与。
