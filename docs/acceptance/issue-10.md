# Issue #10 验收映射

| 验收项 | 自动证据 |
| --- | --- |
| 空闲 CPU 接近噪声、无高频全局扫描 | 调度器/telemetry 均无后台线程，只在 `SchedulingEvent`/pulse 调用执行；`indexed_top_k_selects_heterogeneous_variant_and_precomputes_fallback` 以 35+ 节点证明只完整评估 Top-K。 |
| 字典序优先级与本地实时优先 | `lexical_priority_preserves_safety_latency_and_local_work`。 |
| CPU、CUDA、Metal 异构 placement | `indexed_top_k_selects_heterogeneous_variant_and_precomputes_fallback` 按 capability/plugin 索引分别选择 CUDA 与 Metal 变体，并保留 CUDA fallback。 |
| SLO、最低质量和真实运行画像 | `PerformanceModel` 的有界 histogram/EWMA/P50/P95/P99/吞吐/峰值内存/失败率；`performance_model_is_bounded_and_adds_uncertainty`。 |
| 过期全局状态由最终 Admission 修正 | `local_admission_corrects_stale_state_and_protects_local_reserve` 验证 CapabilityChanged、Overloaded、Reservation 和本地硬保留。 |
| 本地负载升高时远程工作退让 | 同测试验证压力触发 CancelRemoteBackground；中间压力依次返回降低并发与暂停 Batch。 |
| 控制面不转发大数据、重连/修复受预算 | `telemetry_and_network_work_only_on_events_and_degrade_by_budget` 拒绝 Leader data forwarding，验证控制保留、并发/字节限制、拥塞降级和 hash/disk/调度操作预算。 |
| 短任务/帧任务不发生远程负优化 | `profitability_gate_keeps_short_and_frame_tasks_local` 与 `cargo bench -p mutsuki-distributed-runtime --bench placement_profitability`；基准对一百万次决策断言负收益远程接收数为 0。 |

Core、ServiceHost、Runner 和插件执行路径未改动。DistributedHost 只消费通用本地观察/控制状态；placement
结果仍通过已有 Host adapter 提交普通 Task，所有大数据仍走已有点对点 resource/result 数据通道。
