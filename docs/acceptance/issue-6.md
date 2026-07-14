# Issue #6 验收映射

| 验收项 | 自动证据 |
| --- | --- |
| Leader 被杀，多数派自动接管 | `leader_kill_elects_successor_and_fences_the_recovered_old_leader`。 |
| 旧 Leader 无法提交旧任期写 | 同测试恢复 node-a 后断言 Follower + `NotLeader`。 |
| 控制状态真实持久化/复制 | 三个真实临时 node snapshot；进程对象销毁后重开并重放。 |
| Phase 4 Registry 接入 CFT | `phase4_registry_records_roundtrip_through_the_cft_backend` 从 node-c 恢复真实 registry WAL。 |
| 3 Full 或 2 Full + Witness | `two_full_nodes_and_witness_form_quorum_without_allowing_witness_leadership`。 |
| Leader 切换时执行继续 | 旧 term 的有效 ExecutionGrant 在选举后仍获准继续。 |
| 新授权 fencing 旧结果 | 新 Leader 续发更高 epoch，旧 result 返回 `Fenced`。 |
| ControlLease 与 ExecutionGrant 分离 | `short_control_lease_expires_before_longer_execution_grant`。 |
| quorum 丢失无脑裂、可暂存结果 | `quorum_loss_blocks_global_writes_but_valid_grants_continue_and_reconcile`。 |
| 隔离节点不能自选 | `isolated_node_cannot_self_elect_from_its_own_log`。 |
| 任意管理节点查询/Follower 转发 | `control_log_is_bounded_and_queries_are_available_on_every_management_node`。 |
| 大型数据不进控制日志 | 同测试拒绝 70 KiB record；Phase 4 另有 2 MiB 数据/WAL 分离测试。 |
| Healthy → Suspect → Dead 与版本 pulse | `failure_detector_uses_versioned_pulses_and_suspect_before_dead`。 |
| 全部可用性状态 | `availability_distinguishes_impaired_degraded_quorum_lost_and_safe_stop`。 |
| Leader 偏好有滞回且不以 GPU 为准 | `leadership_preference_requires_health_margin_and_sustained_hysteresis`。 |

Phase 5 模块不依赖 ServiceHost runtime 或 Core 热路径，不启动采样线程；现有 Disabled Sidecar 的零后台任务
验收保持不变。
