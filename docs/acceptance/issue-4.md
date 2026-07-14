# Issue #4 验收映射

| 验收项 | 自动证据 |
| --- | --- |
| 完整 GlobalTask 状态与 Attempt 映射 | `task_failure_recovery_and_cancellation_have_explicit_terminal_states`、两阶段输出测试。 |
| Durable 返回前持久复制 | `acceptance_modes_are_explicit_and_durable_records_survive_entry_loss` 使用真实 primary/follower WAL。 |
| 入口节点丢失后任意管理节点查询 | 删除 primary 进程对象后从 follower WAL 按 GlobalTaskId 重放查询。 |
| Fast/Durable/Critical 不伪装 | `durable_acceptance_fails_without_real_metadata_or_input_redundancy`；receipt 断言实际模式/副本数。 |
| ContentId、chunk hash、去重、断点续传 | `chunk_upload_resumes_after_reopen_and_deduplicates_content`，并验证损坏 chunk 被拒绝。 |
| 真实资源副本 | `two_copy_resource` 在两个独立临时 ContentStore 写入并读取相同 ContentId。 |
| 控制日志无大型数据 | `large_input_bytes_never_enter_the_registry_log`：2 MiB 输入、WAL 小于 64 KiB 且无 payload bytes。 |
| 控制/数据独立预算 | Link control/resource/result channel descriptor + `direct_resource_and_result_lanes_obey_independent_data_budget`。 |
| OutputStaged 后接管提交 | `output_is_staged_then_committed_by_another_management_node`。 |
| 旧 Attempt 不覆盖 | `stale_attempt_output_is_preserved_as_conflict_and_never_committed`。 |
| 资源修复和安全回收 | `resource_catalog_plans_bounded_repair_and_retention_aware_gc`。 |

这些测试使用真实临时目录、文件、fsync、WAL 重放和 chunk hash；transport 只在 Phase 3 的 wire e2e 中使用
loopback，未把内存 mock 结果当作持久化验收。
