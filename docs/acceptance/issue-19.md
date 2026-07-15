# Issue #19 验收映射

## 持久化协议

`PersistentRegistry` 以一个 transaction mutex 保护当前 records、每任务 version、单调
log index、pending transaction、本地 WAL 和 snapshot 轮换。每个状态转换在锁内完成：

1. 根据锁内当前 record 校验 transition，并只生成目标 record 的候选新值。
2. 生成包含 transaction id、log index、previous transaction、previous/new version 的
   prepared payload，先写本地 framed WAL。
3. 以相同 transaction/log index 遍历副本，直到达到回执数或候选耗尽。
4. 达到 Fast/Durable/Critical 要求后写本地 commit frame，最后发布候选 record。

方法失败时不会发布正式内存状态。未提交 prepare 在重启时从其首字节安全截断；副本不足时，
同一进程内重试复用原 transaction，已经成功的副本回执不会重复计数，副本 commit 自身幂等。
`AcceptanceReceipt` 携带 transaction id、log index 和每个 committed replica 的证明；
`last_commit_report` 同时保留已尝试副本的成功或结构化失败类型。

## WAL 与 snapshot

- WAL frame 是 `length + magic/version/kind + log index + payload length + payload + SHA-256`。
- replay 只应用 prepare/commit 配对且 transaction chain、log index 和 task version expectation
  全部一致的记录。
- EOF 半写 frame 或未完成 prepare 自动截断；完整 frame checksum 失败返回 `Corrupt`，并在
  error detail 中报告精确 byte offset。JSON 只保留为 snapshot/诊断 payload，不再承担 framing。
- snapshot 先写临时文件并 fsync，再原子 rename 并 fsync 父目录；之后才在现有 WAL 句柄上
  truncate 并 `sync_all`。snapshot 已 rename 但旧 WAL 尚未截断时，replay 按
  `last_included_log_index` 跳过旧记录。
- `RegistryOptions` 同时限制 task/record，并以 WAL transaction count 或 bytes 触发压缩；
  因为每次 commit 后检查，WAL 最大越界量至多为当前 bounded transaction 的两个 frame。

## 自动验收证据

| 验收项 | 行为测试或证据 |
| --- | --- |
| 所有状态转换在线性化临界区 | `concurrent_transitions_linearize_and_replay_to_the_same_record`，32 线程争用只允许一个 assign 成功，重启状态完全一致。 |
| 失败 mutation 不污染 replay WAL | `failed_local_prepare_or_commit_never_publishes_memory_or_replay_state` 分别注入 prepare 和 commit 写失败；`replica_leading_local_commit_failure_recovers_the_prepared_transaction` 验证副本领先后重启仍重试原 transaction。 |
| 前置副本失败后继续尝试 | `replica_selection_skips_failures_and_retry_reuses_the_transaction` 验证失败副本后命中后续健康副本。 |
| Durable/Critical 可证明 | receipt 断言 transaction/log index/replica proof；既有 Durable/Critical 真实文件副本和 CFT 恢复测试继续通过。 |
| 半写恢复与中间损坏定位 | `half_written_registry_tail_is_truncated_without_losing_committed_state` 与 `checksum_corruption_reports_the_exact_frame_offset`。 |
| snapshot/compaction crash window | `snapshot_compaction_recovers_before_and_after_wal_truncation` 覆盖未完成 snapshot temp、snapshot 已发布但旧 WAL 尚存、WAL 已截断。 |
| 重复 transaction 幂等 | scripted replica 首次失败后，同一调用重试收到完全相同 transaction id/log index；文件和 CFT replica 也按该 pair 去重。 |
| 10 万/100 万长期运行 | `persistent_registry_stress` 在真实文件 WAL 上执行 submit/assign/running/cancel，最终压缩、重启并逐项核对 log index/样本记录。结果见 `artifacts/issue-19-registry-stress.json`。 |

长期运行结果为本机 optimized profile 的回归基线，不声明跨硬件绝对性能：10 万 mutation
1.276429 秒、重启 0.122532 秒；100 万 mutation 12.947083 秒、重启 1.263300 秒；两次最终
WAL 均压缩为 0 bytes，权威状态保存在原子 snapshot。
