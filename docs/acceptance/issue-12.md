# Issue #12 验收映射

| 验收项 | 自动证据 |
| --- | --- |
| 未认证/吊销节点不能加入、续租、读取资源或提交结果 | `encrypted_identity_rotation_revocation_and_resource_leases_are_fenced` 验证加密链路、审批、轮换、吊销、identity generation 与 term/epoch 授权 fencing。 |
| 敏感任务不进入低可信节点 | `sensitive_policy_artifact_integrity_and_attestation_are_hard_filters`；`scheduler_rejects_revoked_identity_and_unverified_runtime` 同时验证 scheduler 身份/制品硬过滤。 |
| 制品与执行环境完整性 | 同测试篡改 ContentId 后 allowlist/HMAC 拒绝，并验证 attestation 绑定 NodeId、identity key、Host/制品摘要和有效期。 |
| 收据可独立验证身份、实现、输入输出与提交状态 | `receipts_bind_identity_implementation_attempt_epoch_and_commit` 验证 HMAC receipt、CommitProof、旧 Attempt fencing、篡改输出拒绝、治理 certificate 与通用 signed grant binding。 |
| 确定性错误与近似任务容差 | `deterministic_and_approximate_verification_quarantine_wrong_results` 验证独立 replay digest、显式向量容差、所有策略的工作计划，并证明无可信真值的 N-of-M 进入 ManualReview。 |
| 追加式审计与完整执行链追踪 | `persistent_audit_chain_merkle_proofs_and_task_trace_are_verifiable` 使用真实文件 append/fsync/reopen，验证 hash chain、Merkle inclusion、segment consistency 及 Task/Attempt/Node trace。 |
| 节点失陷隔离、授权废止与影响追踪 | `compromise_drill_revokes_access_quarantines_provenance_and_fences_epoch` 验证 quarantine、epoch +1、授权撤销、受影响 Task/content 和新身份+完整性检查后重入。 |
| 信誉慢变有界且不影响控制安全 | `reputation_is_bounded_slow_and_never_enables_disabled_background_work` 验证最小样本、EWMA/anomaly、风险只读输入和固定容量。 |
| 高级功能关闭无全量复算开销 | 同测试验证 disabled profile 不激活 signed audit/attestation/reexecution；TrustPlane 无轮询或后台线程。 |
| 独立安全预算 | 同测试验证签名、验证、replay、audit bytes 与 compute/network/storage 超额拒绝。 |

签名、Merkle、信誉和多数输出均不被描述为业务正确性证明；ByzantineResistant 不作为默认 CFT 控制面的
替代品，也没有把不受信任节点引入权威状态维护。
