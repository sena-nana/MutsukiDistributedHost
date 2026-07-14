# Phase 8：可选 TrustPlane

TrustPlane 只存在于 DistributedHost。Core Task、ServiceHost 本地控制 API、Runner 和插件 ABI 不获得
NodeId、签名、attestation 或分布式上下文；资源通过授权后仍在 Worker 本地转换为普通 `ResourceRef`。

## 身份与信任模式

非本机分布式链路必须是 Mutsuki Link `AuthenticatedEncrypted`。节点使用稳定 NodeId、递增 key generation、
有效期和信任等级；加入需要显式审批，轮换清零旧 key，吊销/隔离立即停止签名、资源访问和正式结果接受。
隔离后只能以更高 generation 的新身份和仍有效的完整性 verdict 重新加入，旧 lease/grant 因身份 generation
或 fencing epoch 不再有效。身份不可用只移除该远程节点，不改变本地 Core/ServiceHost 执行。

| 模式 | 语义 |
| --- | --- |
| TrustedLan | 管理员控制 CFT 集群、双向认证、ContentId 与最低完整性。 |
| AuditedLan | 增加收据、追加式审计、Merkle proof 和按策略验证。 |
| RestrictedWorkers | 低可信 Worker 只能执行显式允许的数据和任务。 |
| ByzantineResistant | 仅为未来 BFT/quorum-certificate 后端保留模式；不属于默认完成依赖。 |

本实现不包含 PoW、代币、最长链、公链广播或默认全量多副本计算。

## 数据、制品与授权

任务策略声明 Public/Internal/Confidential/Restricted/Credential、最低信任、外部节点/持久缓存许可、
attestation 和结果验证等级。Confidential 以上不得进入 Restricted/Untrusted；Credential 只允许 Trusted。
Phase 7 scheduler 同时硬拒绝非 Active 身份和未验证执行制品。

Host、Runner 和插件制品由 ContentId、版本、generation、authority HMAC 与 allowlist 校验。可选 attestation
遵循 `evidence -> verifier -> policy`，把 provider、NodeId、identity key、Host/制品摘要和有效期绑定；无证明
节点不能冒充高可信环境，但策略可允许其执行低风险任务。

资源授权由独立 Controller authority key 签发，并绑定 Task、Attempt、NodeId、identity generation、term、
epoch、ContentId、最小 scope、缓存策略和过期 tick。Worker 自身不能伪造 Controller 授权；任务结束、节点
轮换/降级、epoch 变化或显式撤销都会使授权无效。AssignmentLease、ExecutionGrant、receipt 和 resource
manifest 可用统一 `SignedStateBinding` 绑定 subject/signer 身份与 term/epoch。

## 收据与结果正确性

Worker receipt 的 HMAC 覆盖 Task/Attempt/term/epoch、输入输出 ContentId、task schema、Runner/插件
generation、execution variant、policy/quality/degraded flags 和环境摘要；正式接受可附带 control log/quorum
certificate 与 audit inclusion 信息。旧 Attempt/term/epoch 一律 fenced。

收据只能证明“该身份声明以该输入、实现与策略产生该输出，并被某次集群状态接受”，不能证明业务语义。
验证策略显式支持 None、HashOnly、DeterministicReplay、SpotCheck、N-of-M、DomainVerifier、Replayable、
ProofCarrying 和 ManualReview。确定性结果比较 canonical digest；近似结果必须提供容差/领域 verifier。
N-of-M 没有可信 expected digest 或领域判定时进入 ManualReview，绝不把最快结果或简单多数自动当作真值。

## 审计、信誉与失陷

审计事件只含摘要和最长 256 字节的小型 metadata，拒绝 secret/token/password/credential 和原始 input/output。
每次 append 写 JSONL、链接前一事件 hash 并 `sync_data`；显式批处理生成 Merkle root、inclusion proof，连续
segment 通过 previous root 验证一致性。可按 GlobalTaskId、Attempt 或 NodeId 重建 assignment、lease、
commit、verification、revocation 与人工裁决链。逐帧/心跳/本地实时路径不签名也不逐项多签。

信誉按 Node × capability × plugin generation 使用慢变 EWMA、最小样本和 uncertainty，只作为 scheduler
risk、抽检和审查输入，不进入选举或覆盖硬策略。失陷处置隔离身份、提升 fencing epoch、撤销授权、标记该
节点产出的结果/checkpoint/resource provenance，并输出验证/重算、可信副本修复、key 轮换、回滚/补偿/
人工审查动作；证据保留在审计链。

## 预算与关闭语义

签名、验证、replay、attestation、信誉、审计，以及 compute/network/storage 均有独立每 tick 预算。预算
不足时拒绝高保障声明，不静默降低验证或身份级别。所有高级操作由显式调用驱动；profile disabled 时即使
配置了 signed audit/attestation/reexecution，也不会激活后台工作。最低身份认证可作为独立部署基线保留。
