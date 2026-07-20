# Issue #23 验收映射

## 根因

旧 `1 GiB` content case 的大小是每个 transfer 的大小，因此 c4 实际搬运 4 GiB，c16
实际搬运 16 GiB。后者在 16 GiB Apple M4 上是容量与磁盘压力场景，不能作为等量扩展比较；
没有证据表明 production content/registry API 存在共享锁或缓存路径退化。

Durable/Critical 每次 mutation 分别执行 3/4 个顺序持久化同步点。旧 benchmark 还把自动
compaction 混入 mutation 总耗时，并且每个进程只形成一个样本，因此约 83/63 mut/s 的契约
成本被误读为锁竞争和抖动。

## Benchmark 修复

- 保留 `1 GiB × concurrency`，标记为 `per-transfer-capacity-stress` 并记录 aggregate bytes
  与 RAM pressure ratio，不再作为 c4/c16 等量扩展门禁。
- 增加固定 4 GiB 总量 lane：c4 为 `4 × 1 GiB`，c16 为 `16 × 256 MiB`；c16/c4 miss
  median 必须不低于 0.90。
- 每个 process run 轮换 concurrency 顺序，并执行一个不计入样本的 warmup；digest、完整
  落盘、offline hit 与 resume correctness 保持零容忍。
- Registry mutation lane 禁用自动 compaction，snapshot/compact/reopen 独立测量；10,000
  mutations 按固定 100-mutation window 输出 median/p95/p99/MAD，并记录预期同步点数。
- Durable/Critical 的 window MAD/median 必须不高于 0.10；reopen、log index、首尾 task、
  committed mutation 与 WAL 状态继续作为行为断言。

实现 revision 为 `bb94ad8b54044f62e16abf2b0281c14427c72573`。

## 固定机证据

`artifacts/performance/reference-macos-arm64-issue23/` 来自干净、独立 checkout 的三次完整
reference run。环境为 Apple M4、16 GiB RAM、macOS arm64、AC power、低电量模式关闭、无
活动虚拟化；报告固定 DistributedHost `bb94ad8b54044f62e16abf2b0281c14427c72573` 与
ServiceHost `9fb03856c95027f849753c3b012e87a52f1598e7`，两者均为 clean checkout。

| Issue #23 gate | 结果 | 门槛 |
| --- | ---: | ---: |
| 固定 4 GiB c4 miss median | 1.750 GB/s | 记录值 |
| 固定 4 GiB c16 miss median | 2.063 GB/s | 记录值 |
| c16/c4 miss ratio | 1.179 | >= 0.90 |
| Durable window MAD/median | 0.0237 | <= 0.10 |
| Critical window MAD/median | 0.0776 | <= 0.10 |
| correctness non-zero counters | 0 | 0 |

`report-analysis.json` 的 `issue23.passed` 为 `true`。总报告的 `case-specific-noise` 来自既有、
非 Issue #23 latency cases 的 MAD，不属于 content fixed-total 或 registry window gate，不能
改写为本问题的回归。

MutsukiCore report validator 接受全部 251 cases；exact-byte approval 的报告 SHA-256 为
`5ef6804a5267b1bee603aa90b8a70b1c08efc47d44ba37a4ca4f652415610456`，同一报告的 baseline
comparison 共验证 1,355 项、0 失败。
