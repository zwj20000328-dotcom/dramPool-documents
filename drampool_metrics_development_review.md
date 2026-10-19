# DramPool Metrics 开发流程总结

> 覆盖区间：2026-09-17 ~ 2026-10-16 ｜ 分支 `z00984767-0917` ｜ 报告日期：2026-10-16
> 素材来源：24 个会话转录、仓库代码（`ucm/store/dram/cc/drampool/`）、以及 `dramPool_documents/` 下的设计/联调文档。
> 本文所有代码结论均标注 `file:line`；所有历史结论均标注会话 id、提交号或文档出处。**指标名、PromQL、错误码、配置键保持英文原文。**

---

## 0. 摘要

DramPool 是一个独立于 vLLM 的 C++ 守护进程，因此它的指标不能直接用 Prometheus client 暴露，必须走一条跨进程链路回流到 vLLM 的 `/metrics`。这个约束决定了整个开发流程的形状：**先定指标口径，再解决"怎么把值搬出去"，最后在真实硬件上逐层对数。**

```
DramPool 进程(C++)                UCM/vLLM 进程(Python)
 业务埋点 UpdateStats
   └─ thread-local 双 buffer（UC::Metrics，无锁）
        └─ MetricsReporter 线程：每 metrics.interval_ms 读后即清
             └─ 累积为进程级累计值 → 单行 JSON → tmp + rename 原子替换
                  └─ drampool_metrics.json  ←── 跨进程唯一通道 ──┐
                                                              ↓
                              DramPoolResourceReporter（flock 选主、尾部读、差分）
                                └─ ucmmetrics → vLLM `/metrics`（前缀 `ucm:`）
                                     └─ Prometheus → Grafana
```

规模演进：

| 阶段 | 数量 | 说明 |
|---|---|---|
| 设计文档定稿（2026-09-18） | **32** | 13 Histogram + 13 Counter + 6 Gauge（其中 1 类动态注册） |
| 设计过程六轮增删 | 41 → 38 → 36 → 34 → 32 | 见 §1.4，每一轮都有明确的留痕与恢复方式 |
| 当前代码（2026-10-16） | **41 个名字** | = 40 个静态注册项 + 1 个仅作前缀的基名常量；另有按 block size 运行时动态注册的 N 个 gauge |
| Python 侧附加 | +1 | `drampool_resource_log_read_errors_total`（回流器自监控），`metrics_configs.yaml` 中共 42 条 |

净增的 9 个全部落在"把粗粒度拆成可归因的细粒度"上：`dump/load_metadata_duration_ms`、`dump/load_submit_duration_ms`、`get_status_duration_ms`、`response_submit_duration_ms`、`queue_request_residence_ms`、`queue_request_capacity`、`queue_completion_capacity`。

三个需要说清的口径（否则容易被数字误导）：
- **41 是"名字/指标类"数，不等于运行时注册数。** `SetupDrampoolMetrics()` 实际注册 40 个静态名 + **每个配置的 block size 各 1 个** gauge（`--kvcache-block-sizes` 是必填项），所以运行时注册数 **≥ 41**；而 `metrics.enabled = false` 时为 **0**。
- **整个导出面上是 42 个名字**（多出的是 Python 侧 `drampool_resource_log_read_errors_total`）。它会被快照导入环节按前缀过滤掉，因此不属于 32 那套"业务指标"范畴。
- **没有任何指标被删除、也没有任何指标改变类型**（13 个 Counter 保持不变）——这一轮的演化是纯增量。

一句话结论：**指标骨架（分组、口径、哨兵机制、桶策略）是扎实的，并在真实 vLLM 推理上完成了四层数值对齐；但"准确反映性能变化"这一条目前不成立——存在幸存者偏差、双写者竞争、Gauge 假 0 三类会输出错误数字的缺陷，以及 cache 业务主线（命中率/淘汰/占用）的整体空白。**

---

## 1. 指标设计

### 1.1 设计起点：五条框架语义约束

埋点方案不是自由选择，是被 `ucm/shared/metrics` 的实现语义锁死的。设计文档 §2.3 把约束显式列出，这一节是后续所有取舍的根：

| # | 语义 | 对方案的强制约束 |
|---|---|---|
| **C1** | **无标签维度**：内部是纯 `name → double` 扁平映射 | 带 `{op}`、`{slot_size}` 维度的指标必须拆成独立名字，或把维度编码进名字 |
| **C2** | **未注册名字静默丢弃**（`UpdateStats` 直接 return） | 所有指标必须在启动期集中注册；单测需覆盖注册完备性 |
| **C3** | **Histogram 存原始值向量**，上限 `maxVectorLen`，**满后丢弃后续观测** | 观测点必须控制在批次/请求粒度，不能每 entry 打点 |
| **C4** | **`GetAllStatsAndClear()` 读后即清**，返回的是"距上次调用的增量" | 写方必须自己累加成累计值再落 JSON；增量换算交给消费侧 |
| **C5** | 线程级双缓冲，首次 `UpdateStats` 的线程自动注册 | 各 worker 线程直接调用即可，无需手工登记 |

采集方案选型上，Handler 方案（位置编码 + `/` 分割）与 MetricsSlot 方案（全局固定数组 + 锁）均被否决，理由是"字段位置编码易错位"与"Histogram 更新存在锁竞争"；最终采用 **UCM thread-local + 双 buffer**（设计文档 §2.1）。

### 1.2 设计原则：S0–S4 判定标准

每个候选指标按优先级过筛（设计文档 §3.1）：

| # | 标准 | 含义 |
|---|---|---|
| **S0** | 业务直接相关 | 直接反映 DUMP/LOAD/LOOKUP 的服务质量（请求量、成功率、时延、命中率）、批次粒度耗时或资源水位。**纯系统性/接入层/内部调度/低频异常分类指标此阶段不涉及** |
| **S1** | 不可推导 | 无法由其他指标经 PromQL 运算等价获得 |
| **S2** | 可行动 | 指标异常时存在明确的运维/开发动作 |
| **S3** | 业务失败分类完整 | 缺失会导致业务失败被误判（如"重复写"与"真失败"） |
| **S4** | 开销合理 | 热路径 Histogram 数量从严控制 |

这条标准线砍掉了 22 个候选：**推导类 6 项**（带宽、batch_entries、ttl、命中率 Gauge 等）+ **非业务类 16 项**（接入层协议错误、内部调度诊断、低频异常分类、传输超时诊断、内部池背压、容量中间信号、GC 诊断）。被裁项全部留痕，并写明恢复方式（设计文档 §10）——这是这套设计里最值得保留的工程习惯。

失败类指标另有两条准入门槛：**(a) 明确归因价值** 或 **(b) 高发生概率**。不满足者按**管线阶段**合并成大类（`submit_failures_total` / `transfer_failures_total` / `response_failures_total`），保留定位能力而不留噪声。

### 1.3 分组结构（A–I 九组）

| 组 | 主题 | 现数量 | 设计意图 |
|---|---|---|---|
| A | 请求量 | 3 Counter | rate 类指标的分母基准 |
| B | DUMP 业务 | 2 Counter + 3 Hist | 写入侧规模、失败归因、本地准备耗时 |
| C | LOAD 业务 | 1 Counter + 3 Hist | 读取侧 miss、本地准备耗时 |
| D | LOOKUP 业务 | 1 Counter + 1 Hist | 查询 miss、扫描耗时 |
| E | 传输与响应 | 3 Counter + 5 Hist | 异步搬运、终态、响应回填链路 |
| F | 资源水位 | 2 Gauge + N 动态 Gauge | 内存/元数据容量压力 |
| G | Metadata 结算耗时 | 3 Hist | 模块边界测量（µs 精度） |
| H | 队列与阻塞 | 3 Counter + 2 Hist + 5 Gauge | 三条阻塞链的唯一量化信号 |
| I | 批次端到端 | 3 Hist | 用户明确要求的"整批数据总耗时" |

### 1.4 六轮增删：从 41 收敛到 32

设计不是一次成型的，`metrics_summary.md` 完整记录了六轮修改。这一节是理解"为什么最终是这些指标"的关键：

| 轮次 | 变更 | 数量 |
|---|---|---|
| 首轮重设计主线 | DUMP 新增 NoSpace **直测**（`dump_nospace_failures_total`，两次驱逐重试后仍 NoSpace 才计），删除 `dump_storebegin_failures_total`；LOAD miss 口径明确为"LoadBegin 失败 + 请求长度大于存储长度"；LOOKUP hit/miss 改累计 Counter；新增 I 组批次端到端 3 个 Hist | 41 |
| **二次修改** | **删除全部 [首条] 粒度观测**——G 组 storebegin/allocate/shard_register/evict_sync/loadbegin 5 个直方图 + `lookup_first_exist_duration_ms`，`thread_local` 首条门控机制整体移除 | — |
| **三次修改** | 批次实况 Gauge ×2 改造为 Counter `dump_failed_entries_total`；`lookup_batch_hits`（与 hit Counter 同点完全重复）与两个比率 Gauge（可由公式推导）删除 | 41 → 38 |
| **四次修改** | 删除吞吐字节 `dump/load_bytes_total`，带宽/批量推导随之取消 | 38 → 36 |
| **五次修改** | 删除 `load_initialized_entries_total`（miss 子类归因）与 `lookup_hit_entries_total`（命中率分子） | 36 → 34 |
| **六次修改** | 删除队列累计 Counter ×4（enqueued/dequeued），替代为**当前长度直测** Gauge ×2（`queue_request_size` / `queue_completion_size`） | 34 → **32** |

前三轮的核心动机是**批次观测**（用户口径："整批总耗时" + "批次失败累计"），后三轮则是在用户反复追问下**主动做减法**——删掉可推导项、删掉与既有指标重复项。剔除命中率分子/分母的代价是真实的：**两侧命中率目前都无法由指标推导**，只能监控 miss 绝对速率与突增（设计文档 §4.4 与 §10 对此留痕并给出恢复方式）。

### 1.5 设计之后的实现增补（32 → 41）

设计定稿后，实现阶段又做了两类补充，都是"粗粒度之外补一层细粒度"：

**① 把 prepare 拆成可归因的三段**（`task_worker.cc`）

```
dump_prepare_duration_ms   [批次] ← 整段，仍在（向后兼容）
 ├─ dump_metadata_duration_ms    ← 逐 entry StoreBegin + 缓冲分配 + 驱逐重试 + 传输段构建
 └─ dump_submit_duration_ms      ← ExecuteAsync 提交本身
```
代码注释明说拆分的理由：*"timed separately from the ExecuteAsync submission below so a prepare slowdown can be attributed; the prepare histogram keeps covering both plus the completion handoff."*（`task_worker.cc:151-155`）LOAD 侧对称。

**② 把"无法测量"的部分拆出可测量部分**

`*_transfer_duration_ms` 从提交起算到**终态观测**，本身就混合了三样东西：actual HiXL 搬运 + Poller 轮询延迟 + 终态 `GetStatus()` 执行。设计时无法拆；实现时补了 `get_status_duration_ms`（`completion_poller.cc:235` 与 `:359` 两处）与 `response_submit_duration_ms`（`completion_poller.cc:331` 仅测 `ExecuteAsync` 调用本身）。`drampool_metrics.h:65-68` 的注释把这层意图写得很清楚。

**③ 队列长度与容量**

SPSC 队列没有 size 查询接口，且 gauge 只能被单线程写（thread-local buffer 语义），因此用**生产者/消费者各自维护的原子计数器**做账，由各自线程刷新 gauge（`drampool_metrics.h:37-44`）。两个 `*_capacity` gauge 是为了让"队列利用率 = size / capacity"可以可靠计算，从常驻的 TaskWorker 循环里刷新，从而不依赖 GC 或流量（`task_worker.cc:49-55`）。

### 1.6 维度方案：从"聚合"到"名字后缀编码 + 导出侧解码"

这是整个 metrics 工作中**设计密度最高的一段**（会话 `7d189a46`）。起因是 `drampool_buffer_pool_usage_ratio` 需要按 block size 分维度，而框架 C1 明确"没有 label 的位置"。

三条路线被摆上台面：

| 方向 | 内容 | 结论 |
|---|---|---|
| A | 保持单一聚合值 | 拒绝——报告期聚合**永久销毁**了分尺寸可观测性 |
| **B-full** | 全框架引入真 label（`(name, labels) → value` 贯穿所有层） | **拒绝，判定为过度工程**。需改 ~12–14 个文件，包括域层标识模型（`name → MetricId`）、`GetAllStatsAndClear` 返回类型、pybind 签名、`CachedMetric`/`NAME_TO_METRIC_ID` 的缓存假设——**对每一个既有指标都是回归风险** |
| **B-scoped** | 线格式完全不变，动态维度用**数字后缀编码**进名字（`drampool_buffer_pool_usage_ratio_262144`），**只在两个 Python 导出端点解码成真 label** | **采纳** |

把 B-full 收敛成 B-scoped 的关键一步，是用户的一句反问——**"能否先指明有哪些指标需要使用label方式？"**。盘点后的结论是：**真正因缺 label 而丢失信息的指标只有一个**。

盘点把候选分三类：
- **需要 label**：`drampool_buffer_pool_usage_ratio` + `slot_size`。当时它把 per-size 数据丢掉了——`BufferManager::GetUsedSlotRatio(size)` 存在但**零调用点（死代码）**，而服务端报的是 `GetTotalUsedSlotRatio()`（`drampool_server.cc:499`）。
- **不需要 label（名字即维度）**：`{dump,load,lookup}_requests_total`、`*_batch_total_duration_ms`、`*_prepare/_metadata/_submit/_transfer_duration_ms` 等 op 维度族。理由是**无信息损失**（名字已经区分了 op），且为风格统一改动 ~15 个指标 + 面板不划算。
- **明确排除**：`shard` 作为 label。`kShardCnt = 1024` 是硬编码的锁竞争常量（`metadata.h:197`），**1024 倍基数爆炸换零运维价值**。

实现上有两条硬性发现，决定了方案形状：

1. **动态名字在 Python 侧会"死两次"**——原生 store 对未注册名字静默丢弃（`domain/metrics.cc:88`，`id == INVALID_METRIC_ID → return`），而 `_4096` 这类名字不可能出现在 YAML 里（block size 是 C++ 运行时配置）。→ 解法：`resource_reporter._ensure_gauges_registered()` 惰性 `create_stats`（幂等）。
2. **链路上有 4 道"只认精确名字"的关卡**——① C++ 注册 ② C++ `Render` ③ dispatcher 白名单 ④ 导出端。→ 解法：`metrics_config.py` 提供一个**共享** `split_dynamic_metric_name()` / `dynamic_label_bases()`，四处复用；配置用 `dynamic_labels: ["slot_size"]` 单点声明驱动全部 4 道关卡。空 `_dynamic_bases` 时该函数恒返回 `None`，走原静态路径——**向后兼容由构造保证**。

**历史发现（重要）**：`93f01c2f` 之前的 per-size 实现**从未真正把 per-size 系列导出到 Prometheus**——dispatcher 白名单和原生 store 两层当时就在，两边对不上。所以 B-scoped 的**真正新增价值全部在 Python 侧的 label 解码**，C++ 侧只是"恢复"（恢复的正是自己写过的、已知可编译的旧实现，这是编译信心的最强来源）。

### 1.7 桶集设计

当前 6 组桶常量（`drampool_metrics.h:159-168`），按生命周期的延迟尺度分层：

| 常量 | 桶数 | 数值 | 使用者 |
|---|---|---|---|
| `kMsBucketsSettlement` | 12 | 0.01 … 500 | `dump/load_submit`、`get_status`、`response_submit`、`storeend`、`loadend`、`request_enqueue_wait` |
| `kMsBucketsPrepare` | 14 | 0.1 … 5000 | `dump/load_prepare`、`dump/load_metadata`、`request_residence` |
| `kMsBucketsTransfer` | 14 | 0.1 … 5000 | `dump/load_transfer`、`response_rtt`、`dump/load_batch_total` |
| `kMsBucketsScan` | 10 | 0.1 … 500 | `lookup_scan` |
| `kMsBucketsLookupBatch` | 14 | 0.1 … 5000 | `lookup_batch_total` |
| `kMsBucketsGc` | 12 | 1 … 10000 | `metadata_evict_gc` |

**桶集统一（`7ce12bca`，2026-10-09）**：起因是用户报告的**面板现象**——"两者的纵坐标轴不统一"：两张"端到端延迟"热力图，DramStore 侧 Y 轴到 10s 而 DramPool 侧只到 1s。根因不是面板配置（两张面板的 `yAxis` 与 `le` 过滤正则**完全相同**），而是**桶集本身不同**：`operation=lookup` 时 drampool 用 11 值桶（上限 1000ms），2000/5000/10000 的 `le` 序列根本不存在。用户直接给出了要求的刻度并指示改代码：

> 好的，修改代码，要求刻度范围是 0.1,0.5,1,2,5,10,20,50,100,200,500,1000,2000,5000

于是 `transfer`/`rtt`/`lookup_batch` 统一到 14 值布局（transfer 去掉 10000，lookup_batch 从 11 值扩到 14 值），共 3 个文件 20 处取值改动。**已接受的代价**：transfer/rtt 类原本 5000–10000 有独立桶，现在 >5000ms 全部落入 `+Inf`——这个损失与 §4.1-4 是同一个问题的两面。

统一后 `kMsBucketsPrepare` / `kMsBucketsTransfer` / `kMsBucketsLookupBatch` 三者**内容完全相同**但仍保留为三个独立数组（判定为无害的历史冗余，未合并；要合并需先确认它们被期望永久对齐）。

> 一个值得记住的机制（来自一次历史 bug）：框架的 `NormalizeBuckets` **只追加 `+Inf`、没有默认桶集**（`metrics.cc:31-38`）。所以直方图注册时**必须显式传桶数组**，否则不会退化成粗粒度直方图，而是**只有一行 `+Inf`、完全没有分辨率**——该 bug 曾让热力图整块空白（详见 §3.6-㉔）。

时钟精度：新增 `SteadyNowUs()`（µs，`drampool_types.h`）。原因是 metadata 单次操作是微秒量级，毫秒时钟下直方图会退化成 0/1 两值。所有 Histogram 内部以 µs 计时、以 **ms（double）** 入直方图，指标名统一 `_ms` 后缀。

### 1.8 交付形态与开关链

四层开关，都在进程启动时读一次（无热重载）：

```
gc.enabled (默认 true)                    → 唯一的 F 组 gauge 生产者
  └─ metrics.enabled (默认 false)         → 决定 SetupDrampoolMetrics() 是否执行（含 SetUp + 全部 CreateStats）
       └─ drampool_resource_metrics_enable → Python 回流器是否启动
            └─ Prometheus scrape
```

`metrics.enabled` 默认 **false**（`drampool_config.h:67`），且校验"enabled ⇒ interval_ms > 0"（`drampool_yaml_config.cc:315-317`）。

---

## 2. 重点指标解释

以下按组给出。格式统一为 **测什么 → 反映什么 → 怎么用**，并标注埋点位与社区观测粒度（**[批次]** 每批次一次 / **[entry]** 每 entry 一次 / **[轮]** GC 每轮一次 / **[覆盖]** Gauge 覆盖写）。

### 2.1 A/B/C/D 组：业务量与失败

| 指标 | 类型 | 测什么 | 反映什么 | 怎么用 |
|---|---|---|---|---|
| `drampool_dump_requests_total` | Counter | 累计接收并处理的 DUMP 请求数（每请求 +1，与 batch 内 entry 数无关） | 写入负载规模 | 写入侧所有 rate 指标的分母；负载构成分析 |
| `drampool_load_requests_total` | Counter | 同上，LOAD 侧 | 读取负载规模 | 读取侧 rate 指标基准 |
| `drampool_lookup_requests_total` | Counter | 同上，LOOKUP 侧 | 查询负载规模 | 换算单请求查询成本的分母 |
| `drampool_dump_nospace_failures_total` | Counter | StoreBegin 中**缓冲分配最终失败且原因为 NoSpace** 的 entry 数——两次驱逐重试后仍 NoSpace 才计，注册失败不计 | 内存压力的**精确归因** | 持续增长 = 数据池容量不足或驱逐策略失效；与 `buffer_pool_usage_ratio_*` 交叉验证；动作 = 扩容 / 调驱逐比例 / 排查突发写入 |
| `drampool_dump_failed_entries_total` | Counter | 累计**结果为 Failed** 的 DUMP entry 数（四条失败路径合并，`record.results` 定稿值） | 写入失败总规模 | 速率持续 >0 = 写入链路异常；结合 `nospace_failures`（元数据侧）与 `transfer/submit_failures`（传输/提交侧）区分根因 |
| `drampool_load_miss_entries_total` | Counter | LOAD 中**未取到数据**的 entry 数 = LoadBegin 失败 + 请求长度大于存储长度 | 读取未命中规模 | 突增 = 读到了被驱逐/过期/未写完的数据。**注意命中率算不出来**（见 §4.2） |
| `drampool_lookup_miss_entries_total` | Counter | LOOKUP 中**不存在或非 READY** 的 entry 数（在 `completion_poller.cc:59-62` 由 `record.results` 统计） | 查询未命中规模 | 突增**通常先于** LOAD miss 出现，是"驱逐过快"的前兆信号 |

> `drampool_dump_nospace_failures_total` 是这组里设计质量最高的一个：它是**直测点**而非推导值，在 `metadata.cc` 的 `StoreBegin` 两次驱逐重试后、最终 `return Error` 前的 `st == Status::NoSpace()` 判定处计数，把"注册失败"排除在外。**读它时需要知道**：默认配置（`default_evict_ratio = 0.0`）下两次重试里的第二次（deep）释放 0 个条目，且每次 on-demand 驱逐只覆盖 1/1024 分片——详见 §3.1-③。

### 2.2 E 组：传输与响应

| 指标 | 类型 | 测什么 | 反映什么 | 怎么用 |
|---|---|---|---|---|
| `drampool_dump_transfer_duration_ms` | Hist **[批次]** | DUMP 传输从提交（`submit_ms`）到**终态**（Completed/Failed/GetStatus 异常）的异步耗时 | 实际搬运能力 | 与 prepare 相加 ≈ DUMP 服务端处理主体 |
| `drampool_load_transfer_duration_ms` | Hist **[批次]** | 同上，LOAD 侧 | 同上 | 同上 |
| `drampool_transfer_failures_total` | Counter | 数据传输以**非 Completed 终态**结束的请求数（DUMP/LOAD 合并） | 传输失败强度 | 增长需排查对端网络/连接状态 |
| `drampool_response_rtt_ms` | Hist **[批次]** | 响应从本地提交到**写回客户端内存完成**的端到端耗时（含响应传输本身） | 客户端感知的"结果返回"时延 | P99 高 = 对端写入慢 |
| `drampool_response_failures_total` | Counter | 响应返回链路失败（本地 Allocate 非 NoSpace 失败 / Pack 失败 / ExecuteAsync 失败 + 写回传输失败，**四点合并**） | 结果返回通道健康度 | 增长锁定响应链路故障域。**归因能力有限**，见 §4.1 |
| `drampool_submit_failures_total` | Counter | 数据传输**提交失败**的请求数（ExecuteAsync 失败或 handle 无效） | 传输子系统提交路径健康度 | 增长指向传输子系统初始化/资源异常，客户端整批失败 |
| `drampool_get_status_duration_ms` | Hist | `GetStatus()` 执行时长（Poller 查询**数据或响应**传输时都会计） | 终态轮询本身的开销 | 用于从 `*_transfer_duration_ms` 里剥离"轮询/终态查询"成分 |
| `drampool_response_submit_duration_ms` | Hist | 响应写回 `ExecuteAsync` 提交时长 | 响应提交的本地开销 | 纯本地成本，用于区分"提交慢"与"传输慢" |

**本条最重要的一处口径说明**（`drampool_metrics.h:65-68`）：`*_transfer_duration_ms` / `response_rtt_ms` 从提交起算到终态观测，**混合了** HiXL 实际搬运、Poller 轮询延迟、终态 `GetStatus()` 执行三部分。这是设计时就承认的口径妥协；`get_status_duration_ms` 与 `*_submit_duration_ms` 是后来补上的、可客户端测量的成分拆分。

### 2.3 F 组：资源水位

| 指标 | 类型 | 测什么 | 反映什么 | 怎么用 |
|---|---|---|---|---|
| `drampool_metadata_entry_count` | Gauge **[覆盖]** | 当前池内缓存条目（block）总数（1024 分片求和） | 元数据规模容量水位 | 增长斜率反映写入/驱逐平衡 |
| `drampool_buffer_pool_usage_ratio_<slot_size>` | Gauge **[覆盖]**（动态注册，每 block size 一个） | 各 block 尺寸数据池**已用槽位占比**（used / slot count） | 分尺寸内存水位 | **容量规划第一信号**。逼近 1 = 该尺寸池即将触发驱逐重试（DUMP prepare 抖动与 `nospace_failures` 的前兆） |
| `drampool_flag_pool_usage_ratio` | Gauge **[覆盖]** | flag 响应缓冲池已用槽位占比 | 响应回填缓冲水位 | 逼近 1 = B2 链前兆；为"flag 池扩容"决策提供交叉验证 |

**`slot_size` 的语义（这是最容易被误读的地方）**：
- label 值是**原始字节数**，不是人类可读字符串。面板图例直接显示 `262144 / 524288 / 7340032 / 14680064`，会话里开发者本人的第一反应是"行名好奇怪呀"。
- 来源是 CLI `--kvcache-block-sizes`（**必填、无 YAML 入口、无代码默认值**），因此**面板上出现新行一定意味着 daemon 用新配置重启过**。
- 触发点是**某个已配置 size 的首次成功分配**（不是"首次请求"）——未配置的 size 在 `BufferManager::Allocate` 就返回 `NotFound`，根本不会建池，所以 label 域仍被启动配置封闭。
- **reveal 是粘性（sticky）的**：某 size 首次被用后永久存在于 exposition，即使随后排空也会继续报 0。

### 2.4 G 组：Metadata 结算耗时（µs 精度）

| 指标 | 类型 | 测什么 | 反映什么 | 怎么用 |
|---|---|---|---|---|
| `drampool_metadata_storeend_duration_ms` | Hist **[entry]** | 每次 `StoreEnd`（DUMP 传输 Completed 后 entry INITIALIZED→READY）耗时 | DUMP 终态结算的单 entry 成本 | 高 = 分片读锁竞争（GC 每秒全分片扫描持读锁） |
| `drampool_metadata_loadend_duration_ms` | Hist **[entry]** | 每次 `LoadEnd`（TryDecRef 减引用）耗时 | 引用释放成本（单次应近常数） | 异常升高 = 锁竞争 |
| `drampool_metadata_evict_gc_duration_ms` | Hist **[轮]** | 后台 GC 每轮全分片驱逐扫描总耗时（1024 shard 之和，每 `gcIntervalMs` 默认 1s 一轮） | GC 对系统的持续开销 | **占空比 = 均值 ÷ gcIntervalMs**；高 = 频繁持锁干扰业务，是 storeend/loadend 变慢的常见外因 |

G 组用 `ScopedTimer` RAII 观测，零逻辑侵入；观测点位于 `metadata.cc` / `drampool_server.cc` 内部（模块边界测量）。

### 2.5 H 组：队列与阻塞链（本套设计最有价值的诊断资产）

线程衔接模型：

```
RequestReceiver ─TryPush─▶ requestQueue ─TryPop─▶ TaskWorker ─Push─▶ completionQueue ─TryPop─▶ CompletionPoller
      ▲ B1：满→sleep 重试（TCP 收包停滞）              ▲ B3：满→Push 自旋停摆                        │
                                                                                              ▼
                                                        pending_ 窗口（completion_inflight）
                                                         ─B2：flag 池 NoSpace 留 pending 下轮重试 ─▶ 响应写回
```

| 链 | 阻塞位置 | 形态 | 观测信号 |
|---|---|---|---|
| **B1** | Receiver：requestQueue 满 | 主动 sleep 阻塞（`requestReceiverIdleWaitUs` 默认 100µs）→ TCP 收包停滞 | `queue_request_full_total` ↑ + `queue_request_enqueue_wait_ms` 右移 |
| **B3** | TaskWorker：completionQueue 满 | Push **自旋停摆** → requestQueue 堆积反压 B1 | `queue_completion_full_total` ↑ → `queue_request_full_total` 连锁 ↑；前置信号是 `completion_size` / `request_size` 持续增长 |
| **B2** | Poller：flag 池 NoSpace | 单批次留 pending 下轮重试（**不阻塞 Poller 线程**） | `queue_response_buffer_retry_total` ↑ + `completion_inflight` 贴近 `pollerPendingDepth` + `flag_pool_usage_ratio` 逼近 1 |

指标明细：

| 指标 | 类型 | 测什么 |
|---|---|---|
| `drampool_queue_request_full_total` | Counter | requestQueue 满导致请求经历入队退避的次数 |
| `drampool_queue_request_enqueue_wait_ms` | Hist **[批次]** | 每请求从准备入队到 TryPush 成功的**入队前等待**（含满重试 sleep，未排队 ≈ 0） |
| `drampool_queue_request_residence_ms` | Hist **[批次]** | 每请求在 requestQueue 中的**驻留时长**（成功入队 → TaskWorker 出队），**不含**入队等待 |
| `drampool_queue_request_size` | Gauge **[覆盖]** | requestQueue **当前排队请求数** |
| `drampool_queue_request_capacity` | Gauge | 配置的 requestQueue 深度（`requestQueueDepth`，默认 65536） |
| `drampool_queue_completion_full_total` | Counter | completionQueue 满、`SubmitCompletion` 被迫自旋等待的记录数 |
| `drampool_queue_completion_inflight` | Gauge **[覆盖]** | Poller pending 窗口内在途完成记录数 |
| `drampool_queue_completion_size` | Gauge **[覆盖]** | completionQueue **当前排队完成记录数** |
| `drampool_queue_completion_capacity` | Gauge | 配置的 completionQueue 深度（默认 65536） |
| `drampool_queue_response_buffer_retry_total` | Counter | flag 缓冲池 NoSpace、SubmitResponse 留 pending 重试的事件数（**B2 链唯一信号**） |

**B3 链为什么必须存在**：TaskWorker 在 completionQueue 满时是 `Push` 自旋，**既不取新请求也不响应停止指令**，且**没有任何日志兜底**——`queue_completion_full_total` 是这条停摆的唯一观测手段。这是 H 组设计时最强的一条论据。

**归因速查**（设计文档 §4.6）：
- `request_size` 持续增长而 `completion_full` 平 → **B1**（TaskWorker 消费不足）
- `completion_full` ↑ 且 `request_full` 随后连锁 ↑（`completion_size` 先增）→ **B3**（Poller 消费不足，反压传导）
- `retry` ↑ + `inflight` 贴满 + `flag_pool_usage_ratio` 逼近 1 → **B2**（flag 池需扩容；flag 池是独立 region，与数据池无关）
- 全为 0 但吞吐低 → 转向阶段归因（§2.7）

### 2.6 I 组：批次端到端耗时（用户明确要求的口径）

**当前口径**：请求从 **requestQueue push** 到 **响应传输终态**（或记录因永久响应失败离开 Poller）的完整服务端生命周期。

**包含**：队列驻留、TaskWorker 处理、传输终态等待、响应缓冲等待（NoSpace 重试期间计入）、响应打包/提交、响应传输完成。
**不含**：入队前的 TryPush 等待（H 组 `enqueue_wait_ms` 覆盖）。

| 指标 | 类型 | 怎么用 |
|---|---|---|
| `drampool_dump_batch_total_duration_ms` | Hist **[批次]** | 与 `dump_prepare_duration_ms` 的差值 = 传输等待 + Poller 结算 + flag 等待 |
| `drampool_load_batch_total_duration_ms` | Hist **[批次]** | 同上 |
| `drampool_lookup_batch_total_duration_ms` | Hist **[批次]** | LOOKUP 无数据传输，差值 = 轮询调度延迟 + flag 等待（伴随 `response_buffer_retry` 增长） |

**为什么必须直测**：分位数**不可**由 prepare + transfer + rtt 三段速率均值推导（仅稳态近似）。

**实现机制（跨线程 + 一次性上报）**：
- 起点 `enqueue_us` 由 RequestReceiver 在入队瞬间打戳（`drampool_server.cc:463`），经 `RequestTask` → `CompletionRecord` 传递到 Poller。
- 终点在**终态路径**，而非 `SubmitResponse()` 入口。
- `SubmitResponse()` 会因 flag 池 NoSpace **同阶段重入**，因此需要哨兵保证只上报一次。设计文档原方案是把 `begin_us` 复用为哨兵（上报后置 0）；**当前实现改成了两个独立机制**：`batch_outcome_reported`（bool，管 `ReportBatchMetrics`）与 `enqueue_us`（置 0，管 `ReportBatchEndToEnd`），见 `completion_poller.cc:41-93`。
- 两个上报入口共 5 处调用，都在终态：`SettleResponseTransfer`（成功/失败）、flag alloc 永久失败、Pack 失败、ExecuteAsync 失败。

> **口径在实现中被放宽了一次**：设计文档写的是"TaskWorker **出队** → 响应**提交完成**"，当前是"requestQueue **push** → 响应传输**终态**"。放宽的理由是让 S1/S2 区段（送包到达→入队、完成记录排队）有归属，代价是 I 组与 H 组的 `request_residence_ms` 出现重叠区间——读图时需要意识到这一点。

**边界**：接入层拒绝（参数非法 / opcode 非法 / 响应超 flag slot 上限）不产生 `CompletionRecord`，不进入批次统计（该路径有 `UC_ERROR` 日志兜底）。`batch_size` 为 0 的请求仍观测批次总耗时。

### 2.7 阶段归因树与症状对照

```
DUMP 请求（I 组：dump_batch_total_duration_ms，批次端到端）
├─ dump_prepare_duration_ms [批次]（本地准备总视角）
│   ├─ dump_metadata_duration_ms（StoreBegin + 分配 + 驱逐重试）
│   └─ dump_submit_duration_ms（ExecuteAsync 提交）
├─ dump_transfer_duration_ms [批次]（异步搬运，poller 结算）
│   └─ Σ metadata_storeend_duration_ms [entry]（INITIALIZED→READY）
└─ flag 缓冲等待 + Poller 调度（信号：response_buffer_retry / get_status_duration_ms）
LOAD 请求
├─ load_prepare_duration_ms → metadata / submit 同上
└─ load_transfer_duration_ms → Σ metadata_loadend_duration_ms [entry]
LOOKUP 请求（无数据传输）
├─ lookup_scan_duration_ms [批次]（主体耗时；单 key 成本 ≈ scan 均值 ÷ 平均 batch_size）
└─ lookup_batch_total_duration_ms [批次]（端到端）
后台 GC（每 gcIntervalMs 一轮）
└─ metadata_evict_gc_duration_ms [轮]
```

| 症状组合 | 结论 | 动作方向 |
|---|---|---|
| `nospace_failures` 增长 + `usage_ratio` 逼近 1 | 内存压力触发驱逐重试 | 扩容 / 调驱逐比例 / 排查突发写入 |
| `prepare` P99 高而 `transfer` / 批次总耗时正常 | 服务端 CPU 侧开销（metadata 操作 / 缓冲分配） | 结合 `nospace_failures` 与 GC 占空比定位 |
| `storeend` / `loadend` 抬高，与 `evict_gc` 同步 | 后台驱逐读锁与业务锁互扰 | 调大 `gcIntervalMs` 或降驱逐比例 |
| 批次总耗时 P99 高而 `prepare`/`scan` 正常 | 传输终态等待 / flag 缓冲等待 / Poller 调度延迟 | 查 `transfer_duration_ms` P99、`response_buffer_retry`、`get_status_duration_ms` |

### 2.8 PromQL 速查

```promql
# 注意：导出名保留冒号，必须用 __name__ 选择器
sum({__name__="ucm:drampool_dump_requests_total"})

# 累计 counter 用 sum()，瞬时 gauge 用 max()
max({__name__="ucm:drampool_metadata_entry_count"})
max by (slot_size) ({__name__="ucm:drampool_buffer_pool_usage_ratio"})

# GC 线程占空比
rate(ucm:drampool_metadata_evict_gc_duration_ms_sum[5m])
  / rate(ucm:drampool_metadata_evict_gc_duration_ms_count[5m]) / (gcIntervalMs 秒数)

# 批次耗时分解（差值 = 传输终态等待 + Poller 结算 + flag 等待）
rate(ucm:drampool_dump_batch_total_duration_ms_sum[5m])
  / rate(ucm:drampool_dump_batch_total_duration_ms_count[5m])
  - rate(ucm:drampool_dump_prepare_duration_ms_sum[5m])
  / rate(ucm:drampool_dump_prepare_duration_ms_count[5m])

# 队列当前排队数（直测 gauge）
ucm:drampool_queue_request_size
ucm:drampool_queue_completion_size
```

> **命中率目前无法用 PromQL 表达**。LOAD 精确式为 `1 - miss/(miss + 成功加引 entry)`，LOOKUP 为 `hit/(hit+miss)`——两侧的分子/分母指标都已在第三/五轮修改中删除（§1.4），单 key 查询成本的分母同样缺失。

---

## 3. 遇到的问题及优化

### 3.1 语义类（指标口径错误）

**① `queue_request_full_total` 的实际口径 ≠ 文档口径**

文档（设计文档 §3.2 H 组 / YAML `documentation`）写的是"每次 TryPush 失败 +1 按次计"，并据此推导"接收阻塞时长 ≈ full 数 × 100µs"。实际代码是：

```cpp
bool queueFullLogged = false;
while (!requestReceiverStop_.load(std::memory_order_acquire)) {
    task->enqueue_us = SteadyNowUs();
    if (requestQueue_.TryPush(std::move(task))) { ...; break; }
    if (!queueFullLogged) {
        UC::Metrics::UpdateStats(NAME_TO_METRIC_ID(kQueueRequestFullTotal), 1);
        UC_WARN(...); queueFullLogged = true;
    }
    std::this_thread::sleep_for(idleWait);
}
```
（`drampool_server.cc:457-476`）

`queueFullLogged` 使该 counter **每个请求最多 +1**——它计的是"经历过背压的请求数"，不是"满事件数"。因此文档里的阻塞时长推导式**不成立**（阻塞可以任意长，只计一次）。`queue_completion_full_total` 同理是"每个遭遇满队列的记录 +1"（`task_worker.cc:404-406`），它的文档措辞（"full events that forced SubmitCompletion to spin-wait"）倒是准确的。

**但自旋等待并非"没有观测"，而是"被吞进别的直方图且无法归因"**（这一条经独立核验修正过，是比"没观测"更值得记的结论）：
- DUMP/LOAD 的成功路径上，`prepare` 的 `ScopedTimer` 直到 `return SubmitCompletion(...)` 执行完才析构（`task_worker.cc:129` 构造、`:213` 武装、`:226` 返回），所以自旋等待被算进 `dump/load_prepare_duration_ms`——`task_worker.cc:152-154` 的注释明说 prepare 覆盖 "both plus the completion handoff"。
- 所有 opcode、所有路径上，自旋都在 `*_batch_total_duration_ms` 的区间内（从 `enqueue_us` 到 Poller 终态观测）。
- 结论：**completionQueue 自旋没有专属指标，它静默地抬高 prepare 与批次总耗时，且无法与传输延迟或 metadata 延迟区分开**——要单独看，得靠 `queue_completion_full_total` 的计数与 `completion_size` 的走向间接推断。

**② `--kvcache-block-sizes` 与真实 KV 布局无任何关联校验**（会话 `7c33b968`）

`BufferManager::GetPool(size)` 是精确匹配（`pools_.find(size)`），配错就 `no pool registered for size X`，DUMP 直接超时（`-50010`）。

实测踩坑过程：首次用 `--kvcache-block-sizes 4096 8192` → 全部超时；从 vLLM 日志读到 `block_size: 14680064`（28 层 × 524288）→ 加进列表；最后发现**真实 per-entry 长度是 262144**（`tensor_size_list=[262144,262144]`，K/V 两半），而 4 个候选尺寸共享 2GB 池导致 262144 那档槽位被饿死：`buffer_pool_262144: no free slots`，`dump_failed_entries_total = 1554`，`metadata_entry_count = 1638` 恰好等于 `2GB ÷ 5 ÷ 262144`（池正好被填满）。改为以 262144 为主尺寸、池扩到 8GB 后归零。

**③ `PosEvictionPolicy` 在 `evict_ratio == 0` 时直接返回空**

```cpp
std::vector<EntryPtr> GetEvictionResults(double evict_ratio) override
{
    std::vector<EntryPtr> victims;
    if (evict_ratio == 0.0 || entries_.empty()) { return victims; }
```
（`pos_eviction_policy.h:55-58`）

而 `metadataDefaultEvictRatio` 默认为 **0.0**（`drampool_config.h:93`，`examples/drampool.yaml:55` 与 en/zh 文档也都写 0.0；该 key 是必填项，启动后无重载）。

**但"默认配置下驱逐重试是空操作"这个结论要收窄**（此处经独立核验后修正，原表述过宽）：

- `StoreBegin` 的两次驱逐重试里，**第一次**用 periodic 策略，默认是 **TTL**（`drampool_config.h:90`），而 `TtlEvictionPolicy` 走的是生命周期超时、**不读 `evict_ratio`**——所以它不是空操作，是真在干活。
- **只有第二次（deep，默认 POSITION）**因为 `evict_ratio == 0.0` 而释放 0 个条目。也就是说"两次驱逐重试后仍 NoSpace"的文档措辞与实现（2× `EvictOneShard` + 3× `Allocate`）是**对得上**的；被高估的只是"第二次 deep 重试确实释放了空间"这一层含义。
- 另有两处让默认配置下的驱逐更弱、且**文档未提**的机制：① on-demand 驱逐每次只用 `rand() % 1024` 挑**一个 shard**（`metadata.cc:188`，`kShardCnt=1024` 见 `metadata.h:197`；`metadata.cc:187` 自带 TODO），只有 GC 线程的 `PerformEvict` 才扫全部分片；② 候选集还要求 `TryMarkEvicting` 通过（READY 且 `refCnt==0` 且 lease 已过期，`entry.h:87-95`），而默认 TTL 是 **120 分钟**（`drampool_config.h:41-42`），所以内存压力下 TTL 路径通常无可驱逐对象。
- 因此准确的表述是：**`metadata.default_evict_ratio = 0.0`（默认）时，第二次 deep 驱逐重试释放 0 个条目；同时每次 on-demand 驱逐只覆盖 1/1024 的分片。**这是配置默认值问题，不是代码缺陷——`default_evict_ratio` 是可调字段。

### 3.2 并发与单写者类

**④ `queue_completion_size` 有两个写线程**

该 gauge 被两个线程写：TaskWorker（push 之后，`task_worker.cc:409`）与 CompletionPoller（pop 之后，`completion_poller.cc:171`）。而框架的 gauge 合并逻辑是按 `buffers_` 链表顺序**后者覆盖前者**：

```cpp
for (const auto& [id, value] : read_buf.gaugeStats_) {
    auto name = MetricName(id);
    if (name) { totalGauge[*name] = value; }
}
```
（`ucm/shared/metrics/cc/domain/metrics.cc:244-247`）

链表顺序由**线程首次 `UpdateStats` 的时机**决定。因此并发负载下导出的队列深度是"push 后的值"还是"pop 后的值"**不确定，且不是最新的**。这恰好违反了 `drampool_metrics.h:37-42` 自己写下的单写者约束。（对照：`queue_request_size` 的两个写点都在 TaskWorker 线程内，是安全的。）

**⑤ `gc.enabled: false` 时资源 gauge 失效——但两种 gauge 的失效形态不同**

`drampool_metadata_entry_count` 与 per-size 的 `drampool_buffer_pool_usage_ratio_<size>` 的唯一刷新点都是 `GCThreadLoop`（`drampool_server.cc:496-506`）。GC 关掉后：

- **`metadata_entry_count` 被渲染成假 0**：它在静态 defs 表内，`Render` 用 `FindValue` 兜底写 `0.0`（`metrics_reporter.cc:47-51`、`:176`）→ 面板显示"池子是空的"，实际可能是满的。**这不是缺失，是主动误导。**
- **per-size gauge 是整条序列不出现**（而非画成 0）：`Render` 对 `gaugeValues_` 里不存在的名字直接 `continue`（`metrics_reporter.cc:185-186`）。

有意思的是，队列 capacity gauge 正是为了规避这类问题才被特意搬进常驻的 TaskWorker 循环（`task_worker.cc:49-51` 的注释写明了理由），但同样的推理没有应用到**静态**资源 gauge 上——2026-09-30 的联调里实际观测到了它：writer JSON 报 `metadata_entry_count = 0` 而 `/metrics` 报 264、`buffer_pool_usage_ratio` 报 0 而 `/metrics` 报 0.1014。

**⑥ 批次哨兵机制在实现中的演进**

设计用 `begin_us` 单一字段兼作"批次起点"与"已上报哨兵"（置 0 表示已报）。实现中做了两处调整：

1. **含义与命名**：起点从"TaskWorker 出队时刻"改为"**requestQueue push 时刻**"，字段随之从 `beginUs` 改名为 `enqueueUs`（`3fc31e21`），由 `RequestReceiver` 打戳（`drampool_server.cc:463`）后作为**第三个函数参数**贯穿 `ProcessDump/ProcessLoad/ProcessLookup/QueueResponse`。
2. **哨兵拆成两个**：`enqueue_us`（置 0 哨兵，管批次总耗时）与新增的 `batch_outcome_reported`（bool，管批次结果计数）。拆开的理由是两个上报点的频次与所在路径不同——批次结果在 `SubmitResponse()` 入口报一次，批次耗时在**终态路径**报一次。

> `enqueue_us == 0` 的语义被正式定义为"本记录不做批次端到端统计"（`completion_poller.cc:78`），这同时成了测试里的安全占位值——单元测试传 `constexpr kEnqueueUs = 0` 即可完全避开批次统计，无需真实时钟读取，也不引入 flakiness。

### 3.3 配置链路类

**⑦ `metrics.enabled = false` 时统计仍在跑**

`drampool_daemon.cc` 原本无条件调用 `SetupDrampoolMetrics()`。由于 `SetUp()` 会把 `isInited_` 置 true 并注册全部 MetricId，而 `UpdateStats` 的**唯一运行时守卫**就是 `isInited_`，结果是：**关闭指标 ≠ 停止统计**。全链路累积的 stats 无人 drain（`MetricsReporter` 是唯一的 `GetAllStatsAndClear()` 调用者）。

用户要求是"不仅不上报，甚至不能开启统计，做到完全隔离"。修复（`900b5e62`，2026-10-14）：

```cpp
// Same gate as the Python side (setup_ucm_metrics() only calls set_up() when
// metrics are enabled) ...
if (g_config.metricsEnabled) { SetupDrampoolMetrics(); }
```

修复后 disabled ⇒ `isInited_` 保持 false ⇒ 每个 `UpdateStats` 在第一行原子 load 处返回，**线程缓冲根本不会创建**。

配套做了 disabled 路径的安全测试（用户明确要求"不让它setup会不会有访问空指针之类的问题"，`d3205631`）：用独立 harness 对着**真实 metrics core** 编译运行，覆盖 11 项检查。过程中发现 harness 自身一个 bug——`NAME_TO_METRIC_ID` 宏在**每个调用点生成独立的 `CachedMetric` 静态实例**，测试读了另一个实例所以 id 仍是 INVALID。

**残留开销被显式接受**：`ScopedTimer` 构造时的 `SteadyNowUs()`、内联时间算术（作为函数实参求值）、`g_requestQueueLen/g_completionQueueLen` 的 relaxed 原子操作——这些在 disabled 时**仍然执行**。理由：守卫在**被调函数的首行**，C++ 求值顺序决定实参与独立原子语句已经执行过；要给 ~40 个调用点全部加门控是"改动面和回归风险大一个量级"。

**⑧ 配置校验顺序错误导致 CI 失败（`818 - DramPoolConfigTest.RejectsInvalidCommandLines`）**

`NormalizeBlockClasses`（重复 size 合并）原本跑在 `ValidateBlockClassCount`（sizes/proportions 等长校验）**之前**，而合并逻辑按 index 配对 `poolBlockProportions[i]`。当 proportions 比 sizes 短时 → **越界读**；并且合并会把长度"补齐"，**吞掉了本应报出的长度不匹配错误**。修复（`0f75a8d7`，2026-10-13）：把校验提到合并之前，并加注释说明这个顺序约束。

**⑨ 重复 block size 的双重代价**

`--kvcache-block-sizes 512 512` 会同时造成：(a) 重复的 JSON key 与重复的 gauge 注册；(b) 更隐蔽的——`CalculatePoolSlotCounts` 按 index 配对 proportions，`BufferManager` 却只建一个池，**第二个 512 的容量配额被静默丢弃**，等于白配了容量。用户提出"动态注册的 kv buffer 要求唯一"后，修复落在**配置层单点**：`NormalizeBlockClasses()` 合并重复 size 并**把 proportions 相加**（保住总容量）。在三个消费循环里各自去重被认为"治标"。

**⑩ `examples/ucm_config_dramstore.yaml` 照抄看不到任何指标**

官方示例既没有 `enable_metrics` 也没有 `metrics_config_path`，且 `drampool_resource_log_path` 用相对路径 `./logs/drampool_metrics.json`（依赖进程 cwd，而 drampool 与 vLLM 是两个独立进程）。实测必须手工补两个键才拿到完整的 114 个 drampool 系列。**读不到文件时没有任何告警日志。**（截至 2026-10-16 仍未见修复，见 §4.3。）

### 3.4 管道与回流类

**⑪ reporter 永久卡死（`Histogram sum changed without samples`）**

触发条件：drampool 重启而 vLLM 不重启 → `/dev/shm` 里的陈旧 state 与新快照桶数不符。原实现把它当致命错误抛出，而 `_collect_once` 整体失败导致 `_write_state` 永不执行 → **state 永不前进 → 此后每次轮询都撞同一份陈旧 state，无限循环报错**。已实测复现：连续 3 个采集周期全部抛异常、持久化基线时间戳冻结在旧值。

修复走的是用户明确指示的方向——**"要向 Prometheus label 方案看齐，可能之前版本采用聚合方式，导致当前的问题"**：`snapshot_deltas` 从"抛 `ValueError("Histogram schema changed")`"改为**重基线化**（把当前完整分布作为新基线，`_write_state` 在同一周期内持久化新 schema），与 counter 的"下降视为 reset"语义对齐。新增回归测试 `test_histogram_schema_change_rebaselines_instead_of_stalling`。

审计中还发现并修复了一个**遗留的同类陷阱**：`drampool_metrics.json` 的 JSON blob 在仓库里是 CRLF 提交的，而脚本用 `json.dump` 写出 LF → 87 行整文件 diff + git newline warning（文本模式读取会自动 CRLF→LF，所以早期的 round-trip 校验看不见这个问题）。

**⑫ 跨层数值不一致（两个独立事件，均未给出最终解释）**

| 观测 | writer JSON | vLLM `/metrics` = Prometheus | 差异 |
|---|---|---|---|
| 2026-09-30（`7c33b968`）`dump_failed_entries_total` | 9576 | 9426 | 150 |
| 同上 `transfer_failures_total` | 504 | 479 | 25 |
| 同上 `metadata_entry_count` | 0 | 264 | Gauge 假 0（见 ⑤） |
| 同上 `buffer_pool_usage_ratio` | 0 | 0.1014 | 同上 |
| 2026-10-09（`e2e_retrospective_20261012.md`）`dump_requests` | 840 | 728 | **112（12.5%）** |
| 同上 `submit_failures` | 840 | 728 | 112 |
| 同上 `dump_failed_entries` | 15960 | 13832 | 2128 = 112×19 |
| 同上 `response_failures` | 855 | 742 | 113 |

两个 counter 的差值（112）在多个指标上**成比例复现**（112 / 112 / 112×19 / 113），说明不是抓取延迟，而是**回流环节系统性丢样本**。当时的判定是"丢失发生在转发环节（reader 采样/增量逻辑）"，并建议为 drampool 增加独立 scrape target 作兜底——**未修复**。

**⑬ 重启后语义未定论**

只重启 vLLM 时，`/metrics` 上 `dump/load/lookup_requests_total` 读 0，而 writer JSON 里 `dump_requests_total = 56`。当时假设"重启后 reporter 重新基线化（delta 计数归零）"，验证脚本已写好但被用户中断——**"drampool 的 counter 是跨重启累计还是重启相对"至今没有文档化**，而这直接决定基于 `rate()` 的告警行为。

### 3.5 Grafana / 面板类

**⑭ Prometheus 保留名字里的冒号**

vLLM 暴露 `ucm:drampool_dump_requests_total`，**Prometheus 不会把 `:` 规范化成 `_`**（与上一轮交接文档的说法相反）。PromQL 必须写：
```promql
{__name__="ucm:drampool_dump_requests_total"}
```
写 `ucm_drampool_...` 会返回空，**看起来像链路断了，实际只是选择器错了**。这个错误说法直接把排查引向了歧途，后来从总结文档中删除。

**⑮ Grafana 11 拒绝重复 refId**

联调脚本生成的 12 面板全部把每个 target 的 refId 写成 `"A"` → `HTTP 400 {"messageId":"query.duplicateRefId"}` → 除单 target 面板外全部报 "No data"（而 target 从未 down、样本连续）。修复：重排 refId（A/B/C/D）并让生成器自动分配。

**⑯ 面板指标名笔误**

panel 引用 `ucm:ucm_hit_blocks_total`，仓库与 Prometheus 中均**不存在**；真实名是 `ucm:ucm_hit_tokens_total`（tokens 非 blocks）→ 该序列恒空。

**⑰ 官方 `grafana_store.json` 不含任何 drampool 面板**

只覆盖 `ucm:cache_*` 与 `ucm:mooncake_*`。PR 1394 加了指标与 `metrics_configs.yaml` 定义，却**没有配套面板**，指标出不了图。这直接催生了仓库自带的 `examples/metrics/grafana_dram.json`（uid `ucm-dram-metrics`，共 16 个 panel，其中 **8 个**用到 drampool 序列：`KV Buffer Slot Usage`、`DramPool request throughput`、`DramPool end-to-end latency distribution`、`DramPool local stage latency - p95`、`Queue utilization`、`Memory pool utilization`、`KV Buffer Slot Usage by Block Size`、`DramPool error rate`；另 8 个是 DramStore 专属） |

**⑱ 幻影序列与化石序列**

- 图例里出现一行无名的 `Value`、恒 0%：这是 **slot_size label 为空**的序列——suffix 拆分约定引入**之前**的旧进程/导出端直接写基名 `drampool_buffer_pool_usage_ratio`。`split_dynamic_metric_name` 把它映射成空 label。当前 C++ 代码只写带后缀的名字（`drampool_server.cc:499`），所以这是环境里的化石。建议在查询里加 `slot_size != ""`（**未被采用**）。
- 从配置里删掉某个 size 后，导出进程里已创建的 label child **不会被回收**，旧行会停在最后一个值上，直到 exporter 进程重启（`observability.py:199-200` 只 `labels().set()`，从不删除）。

**⑲ 面板默认时间窗掩盖活动**

面板默认"最近 30 分钟"，而最后一次请求在 8 小时前 → 曲线停在 168 的平线，**读起来和"没数据"一样**。另外累计 counter 面板在空闲期本来就是阶梯状平线。这两点当时只用口头指导解决（切到 Last 12 hours、看 rate 面板），**未改面板默认值**。

### 3.6 工程与流程类

**⑳ 联调中的自我纠错（值得记录的三个）**

1. **"我一直在跑错的二进制"**：harness A/B 实验（CANN 9.1 vs 8.5.1、6 种设备配对、真实 IP vs loopback）全部跑在一个 **Jul-28 构建**的二进制上，而源码是 Sep-23 的，且该二进制**没有 `CLIENT_INIT` 标记**（成功那次的源码有）。→ **全部结论作废**。重建后才发现：握手与内存注册完全成功，**只有数据搬运失败**。
2. **归因撤回**："0/2/3 做不了传输"的依据中，"真实栈传输失败"是硬的（产品代码路径），"harness 在 6 种配对都失败"是软的——后者已被证伪。
3. **循环论证撤回**：Grafana `orientation: horizontal` 的争议中，助手把自己的语义假设编码进公式再把"匹配"当作独立验证，随后公开撤回。

**㉑ 计量陷阱：日志限流使"数日志行数"严重低估失败规模**

`UC_ERROR` 走限流路径（默认 60s 窗口、每调用点最多 3 条，计数位宽仅 2 bit 且**上限硬编码为 3，环境变量无法调高**）。实证：JSON 里 `submit_failures_total = 840`，而日志只有 15 条（5 批 × 3，恰为限流上限），失败 request_id 呈连续三元组（1,2,3 / 169,170,171 / …）即限流指纹。**差距约两个数量级。**

**㉒ 若干"空壳配置"**

`--nics` 只被 `ValidateNics()` 检查非空，**从未传给 HIXL**；`enable_cs`（YAML）同样从未进入 `HixlInitAttrs`。两者在排障时都消耗了无效功。2026-10-16 复核：`grep -rn 'nics' ucm/store/dram/cc/drampool/` 在非测试代码中**零命中**，确认已解析入 config 但无任何消费点。

**㉓ 指标改动打破了测试契约，导致 PR 门禁两处失败**（会话 `8ef10cad`，PR #3820）

用户报告"pr门禁有两处fail check"。归因到 `3fc31e21 "Refine DramPool metrics"`：

- **cpp_gtest**：该提交为 `ProcessDump/ProcessLoad/ProcessLookup` 增加了第三个参数 `enqueueUs`（含 `beginUs → enqueueUs` 改名），但测试夹具的包装函数没跟着改 → `task_worker_test.cc:127/133/139` 报 `candidate expects 3 arguments, 2 provided`。修复：加 `constexpr std::uint64_t kEnqueueUs = 0;` 并在三处传入（0 即哨兵值，测试无延迟断言，因此不引入 flakiness）。
- **cpp-linter**：18 个改动文件里 6 个存在换行/对齐/头文件顺序违规（Google 风格，`ColumnLimit 100`）。修复为 `clang-format 20.1.8` 纯格式化。

> 一处顺带发现但**未解决**：`task_worker_test.cc` 未被 `ucm/store/test` 下任何 `CMakeLists` 引用，**不确定哪个 CI config 在编译它**——也就是说那个 cpp_gtest 修复未必真被门禁执行到。

**㉔ 历史上"直方图没有桶"这个 bug 曾让热力图全空**

`e5741fab` 版本里 `CreateStats(k*BatchTotalDurationMs, "histogram")` **没有传 bucket 数组**，而框架的 `NormalizeBuckets` **只追加 `+Inf`、没有默认桶集**（`metrics.cc:31-38`）——结果是该直方图只暴露一行 `+Inf`，热力图无从绘制。`3fc31e21` 补上了显式桶数组。这条机制值得记住：**在 C++ 侧注册直方图必须显式给桶**，否则不会退化成一个粗粒度的直方图，而是**完全没有分辨率**。

### 3.7 优化清单

| # | 优化项 | 优化前 | 优化后 | 收益 |
|---|---|---|---|---|
| 1 | **slot-used 计数移出共享 BufferPool**（`0fde501b`） | 为加水位指标改了**共享组件** `ucm/shared/pool/buffer_pool.{h,cc}`（违反设计的"不修改共享组件"约束），所有 BufferPool 使用者都为此付费 | `BufferManager::usedBySize_` 原子计数，共享 BufferPool 零改动、零开销 | 恢复架构约束边界。附带一条被文档化的正确性约定："Callers must Free with the same size used at Allocate, or the counter for the mismatched size drifts." |
| 2 | **指标关闭时完全隔离统计**（`900b5e62`） | `SetUp()` 照跑 → 每个 `UpdateStats` 都真实累积、每线程分配 buffer、无人 drain | `isInited_` 保持 false → 全部 `UpdateStats` 在首行原子 load 处返回，线程缓冲不创建 | 关闭指标真的省 CPU/内存，且与 Python 侧 `setup_ucm_metrics` 的门控语义一致 |
| 3 | **惰性揭示（sticky reveal）替代全量预注册**（`7e515b8c`） | 每个配置 size 从进程启动就上报，未使用的池画成恒 0% 直线（实测 4 行里 3 行是噪声） | 首次成功分配才 reveal，此后永久保留；GC 循环按 `IsRevealed` 过滤，`Render` 跳过 `gaugeValues_` 中不存在的名字 | 序列数 = 实际用到的 size 数，图例稳定、不闪烁 |
| 4 | **桶集统一到 14 值**（`7ce12bca`） | transfer 15 值（含 10000）、lookup_batch 11 值（上限 1000ms）、DramStore 侧 15 值——每个直方图暴露各自的 `le` 序列集 | 统一为 `0.1…5000` 14 值 | 起因是用户报告的**面板现象**："两者的纵坐标轴不统一"——同一个"端到端延迟"热力图，DramStore 侧 Y 轴到 10s、DramPool 侧只到 1s，因为 `operation=lookup` 时 drampool 的 `lookup_batch_total` 用 11 值桶（上限 1000），2000/5000/10000 的 `le` 序列不存在。修复后两张热力图共享同一组有限桶行，跨面板 Y 轴可比。**已接受的代价**：transfer/rtt 类指标原先 5000–10000 有独立桶，现在 **>5000ms 全部落入 `+Inf`**（见 §4.1-4） |
| 5 | **为按值维度引入后缀编码 + 导出侧解码**（`112dd1bb` 等） | 要么报告期聚合成单一值（永久丢信息），要么改 ~12–14 文件的传输模型 | 线格式不变；`dynamic_labels` 单点声明驱动 4 道 name-keyed 关卡；共享 `split_dynamic_metric_name()` | 只改一个指标族的风险面；向后兼容由构造保证（空 `_dynamic_bases` ⇒ 原静态路径） |
| 6 | **重复 block size 在配置层归并**（`0f75a8d7` + 前置实现） | 重复 JSON key、重复 gauge 注册、静默丢弃重复类的容量配额 | `NormalizeBlockClasses()` 合并 size 并**相加 proportions**；校验顺序修正 | 一次修复同时消灭 gauge 唯一性问题与容量静默丢失 |
| 7 | **热路径指标 id 预解析** | 每次 `UpdateStats` 做字符串查表 | `NAME_TO_METRIC_ID()` 生成 per-call-site 静态 `CachedMetric`，id 只解析一次（与 DramStore 同模式） | 热路径零字符串查找 |
| 8 | **观测点选在低频线程** | 数据池水位若每请求上报会侵入 SPSC 管道 | 水位 gauge 挂 GC 线程（`gcIntervalMs` 默认 1s 一轮）；flag 池水位由 CompletionPoller 单线程记账（普通 `size_t`，无需 atomic） | 请求热路径零指标开销 |
| 9 | **Grafana JSON 结构化编辑纪律** | 文本编辑/`json.dump` 产生整文件 diff 与换行告警 | 编辑前后做**字节级** round-trip 校验 + 保留 CRLF；变量级联顺序显式约束 | dashboard diff 可评审（最终 50/37 行，全是语义变更） |
| 10 | **联调脚本加入 RoCE 链路预检** | 数据面故障只表现为难以解读的 `507001` / `503900`，且要等模型加载数分钟后才暴露 | `hccn_tool -i N -link -g` 全卡预检，全 DOWN 时直接报错并给出诊断命令与通过判据 | 本次联调最大的教训被固化成 fail-fast 预检 |

---

## 4. 现存问题

以下均以 2026-10-16 的仓库状态（分支 `z00984767-0917`，HEAD `afdcbda6`）核验过。

### 4.1 P0 — 会输出错误数字

| # | 问题 | 机制 | 影响 |
|---|---|---|---|
| 1 | **延迟直方图存在幸存者偏差：系统越差，采样越消失** | `prepareTimer.Arm()` 在**提交成功之后**才调用（`task_worker.cc:213`、`:316`），失败/短路分支不 `Arm()`；`ReportBatchEndToEnd` 只在**终态路径**调用（`completion_poller.cc:91-109`），卡在 `Waiting` 的 transfer 永远不离开 Poller | 内存压力下的 eviction 重试风暴——prepare 最慢的场景——**一个样本都进不了直方图**；卡住的请求也永远不计入批次耗时。**性能劣化时 p99 反而会变好看** |
| 2 | **`queue_completion_size` 双写者，导出值取决于线程注册顺序** | TaskWorker（`task_worker.cc:409`）与 CompletionPoller（`completion_poller.cc:171`）都写同一 gauge；合并逻辑 `totalGauge[name] = value` 按 `buffers_` 链表顺序后写覆盖先写（`metrics.cc:244-247`） | 并发下队列深度**不确定且非最新**，违反 `drampool_metrics.h:37-42` 自述的单写者约束 |
| 3 | **`gc.enabled: false` 时 `metadata_entry_count` 导出假 0** | 唯一刷新点在 `GCThreadLoop`（`drampool_server.cc:496-497`）；`Render` 对**静态** gauge 用 `FindValue`，未写过的名字返回 `0.0`（`metrics_reporter.cc:47-51`、`:172-177`） | 面板显示"池子是空的"，而实际可能装满了——**不是缺失，是主动误导**。队列 capacity gauge 正是为规避此类问题才被搬进常驻的 TaskWorker 循环（`task_worker.cc:49-51` 注释写明了理由），同样的推理没有应用到资源 gauge 上 |
| 4 | **`kMsBucketsSettlement` 桶上限 500ms，而它承载的 `queue_request_enqueue_wait_ms` 没有时间上界** | `drampool_metrics.h:159-160`（12 个边界、最大 500）；而入队重试循环只以"push 成功或 shutdown"为退出条件（`drampool_server.cc:458-476`），**没有时间上限**；每次失败退避 100µs，破 500ms 只需约 5000 次连续失败 | 超过 500 的值全部落进 `+Inf` 桶（`metrics.cc` 的 `NormalizeBuckets` 追加 `+Inf`，`lower_bound` 定位），而 `grafana_dram.json:642` 正是用 `histogram_quantile(0.95, ...)` 读它——**P95 会被钳在 500ms，无论真实等待多长**。判据应当是"最高 bucket 必须 ≥ 该路径的时间上界" |
| 5 | **`response_failures_total` 把四种成因合并成一个数** | `completion_poller.cc:126/292/312/335` + `PollResponseTransfer` 的 `:365`，即 flag 分配失败（非 NoSpace）、Pack 失败、提交失败、写回传输失败 | **定位能力等于零**；增长时无法区分是本地打包问题还是网络问题 |
| 6 | **`*_requests_total` 在 `dynamic_cast` 校验之前自增** | `task_worker.cc:104-116`；之后的失败只 `return Status` / 打日志 | 请求数与结果数**对不上账**（守恒式不成立），无法判断是业务丢了还是埋点漏了 |

> **注意 #1 只对 `*_prepare_duration_ms` 成立**（已逐行核验）：DUMP/LOAD 的 `*_metadata_duration_ms` 与 `*_submit_duration_ms` **没有** `Arm()` 门控，是**无条件**写入的——`dump_metadata_duration_ms` 在 `task_worker.cc:185-186` 于 transfer_items 空判断之前写出，`dump/load_submit_duration_ms` 在 `:199-200` / `:302-303` 于提交失败判断之前写出，**连失败的提交都被观测到**。所以这轮"把 prepare 拆细"的增补（§1.5）实际上部分补偿了幸存者偏差：整批视角的 `prepare` 有偏，但分段视角的 `metadata`/`submit` 没有。这是"粗粒度之外补一层细粒度"除了归因之外的第二个收益，值得明确写下来。
>
> 另注意：仍能进入 transfer 的**部分失败批次**（DUMP 的 StoreBegin 失败 `break`、LOAD 的 LoadBegin 失败/长度不符 `continue`）**会**继续走到提交并 `Arm()`，所以"失败分支不计数"的准确范围是**函数级提前返回**（DUMP 3 处 / LOAD 3 处：flag slot 超限、无传输短路、提交失败），而不是"任何含失败的批次"。

> **注意 #3 的两种失效形态是不同的，读面板时必须区分**（均以当前代码核验）：
> - **静态 gauge → 假 0**：`metadata_entry_count` 在 defs 表内，`Render` 用 `FindValue` 兜底写出 `0.0`（`metrics_reporter.cc:176`）。GC 关闭时它永远是 0，与真实值无关。
> - **动态 per-size gauge → 序列静默缺失**：`Render` 对 `gaugeValues_` 里不存在的名字**直接 `continue`**（`metrics_reporter.cc:185-186`），所以未揭示/未更新的 size **整条序列不出现**，不会画成 0。
>
> 也就是说，2026-10-13 的健壮性重设计（`7e515b8c`）只修好了动态那一族；**静态资源 gauge 的假 0 问题仍然存在**，且危害更大——曲线存在但值是错的，比曲线缺席更难发现。
>
> 另需留意：`flag_pool_usage_ratio` 与 `queue_completion_inflight` 的写入点在 `PollPendingCompletions` 末尾，只在 `pending_` 非空时执行，因此**完全空闲的 daemon 上它们也走 `FindValue` 兜底**（恰好等于真实值 0，不构成误导，但语义上属于"未测量"而非"测得为 0"）。
>
> **#4 的桶集不对称值得单点指出**：同一个背压现象的两侧用了差 10 倍的桶集——**吸收无界重试的** `queue_request_enqueue_wait_ms` 用 500ms 上限（`drampool_metrics.h:227-228`，`kMsBucketsSettlement`），而队列**另一侧**的 `queue_request_residence_ms` 用 5000ms 上限（`:229`，`kMsBucketsPrepare`）。要修的话，前者才该用宽桶集。

### 4.2 P1 — 业务盲区（"DramPool 本质是个 cache"）

| # | 缺口 | 说明 |
|---|---|---|
| 7 | **命中率算不出来** | 只有 miss 计数（`load_miss_entries_total` / `lookup_miss_entries_total`），**没有 entry 级总数**——`*_requests_total` 计的是 batch，而 `batch_size` 每请求可变且无处记录。对一个 cache pool 来说，缺 hit/evict/occupancy 这组核心业务指标比缺任何延迟指标都严重 |
| 8 | **完全没有 eviction 指标** | on-demand 的 `EvictOneShard`（`metadata.cc:186-194`）被聚合进 `dump_metadata_duration_ms`，既无 `evicted_entries_total` 也无单独时长；`metadata_evict_gc_duration_ms` 只覆盖后台 sweep |
| 9 | **LOAD 的 entry 级结算失败无处计数** | `ReportBatchMetrics`（`completion_poller.cc:41-67`）只有 DUMP 与 LOOKUP 分支，LOAD 落到 `default: break`；而 `load_miss_entries_total` 只统计传输**前**的 miss。所以 RDMA 写回失败的 LOAD entry 在 entry 粒度上**完全静默**——DUMP 有 `dump_failed_entries_total`，LOAD 没有对应物 |
| 10 | **transfer/response 超时只打日志，不计数** | `completion_poller.cc:251-261` 与 `:370-378` 设了 `timeout_reported` 就 `UC_ERROR` 了事。卡住的记录会长期占用一个 pending slot **和一个 flag buffer**，唯一线索是 `queue_completion_inflight` 缓慢爬升 |
| 11 | **没有心跳/自监控指标，无法区分"系统空闲"与"某条流水线线程停止推进"** | 见下方说明（其中"接收线程会被 TCP 异常悄悄杀掉"这一常见担忧**不成立**） |
| 12 | **重复写入被当作"成功"混过去** | `StoreBegin` 返回 `DuplicateKey` 时直接置 `Ok` 并 `continue`（`task_worker.cc:166-169`），没有 `dump_dedup_entries_total` 之类的写侧命中计数 |
| 13 | **未注册 size 的分配失败完全静默**（指标层面） | `BufferManager::Allocate` 对未注册 size 返回 `NotFound`（`buffer_manager.h:119-127`），既不进 `nospace_failures` 也无独立计数。面板上**没有信号**（注册与渲染都只遍历已配置 size），只能靠 `dump_failed_entries_total` 与 `"no pool registered for size"` 日志 |

> **关于 #11 的说明（含一处需要纠正的流传说法）**
>
> 流传的说法是"`RequestReceiveLoop` 遇到 TCP 异常会永久 break，于是进程还活着、`/health` 照样 200、快照照样写、所有指标变平"。**这个机制不成立**——该失败分支实际**不可达**：
>
> ```cpp
> Status ChannelManager::Receive(ManagerID& peer, Metadata& payload)
> {
>     std::unique_lock<std::mutex> lock(mutex_);
>     received_ready_.wait(lock, [this] { return stopping_ || !received_.empty(); });
>     if (received_.empty()) { return Status::Error(); }   // 仅当 stopping_ 置位时可达
>     ...
> }
> ```
> （`ucm/transport/p2p/src/core/channel_manager.cpp:295-305`）
>
> `Receive` 在条件变量上等待，只有 `stopping_` 置位且队列为空时才返回 `Error()`。真正的 TCP 连接错误由**另一个线程** `ChannelManager::ReceiveLoop` 经 `InvalidateSocket()` 处理（同文件 `:307-331`）；对端断连只意味着"不再有消息到来"，接收线程继续等待，**不会退出**。
>
> **但底层缺口依然存在，且与那个具体触发路径无关**：指标集里没有任何心跳，所以任一条流水线线程（TaskWorker、CompletionPoller、GC、MetricsReporter、ChannelManager::ReceiveLoop）若真的停止推进，指标面上只表现为"所有曲线变平"，**没有独立信号可与"系统空闲"区分**。`metrics.enabled = false` 时更彻底——连"指标管道自己是否还在写快照"都无从判断。这正是 §4.4 提案里 L0 自监控存在的理由（该提案给出的触发场景是错的，但要补的指标是对的）。

### 4.3 P2 — 管道、交付与运维

| # | 问题 | 状态 |
|---|---|---|
| 14 | **回流环节系统性丢样本**（840 → 728，12.5%，多指标成比例） | **未修复**。Prometheus 只有 1 个 target（vLLM `:7850`），drampool 无直采目标，丢失无兜底。**做容量/失败率结论时必须以 writer JSON 为准** |
| 15 | **重启后 counter 语义未文档化** | **未定论**。只重启 vLLM 时 `/metrics` 读 0 而 writer JSON 保持累计真值（56），意味着基于 `rate()` 的告警在重启后会静默一段时间 |
| 16 | **`examples/ucm_config_dramstore.yaml` 不含 `enable_metrics` / `metrics_config_path`**，`drampool_resource_log_path` 仍是相对路径 | **未修复**（2026-10-16 核验）。照抄示例看不到任何指标，且读不到文件时**无任何告警日志** |
| 17 | **用户文档只覆盖 10 / 41 个指标（≈24%，约四分之一）** | `docs/docs-site/docs/{zh,en}/developer-guide/cache-configuration/drampool.md` 的 DramPool 指标表各列 10 行：`buffer_pool_usage_ratio`、`metadata_entry_count`、`dump_nospace_failures_total`、`lookup_miss_entries_total`、`transfer_failures_total`、`response_failures_total`、`queue_request_size`、以及三组 `*_batch_total_duration_ms`。**队列与阻塞链（H 组 10 个指标）一个都没有**，而它们正是排障时最有用的那一组 |
| 18 | **`examples/drampool.yaml` 无 INFO 级别的 block size 打印** | 只有 `DEBUG` 级别的 pool_count 日志，导致"谁在什么时候配了这些 size"无法从日志回答，只能读 `/proc/<pid>/cmdline` 并对照进程启动时间 |
| 19 | **`grafana_dram.json` 零测试覆盖** | `GRAFANA_UCM_DASHBOARDS` 白名单只含 `grafana_vllm.json` / `grafana_store.json`。所有面板改动仅靠临时 JSON round-trip 脚本把关，未来查询/面板破损 CI 看不见 |
| 20 | **任何 drampool 指标都没有阈值、告警或 recording rule** | 失败检测靠肉眼看面板 + 脚本 WARN 行。`transfer_failures_total > 0` 与 `dump_failed_entries_total > 0` 曾在真实环境里被当作"已知阻塞"容忍数小时而无自动信号。另：Grafana 用的 `ucm:` recording rule **不在仓库里**，无法核验其是否正确透传 `slot_size` |
| 21 | **动态注册有顺序约束但只有注释保证** | `SetupDrampoolMetrics()` 必须在配置解析**之后**调用（它遍历 `g_config.poolBlockSizes`），仅由注释约束；未来若有人调整初始化顺序，会静默注册 0 个 per-size gauge |
| 22 | **block class 数量无硬上限** | `ValidateBlockClassCount` 存在但未发现 max-class 常量；per-size gauge 基数与面板曲线数随配置无界增长 |
| 23 | **`--kvcache-block-proportions` 默认 1:1 按字节平分，不是按槽位** | 256KiB 档会比 14MiB 档多 56 倍槽位；大块为主的流量可能打爆大块档而小块档闲置。已文档化为配置坑，未加校验 |
| 24 | **`--nics` / `enable_cs` 是空壳配置** | 两者都从未传到传输层；`--nics` 只校验非空。填错不会在启动时报错 |
| 25 | **`ucmstore.test` 在 Windows 开发机上无法本地编译/运行** | fmt/gtest/spdlog 是构建期 FetchContent，目标平台是 Linux/Ascend。所有 C++ 改动（含 reveal 测试、disabled 门控、启动配置修复）**都是靠 CI 验证的**；本地只有 clang-format、shim harness 与人工 diff 审查 |
| 26 | **无 ASan/TSan 覆盖** | MinGW 工具链缺 sanitizer，disabled 路径的并发断言只靠代码审读 + 8 线程 × 20000 压测间接覆盖 |
| 27 | **分支 CI 仍红** | `UCLoggerPerfTest.MultiThreadPerfUcInfoVsRateLimit` flaky（属共享层 logger 测试，不是 drampool 代码）；`test-build-sglang` 因 25 分钟 job timeout + `scope=gpu` 缓存治理问题被取消。修复 patch 已存盘但**按用户要求回退**，待共享层 owner 处理 |
| 28 | **桶定义三处镜像、无一致性测试** | 桶边界同时存在于 `drampool_metrics.h`（C++ 注册的**唯一真源**）、`examples/metrics/metrics_configs.yaml`、`ucm/default_metrics_config.py`。C++ 注册**不读 YAML**；`test_ucm_connector_metrics.py` 只抽查名字与前缀，**不比桶数组** → 三处可以静默漂移。`drampool_metrics_test.cc:171` 也只断言了 `kMsBucketsPrepare` 一组。**顺带一提**：`drampool_metrics.h:155-158` 的注释把 YAML 称作 "the single source of truth"，这与"C++ 注册不读 YAML、真源在头文件"的实际结构自相矛盾，容易误导后来者去改错文件（这正是 PR #3820 调查中用户的第一直觉——"定位是否问题发生在此文件？"） |
| 29 | **`examples/metrics/metrics_configs.yaml` 不随 wheel 打包** | A2 用 editable 仓库树安装所以没暴露问题；A3 容器 `pip install` wheel 时 `package_data` 不含 `examples/` → 若按"已安装包路径"去找配置，daemon 会拿不到指标配置。联调脚本已改为从 `$UCM_ROOT/examples/metrics/` 读取。这是文档里没有、只能靠踩坑得知的部署约束 |
| 30 | **A3 上调度器与 worker 会同用 device 0** | 提交版 `ucm/store/dram/cc/config.h:63-68`：`GetRole()` 以 `deviceId < 0` 判 SCHEDULER，`RuntimeDeviceId()` 对 SCHEDULER **硬编码返回 0**，而 vLLM worker 取 `get_current_device_id()` 也常为 0。HIXL 明确拒绝同卡路由（`hixl_transport.cpp`），**`acl_ipc` 则不受此限制**。需要改 `config.h` 重编或改用 `TRANSPORT=acl_ipc`。A2 联调文档里出现的"2"是当时本地临时改动，**已回退**，仓库里是 0 |

### 4.4 一次完整的设计复审：从 41 到 ~64 的重设计提案

2026-10-15/16 会话 `a437daed` 对本套指标做了一次系统性复审，结论是"正常路径的覆盖度相当好"，但提出四条设计原则与一份六层提案。

**四条原则**

1. **指标集必须自带守恒式，能被验证**
   ```
   requests_received(op) = batch_outcome{ok|partial|failed|abandoned}(op) + inflight(op)
   entries_requested(op)     = entries_ok(op) + entries_failed(op)
   entry_count(t) ≈ entry_count(t-1) + stored - evicted - deleted
   ```
   这些等式**现在都不成立**（见 §4.1-6），所以"对不上账"时无法判断是业务丢了还是埋点漏了。守恒式让埋点本身可测——可以直接写成单测断言。
2. **stage 划分必须穷尽且互不重叠，加起来等于端到端**。按时间戳摊开链路，发现三个洞：① wire 到达 → 入队（解包、peer 校验）无观测；② **ExecuteAsync 返回 → Poller 取走记录（completionQueue 驻留）无观测**——最要命，因为 `*_transfer_duration_ms` 从 `submit_ms` 起算把这段驻留一起吞了，于是**"网络变慢"和"Poller 落后"在图上完全同形**，而两者处置方式截然相反；③ flag buffer 申请重试等待 + pack 只有计数无时长。
3. **事件用 push，状态用 pull**。当前 gauge 是"哪个线程恰好在那儿就由谁写"，直接产出两个 bug（§4.1-2、§4.1-3）。应改为 `MetricsReporter` 每周期在自己线程里调用一组采样回调，统一读 `metadataManager_` / `bufferManager_` / `flagBufferPool_` / 队列长度原子量——**单写者由结构保证，而不是靠约定**。
4. **维度按"归因主体"切，不按 op 机械展开**。判据是"这个阶段慢了，责任在 op 还是在基础设施？"基础设施（两级队列、Poller 轮次、GetStatus、flag 池、响应提交/传输）用**单名无 op 前缀**；op 特有（prepare、buffer 分配、数据传输、settle、batch 端到端）才带 op 前缀。按 op 全展开是 11 stage × 3 op = 33 个直方图，按此判据切完约 21 个且语义更准。

**六层提案（约 64 个指标）**

| 层 | 内容 | 关键新增 |
|---|---|---|
| **L0 自监控** | 现在**完全没有** | `uptime_seconds`、`{receiver,worker,poller,gc}_heartbeat_total`、`snapshot_write_failures_total`。告警只需一条：`rate(heartbeat) == 0` 持续 30s。**注**：提案当时给出的理由（"RequestReceiver 遇到 TCP 异常会永久 break，进程还活着、`/health` 照样 200、所有指标变平"）**经复核不成立**——该分支不可达（见 §4.2-11 的说明）。但 L0 这个缺口本身成立：缺的是"**任意**流水线线程停止推进"的独立信号，与那个具体的（错误的）触发路径无关 |
| **L1 业务/cache 有效性** | 当前**最大空白** | `*_entries_total`（命中率**分母**）、`lookup_hits_total` / `load_hits_total`、`dump_dedup_entries_total`、`entries_evicted_{ttl,forced,gc}_total`、`buffer_pool_{used,capacity}_bytes`（绝对值——比例 0.5 在 1GB 池和 100GB 池上意义完全不同）、`buffer_pool_max_used_ratio`（**取 max 而非 avg**：4KB 池打满、1MB 池空着，均值会读作 ~50%）、`buffer_alloc_unregistered_size_total` |
| **L2 生命周期 stage** | batch 级、可加和 | 补 `ingress_decode`、两级队列驻留、`response_prepare`、`response_submit`、`response_transfer`；两处语义修正：`prepare` 计时器改为**构造即武装**（消除 §4.1-1 的幸存者偏差）；`batch_total` 起点从"入队"改为"**wire 到达**" |
| **L3 饱和** | 每级边界一套 (size, capacity, wait) | 补 `poller_capacity`（`pending_depth` 现在没暴露，inflight 利用率算不出来）与 **`poller_round_duration_ms`**——把 transfer_wait 里的 Poller 拥塞成分拆出来的关键 |
| **L4 失败** | 穷尽 + 不相交 | 拆开 `response_failures_total` 的四种成因；加 `transfer_timeouts_total` / `response_timeouts_total`；加 `batch_outcome_total{ok|partial|failed|abandoned}` 作为守恒式右边 |
| **L5 依赖** | | `get_status_failures_total`；`get_status_duration_ms` **保持** data/response 合并（同一个 API，慢就是 transport 慢，拆开没有归因价值） |

**配套约定**
- **bucket 从 SLA 反推，不猜**。硬规则：**最高 bucket 必须 > `opTimeoutMs`**。每个延迟类 ~10 个 bucket、每十倍频程 2–3 个。
- **bucket 一经发布不可改**——因为改动会使 `/dev/shm` 里的持久化基线失效（这正是 §3.4-⑪ 那个卡死缺陷的触发条件，虽然已改成重基线化，但升级期仍会丢一个周期的差分）。

**另有一处反复出现的浪费**：`task_worker.cc:52-55` 每次循环（含 100µs 空转）都重写两个常量 capacity gauge，约 **2 万次/秒**的无效 `UpdateStats`。提案把 capacity 改为 pull 模型的采样即可消除。

### 4.5 未验证项 / 待确认

| 项 | 说明 |
|---|---|
| **C++ 改动未经本地编译** | reveal 测试（5 用例）、`NormalizeBlockClasses` 两个新测试、disabled 门控、启动配置修复——都只在 CI 验证。`drampool_buffer_manager_reveal_test.cc` 从未真正运行过 |
| **`test_ucm_connector_metrics.py` 本地跑不了** | 需要真实 vllm 模块（stub 缺 `vllm.v1.request`）。涉及改动函数的测试只做了静态推演与 stub 复现，**建议在 CI 里确认全绿** |
| **混合新旧版本部署行为** | 旧版本导出无 `slot_size` label 的聚合序列。缓解方案（critic 提的 `label_replace(...,"legacy",...)`）**按用户决定搁置**——需要用户判断是否存在混合部署 |
| **`ValidateBlockSizes` 是否应直接拒绝重复 size** | 当前实现是"归并 + proportions 相加"（不破坏部署），审计建议的"启动即报错"更严格，**用户选了前者，后者未正式关闭** |
| **repro harness 的去向** | `/tmp/ucm_repro`、`tmp_drampool_disabled_metrics_test/*`、`logger_perf_test_ratelimit_flake_fix.patch`（132 行）都在仓库外，未决定是否收编为仓库测试 |
| **提交策略** | 最后一个批次（重基线化 / 重复 size 归并 / per-size 面板 + gitignore）在会话结束时仍在等用户选择"单个 `[Bugfix]` 还是拆三个提交" |
| **reveal 状态不持久化** | daemon 重启会重置全部 reveal 标志，各池需重新被使用才再次出现。已声明为行为，未提持久化方案 |
| **新 size 序列出现有延迟** | = GC interval + snapshot interval + Prometheus scrape interval。边缘情形：若首个 block 在下个 GC tick 前就被释放，序列首个点是 0%（reveal 仍粘性） |
| **`acl_ipc` 传输的去留** | 为绕过 RoCE 故障而本地新增的传输协议（7 个文件），使数据面首次跑通（`transfer_failures_total` 168 → 0，`metadata_entry_count` 296 → 1638）。**未决定**是永久传输、测试 shim 还是 RoCE 恢复后删除；也**未涉及它是否输出自己的指标** |
| **A2 主机 HIXL 数据面不可用** | 8 张 NPU 的 RoCE 链路自 2026-09-29 17:52:02 起全部 DOWN（主机/交换机层问题）。通过判据：`hccn_tool -i 0 -link -g` 返回 UP |

---

## 附录 A：完整指标清单（按当前注册表顺序）

以下 40 条来自 `drampool_metrics.h:183-244` 的 `DrampoolMetricDefs()`，加 1 条基名常量（`drampool_buffer_pool_usage_ratio`，仅作动态名前缀）+ N 条运行时按 block size 注册的 gauge。

| # | 组 | 名称 | 类型 | 桶集 |
|---|---|---|---|---|
| 1 | A | `drampool_dump_requests_total` | counter | — |
| 2 | A | `drampool_load_requests_total` | counter | — |
| 3 | A | `drampool_lookup_requests_total` | counter | — |
| 4 | B | `drampool_dump_nospace_failures_total` | counter | — |
| 5 | B | `drampool_dump_failed_entries_total` | counter | — |
| 6 | B | `drampool_dump_prepare_duration_ms` | histogram | Prepare |
| 7 | B | `drampool_dump_metadata_duration_ms` | histogram | Prepare |
| 8 | B | `drampool_dump_submit_duration_ms` | histogram | Settlement |
| 9 | C | `drampool_load_miss_entries_total` | counter | — |
| 10 | C | `drampool_load_prepare_duration_ms` | histogram | Prepare |
| 11 | C | `drampool_load_metadata_duration_ms` | histogram | Prepare |
| 12 | C | `drampool_load_submit_duration_ms` | histogram | Settlement |
| 13 | D | `drampool_lookup_miss_entries_total` | counter | — |
| 14 | D | `drampool_lookup_scan_duration_ms` | histogram | Scan |
| 15 | E | `drampool_dump_transfer_duration_ms` | histogram | Transfer |
| 16 | E | `drampool_load_transfer_duration_ms` | histogram | Transfer |
| 17 | E | `drampool_transfer_failures_total` | counter | — |
| 18 | E | `drampool_get_status_duration_ms` | histogram | Settlement |
| 19 | E | `drampool_response_submit_duration_ms` | histogram | Settlement |
| 20 | E | `drampool_response_rtt_ms` | histogram | Transfer |
| 21 | E | `drampool_response_failures_total` | counter | — |
| 22 | E | `drampool_submit_failures_total` | counter | — |
| 23 | F | `drampool_metadata_entry_count` | gauge | — |
| 24 | F | `drampool_flag_pool_usage_ratio` | gauge | — |
| 25 | F | `drampool_buffer_pool_usage_ratio_<slot_size>` | gauge（动态） | — |
| 26 | G | `drampool_metadata_storeend_duration_ms` | histogram | Settlement |
| 27 | G | `drampool_metadata_loadend_duration_ms` | histogram | Settlement |
| 28 | G | `drampool_metadata_evict_gc_duration_ms` | histogram | Gc |
| 29 | H | `drampool_queue_request_full_total` | counter | — |
| 30 | H | `drampool_queue_request_enqueue_wait_ms` | histogram | Settlement |
| 31 | H | `drampool_queue_request_residence_ms` | histogram | Prepare |
| 32 | H | `drampool_queue_request_size` | gauge | — |
| 33 | H | `drampool_queue_request_capacity` | gauge | — |
| 34 | H | `drampool_queue_completion_full_total` | counter | — |
| 35 | H | `drampool_queue_completion_inflight` | gauge | — |
| 36 | H | `drampool_queue_completion_size` | gauge | — |
| 37 | H | `drampool_queue_completion_capacity` | gauge | — |
| 38 | H | `drampool_queue_response_buffer_retry_total` | counter | — |
| 39 | I | `drampool_dump_batch_total_duration_ms` | histogram | Transfer |
| 40 | I | `drampool_load_batch_total_duration_ms` | histogram | Transfer |
| 41 | I | `drampool_lookup_batch_total_duration_ms` | histogram | LookupBatch |
| + | — | `drampool_resource_log_read_errors_total` | counter（**Python 侧**） | — |

统计：20 histogram + 13 counter + 7 gauge（静态） + N 动态 gauge。

## 附录 B：关键提交

| 提交 | 日期 | 内容 |
|---|---|---|
| `4c08b38a` / `8c8e7f53` | 2026-09-17/18 | Add DramPool metrics design and summary ／ Metrics design v2: 38 metrics |
| `439cd6cb` | 2026-09-18 | integrate dramPool metrics into UCM config chain and optimize hot paths |
| `b2d62b9b` | 2026-09-18 | Add file writing support for DramPool metrics |
| `2685cc28` | 2026-09-20 | Change the file snapshot format to JSON |
| `275c29fb` | 2026-09-20 | Remove dramStore from DramPool_core |
| `93f01c2f` | 2026-09-21 | Fix: replace per-block-size buffer pool gauges with a single aggregated |
| `f2bbde33` | 2026-09-21 | Fix incorrect metrics identified during integration testing |
| `649f2b8b` | 2026-09-24 | Add DramStore and DramPool Grafana dashboard (#1438) |
| `3fc31e21` | 2026-09-24 | Refine DramPool metrics |
| `7ce12bca` | 2026-10-09 | metrics: unify transfer/rtt/lookup histogram buckets to 14 values |
| `112dd1bb` | 2026-10-09 | prometheus label |
| `109c42f5` | 2026-10-09 | Fix stale TaskWorker test signatures and clang-format for DramPool metrics |
| `b8a4415d` | 2026-10-10 | Fix Dedicated per-size KV buffer panel; drop slot_size dropdown |
| `7e515b8c` | 2026-10-13 | report buffer pool usage only for pools that served an allocation |
| `0f75a8d7` | 2026-10-13 | Validate block size/proportion counts before collapsing duplicates |
| `900b5e62` | 2026-10-14 | Isolate DramPool metrics collection when disabled |
| `0fde501b` | 2026-10-15 | Move DramPool slot-used counting out of shared BufferPool |
| `afdcbda6` | 2026-10-15 | Fix clang-format violation in CompletionPoller flag pool gauge |
| `84423cf1` | 2026-10-15 | add DramPool documentation (#1504) |

## 附录 C：错误码与术语

| 码 / 术语 | 含义 |
|---|---|
| `-50009` | store 侧无空闲槽位（`buffer_pool_<size>: no free slots`） |
| `-50010` | 客户端超时（`RuntimeError: -50010`，代码库内无定义，属误导性错误码） |
| `507001` | `ACL_ERROR_RT_TS_ERROR`（NPU Task Scheduler 异常） |
| `507033` | `ACL_ERROR_RT_DEV_SETUP_ERROR` |
| `107001` | `ACL_ERROR_RT_INVALID_DEVICEID` |
| `107002` | `ACL_ERROR_RT_CONTEXT_NULL`（线程无 ACL context） |
| `503900` | HIXL FAILED |
| `103900` | HIXL PARAM_INVALID |
| B1 / B2 / B3 | 三条阻塞链：Receiver 满 / Poller flag 池 NoSpace / TaskWorker 自旋停摆 |
| `[批次]` / `[entry]` / `[轮]` / `[覆盖]` | 观测粒度：每批次 / 每 entry / GC 每轮 / Gauge 覆盖写 |
| sticky reveal | 某 size 首次成功分配后永久进入 exposition 的粘性语义 |
| B-scoped | 线格式不变、动态维度用名字后缀编码、仅导出侧解码的 label 方案 |

## 附录 D：证据来源

- **代码**：`ucm/store/dram/cc/drampool/`（`drampool_metrics.h`、`metrics_reporter.{h,cc}`、`completion_poller.{h,cc}`、`task_worker.{h,cc}`、`drampool_server.cc`、`drampool_config.h`、`drampool_launch_config.cc`、`drampool_daemon.cc`、`metadata.cc`、`buffer_manager.h`、`pos_eviction_policy.h`）、`ucm/shared/metrics/`、`ucm/store/dram/resource_reporter.py`、`ucm/shared/metrics/resource_reporter.py`、`ucm/metrics_config.py`、`ucm/default_metrics_config.py`、`examples/metrics/{metrics_configs.yaml,grafana_dram.json}`、`examples/drampool.yaml`、`examples/ucm_config_dramstore.yaml`、`ucm/store/test/`、`test/test_drampool_resource_reporter.py`、`docs/`
- **设计/联调文档**：`dramPool_documents/metrics_design.md`、`metrics_summary.md`、`metrics_summary.csv`、`e2e_retrospective_20261012.md`、`A2_VLLM_DRAMSTORE_GRAFANA_E2E_SUMMARY.md`、`a2-vllm-dramstore-drampool-grafana-z009-flickering-pascal.md`、`PR1394_FULL_CHAIN_HANDOFF.md`
- **会话**：`4c08…9c16` 期间的 24 个主会话转录（2026-09-17 ~ 2026-10-16），其中与指标开发直接相关的为 `ca3c574f`、`dcb074f3`、`a437daed`（设计评审）、`7d189a46`（label 方案）、`308378d8`、`d3205631`（开关隔离 / CI）、`7c33b968`（A2 端到端）、`ae6d9a8e`、`06ce3935`（代码导读 / 面板解读）

> **口径声明**：本文中标注为"核验"的代码结论，均由本次直接读取当前工作区文件（分支 `z00984767-0917`，HEAD `afdcbda6`）得出；标注为会话/文档来源的历史结论，已尽量交叉引用两次以上。凡两者冲突处，以代码复核结果为准。
>
> **本文的 12 条关键论断经过一轮对抗性复核**（每条由独立 agent 专职尝试证伪，只认代码证据）。复核结果：**8 条 CONFIRMED，4 条被修正并已回写本文**——分别是：用户文档覆盖数（10 而非 9，24% 而非 20%）、completionQueue 自旋等待的观测状态（不是"未观测"，而是被吞进 `prepare` 与 `batch_total` 且无法归因）、默认驱逐策略的实际行为（第一次 TTL 重试是真在干活，只有第二次 deep 释放 0 条目）、以及「接收线程会被 TCP 异常静默杀掉」这一条**被证伪**（该分支不可达，见 §4.2-11）。另有一处数字陷阱被澄清：会话里流传的"43 个 `drampool_*` 指标"来自 `grep -c '"drampool_'`，其中两行是 `#include "drampool_*.h"`，**真实为 41 个名字 / 40 个静态注册项**。
>
> 此外，报告中所有"指标名 ↔ 桶集"的归属关系已用脚本从 `drampool_metrics.h` 的注册表反向提取并逐条比对（7 + 5 + 5 + 1 + 1 + 1 = 20 个直方图），附录 A 的 41 条清单亦按注册表顺序核对过。
