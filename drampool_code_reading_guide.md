# DramPool 子系统通读导读清单

> 适用仓库：`unified-cache-management`（梳理时分支 `z00984767-0917`）
> 读法：自上而下（入口 → 协议 → 服务端 → 状态核心 → 客户端 → 底层 → 集成 → 指标 → 验证）。每个文件都给出行数与"该提取什么"，按顺序打开即可，不需要预先熟悉仓库其余部分。
> 范围：DramPool 子系统（`ucm/store/dram`，约 10.2k 行 C++）+ 其直接依赖（`ucm/shared`、`ucm/transport/p2p`、Python 集成层、metrics 链路、测试）。不含 `ucm/sparse`、`kv_semantics`、`toolkit`。

---

## 1. 全景（Architecture at a glance）

### 1.1 术语先对齐

| 名称 | 是什么 | 产物 | 命名空间 |
|---|---|---|---|
| **DramPool** | 独立部署的 DRAM KV-Cache **服务端守护进程**：接收 KV 块、把块驻留在主机 DRAM、按 TTL/位置驱逐 | 可执行文件 `drampool`（经静态库 `drampool_core`） | `UC::DramPool` |
| **DramStore** | 训练/推理进程加载的**客户端共享库**：实现 `UC::StoreV1`，把上层 Task 路由成发往各 DramPool 节点的线协议请求 | 共享库 `libdramstore.so`（目标名 `dramstore`） | `UC::Dram` |
| **kv_protocol** | 两端之间唯一的二进制契约（请求走 TCP、响应走 one-sided RDMA DMA） | `kv_protocol.cc` **同时编译进两个目标** | `UC::DramPool`（`OpType` 来自 `UC::Dram`） |

角色反差（第一天最容易被绊倒的点）：

- 网络角色与名字相反——**DramPool 是 HIXL 的 Client（主动外连），DramStore 才是 HIXL 的 Listener/Server**。
- 动词方向站在客户端 KV-Cache 视角：`DUMP`=把 KV 推进池里；`LOAD`=从池里拉回；`LOOKUP`=存在性探测。

### 1.2 分层栈

```
L7  Python 集成     ucm/utils.py → factory_v1 → pipeline/connector.py
                    │  (dlopen libdramstore.so，唯一 pybind 边界是 ucmpipelinestore)
L6  Store 接口      ucm/store/ucmstore_v1.h  (StoreV1：Setup/Lookup/Load/Dump/Check/Wait)
L5  客户端 Actor    task_manager → node_scheduler/Runner → node_actor
                    reply_service（reply slot 轮询）、transport_executor
L4  传输后端        transport_manager_backend（Ascend-only）+ ucm/transport/p2p
                    ├─ TCP 控制面（two_sided）：只传编码后的请求字节
                    └─ HIXL/RDMA 数据面（one_sided）：KV 负载 + 响应 DMA
L3  线协议          kv_protocol.{h,cc}  ← 同一份 TU 编进客户端与服务端，无版本字段
L2  服务端          drampool_daemon → DramPoolServer
                    RequestReceiver →[SPSC]→ TaskWorker →[SPSC]→ CompletionPoller；GC
                    HealthServer（独立线程）、MetricsReporter（独立线程）
L1  状态核心        metadata（1024 分片 + 两级锁）、entry、buffer_manager
                    eviction_policy（TTL / POSITION；LRU 休眠）
L0  基础设施        UC::BufferPool / BufferRegion / IndexPool、router、status、logger、
                    spsc_ring_queue、lock、UC::Metrics 域、trans（Ascend/CUDA/simu）
```

### 1.3 进程/线程拓扑

```
 客户端进程（vLLM/引擎 + UCM）                         服务端进程（drampool 守护进程）
┌──────────────────────────────────────────┐        ┌──────────────────────────────────────────────┐
│ Python 调用线程                            │        │ main → DramPoolDaemon::Run（主线程）           │
│   Dump/Load/Lookup                         │        │   CLI → YAML → 指标注册 → Logger → Signals     │
│      │ StoreV1                             │        │                            ┌─────────────────┐ │
│      ▼                                     │        │                            │ DramPoolServer  │ │
│ TaskManager（1 worker 线程）                │        │  TCP 请求字节 ───────────▶ │ RequestReceiver │ │
│   路由+切批 → NodeScheduler.Post            │        │  (two_sided 控制通道)      │      │ ReqQ(SPSC)│ │
│      ▼                                     │        │                            │      ▼           │ │
│ NodeScheduler Runner 线程（默认1）          │        │                            │ TaskWorker      │ │
│   NodeActor 状态机（per-node 单线程）       │        │                            │      │ CmpQ(SPSC)│ │
│      │ acquireReplySlot / EncodeRequest    │        │                            │      ▼           │ │
│      ▼                                     │        │  HIXL RDMA ◀────────────▶ │ CompletionPoller │ │
│ TransportExecutor worker（node 哈希固定）   │        │  (KV: Read/Write;         │ GC 线程          │ │
│   → TransportManagerBackend               │        │   响应: Write 回 reply)   └─────────────────┘ │
│ ReplyService observer 线程 ◀── DMA 响应落地 │        │ HealthServer worker（独立裸 socket，默认禁用） │
│ UC::Metrics 域 + resource_reporter 线程     │        │ MetricsReporter worker（drains 指标域）        │
│        ▲                                   │        └──────────┬───────────────────────────────────┘
└────────┼───────────────────────────────────┘                   │ drampool_metrics.json
         └──── 读 drampool_metrics.json ◀─── 文件（跨进程唯一通道）┘
```

进程内共约 6 个工作线程，但归属是分裂的：`DramPoolServer` 只拥有 4 条流水线线程（Receiver/Worker/Poller/GC）；`HealthServer` 和 `MetricsReporter` 各持 1 线程，服务端对它们的存在一无所知，接线只发生在 `DramPoolDaemon::Run`。

### 1.4 代码量与工作量预估

| 范围 | 行数（实测 `wc -l`） |
|---|---|
| 服务端目录 `ucm/store/dram/cc/drampool/`（28 个文件） | **5,406** |
| 客户端目录 `ucm/store/dram/cc/`（含 `kv_protocol.cc` 621 行） | **4,799** |
| C++ 核心合计（去重口径） | **约 10.2k** |
| C++ 测试 `ucm/store/test/case/dram/` | **7,902** |
| Python（utils/factory/pipeline/两个 reporter/metrics 配置） | 约 3.5k（另有 `ucm/integration/vllm/ucm_connector.py` 单文件 3,735 行，**不要全读**） |
| 示例 YAML + 指标 YAML + PR1394 handoff | 约 1.6k |

建议节奏：**必读主线约 7k 行，2–3 天可过完**；含测试与文档完整通读约 2 万行。

---

## 2. 阅读路线（The reading route）

> 优先级：**必读** = 主线路径上不可跳过；**建议** = 首轮可读，帮助建立完整心智模型；**可选** = 按需回查。
> 顺序严格按"入口 → 内部"组织。唯一的小调整建议：正式开读前先花 10 分钟扫 `ucm/shared/infra/status/status.h`（错误码词汇），其余顺序不变——底层设施放在第 7 阶段，是为了带着具体调用点回头读，避免无目的浏览。

### 阶段 1：进程入口与配置加载

**读完这一阶段你应该能回答：** `drampool` 二进制如何启动？命令行和 YAML 各承载什么？`g_config` 在什么时机被填满？

| # | 路径 | 行数 | 优先级 | 本文件要提取什么 |
|---|---|---|---|---|
| 1.1 | `ucm/store/dram/cc/drampool/main.cc` | 30 | 必读 | 进程入口只做一件事：栈上构造 `DramPoolDaemon` 并返回 `Run` 的退出码。 |
| 1.2 | `ucm/store/dram/cc/drampool/drampool_daemon.h` | 44 | 必读 | `Run` 生命周期入口、logger/信号设置、`WaitForShutdown` 与静态 atomic `shutdownRequested_`。 |
| 1.3 | `ucm/store/dram/cc/drampool/drampool_config.h` | 116 | 必读 | 单一聚合体 `DramPoolConfig`（所有字段带结构内默认值）、inline 全局 `g_config`、两个 parser 原型。 |
| 1.4 | `ucm/store/dram/cc/drampool/parse_utils.h` | 165 | 必读 | trim/大小写折叠/带范围校验的整数与布尔解析原语。 |
| 1.5 | `ucm/store/dram/cc/drampool/drampool_launch_config.cc` | 385 | 必读 | 命令行解析、逐字段校验，以及把"总 GiB × 各档比例"换算成每档槽数的数学。 |
| 1.6 | `ucm/store/dram/cc/drampool/drampool_yaml_config.cc` | 547 | 必读 | 手写缩进 YAML 解析：点分键、静态 parser 分派表、endpoints/device_ids、必填键与跨字段校验。 |
| 1.7 | `examples/drampool.yaml` | 72 | 必读 | 运行时 YAML 的权威样例：端点路由表、全部必填键与每个旋钮的内联注释。 |
| 1.8 | `ucm/store/dram/cc/drampool/drampool_daemon.cc` | 139 | 必读 | 本阶段先读 `Run` 主干：严格启动顺序与逆序关停；细节在后面各阶段还会回来。 |

**关键符号：** `main`、`DramPoolDaemon::Run`、`DramPoolConfig`、`g_config`、`ParseCommandLine`、`CalculatePoolSlotCounts`、`ValidatePoolSize/ValidateBlockSizes`、`ParseYamlConfig`、`ValidateRuntimeConfig`、`ValidateRequiredRuntimeConfigKeys`、`CommitEndpointEntry`、`ParseInlineDeviceIds`、`ParseDramPoolEndpoint`、`BuildUsage`、`kRequiredRuntimeConfigKeys`、`kDefaultDramPoolRuntimeConfigPath`。

**边界：** 本阶段全部在主线程，无跨线程；唯一"全局边界"是进程级 `g_config`（header inline，无依赖注入，所有后续组件直接读它）。

**读时要点：**

- 配置严格两段：CLI **只**装进程身份/容量（`--addr`、`--nics`、`--pool-size-gb`、`--kvcache-block-sizes`、可选 `proportions/ttl/config`）；其余全部来自 YAML。
- 结构内默认值大部分是假象：`kRequiredRuntimeConfigKeys` 强制 **20 个键**显式存在（`transport.device_ids/hixl.*`、`queue.*`、`request_receiver`、`poller`、`flag_buffer.*`、`gc.*`、`metadata.*` 六项、`operation.timeout_ms`、`logger.*`），缺一个就拒绝启动。
- 默认 `--config` 是相对路径 `examples/drampool.yaml`；安装后的二进制在 `bin/`，生产环境应始终显式传 `--config`。

---

### 阶段 2：线协议契约（客户端 ↔ 服务端）

**读完这一阶段你应该能回答：** 三种请求/响应各占多少字节、结果如何位打包、为什么改一个偏移量就是静默协议破裂。

| # | 路径 | 行数 | 优先级 | 本文件要提取什么 |
|---|---|---|---|---|
| 2.1 | `ucm/store/dram/cc/types.h` | 157 | 必读 | `OpType` 枚举顺序（LOOKUP=0/DUMP=1/LOAD=2）**就是线上 opcode 字节**；`kMaxProtocolBatchEntries=65535`；`RequestToken`/`ReplySlot` 等词汇先混眼熟。 |
| 2.2 | `ucm/store/dram/cc/kv_protocol.h` | 231 | 必读 | 头部/条目尺寸常量、全部字节偏移、`Kv*Request`/`KvResponse` 数据结构、三个 `KvProtocol` 实现与 `ProtocolManager` 门面。 |
| 2.3 | `ucm/store/dram/cc/kv_protocol.cc` | 621 | 必读 | memcpy 序列化、unpack 严格长度/零字段校验、LSB-first 位打包、opcode 分派、`IsResponseReady` 的 volatile doorbell 读。 |
| 2.4 | `ucm/store/dram/cc/drampool/drampool_types.h` | 157 | 必读 | 打包结果码的人类语义（`DumpLoadResult`、`LookupResult`）与 `CompletionRecord` 三阶段状态机（阶段 3 还会精读）。 |
| 2.5 | `ucm/store/test/case/dram/kv_protocol_test.cc` | 1067 | 建议 | 线协议的可执行规格：逐字节布局、位打包真值表、全部拒绝路径；查布局时翻这里比翻实现快。 |
| 2.6 | `ucm/store/dram/cc/messages.h` | 122 | 可选 | **不是线上消息**：是客户端进程内 actor 的命令/事件词汇，阶段 5 正式读，别在这里找线格式。 |

**关键符号：** `OpType`、`KvDumpRequest`/`KvLoadRequest`/`KvLookupRequest`、`KvDumpEntry`/`KvLoadEntry`、`KvResponse`、`ResponseStatus`、`ProtocolManager`、`KvDumpProtocol`/`KvLoadProtocol`/`KvLookupProtocol`、`PackRequest`/`UnpackRequest`、`PackResponse`/`UnpackResponse`、`IsResponseReady`、`GetPackedResponseSize`、`PackResults`/`UnpackResults`。

**边界：**

- **【跨进程契约】** 无版本号、无 magic、无头长字段；兼容性仅靠两端编译同一份 `kv_protocol.{h,cc}` + 一个 `static_assert(BlockId==16B)`。
- **【跨网络落点】** 请求字段 `resp_addr`（偏移 9）必须是客户端 reply slot 的 one-sided RDMA 暴露地址；响应由服务端 DMA 写回，接收路径上不存在 socket 回复。

**字节布局（建议自己默画一遍再对照）：**

```
DUMP 头 23B:  u8 opcode@0  u64 request_id@1  u64 resp_addr@9  u32 ttl@17  u16 batch_size@21
LOAD/LU 头 19B: u8 opcode@0  u64 request_id@1  u64 resp_addr@9  u16 batch_size@17
Dump/Load entry = 32B: 16B key | u64 addr@16 | u32 len@24 | u32 idx@28
Lookup entry     = 16B: key only
响应: u64 request_id@0 | u8 doorbell@8 (0 Pending,1 Ready) | 位打包结果@9…
       LOOKUP 1 bit/项（8 项/字节）；DUMP/LOAD 4 bit/项（2 项/字节），LSB-first
       语义: Lookup 0=NotFound/1=Exists；Dump/Load 0=Ok/1=Failed
```

---

### 阶段 3：服务端核心与四线程流水线

**读完这一阶段你应该能回答：** 四个线程各自消费谁？启动/关停顺序为什么是负载关键（load-bearing）？背压如何一级级传播？

| # | 路径 | 行数 | 优先级 | 本文件要提取什么 |
|---|---|---|---|---|
| 3.1 | `ucm/store/dram/cc/drampool/drampool_server.h` | 135 | 必读 | `Init/Start/Stop` 与 `ServerState`；枚举全部 Init* 阶段、Start* 阶段、四个 stop 原子与四个线程成员。 |
| 3.2 | `ucm/store/dram/cc/drampool/drampool_server.cc` | 616 | 必读 | 生命周期实现、`RequestReceiveLoop`、GC loop、有序 teardown 与 `ResetInitializedComponents`；线程/背压模型真正接线的地方。 |
| 3.3 | `ucm/store/dram/cc/drampool/task_worker.h` | 59 | 必读 | request queue 唯一消费者的接口：三个 `Process*` 与 `QueueResponse`/`SubmitCompletion`。 |
| 3.4 | `ucm/store/dram/cc/drampool/task_worker.cc` | 416 | 必读 | pop → 元数据 prepare（StoreBegin/LoadBegin/Exist）→ 构造 segment → 一次非阻塞 `ExecuteAsync` → 移交 completion。 |
| 3.5 | `ucm/store/dram/cc/drampool/completion_poller.h` | 55 | 必读 | completion queue 唯一消费者；pending deque 窗口与三个私有阶段方法。 |
| 3.6 | `ucm/store/dram/cc/drampool/completion_poller.cc` | 452 | 必读 | 异步完成状态机：轮询数据传输 → 打包/提交响应 → 轮询响应传输；settle 元数据；全部一次性批次指标在此。 |
| 3.7 | `ucm/shared/infra/template/spsc_ring_queue.h` | 121 | 必读 | 热路径真正用的无锁 SPSC 环：容量 N 实放 **N-1**、非 2 的幂用取模、阻塞 `Push` 自旋 yield。 |
| 3.8 | `ucm/store/dram/cc/bounded_queue.h` | 91 | 建议 | 红鲱鱼：**没有任何 drampool 服务端文件 include 它**；它是客户端的 mutex 环，阶段 5 再用。 |
| 3.9 | `ucm/store/dram/cc/drampool/health_server.h` | 52 | 建议 | 自持 HTTP 监听器接口，不持有任何 `DramPoolServer` 引用。 |
| 3.10 | `ucm/store/dram/cc/drampool/health_server.cc` | 242 | 建议 | 手写非阻塞 POSIX HTTP：仅当请求行严格等于 `GET /health HTTP/1.1` 或 `HTTP/1.0` 才回固定 200，其余一律静默 close；`healthPort==0` 时不启动。 |

**关键符号：** `DramPoolServer`、`ServerState{New,Initialized,Running,Stopped}`、`Initialize*`（8 个）、`Start*`、`RequestReceiveLoop`、`WaitForChannelReady`、`TaskWorkerLoop`、`CompletionPollerLoop`、`GCThreadLoop`、`TaskWorker::Run/ProcessDump/ProcessLoad/ProcessLookup`、`CompletionPoller::Run/FillPendingWindow/PollPendingCompletions/PollDataTransfer/SubmitResponse/PollResponseTransfer/SettleDataTransfer`、`HealthServer::Start/Run/HandleClient/IsHealthRequest`、`UC::SpscRingQueue`。

**边界（逐条对照代码确认）：**

```
【跨线程】RequestReceiver ── TryPush(满则 sleep 重试) ──▶ RequestQueue(SPSC)
【跨线程】RequestQueue ── TryPop ──▶ TaskWorker ── 阻塞 Push 兜底 ──▶ CompletionQueue(SPSC) ──▶ CompletionPoller
【跨线程】GC 线程为第 4 线程，与 Worker/Poller 并发进入同一 MetadataManager
【跨网络】TCP ingress 是 Start 的最后一步（StartTcpMessageChannel），保证监听时消费者已全部就绪
【跨网络】HIXL 以 HixlRole::Client 安装（StartTransportService），主动外连 DramStore，路由全静态
```

**顺序要点：**

- `Start()`：poller → worker → GC → receiver → 传输/HIXL（含 `RegisterBufferPools`）→ TCP 监听。
- `Stop()`：先关 TCP ingress 并 join receiver → join worker（排空已收任务）→ join poller → join GC → 传输 Shutdown → `ResetInitializedComponents`（先析构 worker/非拥有式 `runtime_`，再析构 managers/pools）。
- `Init()` 失败与 `Stop()` 共用同一条 `ResetInitializedComponents()` 清理路径（`MakeScopeExit` 回滚）；部分初始化关停是正常路径，不是异常路径。

---

### 阶段 4：元数据、缓冲与驱逐策略

**读完这一阶段你应该能回答：** 16 字节 BlockId 如何映射到 DRAM 槽位？两级锁各自保护什么？lease 与 life 两个 TTL 有何区别？驱逐为什么是"先标记后收割"？

| # | 路径 | 行数 | 优先级 | 本文件要提取什么 |
|---|---|---|---|---|
| 4.1 | `ucm/store/dram/cc/drampool/buffer.h` | 42 | 必读 | 单个已分配 DRAM 区域的 POD：size-class 池内槽索引、字节长度、数据落地的主机地址。 |
| 4.2 | `ucm/store/dram/cc/drampool/entry.h` | 135 | 必读 | 一条缓存记录：不可变身份/放置字段 + 被自身 Spinlock 保护的 `refCnt/status/leaseTimeout`，以及全部 `Try*` 状态转换。 |
| 4.3 | `ucm/store/dram/cc/drampool/eviction_policy.h` | 144 | 必读 | `EvictionPolicyType{TTL,POSITION}`、抽象接口（无内部锁）与 `OrderedEvictionPolicy<Cmp>`（multiset + BlockId→iterator 索引）。 |
| 4.4 | `ucm/store/dram/cc/drampool/ttl_eviction_policy.h` | 69 | 必读 | 默认**周期**策略：按 `lifeTimeout` 升序走，遇到第一个未到期条目即停；`evict_ratio` 被忽略。 |
| 4.5 | `ucm/store/dram/cc/drampool/pos_eviction_policy.h` | 77 | 必读 | 默认**深度**策略：按推理 token position 降序（平局 lifeTimeout 升序），返回至多 `max(1, ratio*n)` 条；`AccessKey` 有意 no-op。 |
| 4.6 | `ucm/store/dram/cc/drampool/lru_eviction_policy.h` | 110 | 建议 | 生产不可达（枚举/工厂/YAML 都不接），只为理解访问跟踪模式与跑单测。 |
| 4.7 | `ucm/store/dram/cc/drampool/buffer_manager.h` | 198 | 必读 | header-only：按精确槽字节尺寸持一个 `UC::BufferPool`；`Allocate/Free`；发布供 RDMA 注册的 `MemoryRegions`。 |
| 4.8 | `ucm/store/dram/cc/drampool/metadata.h` | 207 | 必读 | `ShardMetadata`（map + 双策略，后置 RwLock）与 `MetadataManager`（1024 分片、hash 定位、包裹 BufferManager）。 |
| 4.9 | `ucm/store/dram/cc/drampool/metadata.cc` | 229 | 必读 | 生命周期与两阶段删除/驱逐；NoSpace 时对**随机分片**先周期后深度驱逐；策略工厂 `CreateEvictionPolicy`。 |

**关键符号：** `Buffer`、`Entry`/`EntryPtr`、`EntryStatus{INITIALIZED,READY,DELETING}`、`TryMarkReady/TryIncRef/TryDecRef/TryMarkEvicting/TryMarkHit`、`EvictionPolicy`、`TtlEvictionPolicy`、`PosEvictionPolicy`、`LruEvictionPolicy`、`BufferManager`、`MetadataConfig`、`ShardMetadata`、`MetadataManager`、`kShardCnt=1024`、`StoreBegin/StoreEnd/LoadBegin/LoadEnd/Exist/Query/Delete/PerformEvict/GetKeyCnt`、`EvictOneShard`。

**边界：**

- **【跨线程并发】** GC、TaskWorker、CompletionPoller 三个线程可同时进入 `MetadataManager/BufferManager`；协调靠每分片 `RwLock`（护 map 与驱逐容器）+ 每 Entry `Spinlock`（护 `refCnt/status/leaseTimeout`）。
- **【锁边界】** 标记发生在分片**读锁**下（`GetEvictionResults` 只把合格 READY 翻成 DELETING）；收割发生在读锁释放**之后**——调用方先 `BufferManager.Free` 再 `ShardMetadata.Delete`（写锁，从 map 和两个策略中擦除）。

**读时必须分清的两组概念：**

1. `leaseTimeout`：可变，LOOKUP 命中即续期（`Exist → TryMarkHit(now+lease)`），是 `TryMarkEvicting` 的真正闸门；`lifeTimeout`：DUMP 时一次盖定、不可变，仅用于 TTL/POS 的 multiset 排序。所以 TTL 可能放过 lifeTimeout 已过但 lease 刚续的条目，也可能驱逐 lifeTimeout 未到但 lease 已过期的条目。
2. INITIALIZED（DUMP 在途，不可驱逐）→ READY（可被 refCnt pin、受 lease 保护）→ DELETING（已判定，等待擦除）。

---

### 阶段 5：客户端库 DramStore（libdramstore）

**读完这一阶段你应该能回答：** 一个 Task 如何扇出成 per-node Request？per-node 状态为什么不需要锁？reply slot 租约如何保证复用安全？超时为什么是"fence 整个节点"而不是重试？

| # | 路径 | 行数 | 优先级 | 本文件要提取什么 |
|---|---|---|---|---|
| 5.1 | `ucm/store/ucmstore_v1.h` | 190 | 必读 | 现代异步接口 `StoreV1`：`Setup(Dictionary)`、三类 Lookup、`Load/Dump → TaskHandle`、`Check/Wait`。 |
| 5.2 | `ucm/store/detail/type/types.h` | 71 | 必读 | `BlockId`（16B）、`TaskHandle`（size_t 令牌）、`Shard`（块+分片索引+per-tensor 设备地址）、`TaskDesc`。 |
| 5.3 | `ucm/store/detail/type/dictionary.h` | 77 | 建议 | 类型擦除的 string→`std::any` 配置；数字归一为 `ssize_t`，解释了配置侧的 `GetNumber/GetNumbers`。 |
| 5.4 | `ucm/store/dram/cc/config.h` | 77 | 必读 | `DramConfig`、`Role`（由 deviceId 符号推断）、`RuntimeDeviceId`、`Parse/Validate`。 |
| 5.5 | `ucm/store/dram/cc/config.cc` | 367 | 建议 | Dictionary→DramConfig 全量键/默认值/边界；静默 per-device 端口偏移；`replySlotCount=node_count*maxInflight`；`MaxReplySize` 会临时构造 `ProtocolManager`。 |
| 5.6 | `ucm/store/dram/cc/messages.h` | 122 | 必读 | 四个 actor 间的命令（Transmit/Connect/FenceEpoch）与事件、`NodeDependencies` 回调束、fail-stop 声明。 |
| 5.7 | `ucm/store/dram/cc/bounded_queue.h` | 91 | 建议 | 客户端 task/completion/command 队列：固定容量、零分配、Push 满即 false，背压显式处理。 |
| 5.8 | `ucm/store/dram/cc/dram_store.cc` | 361 | 必读 | 唯一 `StoreV1` 实现与组合根：backend→reply→router→executor→scheduler→task manager 的构造/接线/逆序关停；`extern "C" MakeDramStore`。 |
| 5.9 | `ucm/store/dram/cc/task_manager.h` | 152 | 必读 | 单 worker 任务 actor：任务准入、taskId→future 表、worker-only `ActiveTask`；公有 Submit/Wait/Check 只入队/观察。 |
| 5.10 | `ucm/store/dram/cc/task_manager.cc` | 452 | 必读 | Task 展开为 per-tensor IoEntry、`RouteKeys` 分组、按 ≤65535 切批；聚合所有 `RequestCompleted` 后兑现 promise；completion 优先于 submission。 |
| 5.11 | `ucm/store/dram/cc/node_scheduler.h` | 71 | 必读 | Runner 线程集合；`Post`（请求入 node 的 runner）、`Publish`（事件入同一 runner）。 |
| 5.12 | `ucm/store/dram/cc/node_scheduler.cc` | 244 | 必读 | per-runner 事件循环：批量取出命令/事件分派给 NodeActor，计算最早 `NextWakeup`；每 runner 初始化 CANN 设备上下文。 |
| 5.13 | `ucm/store/dram/cc/node_actor.h` | 130 | 必读 | 单节点状态机（DISCONNECTED/CONNECTING/ACTIVE/FENCING）、pending 队列、`RequestRecord`（token + reply slot）。 |
| 5.14 | `ucm/store/dram/cc/node_actor.cc` | 642 | 必读 | 请求执行心脏：`StorageKey` 折叠 shardId、取 reply slot、编码提交、TRANSMITTING→INFLIGHT；deadline 触发整节点 FENCING → epoch++ → 重连。 |
| 5.15 | `ucm/store/dram/cc/reply_service.h` | 121 | 必读 | reply slot 的唯一管理者：`Acquire/Release`、`Lease`、`MemoryRegion`。 |
| 5.16 | `ucm/store/dram/cc/reply_service.cc` | 305 | 必读 | 轮询 worker：看门控字节/request_id、校验解包、每个租约恰好发一个 `ReplyObserved`；码语义按 op 反转。 |
| 5.17 | `ucm/store/dram/cc/transport_executor.h` | 111 | 必读 | `ITransportBackend` 抽象与 worker-pool 执行器：node 哈希固定 worker，普通命令与 fence 分别计容量。 |
| 5.18 | `ucm/store/dram/cc/transport_executor.cc` | 243 | 必读 | worker 取命令调后端并回发 NodeEvent；保证同 node（含其 fence）全部钉在同一 worker。 |
| 5.19 | `ucm/store/dram/cc/dram_fatal.cc` | 45 | 建议 | `AbortDramStore`：尽力记日志后 `std::abort()`，fail-stop 语义落地处。 |
| 5.20 | `ucm/store/ucmstore.h` | 51 | 可选 | 旧模板接口 `CCStore<T>`（Alloc/Lookup/Commit），DramStore **不实现**，只用于理解代际分裂。 |

**关键符号：** `MakeDramStore`、`DramStore`、`StoreV1`、`TaskManager`（注意与旧 `UC::TaskManager` 同名）、`NodeScheduler/Runner`、`NodeActor/NodeState/RequestRecord`、`StorageKey`、`ReplyService/Lease/SlotContext`、`TransportExecutor`、`ITransportBackend`、`NodeDependencies`、`TransportCommand`、`NodeEvent`、`RequestCompleted`、`DramConfig/Role`、`AbortDramStore`。

**边界：**

```
【跨线程】caller 线程 ── submissions_ BoundedQueue+CV ──▶ TaskManager 单 worker（立即返回 TaskHandle）
【跨线程】TaskManager ── NodeScheduler.Post ──▶ 所属 node 的 Runner（node 哈希到 runner）
【跨线程】Runner ── TransportExecutor.TryPost ──▶ node 哈希固定的 executor worker
【无锁依据】某 node 的全部状态只被一个 Runner 线程触碰 → per-node 状态无需锁
【fail-stop】Runner/ReplyService/Publish 路径上任何未捕获异常 → abort 整个宿主进程
```

**读时要点：**

- `StorageKey`：`shardId != 0` 时把 shardId 异或混入 16B key 的前 4 字节（因为服务端只按 BlockId 建 key）；shard 0 保留原 key，LOOKUP 永远以 shard 0 发起。
- DUMP 的前置 ACL event 在 node pending 队列队首被 `Trans::Event::Query` 轮询门控，后面的请求不能插队。
- 无跨节点重试/改路由：Router 定死节点；暴露中的请求超 deadline → 整节点 Fence（HIXL Disconnect）、全部暴露请求 Timeout、epoch++ 重连；Connect/Fence 命令本身按 `reconnect_interval_ms` 重试。

---

### 阶段 6：传输层（p2p：TCP 控制面 + HIXL 数据面）

**读完这一阶段你应该能回答：** "Transmit"实际传了什么？Connect/Fence 在 one-sided 语义下对应什么？需要深挖 HIXL 时入口在哪？

| # | 路径 | 行数 | 优先级 | 本文件要提取什么 |
|---|---|---|---|---|
| 6.1 | `ucm/store/dram/cc/transport_manager_backend.h` | 89 | 建议 | Ascend-only 后端声明与 options（HIXL listen port、CS 开关、连接/传输超时、node 表）。 |
| 6.2 | `ucm/store/dram/cc/transport_manager_backend.cc` | 167 | 建议 | 两个平面：`Transmit` 只经 TCP 发编码请求字节；`Connect=ExchangeMetadata+manager_.Connect`；`Fence=manager_.Disconnect`。 |
| 6.3 | `ucm/transport/p2p/include/core/transport.h` | 124 | 必读 | 传输词汇：`TransportManager`、`TransferHandle`、`Operation/Segment`、`ManagerID`、`TransferStatus`、`kInvalidTransferHandle`。 |
| 6.4 | `ucm/transport/p2p/include/core/transport_manager.h` + `src/core/transport_manager.cpp` | 750 | 建议 | 连接与 ManagerID 注册表的持有者；`Connect/Disconnect/ExecuteAsync/GetStatus/RegisterMemory` 的真正实现入口。 |
| 6.5 | `ucm/transport/p2p/include/core/transport_init_attrs.h` | — | 建议 | `HixlRole{Server,Client,Bidirectional}` 与 `HixlInitAttrs`（ip / per-device instances / 两个超时）。 |
| 6.6 | `ucm/transport/p2p/include/channels/tcp/tcp_message_channel.h` | 91 | 建议 | `TcpMessageChannel` 的 `Init/Receive/Send` 接口与就绪语义（服务端 `WaitForChannelReady` 等的就是它）。 |
| 6.7 | `ucm/transport/p2p/src/channels/tcp/tcp_message_channel.cpp` | 553 | 可选 | epoll io 线程实现；排查 TCP 收包/连接细节时才下钻。 |
| 6.8 | `ucm/transport/p2p/src/protocols/hixl/hixl_instance.{h,cpp}` | ~360 | 可选 | 持一个 `hixl::Hixl` 引擎，用专属 worker 线程把其操作串行化（`packaged_task` 队列）；原生 API 只有 Initialize/RegisterMem/Connect/TransferAsync/GetTransferStatus/Disconnect/Finalize。 |
| 6.9 | `ucm/transport/p2p/src/protocols/hixl/hixl_transport.{h,cpp}` | 746 | 可选 | 实现 `transport::Transport`：多设备实例、peer 路由表、内存注册表、在途传输表；`Export/ImportMetadata` 经 TCP 带外互换 peer 地址。 |
| 6.10 | `ucm/transport/p2p/include/control/control_channel.h` + `src/control/control_channel.cpp` | 469 | 可选 | 建连握手/metadata exchange 所在；要确认 one_sided 端口在建连时如何使用时再进。 |
| 6.11 | `ucm/transport/p2p/CMakeLists.txt` | ~120 | 建议 | HIXL 探测逻辑：`HIXL_ROOT/INCLUDE_DIR/LIB_DIR` → 找 `hixl/hixl.h` 与 `cann_hixl`，找不到则 `P2P_HIXL_AVAILABLE=OFF` 并跳过编译。 |
| 6.12 | `ucm/shared/trans/device.h`、`stream.h`、`event.h`、`buffer.h` | — | 建议 | 后端抽象接口层：`Device::MakeBuffer` 决定内存从哪来，`Event::Query` 是 DUMP 前置门控。只读接口，不进 ascend/cuda/simu 实现。 |

**关键符号：** `TransportManagerBackend`、`CreateTransportManagerBackend`、`HixlInitAttrs`、`HixlRole`、`transport::TransportManager`、`TransportManager::Connect/Disconnect/ExecuteAsync/GetStatus/RegisterMemory`、`HixlInstance`、`HixlTransport`、`TcpMessageChannel`、`TransferHandle/TransferStatus`、`Operation/Segment`、`ManagerID`、`Trans::Device/Event`。

**边界：** **【跨网络】** TCP/two_sided 只承载请求头与条目；**【跨网络 RDMA】** HIXL/one_sided 承载 KV 负载（DUMP 是 Read、LOAD 是 Write）与响应 Write；DramPool 端 `HixlRole::Client` 主动外连。

> 注：HIXL 是 CANN 提供的**外部**单边传输库（链接 `cann_hixl`，头 `hixl/hixl.h`，有独立版本号与 `.run` 安装包），不是本仓库代码；仓库里没有展开这个缩写。本仓库只包了一层适配器把它收敛到 `transport::Transport` 抽象后面。首轮通读停在 `transport.h` 接口层即可；只有排查建连失败或 RDMA 内存注册问题时才需要下钻 6.8–6.10。

---

### 阶段 7：内存池与基础设施（带着调用点回头读）

**读完这一阶段你应该能回答：** KV 槽与 reply 槽从哪里来？槽索引如何无锁回收？Router 默认什么策略？日志为什么会"丢行"？

| # | 路径 | 行数 | 优先级 | 本文件要提取什么 |
|---|---|---|---|---|
| 7.1 | `ucm/shared/infra/status/status.h` | 120 | 必读 | 通用返回类型：int32 码+message，`Expected<T>`；OK=0、通用 Error=-1、专用码 -50000-i。 |
| 7.2 | `ucm/shared/infra/logger/logger.h` | 71 | 必读 | `UC_DEBUG/INFO/WARN/ERROR` 门面（默认按 file:line 限流），`_UNLIMITED` 变体与 `Setup`。 |
| 7.3 | `ucm/shared/infra/logger/logger.cc` | 62 | 建议 | 薄转发 shim 与字符串 intern，落到 Meyers 单例。 |
| 7.4 | `ucm/shared/infra/logger/cc/spdlog_logger.h` | 114 | 建议 | 双 spdlog logger、512×4 限流表、崩溃刷盘信号处理。 |
| 7.5 | `ucm/shared/infra/logger/cc/spdlog_logger.cc` | 278 | 建议 | 异步控制台+轮转文件构造、无锁限流、`UC_LOGGER_LEVEL` 等环境变量。 |
| 7.6 | `ucm/shared/pool/buffer_region.h` | 50 | 必读 | 单个连续 arena：`shared_ptr<void>` owner + 成对 local/device 地址 + Host/Device/Mapped 类型。 |
| 7.7 | `ucm/shared/pool/buffer_region.cc` | 84 | 建议 | 内存类型映射到 `Trans::Device::MakeBuffer`；不支持的映射返回 Unsupported。 |
| 7.8 | `ucm/shared/infra/thread/index_pool.h` | 94 | 必读 | 无锁 Treiber 自由索引栈；1-based 内部槽、8B 版本化 CAS 指针防 ABA；返回 0-based。 |
| 7.9 | `ucm/shared/pool/buffer_pool.h` | 92 | 必读 | 固定槽池契约：`Slot`（双地址/长度/索引/偏移）、`Init/Allocate/Free`、`ComputeSlotStride`。 |
| 7.10 | `ucm/shared/pool/buffer_pool.cc` | 147 | 必读 | stride 64B 对齐切槽、used 计数、可选 init/free memset；**Free 无双重释放防护**。 |
| 7.11 | `ucm/shared/router/router.h` | 184 | 必读 | `RouterType`/`RouterConfig`、`Router` 抽象、`RouteKeys/RouteKey`、`CreateRouter`。 |
| 7.12 | `ucm/shared/router/router.cc` | 340 | 建议 | 虚拟节点 ring hash（默认）、Maglev、两种批量亲和包装；返回 node→输入位置。 |
| 7.13 | `ucm/shared/infra/thread/lock.h` | 113 | 建议 | `SpinLock`、`RwLock`（shared_mutex 包装）与 RAII guard。 |
| 7.14 | `ucm/shared/infra/thread/thread_pool.h` | 296 | 可选 | 带卡死看门狗的 worker 池；drampool 核心不使用（yuanrong 用），首轮跳过。 |
| 7.15 | `ucm/shared/pool/detail/offset_allocator.h` | 125 | 可选 | vendored 变长分配器接口；DramPool 无调用方。 |
| 7.16 | `ucm/shared/pool/detail/offset_allocator.cc` | 453 | 可选 | sebbbi 实现 + 华为加锁/校验；无需通读。 |
| 7.17 | `ucm/shared/pool/variable_buffer_pool.h` | 126 | 可选 | 变长池，无 DramPool 调用方。 |
| 7.18 | `ucm/shared/pool/variable_buffer_pool.cc` | 170 | 可选 | 按 alignment 单位换算；按需查。 |
| 7.19 | 其余参考件 | — | 可选 | `thread/latch.h`(119)、`thread/cpu_affinity.h`(64)、`template/singleton.h`(46)、`template/timer.h`(90)、`time/now_time.h`(42)、`time/stopwatch.h`(50)、`template/topn_heap.h`(153)、`template/hashset.h`(169)。 |

**关键符号：** `Status/Expected`、`UC::Logger::Setup/Flush`、`UC::BufferRegion/Create`、`UC::IndexPool`、`UC::BufferPool/Slot`、`UC::Router/CreateRouter/RouteKeys`、`UC::SpinLock/RwLock`。

**边界：** **【跨后端】** 内存字节不经 malloc，全部来自 `UC::Trans::Device`（Ascend/CUDA/simu 由构建选择）；这是本阶段唯一真正的平台分叉。

---

### 阶段 8：Python 绑定与集成层

**读完这一阶段你应该能回答：** 引擎配置如何一路选中 native DramStore？为什么没有 DramStore 的 pybind 模块？v0/v1 两代工厂如何区分？

| # | 路径 | 行数 | 优先级 | 本文件要提取什么 |
|---|---|---|---|---|
| 8.1 | `examples/ucm_config_dramstore.yaml` | 61 | 必读 | 客户端配置契约：`UcmPipelineStore` + `store_pipeline: Dram`、端点/超时/router 键、snapshot 路径。 |
| 8.2 | `ucm/utils.py` | 90 | 必读 | `Config`：优先 `UCM_CONFIG_FILE` 指定的 YAML，否则用 extra-config 原文，再否则内置默认。 |
| 8.3 | `ucm/store/ucmstore_v1.py` | 232 | 必读 | v1 抽象契约：block id 为 bytes、per-block/per-tensor 双索引、返回可轮询 Task。 |
| 8.4 | `ucm/store/ucmstore.py` | 204 | 建议 | v0 旧契约（str id、create/commit），背景知识。 |
| 8.5 | `ucm/store/factory_v1.py` | 70 | 必读 | 名字→类注册表（lazy importlib）；`UcmPipelineStore` 是到达 DramStore 的唯一入口。 |
| 8.6 | `ucm/store/factory.py` | 70 | 可选 | v0 旧分发表，与 DramPool 无关。 |
| 8.7 | `ucm/store/pipeline/cpy/pipeline_store.py.cc` | 277 | 必读 | pybind 核心：`Stack` 按 `Make<Name>Store` 符号 dlopen、以 `store_backend` 链接各层、可选 health breaker；仅阻塞 Wait 释放 GIL。 |
| 8.8 | `ucm/store/pipeline/connector.py` | 409 | 必读 | Python 门面：block id/shard/torch 指针的 numpy marshal；`_dram_pipeline_builder` Stack `libdramstore.so` 并启动 resource reporter。 |
| 8.9 | `ucm/store/pipeline/errors.py` | 29 | 建议 | C++ 端原生异常在 Python 侧对应的可捕获类型。 |
| 8.10 | `ucm/store/dram/__init__.py` | 2 | 可选 | 只有 docstring——本身说明 DramStore 是 native 库而非 Python 实现。 |

**关键符号：** `Config/get_config`、`UcmConnectorFactoryV1.create_connector`、`UcmKVStoreBaseV1`、`PipelineStore.Stack`、`UcmPipelineStore`、`UcmPipelineStoreBuilder`、`_dram_pipeline_builder`、`_tensor_normalize`。

**边界：** **【C++/Python】** 唯一 pybind 边界是模块 `ucmpipelinestore`；DramStore 经 dlopen 加载（无 `PYBIND11_MODULE`）；GIL 仅在阻塞 Wait 与 native 指标 merge/drain 处释放，marshal 阶段仍持 GIL。

---

### 阶段 9：指标与可观测性链路（DramPool → 客户端域 → Prometheus → Grafana）

**读完这一阶段你应该能回答：** 一个指标跨越两个进程经过哪些环节？为什么说文件是两域唯一通道？三方手工注册表分别在哪？

| # | 路径 | 行数 | 优先级 | 本文件要提取什么 |
|---|---|---|---|---|
| 9.1 | `ucm/store/dram/cc/drampool/drampool_metrics.h` | 254 | 必读 | 完整指标目录（40 余个 `drampool_*`）、六套 ms 桶、`DrampoolMetricDefs()`、RAII `ScopedTimer`、手工队列长度原子。 |
| 9.2 | `ucm/store/dram/cc/drampool/metrics_reporter.h` | 87 | 必读 | 后台单线程导出器声明：drain → 累积/覆盖/合桶 → 原子重写 JSON。 |
| 9.3 | `ucm/store/dram/cc/drampool/metrics_reporter.cc` | 244 | 必读 | 写侧快照契约：严格 histogram 整形（非累积桶、长度 bounds+1）、非有限值钳制、tmp+rename。 |
| 9.4 | `ucm/shared/metrics/cc/domain/metrics.h` | 197 | 必读 | 指标域：线程本地双缓冲 `MetricBuffer`、`CachedMetric`、`registerEpoch_`。 |
| 9.5 | `ucm/shared/metrics/cc/domain/metrics.cc` | 272 | 必读 | 注册（first-name-wins、+Inf 归一）、按线程累积、严格 histogram 导入、`GetAllStatsAndClear` 全局 drain。 |
| 9.6 | `ucm/shared/metrics/cc/api/metrics_api.h` | 55 | 必读 | 门面与 `NAME_TO_METRIC_ID`（函数局部 static，靠 epoch 自愈）。 |
| 9.7 | `ucm/shared/metrics/cc/api/metrics_api.cc` | 62 | 建议 | 到单例的一行转发 shim。 |
| 9.8 | `ucm/shared/metrics/cpy/metrics.py.cc` | 84 | 建议 | `ucmmetrics` pybind：histogram 转 (bucket_counts, sum)、拒 bool/float、drain/merge 放 GIL。 |
| 9.9 | `ucm/shared/metrics/resource_reporter.py` | 206 | 必读 | 通用文件轮询框架：flock 选主、64KiB 倒序扫描、reset 检测、基线持久化。 |
| 9.10 | `ucm/store/dram/resource_reporter.py` | 231 | 必读 | DramPool 快照校验/差分（含 histogram reset 推断）与导入本进程域；`start_drampool_resource_reporter` 进程单例。 |
| 9.11 | `examples/metrics/metrics_configs.yaml` | 789 | 建议 | 指标定义声明源（含 42 条 `drampool_*`）；消费方默认只开 `vllm_connector`。 |
| 9.12 | `ucm/default_metrics_config.py` | 1298 | 可选 | YAML 的手工进程内镜像，测试强制其与 YAML 一致。 |
| 9.13 | `ucm/metrics_config.py` | 254 | 必读 | 原始配置 → `MetricDefinition`，Python 侧注册循环，multiproc 前缀与 vllm 改名/缩放。 |
| 9.14 | `ucm/metrics_dispatcher.py` | 150 | 必读 | 进程级唯一 drainer：一次 drain 扇出到 multiproc / vllm_connector 两个消费缓冲。 |
| 9.15 | `ucm/observability.py` | 218 | 必读 | prometheus_client multiproc 桥：注册对象、周期线程把消费缓冲写入 multiproc 文件。 |
| 9.16 | `ucm/shared/metrics/__init__.py` | 13 | 建议 | 以 RTLD_GLOBAL 预加载 libmetrics.so，保证后加载的库符号可解析。 |
| 9.17 | `ucm/integration/vllm/ucm_connector.py` | 3735 | 可选 | 只读 `_setup_ucm_metrics` 与 `get_kv_connector_stats` 相关段落，勿通读。 |

**关键符号：** `SetupDrampoolMetrics`、`DrampoolMetricDef/DrampoolMetricDefs`、`ScopedTimer`、`MetricsReporter::CollectAndWrite/Render/WriteFile`、`UC::Metrics`、`CreateStats/UpdateStats/GetAllStatsAndClear/MergeHistogramStats`、`MetricBuffer/SwitchBuffer`、`NAME_TO_METRIC_ID`、`FileResourceMetricsReporter`、`DramPoolResourceReporter`、`parse_drampool_resource_snapshot/snapshot_deltas`、`MetricsDispatcher.drain_to_consumers`、`PrometheusStatsLogger`。

**边界：**

```
【跨进程·文件】守护进程 UC::Metrics 域 ── drampool_metrics.json（单行、尾换行、tmp+rename）──▶ 客户端进程的 UC::Metrics 域；两域间无共享内存/socket
【跨线程】客户端 reporter 线程（flock 每主机选出一个）导入本进程域；dispatcher 为唯一 drainer
【跨网络】仅最后一跳：Prometheus 拉取 /metrics（生产为 vLLM endpoint）→ Grafana 查询
【标签】C++ 侧无任何 label；model_name/worker_id 只在 Python prometheus_client 侧出现
```

> 设计意图与取舍留痕见 `dramPool_documents/metrics_design.md`（32 指标全集、六次修改记录、被裁指标与恢复方式）。

---

### 阶段 10：测试、文档与部署产物

**读完这一阶段你应该能回答：** 测试如何组织、每个套件钉住什么、真正的配置文档在哪里、部署 chart 是否覆盖 DramPool。

| # | 路径 | 行数 | 优先级 | 本文件要提取什么 |
|---|---|---|---|---|
| 10.1 | `ucm/store/test/CMakeLists.txt` | 43 | 必读 | `GLOB_RECURSE` 把全部 `case/**/*.cc` 编进**单一** `ucmstore.test`，链接 dramstore + drampool_core。 |
| 10.2 | `ucm/store/test/case/dram/dram_test_common.h` | 77 | 必读 | 测试公共件：`MakeEntry`、`KeyFromHex`、微型 `MakeBufferManager`。 |
| 10.3 | `ucm/store/test/case/dram/drampool/dram_metadata_test.cc` | 427 | 必读 | 元数据状态机与 1024 分片管理的可执行规格。 |
| 10.4 | `…/drampool/dram_ttl_eviction_policy_test.cc` | 148 | 必读 | TTL：过期 READY 被选中；非 READY/被 pin/lease 未过者存活。 |
| 10.5 | `…/drampool/dram_pos_eviction_policy_test.cc` | 197 | 必读 | POS 排序（position 降序、lifeTimeout 平局）与同样的资格闸。 |
| 10.6 | `…/drampool/dram_lru_eviction_policy_test.cc` | 268 | 建议 | 休眠中 LRU 的完整规格，作接口参考。 |
| 10.7 | `…/drampool/task_worker_test.cc` | 369 | 必读 | prepare 半程最佳规格；**注意本分支当前无法编译**（见第 5 节）。 |
| 10.8 | `…/drampool/completion_poller_test.cc` | 502 | 必读 | settle 半程最佳规格：窗口深度、元数据 settle、超时边界、flag 槽生命周期。 |
| 10.9 | `…/drampool/drampool_yaml_config_test.cc` | 294 | 必读 | 全字段映射、策略大小写、19 组非法值矩阵与失败事务性。 |
| 10.10 | `…/drampool/drampool_startup_test.cc` | 731 | 必读 | CLI/槽数计算、服务端状态机拒绝、daemon 退出码；运行时集成用例被宏编译掉。 |
| 10.11 | `…/drampool/health_server_test.cc` | 250 | 建议 | 健康端点逐字节规格：分片、其他路径拒绝、超时、端口占用。 |
| 10.12 | `…/drampool/drampool_metrics_test.cc` | 260 | 建议 | 注册面与三类指标行为；静态名表落后于完整 defs 表。 |
| 10.13 | `ucm/store/test/case/dram/dram_task_manager_test.cc` | 822 | 建议 | 客户端任务并发/不阻塞/句柄一次性/关停语义。 |
| 10.14 | `…/dram/dram_node_scheduler_test.cc` | 789 | 建议 | NodeActor 事件状态机、重连排队、Runner 并发规则。 |
| 10.15 | `…/dram/dram_reply_service_test.cc` | 182 | 建议 | reply slot 租约/校验释放/重用/并发。 |
| 10.16 | `…/dram/dram_reply_service_polling_test.cc` | 255 | 建议 | 轮询观测路径规格。 |
| 10.17 | `…/dram/dram_config_test.cc` | 296 | 建议 | 客户端配置解析与校验。 |
| 10.18 | `…/dram/dram_construction_dependencies_test.cc` | 44 | 建议 | `MakeDramStore` 冒烟：Setup 拿到非法 Dictionary 仍可用。 |
| 10.19 | `…/dram/dram_prerequisite_test.cc` | 300 | 可选 | DUMP 前置事件门控。 |
| 10.20 | `…/dram/dram_transport_backend_test.cc` | 212 | 可选 | 传输后端行为。 |
| 10.21 | `…/dram/dram_bounded_queue_test.cc` | 53 | 可选 | BoundedQueue 语义。 |
| 10.22 | `…/dram/dram_metrics_test.cc` | 359 | 可选 | 客户端侧指标行为。 |
| 10.23 | `test/test_drampool_resource_reporter.py` | 447 | 建议 | Python 桥最佳规格；不在默认 testpaths，需按文件路径显式运行。 |
| 10.24 | `docs/docs-site/docs/en/developer-guide/architecture.md` | 42 | 必读 | 通用 UCM 架构/工厂/流水线定位，无 DramPool 专页。 |
| 10.25 | `docs/docs-site/docs/en/developer-guide/request-lifecycle.md` | 52 | 必读 | vLLM 前缀缓存请求全流程叙事，与本系统流水线最接近的 prose 对照。 |
| 10.26 | `docs/docs-site/docs/en/developer-guide/add-metrics.md` | 117 | 建议 | 指标定义/更新/配置/Grafana 验证指南，配阶段 9 读。 |
| 10.27 | `docs/docs-site/docs/en/developer-guide/cache-configuration/index.md` | 33 | 可选 | 后端选择索引；其中无 DramPool/DramStore，证实无专门文档页。 |
| 10.28 | `PR1394_DRAMPOOL_DRAMSTORE_GRAFANA_E2E_HANDOFF.md` | 277 | 建议 | 中文 E2E 验证日志，自标 superseded；快照契约描述仍准确（见第 5 节）。 |
| 10.29 | `charts/unified-cache-chart/` | — | 可选 | Helm chart 全文搜 "dram" 无命中，不部署本子系统。 |

---

## 3. 五条主线（The five end-to-end paths）

> 日后按场景回查时直接走对应链条。标记：**[跨线程]** **[TCP]** **[RDMA]** **[跨进程·文件]**。每条末尾给出该场景的最小阅读集。

### 主线 1：守护进程启动（main → 开始服务）

1. `drampool/main.cc::main` — 栈构造 daemon，返回 `Run` 退出码。
2. `drampool_daemon.cc::DramPoolDaemon::Run` — 编排主干（CLI→YAML→指标→日志→信号→组件）。
3. `drampool_launch_config.cc::ParseCommandLine` — 重置并填充 g_config 的身份/容量字段，强制四项必填。
4. `drampool_launch_config.cc::CalculatePoolSlotCounts` — GiB×比例 → 每档 64B 对齐槽数。
5. `drampool_yaml_config.cc::ParseYamlConfig` — 手写解析，全部成功才提交（失败不污染 g_config）。
6. `drampool_yaml_config.cc::ValidateRequiredRuntimeConfigKeys` → `ValidateRuntimeConfig` — 20 必填键 + 跨字段校验（含 `flagBufferSlotCount >= pending_depth`）。
7. `drampool_metrics.h::SetupDrampoolMetrics` — `SetUp` 后按 defs 表逐个 `CreateStats`。
8. `drampool_daemon.cc::SetupLogger` — 异步 spdlog；级别来自 `UC_LOGGER_LEVEL`。
9. `drampool_daemon.cc::SetupSignals` — SIGINT/SIGTERM → 只置 atomic。
10. `drampool_server.cc::DramPoolServer::Init` — ScopeExit 回滚保护，顺序执行 8 个 Initialize*。
11. `drampool_server.cc::InitializeDeviceRuntime` — 随机选 device；aclInit（DuplicateKey 视为借用运行时）。
12. `drampool_server.cc::InitMemoryPool` — `BufferManager`：每档一个 `BufferPool`，经 `BufferRegion/trans` 拿内存。
13. `drampool_server.cc::InitFlagBufferPool` — 独立小池，仅供打包响应。
14. `drampool_server.cc::InitMetadata` — 1024 分片，每片 TTL+POSITION 双策略。
15. `drampool_server.cc::InitProtocol` — `ProtocolManager`（同 TU 两端共编）。
16. `drampool_server.cc::InitQueues` — 两个 SPSC 环（65536 实容 65535）+ capacity gauge。
17. `drampool_server.cc::InitTransportManager` — 由本机 two_sided 在路由表反查 one_sided ManagerID。
18. `drampool_server.cc::CreateRuntimeContext` — **非拥有式** `DramPoolRuntime` 裸引用束。
19. `drampool_server.cc::Start` — ScopeExit 失败即 Stop。
20. **[跨线程]** `StartCompletionPoller` → `StartTaskWorker` → `StartGCThread` → `StartRequestReceiver`（receiver 先在 `WaitForChannelReady` 上阻塞）。
21. `StartTransportService` — **[RDMA]** `HixlRole::Client` 安装、`RegisterBufferPools` 注册 KV 池与 flag 池、`Init` 接受 metadata exchange。
22. `StartTcpMessageChannel` — **[TCP]** 在 `--addr` bind/listen + epoll io 线程，置 ready 唤醒 receiver；state=Running。
23. `drampool_daemon.cc::healthServer.Start` — `healthPort==0` 则跳过（默认）。
24. `drampool_daemon.cc::metricsReporter.Start` — `metricsEnabled=false` 则跳过；否则 worker 开始写 JSON（**[跨进程·文件]** 契约生效）。
25. 就绪日志 `"DramPool service ready"` → `WaitForShutdown`（主线程 100ms 轮询）。
26. 信号后逆序：`metricsReporter.Stop` → `healthServer.Stop` → `server.Stop`（ingress→receiver→worker→poller→GC→传输→析构）→ `Logger::Flush`。

**最小阅读集：** `main.cc`、`drampool_daemon.{h,cc}`、`drampool_config.h`、`drampool_launch_config.cc`、`drampool_yaml_config.cc`、`drampool_server.{h,cc}`、`examples/drampool.yaml`。

---

### 主线 2：KV 写入 DUMP（引擎 → 池 → RDMA 响应 → 调用方）

1. `pipeline/connector.py::UcmPipelineStore.dump/dump_data` — join block id、pack shard index、归一化 torch 指针（`dump_data` 带 prerequisite）。
2. **[pybind]** `pipeline_store.py.cc::PipelineStore::Dump/MakeTaskDesc` — numpy → `Detail::TaskDesc`，调 `StoreBack()->Dump`。
3. `dram_store.cc::DramStore::Dump → SubmitTransfer` — 空任务拒绝，转 TaskManager。
4. `task_manager.cc::EnqueueTask` — promise/future、deadline=now+dump timeout、Submission 入队；**[跨线程]** 调用线程在此返回 TaskHandle。
5. `task_manager.cc::Run → ProcessSubmission` — completion 优先；过期即 Timeout；读 prerequisite。
6. `task_manager.cc::NormalizeTransfer/BuildRequests` — 每 Shard 展开 per-tensor IoEntry；`RouteKeys` 分组并切批 ≤65535。
7. **[跨线程]** `node_scheduler.cc::NodeScheduler.Post` — Request 入 node 所属 Runner。
8. `node_actor.cc::NodeActor::Handle(Request)` — 入 pendingRequests_（已过期立即 Timeout）。
9. `node_actor.cc::Advance/DispatchPendingRequests/TryStartRequest` — ACTIVE 且未超 inflight；DUMP 前置 ACL event 经 `Event::Query` 门控。
10. `reply_service.cc::ReplyService.Acquire` — 分配 DeviceMappedHost slot 并记 Lease；其地址即 `resp_addr`。
11. `node_actor.cc::EncodeRequest`（DUMP）— **wire ttl 硬编码 0**；`StorageKey` 仅在 shardId≠0 时折叠前 4 字节；`PackRequest` 出 23B 头 + 32B 条目。
12. **[跨线程]** `transport_executor.cc::TryPost` — node 哈希固定 worker。
13. `transport_manager_backend.cc::Transmit` — **[TCP]** 只把编码请求字节发往 two_sided 控制端口；KV 不走 TCP。
14. （前置条件）`transport_manager_backend.cc::Connect` = `ExchangeMetadata + manager_.Connect` — **[RDMA]** 数据平面此前已建立。
15. `drampool_server.cc::RequestReceiveLoop::Receive` — **[TCP]** receiver 阻塞收齐请求字节。
16. `kv_protocol.cc::KvDumpProtocol.UnpackRequest` — 校验 23B 头与总长 == 头+batch*32。
17. `drampool_server.cc` twoSided→oneSided 静态查表 — 构造 `RequestTask{request, peer}`。
18. **[跨线程]** `requestQueue_.TryPush` — 每次重试前重盖 `enqueue_us`；满则记 full 一次并 sleep 重试。
19. `task_worker.cc::Run/ProcessDump` — pop、报 residence；`ttl=?:defaultDumpTtlMs`、盖 lifeTimeout；构造 Read/RemoteDeviceHost operation。
20. `metadata.cc::MetadataManager.StoreBegin → BufferManager.Allocate`。
21. `metadata.cc` NoSpace → `EvictOneShard(随机分片)` 周期 TTL → 仍 NoSpace → 深度 POSITION → 再试。
22. `metadata.cc::ShardMetadata.StoreBegin` — 写锁、拒重复（DuplicateKey）、双策略 AddKey、插入 INITIALIZED Entry。
23. `task_worker.cc` DuplicateKey 视为 Ok 跳过（幂等重 dump）；成功项追加 `Segment{池槽地址 ← entry.addr}`。
24. `task_worker.cc::transport.ExecuteAsync`（一次、不等）— **[RDMA]** HIXL Read 把客户端设备数据拉入池槽。
25. **[跨线程]** `SubmitCompletion` — TryPush；completion 环满则记 full 并退化到阻塞 Push。
26. `completion_poller.cc::FillPendingWindow` — 出环入 deque，窗口 ≤ pendingDepth。
27. `completion_poller.cc::PollDataTransfer → SettleDataTransfer` — Waiting 仅诊断超时；终态 Completed 调 `StoreEnd`（INITIALIZED→READY），非 Completed 调 `Delete`；写 results。
28. `completion_poller.cc::SubmitResponse` — flag 槽 Allocate（NoSpace 则下轮 parked 重试）、`PackResponse`（位打包 + 翻 doorbell=Ready）、**[RDMA]** Write 回 `resp_addr`。
29. `completion_poller.cc::PollResponseTransfer/SettleResponseTransfer` — 报 e2e（enqueue_us 归零）、释放 flag 槽、记 response_rtt。
30. 回到客户端 `reply_service.cc::Run → CompletionReady` — volatile 读 doorbell + request_id 校验 + acquire fence。
31. `reply_service.cc::DecodeReply` — code==0 为成功；发一个 `ReplyObserved`（**[跨线程]** 回 Runner）。
32. `node_actor.cc::Handle(ReplyObserved)` — token 校验、非零码=整请求 Error；`FinalizeRequests` 释放 slot、发 `RequestCompleted`。
33. `task_manager.cc::Publish → CompleteRequest` — 最后一个 per-node 请求完成时兑现 promise。
34. `dram_store.cc::Wait` — 调用方 Wait 返回；同一 handle 二次 Wait 得 NotFound。

**最小阅读集：** `connector.py`（dump 段）、`dram_store.cc`、`task_manager.cc`、`node_actor.cc`、`reply_service.cc`、`transport_manager_backend.cc`、服务端 `drampool_server.cc`、`task_worker.cc`、`completion_poller.cc`、`metadata.cc`、`kv_protocol.{h,cc}`。

---

### 主线 3：KV 读取 LOAD（请求 → 命中/未命中 → RDMA 写回 NPU → 完成）

1. `dram_store.cc::DramStore.Load → SubmitTransfer(LOAD)` — 立即返回 handle。
2. `task_manager.cc::EnqueueTask` — deadline 取 load timeout；**[跨线程]** 入队。
3. `task_manager.cc::NormalizeTransfer` — shardId = shard.index*tensorCount+tensorIndex；强制 maxIoEntries。
4. `task_manager.cc::BuildRequests` — `RouteKeys`（ring-hash 默认）+ 切批；**[跨线程]** `submitRequest`。
5. `node_scheduler.cc::Post` → `node_actor.cc::Handle(Request)` — 入 pending。
6. `node_actor.cc::StartRequest` — 构 RequestToken、`ReplyService.Acquire` 取 slot。
7. `node_actor.cc::EncodeRequest`（LOAD）— `resp_addr=slot.localAddr`；条目 addr=客户端 NPU 目的地址、idx=shardId。
8. **[跨线程]** `transport_executor.cc::TryPost` → `transport_manager_backend.cc::Transmit` — **[TCP]** 仅发请求头/条目。
9. `node_actor.cc::Handle(TransmitCompleted)` — TRANSMITTING→INFLIGHT。
10. `drampool_server.cc::RequestReceiveLoop` 收包 → `KvLoadProtocol.UnpackRequest`（19B 头 + 总长校验）。
11. peer 查表 + **[跨线程]** TryPush 入 RequestQueue。
12. `task_worker.cc::ProcessLoad` — **命中分支点**：`LoadBegin`（读锁找 Entry + `TryIncRef` pin READY）；key 缺失/非 READY = MISS；`len > size` 也 MISS（先 LoadEnd）。
13. `task_worker.cc` 构造 Write operation：`Segment{metadataEntry.buffer.addr（主机池槽） → entry.addr（客户端 NPU）}`，`ExecuteAsync` 一次 — **[RDMA]** KV 字节在此移动。
14. **[跨线程]** 全 MISS → `QueueResponse`（直接 SubmitResponse）；否则 `SubmitCompletion`（满则阻塞 Push）。
15. `completion_poller.cc::FillPendingWindow → PollDataTransfer` — Waiting 保持 pending。
16. `SettleDataTransfer`（LOAD）— `LoadEnd` 解 pin；Completed→Ok，否则 Failed；推进到 SubmitResponse。
17. `SubmitResponse` — flag 槽 + PackResponse + **[RDMA]** 响应 Write 到 reply slot。
18. `PollResponseTransfer/SettleResponseTransfer` — e2e、释放 flag 槽。
19. 客户端 `reply_service.cc` 轮询到 Ready → `DecodeReply`（code==0 成功）→ `ReplyObserved`。
20. `node_actor.cc` 校验、聚合 item 失败 → RetireRequest（还 slot）。
21. `task_manager.cc::CompleteRequest` — promise 兑现，调用方 Wait 返回。

**最小阅读集：** `dram_store.cc`、`task_manager.cc`、`node_actor.{h,cc}`、`reply_service.cc`、`transport_executor.cc`、`transport_manager_backend.cc`、服务端 `task_worker.cc`（ProcessLoad）、`completion_poller.cc`、`metadata.h`（LoadBegin/LoadEnd 语义）、`kv_protocol.h`。

**注意：** MISS 不重试、不改路由、无降级读；任一 item 非零码即整个任务 Error。

---

### 主线 4：驱逐与容量管理

1. `drampool_config.h::DramPool` 默认：periodic=TTL、deep=POSITION、lease=5000ms、ratio=0.0、gc=1000ms。
2. `drampool_server.cc::InitMemoryPool/InitMetadata` — 每档一个固定池；1024 分片各建双策略。
3. **触发 B** `drampool_server.cc::GCThreadLoop` — 每 gcIntervalMs（可中断等待）刷 entry_count/usage_ratio gauge 后 `PerformEvict`。
4. `metadata.h::MetadataManager.PerformEvict` — 顺序遍历全部 1024 分片。
5. `metadata.cc::EvictOneShard` — `EvictPeriodic` 取受害者 → 锁外 `BufferManager.Free` → `ShardMetadata.Delete`。
6. `metadata.cc::ShardMetadata.EvictPeriodic/EvictDeep` — 读锁委托策略。
7. `ttl_eviction_policy.h::GetEvictionResults` — lifeTimeout 升序、遇未来即停、忽略 ratio；候选逐个 `TryMarkEvicting`（lease 才是闸门）。
8. `pos_eviction_policy.h::GetEvictionResults` — position 降序；ratio==0.0 直接返回；目标 max(1, ratio*n)。
9. `entry.h::TryMarkEvicting` — READY + refCnt==0 + leaseTimeout≤now 三条件，是选择与收割间的安全边界。
10. `buffer_manager.h::Free` — 按精确尺寸定位全局池；任意分片的受害者都释放同一尺寸池 → 随机分片驱逐因此正确。
11. `buffer_pool.cc::Free` — 可选清零后 `IndexPool.Release`；无双重释放防护。
12. `metadata.cc::ShardMetadata.Delete` — 写锁，从双策略与 map 擦除；EntryPtr 在三方引用全部释放后才析构。
13. **触发 A** `task_worker.cc::ProcessDump → MetadataManager.StoreBegin`。
14. `metadata.cc::StoreBegin` — Allocate NoSpace：随机分片周期驱逐→重试→随机分片深度驱逐→重试→仍失败则记 `dump_nospace_failures_total` 并返回 Error。
15. `metadata.cc::ShardMetadata.StoreBegin` — 写锁插入 INITIALIZED（不可驱逐）。
16. LOAD 交互：`task_worker.cc::ProcessLoad` 的 `LoadBegin/TryIncRef` 让 GC 在 RDMA 在途期间无法回收。
17. LOOKUP 交互：`ProcessLookup → Exist → TryMarkHit` 续 lease（有状态副作用，非纯谓词）。
18. **[跨线程]** `completion_poller.cc::SettleDataTransfer` — DUMP Completed→`StoreEnd`（可驱逐）；非 Completed→`Delete`；LOAD→`LoadEnd`。
19. `drampool_server.cc::Stop` 顺序保证 worker 完成 StoreBegin（含同步驱逐）、poller 完成 settle 之后，metadata/池才被析构。

**最小阅读集：** `drampool_config.h`、`entry.h`、`buffer.h`、`eviction_policy.h`、`ttl/pos_eviction_policy.h`、`buffer_manager.h`、`metadata.{h,cc}`、`drampool_server.cc`（GC loop/Stop）、`task_worker.cc`、`completion_poller.cc`。

---

### 主线 5：指标发射到 Grafana（PR1394 链路）

1. `drampool_metrics.h::DrampoolMetricDefs` — C++ 指标目录（40 余项，桶手工镜像自 YAML）。
2. `drampool_metrics.h::SetupDrampoolMetrics` — daemon 端一次性注册（first-name-wins）。
3. `metrics.cc domain::CreateStats` — +Inf 桶归一、bump `registerEpoch_`。
4. `drampool_daemon.cc::Run` — 指标注册早于日志/信号/服务端；`MetricsReporter.Start` 晚于 server+health。
5. `drampool_server.cc::RequestReceiveLoop` — enqueue-wait 计时、full 一次性、`g_requestQueueLen`。
6. `task_worker.cc::Run` — 每轮重写 capacity gauge；pop 时刷新 size/驻留。
7. `task_worker.cc::ProcessOneRequest` — 每请求恰好一个量计数器。
8. `task_worker.cc::Process*` — prepare/metadata/submit 计时（timer 默认 disarm，成功路径才 Arm）；miss 计数。
9. `task_worker.cc::SubmitCompletion` — completion full 计数 + `g_completionQueueLen`。
10. `completion_poller.cc::FillPendingWindow` — 出环减计数、刷 size。
11. `completion_poller.cc::PollDataTransfer` — get_status 计时；Waiting 超时仅诊断。
12. `metadata.cc` — StoreEnd/LoadEnd 计时；`dump_nospace` 只在两轮驱逐仍失败后计。
13. `completion_poller.cc::ReportBatchMetrics` — 一次性哨兵保护，靠数 results 向量得 failed/miss。
14. `completion_poller.cc::SubmitResponse` — response retry 是 parked 非失败；永久失败才计 response_failures。
15. `completion_poller.cc::ReportBatchEndToEnd/SettleResponseTransfer` — enqueue_us 哨兵归零；inflight/flag gauge。
16. `drampool_server.cc::GCThreadLoop` — entry_count、usage_ratio（槽位口径）、GC 计时。
17. `metrics_api.h::NAME_TO_METRIC_ID + UpdateStats(CachedMetric&,...)` — epoch 机制让先于注册的调用点事后自愈，勿"简化"。
18. `metrics_reporter.cc::Start/Run` — 门控关则无线程；否则定输出路径起 worker。
19. `metrics_reporter.cc::CollectAndWrite` — `GetAllStatsAndClear`（双缓冲切换+等 writer+折叠+清零），counter 累加/gauge 覆盖/histogram 合桶。
20. `metrics_reporter.cc::Render/RenderHistogram/WriteFile` — 单行 JSON、非累积桶长度 bounds+1、tmp+rename（**[跨进程·文件]**）。
21. `connector.py::_dram_pipeline_builder` — Stack 之后启动 reporter。
22. `dram/resource_reporter.py::start_drampool_resource_reporter` — 仅 scheduler 侧（device_id<0）；进程单例。
23. `shared/metrics/resource_reporter.py::_try_become_leader` — /dev/shm flock，每主机每文件一个 reporter。
24. `_read_latest_complete_line` — 64KiB 倒序扫，无尾换行的最后一行视为撕裂丢弃；1MiB 上限。
25. `dram/resource_reporter.py::parse_snapshot/snapshot_deltas` — 只收 `drampool_*`、丢弃 `drampool_resource_*`；下降即 reset；桶零而 sum 变则抛错（下轮自愈）。
26. `DramPoolResourceReporter._report_snapshot` — 先 merge/update 进**本进程域**，成功后才写基线。
27. `metrics.py.cc` ucmmetrics — 严格 histogram 校验，重操作放 GIL。
28. `metrics_config.py` — MetricDefinition 与 Python 注册循环。
29. `metrics_dispatcher.py::drain_to_consumers` — 唯一 drain，扇出两消费方。
30. `observability.py::PrometheusStatsLogger` — 注册带 label 的 prom 对象，周期线程写 multiproc 文件（`ucm:` → 实际序列名 `ucm_`）。
31. `vllm/ucm_connector.py` — 生产启动 Prometheus 腿；或默认把样本送 vllm_connector 消费方。
32. **[网络]** Prometheus scrape /metrics → Grafana PromQL（用下划线名）。

**最小阅读集：** `drampool_metrics.h`、`metrics_reporter.{h,cc}`、`ucm/shared/metrics/cc/domain/metrics.{h,cc}`、`metrics_api.h`、两个 `resource_reporter.py`、`metrics_dispatcher.py`、`observability.py`、`metrics_config.py`、`examples/metrics/metrics_configs.yaml`（drampool 段与 consumers）。

---

## 4. 易错点与陷阱（Gotchas）

### 4.1 误导性命名——名字说的和代码做的不是一回事

| 陷阱 | 真相 | 读什么时注意 |
|---|---|---|
| `messages.h` | **不含任何线上消息**。它是客户端进程内 actor 的命令/事件结构（`namespace UC::Dram`）。线上消息是 `kv_protocol.h` 里的 `KvDumpRequest/KvLoadRequest/KvLookupRequest`（`namespace UC::DramPool`）。 | 找线格式时别打开它 |
| `bounded_queue.h` | **服务端热路径完全不用它**——没有任何 drampool 服务端文件 include 它。热路径用 `UC::SpscRingQueue`（无锁）。`BoundedQueue` 是 mutex 环，只被客户端 `task_manager.h` / `transport_executor.h` 使用。 | 阶段 3 别顺着它追请求流 |
| `two_sided` / `one_sided` | 方向与直觉相反：`two_sided` = **TCP 控制/请求通道**（必须含本机 `--addr`）；`one_sided` = **RDMA/HIXL 数据端**。客户端 YAML 里 `local_control_endpoint=two_sided`、`local_transport_manager_id=one_sided`；同一地址不得同时出现在两侧（校验会拒）。 | `drampool_yaml_config.cc`、两份 example YAML |
| `Transmit` | 只把**编码后的请求头/条目**经 TCP 发到对端控制端口。KV 负载字节根本不走这条路——它们由 HIXL RDMA 在客户端已注册的设备内存与池之间直传。 | `transport_manager_backend.cc` |
| **DramPool 是 Client** | 网络角色与名字相反：`drampool` 守护进程以 `HixlRole::Client` 安装并**主动外连**；DramStore 才是 listener/server。Client 实例用 host-only engine ID，**不打开 HIXL 监听端口**（`instance.port = -1`）。 | `StartTransportService` |
| `POS` 驱逐 | 不是"缓存中的位置"，而是**推理请求的 token position**（`KvDumpEntry.idx` → `Entry.position`）。按 position **降序**（越靠后的 token 越先被驱逐），平局按 lifeTimeout 升序。`AccessKey` 有意 no-op，所以顺序在插入时即固定。 | `pos_eviction_policy.h` |
| `DUMP` / `LOAD` 方向 | 站在**客户端 KV-cache 视角**：DUMP=推进池、LOAD=拉回、LOOKUP=探测。在服务端代码里读起来像 receive/send，容易反向理解。服务端 DUMP 用 `Opcode::Read`（从远端 NPU 读入池），LOAD 用 `Opcode::Write`（写到远端 NPU）。 | `task_worker.cc` |
| 两个 `TaskManager` | `UC::TaskManager`（`ucm/store/detail/task/task_manager.h`，旧 CCStore 世界）与 `UC::Dram::TaskManager`（dram 目录）同名不同物。DramStore **只用后者**；`#include "task_manager.h"` 按 include 路径解析到不同文件。 | 阶段 5 |
| `factory_v1.py` 注册名 | `'UcmNfsStore'` 实际加载的是 `pcstore_connector_v1.UcmPcStoreV1`。**不要从注册名推断后端**。 | 阶段 8 |
| `health` 端点 | **不是 readiness**。响应是常量 200，对服务端状态/队列深度/设备健康**零检查**。真正的就绪信号是 `server.Start()` 成功后才启动它。 | `health_server.cc` |
| `metadata.evict_period_ms` | 被解析、被校验，但**从不驱动调度**。GC 线程用的是 `gc.interval_ms`（默认 1000ms）。找驱逐节拍请跟 `gcIntervalMs`。 | `drampool_server.cc::GCThreadLoop` |
| `drampool_resource_*` | 由 Python 导入框架发出，C++ 守护进程从不发；而解析器又**显式跳过** `drampool_resource_` 前缀。它存在于 YAML/default config 却缺席 `DrampoolMetricDefs`——这个不对称是有意的，看着像漏了指标。 | 阶段 9 |

### 4.2 所有权与生命周期——最容易写出悬垂引用的地方

- **`DramPoolRuntime` 拥有零个对象**（`drampool_types.h`）。它是一束裸引用，`DramPoolServer` 拥有全部被引用组件。任何持有 runtime 的东西都不得活过 server 的 pools/managers。
- **teardown 顺序是负载关键的**：ingress（TCP channel）先关 → receiver 退出 → worker 排空已接受任务 → poller 排空。`ResetInitializedComponents` 先析构 worker 与非拥有式 `runtime_`，**再**析构 managers/pools。颠倒任一处都是悬垂引用。
- **`Init()` 失败与 `Stop()` 共用同一条清理路径**（`MakeScopeExit` 回滚）。部分初始化后关停是**预期路径，不是异常路径**——读 `Stop()` 时要能接受"从未 Running 过"的状态。
- **设备运行时可能是借来的**：`device_.Init` 返回 `DuplicateKey` 视为借用，只有 `deviceRuntimeOwned_` 为真才 `Finalize`。
- **`SubmitCompletion` 的兜底是无界阻塞**：completion 环满时 TaskWorker 调 `SpscRingQueue::Push` 自旋 yield 直到 poller 腾出空间。这只在 `Stop()` 先 join worker 再停 poller 的前提下安全——改 teardown 顺序就会挂死。
- **Entry 被三方共享持有**：`metadata_` map + **两个** evictor 各持一个 `EntryPtr`，`GetEvictionResults` 还会再发副本。受害者要活到 `Delete()` 把它从全部三处摘掉。`ShardMetadata::StoreBegin` 在 deepEvictor AddKey 失败时会回滚 periodicEvictor 的 AddKey。
- **驱逐是"标记—收割"两段式，中间有释放窗口**：`GetEvictionResults` 只持分片**读锁**就把受害者 READY→DELETING；`EvictOneShard` 在读锁**释放之后**才 `BufferManager.Free` + `ShardMetadata.Delete`（写锁）。**驱逐器自己从不擦除自己的条目**——`Delete()` 不跑，受害者就永远留在 multiset/索引里。
- **`Delete` 也是两段式且容错**：`TryDelete`（读锁，要求 refCnt==0）→ `bufferManager_.Free` → `ShardMetadata::Delete`（写锁）。若 Free 失败，Delete 照跑，槽**只是泄漏**（记 UC_ERROR）。这正是 refCnt 要把住标记关的原因。
- **客户端 `Wait()` 会先擦除任务项再阻塞**，所以同一 `TaskHandle` 二次 `Wait`/`Check` 得 `NotFound`。
- **reply slot 故意没有 generation counter**：`NodeActor` 只在 reply-observed、确定未发出、或 epoch fence 完成后才释放，复用安全性由构造保证。

### 4.3 双份编译与协议兼容

- **`kv_protocol.cc` 被编译两次**：进 `dramstore` SHARED（CMakeLists 第 10 行）和 `drampool_core` STATIC（第 70 行），测试二进制里还有第三份。同时链接两者的进程会携带两份独立编译副本，跨链接单元的重复符号是**预期行为，不是 bug**。
- 契约成立的唯一理由是**两端编译同一份 TU**。任何编译 flag / header 分歧都会**静默破坏**客户端-服务端互操作。
- **协议无版本号、无 magic、无头长字段**。兼容性只靠一个 `static_assert(BlockId==16B)`；其余每一处改动都是静默线格式破裂，需要客户端+服务端协同重建。
- **字节序隐式小端**：整数用宿主值裸 memcpy 序列化（`PackHeader`/`PackDumpLoadEntry`），测试独立断言 LE。这只在 x86 客户端与 Ascend（ARM LE）服务端都小端时安全——**没有 byteswap 层**。
- **`result_count` 不编码进响应**：客户端必须把期望的 u16 数量回传给每次 `UnpackResponse`/`GetPackedResponseSize`。数量错了会误解位域。批大小仅受 u16 约束（`kMaxProtocolBatchEntries=65535`）。
- **opcode 0（LOOKUP）与全零内存冲突**，所以全零 16B key 在每条路径上都被拒，且 `request_id`/`resp_addr`/`batch_size` 必须全非零——零填充缓冲区绝不能看起来像合法请求。
- **DUMP 头 23B，LOAD/LOOKUP 头 19B**（DUMP 多一个 u32 ttl@17，把 batch_size 挤到 @21）。三者复用同一常量是 bug 磁铁；代码正确地把 `kDumpBatchSizeOffset` 分开。
- **`GetPackedRequestSize` 在类型与 opcode 不匹配时返回 0**（dynamic_cast 失败）而非报错。用这个尺寸分配又不检查的调用方会把零长缓冲交给 `PackRequest`。
- **`PackResults` 只零填充恰好打包的字节区**（末字节未用高位保证为 0），绝不触碰超出打包尺寸的部分；测试故意在后面放 `0xA5` 金丝雀证明无溢出。这两个性质对 DMA 缓冲复用都是负载关键的。
- **响应永远不是"收到的消息"**，而是 one-sided RDMA 写入：请求字段 `resp_addr`（偏移 9）必须是客户端 reply slot 的远端暴露地址，服务端把整个响应 DMA 过去，客户端轮询第 8 字节。`IsResponseReady` 的 volatile 读**只在 `reply_service.cc` 里**配了显式 `atomic_thread_fence(acquire)`——协议类本身不 fence。
- 空结果校验不对称：Lookup `PackResponse` 拒绝空 results 向量，Dump/Load 允许 `result_count==0`。
- Dump/Load 预留 4 bit（15 个可能码）但今天只定义 0=Ok、1=Failed；值 2..15 能过线但无语义。
- `UnpackRequest` 失败时出参 `unique_ptr` **保持不动**（实现先建 local `make_unique`，全量校验通过后才 move 进 out）；`UnpackRequestFailurePreservesOutput` 钉住了这个行为。

### 4.4 并发与锁

- **两级锁**：分片 `RwLock`（`std::shared_mutex`）只护 map 与驱逐容器；`refCnt/status/leaseTimeout` 由 Entry 自己的 `Spinlock` 护。**这就是为什么 `LoadBegin`、`Exist`、`TryDelete`、`Evict*` 只取读锁却会修改 entry**。
- **`EvictionPolicy` 自身无任何内部同步**（头注释明说）。这对 TTL/POS 是安全的（`AccessKey` no-op、修改只在分片写锁下），但 **LRU 的 `AccessKey` 会在共享锁下 splice `std::list`——一旦启用就会 race**。这是 LRU 休眠的另一个理由。
- **三个线程可同时进入 `MetadataManager`/`BufferManager`**：GC、TaskWorker、CompletionPoller。
- **NoSpace 时驱逐的是随机分片**（`rand()%kShardCnt`），不是新 key 的分片；源码里有 TODO 承认随机分片未必有所需尺寸的条目。这之所以正确，全靠 `BufferPool` 是**全局的、按精确尺寸索引**——任何分片的受害者都释放同一 size-class 池的槽。尺寸匹配是**精确**的：未注册的请求尺寸返回 `Status::NotFound`。
- **`SpscRingQueue` 严格单生产者/单消费者**：可用槽是 `capacity-1`（牺牲一个环条目区分满/空），所以配置的 65536 深度**实际只接受 65535**；非 2 的幂容量静默退化为取模（掩码只用于 2 的幂）；`Push(T&&)` 满时永久自旋 yield（热路径用非阻塞的 `TryPush`/`TryPop`）；元素 move-only。`ConsumerLoop` 自旋 yield 16 次后睡 100us，且**每 64 个任务才重新检查一次 stop**。
- **`BufferPool::Free` 只收 uint32 slotIndex，没有 used-bit / 双重释放防护**：同一索引 Free 两次会把它重复链回 IndexPool 自由栈，之后一个槽会发给两个分配者。调用方必须严格一对一配对。
- **per-node 状态无锁的依据**：某个 node 的全部状态只被一个 Runner 线程触碰。`TransportExecutor` 用 node 哈希把同一 node（含其 fence 命令）钉在同一 worker，正是为了维持这个不变式。
- **`enqueue_us` 一个字段三用**：请求时间戳、e2e 延迟起点、以及上报后归零当哨兵。同理 LOOKUP 的 results 向量生来全是 NotFound，miss 计数是事后数出来的。**不知道这个哨兵复用，读指标代码会以为撞上了 use-after-zero bug。**

### 4.5 构建门控与平台假设

- **整个 `ucm/store/dram` 子树只在 `UCM_RUNTIME_ASCEND_FAMILY` 或 `RUNTIME_ENVIRONMENT=simu` 下构建**（CMakeLists 顶部 early return）。仓库别处有 CUDA 路径，但**这里没有 CUDA 构建**。CI 只在 `simu` 模拟器下跑它。
- `kv_protocol` 代码本身**零 Ascend ifdef**——平台门控纯粹在构建文件层。
- `transport_manager_backend.cc` **仅在 Ascend 下编译**（`UC_DRAM_ASCEND_BACKEND` 守卫）。`DramStore::Compose` 的函数体整个坐在这个 ifdef 里，否则返回 `Status::Unsupported()`。
- **HIXL 后端是条件编译的**：`ucm/transport/p2p/CMakeLists.txt` 要同时找到 `hixl/hixl.h` 和 `cann_hixl` 库才 `set(P2P_HIXL_AVAILABLE ON)` 并把两个 `hixl_*.cpp` 编进去，否则打印 "P2P HIXL transport backend is disabled" 并跳过。**没有 HIXL 就没有 DramPool**。
- `drampool` 可执行文件额外用 `--allow-shlib-undefined` 链接，因为 CANN 驱动 DSO 由运行时宿主提供。
- **POSIX/Linux 假设遍布**：`accept4`/`SOCK_CLOEXEC`/`netdb.h`（health server）、`getpid`/`unistd`、`syscall(SYS_gettid)`、`pthread_setname_np`、`sched_setaffinity`、x86 `_mm_pause` / aarch64 `yield`、`fcntl` flock、`/dev/shm`、dlopen、`RTLD_GLOBAL`。Windows 上 `_preload_library` 静默 no-op，拿不到 fcntl 的 reporter 记一条 warning 后自行退出。
- `health_server_test.cc` 无 Windows 守卫地 include POSIX 头，只在 Linux 构建下可行。
- **日志默认限流**：`UC_LOG/UC_DEBUG/...` 每 file:line 每 60s 窗口只放 3 条（512 槽 × 4 项哈希表，确定性淘汰）。**"丢失"的日志行是正常的**，调试时用 `_UNLIMITED` 变体。spdlog 异步 logger 用 `overrun_oldest`（过载会丢）+ flush-on-warn。
- Logger 构造时**安装进程级 SIGSEGV/SIGABRT/SIGFPE/SIGILL/SIGINT 处理器**（唯一职责是刷日志）——仅仅实例化这个单例就会**覆盖已有的信号处理器**。
- `InternSourceString()` 把每个 `__FILE__`/`__FUNCTION__` 字符串**永久**存进一个进程级 `unordered_set`（一把锁），因为异步格式化在后台线程而 `spdlog::source_loc` 持裸 char 指针。
- `Logger::Setup(path, max_files, max_size)` 把 max_size 当**兆字节**（×1048576），而 sink 构造器收裸字节。
- `compress_rotate_file_sink.h` 通过**显式模板特化** `spdlog::sinks::rotating_file_sink<std::mutex>` 替换行为，只在"该头在 sink 首次实例化前被 include"的 TU 里生效（`spdlog_logger.cc` 满足）。它依赖 zlib 并在 gzip 后删除未压缩文件——这是 include 顺序/ODR 陷阱，不是普通子类。
- **日志级别不在任何配置文件里**：来自 `UC_LOGGER_LEVEL` 环境变量，在 infra logger 内部读取；drampool 代码里没有 getenv。

### 4.6 指标语义（近期提交刚改过，最易读错）

- **`drampool_buffer_pool_usage_ratio` 是槽数比，不是字节容量比**：`sum(used slots)/sum(total slots)` 跨全部 block-size 池（`BufferManager::GetTotalUsedSlotRatio`）。配置了不同 block size 时，大块池的一个槽和小块池的一个槽**等权计数**。提交 `93f01c2f` 正是**故意**用它替换掉旧的 per-block-size gauges。
- **`*_duration_ms` 系列的时钟分辨率并不一致**：transfer/RTT 类（`dump/load_transfer_duration_ms`、`response_rtt_ms`）由 `SteadyNowMs()` 整数毫秒时间戳算出，**量化到整毫秒**；`get_status`/`response_submit`/`enqueue_wait`/`residence` 用 `SteadyNowUs()/1000.0` 的 double。提交 `9028c626`（"avoid an internal division operation"）移除了旧的 double-秒 `NowTime` 换算，全面切到 `std::chrono`。
- **transfer 类 histogram 不是纯网络延迟**：它包含 HIXL 传输 + Poller 延迟 + 终态 `GetStatus`（头注释有警告）。
- **`ScopedTimer` 默认 disarm，只有 `Arm()` 之后才观测**。prepare 计时器只在成功路径上 arm，所以**失败/超时批次刻意缺席** prepare/metadata histogram——不是丢了。
- **`enqueue_wait_ms` 与 `residence_ms` 是刻意拆开的一对**：receiver 在每次 TryPush 前重盖 `enqueue_us`，前者覆盖满队列重试等待；后者从成功 push 那一刻起算。**单看任一个都会低估队列阻塞。**
- **`queue_request_full_total` 每请求最多记一次**（`queueFullLogged` 闩，之后静默重试）；**`queue_completion_full_total` 每记录一次但不是丢弃**——生产者立刻退化到阻塞 Push。所以这个计数器上升意味着 TaskWorker 自旋等待过，**不代表数据丢失**。
- **`queue_response_buffer_retry_total` 是 parked-retry 计数**（flag 池临时满），**明确不是响应失败**；`response_failures_total` 只计永久的 submit/pack/write-back 失败。
- **批次结果计数器与 `batch_total_duration` 各有一次性哨兵保护**（`CompletionRecord.batch_outcome_reported`、`enqueue_us` 上报后归零），因为 NoSpace 重试会重入 `SubmitResponse`。miss/failed 数字是**数 results 向量**得来的，不是扫描中累加。
- **`dump_nospace_failures_total` 只在两轮驱逐（先周期后深度）仍 NoSpace 之后才触发**；初次 StoreBegin 注册失败按文档字符串被排除。
- **C++ 侧指标无 label**。`drampool_metrics.h` 注册裸名；label（`model_name`、`worker_id`）只在 `observability.py` 的 Python prometheus_client 侧出现。端到端追一个值时，**label 集在最后一跳才出现，不在源头**。
- **未注册的指标名在每一层都被静默丢弃**（C++ `UpdateStats`、JSON 缺名、Python `_metric_mappings` 未命中记 error）。**一个从不出现的指标通常是注册顺序问题，不是抓取问题。**
- **`NAME_TO_METRIC_ID` 藏着 epoch 机制**：早于 `CreateStats` 执行的调用点会缓存 `INVALID_METRIC_ID` 连同 `seenEpoch`；每次新注册 bump `registerEpoch_`，于是缓存的引用**事后透明解析**。**不要把它"简化"成普通静态名查表。**
- **`GetAllStatsAndClear` 是 read-and-RESET 的增量 drain**，跨线程本地双缓冲；生产者线程首次 update 时自注册并**永久留在 `buffers_`**（无注销）。同进程内从两处调它（`PrometheusStatsLogger` 与 vllm connector 路径）意味着每个样本只进一个消费者的拉取——**这正是 `MetricsDispatcher` 作为唯一 drainer 存在的原因；绕过它会把时间序列劈成两半**。
- **两个进程各有一个独立指标域**：C++ 守护进程写 `drampool_metrics.json`；跑在 UCM/vLLM 进程里的 `DramPoolResourceReporter` 读它并把增量导入**自己进程的** ucmmetrics 域。任一侧重启由增量 reset 检测处理，**两域之间没有共享内存**。
- **快照文件是单行累积 JSON，必须有尾换行**；读取端把没有 `\n` 的最后一行当撕裂写丢弃。Histogram `bucket_counts` 是**原始非累积**值，长度 = n+1（n 个有限 upper_bounds，最后一桶 +Inf）；Python 域在注册时做同样的 +Inf 归一，所以长度与 `merge_histogram_stats` 精确匹配。
- **`MergeHistogramStats` 刻意严格**（桶数长度相等、sum 有限非负、count==0 ⇒ sum==0、uint64 溢出检查），违反即抛。`snapshot_deltas` 在两次轮询之间漏掉守护进程重启时会抛 `'Histogram sum changed without samples'`——现在被基类每轮 catch，**下一个好快照即自愈**（不再永久卡死）。
- **Prometheus 把配置的 `ucm:` 前缀归一为下划线**：`ucm:drampool_dump_requests_total` 实际暴露/抓取为 `ucm_drampool_dump_requests_total`。**Grafana PromQL 必须用下划线名。**
- **指标默认关**：C++ `DramPoolConfig.metricsEnabled=false`，但 `examples/drampool.yaml` 出厂即 `metrics.enabled: true`，Python 侧除非 `enable_metrics: false` 否则默认开。有效开关是**运行时 YAML + CLI**，不是编译期。
- **三份手工维护的指标注册表**：`metrics_configs.yaml`、`default_metrics_config.py`（头部声称"由 YAML 生成"但**仓库里没有生成器**；`add-metrics.md` 说两者都手改，`test_default_metrics_config_matches_example_yaml` 强制一致）、以及 C++ 的 `DrampoolMetricDefs`（**第三份手工副本，且无针对 YAML 的自动一致性测试**）。
- **出厂 Grafana 面板（`grafana_store/connector/vllm/kv_client.json`）当前对 `drampool_` 指标零引用**。PR1394 的 Grafana 验证用的是临时面板 + 外部 `run_exporter.py`——**别指望示例面板已经画了 DramPool**。
- **counter/histogram reset 检测有已知盲区**：守护进程重启后，若新累积值在下次轮询前就已超过旧基线，这次 reset 与正常增长无法区分，会被当成增量计入（`test_baseline_restart_and_large_integer_counter` 显式断言了这一点）。
- **基线写失败是有意为之**：若持久化新基线失败，下一轮从旧的已存快照重新做差，**故意重发同一增量**而不是前进。配合 ucmmetrics 是 drain-and-clear 注册表来理解。
- **histogram 形状对生产者极易写错**：`len(bucket_counts)` 必须等于 `len(upper_bounds)+1`（尾随 +Inf 桶）、计数必须是精确非 bool 的 uint64、计数之和必须等于 `count`、bounds 递增、`count==0` 要求 `sum==0`。

### 4.7 死代码、休眠代码与 vendored 代码

- **`lru_eviction_policy.h` 在生产中不可达**：`EvictionPolicyType` 只有 TTL 和 POSITION，`metadata.cc::CreateEvictionPolicy()` 从不构造 LRU，YAML 解析器（`ParseEvictionPolicyValue`）只接受 `'ttl'`/`'position'` 并**显式拒绝 LRU/LFU 为 unsupported**。仓库内唯一使用者是它自己的 268 行测试。读它是为了理解访问跟踪模式，不是为了运行时行为。
- **`VariableBufferPool` 与 `OffsetAllocator` 当前无任何 DramPool 调用方**（已 grep 全 `ucm/` 验证）：`drampool/buffer_manager.h` 从按 block size 索引的**固定** `BufferPool` 分配 KV 块，reply 缓冲也用固定池。把它们当**可用基础设施**读，不是活的 KV 块路径。
- **`detail/` 下的 `health_check_executor.h`、`template/task_wrapper.h`、`detail/task/*` 未被任何 dramstore 源文件 include**——是其他 store 的共享脚手架。当成 DramStore 依赖来读是浪费时间。
- **`factory.py`（v0）对 DramPool 实际是死的**：`ucm/` 里没有东西 import `UcmConnectorFactory`（只有测试/文档引用），其注册表既无 pipeline 也无 Dram 项。DramStore **只能**经 `factory_v1.py` → `'UcmPipelineStore'` + `store_pipeline: 'Dram'` 到达。读它只为理解两代 API。
- **`ThreadPool` 的看门狗 `Monitor()` 会 detach（不 join）** 超过 `timeoutMs` 的 worker（调完超时回调后），再重新生成一个——**卡死任务的线程与栈按设计会泄漏**。它还是 Linux-only，且被 yuanrongstore 用，**drampool 核心不用**。
- **Router 的死旋钮**：两种 affinity 配置里的 `dynamicAdjustEnabled` 字段被解析、被存储，但在 `router.cc` 里**从不读取**。
- **`RouterType::RING_HASH` 只是 `RING_HASH_FULL_SPREAD` 的别名**（MAGLEV 同理）。ring 构建靠追加 `#salt` 去重碰撞哈希，但**某节点超过 4096 个 salt 后就静默放弃**继续加虚拟节点。Maglev 在配置值非素数时**静默把 tableSize 重置为 65537**。
- **`CONTIGUOUS_BLOCK_AFFINITY` 按 `RouteKeys` 输入向量里的位置分组**（每 blockCount 个连续条目继承首条目选中的节点），**不是按 key 内容哈希**；重排批次会改变路由。`BATCH_TOPK` 从整个有序批次的指纹（`'batch-size#n#i:key...'`）选候选节点集，所以**同样的 key 换个顺序可能得到不同节点**。
- **Router 是纯 `std::string` key → `uint64` NodeId**，对 socket/池/KV 块一无所知；`dram_store.cc` 传的是**空哈希函数**，所以默认走 `router.cc` 里的软件 CRC32 表（不是硬件哈希）。
- `topn_heap.h` 与 `hashset.h` 用了 `std::vector`/`type_traits` 却没 include 全部必需头（依赖消费者的传递 include），且**无 DramPool 调用点**。
- `offset_allocator.cc` 的头注释说"对象生命周期由外部同步"，但**华为 fork 加了一把 mutable `std::shared_mutex`**（Allocate/Free/Reset 取 unique，报告取 shared）。`BIN_SCAN_LIMIT=8` 的低 bin 扫描与精确匹配扫描也是上游之外新加的——**别假设这是原味 sebbbi 代码**。
- `OffsetAllocator::Allocate` 在 `freeStorage_` 看起来够大时仍可能返回 `NO_SPACE`，有两种不显然的情形：(a) 碎片化导致没有 bin 含 ≥ 请求尺寸的节点；(b) 有匹配但切分余量需要一个空闲 metadata 节点而 `freeOffset_==0`（metadata 耗尽后只有精确匹配能成功）。
- `VariableBufferPool` 与 `OffsetAllocator` **以 allocationAlignment（默认 64B）为单位计数，不是字节**；`Allocation` 里返回的 offset 是单位 offset（`byteOffset = unitOffset * alignment`）。`NO_SPACE` 是 `0xffffffff`。
- `BufferRegion` **从不用 malloc**：字节来自 `UC::Trans::Device{}.MakeBuffer()`，生命周期是带后端 deleter 的 `shared_ptr<void>` owner。Host/Device 类型下 `localAddr==deviceAddr`，只有 HostMappedDevice 产生不同地址；两个 `*Mapped*` 类型在未实现的后端上返回 `Status::Unsupported()`。**每次 `Create` 都新构造一个 `Trans::Device{}`**。
- `Slot.length` 是 slotCapacity（可用字节），但每个槽实际占 slotStride 字节（capacity 上取整到 64B 对齐）；region 总大小是 `slotStride*slotNum`，**不是** `capacity*slotNum`。设备与本地地址用同一 stride/offset。
- `IndexPool` 内部 1-based（槽 1..capacity，链式 next 指针）但返回 0-based（返回 slot-1）；其 8 字节 `Pointer{slot,ver}` 是版本化 CAS 以防 ABA；`Release` 静默忽略 `idx >= capacity`；`npos=UINT32_MAX` 正是 `BufferPool` 拒绝接近 uint32 上限的 slotNum 的原因。
- `enableZero_` 打开时，`Free` 会先 memset 整个槽 stride（变长池是整个 allocatedSize）再经 `Trans::Memset` 释放索引；**memset 失败则返回状态且槽被泄漏**（直到 Reset 才回收）。Init 会预先清零整个 region。

### 4.8 客户端行为语义

- **fail-stop，不优雅降级**：`AbortDramStore` 在 NodeScheduler runner、ReplyService observer 或 completion 发布路径上遇到任何未捕获异常就 `std::abort()` **整个宿主进程**。DramStore 的故障能带走链接了它的训练/推理进程。
- **无跨节点重试、无改路由**：Router 把节点定死。暴露中的请求撞上 deadline → 整节点被 fence（HIXL Disconnect）、全部暴露请求以 Timeout 失败、epoch bump、重连。重试只存在于 Connect/Fence 命令自身，按 `reconnect_interval_ms`。
- **超时对 Waiting 传输只做诊断**：DramPool **从不**自己拆连接或释放传输缓冲，它显式等待 Store 侧发起的 disconnect。**一个卡死的对端能让记录在 poller 窗口内无限期 pending。**
- **reply code 语义按 op 反转**（`reply_service.cc::DecodeReply`）：LOOKUP 下非零码表示"找到了"；DUMP/LOAD 下 0 表示成功、非零是 per-item 错误。**dump/load 的 item 级错误会让整个请求失败。**
- **`StorageKey` 会改写 key**：`shardId != 0` 时把 shardId 异或混入 16 字节 key 的前 4 字节（因为 DramPool 节点只按 BlockId 建 key）；shard 0 保留原 key，**LOOKUP 永远以 shard 0 发起**，这样才能命中已存条目。
- **DUMP 前置顺序**：`TaskDesc.prerequisiteHandle` 是 ACL event 句柄；请求在 NodeActor pending 队列里等待，按 `poll_interval_us` 经 `UC::Trans::Event::Query` 轮询，**门控该 node pending 队列的队首**（后面的请求不能插队）。
- **TaskManager worker 让 completion 优先于 submission**（先排空 `completions_`）。
- **Role 靠推断不靠配置**：`deviceId < 0`（默认 -1）⇒ SCHEDULER（仍会经 `RuntimeDeviceId` 绑物理设备 0），`>= 0` ⇒ WORKER；超过 65 被拒。**只有 WORKER 注册调用方的 `gpuKvBufferAddrs`**。
- **客户端静默端口重写**（`config.cc`）：WORKER 的 hixl listen port 变成 `configured+deviceId+1`，其 control/manager 端口也同样 +`deviceId+1`；SCHEDULER 加 0。**你在客户端 YAML 里写的端点字符串不是最终线上端口**，且 NodeId 隐式就是 `node_control_endpoints` 的数组下标。
- `config.cc` 解析期间会**临时构造一个用完即弃的 `DramPool::ProtocolManager`** 只为算打包响应尺寸（`MaxReplySize`），所以配置解析**传递性地依赖协议层**；`replySlotCount` 推导为 `node_count * maxInflight`。
- **DUMP 的 wire ttl 硬编码为 0**；服务端用 `ttl ?: defaultDumpTtlMs` 兜底。`defaultDumpTtlMs` 来自 `--ttl-minutes`（默认 120），所以**CLI 那个 flag 只是兜底，不是硬 TTL**。
- **`Exist()` 不是纯谓词**：LOOKUP 路径的存在性检查会**顺带刷新 `leaseTimeout`**（且对 INITIALIZED/DELETING 条目返回 false），所以一次 LOOKUP 会改变未来的驱逐资格。
- **`StoreBegin` 对已存在 key 返回 `DuplicateKey`，而 `ProcessDump` 把它当成功结果返给客户端**（重复 dump 已缓存块是 OK 的）；分片 StoreBegin 失败时预分配的槽在 `MetadataManager::StoreBegin` 内部被释放。
- **TTL 完全忽略 `evict_ratio`**（每轮 GC 驱逐全部过期项）；POS 与 LRU 则瞄准 `max(1, ratio*n)`。默认 `metadataDefaultEvictRatio=0.0` 下，周期 TTL 不受影响（ratio 被忽略），而 POS 深度驱逐因 `max(1,...)` 下限**每分片仍至少驱逐 1 个合格条目**。
- **TTL 可能"看错"**：由于 lease 才是 `TryMarkEvicting` 的闸门，TTL 会放过 lifeTimeout 已过期但 lease 刚被 LOOKUP 续期的条目，反之也会驱逐 lifeTimeout 未到但 lease 已过期的条目。
- `flag_buffer` 是**独立的小 staging 池**，供响应/flag RDMA 写用，与 `--pool-size-gb` 的 KV 池无关；校验要求其算出的槽数 ≥ `poller.pending_depth`。两个池都用**重复硬编码的 64 字节槽 stride 常量**（`launch_config.cc` 的 `kSlotAlignment` vs `drampool_config.h` 的 `kFlagBufferSlotAlignment`）。
- 信号处理极简：只对 SIGINT/SIGTERM 装 `std::signal` 处理器，仅置一个静态 atomic；主线程在 `WaitForShutdown` 里每 100ms 轮询它，所以**关停延迟最高 100ms**，且处理器是在解析、指标注册、日志设置**之后**才装的（**早期启动无信号保护**）。**不处理 SIGPIPE/SIGHUP。**
- 启动失败时的 teardown 是手工且不完整的：`healthServer.Start` 或 `metricsReporter.Start` 失败只停已启动的组件然后退出；`DramPoolServer` 的栈析构随后才跑，所以它的 `Stop` 必须幂等（内部有 `ServerState` 机器）。
- **Python 侧 Role 由 device_id 编码**，不是显式枚举：缺省/-1 表示 scheduler 侧（`device_id < 0`），`>=0` 表示 NPU worker。`start_drampool_resource_reporter` 在 `device_id >= 0` 时**拒绝启动**，所以每主机只有 scheduler 侧进程去 tail 快照日志。
- `start_drampool_resource_reporter` **缓存一个进程级单例** `_REPORTER`，由第一次见到的 config 创建；后续用不同 log path/interval 的调用**返回既有实例并忽略新配置**。
- `dump_data` 的 `prerequisite_handle`（与 YAML 的 `enable_event_sync`）是 **Python-C++ 流同步点**：非零句柄让 cache stream 在 dump 暴露 KV 内存前等待 compute-stream event；忽略它会让设备写与远端读发生竞争。
- **GIL 处理是窄口径而非全面放开**：只有 `PipelineStore::Wait`、`ucmmetrics.merge_histogram_stats`、`get_all_stats_and_clear` 释放 GIL。Python 侧 marshaling（numpy buffer join、torch `data_ptr` 向量化）与其余全部 pybind 调用**都持 GIL**，所以并发 Python 线程无法并行做数据 marshaling。
- `ucm/shared/metrics/__init__.py` 在 import `ucmmetrics` pybind 扩展**之前**用 `RTLD_GLOBAL` 预加载 `libmetrics.so`（该扩展自身以 RPATH `$ORIGIN` 链接 metrics）。**调换这些 import 的顺序会在其他共享库（如 libdramstore）引用 metrics 符号时导致未解析符号失败。**
- `test/test_drampool_resource_reporter.py` 在 **import 时做激进的全局 setup**：伪造 `ucm` 与 `ucm.logger`，并用 `sys.modules` 把 `ucm.shared.metrics.ucmmetrics` 换成内存 FakeMetrics。**把它当库 import（而非经 pytest）会产生这些进程级副作用**；多数测试既不需要 libdramstore 也不需要 libmetrics，只有 `test_real_prometheus_export` 与 flock 测试触及真实 native/OS 设施。
- 白盒测试在 include `task_worker.h` / `completion_poller.h` **之前**立刻 `#define private public`（之后 `#undef`）——**重排这些 include 会静默改变测试能看到什么**。

---

## 5. 动手验证（Hands-on）

### 5.1 构建

```bash
# 核心构建（DramPool 只在 Ascend 家族或 simu 下参与构建）
cmake -B build -DBUILD_UNIT_TESTS=ON -DRUNTIME_ENVIRONMENT=simu
cmake --build build -j

# CI 的完整口径
cmake -B build -DBUILD_UNIT_TESTS=ON -DRUNTIME_ENVIRONMENT=simu -DBUILD_UCM_ASU=ON
```

产物：`libdramstore.so`（客户端共享库）、`drampool`（服务端可执行文件，装到 `bin/`）、`ucmstore.test`（**单一** gtest 二进制）。

> **重要：没有 per-suite CMake target。** `ucm/store/test/CMakeLists.txt` 用 `GLOB_RECURSE` 把全部 `case/**/*.cc` 编进**一个** `ucmstore.test`，同时链接 dramstore + drampool_core。过滤只能在运行时做。

### 5.2 已知构建阻塞（梳理时分支状态）

**`ucmstore.test` 在该分支上无法编译**：`task_worker.h` 的 `ProcessDump`/`ProcessLoad`/`ProcessLookup` 需要第三个 `std::uint64_t enqueueUs` 参数，但 `task_worker_test.cc` 的包装器仍按两个参数调用。PR1394 交接文档在 2026-09-20 就标记过这处漂移（当时参数名叫 `beginUs`），至今未修，只是参数被改名为 `enqueueUs`。

**读测试前先知道这一点**——否则你会以为是自己环境的问题。要跑测试就得先补上第三个实参。

### 5.3 运行测试

```bash
ctest --test-dir build                                  # 全量
./build/.../ucmstore.test --gtest_filter='TaskWorkerTest.*'
./build/.../ucmstore.test --gtest_filter='*EvictionPolicy*'
./build/.../ucmstore.test --gtest_filter='KvProtocolTest.*'

# Python 桥测试不在 pytest 默认 testpaths（= test/suites）里，必须显式给路径
pytest test/test_drampool_resource_reporter.py
```

**被宏编译掉的测试**：`UCM_DRAMPOOL_RUNTIME_INTEGRATION_TESTS` 只在 `drampool_startup_test.cc` 内部使用，**任何 CMake 文件都不定义它**，所以全部 fork / raw-socket / 真实服务端 / SIGTERM 用例都被编译掉。正常构建下该文件**只跑**配置解析、服务端状态拒绝、daemon 非法参数三类用例。

### 5.4 哪些测试是最好的可执行规格

| 想搞清什么 | 读哪个测试 |
|---|---|
| 线协议逐字节布局、位打包真值表、全部拒绝路径 | `kv_protocol_test.cc`（1067 行，比读实现快） |
| 元数据状态机 + 1024 分片管理 | `drampool/dram_metadata_test.cc` |
| prepare 半程（StoreBegin/LoadBegin/Exist 的调用契约） | `drampool/task_worker_test.cc` |
| settle 半程（窗口深度、元数据 settle、超时边界、flag 槽生命周期） | `drampool/completion_poller_test.cc` |
| TTL 资格闸（过期 READY 被选；非 READY / 被 pin / lease 未过者存活） | `dram_ttl_eviction_policy_test.cc` |
| POS 排序（position 降序、lifeTimeout 平局）与同样的资格闸 | `dram_pos_eviction_policy_test.cc` |
| YAML 全字段映射、策略大小写、19 组非法值矩阵、失败事务性 | `drampool_yaml_config_test.cc` |
| CLI/槽数计算、服务端状态机拒绝、daemon 退出码 | `drampool_startup_test.cc` |
| 健康端点逐字节规格（分片、其他路径拒绝、超时、端口占用） | `health_server_test.cc` |
| NodeActor 事件状态机、重连排队、Runner 并发规则 | `dram_node_scheduler_test.cc` |
| 客户端任务并发/不阻塞/句柄一次性/关停语义 | `dram_task_manager_test.cc` |
| reply slot 租约/校验释放/重用/并发 | `dram_reply_service_test.cc` + `dram_reply_service_polling_test.cc` |
| Python 快照桥（flock 选主、reset 检测、histogram 严格校验） | `test/test_drampool_resource_reporter.py` |

### 5.5 配置文件即文档

**文档站没有 DramPool/DramStore 专页**——`cache-configuration/index.md` 只列 pipeline、NFS、3FS、Mooncake、compression；`architecture.md` 与 `request-lifecycle.md` 是通用 vLLM 叙事。所以：

| 想知道什么 | 权威来源 |
|---|---|
| 服务端运行时全部旋钮与端点路由表 | `examples/drampool.yaml`（72 行，每个键都有内联注释） |
| 客户端如何配到 DramStore | `examples/ucm_config_dramstore.yaml`（61 行） |
| 指标定义与消费方 | `examples/metrics/metrics_configs.yaml`（789 行，含 42 条 `drampool_*`） |
| 指标埋点设计意图与取舍留痕 | `dramPool_documents/metrics_design.md` |
| 指标如何新增/验证到 Grafana | `docs/docs-site/docs/en/developer-guide/add-metrics.md` |
| 通用架构定位 | `docs/docs-site/docs/en/developer-guide/architecture.md` |
| 请求生命周期叙事 | `docs/docs-site/docs/en/developer-guide/request-lifecycle.md` |

**手动起一个 drampool（simu 环境下）：**

```bash
./bin/drampool --addr <host>:<port> --nics <nic> \
               --pool-size-gb 8 --kvcache-block-sizes 65536 \
               --config examples/drampool.yaml
# --config 必须显式传：默认值是相对路径 examples/drampool.yaml，
# 而安装后的二进制在 bin/，从其他工作目录启动会找不到配置。
```

### 5.6 PR1394 交接文档的现状

`PR1394_DRAMPOOL_DRAMSTORE_GRAFANA_E2E_HANDOFF.md`（277 行，中文 E2E 验证日志）：

- **它的"已被取代"声明指向的文件不在代码仓里**：banner 说被 `PR1394_FULL_CHAIN_HANDOFF.md` 取代——该文件实际在 `dramPool_documents/`（独立 git 仓库），不在 `unified-cache-management` checkout 内；它引用的测试专用 `drampool_e2e.cpp` / `run_drampool_e2e.sh` 则从未提交。
- **仍然准确的部分**：单行 JSON 快照契约（`event=drampool_metrics_snapshot` 带 counters/gauges/histograms）、原子 tmp+rename 写、`DramPoolResourceReporter` 读取端——都与当前代码一致。
- **已被推翻的部分**：文档开头自己就声明"ExchangeMetadata 阻塞"结论已被推翻。它记录的"reporter 永久卡死"缺陷**已大幅缓解但未完全消失**：`snapshot_deltas` 在桶增量全零而 sum 增量非零时仍会抛 `'Histogram sum changed without samples'`，且状态仍只在导入成功后写入；但基类 `FileResourceMetricsReporter` 现在每轮 catch 该异常，所以**下一个好快照即自愈**，不再永久 wedge。
- **Helm chart 不部署 DramPool**：`charts/unified-cache-chart` 是 Kthena/CRD 路线，只接 UCM Posix/Mooncake；全 chart 大小写不敏感搜 `dram` **零命中**。

---

## 6. 可选/可跳过（Skip list）

首轮通读**不要**碰这些，尽管它们看起来像核心：

| 路径 | 行数 | 为什么可以跳过 |
|---|---|---|
| `ucm/integration/vllm/ucm_connector.py` | **3,735** | 单文件巨兽，且是 vLLM 集成胶水。只 grep `_setup_ucm_metrics` 与 `get_kv_connector_stats` 两段，**绝不通读**。这是最容易吞掉一整天却对理解 DramPool 帮助最小的文件。 |
| `ucm/sparse/**`（`gsa_on_device.py` 1612 行、`gsa.py` 1165 行、`kvstar/`、`esa.py`、`rerope/`、Ascend kernel 头 1523+988 行） | **数万行** | 稀疏注意力算法族，与 DramPool 是**平行的独立子系统**。DramPool 只做 KV 块的存取，不涉及稀疏检索算法。 |
| `ucm/sandbox/sparse/retake/**`（`qwen2_vl.py` 969、`longvideo_cache.py` 952、`qwen2_5_vl.py` 904、`llava_onevision.py` 719 行） | ~4k | 多模态实验沙盒，与生产路径无关。 |
| `ucm/shared/pool/detail/offset_allocator.{h,cc}` | 578 | vendored sebbbi 分配器 + 华为 fork 改动。**DramPool 无调用方**。 |
| `ucm/shared/pool/variable_buffer_pool.{h,cc}` | 296 | 变长池，**无 DramPool 调用方**（KV 块与 reply 缓冲都走固定 `BufferPool`）。 |
| `ucm/shared/infra/thread/thread_pool.h` | 296 | 带看门狗的 worker 池，**drampool 核心不用**（yuanrongstore 用）。 |
| `ucm/shared/infra/template/topn_heap.h`、`hashset.h` | 322 | 通用 header-only 工具，**无 DramPool 调用点**，且依赖传递 include。 |
| `ucm/shared/trans/cuda/**`（`gdr_copy.cc` 809、`gdr_stream.cc` 503 行等） | ~2k | CUDA/GDR 内部实现。DramPool 是 Ascend-only 构建，**这条路径根本不参与编译**。只需在 `device.h`/`stream.h`/`event.h` 接口层停住。 |
| `ucm/store/detail/health_check_executor.h`、`template/task_wrapper.h`、`detail/task/*` | — | **未被任何 dramstore 源文件 include**，是其他 store 的共享脚手架。当依赖读是纯浪费。 |
| `ucm/store/{asu,mooncakestore,yuanrongstore,pcstore,ds3fs,nfsstore,compress,empty,fake}/` | 数千行 | 兄弟后端实现。DramStore 与它们同实现 `StoreV1` 但互不调用。只在需要对比后端契约时抽查。 |
| `ucm/store/test/case/{asu,delegator,posix,pipeline}/` | ~3k | 其他后端的测试（`asu_store_test.cc` 1412 行、`delegator_executor_test.cc` 1018 行）。 |
| `ucm/store/test/e2e/cache_posix_bw.py`(817)、`cache_fake_bw.py`(795) | 1.6k | POSIX/fake 后端的带宽压测脚本，与 DramPool 无关。 |
| `ucm/store/ucmstore.h` + `factory.py` + `ucmstore.py` | ~325 | v0 旧代接口与旧分发表。**DramStore 不实现 `CCStore<>`**，`factory.py` 里也搜不到 dram。只为理解两代 API 分裂时扫一眼。 |
| `ucm/store/dram/cc/drampool/lru_eviction_policy.h` + 其测试 | 378 | **生产不可达**（枚举无 LRU、工厂不构造、YAML 显式拒绝）。需要理解 `AccessKey` 访问跟踪模式时才读——并注意它的 `AccessKey` 在共享锁下 splice list，一旦启用就会 race。 |
| `ucm/transport/p2p/src/protocols/hixl/**`、`src/control/**`、`src/channels/tcp/*.cpp` | ~2.3k | 传输层内部实现。DramPool 侧只经 `ITransportBackend` 的三个方法（`Transmit`/`Connect`/`Fence`）使用它。**首轮停在 `transport.h` 接口层**；只有排查建连失败或 RDMA 注册问题时才下钻。 |
| `ucm/default_metrics_config.py` | 1,298 | YAML 的手工进程内镜像。有测试强制它与 YAML 一致，所以**读 YAML 就够了**；只在查某个指标的定义细节时按名 grep。 |
| `kv_semantics/**` | **32k** | 独立的 KV client/transport/provider 子系统（自己的 `kv_protocol.cc` 1271 行、`kv_client_impl.cc` 941 行）。**与 `ucm/store/dram` 无编译或调用关系**，仅名字相似。 |
| `toolkit/**` | **87k** | 独立工具包（诊断/计算器/沙盒），与 DramPool 运行路径无关。 |
| `charts/unified-cache-chart/**` | — | **不部署 DramPool**（全文搜 "dram" 零命中）。 |

---

## 7. 一句话收尾

主线只有约 7k 行、2–3 天可过完。真正的理解门槛不在代码量，而在四处反直觉设计：

1. **名字与网络角色相反**（DramPool 是 HIXL Client，主动外连）
2. **响应不是消息而是 RDMA 写入**（没有 socket 回复路径）
3. **两级锁 + 标记-收割驱逐**（读锁下修改 entry 是有意为之）
4. **指标跨两个进程只经一个 JSON 文件**（无共享内存）

先把这四点记住，再按阶段 1→10 走，就不会在 `messages.h` 和 `bounded_queue.h` 这类红鲱鱼上浪费时间。

---

## 附：术语速查

| 术语 | 含义 |
|---|---|
| **HIXL** | CANN 提供的外部单边传输库（链接 `cann_hixl`，头 `hixl/hixl.h`），语义等同 RDMA READ/WRITE。本仓库只包了一层适配器（`hixl_instance` / `hixl_transport`）收敛到 `transport::Transport` 接口后。仓库内未展开该缩写。 |
| **one-sided / two-sided** | two_sided = TCP 控制面（传请求字节）；one_sided = HIXL 数据面（搬 KV 与响应）。 |
| **BlockId** | 16 字节块哈希，`std::array<std::byte,16>`，是唯一的线上 key 类型。 |
| **Entry** | 一条缓存记录，状态机 INITIALIZED → READY → DELETING。 |
| **leaseTimeout / lifeTimeout** | 前者可变、LOOKUP 续期、是驱逐闸门；后者 DUMP 时一次盖定、只用于策略排序。 |
| **flag buffer** | 独立小池，只用于打包响应并 RDMA 写回客户端 reply slot。 |
| **reply slot** | 客户端 DeviceMappedHost 内存槽，已注册给 HIXL，服务端把响应 DMA 写进来，客户端轮询第 8 字节 doorbell。 |
| **fence** | 客户端对某 DramPool 节点的整体隔离：HIXL Disconnect + epoch++ + 重连，不重试不改路由。 |
| **POS 驱逐** | 按推理 token position 降序驱逐（越靠后的 token 越先走）。 |
| **SPSC ring** | 无锁单生产者单消费者环，容量 N 实放 N-1。服务端两条热路径队列都用它。 |
