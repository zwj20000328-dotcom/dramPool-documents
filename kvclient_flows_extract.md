# KvClient 全操作流程图

> 源码仓库：`D:\xiaojun\work\DramPool\unified-cache-management`
> 分支：`z00984767-0917`
> 生成日期：2026-10-21
> 图中行号均来自当前分支代码，路径相对仓库根目录。
> 图使用 Mermaid 语法，VSCode Markdown 预览可直接渲染（建议安装 Mermaid 插件）。

---

## 目录

1. [图例：线程与三级任务模型](#1-图例线程与三级任务模型)
2. [Init 全流程](#2-init-全流程)
3. [后台视图刷新](#3-后台视图刷新)
4. [Shutdown 全流程](#4-shutdown-全流程)
5. [QueryAsync + Check/Wait](#5-queryasync--checkwait)
6. [DeleteAsync + Wait](#6-deleteasync--wait)
7. [StoreAsync / LoadAsync（单条 entry 路径）](#7-storeasync--loadasync单条-entry-路径)
8. [BatchStoreAsync / BatchLoadAsync](#8-batchstoreasync--batchloadasync)
9. [RegisterRegions / UnregisterRegions](#9-registerregions--unregisterregions)
10. [Check / Wait / 任务状态机](#10-check--wait--任务状态机)
11. [读图时最容易看错的 8 个点](#11-读图时最容易看错的-8-个点)
12. [附录：关键源码索引](#12-附录关键源码索引)

---

## 1. 图例：线程与三级任务模型

```mermaid
flowchart LR
    subgraph CALLER["调用方线程"]
        API["6 个 Async API"]
    end
    subgraph CLIENT["每个 KvClient"]
        Q1["SPSC taskQueue_"]
        CW["Client WorkerLoop<br/>单线程"]
    end
    subgraph NODE["每个 ASU / AsuTransport"]
        Q2["SPSC executeQueue_"]
        TW["Transport WorkerLoop<br/>单线程"]
        TC["CompletionLoop<br/>单线程轮询"]
        CM["ConnectionManager<br/>recover 线程"]
    end
    API --> Q1 --> CW
    CW --> Q2 --> TW
    TW --> TC
    CM -.连接恢复.-> Q2
    TW --> HW["TransProvider Send"]
    TC --> HW
```

| 层级 | 对象 | 数量 | 完成判定 |
|---|---|---|---|
| L1 | `ClientTask` | 每次 API 调用 1 个 | `remainingTransportTasks` 归零 → `Finalize` |
| L2 | `TransportTask` | 每个被路由命中的 ASU 1 个 | `remainingSubBatchCount` 归零 |
| L3 | `TransportSubBatch` | 每个 SQE 1 个 | 收到匹配 cid 的 CQE |

**两条独立超时**

- `TransportConfig.timeoutMs`（默认 100ms）：管 TransportTask 的子批 deadline。
- `KvClientConfig.defaultWaitTimeoutMs`（默认 100ms）：管调用方 `Wait` 的阻塞时长。

**两类入口**

- **keys 路径**：`QueryAsync` / `DeleteAsync`。
- **entries 路径**：`LoadAsync` / `StoreAsync` / `BatchLoadAsync` / `BatchStoreAsync`。

**线程数量**

- 每个 `KvClient`：1 个 Client WorkerLoop。
- 每个 `AsuTransport`：1 个 WorkerLoop + 1 个 CompletionLoop + ConnectionManager 的 recover 线程。
- CQE 是**轮询**出来的，不是中断/回调推送。

---

## 2. Init 全流程

```mermaid
flowchart TD
    A["Init(configPath)"] --> B["LoadKvClientConfig<br/>config_utils.cc:125-249"]
    B --> C["Init(KvClientConfig)<br/>kv_client_impl.cc:125"]
    C --> D{"initialized_ ?"}
    D -- 是 --> E["RESOURCE_BUSY"]
    D -- 否 --> F{"sharedProviderMode<br/>0 或 1 ?"}
    F -- 否 --> G["INVALID_ARGUMENT"]
    F -- 是 --> H["保存 config_<br/>创建 ViewServer"]
    H --> I["transportConfigs_[nodeId] = cfg"]
    I --> J{"transportConfigs 为空 ?"}
    J -- 是 --> K["INVALID_ARGUMENT"]
    J -- 否 --> L["GetGlobalView(view)"]
    L -- 失败 --> M["原样返回错误"]
    L --> N["创建业务内存 provider<br/>providerMemoryStates_[0]"]
    N --> O["BuildSnapshot(view, nullptr)"]
    O --> P["GetSortedAsuIds<br/>过滤 invalid + 排序"]
    P --> Q["逐个 ASU: BuildTransport"]
    Q --> R{"oldSnapshot 有同 nodeId ?"}
    R -- 是 --> S["复用 transport<br/>不重建、不重新 bind"]
    R -- 否 --> T{"sharedProviderMode == SHARED ?"}
    T -- 是 --> U["transProvider = memoryProvider_<br/>Init 后直接返回"]
    T -- 否 --> V["新建 provider<br/>Bind 已注册 region<br/>登记 ProviderMemoryState"]
    U --> W["CreateRouter(nodeIds, config.attrs)"]
    V --> W
    W --> X["taskQueue_.Setup(max(2,maxInflight)+1)"]
    X --> Y["启动 Client WorkerLoop"]
    Y --> Z["发布 snapshot_<br/>initialized_ = true"]
```

### 关键分支

- `BuildSnapshot` 失败时只回滚 `providerMemoryStates_` 和 `memoryProvider_`（`kv_client_impl.cc:170-175`）；`viewServer_` / `transportConfigs_` / `config_` 保留。
- `BuildTransport` 全程持有 `memoryMu_`，包括 `transport->Init`（建连）和 `BindMemory`（`kv_client_impl.cc:685-739`），因此注册内存与新增 ASU 完全串行。
- **视图可能为空**：配置文件型 ViewServer 读不到 `asuIds` 时 `asuMap` 为空，`Init` 仍返回 OK，直到第一次 Submit 才报 `NOT_INITIALIZED`。
- `CreateRouter` 返回值没有判空；返回 null 会延迟到 Submit 时才暴露。
- 队列有效容量 = `max(2, maxInflightTasks)`（SPSC 环形队列空一格）。
  - `Setup(queueDepth + 1)`，其中 `queueDepth = max(2, maxInflightTasks)`。
- `GetSnapshot()` 在 `mutex_` 下读，`initialized_` 是普通 bool，因此 Init 与并发调用之间依赖外部串行化。

### 初始化失败的分层清理

| 失败点 | 清理动作 |
|---|---|
| ViewServer 工厂返回 null | 无资源需清理 |
| transportConfigs 为空 | 已有 `viewServer_` / `config_` 不回收 |
| GetGlobalView 失败 | 尚未创建 provider |
| memory provider 创建失败 | 无 |
| BuildSnapshot 失败 | 清空 `providerMemoryStates_`、reset `memoryProvider_` |
| transport Init / Bind 失败 | 新建的 transport 随 snapshot 析构触发 Shutdown |

---

## 3. 后台视图刷新

```mermaid
flowchart TD
    A["触发点<br/>QueryAsync 提交失败<br/>Wait 返回后<br/>RegisterRegions 失败<br/>WorkerLoop 处理完任务"] --> B{"initialized_ ?<br/>refreshInProgress_ ?"}
    B -- 已停或已有刷新 --> C["直接丢弃<br/>不排队"]
    B -- 可执行 --> D["置 refreshInProgress_=true<br/>新建 refreshThread_"]
    D --> E["RefreshView"]
    E --> F["持 mutex_ 拷贝 viewServer_/oldSnapshot"]
    F --> G["GetGlobalView（不持锁）"]
    G --> H["持 mutex_ 首次 ShouldPublishView"]
    H -- 不需要发布 --> I["返回 OK"]
    H -- 需要 --> J["BuildSnapshot(view, oldSnapshot)<br/>复用未变 transport"]
    J --> K["持 mutex_ 二次 ShouldPublishView"]
    K -- 不需要 --> I
    K -- 需要 --> L["旧且被移除的 transport<br/>移入 retiredTransports_"]
    L --> M["snapshot_ = 新快照"]
    M --> N["refreshInProgress_=false"]
```

### 关键点

- **发布判据**：双方 `viewEpoch` 都非 0 时要求严格递增；任一方为 0 则每次都发布（`view_server.cc:122-127`）。
- 已存在的 nodeId 直接复用旧 transport，**不会重新执行服务端能力钳制**；只有新建 transport 才重新读取 `ServerKvCapabilities`。
- 被移除的 transport 只进 `retiredTransports_`，**直到 Shutdown 才真正关闭**。
- 刷新失败不会回滚 `BuildTransport` 已追加的 `ProviderMemoryState`。
- **刷新不排队**：`refreshInProgress_` 为 true 时，新的刷新请求直接丢弃。
- **触发白名单**：`CONNECTION_ERROR` / `IO_ERROR` / `TIMEOUT` / `NOT_FOUND` / `BUFFER_NOT_REGISTERED`（聚合状态或任一 entry 状态）。
- **入口不对称**：`QueryAsync` 提交失败与 `Wait` 返回会触发刷新；`LoadAsync` / `StoreAsync` / `BatchLoadAsync` / `BatchStoreAsync` / `DeleteAsync` 的提交失败**不会**触发。

---

## 4. Shutdown 全流程

```mermaid
sequenceDiagram
    autonumber
    participant C as 调用方/析构
    participant K as KvClientImpl
    participant W as Client WorkerLoop
    participant T as AsuTransport
    participant P as TransProvider
    C->>K: Shutdown()
    K->>K: mutex_ 下 initialized_=false<br/>取 defaultWaitTimeoutMs
    K->>K: producerMu_ 下 stopWorker_=true
    K->>W: notify
    K->>K: JoinBackgroundRefresh()
    K->>W: join（尾部 TryPop 排空已入队任务）
    K->>K: 摘出 snapshot_/retiredTransports_<br/>清空 config_/viewServer_/transportConfigs_
    K->>K: memoryMu_ 下摘出 providerMemoryStates_<br/>清空 registeredRegions_
    K->>K: taskManager_.Drain(waitTimeoutMs)
    loop 每个 snapshot transport
        K->>T: Shutdown()
        T->>T: stop worker + join
        opt 有 INFLIGHT 且 timeoutMs != 0
            T->>T: sleep(timeoutMs)
        end
        T->>T: stop completion + join
        T->>T: 剩余任务 Cancel + NotifyCompletion + Remove
        T->>P: 关闭 executor / connManager
    end
    K->>T: 关闭 retiredTransports_
    loop providerMemoryStates_ 逆序
        K->>P: UnregisterMemory(全部 local handles)
    end
    K-->>C: finalStatus
```

### 关键点

- `Drain` 的超时是**逐任务**计算，最坏总等待 ≈ 未完成任务数 × `defaultWaitTimeoutMs`，不是单个全局超时。
- `Drain` 超时后**仍继续**关闭 transport 和注销内存。
- transport `Shutdown` 若仍有 INFLIGHT 且 `timeoutMs != 0`，会先 sleep 一个 `timeoutMs`；多个 transport 的等待会叠加。
- 顺序是 `JoinBackgroundRefresh` → worker join，且 `initialized_` 已先置 false，worker 内部的刷新请求会被丢弃，不会留下未 join 的线程。
- **transport 关闭顺序不确定**：`snapshot->transports` 是 `unordered_map`，遍历顺序不保证。
- **provider 注销严格逆序**：`providerMemoryStates_` 的最后一个先注销，`memoryProvider_`（index 0）最后。
- `Shutdown` 绕过 `UnregisterRegions`，直接对每个 ProviderMemoryState 调用 `UnregisterProviderRegions`。

---

## 5. QueryAsync + Check/Wait

```mermaid
sequenceDiagram
    autonumber
    participant C as 调用方
    participant M as ClientTaskManager
    participant Q as taskQueue_ (SPSC)
    participant W as Client WorkerLoop
    participant R as Router
    participant T as AsuTransport
    participant P as TransProvider
    participant L as CompletionLoop
    C->>M: QueryAsync(keys, taskId)
    M->>M: SubmitAsync(keys)<br/>校验 snapshot/router/transports<br/>opType 必须是 QUERY/DELETE
    M->>M: ClientTask{opType=QUERY, keys,<br/>entryStatus=n*OK, exists=n*0}
    M->>Q: TryPush
    M-->>C: 返回 TaskId（仅代表已入队）
    Q->>W: ConsumerLoop 取出
    W->>W: QUERY 的 prerequisiteEventHandle 恒为 0<br/>跳过 SynchronizeEvent
    W->>R: BuildTransportTasks → RouteKeys（8B key 原始字节）
    R-->>W: unordered_map{nodeId → [原始下标]}
    loop 每个 node
        W->>T: TransportTask.Submit
    end
    T->>T: entryStatus=keys.size()*OK<br/>deadline=now+timeoutMs
    T->>P: Execute：按 queryIoNum(默认256) 切子批<br/>Build Exist SQE（sc 位）→ Send
    loop CompletionLoop 每轮
        L->>L: 从 flag buffer dword3 读 cid
        alt cid==0 或 != 本子批 cid
            L->>L: 本轮跳过
        else cid 匹配
            L->>L: UnpackCqe：status / existing_key_number / 1-bit result
            L->>L: FillEntryStatusFromCqeResult
            Note right of L: sc=true（默认）：按 1-bit 结果逐条<br/>sc=false：用 existing_key_number 当前缀命中数
            L->>T: CompleteSubBatch（--remainingSubBatchCount）
        end
    end
    T->>M: 回调 CompleteTransportTask<br/>（运行在 transport 的线程）
    M->>M: 仅在 transport status OK 时<br/>exists[originalIndices[i]]=result.exists[i]
    M->>M: entryStatus[originalIndices[i]]=result.entryStatus[i]
    M->>M: 计数归零 → Finalize<br/>重算 prefixHitKeys + OK/PARTIAL_FAILED
    M-->>C: cv.notify_all
    C->>M: Wait(taskId, timeoutMs, result)
    M->>M: WaitContext 等待 Done → BuildResult
    M->>M: Remove(taskId)（成功/失败/超时都删）
    M-->>C: TaskResult{status, entryStatus, queryResult}
```

### Query 的 sc / useSeekControl 分支

```mermaid
flowchart TD
    A["CQE status"] --> B{"status == OK ?"}
    B -- 是 --> C["所有 entry = ENTRY_KEY_EXIST"]
    B -- 否 --> D{"status == CQE_CHECK_RESULT_BUFFER ?"}
    D -- 否 --> E["所有 entry = 该 CQE 状态"]
    D -- 是 --> F{"useSeekControl（sc） ?"}
    F -- true --> G["按 dword4 起的 1-bit 结果逐条解码<br/>1 → ENTRY_KEY_EXIST<br/>0 → ENTRY_KEY_NOT_EXIST"]
    F -- false --> H["忽略 result buffer<br/>只读 existing_key_number<br/>前 N 条 = EXIST，其余 = NOT_EXIST"]
```

### 必须记住的语义

- `exists` 在提交时预分配为 `keys.size()` 个 0，最终按 `originalIndices` 回填，**顺序与调用方一致**。
- `prefixHitKeys` 在 transport 层算过一次，但客户端 `Finalize` **会基于合并后的 exists 重新计算并覆盖**（`task_manager.cc:232-238`）。
- Query 的“key 不存在”**不是任务失败**：`CQE_CHECK_RESULT_BUFFER` 被归一化为 OK，缺失 key 只体现在 `entryStatus` 和 `exists=0`。
- **陷阱**：如果某个 transport task 的最终状态非 OK（例如部分子批失败），客户端会**整个跳过 exists 合并**，该分片对应的 `exists` 保持初始 0，看起来像“全部未命中”。
- `Check` 对不存在的 TaskId 返回 `true`；`Wait` 对不存在的返回 `TASK_NOT_FOUND`。
- `Wait` 超时后任务仍被 `Remove`，后台继续跑但结果再也取不到。
- 默认 `sc=true`：走 per-key 1-bit 结果缓冲区；配置 `sc=false` 才走 `existing_key_number` 前缀语义。

---

## 6. DeleteAsync + Wait

```mermaid
sequenceDiagram
    autonumber
    participant C as 调用方
    participant M as ClientTaskManager
    participant Q as taskQueue_
    participant W as Client WorkerLoop
    participant R as Router
    participant T as AsuTransport
    participant P as TransProvider
    participant L as CompletionLoop
    C->>M: DeleteAsync(keys, taskId)
    M->>M: SubmitAsync(keys)<br/>queryResult.exists 长度为 0<br/>不解析 mrKey、不需要注册 buffer
    M->>Q: TryPush
    M-->>C: TaskId
    Q->>W: 取出
    W->>R: RouteKeys(keys)
    R-->>W: nodeId → indices
    W->>T: 每个 node 一个 TransportTask
    T->>P: 按 deleteIoNum(默认254，受 deleteKeys 钳制) 切子批<br/>Build Delete SQE（无 dtype/dspec/sc）→ Send
    L->>L: Poll cid 匹配 → UnpackCqe<br/>1-bit result per key
    L->>L: raw 0 → OK；raw 1 → ENTRY_DELETE_FAILED(0x0201)<br/>其它 → IO_ERROR
    L->>T: CompleteSubBatch
    T->>M: 回调 CompleteTransportTask（无 queryResult 合并）
    M->>M: entryStatus 回填 + 计数归零 → Finalize
    C->>M: Wait → TaskResult（queryResult = nullopt）
```

### 关键点

- Delete 走 keys 重载，**完全不触碰 `registeredRegions_` / mrKey**，调用方无需注册 buffer。
- Delete 的 entry 状态基址是 `0x0200`，与 Query（`0x0300`）、批读写（`0x0100`）不同。
- `CQE_CHECK_RESULT_BUFFER` 对 Delete **不归一化成 OK**，因此任一 key 失败会把整批拉成 `PARTIAL_FAILED`。
- Delete 的 SQE 没有 `dtype` / `dspec` / `sc` / `lr`；`rflag` 恒为 true，响应地址指向 flag buffer。
- **UCM 的 `AsuStore` 从不调用 Delete**（`StoreV1` 没有删除接口），这条路径主要给 kv-test 和单测用。
- 空 key 向量不会报错：路由结果为空 → 直接 `Finalize` 为 OK。

---

## 7. StoreAsync / LoadAsync（单条 entry 路径）

```mermaid
sequenceDiagram
    autonumber
    participant C as 调用方
    participant M as ClientTaskManager
    participant Q as taskQueue_
    participant W as Client WorkerLoop
    participant R as Router
    participant T as AsuTransport
    participant P as TransProvider
    participant L as CompletionLoop
    C->>M: StoreAsync(entries, taskId, eventHandle)<br/>或 LoadAsync(entries, taskId)
    M->>M: SubmitAsync(entries)<br/>opType ∈ {LOAD,STORE,BATCH_LOAD,BATCH_STORE}
    M->>M: memoryMu_ 下：mrKey.reset()<br/>mrKey = registeredRegions_[buffer.handle].tokenId
    Note right of M: 调用方传入的 mrKey 被丢弃<br/>未注册则保持 nullopt，稍后 BUFFER_NOT_REGISTERED
    M->>Q: TryPush
    Q->>W: 取出
    opt prerequisiteEventHandle != 0（只有 Store 能设）
        W->>W: runtime::SynchronizeEvent(handle)<br/>失败 → CompleteWithError
    end
    W->>R: ExtractEntryKeys → RouteKeys
    R-->>W: nodeId → indices
    W->>T: TransportTask.Submit
    T->>T: entryStatus = entries.size()*OK<br/>deadline
    T->>P: Execute
    P->>P: GetSqeIoNum(LOAD/STORE)=1<br/>每条 entry 一个子批/SQE/cid/flag buffer
    P->>P: ValidateSqeMrKeys<br/>Build 64B Store/Retrieve SQE
    P->>L: Send 返回（完成靠轮询）
    L->>L: 读 flag buffer dword3：cid/status
    L->>L: Store/Retrieve 无 result buffer<br/>CQE 状态直接填给该条 entry
    L->>T: CompleteSubBatch
    T->>M: 回调 + 回填 + Finalize
    C->>M: Wait → TaskResult
```

### 关键点

- `GetSqeIoNum` 对 `LOAD` / `STORE` **硬编码返回 1**（`io_scheduler.cc:82`）。所以传 N 条给 `StoreAsync` 会发出 **N 个 SQE、N 个 cid、N 个 flag buffer**；而 `BatchStoreAsync` 只发 `ceil(N/ioNum)` 个。
- 只有 `StoreAsync` / `BatchStoreAsync` 有 `eventHandle` 重载；`LoadAsync` / `BatchLoadAsync` 没有。event 在**唯一的 client worker 线程上同步等待**，会阻塞所有排队的 client 任务（队头阻塞）。
- 单条 SQE 不设置 `rflag` / 响应地址，但 transport 仍分配 17B flag buffer 并轮询 CQE 头。
- `KVBuffer` 没有 per-entry 长度字段：SQE 的 `length` 取 `buffer.region.size`，即整段注册区域的长度。
- Store 额外编码 `dtype`（3 bit）/ `dspec`（5 bit）；Retrieve 没有这两个字段。
- 两种 SQE 都是固定 16 dword = 64 字节。
- 传输长度上限 24 bit（`0xFFFFFF`），且 buffer 地址 / offset 必须 512B 对齐。

---

## 8. BatchStoreAsync / BatchLoadAsync

```mermaid
sequenceDiagram
    autonumber
    participant C as 调用方
    participant M as ClientTaskManager
    participant W as Client WorkerLoop
    participant R as Router
    participant T as AsuTransport
    participant P as TransProvider
    participant L as CompletionLoop
    C->>M: BatchStoreAsync(entries, taskId[, event])
    M->>M: SubmitAsync(BATCH_STORE)<br/>mrKey 解析 + ClientTask
    M-->>C: TaskId
    M->>W: 入队
    W->>W: 可选 SynchronizeEvent（仅 BatchStore）
    W->>R: ExtractEntryKeys → RouteKeys
    R-->>W: 每个 ASU 一个 TransportTask<br/>（先按节点分组）
    W->>T: Submit
    T->>P: Execute
    P->>P: 每个 TransportTask 内再切分<br/>BATCH_LOAD: batchLoadIoNum<br/>BATCH_STORE: batchStoreIoNum<br/>（默认110，Init 时被服务端能力 min 钳制）
    P->>P: 每个子批：ValidateSqeMrKeys<br/>Build Batch SQE：64B 头 + 36B/entry
    P->>P: 一次 Send 发该 task 的所有子批
    L->>L: 逐子批轮询 cid
    L->>L: CQE 0x732 → 从 dword4 起按 4-bit/entry 解码
    L->>L: raw 0x101/0x102/0x103/0x104 → ENTRY_*
    L->>T: CompleteSubBatch
    T->>M: BuildResult 按子批顺序拍平<br/>client 按 originalIndices 还原调用方顺序
    M->>M: 计数归零 → Finalize
    C->>M: Wait → TaskResult.entryStatus
```

### 关键点

- **切分顺序**：先按 ASU 路由，再在每个 TransportTask 内按 ioNum 切。`batch_number` 是子批大小，不是调用方批大小。
- 服务端能力钳制：`min(config, advertised)`，能力值为 0 表示“未声明”、不钳制（`kv_transport_impl.cc:130-138`）。
- **协议硬上限 110**。配置超过 110 且无服务端钳制时，通常是 **flag buffer 容量（默认 71 = 16 + ceil(110/2)）先分配失败**，而不是 SQE 校验失败。
- 每子批的 flag buffer = `16 + ceil(N/2)` 字节；结果 4-bit/entry，entry 0 是第一个结果 dword 的低 4 位。
- 只要任一子批状态非 OK，**transport 任务就是 `PARTIAL_FAILED`，client 顶层也是 `PARTIAL_FAILED`**；具体哪条失败只能看 `entryStatus`。
- `BatchLoadAsync` 没有 `eventHandle` 重载。
- 单条 batch entry 的 36 字节布局：offset(4) + key(8) + reserved(8) + buffer_addr(8) + length/mr_key(4) + mr_key/DptrType(4)。
- `dtype` / `dspec` 只在 BatchStore 中编码；BatchRetrieve 的对应字段保持 0。

---

## 9. RegisterRegions / UnregisterRegions

### 9.1 RegisterRegions

```mermaid
flowchart TD
    A["RegisterRegions(regions)"] --> B["GetSnapshot<br/>未初始化 → NOT_INITIALIZED"]
    B --> C{"transports 为空 ?"}
    C -- 是 --> D["返回 OK（静默 no-op）<br/>不记录、不延迟绑定"]
    C -- 否 --> E["memoryMu_ 加锁"]
    E --> F["memoryProvider_->RegisterMemory<br/>得到 canonical MRHandle"]
    F -- 失败 --> G["清理已返回 handles<br/>PARTIAL_FAILED"]
    F --> H["逐个 GetMemTokenId<br/>失败 → 全部 unregister"]
    H --> I["组装 RegisteredMemory<br/>{region, canonicalHandle, tokenId}"]
    I --> J{"遍历每个 providerMemoryStates_"}
    J --> K{"provider == memoryProvider_ ?"}
    K -- 是 --> L["local handle = canonical handle<br/>（恒等映射）"]
    K -- 否 --> M["BindMemory(addr,size,tokenId)<br/>校验返回 tokenId 必须一致"]
    M -- 失败 --> N["逆序回滚已 bind 的 provider<br/>清空输出 → PARTIAL_FAILED<br/>客户端账本不变"]
    L --> O["提交账本：<br/>state.regionHandles[canonical]=local<br/>registeredRegions_[canonical]=RegisteredMemory"]
    M --> O
    O --> P["返回 RegisteredMemory 列表"]
```

### 9.2 UnregisterRegions

```mermaid
flowchart TD
    A["UnregisterRegions(handles)"] --> B["memoryMu_ 加锁"]
    B --> C["过滤：丢弃 0 和不在 registeredRegions_ 的 handle<br/>（静默忽略，不报错，不去重）"]
    C --> D["逆序遍历 providerMemoryStates_"]
    D --> E["canonical → provider-local 翻译"]
    E --> F["UnregisterMemory（该 provider 的 local handles）"]
    F -- 失败 --> G["记录首个失败<br/>保留该 provider 的全部映射"]
    F -- 成功 --> H["删除该 provider 的映射"]
    G --> I["继续处理下一个 provider"]
    H --> I
    I --> J["对每个 canonical handle：<br/>是否仍有任一个 provider 映射它？"]
    J -- 是 --> K["保留在 registeredRegions_"]
    J -- 否 --> L["从 registeredRegions_ 删除"]
    K --> M["返回首个失败 或 OK"]
    L --> M
```

### 关键点

- **两层 handle 映射**：`registeredRegions_` 用 canonical handle；每个 `ProviderMemoryState.regionHandles` 维护该 provider 的 local handle。`memoryProvider_` 那一层是恒等映射。
- `SubmitAsync(entries)` 在 `memoryMu_` 下用 `registeredRegions_` 填 `mrKey = tokenId`；未注册不会立刻报错，直到 transport 的 `ValidateSqeMrKeys` 返回 `BUFFER_NOT_REGISTERED`。
- `UnregisterRegions` 的部分失败语义是**有意保留映射**：该 canonical handle 会继续留在 `registeredRegions_`，`mrKey` 仍能解析，但 provider 上可能已不存在。重试会重发整批。
- `Shutdown` **绕过** `UnregisterRegions`，直接逆序注销所有 provider state。
- 新建 ASU 时，`BuildTransport` 会把当前 `registeredRegions_` 全部 `BindMemory` 到新 provider；bind 后的 `tokenId` 必须与注册时一致。
- **`transports` 为空时静默返回 OK**：此时注册的 region 不会被记录，后续 ASU 出现时也不会补绑。
- `RegisterRegions` 不检测重复注册；同一次调用中重复的 canonical handle 由 `emplace` 首个生效。
- Header 契约：**被排队或在飞任务引用的 handle 必须保持注册**（`kv_client.h:58-59`）；代码没有对在飞任务做保护。

---

## 10. Check / Wait / 任务状态机

```mermaid
stateDiagram-v2
    [*] --> PENDING: TaskManagerBase::Submit<br/>分配 TaskId
    PENDING --> INFLIGHT: ClientTaskManager::Process<br/>或 TransportTaskExecutor::Execute
    INFLIGHT --> COMPLETED: Finalize<br/>或 CompleteWithError<br/>或 Cancel
    COMPLETED --> REMOVED: Wait / Drain → Remove
    REMOVED --> [*]

    note right of PENDING
        Check → false
        Wait → 阻塞
    end note
    note right of COMPLETED
        Check → true
        Wait → 立即返回结果并 Remove
    end note
    note right of REMOVED
        Check → true（无法区分“不存在”）
        Wait → TASK_NOT_FOUND
    end note
```

```mermaid
flowchart TD
    A["Wait(taskId, timeoutMs)"] --> B["timeoutMs==0 ?<br/>用 defaultWaitTimeoutMs"]
    B --> C{"task 存在 ?"}
    C -- 否 --> D["TASK_NOT_FOUND<br/>不写 result"]
    C -- 是 --> E["cv.wait_for(timeoutMs, Done)"]
    E --> F["BuildResult"]
    F --> G{"Done ?"}
    G -- 是 --> H["status=finalStatus<br/>entryStatus=快照<br/>QUERY 附带 queryResult"]
    G -- 否 --> I["status=TIMEOUT<br/>entryStatus=当前快照"]
    H --> J["Remove(taskId)"]
    I --> J
    J --> K{"status/result 需要刷新视图 ?"}
    K -- 是 --> L["RequestBackgroundRefresh"]
    K -- 否 --> M["返回"]
    L --> M
```

### 结果聚合规则

```mermaid
flowchart TD
    A["子批完成"] --> B{"子批状态 OK ?"}
    B -- 全部 OK --> C["TransportTask.finalStatus = OK"]
    B -- 任一非 OK --> D["TransportTask.finalStatus = PARTIAL_FAILED"]
    C --> E{"所有 TransportTask 都 OK ?"}
    D --> E
    E -- 是 --> F["ClientTask.finalStatus = OK"]
    E -- 否 --> G["ClientTask.finalStatus = PARTIAL_FAILED<br/>原始错误码只在 entryStatus / 日志"]
```

### 关键点

- `Wait` 是**破坏性读**：成功、失败、超时都会 `Remove`。
- `Wait` 返回的是 `WaitContext` 的状态，也就是 `TaskResult.status`；完成后的值可能是 `OK`、`PARTIAL_FAILED`、`TIMEOUT` 或客户端级错误码，**不一定是原始 CQE 错误码**。
- `Check` 对已移除 / 未知 TaskId 返回 `true`，无法区分“不存在”和“已完成”。
- 正常完成但调用方从不 `Wait` 的任务，会一直留在 `ClientTaskManager` 的 map 中，直到 `Shutdown::Drain`。
- `CompleteWithError` 路径会把**所有** `entryStatus` 覆盖为同一个错误码。
- 预发送失败时：`AbortSubBatchesBeforeSend` 先把 task 级 `entryStatus` 全填 `CANCELED`，再由各子批的 `entryStatus` 覆盖；**失败之前已构建但从未发送的子批会保留 OK**。
- transport 侧 `finalStatus` 不只 OK / PARTIAL_FAILED：deadline 到期是 `TIMEOUT`，`Cancel` 是 `CANCELED`。

---

## 11. 读图时最容易看错的 8 个点

1. **顶层状态几乎总是 `PARTIAL_FAILED`**
   任何 transport task 失败都聚合为它，真实错误码只在 `entryStatus` 和日志里。100% 失败也不会是 `IO_ERROR` / `TIMEOUT`。

2. **Query 的 `exists` 在失败的 transport 分片上不合并**
   客户端只在 `completionStatus.ok()` 时写回 exists；失败分片保持初始 0，看起来像“未命中”。

3. **`AbortSubBatchesBeforeSend` 的语义**
   先把 task 级 `entryStatus` 全填 `CANCELED`，再由各子批的 `entryStatus` 覆盖；失败之前的子批从未发送，却保留 OK。

4. **单条 vs 批量的 SQE 数量**
   `ioNum=1` 让 `StoreAsync(N)` 变成 N 个 SQE；`BatchStoreAsync(N)` 是 `ceil(N/110)` 个。

5. **能力钳制只发生一次**
   复用旧 transport 不会重新读取服务端能力；只有新建 transport 才重新 clamp。

6. **`LoadAsync` 不能带 event，`StoreAsync` 可以**
   event 在唯一的 client worker 上阻塞等待，会造成队头阻塞。

7. **`Wait` 只此一次；`Check` 对未知 id 返回 true**
   `Wait` 超时也会移除任务，后台继续执行但结果不可再取；transport 的 `onComplete` 只持有 `weak_ptr`，late completion 会被静默丢弃。

8. **`retiredTransports_` 只在 Shutdown 清理**
   视图频繁变化的长期进程会累积旧 transport 的连接 / 线程 / 缓冲。

### 额外提醒

- 空输入或所有 key 路由失败时，任务会**直接 `Finalize` 为 OK**，完全不碰 transport。这些 entry 的 `entryStatus` 保持初始 OK，Query 的 `exists` 保持 0。
- `TaskId` 在 client 层和 transport 层各自独立编号，同一个数值没有关系。
- `KvClient` 没有取消 API；transport 层有 `Cancel(TaskId)`，但注释明确是 best-effort，不中断底层 UB/RoCE IO。
- `TransportTaskManager::BuildResult` 按子批顺序拍平，**不是调用方顺序**；调用方顺序由 `originalIndices` 在 client 层还原。
- `prefixHitKeys` 会计算两次，客户端 `Finalize` 的结果覆盖 transport 层的值。

---

## 12. 附录：关键源码索引

| 模块 | 文件 | 关键内容 |
|---|---|---|
| 公共接口 | `kv_semantics/include/kv_client.h` | 6 个数据 API、Check/Wait、内存注册 |
| 公共类型 | `kv_semantics/include/kv_types.h` | StatusCode、KVBuffer、MemoryRegion、KvClientConfig |
| 门面实现 | `kv_semantics/src/client/kv_client_impl.cc` | Init/Shutdown/Submit/内存注册/视图刷新 |
| 任务上下文 | `kv_semantics/src/common/utils/task_context.h` | ClientTask / TransportTask / SubBatch |
| 任务管理基类 | `kv_semantics/src/common/utils/task_manager_base.h` | TaskId 分配、map 管理 |
| Client 任务聚合 | `kv_semantics/src/client/task/task_manager.cc` | 路由拆分、完成回填、Finalize、Wait |
| 路由 | `kv_semantics/src/client/router/router.cc` | RingHash / Maglev / 连续块 / BatchTopK |
| 视图 | `kv_semantics/src/client/view/view_server.cc` | GetGlobalView、发布判据、刷新判据 |
| Transport 门面 | `kv_semantics/src/trans/kv_transport_impl.cc` | Init/Submit/两个 Loop/Shutdown |
| Transport 执行 | `kv_semantics/src/trans/task/trans_task_executor.cc` | Execute / Poll / Cancel |
| 子批构建 | `kv_semantics/src/trans/task/kv_submit_flow.cc` | 拆分子批、构建请求、Send |
| 切分粒度 | `kv_semantics/src/trans/task/io_scheduler.cc` | GetSqeIoNum、SplitForAsu |
| 请求构造 | `kv_semantics/src/trans/proto/sqe_request.cc` | Store/Retrieve/Batch/Delete/Exist/KeepAlive |
| 协议编解码 | `kv_semantics/src/trans/proto/kv_protocol.cc` | SQE 打包、CQE 解包、result buffer |
| 状态映射 | `kv_semantics/src/trans/utils/trans_task_utils.cc` | CQE / entry 状态映射 |
| Transport 结果 | `kv_semantics/src/trans/task/trans_task_manager.cc` | 子批聚合、BuildResult |
| 内存缓冲 | `kv_semantics/src/trans/buffer/buffer_manager.h` | slot 分配、双地址（local/device） |
| 连接管理 | `kv_semantics/src/trans/conn/connection_manager.h` | 连接选择、健康度、恢复 |
| Provider 接口 | `kv_semantics/src/trans_provider/trans_provider.h` | Register/Bind/Unregister/Send |
| 配置解析 | `kv_semantics/src/client/utils/config_utils.cc` | KvClientConfig 文件解析 |
| 单测 | `kv_semantics/tests/client/kv_client_impl_test.cc` | 路由、部分失败、内存注册 |
| 用法样例 | `kv_semantics/kv_test/src/kv_client_runner.cc` | 单条 / 批量调用模式 |
| UCM 消费方 | `ucm/store/asu/cc/asu_store.cc` | Lookup / Load / Dump 到 KvClient 的映射 |

### 完整调用链（一句话版）

```
KvClient API
  → ClientTaskManager（TaskId + map）
  → taskQueue_（SPSC）
  → Client WorkerLoop（路由 + 拆分 TransportTask）
  → AsuTransport::Submit
  → executeQueue_（SPSC）
  → Transport WorkerLoop（切子批 + 建 SQE + provider->Send）
  → CompletionLoop（轮询 flag buffer + 解 CQE）
  → TransportTaskManager::NotifyCompletion
  → ClientTaskManager::CompleteTransportTask（originalIndices 回填）
  → Finalize（OK / PARTIAL_FAILED）
  → Wait → TaskResult
```
