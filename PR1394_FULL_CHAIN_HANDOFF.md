# PR1394 全链路验证交接文档（DramPool → DramStore → Prometheus → Grafana）

> 日期：2026-09-23。本文档自包含，供无背景的 Agent 直接续跑。
> **Tier-3（真实 vLLM 推理）已于 09-21 跑通**，PR #1438 dashboard 已迭代至 v15（`e15bc740`），当前现场见第 14 节。
> 此前另一份 `PR1394_DRAMPOOL_DRAMSTORE_GRAFANA_E2E_HANDOFF.md` 是 09-20 的早期记录（含被推翻的结论），以本文档为准。

## 1. 目标与最终结果

验证 PR https://github.com/ModelEngine-Group/unified-cache-management/pull/1394（head `d981b46a`，基线 develop `d12b3b06`）：

**DramPool server（C++）真实 KV 流量 → 原子写 JSON metrics → DramStore（真实 C++ pipeline，torch NPU tensor 驱动）读取/上报 → ucmmetrics(C++) → PrometheusStatsLogger → exporter /metrics → Prometheus → Grafana 全量展示。**

**最终状态：全链路已跑通并保持运行。**

- 真实流量：12 轮 × (4 DUMP blocks + 5 LOOKUP + 4 LOAD)，每轮 LOAD 数据与 DUMP 内容逐字节校验一致
- exporter 上 dramstore_* 551 条 series（55+ 非零）、drampool_* 257 条 series（12/12/12/12 与流量一致）
- Grafana dashboard `pr1394-e2e`：12 个面板覆盖全量 dramstore + drampool 指标（throughput/outcomes/各阶段时延/容量/资源/失败计数/全量 counter 表）
- 三层数值一致：writer JSON = exporter = Prometheus = Grafana

## 2. 环境与关键路径

| 项 | 值 |
| --- | --- |
| SSH 宿主机 | `root@110.138.0.3`（密码 `huawei@1234`），Docker 在宿主机上 |
| 容器 | `codex_drampool_cann851`（**host 网络模式**，CANN/HIXL/HCCL 8.5.1，aarch64） |
| 隔离 checkout | `/home/codex/pr1394-e2e-20260920`（PR head `d981b46a` + 本文档第 6 节的临时改动，**不可推送**） |
| 构建目录 | `/home/codex/pr1394-e2e-20260920/build-pr1394-a2` |
| Python | `/home/codex/pr1396-validation-3c26b610-src/.venv/bin/python`（torch 2.9 + torch_npu 2.9 + prometheus_client + wrapt + yaml，唯一可用的完整环境） |
| 链路文件根 | 容器内 `/tmp/pr1394-chain/`；宿主机 `/tmp/pr1394-observability/` |
| NPU | server 用 device 4，DramStore worker 用 device 5（A2/910B3 共 8 卡） |

**端口分配（host 网络，全部在宿主机上）**：

| 端口 | 用途 |
| --- | --- |
| 9109 | exporter `/metrics`（runner 进程内 prometheus_client） |
| 9110 | Prometheus（容器 `pr1394-prom`） |
| 3110 | Grafana（容器 `pr1394-grafana`，admin/admin1394） |
| 8081 | Grafana image renderer（容器 `pr1394-renderer`） |
| 56200/56300 | drampool server two-sided control / one-sided transport id |
| 56201(+6=56207) / 56301(+6=56307) | DramStore worker control / transport id（worker 角色自动加 `device_id+1` 偏移） |
| 26666 / 36666(+1=36667) | drampool server / DramStore worker 的 HIXL engine listen port |

## 3. 链路架构与数据流

```
┌─ 容器内进程 1: drampool server (device 4) ────────────────────────────┐
│ drampool_e2e server 110.138.0.3:56200 110.138.0.3:56207              │
│                     110.138.0.3:56300 110.138.0.3:56307 4            │
│ env: DRAMPOOL_E2E_SLOT_COUNT=200 DRAMPOOL_E2E_QUEUE_DEPTH=128        │
│  ├─ 真实 KV 服务（TCP 56200 + HIXL one-sided 56300）                  │
│  └─ MetricsReporter: 原子写 /tmp/pr1394-chain/metrics/drampool_metrics.json │
└──────────────────────────────────────────────────────────────────────┘
                                    │ JSON 快照 (200ms 级)
┌─ 容器内进程 2: full-chain runner (device 5, venv python) ─────────────┐
│ /tmp/pr1394-chain/run_full_chain.py（本次新建，详见第 5 节）           │
│  ├─ UcmPipelineStore("Dram") = 真实 libdramstore.so C++ 流水线        │
│  │    (node_actor/task_manager/transport_executor/... 全真实)          │
│  ├─ 真实流量: torch NPU tensor dump/lookup/load + 数据校验             │
│  ├─ PrometheusStatsLogger: 注册 metrics_configs.yaml 全部 265 指标    │
│  ├─ DramPoolResourceReporter: 读 JSON 快照算 delta（真实 reader 类，   │
│  │    直接实例化绕过 device_id<0 guard —— 见第 6 节说明）              │
│  └─ prometheus_client start_http_server(9109)                         │
└──────────────────────────────────────────────────────────────────────┘
                                    │ /metrics (ucm: 前缀)
┌─ 宿主机容器 pr1394-prom (:9110) ── 5s 抓取 ───────────────────────────┐
└──────────────────────────────────────────────────────────────────────┘
                                    │ PromQL
┌─ 宿主机容器 pr1394-grafana (:3110) + pr1394-renderer (:8081) ─────────┐
│ datasource: pr1394-prom (uid afytgdwun8p34c)                          │
│ dashboard: uid pr1394-e2e，12 面板全量指标                             │
└──────────────────────────────────────────────────────────────────────┘
```

注意：Prometheus 会把指标名里的 `:` 规范化为 `_`（`ucm:drampool_*` → `ucm_drampool_*`），Grafana 查询必须用下划线名。

## 4. 已解决的三个关键根因（重要，勿重蹈覆辙）

1. **harness 未设 request_id**：`kv_protocol.cc` 的 `ValidateRequestHeader` 要求 `request_id != 0`，未设置时 `PackRequest` 静默失败（无日志），表现为 client 在 `ExchangeMetadata` 成功后几百微秒内退出。已修复：dump=1/lookup=2/load=3。
2. **harness 缺少 Connect**：`ExchangeMetadata` 只建路由不建 HIXL 连接；真实 DramStore 在 exchange 后调用 `manager_.Connect(Hixl, peer)`（`transport_manager_backend.cc:120`）。harness 已补上；full-chain runner 天然走真实路径无需处理。
3. **KV buffer 必须预注册**：DramStore 没有按请求注册内存的路径——dump/load 的源/目的设备地址必须通过配置 `gpu_kv_buffer_addrs`/`gpu_kv_buffer_sizes` 在 **store 构造时**（HIXL 连接建立前）注册，否则 server 端 RDMA 读 10 秒超时（`CompletionPoller data transfer timed out`，client 侧报 `RuntimeError: -50010`）。runner 已改为先 `torch.zeros(..., device="npu:5")` 再构造 store。

## 5. full-chain runner（`/tmp/pr1394-chain/run_full_chain.py`）

单进程包含全部消费侧组件（这是设计使然：C++ libdramstore 与 Python 侧通过 `_preload_metrics` 共享同一 libmetrics 注册表）：

```python
buf = torch.zeros((4, 4096), dtype=torch.uint8, device="npu:5")   # 先分配
store_config = {
    "store_pipeline": "Dram",
    "local_control_endpoint": "110.138.0.3:56201",   # base，worker 自动 +6
    "local_host": "110.138.0.3",
    "local_transport_manager_id": "110.138.0.3:56301",
    "device_id": 5, "hixl_listen_port": 36666,
    "node_control_endpoints": ["110.138.0.3:56200"],
    "node_transport_manager_ids": ["110.138.0.3:56300"],
    "max_io_entries": 128, "tensor_size_list": [4096],
    "gpu_kv_buffer_addrs": [buf.data_ptr()], "gpu_kv_buffer_sizes": [16384],
    ... 超时/router/线程数 ...
}
store = UcmPipelineStore(store_config)          # 真实 Dram pipeline
stats_logger = PrometheusStatsLogger("qwen3-pr1394-e2e", "worker-0", derived_yaml)
reporter = DramPoolResourceReporter(json_path, interval_sec=2)  # 直接实例化
reporter.start()
start_http_server(9109)
# 流量循环: dump_data → wait → lookup(含 miss) → load_data → wait → torch.equal 校验
```

派生 metrics 配置：由 `examples/metrics/metrics_configs.yaml` 加 `consumers.multiproc=true`、`log_interval=2` 生成 `/tmp/pr1394-chain/metrics_config.yaml`。

## 6. 临时改动边界（全部不可推送）

**A. 远端 checkout 内 test-only harness（`ucm/store/test/case/dram/drampool/drampool_e2e.cpp`）**：
- request_id 赋值 + `CLIENT_RUN/CLIENT_SEND` 阶段日志
- `Init()` 末尾补 `manager_.Connect(Hixl, serverOneSided)`
- `RunServer()` 支持 env 覆盖：`DRAMPOOL_E2E_SLOT_COUNT`（默认 1）、`DRAMPOOL_E2E_QUEUE_DEPTH`（默认 16）
- `ucm/store/dram/CMakeLists.txt` 临时加了 `drampool_e2e` target；`run_drampool_e2e.sh` 为 test 脚本

**B. 源码树内的构建产物副本（非源码修改，但 git status 会显示未跟踪文件）**：
- `ucm/shared/infra/ucmlogger.cpython-311-*.so`（target `ucmlogger`）
- `ucm/shared/metrics/ucmmetrics.cpython-311-*.so` + `libmetrics.so`（target `ucmmetrics`/`metrics`）
- `ucm/store/pipeline/ucmpipelinestore.cpython-311-*.so`（target `ucmpipelinestore`）
- `ucm/store/dram/libdramstore.so` + `libucm_p2p_transport.so`（后者是 RUNPATH `$ORIGIN` 依赖，必须放旁边）

**C. A2-only HIXL host-sync patch**（4 个 hixl 文件，约 +88/-20，`/tmp/pr1394-a2-adapted.patch` 有备份）——09-20 已应用，仅用于 A2 测试。

**D. 生产代码零修改**。所有缺陷都以 test-only 修复或 runner 侧规避。

## 7. 已发现的 PR 缺陷（建议反馈 PR 作者）

**DramPool server 重启可使 reporter 永久卡死**（09-20 两次复现，r9/r10）：

- 触发：server 重启后新实例计数归零；若 client 流量在 1-2 秒内恢复（zeros 快照窗口短于 reporter 2s 轮询，默认 15s 更必错失），reporter 错过 reset 检测；随后 `resource_reporter.py` 的 `snapshot_deltas` 比较"同桶计数、不同 sum"的直方图（如 `drampool_load_prepare_duration_ms` 0.059→0.076）抛 `Histogram sum changed without samples`。
- 恶化：`_collect_once` 全有或全无，异常导致 `_write_state` 永不执行 → state 永不前进 → 之后每 2s 与同一份陈旧 state 比较无限报错（实测连续 133 次）。**重启消费进程也无法恢复**（state 在 /dev/shm 持久）。
- 恢复手段：删 `/dev/shm/ucm_drampool_metrics_<hash>.json`，或让 server 空转写入全零快照 ~15s。
- 修复建议：`snapshot_deltas` 对该情形按 reset 处理（对齐 counter 语义）而非抛异常；或连续 N 次失败后强制重新基线；补"重置+同桶+不同 sum"单测。
- 本链路规避：drampool server 长驻不重启（当前 runner 已删旧 state 重新基线，稳态单调递增不会触发）。

另：`ucmstore.test` 因 PR 自身 `task_worker_test.cc` 未同步三参数 `ProcessDump/ProcessLoad/ProcessLookup` 无法编译（非本轮引入）。

**追加发现（09-21 14:26 当场验证，均为 metrics 问题）：**

**B. 两个 writer gauge 恒为 0 →（09-21 14:41 实验后修正定性：设计耦合，非埋点损坏）**
- 机制：`drampool_metadata_entry_count` 与 `drampool_buffer_pool_usage_ratio_<slot>` 的**唯一更新点是 `GCThreadLoop`**（`drampool_server.cc:558-576`，每 gcIntervalMs 默认 1s 刷新）；`gcEnabled=false` 时 GC 线程不启动（`drampool_server.cc:391` guard）→ gauge 保持初始 0。本 E2E harness 一直 `gcEnabled=false`，故此前所有轮次恒 0。
- 实验验证：`DRAMPOOL_E2E_GC_ENABLE=1` 重启 server（400 槽）+ 2 轮流量（8 blocks）后，JSON 中 `metadata_entry_count=8`、`buffer_pool_usage_ratio_4096=0.02`（=8/400，精确吻合），exporter 上静态名 gauge `metadata_entry_count=8.0` 正常导出。
- 保留的问题定性：**观测与 GC 策略耦合**——`gcEnabled=false` 是合法配置，但关闭后 resource gauge 静默冻结在 0 且无任何告警。建议 gauge 刷新与 GC 解耦（如在 dump/storeend 路径或 metrics reporter 采样时更新），或至少在 gcEnabled=false 时文档/日志声明该 gauge 不可用。

**C. 动态 gauge 在 Prometheus 导出链路上断路（真实缺陷，实验确认与 GC 无关）**
- `examples/metrics/metrics_configs.yaml` 注册的是字面占位符名 `drampool_buffer_pool_usage_ratio_<slot_size>`，而 writer 运行时产出动态名 `..._4096`。该值经 ucmmetrics(C++) 时因 stat 未注册被**静默丢弃**（`metrics.cc:86-91`：`ResolveMetricId` 失败 → INVALID_METRIC_ID → 直接 return，零日志），Python 侧 `_metric_mappings` 也只有占位符名 → /metrics 上只有空 HELP 行（`ucm:drampool_buffer_pool_usage_ratio__slot_size_`），永远不会有数据。
- GC 开启实验中该 gauge 在 JSON 里已有正确值 0.02，但 /metrics 依然没有它——**证明断路与 GC/问题 B 无关，是导出链路自身的双重断点（C++ 静默丢弃 + Python 映射缺失）**。
- 修复方向：reader/PrometheusStatsLogger 对动态名做模式匹配 lazy 注册，或消费端启动时按配置预注册各 slot size 名。

## 8. 当前运行现场（截至交接时）

| 组件 | 位置/命令 | 日志 |
| --- | --- | --- |
| drampool server | 容器内 nohup，PID 见 `pgrep -f drampool_e2e`，device 4 | `/tmp/pr1394-chain/full-server.log` |
| full-chain runner（含 exporter:9109） | 容器内 nohup，`pgrep -f run_full_chain.py`，device 5 | `/tmp/pr1394-chain/full-runner.log`（尾行 `ALL 12 ROUNDS PASSED` 后 idle 保活） |
| Prometheus | 宿主机容器 `pr1394-prom`，:9110 | 配置 `/tmp/pr1394-observability/prometheus.yml`，TSDB `promdata/`（注意目录需 777，容器内非 root） |
| Grafana | 宿主机容器 `pr1394-grafana`，:3110，admin/admin1394 | datasource `pr1394-prom` uid `afytgdwun8p34c`；dashboard uid `pr1394-e2e` |
| renderer | 宿主机容器 `pr1394-renderer`，:8081 | Grafana 渲染出图用 |

## 14. 当前运行现场（2026-09-23 18:10，PR #1438 v15 部署后）

### 14.1 现场组件

| 组件 | 状态 | 日志 |
| --- | --- | --- |
| drampool server | 运行中，PID `pgrep -f drampool_e2e`，device 4，20000×262144B 槽，GC on，TTL 60s | `/tmp/pr1394-chain/fresh-server.log` |
| vLLM (Qwen3-0.6B) | 运行中，device 5，:8300 | `/tmp/pr1394-chain/fresh-vllm.log` |
| traffic_loop | 运行中，每 5s 一个 ~1300-token 推理 | `/tmp/pr1394-chain/traffic.log` |
| Prometheus | `pr1394-prom` :9110，抓取 :8300 | `/tmp/pr1394-observability/prometheus.yml` |
| Grafana | `pr1394-grafana` :3110，provisioning 声明式 | `/tmp/pr1394-observability/grafana/dashboards/grafana_dram_metrics.json` |

### 14.2 当前 metrics 配置（关键！）

`/tmp/pr1394-chain/metrics_config_vllm.yaml` 是由**合并版** `examples/metrics/metrics_configs.yaml` 生成的。合并版 = #1394 的原始 YAML（含 20 个 drampool 指标）+ #1438 的新增项（`reply_buffer_capacity_bytes` gauge + 3 个直方图加 10000ms 桶）。

**重要教训**：不能用 #1438 的 YAML 整体替换 #1394 checkout 的——因为 #1438 基于 develop（不含 #1394），整替会删掉全部 drampool 指标定义，导致 C++ 注册表没有这些名字 → reporter 的 `update_stats` 被静默丢弃 → dashboard 全部 DramPool 面板 No data。

### 14.3 PR #1438 dashboard 迭代记录（v4→v15，按反馈逐版修复）

| 版本 | head | 修复内容 |
| --- | --- | --- |
| v4 | `14f3fdd4` | 初始版（rate+多名正则 422 冲突、legend 乱码占位符） |
| v5 | `73044ce5` | 拆逐指标 target、legend 清理、量级拆分 |
| v7 | `bd9cd576` | `or vector(0)` 兜底（健康=空变 0 平线） |
| v10 | `7edeb765` | le 正则白名单（引入回归：`"5"` 匹配不了 `"5.0"`） |
| v11 | `98d65365` | le 修复（`([.]0)?` 兼容写法） |
| v12 | `5b424806` | 精简（删 remote/total latency 面板） |
| v13 | `927a5d87` | 重组（删 Backlog/reliability，新增 connection and recovery） |
| v14 | `a7dbe148` | 代码改动：reply_service.cc 新增 capacity 指标 + 直方图加 10000ms 桶 |
| v15 | `e15bc740` | UI 微调（删 timepicker）；**配合合并版配置部署后 45/49 有数据** |

### 14.4 当前已知问题清单（截至 v15 验证）

| # | 问题 | 定性 | 影响 |
| --- | --- | --- | --- |
| 1 | **缺陷 A：reporter 毒化 state 永久卡死** | PR #1394 代码 bug（`resource_reporter.py` `snapshot_deltas`） | server 重启后 reporter 可能永久卡死（重启消费进程不恢复）；本链路通过"重启 server 前删 state"规避 |
| 2 | **缺陷 B：GC 关闭时 gauge 恒 0** | PR #1394 设计耦合（gauge 刷新只在 GCThreadLoop） | `gcEnabled=false` 时 `metadata_entry_count` / `buffer_pool_usage_ratio_*` 冻结在 0 |
| 3 | **缺陷 C：动态 gauge 导出断路** | PR #1394 代码 bug（C++ 静默丢弃 + Python 映射缺失） | `buffer_pool_usage_ratio_262144` 永远到不了 /metrics |
| 4 | **"健康=空"（dramstore 事件驱动指标）** | 机制差异（dramstore C++ 事件管道 vs drampool reporter 快照管道） | 失败/超时类 counter（connect/fence/stale 等 7 个）健康时无序列；dashboard 已用 `or vector(0)` 兜底 |
| 5 | **Prerequisite 指标无 C++ 埋点** | #1396 代码不在本 checkout | `dump_prerequisite_duration_ms` 永空 |
| 6 | **vLLM Engine 周期性卡顿** | vLLM 0.18 + vllm-ascend 侧问题（与 UCM 无关） | 约 40min~2h 一次卡顿（API 活但推理超时），重启 vLLM 恢复；累计 6 次 |
| 7 | **capacity 耗尽是默认行为** | 设计（DramStore dump `ttl=0` → server 默认 120min TTL） | 持续流量 ~30min 填满池后 dump NoSpace 失败（推理不受影响）；已配 `DRAMPOOL_E2E_DUMP_TTL_MS=60000` 使 60s 过期回收进入动态平衡 |

### 14.5 快速续跑步骤

```bash
# === 容器内 ===
cd /home/codex/pr1394-e2e-20260920
source /usr/local/Ascend/cann/set_env.sh
export UCM_LOG_LEVEL=info

# 1. 全停
pkill -f traffic_loop; for i in 1 2 3; do pkill -9 -f "entrypoints.cli"; pkill -9 -f "EngineCore"; sleep 1; done
pkill -f "drampool_e2e server"; sleep 3

# 2. 清 state
rm -f /dev/shm/ucm_drampool*.json /dev/shm/ucm_blocksize_*
rm -rf /tmp/pr1394-chain/metrics /tmp/pr1394-chain/vllm-multiproc
mkdir -p /tmp/pr1394-chain/metrics /tmp/pr1394-chain/vllm-multiproc

# 3. 启 server
DRAMPOOL_E2E_GC_ENABLE=1 DRAMPOOL_E2E_SLOT_BYTES="262144" DRAMPOOL_E2E_SLOT_COUNT=20000 \
DRAMPOOL_E2E_QUEUE_DEPTH=512 DRAMPOOL_E2E_DUMP_TTL_MS=60000 \
DRAMPOOL_E2E_EXTRA_MAP="110.138.0.3:57201=110.138.0.3:57301,110.138.0.3:57202=110.138.0.3:57302" \
nohup ./build-pr1394-a2/ucm/store/dram/drampool_e2e server \
  110.138.0.3:57200 110.138.0.3:57207 110.138.0.3:57300 110.138.0.3:57307 4 \
  > /tmp/pr1394-chain/server.log 2>&1 &
# 等 DRAMPOOL_E2E_SERVER_READY

# 4. 启 vLLM (~4 分钟)
export PYTHONPATH=$PWD:$PYTHONPATH
export LD_LIBRARY_PATH=$PWD/ucm/store/dram:$LD_LIBRARY_PATH
export PROMETHEUS_MULTIPROC_DIR=/tmp/pr1394-chain/vllm-multiproc
export ASCEND_RT_VISIBLE_DEVICES=5
VLLMPY=/home/codex/pr1396-validation-3c26b610-src/.venv/bin/python
MODEL=/home/codex/models-hf/models--Qwen--Qwen3-0.6B/snapshots/c1899de289a04d12100db370d81485cdf75e47ca
nohup $VLLMPY -c "from vllm.entrypoints.cli.main import main; main()" serve "$MODEL" \
  --served-model-name qwen3-0.6b --port 8300 --max-model-len 4096 --gpu-memory-utilization 0.85 \
  --kv-transfer-config '{"kv_connector":"UCMConnector","kv_role":"kv_both","kv_connector_extra_config":{"UCM_CONFIG_FILE":"/tmp/pr1394-chain/ucm_config_vllm.yaml"}}' \
  > /tmp/pr1394-chain/vllm.log 2>&1 &
# 等 startup complete

# 5. 启流量
nohup /home/codex/pr1396-validation-3c26b610-src/.venv/bin/python /home/codex/dl-work/traffic_loop.py \
  > /tmp/pr1394-chain/traffic.log 2>&1 &

# === 验证 ===
curl -s http://127.0.0.1:8300/metrics | grep -c "^ucm:"   # 应 >200
# Grafana: http://localhost:3110/d/ucm-dram-metrics/（需 SSH 隧道）
```

### 14.6 注意事项（新增）

1. **不要用 #1438 的 YAML 整替 #1394 checkout 的**——#1438 基于 develop（不含 drampool 指标），会断管道。正确做法：以 #1394 原版为基底，只叠加 #1438 的新增项。
2. **vLLM 卡顿检测**：`tail traffic.log` 若连续 `timed out` → 重启 vLLM（按 14.5 步骤 4）。server 不需要重启（除非也出问题）。
3. **#1438 dashboard 更新流程**：`git fetch upstream pull/1438/head` → `git show FETCH_HEAD:examples/metrics/grafana_darm.json` → 适配 `DS_PROMETHEUS` 默认值为 `pr1394-prom` → 放入 `/tmp/pr1394-observability/grafana/dashboards/grafana_dram_metrics.json`（provisioning 自动加载，10s 生效）。

## 9. 重启/复现全链路的步骤（从零）

```bash
# === 宿主机（SSH 后直接执行，docker 在宿主机）===
# 1. Prometheus / Grafana / renderer（已配置则跳过；promdata 必须 777）
docker start pr1394-prom pr1394-grafana pr1394-renderer   # 或按第 8 节 docker run 重建

# === 容器内（docker exec -i codex_drampool_cann851 bash -s）===
cd /home/codex/pr1394-e2e-20260920
source /usr/local/Ascend/cann/set_env.sh
export UCM_LOG_LEVEL=info
export LD_LIBRARY_PATH=$PWD/ucm/store/dram:$LD_LIBRARY_PATH

# 2. 清理旧进程与毒化 state（重要！见第 7 节）
pkill -f run_full_chain.py; pkill -f drampool_e2e
rm -f /dev/shm/ucm_drampool*.json /dev/shm/ucm_drampool*.lock

# 3. drampool server（长驻）
DRAMPOOL_E2E_SLOT_COUNT=200 DRAMPOOL_E2E_QUEUE_DEPTH=128 \
nohup ./build-pr1394-a2/ucm/store/dram/drampool_e2e server \
  110.138.0.3:56200 110.138.0.3:56207 110.138.0.3:56300 110.138.0.3:56307 4 \
  > /tmp/pr1394-chain/full-server.log 2>&1 &
# 等待日志出现 DRAMPOOL_E2E_SERVER_READY

# 4. full-chain runner（真实 DramStore 流量 + exporter）
nohup /home/codex/pr1396-validation-3c26b610-src/.venv/bin/python \
  /tmp/pr1394-chain/run_full_chain.py > /tmp/pr1394-chain/full-runner.log 2>&1 &
# 等待 "ALL 12 ROUNDS PASSED"

# === 三层验证 ===
curl -s http://127.0.0.1:9109/metrics | grep -E "^ucm:(dramstore_dump_tasks_succeeded|drampool_dump_requests)_total"
curl -s -G 'http://127.0.0.1:9110/api/v1/query' --data-urlencode 'query=ucm_drampool_dump_requests_total'
curl -s -u admin:admin1394 -X POST -H 'Content-Type: application/json' \
  'http://127.0.0.1:3110/api/ds/query' -d '{"queries":[{"refId":"A","datasource":{"type":"prometheus","uid":"afytgdwun8p34c"},"expr":"ucm_drampool_dump_requests_total","instant":true,"intervalMs":5000,"maxDataPoints":1}],"from":"now-5m","to":"now"}'
```

## 10. 常见坑（已踩过）

1. **宿主机 `/tmp/bisect.py` 遮蔽标准库**：在 /tmp 直接 `python3 脚本.py` 会 import 到垃圾模块。任何宿主机 python 脚本放到 `/tmp/pr1394-observability/` 等干净目录执行。
2. **libdramstore 依赖**：RUNPATH `$ORIGIN`，`libucm_p2p_transport.so` 必须与 `libdramstore.so` 同目录（已复制到 `ucm/store/dram/`）。
3. **worker 端口偏移**：DramStore worker（device_id≥0）的 control/manager 端口自动加 `device_id+1`（device 5 → +6），hixl_listen_port +1；drampool server 的 `twoSidedToOneSided` 必须配置**偏移后**的 client 端点。
4. **prometheus TSDB 权限**：promdata 目录需 chmod 777（容器内非 root 用户）。
5. **reporter 毒化 state**：server 重启前先删 `/dev/shm/ucm_drampool*.json`（见第 7 节）。
6. **venv 是唯一可用 python**：容器系统 python 缺 wrapt 等；宿主机 python3.9 缺 prometheus_client。
7. **Prometheus 指标名下划线**：查询用 `ucm_drampool_*`，不是 `ucm:drampool_*`。

## 11. 清理命令（验证结束后）

```bash
# 宿主机
docker rm -f pr1394-prom pr1394-grafana pr1394-renderer
rm -rf /tmp/pr1394-observability
# 容器内
pkill -f run_full_chain.py; pkill -f drampool_e2e
rm -rf /tmp/pr1394-chain /dev/shm/ucm_drampool*
# 隔离 checkout 恢复（删除源码树 .so 副本 + harness 改动 + A2 patch）
cd /home/codex/pr1394-e2e-20260920
rm -f ucm/shared/infra/*.so ucm/shared/metrics/*.so ucm/store/pipeline/*.so \
      ucm/store/dram/*.so
git checkout -- ucm/store/dram/CMakeLists.txt
git clean -fd ucm/store/test/case/dram/drampool/
# 如需完全回基线: git reset --hard refs/codex/a2-e2e-baseline
```

## 12. 遗留待办（可选下一步）

- 第 7 节缺陷的正式修复（PR 代码改动，需与作者沟通）
- `ucmstore.test` 的 test 代码三参数同步（PR 自身问题）
- ~~真实 vLLM 推理级验证~~ → **已完成（第 13/14 节）**
- ~~`drampool_metadata_entry_count` gauge 语义待确认~~ → 已升级为第 7 节追加发现 B/C，连同毒化 state 一起反馈 PR 作者
- vLLM Engine 周期性卡顿根因排查（累计 6 次，约 40min~2h 间隔，症状一致：API 活但推理超时，重启恢复）——vLLM 0.18 + vllm-ascend 侧问题，与 UCM 链路无关

## 13. Tier-3：真实 vLLM 推理全链路（2026-09-21 16:50 跑通，当前运行现场）

### 13.1 架构与最终结果

```
vLLM serve (Qwen3-0.6B, vllm 0.18.0 + vllm-ascend, device 5, :8300)
 ├─ APIServer 进程: /metrics (PROMETHEUS_MULTIPROC_DIR 聚合, 264 个 ucm: 系列)
 └─ EngineCore 进程: UCMConnector(vllm_ascend 注册) → UcmPipelineStore("Dram")
     ├─ DramStore worker (device 5, libdramstore.so 真实流水线)
     └─ DramStore scheduler (device -1, 含 DramPoolResourceReporter)
          ↕ HIXL (A2 host-sync mock, 32KB 拆段)
drampool_e2e server (device 4, GC on, 262144B 槽 × 3000, :57200/:57300)
 └─ MetricsReporter 原子写 /tmp/pr1394-chain/metrics/drampool_metrics.json
Prometheus (pr1394-prom :9110, 抓 8300) → Grafana (pr1394-grafana :3110)
```

**验证证据**：真实推理请求（264-token prompt）→ DUMP 28 层 KV 入 DramPool（上一进程累计 2352 dumps / 3000 entries / 零失败）→ 冷重启 vLLM 后重放 → `hit external: 2` → **LOAD 28 层全部成功**（`dramstore_load_tasks_succeeded_total=28`，零 miss）→ 生成文本正确。vLLM /metrics 上 `ucm:` 系列 264 个（真实标签 `model_name="qwen3-0.6b"`），Prometheus 912 个 ucm series，Grafana dashboard `pr1394-e2e` 已更新为 Tier-3 版（11 面板：vLLM 吞吐/命中率、DramStore 任务/时延、DramPool 请求/资源/传输时延、全量 counter 表）。

### 13.2 Tier-3 踩坑与修复（按发现顺序，全部有代码/日志证据）

1. **vllm-ascend wrapper 漏传 kv_cache_config**：`/vllm-workspace/vllm-ascend/vllm_ascend/distributed/kv_transfer/kv_pool/ucm_connector.py:37` 构造 `UCMConnector(vllm_config, role)` 没传第三参 → `register_kv_caches` 时 `'NoneType' has no attribute 'num_blocks'`。已改为 `ImplCls(vllm_config, role, kv_cache_config)`（环境侧补丁，原文件备份 `.bak-pr1394`）。
2. **worker 逻辑 device 与端口偏移**：`ASCEND_RT_VISIBLE_DEVICES=5` 使 EngineCore 内 device_id=0，worker 控制端口 = base 57201 + 0 + 1 = **57202**（不是物理设备推的 57207）；scheduler 用 base 57201 无偏移。server 的 `DRAMPOOL_E2E_EXTRA_MAP` 需同时含 `57201→57301` 和 `57202→57302`。
3. **vllm-ascend 无视 `--block-size 16` 强制 128**（`FullAttentionSpec(block_size=128)`）→ 每 tensor 每块固定 **262144B**，server 池必须按此尺寸建（`Published store block_size 14680064 = 56×262144`）。
4. **A2 host-sync 单段 >32KB 报 507001（ACL_ERROR_RT_TS_ERROR）**：对照实验证明 4KB/32KB 连续段成功、256KB 失败。修复：`hixl_instance.cpp` 的 `ExecuteTransferSync` 拆成 ≤32KB 子段逐个同步传（仍为同步语义，属 A2 mock 一部分；probe 验证 256KB×4 段 DUMP 0.10s + LOAD 校验一致）。
5. **`metrics_config_path` 必须放在 UCM yaml 顶层（metrics 缺陷级：静默失败）**：`load_launch_metrics_config` 读 launch_config 顶层，放在 `ucm_connector_config` 里会被**静默忽略**——无任何告警日志说明配置未生效，唯一可观察症状是 `/metrics` 上没有 `ucm:` 系列。错误放置时 fallback 到默认配置（只启用 vllm_connector 消费者）→ PrometheusStatsLogger 不启动。修正后日志出现 `UCM metrics enabled for multiproc, vllm_connector: total=265`。建议 PR 作者：找不到配置时至少打 WARN，或对嵌套位置做兼容。
6. **指标暴露机制**：`PROMETHEUS_MULTIPROC_DIR=/tmp/pr1394-chain/vllm-multiproc`（启动前清空），EngineCore 写 mmap 文件、API server 的 MultiProcessCollector 聚合（vllm/v1/metrics/prometheus.py 尊重用户设置的 dir）。
7. **server 重启后 vLLM DramStore 不会自动重连**（连接僵死且 recovery 不触发）→ **每次重启 drampool server 必须重启 vLLM**（约 4 分钟）。同时重启 server 前删 `/dev/shm/ucm_drampool*.json` 防 reporter 毒化（第 7 节缺陷）。
8. **LOAD 触发方法**：vLLM 本地 HBM prefix cache 优先，冲刷无效（3698 块池难驱逐）。可靠方法：**重启 vLLM（本地缓存清空）后立刻重放原 prompt** → `hit external` → LOAD。
9. **未解之谜（记录备查）**：4 个分散 MR 注册的 client 在 connect 阶段报 HIXL 503900（单 MR 正常、vLLM 56 MR 也正常）；不影响本链路（vLLM 的 dump 段是层内连续地址）。
10. **容器内 /tmp 也有垃圾 .py**（bisect.py 等）遮蔽标准库——任何 python 脚本从干净目录跑。
11. **dump 大量 nospace 后 DramStore 退避（含可观测性缺口）**：池满（nospace=2154 历史）后新 dump 不再发出（health breaker 接管）。**观测盲区**：退避状态没有专门指标或显著日志，只能靠 `drampool_dump_requests_total` 停止增长间接推断——生产中容易误判为"没流量"。打流量前确认池有余量或重启 server。
11b. **持续流量的容量耗尽是默认行为（17:44 复现）**：DramStore dump 请求 `ttl=0`（node_actor.cc:124）→ server 用默认 `defaultDumpTtlMs=120 分钟`（drampool_config.h:42-43），TTL 型驱逐策略下未过期条目不可回收 → 20000 槽约 30 分钟被填满 → 之后全部 dump NoSpace 失败（failed 与 nospace 计数一一对应），**推理不受影响**（仅缓存不落盘）。修复：harness 新增 `DRAMPOOL_E2E_DUMP_TTL_MS` env，重启 server 用 60s TTL → entry count 进入 ~6000 动态平衡、nospace 归零。生产提示：需按流量速率做容量规划，或缩短 TTL/调整驱逐策略。
12. **Prometheus 原样保留冒号名指标（易踩坑）**：metrics 配置默认 `metric_prefix: "ucm:"` → exposition/存储名均为 `ucm:drampool_*`（带冒号，Prometheus 2.53 不转下划线），Grafana/PromQL 查询必须用冒号名（`ucm:drampool_dump_requests_total{...}` 在 PromQL 中合法）。下划线查询返回空且无任何报错——表现为面板 No data。根治方案（未实施）：metrics 配置把前缀改为 `ucm_`。dashboard 生成脚本已用冒号名（`/tmp/pr1394-observability/make_tier3_dashboard_v2.py`）。
13. **reporter 在真实 vLLM 进程内验证正常（正面结论）**：DramPoolResourceReporter 在 vLLM scheduler 进程（device -1，guard 放行）持续读取 JSON 快照并正确上报增量（持续流量下 `ucm:drampool_dump_requests_total` 从 0 稳步涨到 2000+）；worker 进程（device 0）被 guard 正确跳过，不重复上报。第 7 节毒化缺陷在本次长跑中未触发（server 未重启）。

### 13.3 关键文件与启动命令

| 项 | 值 |
| --- | --- |
| 模型 | `/home/codex/models-hf/models--Qwen--Qwen3-0.6B/snapshots/c1899de289a.../`（hf-mirror 下载） |
| UCM 配置 | `/tmp/pr1394-chain/ucm_config_vllm.yaml`（**metrics_config_path 在顶层**） |
| metrics 配置 | `/tmp/pr1394-chain/metrics_config_vllm.yaml`（multiproc: true, log_interval 2） |
| 当前运行日志 | vLLM：`/tmp/pr1394-chain/live-vllm.log`；server：`/tmp/pr1394-chain/live-server.log`（池 20000 槽）；流量：`/tmp/pr1394-chain/traffic.log` |
| 持续流量脚本 | 容器内 `/home/codex/dl-work/traffic_loop.py`（每 5s 一个 ~1300-token 请求，新 prompt 触发 dump、定期 replay 触发 prefix lookup；`pkill -f traffic_loop` 停止） |
| dashboard | Grafana uid `pr1394-e2e`，**已切换为 provisioning 声明式管理**：JSON 源文件 `/tmp/pr1394-observability/grafana/dashboards/pr1394-tier3.json`（**改文件 10s 内自动生效**，无需 API/脚本）；provider/datasource 配置在 `/tmp/pr1394-observability/grafana/provisioning/`；旧生成脚本 `make_tier3_dashboard_v2.py` 保留作参考 |
| ucm_patch.pth | 已复制到 venv site-packages（import vllm 时自动 apply patch） |

启动顺序（容器内，`source /usr/local/Ascend/cann/set_env.sh` 后）：

```bash
# 1. drampool server
rm -f /dev/shm/ucm_drampool*.json && rm -rf /tmp/pr1394-chain/metrics && mkdir -p /tmp/pr1394-chain/metrics
DRAMPOOL_E2E_GC_ENABLE=1 DRAMPOOL_E2E_SLOT_BYTES="262144" DRAMPOOL_E2E_SLOT_COUNT=3000 \
DRAMPOOL_E2E_QUEUE_DEPTH=512 \
DRAMPOOL_E2E_EXTRA_MAP="110.138.0.3:57201=110.138.0.3:57301,110.138.0.3:57202=110.138.0.3:57302" \
nohup ./build-pr1394-a2/ucm/store/dram/drampool_e2e server \
  110.138.0.3:57200 110.138.0.3:57207 110.138.0.3:57300 110.138.0.3:57307 4 \
  > /tmp/pr1394-chain/final-server.log 2>&1 &

# 2. vLLM（约 4 分钟就绪）
export PYTHONPATH=/home/codex/pr1394-e2e-20260920:$PYTHONPATH
export LD_LIBRARY_PATH=/home/codex/pr1394-e2e-20260920/ucm/store/dram:$LD_LIBRARY_PATH
export PROMETHEUS_MULTIPROC_DIR=/tmp/pr1394-chain/vllm-multiproc
rm -rf $PROMETHEUS_MULTIPROC_DIR && mkdir -p $PROMETHEUS_MULTIPROC_DIR
export ASCEND_RT_VISIBLE_DEVICES=5
VLLMPY=/home/codex/pr1396-validation-3c26b610-src/.venv/bin/python
nohup $VLLMPY -c "from vllm.entrypoints.cli.main import main; main()" serve <MODEL_DIR> \
  --served-model-name qwen3-0.6b --port 8300 --max-model-len 4096 --gpu-memory-utilization 0.85 \
  --kv-transfer-config '{"kv_connector":"UCMConnector","kv_role":"kv_both","kv_connector_extra_config":{"UCM_CONFIG_FILE":"/tmp/pr1394-chain/ucm_config_vllm.yaml"}}' \
  > /tmp/pr1394-chain/final-vllm2.log 2>&1 &

# 3. 流量
curl http://127.0.0.1:8300/v1/completions -H "Content-Type: application/json" \
  -d '{"model":"qwen3-0.6b","prompt":"<264+ tokens>","max_tokens":20,"temperature":0}'
# LOAD：重启 vLLM 后重放同一 prompt
```

Prometheus 配置已含两个 job（`/tmp/pr1394-observability/prometheus.yml`：9109 旧 exporter 已死可删、8300 vLLM）。Grafana dashboard `pr1394-e2e` 查询已改用 `job="pr1394-vllm-qwen3"` 过滤（不再硬编码 model/worker label）。Tier-3 截图：宿主机 `/tmp/pr1394-observability/tier3-dashboard.png`，本地 `C:\Users\Xuuuuuun\AppData\Local\Temp\opencode\tier3-dashboard.png`。

### 13.4 Tier-3 增加的临时改动边界

- **A2 mock 扩展**（`hixl_instance.cpp` ExecuteTransferSync 拆段 ≤32KB）——test-only，随 A2 patch 一起不推送
- **vllm-ascend wrapper 补丁**（kv_cache_config 透传）——环境侧，备份在同目录 `.bak-pr1394`
- harness 新增 env：`DRAMPOOL_E2E_SLOT_BYTES`（逗号分隔多池）、`DRAMPOOL_E2E_EXTRA_MAP`（额外端口映射）
- 模型下载到 `/home/codex/models-hf/`，ucm_patch.pth 装进 venv site-packages

### 13.6 事故记录（2026-09-22 16:20）：server 被外部替换成旧二进制

- 现象：dump 持续 `wait for dump kv cache failed`，server 日志出现 `TransferSync(..., ops=20) returned 507001`（`ops=` 格式 = **拆段修复前**的旧代码日志；拆段版日志为 `chunk=... of segment_bytes=...`）。
- 根因：昨天启动的 server（PID 2034912）在晚间被外部重启成 PID 2063533，运行的是**旧构建产物**（拆段修复未包含）。这台是共享机器，可能有其他人的自动化或手工操作。
- 修复：重新编译 + 按 13.3 完整流程重启（server + vLLM），验证 `dump_requests == dump_tasks_succeeded == 1708`、`failed_entries=0`、零 507001、出现 `chunk=` 格式日志。
- **预防**：续跑前先验证运行中 server 是否为新二进制——`grep -c "chunk=" <server-log>`（≥1 为新版）或看 507001 是否出现；发现旧版立即按 13.3 重启。

### 13.7 PR #1438 最新版 dashboard 部署（2026-09-22 16:35）

- PR #1438 已 force-update（head `73044ce5`）：相对 develop 仅 `examples/metrics/grafana_darm.json`（新版 774 行，12 面板，uid `ucm-dram-metrics`，查询全部为 `ucm:dramstore_*`/`ucm:drampool_*` 家族——**#1394 栈可直接供数，无需重建**；#1437 已合入 develop 但其 kv_semantics 指标本链路不产生）。
- 部署：新版 JSON 已适配 `DS_PROMETHEUS` 默认值 → `pr1394-prom`，放入 provisioning 目录 `/tmp/pr1394-observability/grafana/dashboards/grafana_dram_metrics.json`（旧版 31 面板文件已删除）。访问 `/d/ucm-dram-metrics/`。
- 注意：dashboard 按 `engine`/`worker_rank` label 聚合——本环境 UCM 指标只有 `worker_id` label，相关分组会显示无标签序列（不影响数值）。

### 13.5 清理（在原第 11 节基础上追加）

```bash
# 容器内
pkill -9 -f "entrypoints.cli"; pkill -9 -f "EngineCore"; pkill -f drampool_e2e
rm -rf /tmp/pr1394-chain /home/codex/models-hf /dev/shm/ucm_drampool* /dev/shm/ucm_blocksize_*
# vllm-ascend wrapper 还原
cp /vllm-workspace/vllm-ascend/.../ucm_connector.py.bak-pr1394 /vllm-workspace/vllm-ascend/.../ucm_connector.py
rm /home/codex/pr1396-validation-3c26b610-src/.venv/lib/python3.11/site-packages/ucm_patch.pth
# hixl 拆段改动随 checkout 一起 reset
```
