# A2 服务器 vLLM + DramStore/DramPool + Grafana 端到端联调总结

> 联调时间：2026-09-29 ~ 2026-09-30
> 分支：`z00984767-0917`（HEAD `187e55c`，含 `3fc31e2 Refine DramPool metrics`）
> 环境：远程 A2 服务器 `110.138.0.3`，容器 `z00984767_ascend_test`
> 一键脚本：`e2e_dramstore_all_in_one.sh`（已放入容器 `/root/z00984767/0917/e2e-run/`）

---

## 一、结论

**PR 1394 的 DramPool metrics 全链路，在真实 vLLM 推理上验证通过。**

真实请求 → DramStore → DramPool（C++ writer）→ `drampool_metrics.json` → `DramPoolResourceReporter` → `ucmmetrics` → vLLM `/metrics`（`ucm:drampool_*`）→ Prometheus → Grafana，四层数值一致。

| 指标 | writer JSON | vLLM /metrics | Prometheus | Grafana |
| --- | --- | --- | --- | --- |
| `drampool_dump_requests_total` | 168 | 168 | 168 | ✓ |
| `drampool_lookup_requests_total` | 3 | 3 | 3 | ✓ |
| `drampool_load_requests_total` | 0 | 0 | 0 | ✓ |
| `drampool_lookup_miss_entries_total` | 57 | 57 | 57 | ✓ |
| `drampool_metadata_entry_count` | — | 296 | 296 | ✓ |
| `drampool_buffer_pool_usage_ratio` | — | 0.1137 | 0.1137 | ✓ |

规模：vLLM `/metrics` 暴露 **1163 个 `ucm:` 系列**（其中 drampool 专项 376 个），Prometheus 收到 **114 个 drampool 指标名**，Grafana 面板 12 个，datasource health OK。

**这是历史上第一次跑通到这一层。** 此前所有尝试（见 `PR1394_DRAMPOOL_DRAMSTORE_GRAFANA_E2E_HANDOFF.md` 及本次之前的多轮）都卡在 `transport control peer rejected request`，`DramStore node connected` 从未出现过。

**一个未达成的点**：HIXL 数据面传输失败（`ACL 507001`），所以 `load_requests_total=0`、KV 没能真正落到 DRAM 里被读回。详见第六节。指标链路不受此影响——counters/gauges 由请求计数和内存分配驱动，已完整跑通。

### 访问地址

```
Grafana     http://110.138.0.3:3000/d/drampool-e2e-z009     (admin / admin1394)
Prometheus  http://110.138.0.3:9090/targets
vLLM        http://110.138.0.3:7850/metrics
```

---

## 二、环境

| 项 | 值 |
| --- | --- |
| 服务器 | `110.138.0.3`（ssh root，host 网络） |
| 容器 | `z00984767_ascend_test`，镜像 `z00984767_ascend_img:mount0928` |
| 容器 OS | openEuler 24.03 LTS-SP3，aarch64，host 网络模式 |
| NPU | 昇腾 910B3 × 8，driver 25.5.2 |
| CANN | **9.1.0-beta.1**（`ascend-toolkit/latest` 指向它）；容器内另有 8.5.1 但不可用，见下 |
| HIXL / HCCL | 9.1.0-beta.1（**不是** A2 patch 注释里假设的 8.5.1） |
| Python | venv `/home/z00984767/venv1394`，Python 3.11.14，`--system-site-packages` |
| vLLM | 0.18.0+empty（源码在 `/vllm-workspace/vllm`） |
| vllm-ascend | 0.18.0rc1 |
| torch / torch_npu | 2.9.0+cpu / 2.9.0.post1+gitee7ba04 |
| uc-manager | 0.8.0，editable 安装指向仓库根 |
| 模型 | Qwen3-0.6B，`/home/codex/models-hf/models--Qwen--Qwen3-0.6B/snapshots/c1899de2...` |
| 仓库 | `/root/z00984767/0917/unified-cache-management` |
| 工作目录 | `/root/z00984767/0917/e2e-run`（配置/日志/监控数据，不污染仓库） |
| 监控二进制 | 容器内已有：`/tmp/z009-extract/prometheus`（2.53.0）、`/tmp/z009-extract/grafana-home/bin/grafana`（11.1.0） |

### 设备拓扑（本次成功的关键）

三方必须各占一张不同的卡，否则 HIXL 直接拒绝：

| 角色 | 设备 | 由谁决定 |
| --- | --- | --- |
| vLLM worker（模型） | **device 0** | vLLM 自己选（未设 `ASCEND_RT_VISIBLE_DEVICES`） |
| UCM scheduler 运行时 | **device 2** | 编译进 `config.h::RuntimeDeviceId()` |
| drampool 数据面 | **device 3** | `drampool.yaml` 的 `transport.device_ids` |

**本机实测只有 device 0 / 2 / 3 可用**，1/4/5/6/7 在 `aclrtSetDevice` 时挂起（被宿主机上其它十几个容器占用）。这个约束是动态的，换机器或换时段都要重新探测——一键脚本的 `--check` 会自动做。

### KV 布局（决定 `--kvcache-block-sizes`）

Qwen3-0.6B（28 层、8 KV 头、head_dim 128、bf16）+ `--block-size 128`：

```
tensor_size_list = [262144, 262144]      # K/V 各 128 token
shard_size       = 524288
block_size       = 14680064              # 28 层 × 524288
```

`BufferManager::Allocate` 用 `GetPool(size)` 做**精确匹配**（`pools_.find(size)`），不是"最小可容纳"。所以 `--kvcache-block-sizes` 必须包含实际会请求的尺寸，本次用 `7340032 14680064 524288 262144`。

---

## 三、操作步骤

一键脚本已覆盖全部流程：

```bash
# 容器内
cd /root/z00984767/0917/e2e-run
bash e2e_dramstore_all_in_one.sh            # 全流程（约 4 分钟）
bash e2e_dramstore_all_in_one.sh --check    # 只检查环境 + 服务状态
bash e2e_dramstore_all_in_one.sh --status   # 只看运行状态和访问地址
bash e2e_dramstore_all_in_one.sh --req      # 服务已在跑，只补发请求 + 三层校验
bash e2e_dramstore_all_in_one.sh --build    # 改了 C++ 后重新编译
bash e2e_dramstore_all_in_one.sh --stop     # 停掉全部（含监控）
```

`all` 的七个步骤：检查环境 → 生成配置 → 停旧进程 → 起 drampool → 起 vLLM → 起 Prometheus+Grafana → 发请求并做三层证据校验。

### 手动等价命令（脚本内部做的事）

**1. 编译**（改过 `config.h` 之类的 C++ 头文件后必须重做）

```bash
cd /root/z00984767/0917/unified-cache-management
source /usr/local/Ascend/ascend-toolkit/set_env.sh
cmake --build build-pr1394-new --target ucm_p2p_transport dramstore drampool -j8
# 必须先停进程再拷贝，否则 "Text file busy" 会静默保留旧二进制
cp build-pr1394-new/ucm/transport/p2p/libucm_p2p_transport.so ucm/transport/p2p/
cp build-pr1394-new/ucm/store/dram/libdramstore.so             ucm/store/dram/
cp build-pr1394-new/ucm/store/dram/drampool                    bin/drampool
```

**2. 起 drampool**（device 3，必须先于 vLLM）

```bash
export UC_LOGGER_LEVEL=info
./bin/drampool \
  --addr 127.0.0.1:9000 --nics mlx5_0 --pool-size-gb 2 \
  --kvcache-block-sizes 7340032 14680064 524288 262144 \
  --kvcache-block-proportions 2 1 1 1 \
  --config /root/z00984767/0917/e2e-run/drampool.yaml
```

**3. 起 vLLM**（不能设 `ASCEND_RT_VISIBLE_DEVICES`）

```bash
export ENABLE_UCM_PATCH=1 VLLM_HASH_ATTENTION=0 VLLM_CPU_AFFINITY=0 ENABLE_SPARSE=FALSE
/home/z00984767/venv1394/bin/python -m vllm.entrypoints.cli.main serve "$MODEL" \
  --served-model-name qwen3-0.6b --tensor-parallel-size 1 --max-model-len 8192 \
  --gpu-memory-utilization 0.7 --block-size 128 --trust-remote-code --enforce-eager \
  --host 0.0.0.0 --port 7850 --distributed-executor-backend mp \
  --kv-transfer-config '{"kv_connector":"UCMConnector","kv_connector_module_path":"ucm.integration.vllm.ucm_connector","kv_role":"kv_both","kv_connector_extra_config":{"UCM_CONFIG_FILE":"/root/z00984767/0917/e2e-run/ucm_dramstore.yaml"}}'
```

**4. 发请求**（必须用全新内容的长 prompt，见注意事项 #5）

```bash
curl -sf http://127.0.0.1:7850/v1/completions -H 'Content-Type: application/json' \
  --data-binary @/root/z00984767/0917/e2e-run/req_r1.json
```

**5. 三层校验**

```bash
# 层1 writer JSON
cat /root/z00984767/0917/e2e-run/metrics/drampool_metrics.json
# 层2 vLLM /metrics
curl -s http://127.0.0.1:7850/metrics | grep '^ucm:drampool'
# 层3 Prometheus（注意冒号被保留，必须用 __name__ 选择器）
curl -sG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query={__name__="ucm:drampool_dump_requests_total"}'
```

---

## 四、配置文件

两份关键配置由脚本生成在 `/root/z00984767/0917/e2e-run/`。

### `drampool.yaml`

`transport.endpoints` 是 **peer 白名单**（内部映射 `twoSidedToOneSided`），这是本次最容易踩的坑。DramStore 按角色做端口偏移（`config.cc:188-196`）：

```yaml
transport:
  device_ids: [3]          # drampool 独占卡
  hixl:
    listen_port: 26666
    enable_cs: false       # 置 true 会让 HIXL Initialize 返回 103900
  endpoints:
    - two_sided: "127.0.0.1:9000"   # drampool 自身
      one_sided: "127.0.0.1:4501"
    - two_sided: "127.0.0.1:9001"   # DramStore scheduler（无偏移）
      one_sided: "127.0.0.1:4502"
    - two_sided: "127.0.0.1:9002"   # DramStore worker（9001 + deviceId + 1）
      one_sided: "127.0.0.1:4503"
metrics:
  enabled: true
  output_dir: /root/z00984767/0917/e2e-run/metrics
  interval_ms: 2000
```

### `ucm_dramstore.yaml`

```yaml
ucm_connectors:
  - ucm_connector_name: "UcmPipelineStore"
    ucm_connector_config:
      store_pipeline: "Dram"                      # 触发 _dram_pipeline_builder
      local_control_endpoint: "127.0.0.1:9001"
      local_transport_manager_id: "127.0.0.1:4502"
      hixl_listen_port: 36666
      enable_hixl_cs: false
      node_control_endpoints: ["127.0.0.1:9000"]
      node_transport_manager_ids: ["127.0.0.1:4501"]
      # 必须等于 drampool.yaml 的 metrics.output_dir + /drampool_metrics.json
      drampool_resource_log_path: "/root/z00984767/0917/e2e-run/metrics/drampool_metrics.json"
      drampool_resource_metrics_interval_sec: 2
      dump_timeout_ms: 3000
      router_type: "ring_hash"
enable_event_sync: true
use_layerwise: true
enable_metrics: true
metrics_config_path: "/root/z00984767/0917/unified-cache-management/examples/metrics/metrics_configs.yaml"
```

`store_pipeline: "Dram"` 会在 `ucm/store/pipeline/connector.py:383` 自动启动 `DramPoolResourceReporter`，无需额外配置。reporter 只在 host 侧（`device_id < 0`，即 scheduler 进程）运行，通过 `/dev/shm` flock 选主。

---

## 五、注意事项（踩过的坑，按重要性排序）

### 1. 三个设备号必须互不相同

HIXL 在 `hixl_transport.cpp::BuildRouteLocked` 里明确拒绝同卡路由：

```
[Transport][HIXL] build route failed: local and remote single instances use the same device
```

worker、scheduler、drampool 各占一张卡。这是本次能跑通的第一道门槛——之前所有失败都源于 worker 和 drampool 挤在 device 0。

### 2. 绝不能设 `ASCEND_RT_VISIBLE_DEVICES`

scheduler 的设备号是**编译进** `config.h::RuntimeDeviceId()` 的绝对常量，没有 YAML 开关。一旦限制了可见设备，`aclrtSetDevice(2)` 就会失败：

```
aclrtSetDevice failed: 507033     # ACL_ERROR_RT_DEV_SETUP_ERROR
```

实测：`ASCEND_RT_VISIBLE_DEVICES=0` 时可见卡数为 1，`SetDevice(3)` 和 `SetDevice(4)` 都返回 `107001`（`ACL_ERROR_RT_INVALID_DEVICEID`）。

改这个设备号必须重新编译 `dramstore`。本次把它从 `4` 改成了 `2`，因为 device 4 在这台机器上挂起。

### 3. 启动顺序：drampool 先 ready，再起 vLLM；vLLM 运行期间不要重启 drampool

DramStore **只在启动时做一次** HIXL `exchange-metadata` + `connect`。drampool 中途重启后，vLLM 侧只会重建 TCP 控制通道（日志里 `epoch` 递增、`DramStore node connected` 反复出现），**不会重做 HIXL 握手**，于是 drampool 的 HIXL peer 表是空的：

```
[Transport][HIXL] asynchronous transfer failed: unknown peer=127.0.0.1:4503
[Transport][HIXL] disconnect failed: unknown peer=127.0.0.1:4502
```

表现是所有请求 `-50010`（Timeout）超时，且 drampool 侧没有任何请求处理日志。本次就是因为改了白名单后只重启 drampool 而没重启 vLLM，白白绕了一圈。**要改 drampool，就重启整条栈。**

### 4. `transport.endpoints` 缺条目 → 请求被静默丢弃

drampool 的 `RequestReceiver` 用请求来源的 two-sided 端点反查 one-sided peer id：

```
RequestReceiver rejected unconfigured control peer, request_id=149, peer=127.0.0.1:9002
```

白名单里没有 `9002`（worker 偏移后的端口），请求直接被 `continue` 丢掉，客户端只能等到超时。这个日志是 `[W]` 级别，很容易漏看。

### 5. 必须用全新内容的 prompt，且长度 > 128 token

重复 prompt 会被 **GPU 内部 prefix cache** 全命中，根本不会访问外部 DramStore：

```
total_blocks_num: 15, hit hbm: 15, hit external: 0
Prefix cache hit rate: 72.6%
```

此时 counters 恒为 0，会误判成链路没通。判断方法：看 `ucm:total_prefix_query_blocks_total` 有没有增长——本次冒烟测试用的 5-token 短 prompt 只让 `total_prefix_query_tokens_total=5` 而 `blocks_total=0`，不足一个 block，同样不会触发外部存储。

一键脚本每次生成带时间戳和随机数的全新 prompt。

### 6. Prometheus 保留指标名里的冒号

vLLM `/metrics` 暴露的是 `ucm:drampool_dump_requests_total`。Prometheus **没有**把 `:` 规范化成 `_`（这一点与上一轮交接文档的记载相反）。所以 PromQL 必须写：

```promql
{__name__="ucm:drampool_dump_requests_total"}
```

写 `ucm_drampool_dump_requests_total` 会得到 `NO DATA`，看起来像是链路断了，实际只是选择器错了。

### 7. 每次重启前清 `/dev/shm/ucm_drampool_metrics_*`

reporter 把上次快照存在 `/dev/shm`。drampool 重启后 counters 归零，陈旧 state 会触发已知的 `snapshot_deltas` 缺陷（`Histogram sum changed without samples`），且**一旦触发就永久卡死**——state 永不前进，之后每次轮询都和同一份陈旧快照比较并继续抛错。

本次全程清 state + 保证启动顺序，reporter 零错误（`Histogram sum changed` 计数 0，选主日志正常）。

### 8. `enable_cs` / `enable_hixl_cs` 必须 false，两侧一致

置 true 会注入 `LocalCommRes {"version":"1.3"}`，HIXL `Initialize` 直接返回 `103900`，drampool 起不来：

```
[Transport][HIXL] init failed: Initialize("127.0.0.1") returned 103900
```

### 9. 拷贝编译产物前先停进程

`cp: cannot create regular file 'bin/drampool': Text file busy` —— 本次真的踩了：以为回退了实验，实际二进制没换掉，白跑一轮验证。

### 10. 容器内环境零散

- venv1394 的 PATH 里**没有** `vllm`，必须用 `python -m vllm.entrypoints.cli.main`。
- 容器内没有 `hostname` / `ip` / `ifconfig`，取 IP 要读 `/proc/net/route`。
- 容器内没有 `prometheus` / `grafana`，但 `/tmp/z009-extract/` 下有现成的 arm64 二进制。
- 华为云镜像站没有 grafana 的 arm64 包（404），清华源 403，别指望现下。
- `source ascend-toolkit/set_env.sh` 会引用未定义变量，脚本里**不能用 `set -u`**，否则静默退出（本次 vLLM 启动脚本就是这么失败的，日志里只有 85 字节的 `nohup: failed to run command 'vllm'`）。

---

## 六、已知阻塞：HIXL 数据面传输失败

### 现象

控制面完全正常，数据面全部失败：

```
[Transport][HIXL] connection established: local_engine=127.0.0.1 device=3 remote_engine=127.0.0.1:48647
[Transport][HIXL] operation failed: TransferSync("127.0.0.1:55941", ops=28, timeout_ms=5000) returned 507001
```

`507001 = ACL_ERROR_RT_TS_ERROR`。CANN plog 给出底层真相：

```
HCCL [TaskExecStage][RunFailed][HOST_TS] taskType[Memcpy]
  tag[BatchPut_1_0_hixl_127.0.0.1_127.0.0.1:55941]
  src:[0x6ff7ba006c0] dst:[0x12c208200d80] size:[0x17] link type:[HCCS]
  rankSize[0], rankId[0]
RUNTIME rtStreamSynchronizeWithTimeout: ErrCode=507001, InnerCode=0x7150057
```

`rankSize[0]` 说明 HCCL 通信组从未正确建立。连 23 字节（`size:[0x17]`）的响应写回都失败，所以**不是数据量或对齐问题**，是 group 本身的问题。

### DUMP 走到哪一步

从直方图能精确定位卡点——前面所有阶段都成功，只有 transfer 超时：

| 阶段 | count | sum(ms) |
| --- | --- | --- |
| `dump_prepare` | 168 | 141 |
| `dump_metadata` | 168 | 4.6 |
| `dump_submit` | 168 | 136 |
| **`dump_transfer`** | **168** | **475965**（= 168 × ~2832ms，全部超时） |
| `response_submit` | 171 | 4851 |
| `lookup_scan` | 3 | 0.03 |

即：请求接收 ✓ → 元数据 `StoreBegin` ✓ → buffer 槽位分配 ✓（`metadata_entry_count` 296、`buffer_pool_usage_ratio` 0.114 证明确实占了 DRAM）→ **HIXL 传输 ✗**。

因为传输失败，KV 没有真正落到 DRAM，后续 `LOAD` 无从发生，所以 `load_requests_total=0`、`hit external: 0`。

### 已排除的原因

| 假设 | 验证方式 | 结论 |
| --- | --- | --- |
| CANN 版本不匹配（当前 9.1.0-beta.1，patch 假设 8.5.1） | 切 `cann-8.5.1/set_env.sh` 重启 | **排除**：8.5.1 缺 `libhccl_v2.so`，且二进制 RPATH 硬绑 `ascend-toolkit/latest`（=9.1.0-beta.1），切环境变量无效，drampool 直接 `507033` 起不来 |
| A2 host-sync patch 在 9.1 上多余（`if (uses_host_memory)` 强制走 sync） | 改成 `if (false && ...)` 走原生 async，重编重测 | **排除**：async 路径**更差**（`metadata_entry_count=0`、传输超时、`GetStatus` 失败），sync 路径至少能分配 buffer 并存入 metadata。已回退，`git diff` 干净 |
| `enable_cs` 未开 | 两侧同时置 true | **排除**：HIXL `Initialize` 返回 `103900`，drampool 起不来 |
| block size 不匹配 | 覆盖 `262144/524288/7340032/14680064` 四档 | **已修复且非本因**：修复后 `metadata_entry_count` 从 0 涨到 296，说明 buffer 分配已正常，但 transfer 仍失败 |
| 设备间无互联 | `npu-smi info -t topo` | **排除**：device0 ↔ device3 是 HCCS 直连 |
| device 3 故障 | `npu-smi info -t health -i 3 -c 0` | **排除**：Health OK，Error Code NA |
| 同卡冲突 | 三方分离到 device 0/2/3 | **已修复**：HIXL 握手成功（`connection established`），但数据传输仍失败 |

### 下一步建议

最可疑的方向是 **HIXL 9.1.0-beta.1 上 host↔device 单向传输的 HCCL group 初始化**。`rankSize[0]` 是强信号——group 建立时 rank 数为 0，说明 `BatchPut` 通道没协商成。建议：

1. 在**干净的 HIXL 8.5.1 环境**（如交接文档提到的 `codex_drampool_cann851` 容器）里跑同一套配置。那里 patch 的前提成立，如果数据面能通，就确认是 9.1.0-beta.1 的回归。
2. 找 HIXL/HCCL 侧确认 `127.0.0.1` 回环 + host 内存 `BatchPut` 在 9.1.0-beta.1 上是否受支持，`rankSize[0]` 是否是已知问题。
3. 检查 drampool 的 `HixlRole::Client` 与 DramStore 的默认 `Bidirectional` 角色组合在 9.1 上是否还合法（`drampool_server.cc:266` vs `transport_init_attrs.h:49`）。

---

## 七、项目待改进的地方

按影响面排序。

### P0 — 会让部署直接失败且难排查

**1. scheduler 设备号硬编码在 `config.h`，无运行时开关**

```cpp
std::int32_t RuntimeDeviceId() const noexcept
{
    return GetRole() == Role::SCHEDULER ? 2 : deviceId;   // 常量
}
```

后果：换一台机器、或那张卡被占，就得改源码 + 重新编译。更糟的是它和 `ASCEND_RT_VISIBLE_DEVICES` 存在隐性冲突——用户按常规做法限制可见设备，就会得到一句语义模糊的 `aclrtSetDevice failed: 507033`，完全看不出是"设备号越界"。

建议：加 `scheduler_device_id` YAML 配置项，缺省回落到当前行为；并在解析时校验它落在可见设备范围内，越界时给出明确错误（"scheduler_device_id=2 不在可见设备 [0] 内，请检查 ASCEND_RT_VISIBLE_DEVICES"）。

**2. `transport.endpoints` 白名单缺失时请求被静默丢弃**

```cpp
UC_WARN("RequestReceiver rejected unconfigured control peer, request_id={}, peer={}", ...);
continue;
```

只有 `[W]` 级别日志，客户端侧看到的是 `-50010 Timeout`——一个完全误导人的错误码。而 DramStore 的实际端口还要经过角色相关的偏移计算（`config.cc:188-196`），用户几乎不可能从 yaml 字面值推断出该配哪些。

建议：DramStore 侧把自己计算出的真实 control/transport 端点写进握手或启动日志；drampool 启动时如果能拿到对端信息，主动校验白名单完整性；至少把这条日志提到 `[E]` 并附带"请检查 transport.endpoints 是否包含 <peer>"。

**3. drampool 重启后 DramStore 不重做 HIXL 握手**

重连只恢复 TCP 控制通道，HIXL peer 表留空，之后每个传输都是 `unknown peer`。运维视角看，"drampool 重启了，vLLM 日志显示 node connected，但所有请求超时"——三个信号互相矛盾。

建议：reconnect 路径上重新执行 `ExchangeMetadata` + `Connect`；或检测到 HIXL peer 缺失时主动触发完整重建，而不是继续用空表发请求。

### P1 — 正确性/健壮性

**4. `snapshot_deltas` 缺陷会让 reporter 永久卡死**（上一轮已记录，本次未修）

`resource_reporter.py` 遇到同桶不同 sum 的直方图会抛 `Histogram sum changed without samples`，且 `_collect_once` 整体失败导致 `_write_state` 永不执行，state 永不前进——此后每次轮询都撞同一份陈旧 state，**无限循环报错**。生产场景（drampool 重启而 vLLM 不重启）必然触发。

建议：同桶 sum 变小视为 counter reset 并重置该直方图的 state；或让 state 按指标粒度提交，单个指标失败不拖累整体；至少要有一条"检测到陈旧 state，已自动重置"的自愈路径。

**5. `--kvcache-block-sizes` 与真实 KV 布局无任何关联校验**

`BufferManager::GetPool(size)` 是精确匹配，配错就 `no pool registered for size X` 然后 DUMP 失败。但用户从模型配置推不出这个数字（要算 `2 × num_kv_heads × head_dim × block_size × dtype_size × num_layers`），而 vLLM 日志里的 `block_size: 14680064` 又是另一个粒度的值。

建议：`BufferManager::Allocate` 找不到精确池时，日志里列出已注册的尺寸（"no pool for size 7340032; registered: [4096, 8192]"）；进一步可以支持"最小可容纳"回退，或在 DramStore 侧启动时把自己要用的 entry 尺寸告知 drampool 做前置校验。

**6. `ucmstore.test` 编译失败**（上一轮已记录，仍未修）

`task_worker_test.cc` 用两参调用已变成三参（新增 `beginUs`）的 `ProcessDump/ProcessLoad/ProcessLookup`，首个错误 `candidate expects 3 arguments, 2 provided`。导致 `BUILD_UNIT_TESTS=ON` 无法通过，CI 覆盖不到这块。

**7. A2 host-sync patch 的版本前提写死在注释里，无运行时判定**

```cpp
// Ascend A2 with HIXL 8.5.1 cannot execute Host-memory transfers through
// the native async path.
if (uses_host_memory) { ... QueueTransferSync ... }
```

本次环境是 HIXL 9.1.0-beta.1，patch 的前提可能已不成立，但代码无条件走 sync 路径。实测两条路径都失败（sync 507001 / async 超时），无法据此判断，但这种"按版本号写死行为且无探测"的写法本身是隐患。

建议：启动时探测 HIXL 版本或做一次能力试探，据此选择路径，并在日志里说明选了哪条、为什么。

### P2 — 可用性/文档

**8. `enable_cs` 置 true 导致 HIXL 初始化失败，但错误码不可解读**

`Initialize("127.0.0.1") returned 103900` 没有任何上下文。建议包一层说明（"CS 模式在当前 CANN/HIXL 版本不受支持"），或直接在文档里标注该选项的适用范围。

**9. 仓库自带的 Grafana 面板不覆盖 drampool 指标**

`examples/metrics/grafana_store.json` 只有 `ucm:cache_*` 和 `ucm:mooncake_*`，**没有任何 `drampool_*` 面板**。而 `metrics_configs.yaml` 里定义了 13 个 counter、8 个 gauge、20 个 histogram 的 drampool 指标。本次只能手写 12 个面板（已在 `e2e_dramstore_all_in_one.sh` 的 `build_dashboard()` 里）。

建议：补一份 `examples/metrics/grafana_drampool.json`，可以直接从本次脚本里的面板定义生成。

**10. 文档与实际行为不符**

- 指标名里的冒号没有任何文档说明。官方 `docs/.../observability/metrics.md` 的示例是对的（`ucm:cache_load_bytes_total`），但它只演示了 `/metrics` 的原始输出，**没有说明在 PromQL 里查询时必须用 `{__name__="ucm:xxx"}` 选择器**——直接写 `ucm:xxx` 会被解析成 label matcher 而报错，写 `ucm_xxx` 则静默返回空。上一轮交接文档 `PR1394_DRAMPOOL_DRAMSTORE_GRAFANA_E2E_HANDOFF.md` 还记了个错误结论："Prometheus 把 `:` 规范化为 `_`"，本次实测**不会**（114 个 drampool 系列全部保留冒号），这个错误说法会直接把排查引向歧途。
- 没有任何文档说明 DramStore 的端口偏移规则（worker `+(deviceId+1)`），而这直接决定了 `transport.endpoints` 该怎么配。
- `examples/drampool.yaml` 的注释只说"每个 peer 的 two-sided 端点必须各出现一次"，没说 peer 是谁、端口怎么算。
- 没有文档说明 scheduler 的设备号来自 `config.h` 硬编码、且与 `ASCEND_RT_VISIBLE_DEVICES` 冲突（见 P0-1）。

**11. `--nics` 参数无实际校验**

`ValidateNics` 只检查非空，不验证网卡是否存在、是否可用于 RDMA。填错名字不会在启动时报错，要到传输阶段才失败。容器里既没有 `eth0`（bash history 里用过）也没有 `lo` 之外的常规名，实际可用的是 `mlx5_0/1/2` 和一堆 `enp*`——全靠猜。

**12. 官方 DramStore 示例配置根本没开 metrics**

`examples/ucm_config_dramstore.yaml` 是 DramStore 的唯一示例，但它**既没有 `enable_metrics` 也没有 `metrics_config_path`**，全文与指标相关的只有第 29 行：

```yaml
drampool_resource_log_path: "./logs/drampool_metrics.json"
```

两个问题：

- 用户照抄这份示例启动，`drampool_*` 指标能不能上报完全依赖内置默认行为，而示例本身没有表达"这里需要开指标"的意图。本次必须手动补 `enable_metrics: true` + `metrics_config_path` 才拿到完整的 114 个 drampool 系列。
- `drampool_resource_log_path` 是**相对路径**，依赖进程 cwd。而它必须精确等于 drampool.yaml 里 `metrics.output_dir` + `/drampool_metrics.json`——drampool 和 vLLM 是两个独立进程，cwd 未必相同，一旦不一致 reporter 就读不到文件。文档里说"指标文件必须存在且可被读取，否则 UCM 指标不会暴露"，但读不到时**没有任何告警日志**。

建议：示例里补上 `enable_metrics: true` 和 `metrics_config_path`；两个路径都改用绝对路径并加注释说明二者必须一致；reporter 在文件不存在或长时间无更新时打一条明确的 `[W]`/`[E]`。

---

## 八、本次对仓库的改动

**已提交在分支上的**（不是本次改的）：
- `ab6f56d fix: enable A2 to support HiXL asynchronous transfer API` —— A2 host-sync patch，本次实验证明在当前环境下 sync 路径优于 async，已保持原样。

**本次的本地未提交改动**（仅测试适配，不应推送）：

```
ucm/store/dram/cc/config.h                   | 10 ++++++++--   # RuntimeDeviceId 4 -> 2
ucm/store/test/case/dram/dram_config_test.cc |  2 +-
```

`config.h` 的改动原因：device 4 在这台机器上 `aclrtSetDevice` 挂起，必须换到可用的 device 2。注释里已写明推理过程（device 0 被 vLLM worker 占、device 1 有 checkcode mismatch、device 3 归 drampool）。

`ucm/transport/p2p/src/protocols/hixl/hixl_transport.cpp` 曾被临时改成走原生 async 路径做实验，**已完全回退**，`git diff` 为空。

---

## 九、当前现场（保留运行）

```
drampool   pid 69959   device 3   :9000/:4501
vLLM       pid 70094   device 0   :7850    (Qwen3-0.6B, TP=1, enforce-eager)
Prometheus pid 71189              :9090    (scrape 127.0.0.1:7850, 5s)
Grafana    pid 71241              :3000    (admin/admin1394, dashboard uid=drampool-e2e-z009)
```

停止：

```bash
bash /root/z00984767/0917/e2e-run/e2e_dramstore_all_in_one.sh --stop
```

关键文件：

| 路径 | 内容 |
| --- | --- |
| `e2e-run/e2e_dramstore_all_in_one.sh` | 一键联调脚本（含全部约束说明） |
| `e2e-run/drampool.yaml` / `ucm_dramstore.yaml` | 两份配置 |
| `e2e-run/launch_drampool.sh` / `launch_vllm.sh` | 启动器（脚本生成） |
| `e2e-run/logs/{drampool,vllm,prometheus,grafana}.log` | 各组件日志 |
| `e2e-run/metrics/drampool_metrics.json` | C++ writer 输出（层1 证据） |
| `e2e-run/monitoring/` | Prometheus/Grafana 配置、数据、面板 JSON |
| `/root/ascend/log/debug/plog/plog-<drampool-pid>_*.log` | HCCL/RUNTIME 底层错误（507001 证据） |
