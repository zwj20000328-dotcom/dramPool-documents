# A2 服务器 vLLM + DramStore/DramPool + Grafana 端到端联调

## Context（背景）

PR 1394 给 DramPool 加了 metrics 写读链路（C++ writer → JSON 快照 → Python `DramPoolResourceReporter` → `ucmmetrics` → vLLM `/metrics`）。上一轮交接（`PR1394_DRAMPOOL_DRAMSTORE_GRAFANA_E2E_HANDOFF.md`，2026-09-20）是用**手写 test harness**（mock client）验证的，并没有跑真实 vLLM 推理。本次目标是在远程 A2 服务器、容器 `z00984767_ascend_test` 内，用**真实 vLLM 服务**把 DramStore 当 KV cache 后端拉起来，发真实请求，并在 Grafana 看到 `ucm_drampool_*` 指标。

关键事实（已在本仓库代码中确认）：
- vLLM 通过 `--kv-transfer-config` 挂 `UCMConnector`（`ucm/integration/vllm/ucm_connector.py`），运行时需 `ENABLE_UCM_PATCH=1`。
- `store_pipeline: "Dram"` 会在 `ucm/store/pipeline/connector.py:383` `_dram_pipeline_builder` 自动启动 `DramPoolResourceReporter`（仅 host 侧 `device_id<0` 生效，`/dev/shm` flock 选主）。
- UCM 指标**复用 vLLM 自己的 `/metrics` 端点**（`vllm_connector` consumer，前缀 `ucm:`），无需独立 exporter。Prometheus 把 `:` 规范化成 `_`，查询名是 `ucm_drampool_*`。
- 指标**只有在 vLLM 处理请求后**才刷新（`get_kv_connector_stats()`），空闲时不更新。
- editable 安装（`pip install -e .`）把 CMake install prefix 设为仓库根 → `drampool` 可执行文件落在 **`$UCM_ROOT/bin/drampool`**，`libdramstore.so`/`libmetrics.so`/`ucmmetrics*.so` 落在源码树对应目录。

数据通路：真实 vLLM 请求 → DramStore(`libdramstore.so`) → DramPool daemon(C++ writer) → `<output_dir>/drampool_metrics.json` → `DramPoolResourceReporter` → `ucmmetrics` → vLLM `/metrics`(`ucm:drampool_*`) → Prometheus(`ucm_drampool_*`) → Grafana。

## 环境约定（本次锁定）

| 项 | 值 |
| --- | --- |
| 远程容器 | `z00984767_ascend_test`（所有步骤在容器内执行，除非注明"宿主机"） |
| 仓库 checkout | `/root/z00984767/0917/unified-cache-management`（下称 `$UCM_ROOT`） |
| 模型 | Qwen3-0.6B：`/home/codex/models-hf/models--Qwen--Qwen3-0.6B/snapshots/c1899de289a04d12100db370d81485cdf75e47ca` |
| 监控栈 | 在**被测容器内**直接跑 Prometheus + Grafana 二进制（不新起 docker 容器） |
| 本地分支 | `z00984767-0917`，HEAD `3fc31e21`（origin 上还没有这个分支，见步骤 0） |

> 约定：下面所有命令里 `$UCM_ROOT`、`$MODEL`、`$WORK` 先用 export 设好，后续直接引用。`$WORK=/root/z00984767/0917/e2e-run` 是本次联调的临时工作目录（配置、日志、监控数据都放这，不污染仓库）。

---

## 步骤 0：把本地分支代码送到远程容器

本地分支 `z00984767-0917` 尚未推到 origin（`git ls-remote origin` 为空），所以远程 checkout 拿不到。二选一：

**方案 A（推荐，最干净）——推分支后远程 fetch**
```bash
# 本地（当前工作区）
git push -u origin z00984767-0917
```
```bash
# 远程容器内
export UCM_ROOT=/root/z00984767/0917/unified-cache-management
cd "$UCM_ROOT"
git fetch origin
git checkout z00984767-0917        # 或 git reset --hard origin/z00984767-0917
git rev-parse HEAD                  # 应等于本地 3fc31e21
```

**方案 B——bundle 传输（不想推远端时）**
```bash
# 本地
git bundle create /tmp/z00984767-0917.bundle z00984767-0917
scp /tmp/z00984767-0917.bundle <user>@<A2-host>:/tmp/
```
```bash
# 远程容器内（假设 bundle 已拷进容器 /tmp）
cd "$UCM_ROOT"
git fetch /tmp/z00984767-0917.bundle z00984767-0917:z00984767-0917
git checkout z00984767-0917
git rev-parse HEAD
```

> 注意：交接文档强调**不得 reset 共享目录 / 用户未提交修改**。`$UCM_ROOT` 是用户指定路径，checkout 前先 `git status` 确认没有要保留的改动；若有，先 stash。

---

## 步骤 1：检查环境（容器内，全部只读）

```bash
export UCM_ROOT=/root/z00984767/0917/unified-cache-management
export MODEL=/home/codex/models-hf/models--Qwen--Qwen3-0.6B/snapshots/c1899de289a04d12100db370d81485cdf75e47ca
export WORK=/root/z00984767/0917/e2e-run
mkdir -p "$WORK/logs" "$WORK/metrics"

# 1.1 NPU 设备与空闲情况（A2 用 npu-smi）
npu-smi info
#   关注：8 卡是否都在、Health OK、哪些卡显存/进程空闲。本次单卡即可，记下空闲卡号（如 0）。

# 1.2 CANN / HIXL / HCCL 版本（A2 需 8.5.1 匹配）
cat /usr/local/Ascend/ascend-toolkit/latest/*/ascend_toolkit_install.info 2>/dev/null
ls /usr/local/Ascend/ascend-toolkit/set_env.sh /usr/local/Ascend/nnal/atb/set_env.sh
# HIXL/HCCL 版本（路径按容器实际，交接文档里是 Version=8.5.1）
find /usr/local/Ascend -iname '*hixl*version*' -o -iname '*hccl*version*' 2>/dev/null | head

# 1.3 vLLM / torch / torch_npu 是否就绪
python3 -c "import vllm, torch, torch_npu; print('vllm', vllm.__version__); print('torch', torch.__version__)"
which vllm

# 1.4 模型权重存在
ls "$MODEL"/config.json "$MODEL"/*.safetensors 2>/dev/null | head

# 1.5 网卡名（drampool --nics 用；单节点回环通常用 lo 或实际 RDMA 网卡）
ip -o link show | awk -F': ' '{print $2}'
ls /sys/class/infiniband 2>/dev/null   # 有 RDMA 网卡时这里会列出 mlx5_x 等

# 1.6 端口占用预检（本次将用到 7850/9000/9001/4501/4502/36666/9090/3000）
for p in 7850 9000 9001 4501 4502 36666 9090 3000; do
  (echo >/dev/tcp/127.0.0.1/$p) >/dev/null 2>&1 && echo "port $p BUSY" || echo "port $p free"
done
```

判定：NPU 有空闲卡、vllm 可 import、模型文件在、关键端口空闲 → 继续。

---

## 步骤 2：编译 / 安装 UCM（容器内）

```bash
cd "$UCM_ROOT"
source /usr/local/Ascend/ascend-toolkit/set_env.sh
source /usr/local/Ascend/nnal/atb/set_env.sh

# A2 → PLATFORM=ascend；editable 安装，drampool 会落到 $UCM_ROOT/bin/
export PLATFORM=ascend
export ENABLE_SPARSE=FALSE          # 本次不需要 sparse，关掉省编译时间
python3 -m pip install -v -e . --no-build-isolation 2>&1 | tee "$WORK/logs/build.log"
```

验证产物：
```bash
ls -l "$UCM_ROOT/bin/drampool"                       # DramPool daemon 可执行
ls -l "$UCM_ROOT/ucm/store/dram/libdramstore.so"     # DramStore 后端
ls -l "$UCM_ROOT/ucm/shared/metrics/"*ucmmetrics*.so "$UCM_ROOT/ucm/shared/metrics/libmetrics.so"
python3 -c "import ucm; print(ucm.__file__)"
```

> 已知坑（交接文档）：`ucmstore.test` 单测目标在 PR head 编译失败（`task_worker_test.cc` 用 2 参调用已变 3 参的 `ProcessDump/Load/Lookup`）。**这不影响生产 `drampool`/`dramstore`/wheel**，本次不编单测、不修生产接口。若 `pip install -e .` 因别的目标失败，单独看 `$WORK/logs/build.log` 第一个决定性错误。

---

## 步骤 3：准备配置（容器内，写到 `$WORK`，不改仓库示例）

### 3.1 DramPool daemon 运行配置 `$WORK/drampool.yaml`
基于 `examples/drampool.yaml` 改：单节点只留一个 endpoint、metrics 间隔调短、device_ids 对齐 vLLM 用的卡。
```bash
cat > "$WORK/drampool.yaml" <<'YAML'
transport:
  device_ids: [0]            # 改成步骤1选定的空闲卡号；单卡即可
  hixl:
    listen_port: 26666
    enable_cs: false
  endpoints:
    - two_sided: "127.0.0.1:9000"   # DramPool 控制端（= ucm yaml 的 node_control_endpoints）
      one_sided: "127.0.0.1:4501"   # DramPool 传输端（= ucm yaml 的 node_transport_manager_ids）
health:
  port: 8081                   # 非0则暴露 GET /health，便于探活
metrics:
  enabled: true
  output_dir: /root/z00984767/0917/e2e-run/metrics   # 绝对路径，写出 drampool_metrics.json
  interval_ms: 2000            # 2s 刷新，联调时更快看到变化
queue:
  request_depth: 65536
  completion_depth: 65536
request_receiver:
  idle_wait_us: 100
poller:
  pending_depth: 64
flag_buffer:
  capacity_mb: 64
  slot_size_bytes: 64
gc:
  enabled: true
  interval_ms: 1000
metadata:
  periodic_eviction_policy: TTL
  deep_eviction_policy: POSITION
  lease_time_ms: 5000
  default_evict_ratio: 0.0
  evict_period_ms: 31536000000
operation:
  timeout_ms: 5000
logger:
  dir: /root/z00984767/0917/e2e-run/logs
  max_files: 10
  max_size_mb: 5
YAML
```

### 3.2 UCM/DramStore 配置 `$WORK/ucm_dramstore.yaml`
基于 `examples/ucm_config_dramstore.yaml`，**必须加 `enable_metrics` 和 `metrics_config_path`**，并把 `drampool_resource_log_path` 指向 3.1 的 metrics 输出。
```bash
cat > "$WORK/ucm_dramstore.yaml" <<'YAML'
ucm_connectors:
  - ucm_connector_name: "UcmPipelineStore"
    ucm_connector_config:
      store_pipeline: "Dram"
      local_control_endpoint: "127.0.0.1:9001"
      local_host: "127.0.0.1"
      local_transport_manager_id: "127.0.0.1:4502"
      hixl_listen_port: 36666
      enable_hixl_cs: false
      node_control_endpoints: ["127.0.0.1:9000"]
      node_transport_manager_ids: ["127.0.0.1:4501"]
      # 必须 == drampool.yaml 的 metrics.output_dir + /drampool_metrics.json
      drampool_resource_log_path: "/root/z00984767/0917/e2e-run/metrics/drampool_metrics.json"
      drampool_resource_metrics_interval_sec: 2
      max_io_entries: 10000
      lookup_timeout_ms: 1000
      dump_timeout_ms: 3000
      load_timeout_ms: 3000
      reconnect_interval_ms: 3000
      router_type: "ring_hash"
      transport_worker_count: 1
      node_runner_count: 1

enable_event_sync: true
use_layerwise: true

# —— 指标：复用 vLLM /metrics，前缀 ucm: ——
enable_metrics: true
metrics_config_path: "/root/z00984767/0917/unified-cache-management/examples/metrics/metrics_configs.yaml"
YAML
```
> 端口对应关系（务必一致）：
> - DramPool 侧 `--addr` = `127.0.0.1:9000`（two_sided），其 transport one_sided = `4501`。
> - DramStore(vLLM) 侧 `local_control_endpoint=9001`、`local_transport_manager_id=4502`。
> - 这两对必须都出现在 drampool.yaml 的 `endpoints` 里（本例单节点只需 9000/4501 这一对，因为 DramStore 与 DramPool 同机，9001/4502 是 DramStore 自己的监听端）。

---

## 步骤 4：启动 DramPool daemon（容器内，后台）

```bash
cd "$UCM_ROOT"
source /usr/local/Ascend/ascend-toolkit/set_env.sh
export UC_LOGGER_LEVEL=info        # 排错时可改 debug
# --nics：单节点回环用 lo；若步骤1.5发现真实 RDMA 网卡(mlx5_x)且要走 RDMA，则换成该网卡名
nohup ./bin/drampool \
  --addr 127.0.0.1:9000 \
  --nics lo \
  --pool-size-gb 2 \
  --kvcache-block-sizes 4096 8192 \
  --config "$WORK/drampool.yaml" \
  > "$WORK/logs/drampool.log" 2>&1 &
echo $! > "$WORK/drampool.pid"
```

验证 daemon 起来且开始写 metrics：
```bash
sleep 3
cat "$WORK/drampool.pid"; ps -p "$(cat "$WORK/drampool.pid")" -o pid,cmd
curl -sf http://127.0.0.1:8081/health && echo " <- drampool health OK"
# metrics 快照文件应出现（此时 counter 多为 0，正常）
ls -l "$WORK/metrics/drampool_metrics.json"
head -c 400 "$WORK/metrics/drampool_metrics.json"; echo
```
> `--kvcache-block-sizes` 必须覆盖 vLLM 实际产生的 KV block 字节数。Qwen3-0.6B + `--block-size 128` 下单 block 字节数由模型 head_dim/层数决定；若启动后 dump/load 一直 miss 或报 block size 不匹配，回到这里把实际 block 字节数加进列表（可先跑一次 vLLM 看日志里 UCM 报告的 tensor/block size，再回填）。`--pool-size-gb 2` 对 0.6B 足够。

---

## 步骤 5：启动 vLLM（容器内，挂 UCM/DramStore connector）

```bash
cd "$UCM_ROOT"
source /usr/local/Ascend/ascend-toolkit/set_env.sh
source /usr/local/Ascend/nnal/atb/set_env.sh

export ENABLE_UCM_PATCH=1          # 运行时 patch 钩子，必须
export VLLM_HASH_ATTENTION=0
export VLLM_CPU_AFFINITY=0
export ENABLE_SPARSE=FALSE
export ASCEND_RT_VISIBLE_DEVICES=0 # 用步骤1选定的空闲卡，和 drampool.yaml device_ids 对齐
export VLLM_LOGGING_LEVEL=INFO

nohup vllm serve "$MODEL" \
  --served-model-name qwen3-0.6b \
  --tensor-parallel-size 1 \
  --max-model-len 8192 \
  --gpu-memory-utilization 0.85 \
  --block-size 128 \
  --trust-remote-code \
  --enforce-eager \
  --host 0.0.0.0 \
  --port 7850 \
  --distributed-executor-backend mp \
  --kv-transfer-config '{
    "kv_connector": "UCMConnector",
    "kv_connector_module_path": "ucm.integration.vllm.ucm_connector",
    "kv_role": "kv_both",
    "kv_connector_extra_config": {"UCM_CONFIG_FILE": "/root/z00984767/0917/e2e-run/ucm_dramstore.yaml"}
  }' \
  > "$WORK/logs/vllm.log" 2>&1 &
echo $! > "$WORK/vllm.pid"
```

等服务就绪（首次加载 0.6B 约 1-3 分钟）：
```bash
# 轮询健康检查
for i in $(seq 1 60); do
  curl -sf http://127.0.0.1:7850/health >/dev/null && { echo "vLLM ready after ${i}0s"; break; }
  sleep 10
done
curl -sf http://127.0.0.1:7850/v1/models | head -c 300; echo
# 确认 connector 真的挂上了
grep -E "create UcmPipelineStore with config|UCMConnector|Dram" "$WORK/logs/vllm.log" | head
```
判定：`/health` 200、`/v1/models` 列出 `qwen3-0.6b`、日志有 `create UcmPipelineStore`。

---

## 步骤 6：发真实请求（触发 KV dump/load，产生指标）

```bash
# 6.1 构造一个足够长、可重复的 prompt（跨多个 128-token block 才会走外部 cache）
python3 - <<'PY'
import json
from pathlib import Path
Path('/root/z00984767/0917/e2e-run/req.json').write_text(json.dumps({
    "model": "qwen3-0.6b",
    "prompt": "Explain how a shared external KV cache reuses previous computation across requests. " * 200,
    "max_tokens": 64,
    "temperature": 0,
}))
PY

# 6.2 第一次请求：冷启动，应产生 dump（写 KV 到 DramPool）
curl -sf http://127.0.0.1:7850/v1/completions \
  -H 'Content-Type: application/json' \
  --data-binary @/root/z00984767/0917/e2e-run/req.json | head -c 400; echo

# 6.3 第二次同样请求：应命中外部 cache，产生 lookup hit + load
curl -sf http://127.0.0.1:7850/v1/completions \
  -H 'Content-Type: application/json' \
  --data-binary @/root/z00984767/0917/e2e-run/req.json | head -c 400; echo

# 6.4 多打几次，让指标更明显
for n in 1 2 3; do
  curl -sf http://127.0.0.1:7850/v1/completions -H 'Content-Type: application/json' \
    --data-binary @/root/z00984767/0917/e2e-run/req.json >/dev/null && echo "req $n done"
done
```

---

## 步骤 7：三层证据校验（先于 Grafana，确认数据真的在流）

```bash
# 层1：DramPool C++ writer 写出的 JSON 快照，counter 应非零
python3 - <<'PY'
import json
p="/root/z00984767/0917/e2e-run/metrics/drampool_metrics.json"
d=json.loads(open(p).read().strip().splitlines()[-1])
c=d.get("counters",{})
for k in ("drampool_dump_requests_total","drampool_load_requests_total","drampool_lookup_requests_total","drampool_lookup_miss_entries_total"):
    print(k, c.get(k))
PY

# 层2：vLLM /metrics 端点（UCM 指标前缀 ucm:）
curl -sf http://127.0.0.1:7850/metrics | grep -E '^ucm:drampool_(dump|load|lookup)_requests_total'

# 若层2为空：多半是请求还没触发 connector stats 刷新，或 metrics_config_path 没被读到。
# 排查：grep -iE "metrics|ucmmetrics|PrometheusStatsLogger|resource_reporter" "$WORK/logs/vllm.log"
```
判定：层1 counter 非零、层2 能看到 `ucm:drampool_*_requests_total` 且数值与层1一致 → 数据通路 OK，再接监控。

---

## 步骤 8：Prometheus + Grafana（容器内直接跑二进制）

> 用户选择在**被测容器内**跑监控栈。需要 prometheus / grafana 二进制；若容器没有，用宿主机 docker 起（见 8.4 备选）。

### 8.1 Prometheus 配置 `$WORK/prometheus.yml`
```bash
cat > "$WORK/prometheus.yml" <<'YAML'
global:
  scrape_interval: 5s
  evaluation_interval: 30s
scrape_configs:
  - job_name: vllm
    metrics_path: /metrics
    static_configs:
      - targets: ["127.0.0.1:7850"]   # 容器内跑，直接抓本机 vLLM
YAML
```

### 8.2 启动 Prometheus（容器内，:9090）
```bash
# 若容器无 prometheus 二进制，先确认：which prometheus || ls /opt/prometheus* 2>/dev/null
nohup prometheus \
  --config.file="$WORK/prometheus.yml" \
  --storage.tsdb.path="$WORK/promdata" \
  --web.listen-address=0.0.0.0:9090 \
  > "$WORK/logs/prometheus.log" 2>&1 &
echo $! > "$WORK/prom.pid"
sleep 3
curl -sf http://127.0.0.1:9090/-/ready && echo " prom ready"
# 确认 target UP 且能查到 ucm 指标（Prometheus 名里 : 变 _）
curl -sf 'http://127.0.0.1:9090/api/v1/query?query=ucm_drampool_dump_requests_total' | head -c 400; echo
```

### 8.3 启动 Grafana（容器内，:3000）
```bash
# grafana 二进制方式（standalone）
nohup grafana server \
  --homepath=/usr/share/grafana \
  --config=/etc/grafana/grafana.ini \
  cfg:default.paths.data="$WORK/grafana-data" \
  cfg:default.paths.logs="$WORK/logs/grafana" \
  cfg:default.server.http_port=3000 \
  > "$WORK/logs/grafana.log" 2>&1 &
echo $! > "$WORK/grafana.pid"
sleep 4
curl -sf http://127.0.0.1:3000/api/health && echo " grafana OK"
```
配置数据源 + 导入面板（用 API 免手点）：
```bash
# 8.3.1 加 Prometheus 数据源
curl -sf -X POST http://admin:admin@127.0.0.1:3000/api/datasources \
  -H 'Content-Type: application/json' \
  -d '{"name":"prom","type":"prometheus","url":"http://127.0.0.1:9090","access":"proxy","isDefault":true}'

# 8.3.2 导入仓库自带 store 面板（含 drampool 指标）
curl -sf -X POST http://admin:admin@127.0.0.1:3000/api/dashboards/db \
  -H 'Content-Type: application/json' \
  -d "{\"dashboard\": $(python3 -c 'import json,sys;print(json.dumps(json.load(open(sys.argv[1]))))' "$UCM_ROOT/examples/metrics/grafana_store.json"), \"overwrite\": true}"
```
> `examples/metrics/grafana_store.json` 是仓库自带的 Store 面板。若它未直接含 `drampool_*` 面板，可在 Grafana 里手动加 panel，PromQL 用 `ucm_drampool_dump_requests_total`、`ucm_drampool_load_requests_total`、`ucm_drampool_lookup_requests_total`、`ucm_drampool_buffer_pool_usage_ratio`、`ucm_drampool_metadata_entry_count`。

### 8.4 备选：容器内无 prometheus/grafana 二进制时，用宿主机 docker
```bash
# 宿主机执行（容器是 host 网络，vLLM :7850 在宿主机可见）
docker network create ucm-monitoring 2>/dev/null
docker run -d --name z00984767-prom --network ucm-monitoring \
  -p 9090:9090 -v "$WORK/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  quay.io/prometheus/prometheus:v2.53.0
docker run -d --name z00984767-grafana --network ucm-monitoring \
  -p 3000:3000 docker.m.daocloud.io/grafana/grafana:11.1.0
# 注意：prometheus.yml 里 target 要写宿主机能访问到的 vLLM 地址（host 网络下 127.0.0.1:7850 即可）
```

### 8.5 浏览器访问
- Prometheus：`http://<A2-host-ip>:9090/targets`（确认 `vllm` 为 UP）
- Grafana：`http://<A2-host-ip>:3000`（admin/admin，首次登录改密）→ 导入的 Store 面板 → 应看到 `ucm_drampool_*` 曲线随请求增长。

---

## 步骤 9：端到端验收判定

| 层 | 检查 | 通过标准 |
| --- | --- | --- |
| writer JSON | `$WORK/metrics/drampool_metrics.json` | dump/load/lookup counter 随请求非零增长 |
| vLLM /metrics | `curl :7850/metrics \| grep '^ucm:drampool'` | 出现 `ucm:drampool_*_requests_total` 且与 JSON 一致 |
| Prometheus | `:9090/api/v1/query?query=ucm_drampool_dump_requests_total` | 返回 series，值匹配 |
| Grafana | 面板曲线 / `:3000/api/ds/query` | datasource health OK，曲线随请求上升 |

四层数值一致即端到端通过。

---

## 已知风险与排查

1. **reporter 重启卡死（PR 1394 已知缺陷）**：DramPool daemon 重启而 vLLM 不重启时，`resource_reporter.py` 的 `snapshot_deltas` 可能抛 `Histogram sum changed without samples` 并永久卡死。
   - 现象：`$WORK/logs/vllm.log` 反复报该错，Grafana 指标不再更新。
   - 恢复：删 `/dev/shm/ucm_drampool_metrics_*.json` 后重启 vLLM，或只起 daemon 不发请求等 ~15s 让其写全零快照自愈。
   - 规避：联调期间**尽量别单独重启 drampool**；要重启就连 vLLM 一起重启。
2. **`--kvcache-block-sizes` 不匹配**：若 vLLM 实际 KV block 字节数不在 daemon 启动参数里，dump/load 会失败或 miss。先跑一次看 vLLM 日志里 UCM 报告的 block/tensor size，回填到步骤4再重启 daemon。
3. **`--nics` 取值**：单节点回环用 `lo`；若容器有真实 RDMA 网卡且 DramStore/DramPool 走 RDMA，换成 `ip link`/`/sys/class/infiniband` 里的网卡名（如 `mlx5_0`）。
4. **端口冲突**：步骤1.6 已预检；若 9000/9001/4501/4502/36666 被占，统一改 drampool.yaml + ucm_dramstore.yaml 两边对应值。
5. **指标不刷新**：UCM 指标只在 vLLM 处理请求后更新，空闲时 `/metrics` 里 `ucm:` 可能很少甚至没有，属正常——多发几次请求再看。
6. **A2 host-sync patch**：交接文档提到 A2 上 HIXL host transfer 需要临时 patch（`dramstore_a2_hixl_host_sync.patch`，改 `ucm/transport/p2p/.../hixl_*`）。**仅当步骤5/6 出现 host memory transfer 卡住/超时**才考虑，且只在隔离 checkout 临时应用、不推送。先按现状跑，能通就不打 patch。

---

## 清理（验证结束后，容器内）

```bash
kill "$(cat "$WORK/vllm.pid")" "$(cat "$WORK/drampool.pid")" \
     "$(cat "$WORK/prom.pid")" "$(cat "$WORK/grafana.pid")" 2>/dev/null
# 宿主机 docker 方案则：docker rm -f z00984767-prom z00984767-grafana
rm -rf /dev/shm/ucm_drampool_metrics_*   # 清 reporter 选主/状态文件
# $WORK 目录（配置/日志/监控数据）按需保留或删除；不要动仓库源码与 git 状态
```
