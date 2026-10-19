#!/bin/bash
# =============================================================================
#  DramPool + DramStore + vLLM + Grafana 一键联调脚本 (昇腾 A2)
#
#  在容器 z00984767_ascend_test 内运行。串起完整链路：
#    真实 vLLM 请求 -> DramStore -> DramPool(C++ writer) -> drampool_metrics.json
#    -> DramPoolResourceReporter -> ucmmetrics -> vLLM /metrics (ucm:*)
#    -> Prometheus -> Grafana
#
#  用法:
#    bash e2e_dramstore_all_in_one.sh           # 全流程
#    bash e2e_dramstore_all_in_one.sh --check   # 只检查环境+服务状态
#    bash e2e_dramstore_all_in_one.sh --stop    # 停掉本轮拉起的进程
#    bash e2e_dramstore_all_in_one.sh --req     # 只发请求(服务已在跑)
#
#  本脚本编码了本次联调得到的全部关键约束，改任何一处前请先读文末「不可违反的约束」。
# =============================================================================

# ---------- 可调参数 ----------
UCM_ROOT=${UCM_ROOT:-/root/z00984767/0917/unified-cache-management}
WORK=${WORK:-/root/z00984767/0917/e2e-run}
PY=${PY:-/home/z00984767/venv1394/bin/python}
MODEL=${MODEL:-/home/codex/models-hf/models--Qwen--Qwen3-0.6B/snapshots/c1899de289a04d12100db370d81485cdf75e47ca}

# 设备拓扑：三者必须互不相同，否则 HIXL 拒绝同卡路由
WORKER_DEV=${WORKER_DEV:-0}        # vLLM worker(模型) 用卡；由 vLLM 自己选，此处仅记录
SCHED_DEV=${SCHED_DEV:-2}          # UCM scheduler 运行时卡；编译进 config.h RuntimeDeviceId()
POOL_DEV=${POOL_DEV:-3}            # drampool 数据卡；写入 drampool.yaml transport.device_ids

VLLM_PORT=${VLLM_PORT:-7850}
PROM_PORT=${PROM_PORT:-9090}
GRAFANA_PORT=${GRAFANA_PORT:-3000}
POOL_GB=${POOL_GB:-2}
NIC=${NIC:-mlx5_0}
# Qwen3-0.6B @ block-size 128 的真实 KV 布局：
#   tensor_size_list=[262144,262144]  shard_size=524288  block_size=14680064(28层*524288)
# BufferManager 按 size 精确匹配 slot 池，所以必须包含实际会请求的尺寸。
BLOCK_SIZES=${BLOCK_SIZES:-"7340032 14680064 524288 262144"}

PROM_BIN=${PROM_BIN:-/tmp/z009-extract/prometheus}
GRAF_BIN=${GRAF_BIN:-/tmp/z009-extract/grafana-home/bin/grafana}
GRAF_HOME=${GRAF_HOME:-/tmp/z009-extract/grafana-home}

mkdir -p "$WORK/logs" "$WORK/metrics" "$WORK/monitoring/promdata" \
         "$WORK/monitoring/grafana-data" "$WORK/monitoring/grafana-logs" \
         "$WORK/monitoring/dashboards" "$WORK/monitoring/provisioning/datasources" \
         "$WORK/monitoring/provisioning/dashboards"

log()  { printf '\033[1;36m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
ok()   { printf '\033[1;32m  OK\033[0m   %s\n' "$*"; }
warn() { printf '\033[1;33m  WARN\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m  FAIL\033[0m %s\n' "$*"; exit 1; }

# 注意：source Ascend set_env.sh 会引用未定义变量，脚本内不得使用 `set -u`

# =============================================================================
step_check_env() {
  log "步骤 1/7  检查环境"
  [ -d "$UCM_ROOT" ] || die "仓库不存在: $UCM_ROOT"
  [ -x "$PY" ]       || die "Python 不存在: $PY"
  [ -d "$MODEL" ]    || die "模型权重不存在: $MODEL"
  [ -f "$UCM_ROOT/bin/drampool" ] || die "drampool 未编译，先跑步骤 build"
  [ -f "$UCM_ROOT/ucm/store/dram/libdramstore.so" ] || die "libdramstore.so 缺失"
  ok "仓库/Python/模型/drampool 二进制齐备"

  $PY -c "import vllm,torch_npu,ucm" 2>/dev/null \
    && ok "venv 内 vllm + torch_npu + ucm 均可导入" \
    || die "venv 缺依赖（需 vllm/torch_npu/uc-manager editable）"

  # NPU 可用性：逐卡隔离探测，挂起的卡会被其它容器占用
  log "  探测可用 NPU（每卡独立进程，避免相互影响）"
  USABLE=""
  for d in 0 1 2 3 4 5 6 7; do
    if ASCEND_RT_VISIBLE_DEVICES=$d timeout 25 $PY -u -c "
import ctypes
a=ctypes.CDLL('libascendcl.so'); a.aclInit(None)
import sys; sys.exit(0 if a.aclrtSetDevice(0)==0 else 1)
" >/dev/null 2>&1; then
      USABLE="$USABLE $d"; printf '    NPU %s: 可用\n' "$d"
    else
      printf '    NPU %s: 不可用/挂起(被其它容器占用)\n' "$d"
    fi
  done
  ok "可用 NPU:$USABLE"
  for need in "$WORKER_DEV" "$SCHED_DEV" "$POOL_DEV"; do
    case " $USABLE " in *" $need "*) ;; *) die "设备 $need 不可用，请改 WORKER_DEV/SCHED_DEV/POOL_DEV" ;; esac
  done
  if [ "$WORKER_DEV" = "$SCHED_DEV" ] || [ "$SCHED_DEV" = "$POOL_DEV" ] || [ "$WORKER_DEV" = "$POOL_DEV" ]; then
    die "三个设备号必须互不相同（HIXL 拒绝同卡路由）"
  fi
  ok "设备拓扑合法: worker=$WORKER_DEV scheduler=$SCHED_DEV drampool=$POOL_DEV"

  # 校验编译进 .so 的 scheduler 设备号是否与 SCHED_DEV 一致
  local cur
  cur=$(grep -oE 'SCHEDULER \? [0-9]+' "$UCM_ROOT/ucm/store/dram/cc/config.h" 2>/dev/null | grep -oE '[0-9]+' | head -1)
  if [ "$cur" != "$SCHED_DEV" ]; then
    die "config.h RuntimeDeviceId() 是 $cur，但 SCHED_DEV=$SCHED_DEV。
       改 ucm/store/dram/cc/config.h 的 RuntimeDeviceId() 后必须重新编译(见 --build)"
  fi
  ok "config.h scheduler 设备($cur) 与 SCHED_DEV 一致"

  # 端口占用
  for p in $VLLM_PORT 9000 9001 4501 4502 $PROM_PORT $GRAFANA_PORT; do
    if (exec 3<>/dev/tcp/127.0.0.1/$p) 2>/dev/null; then
      case "$p" in
        $PROM_PORT|$GRAFANA_PORT|$VLLM_PORT) warn "端口 $p 已被占用（可能是本轮服务，继续）" ;;
        *) warn "端口 $p 被占用，若启动失败请检查" ;;
      esac
      exec 3>&- 3<&-
    fi
  done
  ok "端口检查完成"
}

# =============================================================================
step_build() {
  log "步骤 2/7  编译 UCM (dramstore / drampool / transport)"
  cd "$UCM_ROOT" || die "cd 失败"
  source /usr/local/Ascend/ascend-toolkit/set_env.sh 2>/dev/null
  [ -d build-pr1394-new ] || die "构建目录不存在，请先跑一次完整 cmake 配置"
  cmake --build build-pr1394-new --target ucm_p2p_transport dramstore drampool -j8 \
    2>&1 | tail -8 || die "编译失败"

  # 必须先停服务再拷贝，否则 "Text file busy" 会导致二进制未替换
  pkill -9 -f 'bin/drampool' 2>/dev/null; sleep 2
  cp build-pr1394-new/ucm/transport/p2p/libucm_p2p_transport.so ucm/transport/p2p/ || die "拷贝 transport 失败"
  cp build-pr1394-new/ucm/store/dram/libdramstore.so             ucm/store/dram/     || die "拷贝 dramstore 失败"
  cp build-pr1394-new/ucm/store/dram/drampool                    bin/drampool        || die "拷贝 drampool 失败"
  chmod +x bin/drampool
  ok "编译并安装完成: $(stat -c %y bin/drampool | cut -d. -f1)"
}

# =============================================================================
step_configs() {
  log "步骤 3/7  生成配置文件"

  # ---- drampool.yaml ----
  # transport.endpoints 是 peer 白名单(twoSidedToOneSided)。DramStore 按角色做端口偏移:
  #   scheduler: 无偏移          -> 9001 / 4502
  #   worker(dev=0): +(devId+1)  -> 9002 / 4503
  # 缺任何一条，drampool 都会 "rejected unconfigured control peer"，请求全部超时(-50010)。
  cat > "$WORK/drampool.yaml" <<EOF
transport:
  device_ids: [$POOL_DEV]
  hixl:
    listen_port: 26666
    enable_cs: false          # 置 true 会让 HIXL Initialize 返回 103900，必须 false
  endpoints:
    - two_sided: "127.0.0.1:9000"   # drampool 自身控制端(--addr)
      one_sided: "127.0.0.1:4501"   # drampool 自身传输端
    - two_sided: "127.0.0.1:9001"   # DramStore scheduler 控制端(无偏移)
      one_sided: "127.0.0.1:4502"   # DramStore scheduler 传输端
    - two_sided: "127.0.0.1:9002"   # DramStore worker 控制端(9001+deviceId+1)
      one_sided: "127.0.0.1:4503"   # DramStore worker 传输端(4502+deviceId+1)
health:
  port: 0
metrics:
  enabled: true
  output_dir: $WORK/metrics
  interval_ms: 2000
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
  dir: $WORK/logs
  max_files: 10
  max_size_mb: 5
EOF

  # ---- ucm_dramstore.yaml ----
  # drampool_resource_log_path 必须等于 drampool.yaml 的 metrics.output_dir + /drampool_metrics.json
  # enable_metrics + metrics_config_path 决定 ucm:* 指标是否挂到 vLLM /metrics
  cat > "$WORK/ucm_dramstore.yaml" <<EOF
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
      drampool_resource_log_path: "$WORK/metrics/drampool_metrics.json"
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
enable_metrics: true
metrics_config_path: "$UCM_ROOT/examples/metrics/metrics_configs.yaml"
EOF
  ok "drampool.yaml + ucm_dramstore.yaml 已生成"

  # ---- 启动器：drampool ----
  cat > "$WORK/launch_drampool.sh" <<EOF
#!/bin/bash
source /usr/local/Ascend/ascend-toolkit/set_env.sh 2>/dev/null
cd "$UCM_ROOT" || exit 1
export UC_LOGGER_LEVEL=\${UC_LOGGER_LEVEL:-info}
exec ./bin/drampool \\
  --addr 127.0.0.1:9000 \\
  --nics $NIC \\
  --pool-size-gb $POOL_GB \\
  --kvcache-block-sizes $BLOCK_SIZES \\
  --kvcache-block-proportions 2 1 1 1 \\
  --config "$WORK/drampool.yaml"
EOF

  # ---- 启动器：vLLM ----
  # 不能设 ASCEND_RT_VISIBLE_DEVICES：scheduler 硬编码 device $SCHED_DEV，
  # 限制可见设备会让 aclrtSetDevice($SCHED_DEV) 返回 507033(DEV_SETUP_ERROR)。
  # venv1394 的 PATH 里没有 vllm，必须用 python -m vllm.entrypoints.cli.main。
  cat > "$WORK/launch_vllm.sh" <<EOF
#!/bin/bash
source /usr/local/Ascend/ascend-toolkit/set_env.sh 2>/dev/null
source /usr/local/Ascend/nnal/atb/set_env.sh 2>/dev/null
cd "$UCM_ROOT" || exit 1
export ENABLE_UCM_PATCH=1
export VLLM_HASH_ATTENTION=0
export VLLM_CPU_AFFINITY=0
export ENABLE_SPARSE=FALSE
export VLLM_LOGGING_LEVEL=INFO
export UCM_LOG_LEVEL=\${UCM_LOG_LEVEL:-info}
unset ASCEND_RT_VISIBLE_DEVICES
exec "$PY" -m vllm.entrypoints.cli.main serve "$MODEL" \\
  --served-model-name qwen3-0.6b \\
  --tensor-parallel-size 1 \\
  --max-model-len 8192 \\
  --gpu-memory-utilization 0.7 \\
  --block-size 128 \\
  --trust-remote-code \\
  --enforce-eager \\
  --host 0.0.0.0 \\
  --port $VLLM_PORT \\
  --distributed-executor-backend mp \\
  --kv-transfer-config '{"kv_connector":"UCMConnector","kv_connector_module_path":"ucm.integration.vllm.ucm_connector","kv_role":"kv_both","kv_connector_extra_config":{"UCM_CONFIG_FILE":"$WORK/ucm_dramstore.yaml"}}'
EOF
  chmod +x "$WORK/launch_drampool.sh" "$WORK/launch_vllm.sh"
  ok "启动器已生成"
}

# =============================================================================
stop_all() {
  log "停止全部本轮进程"
  pkill -f 'vllm.entrypoints.cli.main' 2>/dev/null; sleep 4
  pkill -9 -f 'vllm.entrypoints.cli.main' 2>/dev/null
  pkill -9 -f EngineCore 2>/dev/null; pkill -9 -f VllmWorker 2>/dev/null
  [ -f "$WORK/drampool.pid" ] && kill "$(cat "$WORK/drampool.pid")" 2>/dev/null
  sleep 3; pkill -9 -f 'bin/drampool' 2>/dev/null
  # reporter 选主/状态文件必须清，否则会触发已知的 snapshot_deltas 卡死缺陷
  rm -f /dev/shm/ucm_drampool_metrics_*.json /dev/shm/ucm_drampool_metrics_*.lock 2>/dev/null
  ok "已停止 vLLM / drampool，并清理 /dev/shm reporter 状态"
}

# =============================================================================
step_start_pool() {
  log "步骤 4/7  启动 drampool（必须先于 vLLM）"
  : > "$WORK/logs/drampool.log"
  setsid bash -c "exec '$WORK/launch_drampool.sh' >> '$WORK/logs/drampool.log' 2>&1" </dev/null &
  echo $! > "$WORK/drampool.pid"; disown 2>/dev/null || true
  for i in $(seq 1 40); do
    grep -q "service ready" "$WORK/logs/drampool.log" 2>/dev/null && break
    grep -q "init failed"   "$WORK/logs/drampool.log" 2>/dev/null && \
      die "drampool 启动失败:\n$(tail -5 "$WORK/logs/drampool.log")"
    sleep 1
  done
  grep -q "service ready" "$WORK/logs/drampool.log" \
    && ok "drampool ready (pid $(cat "$WORK/drampool.pid"), device $POOL_DEV)" \
    || die "drampool 40s 内未 ready，看 $WORK/logs/drampool.log"
  grep -c "BufferManager initialized BufferPool" "$WORK/logs/drampool.log" >/dev/null && \
    log "  slot 池: $(grep -oE 'slot_size=[0-9]+, slot_count=[0-9]+' "$WORK/logs/drampool.log" | tr '\n' ' ')"
}

# =============================================================================
step_start_vllm() {
  log "步骤 5/7  启动 vLLM（drampool ready 之后）"
  : > "$WORK/logs/vllm.log"
  setsid bash -c "exec '$WORK/launch_vllm.sh' >> '$WORK/logs/vllm.log' 2>&1" </dev/null &
  echo $! > "$WORK/vllm.pid"; disown 2>/dev/null || true
  local ready=0
  for i in $(seq 1 60); do
    if curl -sf --max-time 5 "http://127.0.0.1:$VLLM_PORT/health" >/dev/null 2>&1; then ready=1; break; fi
    kill -0 "$(cat "$WORK/vllm.pid")" 2>/dev/null || break
    sleep 6
  done
  [ $ready -eq 1 ] || {
    echo "--- vllm.log 关键错误 ---"
    grep -nE "ERROR|Traceback|507033|aclrtSetDevice|RuntimeError|EngineCore failed" "$WORK/logs/vllm.log" | tail -20
    die "vLLM 未就绪，日志: $WORK/logs/vllm.log"
  }
  ok "vLLM ready (pid $(cat "$WORK/vllm.pid"), :$VLLM_PORT)"
  grep -q "create UcmPipelineStore" "$WORK/logs/vllm.log" \
    && ok "UCM connector 已挂载 (store_pipeline=Dram)" \
    || warn "未见 create UcmPipelineStore，检查 --kv-transfer-config"
  grep -oE "'block_size': [0-9]+" "$WORK/logs/vllm.log" | head -1 | \
    sed 's/^/  实际 KV block_size = /'
}

# =============================================================================
step_start_monitoring() {
  log "步骤 6/7  启动 Prometheus + Grafana（容器内二进制）"
  [ -x "$PROM_BIN" ] || die "prometheus 二进制缺失: $PROM_BIN"
  [ -x "$GRAF_BIN" ] || die "grafana 二进制缺失: $GRAF_BIN"

  cat > "$WORK/monitoring/prometheus.yml" <<EOF
global:
  scrape_interval: 5s
  evaluation_interval: 30s
scrape_configs:
  - job_name: vllm
    metrics_path: /metrics
    static_configs:
      - targets: ["127.0.0.1:$VLLM_PORT"]
        labels:
          instance: "z00984767-a2-vllm"
EOF

  pkill -f 'z009-extract/prometheus' 2>/dev/null; sleep 1
  setsid "$PROM_BIN" \
    --config.file="$WORK/monitoring/prometheus.yml" \
    --storage.tsdb.path="$WORK/monitoring/promdata" \
    --web.listen-address=0.0.0.0:$PROM_PORT \
    --web.enable-lifecycle \
    >> "$WORK/logs/prometheus.log" 2>&1 </dev/null &
  echo $! > "$WORK/prom.pid"; disown 2>/dev/null || true
  sleep 6
  curl -sf --max-time 6 "http://127.0.0.1:$PROM_PORT/-/ready" >/dev/null \
    && ok "Prometheus ready :$PROM_PORT" || die "Prometheus 启动失败，看 $WORK/logs/prometheus.log"

  cat > "$WORK/monitoring/grafana.ini" <<EOF
app_mode = production
[paths]
data = $WORK/monitoring/grafana-data
logs = $WORK/monitoring/grafana-logs
plugins = $WORK/monitoring/grafana-plugins
provisioning = $WORK/monitoring/provisioning
[server]
http_addr = 0.0.0.0
http_port = $GRAFANA_PORT
[security]
admin_user = admin
admin_password = admin1394
[auth.anonymous]
enabled = true
org_role = Viewer
[analytics]
reporting_enabled = false
check_for_updates = false
[log]
mode = file
level = warn
EOF

  cat > "$WORK/monitoring/provisioning/datasources/prom.yml" <<EOF
apiVersion: 1
datasources:
  - name: prom
    type: prometheus
    access: proxy
    url: http://127.0.0.1:$PROM_PORT
    isDefault: true
    uid: prom-a2
    jsonData:
      timeInterval: "5s"
EOF

  cat > "$WORK/monitoring/provisioning/dashboards/dash.yml" <<EOF
apiVersion: 1
providers:
  - name: ucm-drampool
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    allowUiUpdates: true
    options:
      path: $WORK/monitoring/dashboards
EOF

  pkill -f 'grafana-home/bin/grafana' 2>/dev/null; sleep 1
  setsid "$GRAF_BIN" server \
    --homepath "$GRAF_HOME" \
    --config "$WORK/monitoring/grafana.ini" \
    >> "$WORK/logs/grafana.log" 2>&1 </dev/null &
  echo $! > "$WORK/grafana.pid"; disown 2>/dev/null || true
  for i in $(seq 1 20); do
    curl -sf --max-time 5 "http://127.0.0.1:$GRAFANA_PORT/api/health" >/dev/null 2>&1 && break
    sleep 2
  done
  curl -sf --max-time 6 "http://127.0.0.1:$GRAFANA_PORT/api/health" >/dev/null \
    && ok "Grafana ready :$GRAFANA_PORT (admin/admin1394)" \
    || die "Grafana 启动失败，看 $WORK/logs/grafana.log"

  build_dashboard
  import_dashboard
}

# =============================================================================
build_dashboard() {
  # Prometheus 保留 'ucm:' 里的冒号，所以面板必须用 {__name__="ucm:..."} 选择器，
  # 直接写 ucm_drampool_* 会查不到任何数据。
  "$PY" - "$WORK/monitoring/dashboards/drampool_e2e.json" <<'PYGEN'
import json, sys
DS = {"type": "prometheus", "uid": "prom-a2"}
def m(n): return '{__name__="ucm:%s"}' % n
def t(e, l): return {"expr": e, "legendFormat": l, "refId": "A"}
def p(i, title, targets, unit, x, y, w=12, h=8):
    return {"id": i, "type": "timeseries", "title": title, "datasource": DS,
            "targets": targets, "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "fieldConfig": {"defaults": {"unit": unit, "custom": {"fillOpacity": 8,
                            "lineWidth": 2, "drawStyle": "line", "showPoints": "never"}},
                            "overrides": []},
            "options": {"legend": {"displayMode": "table", "placement": "bottom",
                                   "calcs": ["lastNotNull", "max"]},
                        "tooltip": {"mode": "multi", "sort": "desc"}}}
P = [
 p(0,"DramPool KV Requests (rate/s)",[
   t('sum(rate(%s[1m]))'%m("drampool_dump_requests_total"),'dump/s'),
   t('sum(rate(%s[1m]))'%m("drampool_lookup_requests_total"),'lookup/s'),
   t('sum(rate(%s[1m]))'%m("drampool_load_requests_total"),'load/s')],"reqps",0,0),
 p(1,"DramPool KV Requests (cumulative)",[
   t('sum(%s)'%m("drampool_dump_requests_total"),'dump'),
   t('sum(%s)'%m("drampool_lookup_requests_total"),'lookup'),
   t('sum(%s)'%m("drampool_load_requests_total"),'load')],"short",12,0),
 p(2,"DramPool Pool Usage Ratio",[
   t('max(%s)'%m("drampool_buffer_pool_usage_ratio"),'buffer_pool'),
   t('max(%s)'%m("drampool_flag_pool_usage_ratio"),'flag_pool')],"percentunit",0,8),
 p(3,"DramPool Metadata Entries (resident blocks)",[
   t('max(%s)'%m("drampool_metadata_entry_count"),'entries')],"short",12,8),
 p(4,"Entry Outcomes (rate/s)",[
   t('sum(rate(%s[1m]))'%m("drampool_lookup_miss_entries_total"),'lookup_miss'),
   t('sum(rate(%s[1m]))'%m("drampool_load_miss_entries_total"),'load_miss'),
   t('sum(rate(%s[1m]))'%m("drampool_dump_failed_entries_total"),'dump_failed')],"reqps",0,16),
 p(5,"Failure Counters (rate/s)",[
   t('sum(rate(%s[1m]))'%m("drampool_transfer_failures_total"),'transfer'),
   t('sum(rate(%s[1m]))'%m("drampool_response_failures_total"),'response'),
   t('sum(rate(%s[1m]))'%m("drampool_submit_failures_total"),'submit'),
   t('sum(rate(%s[1m]))'%m("drampool_dump_nospace_failures_total"),'dump_nospace')],"reqps",12,16),
 p(6,"Batch Latency p50/p99 (ms)",[
   t('histogram_quantile(0.5, sum by (le) (rate(%s[2m])))'%m("drampool_dump_batch_total_duration_ms_bucket"),'dump p50'),
   t('histogram_quantile(0.99, sum by (le) (rate(%s[2m])))'%m("drampool_dump_batch_total_duration_ms_bucket"),'dump p99'),
   t('histogram_quantile(0.5, sum by (le) (rate(%s[2m])))'%m("drampool_lookup_batch_total_duration_ms_bucket"),'lookup p50'),
   t('histogram_quantile(0.5, sum by (le) (rate(%s[2m])))'%m("drampool_load_batch_total_duration_ms_bucket"),'load p50')],"ms",0,24),
 p(7,"Transfer / Submit Latency p50 (ms)",[
   t('histogram_quantile(0.5, sum by (le) (rate(%s[2m])))'%m("drampool_dump_transfer_duration_ms_bucket"),'dump_transfer'),
   t('histogram_quantile(0.5, sum by (le) (rate(%s[2m])))'%m("drampool_load_transfer_duration_ms_bucket"),'load_transfer'),
   t('histogram_quantile(0.5, sum by (le) (rate(%s[2m])))'%m("drampool_dump_submit_duration_ms_bucket"),'dump_submit'),
   t('histogram_quantile(0.5, sum by (le) (rate(%s[2m])))'%m("drampool_get_status_duration_ms_bucket"),'get_status')],"ms",12,24),
 p(8,"Request / Completion Queue Depth",[
   t('max(%s)'%m("drampool_queue_request_size"),'request queue'),
   t('max(%s)'%m("drampool_queue_completion_inflight"),'completion inflight'),
   t('max(%s)'%m("drampool_queue_completion_size"),'completion queue')],"short",0,32),
 p(9,"Reporter / Backpressure Health",[
   t('sum(rate(%s[1m]))'%m("drampool_resource_log_read_errors_total"),'log_read_errors'),
   t('sum(rate(%s[1m]))'%m("drampool_queue_request_full_total"),'queue_request_full'),
   t('sum(rate(%s[1m]))'%m("drampool_queue_completion_full_total"),'queue_completion_full'),
   t('sum(rate(%s[1m]))'%m("drampool_queue_response_buffer_retry_total"),'response_buffer_retry')],"reqps",12,32),
 p(10,"vLLM Cache Hits (UCM connector, blocks/s)",[
   t('sum(rate(%s[1m]))'%m("total_prefix_query_blocks_total"),'prefix_query'),
   t('sum(rate(%s[1m]))'%m("gpu_hbm_hit_blocks_total"),'gpu_hbm_hit'),
   t('sum(rate(%s[1m]))'%m("ucm_hit_blocks_total"),'external_ucm_hit')],"reqps",0,40),
 p(11,"DramStore Connect Attempts / Task Queue",[
   t('sum(%s)'%m("dramstore_connect_attempts_total"),'connect_attempts'),
   t('max(%s)'%m("dramstore_task_queue_size"),'task_queue_size')],"short",12,40),
]
dash = {"uid":"drampool-e2e-z009",
        "title":"DramPool / DramStore E2E - A2 z00984767 (PR1394)",
        "tags":["ucm","drampool","dramstore","pr1394","a2"],
        "timezone":"browser","schemaVersion":39,"version":1,"refresh":"5s",
        "editable":True,"graphTooltip":1,"time":{"from":"now-30m","to":"now"},"panels":P}
json.dump(dash, open(sys.argv[1],"w"), indent=1)
json.dump({"dashboard":dash,"overwrite":True}, open("/tmp/dash_payload.json","w"))
print("  dashboard: %d panels -> %s" % (len(P), sys.argv[1]))
PYGEN
}

import_dashboard() {
  local code
  code=$(curl -s -o /tmp/import_resp.json -w '%{http_code}' -X POST \
    -u admin:admin1394 -H 'Content-Type: application/json' \
    --data-binary @/tmp/dash_payload.json \
    "http://127.0.0.1:$GRAFANA_PORT/api/dashboards/db" 2>/dev/null)
  [ "$code" = "200" ] && ok "Grafana 面板已导入 (uid=drampool-e2e-z009)" \
                      || warn "面板导入返回 $code: $(head -c 200 /tmp/import_resp.json)"
}

# =============================================================================
step_requests() {
  log "步骤 7/7  发真实请求 + 三层证据校验"

  # 必须用全新内容的 prompt：重复 prompt 会被 GPU 内部 prefix cache 全命中
  # (日志 hit hbm=N / Prefix cache hit rate 上升)，根本不会走外部 DramStore。
  # prompt 需 >128 token 才会产生至少一个完整 block。
  "$PY" - <<'PY'
import json, time, random
base=("The unified cache manager offloads KV cache blocks from accelerator memory into a "
      "pooled host DRAM tier so that a later request can restore them instead of recomputing "
      "every transformer layer from the original prompt tokens. ")
tag=f"e2e-{int(time.time())}-{random.randint(1000,9999)}"
for n in ("r1","r2","r3"):
    json.dump({"model":"qwen3-0.6b","prompt":f"[{tag}-{n}] "+base*60,
               "max_tokens":8,"temperature":0},
              open(f"/root/z00984767/0917/e2e-run/req_{n}.json","w"))
print(f"  生成 3 个全新 prompt, tag={tag}")
PY

  for n in r1 r2 r3; do
    printf '    发送 %s -> ' "$n"
    curl -sf --max-time 120 "http://127.0.0.1:$VLLM_PORT/v1/completions" \
      -H 'Content-Type: application/json' --data-binary @"$WORK/req_$n.json" >/dev/null 2>&1 \
      && echo "200 OK" || echo "FAILED"
    sleep 5
  done
  sleep 5   # 等 reporter(2s) + prometheus(5s) 抓取

  echo
  log "三层证据对比"
  "$PY" - <<'PY'
import json, urllib.request, urllib.parse
def get(u):
    try: return urllib.request.urlopen(u, timeout=10).read().decode()
    except Exception: return ""
P="/root/z00984767/0917/e2e-run/metrics/drampool_metrics.json"
try:
    lines=[l for l in open(P).read().splitlines() if l.strip()]
    d=json.loads(lines[-1])
except Exception as e:
    print("  writer JSON 读取失败:", e); raise SystemExit(1)
c, g = d.get("counters",{}), d.get("gauges",{})
raw = get("http://127.0.0.1:7850/metrics")
ROWS=[("drampool_dump_requests_total","c"),("drampool_lookup_requests_total","c"),
      ("drampool_load_requests_total","c"),("drampool_lookup_miss_entries_total","c"),
      ("drampool_dump_failed_entries_total","c"),("drampool_transfer_failures_total","c"),
      ("drampool_metadata_entry_count","g"),("drampool_buffer_pool_usage_ratio","g")]
def f(v):
    try: return "%.4g" % float(v)
    except Exception: return str(v)
print(f"  {'指标':38s} {'writerJSON':>11s} {'vLLM/metrics':>13s} {'Prometheus':>11s}")
print("  "+"-"*78)
for k,kind in ROWS:
    v1=f((c if kind=="c" else g).get(k,0))
    v2="n/a"
    for line in raw.splitlines():
        if line.startswith("ucm:%s{"%k): v2=f(line.split()[-1]); break
    q=urllib.parse.quote('{__name__="ucm:%s"}'%k)
    v3="n/a"
    try:
        r=json.loads(get("http://127.0.0.1:9090/api/v1/query?query=%s"%q))["data"]["result"]
        if r: v3=f(r[0]["value"][1])
    except Exception: pass
    flag="✓" if v1==v2==v3 else ("~" if v3!="n/a" else "?")
    print(f"  {k:38s} {v1:>11s} {v2:>13s} {v3:>11s}  {flag}")
PY

  echo
  log "Grafana 面板查询验证"
  curl -sf --max-time 10 -u admin:admin1394 -G \
    "http://127.0.0.1:$GRAFANA_PORT/api/datasources/proxy/uid/prom-a2/api/v1/query" \
    --data-urlencode 'query={__name__="ucm:drampool_dump_requests_total"}' 2>/dev/null \
    | grep -q '"result":\[{' && ok "Grafana -> Prometheus 查询返回真实数据" \
                             || warn "Grafana 查询无数据"

  echo
  log "数据面状态"
  local n507
  n507=$(grep -c 507001 "$WORK/logs/drampool.log" 2>/dev/null)
  if [ "${n507:-0}" -gt 0 ]; then
    warn "HIXL 传输失败 $n507 次 (ACL 507001) —— 见总结文档「已知阻塞」章节"
    warn "指标链路不受影响：counters/gauges 由请求计数与内存分配驱动，已正常上报"
  else
    ok "无 HIXL 传输错误"
  fi
  grep -E "hit hbm|hit external" "$WORK/logs/vllm.log" 2>/dev/null | tail -2 | \
    sed 's/^.*request_id/request_id/' | sed 's/^/    /'
}

# =============================================================================
print_access() {
  # 容器内通常没有 hostname / ip / ifconfig，用 Python 通过默认路由接口取地址；
  # 也可用 HOST_IP 环境变量直接覆盖。
  local ip="${HOST_IP:-}"
  if [ -z "$ip" ]; then
    ip=$("$PY" - <<'PYIP' 2>/dev/null
import socket, struct
def default_iface():
    for line in open("/proc/net/route").read().splitlines()[1:]:
        f = line.split()
        if len(f) > 2 and f[1] == "00000000":
            return f[0]
    return None
iface = default_iface()
if iface:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        packed = struct.pack("256s", iface.encode()[:15])
        ip = socket.inet_ntoa(
            __import__("fcntl").ioctl(s.fileno(), 0x8915, packed)[20:24])  # SIOCGIFADDR
        print(ip)
    except Exception:
        pass
PYIP
)
  fi
  [ -n "$ip" ] || ip="<host-ip>"
  echo
  echo "=========================================================================="
  echo "  访问地址 (宿主机 $ip)"
  echo "--------------------------------------------------------------------------"
  echo "  Grafana     http://$ip:$GRAFANA_PORT/d/drampool-e2e-z009"
  echo "              账号 admin / admin1394"
  echo "  Prometheus  http://$ip:$PROM_PORT/targets"
  echo "  vLLM        http://$ip:$VLLM_PORT/metrics"
  echo "--------------------------------------------------------------------------"
  echo "  日志目录    $WORK/logs/{drampool,vllm,prometheus,grafana}.log"
  echo "  writer JSON $WORK/metrics/drampool_metrics.json"
  echo "  停止服务    bash $0 --stop"
  echo "=========================================================================="
}

# =============================================================================
status_only() {
  log "当前状态"
  for f in drampool vllm prom grafana; do
    if [ -f "$WORK/$f.pid" ] && kill -0 "$(cat "$WORK/$f.pid")" 2>/dev/null; then
      ok "$f 运行中 (pid $(cat "$WORK/$f.pid"))"
    else
      warn "$f 未运行"
    fi
  done
  for u in "vLLM:$VLLM_PORT/health" "Prometheus:$PROM_PORT/-/ready" "Grafana:$GRAFANA_PORT/api/health"; do
    local name=${u%%:*} path=${u#*:}
    printf '  %-12s %s\n' "$name" "$(curl -sf -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:$path" 2>/dev/null || echo FAIL)"
  done
  print_access
}

# =============================================================================
case "${1:-all}" in
  --check)  step_check_env; status_only ;;
  --build)  step_check_env; step_build ;;
  --stop)   stop_all; pkill -f 'z009-extract/prometheus' 2>/dev/null
            pkill -f 'grafana-home/bin/grafana' 2>/dev/null
            ok "已停止 Prometheus / Grafana" ;;
  --req)    step_requests; print_access ;;
  --status) status_only ;;
  all)
    step_check_env
    step_configs
    stop_all
    step_start_pool
    step_start_vllm
    step_start_monitoring
    step_requests
    print_access
    ;;
  *) echo "用法: $0 [--check|--build|--stop|--req|--status|all]"; exit 2 ;;
esac

# =============================================================================
# 不可违反的约束（本次联调实测得出，违反任意一条链路就断）
#
# 1. 三个设备号必须互不相同：vLLM worker / UCM scheduler / drampool。
#    HIXL 明确拒绝同卡路由(hixl_transport.cpp BuildRouteLocked "same device")。
#
# 2. scheduler 的设备号是编译进 config.h RuntimeDeviceId() 的常量，无 YAML 开关。
#    改它必须重新编译 dramstore。且因为它是绝对设备号，vLLM 进程绝不能设
#    ASCEND_RT_VISIBLE_DEVICES，否则 aclrtSetDevice 返回 507033(DEV_SETUP_ERROR)。
#
# 3. 启动顺序：drampool 先 ready，再起 vLLM。DramStore 只在启动时做一次 HIXL
#    exchange-metadata + connect；drampool 中途重启后，vLLM 只会重建 TCP 控制通道
#    (epoch 递增) 而不会重做握手，drampool 的 HIXL peer 表为空 -> 所有传输
#    "unknown peer"。vLLM 运行期间不要重启 drampool。
#
# 4. drampool.yaml 的 transport.endpoints 是 peer 白名单(twoSidedToOneSided)，
#    必须覆盖 DramStore 偏移后的真实端口：scheduler 9001/4502、worker 9002/4503。
#    缺条目 -> "rejected unconfigured control peer" -> 全部 -50010 超时。
#
# 5. --kvcache-block-sizes 必须包含实际请求的 entry 长度。BufferManager::Allocate
#    用 GetPool(size) 精确匹配(pools_.find)，不是"最小可容纳"。Qwen3-0.6B
#    @ block-size 128 的真实布局是 262144/524288/7340032/14680064。
#
# 6. enable_cs / enable_hixl_cs 必须 false（两侧一致）。置 true 时 HIXL
#    Initialize 返回 103900，drampool 直接起不来。
#
# 7. Prometheus 保留指标名里的冒号，Grafana/PromQL 必须写 {__name__="ucm:xxx"}，
#    写成 ucm_xxx 查不到数据。
#
# 8. 必须用全新内容的 prompt 且 >128 token。重复 prompt 会被 GPU 内部 prefix
#    cache 全命中(hit hbm=N)，不会访问外部 DramStore，counters 保持 0。
#
# 9. 每次重启前清 /dev/shm/ucm_drampool_metrics_*.json|.lock，否则 reporter 的
#    陈旧 state 会触发已知的 snapshot_deltas 卡死缺陷。
#
# 10. venv1394 的 PATH 里没有 vllm，用 python -m vllm.entrypoints.cli.main。
#     拷贝编译产物前必须先停进程，否则 "Text file busy" 会静默保留旧二进制。
# =============================================================================
