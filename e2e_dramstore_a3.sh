#!/bin/bash
# =============================================================================
#  DramPool + DramStore + vLLM + Grafana 一键联调脚本 (昇腾 A3 / 910C)
#
#  这是 e2e_dramstore_all_in_one.sh 的 A3 版本。与 A2 版最大的不同：
#    * A3 容器里 UCM 是 pip 安装的（wheel 或 editable），没有 build-pr1394-new
#      这种 cmake 构建树，也不需要手动 cp 二进制。drampool / libdramstore.so /
#      libucm_p2p_transport.so 的位置全部自动探测。
#    * examples/ 不会被打进 wheel，所以 metrics_configs.yaml 从仓库源码目录读。
#    * config.h 里 scheduler 运行时设备号当前是 0（不是 A2 文档里的 2），本脚本
#      实时 grep 源码确定，不写死。
#    * A3 平台编译开的是 SDMA_DIRECT（A2 是 IO_AGGREGATION）；vLLM 侧按 A3 runbook
#      额外导出 ASCEND_A3_ENABLE=1。
#
#  在 A3 容器内运行。串起完整链路：
#    真实 vLLM 请求 -> DramStore -> DramPool(C++ writer) -> drampool_metrics.json
#    -> DramPoolResourceReporter -> ucmmetrics -> vLLM /metrics (ucm:*)
#    -> Prometheus -> Grafana
#
#  用法:
#    bash e2e_dramstore_a3.sh              # 全流程 (all)
#    bash e2e_dramstore_a3.sh --check      # 只检查环境 + 服务状态
#    bash e2e_dramstore_a3.sh --build      # 改了 C++/config.h 后重新编译(editable)
#    bash e2e_dramstore_a3.sh --stop       # 停掉本轮拉起的进程
#    bash e2e_dramstore_a3.sh --req        # 只发请求(服务已在跑)
#    bash e2e_dramstore_a3.sh --status     # 只看运行状态和访问地址
#
#  本脚本编码了 A2/A3 两轮联调得到的全部关键约束，改任何一处前请先读文末
#  「不可违反的约束」。
#
#  ⚠ A3 + hixl 首次运行前必看：
#    当前源码 config.h::RuntimeDeviceId() 把 scheduler 运行时设备号硬编码为 0，
#    而 vLLM worker 通常也自然选中 device 0。hixl 后端拒绝同卡路由，--check 会
#    直接 die。两种解法二选一：
#      (a) 改 ucm/store/dram/cc/config.h 的 scheduler 设备号到一个空闲卡(如 2)，
#          跑 `bash e2e_dramstore_a3.sh --build` 重编，再 POOL_DEV=3 起服务；
#          推荐拓扑 worker=0 / scheduler=2 / pool=3（与 A2 验证过的一致）。
#      (b) 若只想单机验证且 RoCE 不通，改 TRANSPORT=acl_ipc（无同卡限制）。
# =============================================================================

# ---------- 可调参数（全部可用环境变量覆盖）----------
# A3 官方容器把仓库放在 /workspace/unified-cache-management
UCM_ROOT=${UCM_ROOT:-/workspace/unified-cache-management}
WORK=${WORK:-/workspace/e2e-run}
# A3 容器里 python3 一般就在 PATH 上；若用独立 venv 请覆盖 PY
PY=${PY:-$(command -v python3 2>/dev/null || echo /usr/bin/python3)}
# 模型权重目录：A3 机器上按实际路径覆盖（脚本不猜）
MODEL=${MODEL:-}

# 设备拓扑：hixl 后端下三者必须互不相同，否则 HIXL 拒绝同卡路由。
# scheduler 设备号是编译进 config.h RuntimeDeviceId() 的常量，下面 step_check_env
# 会 grep 出真实值并覆盖 SCHED_DEV；worker 由 vLLM 自己选卡。
WORKER_DEV=${WORKER_DEV:-0}        # vLLM worker(模型) 用卡；由 vLLM 自己选，此处仅记录
SCHED_DEV=${SCHED_DEV:-}           # 留空则从 config.h 自动读取（当前源码值为 0）
POOL_DEV=${POOL_DEV:-3}            # drampool 数据卡；写入 drampool.yaml transport.device_ids
                                   # 推荐拓扑 worker=0 / scheduler=2 / pool=3；scheduler=2 需改 config.h 后 --build

VLLM_PORT=${VLLM_PORT:-7850}
PROM_PORT=${PROM_PORT:-9090}
GRAFANA_PORT=${GRAFANA_PORT:-3000}
POOL_GB=${POOL_GB:-2}
# --nics 是空壳参数：只被 ValidateNics() 检查非空，从未传给 HIXL/传输层。
# 填什么都不影响数据面，保留只是为了通过非空校验。
NIC=${NIC:-mlx5_0}

# 传输后端：hixl（默认，走 HCCL/RoCE，A3 runbook 主路径）或 acl_ipc（走 ACL IPC +
# aclrtMemcpy，单机、不依赖 RoCE）。
#   - hixl 要求 NPU 的 RoCE 链路 UP，否则建组失败、传输全部返回 503900/507001。
#   - acl_ipc 仅支持单机、drampool 侧实际只用 device_ids 的第一张卡。
# 两侧进程(drampool + vLLM)必须设成同一个值，否则 InstallTransport 注册的协议与
# 路由决策不一致，所有传输报 "protocol not installed"。
TRANSPORT=${TRANSPORT:-hixl}

# KV block 尺寸：模型/runtime 相关，不是设备相关。默认值对应 Qwen3-0.6B
# @ --block-size 128（8 KV 头 * head_dim 128 * bf16 * 128 token = 262144/tensor，
# K+V shard=524288，28 层整块=14680064，半层组=7340032）。换模型必须重算：
#   per-tensor bytes/block = prod(tensor.shape[1:]) * element_size
# BufferManager 按 size 精确匹配 slot 池(pools_.find)，没有"最小可容纳"回退，
# 所以必须包含实际会请求的每一个尺寸。
BLOCK_SIZES=${BLOCK_SIZES:-"7340032 14680064 524288 262144"}
BLOCK_PROPS=${BLOCK_PROPS:-"2 1 1 1"}

# A3 vLLM runbook 要求的环境变量（仅文档要求，UCM 代码不强制）。设为 0 可关闭。
ASCEND_A3_ENABLE=${ASCEND_A3_ENABLE:-1}

# 监控二进制：A3 容器不一定自带，按实际路径覆盖。缺失时 step_start_monitoring
# 会明确报错并给出获取方式（不会静默跳过，因为你要求了全链路）。
PROM_BIN=${PROM_BIN:-}
GRAF_BIN=${GRAF_BIN:-}
GRAF_HOME=${GRAF_HOME:-}

# 构建模式：editable（pip install -e .，A3 dev 文档流程，产物落回仓库）或 skip。
BUILD_MODE=${BUILD_MODE:-editable}

mkdir -p "$WORK/logs" "$WORK/metrics" "$WORK/monitoring/promdata" \
         "$WORK/monitoring/grafana-data" "$WORK/monitoring/grafana-logs" \
         "$WORK/monitoring/dashboards" "$WORK/monitoring/provisioning/datasources" \
         "$WORK/monitoring/provisioning/dashboards"

log()  { printf '\033[1;36m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
ok()   { printf '\033[1;32m  OK\033[0m   %s\n' "$*"; }
warn() { printf '\033[1;33m  WARN\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m  FAIL\033[0m %s\n' "$*"; exit 1; }

# 注意：source Ascend set_env.sh 会引用未定义变量，脚本内不得使用 `set -u`

# ---------- 运行期路径（step_check_env 里探测后填充）----------
DRAMPOOL_BIN=""       # drampool 可执行文件
P2P_SO=""             # libucm_p2p_transport.so
DRAMSTORE_SO=""       # libdramstore.so
UCM_PKG_DIR=""        # site-packages/ucm 或仓库 ucm/
METRICS_CFG=""        # metrics_configs.yaml 绝对路径

# 在候选列表里挑第一个存在且可执行/可读的文件
pick_file() {  # pick_file <want-exec:1|0> <candidate>...
  local want_exec=$1; shift
  local c
  for c in "$@"; do
    [ -n "$c" ] || continue
    if [ "$want_exec" = 1 ]; then [ -x "$c" ] && { echo "$c"; return 0; }
    else [ -f "$c" ] && { echo "$c"; return 0; }
    fi
  done
  return 1
}

# 探测 UCM 安装布局：drampool / 两个 .so / metrics_configs.yaml
discover_ucm_layout() {
  # python 包目录（wheel 安装 = site-packages/ucm；editable = 仓库/ucm）
  UCM_PKG_DIR=$("$PY" - <<'PYPKG' 2>/dev/null
import os
try:
    import ucm
    print(os.path.dirname(os.path.abspath(ucm.__file__)))
except Exception:
    pass
PYPKG
)
  local pkg_bin=""
  [ -n "$UCM_PKG_DIR" ] && pkg_bin="$(dirname "$UCM_PKG_DIR")/bin"

  # drampool：优先显式指定，其次仓库构建产物，其次 PATH，其次 site-packages/bin
  DRAMPOOL_BIN=$(pick_file 1 \
    "$DRAMPOOL_BIN" \
    "$UCM_ROOT/bin/drampool" \
    "$(command -v drampool 2>/dev/null)" \
    "$pkg_bin/drampool" \
    "$UCM_PKG_DIR/../bin/drampool") || DRAMPOOL_BIN=""

  # 两个 .so：仓库内(editable) 或 site-packages 内(wheel)
  P2P_SO=$(pick_file 0 \
    "$UCM_ROOT/ucm/transport/p2p/libucm_p2p_transport.so" \
    "$UCM_PKG_DIR/transport/p2p/libucm_p2p_transport.so") || P2P_SO=""
  DRAMSTORE_SO=$(pick_file 0 \
    "$UCM_ROOT/ucm/store/dram/libdramstore.so" \
    "$UCM_PKG_DIR/store/dram/libdramstore.so") || DRAMSTORE_SO=""

  # metrics_configs.yaml 不在 wheel 里，只能从仓库源码目录取
  METRICS_CFG=$(pick_file 0 \
    "$METRICS_CONFIG_PATH" \
    "$UCM_ROOT/examples/metrics/metrics_configs.yaml" \
    "$UCM_PKG_DIR/../examples/metrics/metrics_configs.yaml") || METRICS_CFG=""
}

# =============================================================================
step_check_env() {
  log "步骤 1/7  检查环境 (A3)"
  [ -d "$UCM_ROOT" ] || die "仓库不存在: $UCM_ROOT（用 UCM_ROOT=... 覆盖）"
  [ -x "$PY" ]       || die "Python 不存在: $PY（用 PY=... 覆盖）"
  [ -n "$MODEL" ] && [ -d "$MODEL" ] || die "模型权重目录未设置或不存在: '${MODEL:-<空>}'（用 MODEL=/path/to/snapshot 覆盖）"

  # 探测安装布局
  discover_ucm_layout
  [ -n "$DRAMPOOL_BIN" ] || die "找不到 drampool 可执行文件。
     已探测: \$DRAMPOOL_BIN / $UCM_ROOT/bin/drampool / PATH / site-packages/bin/drampool
     若尚未安装，先跑: bash $0 --build   或用 DRAMPOOL_BIN=/abs/path 指定"
  ok "drampool: $DRAMPOOL_BIN"
  [ -n "$DRAMSTORE_SO" ] && ok "libdramstore.so: $DRAMSTORE_SO" \
                         || warn "未找到 libdramstore.so（若 python import ucm 正常可忽略）"
  [ -n "$P2P_SO" ]       && ok "libucm_p2p_transport.so: $P2P_SO" \
                         || warn "未找到 libucm_p2p_transport.so（传输后端探测将跳过）"

  # python 依赖
  "$PY" -c "import vllm,torch_npu,ucm" 2>/dev/null \
    && ok "venv 内 vllm + torch_npu + ucm 均可导入" \
    || die "python 缺依赖（需 vllm / torch_npu / uc-manager）。
       A3 官方镜像应已内置；若用独立 venv 请先 pip install -e . (PLATFORM=ascend-a3)"

  # metrics_configs.yaml（enable_metrics 依赖它暴露完整 ucm:drampool_* 系列）
  [ -n "$METRICS_CFG" ] && ok "metrics_configs.yaml: $METRICS_CFG" \
    || warn "未找到 metrics_configs.yaml，vLLM 将用内置默认指标集（drampool 专项可能不全）"

  # ---- 传输后端可用性探测（早失败，避免起了服务才发现协议没编进去）----
  if [ -n "$P2P_SO" ]; then
    if [ "$TRANSPORT" = "hixl" ]; then
      # hixl 需要 libucm_p2p_transport.so 链接了 cann_hixl
      if ldd "$P2P_SO" 2>/dev/null | grep -qi 'cann_hixl\|hixl'; then
        ok "传输后端 hixl 可用（$P2P_SO 已链接 cann_hixl）"
      else
        warn "ldd 未见 cann_hixl —— 该构建可能没编进 HIXL 后端。
       若启动后传输报 'protocol not installed' 或 503900，请改用 TRANSPORT=acl_ipc。"
      fi
    elif [ "$TRANSPORT" = "acl_ipc" ]; then
      if strings "$P2P_SO" 2>/dev/null | grep -q 'AclIpcTransport\|aclrtIpcMem'; then
        ok "传输后端 acl_ipc 可用（$P2P_SO 含 AclIpcTransport 符号）"
      else
        die "TRANSPORT=acl_ipc 但该构建未编进 ACL IPC 后端（$P2P_SO 无相关符号）。
       请用带 acl_ipc 的构建，或改 TRANSPORT=hixl。"
      fi
    else
      die "TRANSPORT 只能是 hixl 或 acl_ipc，当前='$TRANSPORT'"
    fi
  fi
  warn "传输后端: TRANSPORT=$TRANSPORT$([ "$TRANSPORT" = acl_ipc ] && echo '  (ACL IPC + aclrtMemcpy，单机、不依赖 RoCE)')"
  if [ "$TRANSPORT" = "acl_ipc" ]; then
    warn "acl_ipc 仅支持单机；worker 与 drampool 必须在同一台机器上，且 drampool 只用 device_ids 的第一张卡"
  fi

  # ---- scheduler 设备号：实时从 config.h 读取（不写死）----
  local cur
  cur=$(grep -oE 'SCHEDULER \? [0-9]+' "$UCM_ROOT/ucm/store/dram/cc/config.h" 2>/dev/null | grep -oE '[0-9]+' | head -1)
  if [ -n "$cur" ]; then
    if [ -n "$SCHED_DEV" ] && [ "$SCHED_DEV" != "$cur" ]; then
      die "config.h RuntimeDeviceId() 编译值是 $cur，但 SCHED_DEV=$SCHED_DEV。
       scheduler 设备号是编译进 .so 的常量，无 YAML/env 开关。
       改 ucm/store/dram/cc/config.h 的 RuntimeDeviceId() 后必须重新编译(见 --build)。"
    fi
    SCHED_DEV=$cur
    ok "config.h scheduler 运行时设备 = $SCHED_DEV"
  else
    SCHED_DEV=${SCHED_DEV:-0}
    warn "无法从 config.h 解析 scheduler 设备号，回退 SCHED_DEV=$SCHED_DEV"
  fi

  # ---- NPU 可用性逐卡探测 ----
  # 挂起的卡不一定是被占用；hixl 模式下真因常是 RoCE 链路 DOWN（见下方 RoCE 预检）。
  # 探测有副作用：对挂起的卡反复 timeout 杀进程会留下半初始化的设备上下文，勿循环重试。
  log "  探测可用 NPU（每卡独立进程，避免相互影响）"
  local USABLE=""
  local ndev
  # npu-smi -l 每张卡输出一行 "NPU ID : N"；缺失或为 0 时回退 8
  ndev=$(npu-smi info -l 2>/dev/null | grep -c 'NPU ID')
  case "$ndev" in (''|*[!0-9]*) ndev=0 ;; esac
  [ "$ndev" -ge 1 ] || ndev=8
  for d in $(seq 0 $((ndev-1))); do
    if ASCEND_RT_VISIBLE_DEVICES=$d timeout 25 "$PY" -u -c "
import ctypes
a=ctypes.CDLL('libascendcl.so'); a.aclInit(None)
import sys; sys.exit(0 if a.aclrtSetDevice(0)==0 else 1)
" >/dev/null 2>&1; then
      USABLE="$USABLE $d"; printf '    NPU %s: 可用\n' "$d"
    else
      printf '    NPU %s: 不可用/挂起\n' "$d"
    fi
  done
  ok "可用 NPU:$USABLE"
  warn "探测有副作用：对挂起的卡反复 timeout 杀进程会留下半初始化上下文，勿循环重试"

  for need in "$WORKER_DEV" "$SCHED_DEV" "$POOL_DEV"; do
    case " $USABLE " in *" $need "*) ;; *) die "设备 $need 不可用，请改 WORKER_DEV/SCHED_DEV/POOL_DEV" ;; esac
  done

  if [ "$TRANSPORT" = "hixl" ]; then
    # HIXL 明确拒绝同卡路由(hixl_transport.cpp BuildRouteLocked "same device")。
    if [ "$WORKER_DEV" = "$SCHED_DEV" ] || [ "$SCHED_DEV" = "$POOL_DEV" ] || [ "$WORKER_DEV" = "$POOL_DEV" ]; then
      die "hixl 后端下三个设备号必须互不相同（worker=$WORKER_DEV scheduler=$SCHED_DEV pool=$POOL_DEV）。
       scheduler 设备号($SCHED_DEV)是编译进 config.h 的常量：若它与 worker/pool 冲突，
       需改 ucm/store/dram/cc/config.h 的 RuntimeDeviceId() 后跑 --build 重编，
       或改 WORKER_DEV/POOL_DEV 让三者错开。
       （acl_ipc 后端无此限制。）"
    fi
    ok "设备拓扑合法(hixl): worker=$WORKER_DEV scheduler=$SCHED_DEV drampool=$POOL_DEV"
  else
    # acl_ipc 无同卡拒绝；只提示 pool 设备
    [ "$WORKER_DEV" = "$SCHED_DEV" ] && warn "acl_ipc: worker 与 scheduler 同卡($WORKER_DEV)，acl_ipc 允许但请确认是预期"
    ok "设备拓扑(acl_ipc): worker=$WORKER_DEV scheduler=$SCHED_DEV drampool=$POOL_DEV"
  fi

  # ---- RoCE 链路预检（仅 hixl 决定性；acl_ipc 不依赖 RoCE）----
  # HIXL/HCCL 建通信组依赖 NPU 的 RoCE 链路。链路 DOWN 时：aclrtSetDevice 可能挂起、
  # HIXL 握手能成但传输全部返回 503900/507001、HCCL plog 报 rankSize[0]。
  # 注意 HCCS 健康不代表 RoCE 健康——torch_npu 跨卡拷贝走 HCCS，会在 RoCE 全断时依然成功。
  local hccn=/usr/local/Ascend/driver/tools/hccn_tool
  if [ "$TRANSPORT" = "hixl" ] && [ -x "$hccn" ]; then
    log "  检查 RoCE 链路状态（hixl 传输的必要条件）"
    local up=0 down=0
    for d in $(seq 0 $((ndev-1))); do
      local st
      st=$("$hccn" -i "$d" -link -g 2>&1 | grep -oE 'link status: *[A-Z]+' | grep -oE '[A-Z]+$')
      case "$st" in
        UP)   up=$((up+1)) ;;
        DOWN) down=$((down+1)); printf '    NPU %s: RoCE DOWN\n' "$d" ;;
        *)    printf '    NPU %s: RoCE 状态未知(%s)\n' "$d" "${st:-查询失败}" ;;
      esac
    done
    echo "    RoCE: UP=$up DOWN=$down"
    if [ "$up" -eq 0 ]; then
      die "全部 NPU 的 RoCE 链路都是 DOWN —— hixl 数据面必然失败(503900/507001)。
       这是宿主机/交换机层面的问题，软件层无法绕过，需运维恢复链路。
       排查: hccn_tool -i 0 -link_stat -g   (看 LINK DOWN 的确切时刻)
       判断: hccn_tool -i 0 -link -g 应返回 'link status: UP'
       绕过: TRANSPORT=acl_ipc bash $0 all   (改用不依赖 RoCE 的单机传输后端)"
    elif [ "$up" -lt 2 ]; then
      warn "只有 $up 张卡 RoCE UP，hixl 传输可能不稳定"
    else
      ok "RoCE 链路正常 ($up 张卡 UP)"
    fi
  elif [ "$TRANSPORT" = "hixl" ]; then
    warn "找不到 hccn_tool，跳过 RoCE 预检（hixl 传输失败时请优先检查 RoCE 链路）"
  else
    ok "TRANSPORT=acl_ipc 不依赖 RoCE，跳过链路预检"
  fi

  # ---- 端口占用 ----
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
  log "步骤 2/7  编译 UCM (A3: PLATFORM=ascend-a3)"
  if [ "$BUILD_MODE" = "skip" ]; then
    warn "BUILD_MODE=skip，跳过编译"
    return 0
  fi
  cd "$UCM_ROOT" || die "cd 失败"
  source /usr/local/Ascend/ascend-toolkit/set_env.sh 2>/dev/null
  source /usr/local/Ascend/nnal/atb/set_env.sh 2>/dev/null

  # A3 dev 文档流程：editable 安装，产物(bin/drampool, ucm/store/dram/*.so,
  # ucm/transport/p2p/*.so)落回仓库目录，随后 discover_ucm_layout 会优先命中它们。
  # ENABLE_SPARSE=false 避免 910C 自定义算子对 moe_distribute_base.h 的硬依赖。
  export PLATFORM=ascend-a3
  export ENABLE_SPARSE=${ENABLE_SPARSE:-false}
  export WORKSPACE=${WORKSPACE:-$(dirname "$UCM_ROOT")}
  log "  PLATFORM=$PLATFORM ENABLE_SPARSE=$ENABLE_SPARSE (editable 安装)"

  # 必须先停服务再重装，否则 "Text file busy" 会导致二进制未替换
  discover_ucm_layout
  pkill -9 -f "$WORK/launch_drampool.sh" 2>/dev/null
  [ -n "$DRAMPOOL_BIN" ] && pkill -9 -f "$DRAMPOOL_BIN" 2>/dev/null
  pkill -9 -f 'bin/drampool' 2>/dev/null
  sleep 2

  "$PY" -m pip install -e . --no-build-isolation 2>&1 | tail -15 \
    || die "editable 编译/安装失败。若只想用容器已装好的 wheel，设 BUILD_MODE=skip 并跳过本步。"

  discover_ucm_layout
  [ -n "$DRAMPOOL_BIN" ] || die "编译后仍找不到 drampool，检查上面的构建日志"
  ok "编译并安装完成: $DRAMPOOL_BIN ($(stat -c %y "$DRAMPOOL_BIN" 2>/dev/null | cut -d. -f1))"
  warn "若改过 config.h RuntimeDeviceId()，请重跑 --check 确认 scheduler 设备号已更新"
}

# =============================================================================
step_configs() {
  log "步骤 3/7  生成配置文件"

  # ---- drampool.yaml ----
  # transport.endpoints 是 peer 白名单(twoSidedToOneSided)。DramStore 按角色做端口偏移:
  #   scheduler: 无偏移          -> 9001 / 4502
  #   worker(dev=N): +(N+1)      -> 9002+N / 4503+N   (dev=0 -> 9002 / 4503)
  # 缺任何一条，drampool 都会 "rejected unconfigured control peer"，请求全部超时(-50010)。
  # 注意 transport.hixl.listen_port 即使走 acl_ipc 也必须存在且非零（YAML 校验强制）。
  local wctl=$((9002 + WORKER_DEV)) wtr=$((4503 + WORKER_DEV))
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
    - two_sided: "127.0.0.1:$wctl"  # DramStore worker 控制端(9001+deviceId+1)
      one_sided: "127.0.0.1:$wtr"   # DramStore worker 传输端(4502+deviceId+1)
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
  # DramPoolResourceReporter 只在 scheduler 进程(device_id<0)启动，通过 /dev/shm flock 选主。
  local mcfg="${METRICS_CFG:-$UCM_ROOT/examples/metrics/metrics_configs.yaml}"
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
metrics_config_path: "$mcfg"
EOF
  ok "drampool.yaml + ucm_dramstore.yaml 已生成 (worker 端口 $wctl/$wtr)"

  # ---- 启动器：drampool ----
  cat > "$WORK/launch_drampool.sh" <<EOF
#!/bin/bash
source /usr/local/Ascend/ascend-toolkit/set_env.sh 2>/dev/null
cd "$UCM_ROOT" || exit 1
export UC_LOGGER_LEVEL=\${UC_LOGGER_LEVEL:-info}
export UCM_LOG_LEVEL=\${UCM_LOG_LEVEL:-info}
# 传输后端选择，必须与 launch_vllm.sh 里的值一致
export UCM_P2P_TRANSPORT=$TRANSPORT
exec "$DRAMPOOL_BIN" \\
  --addr 127.0.0.1:9000 \\
  --nics $NIC \\
  --pool-size-gb $POOL_GB \\
  --kvcache-block-sizes $BLOCK_SIZES \\
  --kvcache-block-proportions $BLOCK_PROPS \\
  --config "$WORK/drampool.yaml"
EOF

  # ---- 启动器：vLLM ----
  # 不能设 ASCEND_RT_VISIBLE_DEVICES：scheduler 硬编码绝对设备号(config.h)，
  # 限制可见设备会让 aclrtSetDevice($SCHED_DEV) 返回 507033(DEV_SETUP_ERROR)。
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
# 传输后端选择，必须与 launch_drampool.sh 里的值一致
export UCM_P2P_TRANSPORT=$TRANSPORT
# A3 runbook 要求（仅文档要求，UCM 不强制；设 0 可关）
export ASCEND_A3_ENABLE=$ASCEND_A3_ENABLE
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
  ok "启动器已生成 (UCM_P2P_TRANSPORT=$TRANSPORT, ASCEND_A3_ENABLE=$ASCEND_A3_ENABLE)"
}

# =============================================================================
stop_all() {
  log "停止全部本轮进程"
  # --stop 独立调用时 DRAMPOOL_BIN 尚未探测，这里补一次以便精确匹配
  [ -n "$DRAMPOOL_BIN" ] || discover_ucm_layout
  pkill -f 'vllm.entrypoints.cli.main' 2>/dev/null; sleep 4
  pkill -9 -f 'vllm.entrypoints.cli.main' 2>/dev/null
  pkill -9 -f EngineCore 2>/dev/null; pkill -9 -f VllmWorker 2>/dev/null
  [ -f "$WORK/drampool.pid" ] && kill "$(cat "$WORK/drampool.pid")" 2>/dev/null
  sleep 3
  # 只杀 drampool 二进制与其启动器，避免误伤含 "drampool" 字样的无关进程
  pkill -9 -f "$WORK/launch_drampool.sh" 2>/dev/null
  [ -n "$DRAMPOOL_BIN" ] && pkill -9 -f "$DRAMPOOL_BIN" 2>/dev/null
  pkill -9 -f 'bin/drampool' 2>/dev/null
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
    grep -q "DramPool service ready" "$WORK/logs/drampool.log" 2>/dev/null && break
    grep -qE "init failed|start failed" "$WORK/logs/drampool.log" 2>/dev/null && \
      die "drampool 启动失败:
$(tail -8 "$WORK/logs/drampool.log")"
    kill -0 "$(cat "$WORK/drampool.pid")" 2>/dev/null || {
      die "drampool 进程已退出:
$(tail -8 "$WORK/logs/drampool.log")"; }
    sleep 1
  done
  grep -q "DramPool service ready" "$WORK/logs/drampool.log" \
    && ok "drampool ready (pid $(cat "$WORK/drampool.pid"), device $POOL_DEV)" \
    || die "drampool 40s 内未 ready，看 $WORK/logs/drampool.log"
  # 记录实际选中的传输协议，便于核对两侧一致
  grep -oE "transport service: protocol=[0-9]+" "$WORK/logs/drampool.log" | head -1 | sed 's/^/  /'
  grep -c "BufferManager initialized BufferPool" "$WORK/logs/drampool.log" >/dev/null 2>&1 && \
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
    grep -nE "ERROR|Traceback|507033|aclrtSetDevice|RuntimeError|EngineCore failed|protocol not installed" "$WORK/logs/vllm.log" | tail -20
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
  log "步骤 6/7  启动 Prometheus + Grafana"
  # A3 容器不一定自带监控二进制；缺失时明确报错并给获取方式（不静默跳过）
  [ -n "$PROM_BIN" ] && [ -x "$PROM_BIN" ] || die "PROM_BIN 未设置或不可执行: '${PROM_BIN:-<空>}'
     A3 容器内若无 prometheus，请指定 PROM_BIN=/abs/prometheus（arm64/x86 视机器架构）"
  [ -n "$GRAF_BIN" ] && [ -x "$GRAF_BIN" ] || die "GRAF_BIN 未设置或不可执行: '${GRAF_BIN:-<空>}'
     请指定 GRAF_BIN=/abs/grafana 和 GRAF_HOME=/abs/grafana-home"
  [ -n "$GRAF_HOME" ] && [ -d "$GRAF_HOME" ] || die "GRAF_HOME 未设置或不存在: '${GRAF_HOME:-<空>}'"

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
          instance: "ucm-a3-vllm"
EOF

  pkill -f "$PROM_BIN" 2>/dev/null; sleep 1
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
    uid: prom-a3
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

  pkill -f "$GRAF_BIN" 2>/dev/null; sleep 1
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
  "$PY" - "$WORK/monitoring/dashboards/drampool_e2e.json" "$WORK" <<'PYGEN'
import json, sys
DS = {"type": "prometheus", "uid": "prom-a3"}
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
dash = {"uid":"drampool-e2e-a3",
        "title":"DramPool / DramStore E2E - A3 (910C)",
        "tags":["ucm","drampool","dramstore","a3"],
        "timezone":"browser","schemaVersion":39,"version":1,"refresh":"5s",
        "editable":True,"graphTooltip":1,"time":{"from":"now-30m","to":"now"},"panels":P}
out = sys.argv[1]
work = sys.argv[2]
json.dump(dash, open(out,"w"), indent=1)
json.dump({"dashboard":dash,"overwrite":True}, open(work+"/monitoring/dash_payload.json","w"))
print("  dashboard: %d panels -> %s" % (len(P), out))
PYGEN
}

import_dashboard() {
  local code
  code=$(curl -s -o "$WORK/monitoring/import_resp.json" -w '%{http_code}' -X POST \
    -u admin:admin1394 -H 'Content-Type: application/json' \
    --data-binary @"$WORK/monitoring/dash_payload.json" \
    "http://127.0.0.1:$GRAFANA_PORT/api/dashboards/db" 2>/dev/null)
  [ "$code" = "200" ] && ok "Grafana 面板已导入 (uid=drampool-e2e-a3)" \
                      || warn "面板导入返回 $code: $(head -c 200 "$WORK/monitoring/import_resp.json")"
}

# =============================================================================
step_requests() {
  log "步骤 7/7  发真实请求 + 三层证据校验"

  # 必须用全新内容的 prompt：重复 prompt 会被 GPU 内部 prefix cache 全命中
  # (日志 hit hbm=N / Prefix cache hit rate 上升)，根本不会走外部 DramStore。
  # prompt 需 > --block-size(128) token 才会产生至少一个完整 block。
  WORK="$WORK" "$PY" - <<'PY'
import json, time, random, os
work = os.environ["WORK"]
base=("The unified cache manager offloads KV cache blocks from accelerator memory into a "
      "pooled host DRAM tier so that a later request can restore them instead of recomputing "
      "every transformer layer from the original prompt tokens. ")
tag=f"e2e-{int(time.time())}-{random.randint(1000,9999)}"
for n in ("r1","r2","r3"):
    json.dump({"model":"qwen3-0.6b","prompt":f"[{tag}-{n}] "+base*60,
               "max_tokens":8,"temperature":0},
              open(os.path.join(work, f"req_{n}.json"),"w"))
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
  WORK="$WORK" VLLM_PORT="$VLLM_PORT" PROM_PORT="$PROM_PORT" "$PY" - <<'PY'
import json, os, urllib.request, urllib.parse
work=os.environ["WORK"]; vport=os.environ["VLLM_PORT"]; pport=os.environ["PROM_PORT"]
def get(u):
    try: return urllib.request.urlopen(u, timeout=10).read().decode()
    except Exception: return ""
P=os.path.join(work,"metrics","drampool_metrics.json")
try:
    lines=[l for l in open(P).read().splitlines() if l.strip()]
    d=json.loads(lines[-1])
except Exception as e:
    print("  writer JSON 读取失败:", e); raise SystemExit(1)
c, g = d.get("counters",{}), d.get("gauges",{})
raw = get("http://127.0.0.1:%s/metrics"%vport)
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
        r=json.loads(get("http://127.0.0.1:%s/api/v1/query?query=%s"%(pport,q)))["data"]["result"]
        if r: v3=f(r[0]["value"][1])
    except Exception: pass
    flag="OK" if v1==v2==v3 else ("~" if v3!="n/a" else "?")
    print(f"  {k:38s} {v1:>11s} {v2:>13s} {v3:>11s}  {flag}")
PY

  echo
  log "Grafana 面板查询验证"
  curl -sf --max-time 10 -u admin:admin1394 -G \
    "http://127.0.0.1:$GRAFANA_PORT/api/datasources/proxy/uid/prom-a3/api/v1/query" \
    --data-urlencode 'query={__name__="ucm:drampool_dump_requests_total"}' 2>/dev/null \
    | grep -q '"result":\[{' && ok "Grafana -> Prometheus 查询返回真实数据" \
                             || warn "Grafana 查询无数据"

  echo
  log "数据面状态"
  if [ "$TRANSPORT" = "hixl" ]; then
    local n507
    n507=$(grep -c 507001 "$WORK/logs/drampool.log" 2>/dev/null)
    if [ "${n507:-0}" -gt 0 ]; then
      warn "HIXL 传输失败 $n507 次 (ACL 507001) —— 通常是 RoCE 链路问题，见 --check 的 RoCE 预检"
      warn "指标链路不受影响：counters/gauges 由请求计数与内存分配驱动，已正常上报"
    else
      ok "无 HIXL 传输错误"
    fi
  else
    local nerr
    nerr=$(grep -cE "aclrtMemcpy failed|import key failed|not connected" "$WORK/logs/drampool.log" 2>/dev/null)
    [ "${nerr:-0}" -eq 0 ] && ok "无 acl_ipc 传输错误" \
      || warn "acl_ipc 传输失败 $nerr 次，看 $WORK/logs/drampool.log"
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
  echo "  访问地址 (A3 宿主机 $ip)"
  echo "--------------------------------------------------------------------------"
  echo "  Grafana     http://$ip:$GRAFANA_PORT/d/drampool-e2e-a3"
  echo "              账号 admin / admin1394"
  echo "  Prometheus  http://$ip:$PROM_PORT/targets"
  echo "  vLLM        http://$ip:$VLLM_PORT/metrics"
  echo "--------------------------------------------------------------------------"
  echo "  传输后端    TRANSPORT=$TRANSPORT"
  echo "  drampool    $DRAMPOOL_BIN"
  echo "  日志目录    $WORK/logs/{drampool,vllm,prometheus,grafana}.log"
  echo "  writer JSON $WORK/metrics/drampool_metrics.json"
  echo "  停止服务    bash $0 --stop"
  echo "=========================================================================="
}

# =============================================================================
status_only() {
  discover_ucm_layout
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
  --build)  step_build ;;
  --stop)   stop_all
            [ -n "$PROM_BIN" ] && pkill -f "$PROM_BIN" 2>/dev/null
            [ -n "$GRAF_BIN" ] && pkill -f "$GRAF_BIN" 2>/dev/null
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
# 不可违反的约束（A2 + A3 两轮联调实测得出，违反任意一条链路就断）
#
# 1. [hixl] 三个设备号必须互不相同：vLLM worker / UCM scheduler / drampool。
#    HIXL 明确拒绝同卡路由(hixl_transport.cpp BuildRouteLocked "same device"，
#    仅当两侧都是单实例、同 host、设备号相等时触发)。acl_ipc 后端无此限制。
#
# 2. scheduler 的设备号是编译进 config.h RuntimeDeviceId() 的常量，无 YAML/env 开关
#    （当前源码值是 0）。改它必须重新编译 dramstore。且因为它是绝对设备号，vLLM
#    进程绝不能设 ASCEND_RT_VISIBLE_DEVICES，否则 aclrtSetDevice 返回 507033。
#    本脚本 --check 会实时 grep config.h 得到 SCHED_DEV，不写死。
#
# 3. 启动顺序：drampool 先 ready(日志 "DramPool service ready")，再起 vLLM。
#    注：A2 文档说"drampool 重启后 HIXL 握手不会重做"已过时——当前树 node_actor
#    恢复状态机会重发 Connect 命令，backend 每次都重跑 ExchangeMetadata+Connect。
#    但仍建议：要改 drampool 就重启整条栈，最稳妥。
#
# 4. drampool.yaml 的 transport.endpoints 是 peer 白名单(twoSidedToOneSided)，
#    必须覆盖 DramStore 偏移后的真实端口：scheduler 9001/4502、worker(dev=N)
#    (9002+N)/(4503+N)。缺条目 -> "rejected unconfigured control peer"([W]级易漏看)
#    -> 全部 -50010 超时。本脚本按 WORKER_DEV 自动算端口。
#
# 5. --kvcache-block-sizes 必须包含实际请求的 entry 长度。BufferManager::Allocate
#    用 GetPool(size) 精确匹配(pools_.find)，没有"最小可容纳"回退，配错即
#    "no pool registered for size X" 然后 DUMP 失败。默认值对应 Qwen3-0.6B@128。
#
# 6. enable_cs / enable_hixl_cs 必须 false（两侧一致）。置 true 时 HIXL
#    Initialize 返回 103900(PARAM_INVALID)，drampool 直接起不来。
#
# 7. transport.hixl.listen_port 即使走 acl_ipc 也必须存在且非零——drampool YAML
#    校验无条件要求它(drampool_yaml_config.cc)，缺了直接 InvalidParam。
#
# 8. UCM_P2P_TRANSPORT 两侧进程(drampool + vLLM)必须一致。它被读一次并缓存整个
#    进程生命周期；不一致会让 InstallTransport 注册一个协议、路由却选另一个，
#    所有传输报 "protocol not installed"。取值：hixl / acl_ipc（大小写敏感的
#    固定几种写法，见 transport_manager.cpp）。
#
# 9. Prometheus 保留指标名里的冒号，Grafana/PromQL 必须写 {__name__="ucm:xxx"}，
#    写成 ucm_xxx 查不到数据(静默返回空)。
#
# 10. 必须用全新内容的 prompt 且 > --block-size(128) token。重复 prompt 会被 GPU
#     内部 prefix cache 全命中(hit hbm=N)，不会访问外部 DramStore，counters 保持 0。
#
# 11. 每次重启前清 /dev/shm/ucm_drampool_metrics_*.json|.lock，否则 reporter 的
#     陈旧 state 会触发 snapshot_deltas 缺陷（"Histogram sum changed without
#     samples"，在 _write_state 之前 raise，state 永不前进 -> 无限循环报错）。
#     该缺陷当前树仍未修。
#
# 12. --nics 参数是空壳：只被 ValidateNics() 检查非空，从未传给 HIXL/传输层。
#     改 mlx5_0 / eth0 / lo 都无效，不要在这里浪费时间。
#
# 13. [A3] UCM 是 pip 安装的，没有 A2 的 build-pr1394-new 构建树。drampool /
#     libdramstore.so / libucm_p2p_transport.so 位置随安装方式(editable vs wheel)
#     变化，本脚本 discover_ucm_layout() 自动探测。改了 C++/config.h 后跑 --build
#     (PLATFORM=ascend-a3 的 editable 重装) 才会生效。
#
# 14. [A3] examples/ 不进 wheel，metrics_configs.yaml 从 $UCM_ROOT/examples/metrics/
#     读；找不到时 vLLM 用内置默认指标集，drampool 专项系列可能不全。
#
# 15. [hixl] RoCE 链路必须 UP，否则 HIXL 数据面必然失败(rankSize[0]，503900/507001)。
#     hccn_tool -i <N> -link -g 全 DOWN 时无法软件绕过，需运维恢复或改 acl_ipc。
#     特别注意：HCCS 健康 ≠ RoCE 健康；npu-smi 显示无进程占用也不代表卡可用。
#
# 16. 做 A/B 对照前先验证二进制与源码对应（A2 曾用旧两个月的 harness 跑完整轮，
#     结论全废）。用 strings/nm 查特征标记即可验证。
# =============================================================================
