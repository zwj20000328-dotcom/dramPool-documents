# 昇腾 A2 / PR 1394 DramPool–DramStore–Grafana 端到端联调复盘

| 项 | 值 |
|---|---|
| 复盘日期 | 2026-10-12 |
| 联调窗口 | 2026-10-09 ~ 2026-10-10（请求阶段于 10-10 15:18:03 结束，此后空闲） |
| 验证目标 | PR https://github.com/ModelEngine-Group/unified-cache-management/pull/1394 |
| 代码基线 | HEAD `7ce12bc` "metrics: unify transfer/rtt/lookup histogram buckets to 14 values"（= PR 1394 head） |
| 主机 | 110.138.0.3，昇腾 A2，8× 910B3，host glibc 2.34 |
| 容器 | `z00984767_ascend_test`，network mode = **host**，容器内 glibc 2.38 |
| CANN | 容器内两套：cann-8.5.1（libcann_hixl md5 `8d5354a1…`）与 cann-9.1.0-beta.1（md5 `c182e7c1…`）；`ascend-toolkit/latest → /usr/local/Ascend/cann → cann-9.1.0-beta.1` |
| 最终结论 | **指标链路：通**（15/15 官方面板出实时数据）。**数据面：未通**（HIXL 传输 0 字节，840/840 dump 在提交阶段失败，错误码 507001） |

---

## 一、环境与拓扑

### 1.1 路径（注意：哪些只在容器内）

| 路径 | 位置 | 说明 |
|---|---|---|
| `/root/z00984767/e2e1009/unified-cache-management` | **仅容器内**（overlay fs） | 本次联调真正使用的仓库，HEAD `7ce12bc`。宿主机看不到 |
| `/root/z00984767/e2e1009/run/` | 仅容器内 | 配置/日志/指标/启动脚本/RUNBOOK |
| `/root/z00984767/e2e1009/hixl-ga/lib64/` | 仅容器内 | GA 9.1.0 `libcann_hixl.so` 副本（md5 `c38c1dee…`），源自 `/home/lx/test/hixl/build/` |
| `/root/z00984767/unified-cache-management` | 宿主机 | 分支 `z00984767-0821`；981 modified + 6 untracked，其中 13 个文件含真实内容改动（drampool metrics 工作），**并非纯行尾翻转**；本次误操作已还原 |
| `/root/z00984767/0917/unified-cache-management` | 宿主机 | 分支 `z00984767-0917`；旧 patch 以 commit `ab6f56d` 存在；被旧 e2e 脚本使用，与本次运行无关 |

### 1.2 端口与进程（最终运行态）

| 端口 | 归属 | 说明 |
|---|---|---|
| 7850 | vLLM APIServer (pid 65622) | `/metrics` 暴露 `ucm:*` 系列 |
| 9000 | drampool (pid 65502) | KV 控制面 |
| 9001 | **vLLM EngineCore 进程内** (pid 66027) | DramStore scheduler 控制口（不是独立守护进程） |
| 9002 | **vLLM Worker 进程内** (pid 66191) | DramStore worker 控制口 |
| 9090 / 3000 | z009-prom / z009-grafana（host docker） | 本次监控栈 |
| 9110 / 3110 | pr1394-prom / pr1394-grafana | **遗留栈**，target `:8300` 已 down，为冻结历史 |

> 重要：9001/9002 由 vLLM 的 EngineCore/Worker **进程内**绑定。"重启 scheduler/worker" 等于重启 vLLM，不存在独立守护进程可单独重启。

### 1.3 设备与网络

| 项 | 值 |
|---|---|
| drampool 传输设备 | `transport.device_ids=[2]`（drampool.yaml） |
| vLLM worker 设备 | device 0（`get_current_device_id()`，逻辑序号） |
| DramStore scheduler 设备 | device 0（**硬编码**，`config.h:65-68`） |
| RoCE link | UP: 0/2/4/6；DOWN: 1/3/5/7（`hccn_tool -i N -link -g`；`-roce_status` 不是合法子命令） |
| dev0 net_health | `Receive timeout` —— **netdetect 目标配错**：`/etc/hccn.conf` 中 `address_0=192.168.190.170` 但 `netdetect_0=192.168.100.2`（不同 /24）。dev2 两者一致故 `Success`。dev0 RoCE 实际可用（实测 128 次 `HcclCommPrepare success`：drampool 64 + EngineCore 32 + Worker 32） |
| RoCE 网段 | 192.168.190.x（`/etc/hccn.conf`） |
| HCCP 设备侧服务网段 | 192.168.2.x（dev0=.199，dev2=.197），与 RoCE 网段无关；默认固定端口 **16666**（hcomm `rs.h:218 RS_SOCK_PORT_DEF`） |

### 1.4 HIXL 库版本（本次事故的轴心）

| 库 | md5 | 日期 | 认 `comm_resource_config.listen_port`？ |
|---|---|---|---|
| beta（默认链接） | `c182e7c1…` | May 27 | **否**（无 `ParseCommResourceListenPortConfig`）→ 静默忽略 |
| GA 9.1.0 | `c38c1dee…` | Jun 12 | **是**（`ParseCommResourceListenPortConfig` → `LocalCommResGenerator::Generate(..., device_port)`；范围 1..65535） |

- 宿主机 `/usr/local/Ascend/cann-9.1.0-beta.1/cann/aarch64-linux/lib64/` 下另有一份 GA 内容（md5 相同，mtime Jun 6），**该路径只在宿主机存在、容器内没有**。联调中曾误 grep 它并据此"纠正"子代理，属误判（见 §3.5-c）。
- GA 库中并不存在字面名为 `SetDevicePort` 的导出符号（被内联/改名），事实链应表述为 `ParseCommResourceListenPortConfig → LocalCommResGenerator::Generate(…, std::optional<unsigned>)`。
- **必须用 `LD_PRELOAD` 而非 `LD_LIBRARY_PATH`** 强制 GA 库：`libucm_p2p_transport.so` 带绝对 `DT_RPATH=/usr/local/Ascend/ascend-toolkit/latest/lib64:…`，而 DT_RPATH 优先级高于 LD_LIBRARY_PATH。

### 1.5 Patch 状态（最终）

| Patch | 文件 | 状态 |
|---|---|---|
| 旧 `dramstore_a2_hixl_host_sync.patch`（QueueTransferSync 路线，4 个 hixl 文件） | hixl_transport.{h,cpp}, hixl_instance.{h,cpp} | 曾应用，**已完整还原到 HEAD**（含 CRLF 行尾） |
| 新 `a2_hixl_host_sync_mock.patch`（mock 路线，2 个文件） | hixl_instance.{h,cpp} | **已应用**；`git diff --stat` = 2 files, +77/−41 |
| 重编产物 | `ucm/transport/p2p/libucm_p2p_transport.so` md5 `69aa204ca1…` | 即 in-tree 构建产物（与 `build-e2e1009/` 内一致），三个活进程均映射它；**不存在单独的 /usr/local 安装副本** |

> 复核 patch 是否编入：`strings` 只能看到日志格式串 `A2-sync TransferAsync(`、`TransferSync(..., chunk=`；`async_results_`/`next_fake_request_`/`kMaxSyncSegmentBytes` 是 C++ 标识符不是字符串字面量，`strings | grep` 会假阴性 —— 应 grep **源码**或 `nm -DC | grep QueueTransferSync`（应为 0）。

---

## 二、运行步骤（冷启动，可复跑）

### 2.0 本地 SSH 隧道（看 Grafana 用）

```bash
ssh -N -L 3000:127.0.0.1:3000 -L 9090:127.0.0.1:9090 root@110.138.0.3
# 浏览器：
#   http://localhost:3000/d/ucm-dram-metrics        官方 15 面板（本次导入，数据源 prom-a2）
#   http://localhost:3000/d/drampool-e2e-z009       setup 脚本生成的 12 面板
#   凭据 admin / admin1394
```

若出现 `Warning: remote port forwarding failed for listen port 15721`：来自本地 `~/.ssh/config` 的 `RemoteForward`，与 `-L` 隧道无关，可忽略（宿主机上该端口已被另一个 sshd 会话占用）。

### 2.1 容器内 —— 清场

```bash
docker exec -it z00984767_ascend_test bash
cd /root/z00984767/e2e1009/run

# 坑1：pkill 模式必须写成 'dramp[o]ol'，字面 'drampool --addr' 会匹配到自己的 shell 并把它杀掉
pkill -9 -f 'dramp[o]ol --addr'; pkill -9 -f 'vllm[.]entrypoints'; pkill -9 -f 'EngineCor[e]'
sleep 5

# 坑2：孤儿 VLLM::Worker 的 comm 被改名、cmdline 不含 vllm.entrypoints，上述 pkill 匹配不到；
#      它会占住 dev0 约 44GB 显存，导致下次 vLLM 启动报
#      ValueError: Free memory on device (17.63/60.96 GiB) ... less than desired GPU memory utilization
for p in $(ps -eo pid,stat,comm | awk '$2!~/Z/ && $3~/^VLLM/{print $1}'); do kill -9 $p 2>/dev/null; done
sleep 5
npu-smi info | sed -n '/NPU.*Chip.*Process id/,/NPU *1/p'   # dev0 应为 No running processes

# 坑3：reporter 选主/状态文件必须清，否则触发已知的 snapshot_deltas 永久卡死
rm -f /dev/shm/ucm_drampool_metrics_*
mkdir -p logs/prev && mv logs/*.log logs/prev/ 2>/dev/null
rm -f metrics/drampool_metrics.json
```

### 2.2 容器内 —— 起 drampool（device 2，必须先于 vLLM）

```bash
cd /root/z00984767/e2e1009/run
setsid bash -c './launch_drampool.sh > logs/drampool.log 2>&1' </dev/null &
for i in $(seq 1 30); do grep -q "service ready" logs/drampool.log && break; sleep 1; done
grep "service ready" logs/drampool.log     # 成功标志

# 自检：GA 库 + mock patch 均已加载
P=$(pgrep -f 'bin/dramp[o]ol'|head -1)
grep -o '/[^ ]*lib\(cann_hixl\|ucm_p2p_transport\)\.so' /proc/$P/maps|sort -u|xargs md5sum
#   期望 c38c1dee…（GA hixl）与 69aa204ca1…（mock patch transport）
tr '\0' '\n' < /proc/$P/environ | grep ^LD_PRELOAD    # 必须存在
```

### 2.3 容器内 —— 起 vLLM

```bash
cd /root/z00984767/e2e1009/run
setsid bash -c './launch_vllm.sh > logs/vllm.log 2>&1' </dev/null &
for i in $(seq 1 40); do curl -sf --max-time 5 http://127.0.0.1:7850/health >/dev/null && break; sleep 6; done
curl -sf http://127.0.0.1:7850/health >/dev/null && echo "vLLM OK" || tail -30 logs/vllm.log
```

> 坑4：**不能设 `ASCEND_RT_VISIBLE_DEVICES`**。scheduler 硬编码逻辑 device 0、worker 取逻辑当前设备，两者会一起平移仍同卡；且该变量会触发 `aclrtSetDevice` 507033。launch_vllm.sh 已 `unset`。
> 坑5：launch 脚本必须带 `PYTHONPATH=/root/z00984767/e2e1009/unified-cache-management`，否则 venv1394 的 editable 安装会 import 到旧克隆。

### 2.4 容器内 —— 验证连接层与 patch

```bash
sleep 20
grep -c "DramStore node connected"      logs/vllm.log     # 初始为 2；随恢复风暴增长（本次最终 32）
grep -c "DramStore node connect failed" logs/vllm.log     # 期望 0（beta 期为 2538）
grep -c "HIXL connection failed"        logs/drampool.log # 期望 0（beta 期为 2534）

# 决定性证据：同卡两引擎绑到不同 HCCP 端口（GA 库生效的标志）
grep -h "Listen on ip" /root/ascend/log/run/plog/plog-*.log | grep -oE "port\[[0-9]+\] success, devPhyId\[[0-9]+\]" | sort -u
#   期望 36666(dev0) 36667(dev0) 26666(dev2)，全部 isAutoPort[0]；beta 期则是两个引擎抢 16666
```

### 2.5 宿主机 —— 监控栈

```bash
chmod 777 /root/z009-monitoring/promdata   # Prometheus 以 nobody 运行；root 755 会 Exited(2)
bash /root/z009-monitoring-setup.sh        # 期望 OK Prometheus :9090 / OK Grafana :3000 / MONITORING_READY
```

### 2.6 容器内 —— 发真实请求 + 三层校验

```bash
cd /root/z00984767/e2e1009/run
bash verify_requests.sh    # 3 个全新长 prompt（>128 token，避免命中 GPU prefix cache）
```

判读注意：
- `verify_requests.sh:79` 的 `host-memory queued-as-sync (patch path hit)` 检查 **恒为 0，是假阴性**（grep 的是旧 patch 的 `UC_DEBUG` 串，且 `UC_LOGGER_LEVEL=info` 屏蔽所有 DEBUG）。不要用它判断 patch 是否生效。
- 判断 patch 生效用：`grep -c "A2-sync TransferAsync" logs/drampool.log`（mock patch 独有串，UC_ERROR 级）。
- 三层对比表中 `writerJSON` 与 `vLLM/metrics` **不相等是已知数据丢失**（见 §3 问题 12），不是抓取延迟。

### 2.7 判定标准

| 层 | 通过标志 | 本次结果 |
|---|---|---|
| 连接层 | `node connect failed`=0；vnic 端口 36666/36667/26666 | **通过** |
| 指标链路 | 官方 15 面板出实时数据；三层（JSON→:7850→Prometheus）同向 | **通过**（数值有丢失，见问题 12） |
| 数据面 | `transfer times>0`；`load_requests_total>0`；`hit external>0` | **未通过**（0 字节；840/840 submit 失败；507001） |

---

## 三、问题记录（时间线）

### 问题 1：z009-prom 容器 Exited(2)
- 现象：`bash /root/z009-monitoring-setup.sh` 报 `FAIL Prometheus`；容器退出码 2。
- 根因：`promdata` 目录 root:root 755，而 Prometheus 以 nobody 运行，打开 `/prometheus/queries.active` 权限拒绝后 panic。
- 证据：`docker logs z009-prom` → `Error opening query log file ... permission denied` + `panic: Unable to create mmap-ed active query log`。
- 修复：`chmod 777 /root/z009-monitoring/promdata` 后重启容器。状态：**已修复**。

### 问题 2：patch 目标仓库搞错
- 现象：用户给出的路径 `/root/z00984767/e2e1009/unified-cache-management` 在宿主机不存在。
- 根因：该仓库只在容器 overlay fs 内；宿主机同名目录是另一个 checkout。
- 过程：误将旧 patch 应用到宿主机 `/root/z00984767/unified-cache-management`，随后完整还原（内容 + CRLF 行尾均与 HEAD 一致，patch 标记 0 命中）。
- 状态：**已纠正**。教训：动手前先确认路径在哪个命名空间可见。

### 问题 3：CRLF 行尾导致 `git apply` 失败
- 现象：`git apply --check` 四个 hixl 文件全部 `patch does not apply`。
- 根因：0821 宿主机仓库整体被转成 CRLF（981 modified + 6 untracked；其中 13 个文件含真实内容改动，**不是**纯行尾翻转；仅 4 个 hixl 文件是纯 CRLF 翻转）。
- 修复：将 4 个目标文件规整为 LF 后 patch 干净应用（该仓库后续未再使用；最终工作在容器内仓库完成）。
- 状态：**已解决**。

### 问题 4：HIXL 连接 100% 失败（503900）—— 本次联调的主故障
- 现象：drampool 日志每 ~8s `HIXL connection failed peer=127.0.0.1:9001 ... remote_engine=127.0.0.1:47257 returned 503900`；vLLM 侧 `DramStore node connect failed ... endpoint=127.0.0.1:9000 status=-1`（全部来自 EngineCore）。beta 期真实总数：**2534** 条连接失败 / **2538** 条 node connect failed（503900 字符串出现 7602 次 = 每次失败在 3 个调用点各记一条）。
- 根因（设备层）：scheduler 与 worker 两个 DramStore HIXL 引擎同在物理 device 0；设备侧 HCCP 对同卡只认固定端点 `192.168.2.199:16666`（`RS_SOCK_PORT_DEF`）。worker 先绑成功，scheduler 永久 EADDRINUSE（errno 98，每 8s 重试）。drampool 连 scheduler 引擎时，HCCP 批连接落到 worker 的守护进程，其无匹配 engine tag → 5s 超时 → `RaSocketBatchAbort` → hixl `FAILED=503900`（`hixl_types.h:45`）。
- 根因（库层）：UCM 本已按 `portOffset = physicalDevice*2 + (worker?1:0)` 算出 scheduler 36666 / worker 36667（`config.cc:181-191`）并经 `GlobalResourceConfig={"comm_resource_config.listen_port":N}` 下发（`transport_manager_backend.cc:50-54`、`drampool_server.cc:256-258`），但 **beta 库不含 `ParseCommResourceListenPortConfig`，静默忽略该选项** → 两引擎退回 16666 → 必撞。
- 修复：`LD_PRELOAD` GA 9.1.0 库（launch_drampool.sh / launch_vllm.sh 各加一行 export，含原因注释）。
- 验证：vnic `Listen on ip[192.168.2.199], port[36666] success` 与 `port[36667] success`、drampool `port[26666]`，全部 `isAutoPort[0]`；连接失败 2534→**0**；node connect failed 2538→**0**。
- 状态：**已修复**。
- 附带发现：三个进程 plog 均有 `[HCCL_ENV] HCCL_HOST_SOCKET_PORT_RANGE is not set. Multi-process will not be supported!`（`env_config.cc:292/296`）—— HCCL 明确声明不支持同设备多进程拓扑，即本故障的官方预警。

### 问题 5：过程中的误判与自我纠正（单列，供复盘）

| # | 错误结论 | 为何错 | 正确结论 |
|---|---|---|---|
| a | `local_engine=127.0.0.1`（无端口）是端口丢失 bug | drampool 角色为 `HixlRole::Client`，`hixl_transport.cpp` 对 Client 强制 `port=0`，注释明确"Client 不开监听口" | 设计如此，非 bug |
| b | dev0 `net_health: Receive timeout` = dev0 RoCE 坏 | netdetect 目标 `192.168.100.2` 与真实 RoCE IP `192.168.190.170` 不同 /24，是 `/etc/hccn.conf` 配置错；dev0 实测 32 次 `HcclCommPrepare success` | dev0 RoCE 可用；该告警是配置假象 |
| c | "9.1.0-beta.1 认 `comm_resource_config.listen_port`"（用于纠正子代理） | grep 的是宿主机独有路径 `.../cann-9.1.0-beta.1/cann/...`（GA 内容）；容器内真正链接的 beta 库不含该串 | beta 不认、GA 认；子代理原结论正确 |
| d | mock patch 后 `transfer_failures_total` 168→0 是"关键突破" | submit 失败走 `task_worker.cc:202-204` 的 `kSubmitFailuresTotal` 分支并 return，到不了 `completion_poller.cc:111-125` 的 `ReportDataTransferMetrics` | 失败只是换了计数器；本次最终 submit_failures=840、transfer_failures=0，168→0 无意义 |
| e | 用 `operation failed: TransferSync` 计数=0 推断旧 patch 路径未触发 | 移植版 `ExecuteTransferSync` 删光了日志，该字符串在源码树中 0 命中 | 判据无效；应看 `A2-sync TransferAsync`（新 patch）或反汇编调用图 |
| f | "worker 侧 40 次 HcclCommPrepare success" | 统计窗口不全 | 实测 128 次（drampool 64 + EngineCore 32 + Worker 32） |
| g | beta 期失败数 522/379 | 是中途快照 | 归档 `logs/beta-run/` 真实总数 2534/2538 |

### 问题 6：孤儿 VLLM::Worker 占显存导致 vLLM 启动失败
- 现象：`ValueError: Free memory on device (17.63/60.96 GiB) on startup is less than desired GPU memory utilization (0.7, 42.67 GiB)`。
- 根因：上一轮 EngineCore 已死但 Worker 存活（ppid=1，状态 `Rl`），占 dev0 44060MB；清理命令的 pkill 模式匹配不到它。
- 修复：按 comm `VLLM::Worker` / PID 精确 kill；已写入运行步骤 §2.1。
- 状态：**已修复**（最终运行无孤儿，dev0 HBM 47695/65536 为正常分配）。

### 问题 7：数据面 507001 —— **未解决**
- 现象：mock patch 生效（日志出现 `A2-sync TransferAsync(...) returned 507001`），但所有传输失败；三个进程 HIXL 通道 `transfer times:0, total size:0 kBytes`（drampool 510 条统计行、scheduler/worker 各 254 条，**无一条非零**）。
- 关键证据：
  - 分块前提被证伪：`chunk=32768 of segment_bytes=262144` 与 `chunk=12 of segment_bytes=12`（host→host flag-buffer 响应）**报同一个 507001**。
  - 设备侧前兆：plog-65502 中 `SetWatchDogDevStatus: There is errInfo of devId=2, tsId=0` 比主机可见的首个 507001 **早 22ms**，随后 `TaskExceptionHandler ... hccl task exception callback`、`StreamAbort: Abort stream_id=45`。即故障源于 **dev2（drampool 传输卡）的 NPU Task Scheduler**，507001 = `ACL_ERROR_RT_TS_ERROR`（`rt_error_codes.h:85`，头文件原文拼写 "internel"）只是主机侧翻译。
  - drampool plog **零条 [ERROR]**（10653 INFO / 39 WARNING）—— 设备侧故障只记 INFO，grep ERROR 会漏。
  - `npu-smi` 8 卡 Health 均 OK，非持久硬件故障标志。
  - RUNBOOK 假设 #2 的猜测（"507001 因 drampool 落在 RoCE DOWN 卡"）**被本次运行证伪**：drampool 在 dev2，RoCE UP 且 netdetect 匹配，仍 100% 507001。
  - 每次 `HcclCommPrepare` 都伴随 `RaTlvInit: ra_tlv_init unsuccessful, ret(-524)`（drampool 64 次 + 两 store 各 32 次），栈容忍但异常。
- 伴生现象：**节点恢复风暴**。30 次 `request timeout triggered node recovery`（EngineCore 15 + Worker 15）+ 30 次 fence completed，epoch 单调 1→16；每 epoch = 完整 Disconnect + `HcclOneSidedCommDestroy` + re-Listen + 重连。触发条件：`dump/load_timeout_ms=3000`、`lookup_timeout_ms=1000`（ucm_dramstore.yaml:16-18）vs 批处理耗时数秒。系统处于**稳定失败循环**而非一次性失败。
- 错误码 `-50010`（vLLM 侧 `RuntimeError: -50010`、`task_manager.cc:351`）**在仓库中无任何定义**。
- 状态：**未解决**。候选方向见 §五。

### 问题 8：Grafana 12 面板 "No data" + 粉色错误图标
- 现象：除单 target 的 Metadata Entries 面板外全部报错；数据本身完好（target 从未 down、样本连续）。
- 根因：`z009-monitoring-setup.sh` 生成器把每个 panel 的所有 target 都写 `refId:"A"`；Grafana 11 对同请求重复 refId 直接拒绝：`HTTP 400 {"messageId":"query.duplicateRefId"}`。单 target 面板无重复故正常 —— 与截图形态完全吻合。
- 修复：磁盘 JSON 重排 refId（A/B/C/D）+ 生成器 `p()` 内自动分配；provisioning `updateIntervalSeconds:10` 自动重载（version 1→2）。复验 12 面板全部出数。
- 状态：**已修复**。

### 问题 9：dashboard 指标名笔误
- 现象：panel 11 的 `external_ucm_hit` 序列恒空。
- 根因：引用 `ucm:ucm_hit_blocks_total`，仓库与 Prometheus 中均不存在；真实名为 `ucm:ucm_hit_tokens_total`（tokens 非 blocks）。
- 修复：磁盘 JSON 与生成器同步改名；导入的官方板修复后该序列首次有数据。
- 状态：**已修复**。

### 问题 10：看错 dashboard（与他人展示不一致）
- 现象：用户看到 `PR1394 > UCM DramStore & DramPool Metrics`（stat 大数字、heatmap、Operation 下拉），与 z009 的 12 面板板不同。
- 根因：那是**遗留栈** pr1394-grafana(:3110) + pr1394-prom(:9110)，唯一 target `:8300` 已 down，展示的是旧 run 的冻结历史。
- 修复：将仓库官方 `examples/metrics/grafana_dram.json`（uid `ucm-dram-metrics`，15 面板）经 `/api/dashboards/import` 导入 z009 Grafana(:3000)；官方文件**无 `__inputs` 元数据**，import 的 inputs 参数被忽略，需手工把模板变量 `DS_PROMETHEUS` 绑定到 `prom-a2`；删除联调中产生的派生副本 `ucm-dram-metrics-z009`。复验 15/15 面板出实时数据。
- 状态：**已修复**。

### 问题 11：ssh 隧道警告 `remote port forwarding failed for listen port 15721`
- 根因：本地 ssh config 的 `RemoteForward` 与宿主机上既有 sshd 会话占用的 15721 冲突；与 `-L 3000/-L 9090` 无关。
- 状态：**无需处理**（已解释）。

### 问题 12：指标管道存在数据丢失 —— **未解决（已记录）**
- 现象：`drampool_metrics.json` 与 vLLM `:7850` 转发值（即 Prometheus 值）系统性不等：dump_requests 840 vs 728（差 112）、submit_failures 840 vs 728、dump_failed_entries 15960 vs 13832（=112×19）、response_failures 855 vs 742。
- 根因方向：drampool 指标只经 python 侧 reader 转发进 vLLM `/metrics`；Prometheus **只有 1 个 target**（job=vllm, :7850），没有 drampool 直采目标，故丢失发生在转发环节（reader 采样/增量逻辑），非抓取延迟。
- 影响：Grafana 上所有 drampool 计数**低于真实值**；做容量/失败率结论时须以 JSON 为准。
- 状态：**未解决**，列为代码/管道不足（§四.3）。

### 问题 13：UCM 日志限流使"数日志行数"严重低估失败数 —— 记录在案
- 机制：`logger.h:70` 的 `UC_ERROR` 走 `LogRateLimit`（非 `UC_LOG_UNLIMITED`）；`spdlog_logger.h:49,53-55` 默认 60s 窗口、每调用点最多 3 条；计数位宽仅 2 bit（`kRateLimitCountBits=2`），**上限硬编码为 3，环境变量无法调高**（`spdlog_logger.cc:266-273` 用 `std::min(val, kRateLimitCountMask)` 钳制）。
- 实证：JSON `submit_failures_total=840`，而 drampool.log 仅 15 条 `Dump SubmitAsync failed`（5 批 × 3，恰为限流上限）；失败 request_id 呈连续三元组（1,2,3 / 169,170,171 / …）即限流指纹。
- 可用开关（RUNBOOK 未记载）：`UCM_LOG_RATE_LIMIT_ENABLE=false`、`UCM_LOG_RATE_LIMIT_WINDOW_MS`、`UCM_LOG_RATE_LIMIT_MAX_LOGS`（后者实际被钳到 ≤3）。
- 状态：**已记录**；排障时禁止用日志行数估计失败规模。

---

## 四、代码目前存在的不足

### 4.1 UCM 生产代码

| # | 位置 | 机制 | 影响 | 建议 |
|---|---|---|---|---|
| 1 | `ucm/store/dram/cc/config.h:63-68` | `GetRole()` 以 `deviceId<0` 判 scheduler；`RuntimeDeviceId()` 对 scheduler **硬编码返回 0**（注释自认） | 任何 TP=1/单卡部署必使 scheduler 与 worker 的 HIXL 引擎同物理卡；一旦 HIXL 库忽略 per-engine 设备端口（beta 库）即触发 HCCP 固定端口冲突（问题 4） | 需上游改：允许配置 scheduler 设备，或 scheduler 不创建 HIXL 引擎（它不持有设备内存，只需控制面） |
| 2 | `ucm/integration/vllm/ucm_connector.py:1390-1395` | `device_id = -1 if SCHEDULER else get_current_device_id()`（逻辑序号） | 与 #1 同构；`ASCEND_RT_VISIBLE_DEVICES` 无法分离两者（一起平移），且会触发 507033 | 同 #1 |
| 3 | `ucm/store/dram/cc/transport_manager_backend.cc:49-54` | `HixlInitAttrs::Instance instance{-1, deviceId, {}}` 端口硬编码 −1；listen port 仅塞进 `GlobalResourceConfig` JSON | `hixl_listen_port` 在 beta 库下是**死配置**；端口退化为 `PickAvailablePort` 随机口 | 将 `options_.hixlDeviceListenPort` 直接写入 `instance.port`（配置可解，需重编） |
| 4 | `ucm/store/dram/cc/drampool/drampool_server.cc:249-258` | 同 #3 的死配置模式；且生效端口只在 `UC_DEBUG`(:249) 打印 | info 级别下无法观测实际 listen port | 同 #3；日志升 `UC_INFO` 或 `UC_INFO_UNLIMITED` |
| 5 | `ucm/transport/p2p/src/protocols/hixl/hixl_transport.cpp:43-73` | `PickAvailablePort` TOCTOU：bind 0 口 → getsockname 读回 → **close()** → 返回未持有的端口 | 端口可被抢占；本次未爆但属隐患 | 持有 fd 直至 HIXL 绑定，或改用 SO_REUSEADDR+立即绑定 |
| 6 | `ucm/store/dram/cc/drampool/task_worker.cc:202-204, 307` + `completion_poller.cc:111-125` | submit 失败只加 `kSubmitFailuresTotal` 并 return，**不计入 `kTransferFailuresTotal`**（:122） | 运维以 `transfer_failures_total` 判数据面健康会完全漏掉整类失败（本次 840 vs 0） | 统一终态计数口径，或文档明确两计数器语义 |
| 7 | `ucm/transport/p2p/CMakeLists.txt:159` | 仅设 `INSTALL_RPATH "$ORIGIN"`，未设 BUILD_RPATH | 构建树产物带绝对 DT_RPATH（ascend-toolkit/latest/lib64），**压制 LD_LIBRARY_PATH 覆盖**，迫使本次改用 LD_PRELOAD | 补 `BUILD_RPATH "$ORIGIN"` 或文档说明 |
| 8 | `ucm/integration/vllm/metrics.py`（reader/转发） | drampool JSON → vLLM `/metrics` 转发存在样本丢失（840→728，差 112） | Prometheus/Grafana 数值系统性偏低；Prometheus 仅 1 个 target，无 drampool 直采兜底 | 查 reader 增量逻辑；或为 drampool 增加独立 scrape target |
| 9 | 全局 | 错误码 `-50010` 在仓库中无定义、无文档 | 排障需反查 | 建错误码表 |
| 10 | `ucm/shared/.../spdlog_logger.{h,cc}`（:49,53-55,:266-273,:36-37） | 日志限流默认开、上限硬编码 3 条/60s/调用点且不可调高 | 日志行数与真实失败数差约两个数量级，极易误判规模 | 上限位宽放宽；RUNBOOK 记载开关 |

### 4.2 A2 mock patch（`a2_hixl_host_sync_mock.patch`，test-only）

| # | 位置 | 机制 | 影响 | 建议 |
|---|---|---|---|---|
| 1 | `hixl_instance.h:95` + `hixl_instance.cpp:330`（写）/ `:340-341`（读） | `async_results_` 为**无锁 `std::map`**，写方（TransferAsync 调用线程=TaskWorker）与读方（GetTransferStatus 调用线程=CompletionPoller）不同线程；类内 `mutex_`(:86) 只护 tasks_/stopping_/initialized_ | 数据竞争（UB）：并发下可崩溃或读撕裂；且**从不清理** → 无界增长 | 加锁（或改 `std::atomic` 值 + 分片 map）；完成即 erase |
| 2 | `hixl_instance.cpp:295-333` | 分块前提错误：≤32KiB 并不能避免 507001（12 字节 host→host 同样失败） | patch 的核心假设不成立；分块只增加调用次数 | 待 507001 根因明确后重写或废弃 |
| 3 | `hixl_instance.cpp` mock `TransferAsync` 经 `Run()` 入队 | 单 instance worker 线程串行执行；chunked 同步传输期间**阻塞同实例的 RegisterMemory/Connect 等一切操作**（队头阻塞） | 传输慢时放大超时、加剧恢复风暴 | 独立传输队列/线程，或限制单批 chunk 数 |
| 4 | 成功路径无日志 | 传输成功与否只能靠 HIXL plog 的 `transfer times` 统计行推断 | 可观测性差 | 补 `UC_INFO`（注意限流，见 §4.1-10） |

### 4.3 测试 / 脚本 / 监控

| # | 位置 | 问题 | 状态 |
|---|---|---|---|
| 1 | `run/verify_requests.sh:79,81` | patch 命中检查 grep 旧 patch 的 `UC_DEBUG` 串，info 级别下恒 0 → 永久假阴性 | 未修（记录） |
| 2 | `run/launch_*.sh` | 已含 LD_PRELOAD 与原因注释；但无启动后自检（库 md5 / vnic 端口） | 建议补 |
| 3 | `z009-monitoring-setup.sh` | 曾生成重复 refId（Grafana 11 拒绝）与错误指标名 | **本次已修**（生成器 `p()` 自动分配 refId；`ucm_hit_tokens_total`） |
| 4 | `examples/metrics/grafana_dram.json` | 无 `__inputs` 元数据 → import 无法自动绑定数据源变量 | 未修（记录；本次手工绑 `DS_PROMETHEUS=prom-a2`） |
| 5 | Prometheus 配置 | 仅 1 个 target（vllm :7850）；drampool 无直采，转发丢失无兜底 | 未修（见 §4.1-8） |

### 4.4 文档 / RUNBOOK

| # | 位置 | 问题 | 状态 |
|---|---|---|---|
| 1 | `RUNBOOK.md:28`（假设 #2） | "507001 根因极可能是 drampool 落在 RoCE DOWN 卡" —— 被本次运行证伪（dev2 RoCE UP + netdetect 匹配仍 507001） | 需改写 |
| 2 | `RUNBOOK.md:29-30`（假设 #3） | "scheduler 与 worker 同卡安全，因二者永不互为 HIXL peer" —— UCM 层成立、**设备/HCCP 层不成立**（同卡必撞固定端口，除非库认 listen_port） | 需改写 |
| 3 | RUNBOOK 清场段 | pkill 模式会杀自身 shell；未覆盖孤儿 `VLLM::Worker` | 本文 §2.1 已给正确写法 |
| 4 | RUNBOOK 排错表 | 未记载日志限流开关、未记载 `-50010`、未记载 `hccn_tool -link -g` 正确用法（`-roce_status` 非法） | 需补 |
| 5 | 旧 patch 注释 | 声称 "HIXL 8.5.1"，实际链接 9.1.0-beta.1；版本差异恰是问题 4 的根因 | 需更正 |

---

## 五、未决问题与下一步

**总边界**：指标链路已通（官方 15 面板 + 12 面板均出实时数据；连接层 0 失败）；数据面未通（0 字节、840/840 submit 失败、507001、恢复风暴 epoch 1→16）。以下按性价比排序：

1. **先关掉恢复风暴再谈传输**：把 `ucm_dramstore.yaml` 的 `dump/load_timeout_ms` 从 3000 提到 ≥15000（mock 的 chunked TransferSync 自带 10s 内部超时），观察 507001 是否变为可复现的单次失败而非循环。成本：改配置重启。
2. **设 HCCL 多进程端口范围**：三个进程分别 export `HCCL_HOST_SOCKET_PORT_RANGE` / `HCCL_NPU_SOCKET_PORT_RANGE`（不相交区间）。HCCL 已明确警告未设置时不支持多进程 —— 这可能是 507001 的同源诱因。成本：改 launch 脚本重启。
3. **换 HIXL 构建对照**：容器内另有 cann-8.5.1（md5 `8d5354a1…`）与 `/home/nt/hixl`（1.0.0 系）。用 `strings | grep -c ParseCommResourceListenPortConfig` 与 `nm -DC` 核对 UCM 所需 11 个符号后逐个试。注意 8.5.1 的 host-async 限制正是旧 patch 的出发点。
4. **实测 dev2 的 TS/RoCE 健康**：修 `/etc/hccn.conf` 的 `netdetect_0`（192.168.100.2 → 192.168.190.170）后复测 `net_health`；对 dev2 跑 `hccn_tool -i 2 -eth_test` / `-fec_err` / `-link_stat`；关注 `RaTlvInit ret(-524)` 是否随驱动/固件版本变化。设备侧 watchdog（`SetWatchDogDevStatus devId=2`）提示故障在 NPU TS，必要时提驱动侧工单并附 plog 行号。
5. **试 `enable_hixl_cs: true`**：会切换到新 `hixl::HixlEngine` 路径，其选项白名单**不含** `GlobalResourceConfig`（含则报 "Invalid option"），故须先删除两处 option 写入（`transport_manager_backend.cc:50-54`、`drampool_server.cc:256-258`）再试。风险最高，放最后。
6. **修 mock patch 的 `async_results_` 锁与清理**（§4.2-1）后再做任何数据面结论，排除 UB 干扰。
7. **查指标转发丢失**（§4.1-8）：对齐 JSON 与 `:7850` 的差值 112 的来源；必要时给 drampool 加独立 scrape target 作兜底。

---

## 附录 A：关键证据命令（可直接复跑）

```bash
# 容器内
P=$(pgrep -f 'bin/dramp[o]ol'|head -1)
grep -o '/[^ ]*lib\(cann_hixl\|ucm_p2p_transport\)\.so' /proc/$P/maps|sort -u|xargs md5sum   # 库与产物
tr '\0' '\n' < /proc/$P/environ | grep ^LD_PRELOAD                                            # GA 覆盖
grep -h "Listen on ip" /root/ascend/log/run/plog/plog-*.log | grep -oE "port\[[0-9]+\] success, devPhyId\[[0-9]+\]" | sort -u
grep -oE "transfer times:[0-9]+, total size:[0-9]+ kBytes" $(ls -t /root/ascend/log/run/plog/plog-$P*.log|head -1)|sort|uniq -c
grep -oE "A2-sync TransferAsync\([^)]*\) returned [-0-9]+" logs/drampool.log | sort | uniq -c
grep -c "request timeout triggered node recovery" logs/vllm.log        # 恢复风暴
for i in 0 2; do /usr/local/Ascend/driver/tools/hccn_tool -i $i -link -g; /usr/local/Ascend/driver/tools/hccn_tool -i $i -net_health -g; done
# 宿主机
curl -s 'http://127.0.0.1:9090/api/v1/targets' | python3 -m json.tool | grep -E 'url|health'
curl -s -u admin:admin1394 'http://127.0.0.1:3000/api/dashboards/uid/ucm-dram-metrics' | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["meta"]["version"], len(d["dashboard"]["panels"]))'
```

## 附录 B：dashboard 指标对照（z009 12 面板需要的 32 个）

- Prometheus 实有 30/32。
- 缺 `ucm:drampool_resource_log_read_errors_total`：仅 HELP/TYPE、无样本（值恒 0 被 exporter 跳过），无害。
- 缺 `ucm:ucm_hit_blocks_total`：名字不存在（笔误），已改为 `ucm:ucm_hit_tokens_total`。
- 官方 15 面板复验：15/15 出数（heatmap 各 910 点；p95 各 7 序列；panel 15 的 E 序列为空属正常）。

## 附录 C：本次改动清单与备份

| 改动 | 位置 | 备份 |
|---|---|---|
| launch 脚本加 `LD_PRELOAD` GA 库（含原因注释） | 容器 `run/launch_drampool.sh`、`run/launch_vllm.sh` | `*.bak`（初版 LD_LIBRARY_PATH 版本已被清理，最终版仅 LD_PRELOAD 一行 export） |
| GA 库副本 | 容器 `/root/z00984767/e2e1009/hixl-ga/lib64/libcann_hixl.so` | 源 `/home/lx/test/hixl/build/...` |
| mock patch 应用（2 文件）+ 重编 transport | 容器仓库 `ucm/transport/p2p/src/protocols/hixl/hixl_instance.{h,cpp}`；`ucm/transport/p2p/libucm_p2p_transport.so` | 旧 patch 产物 `/tmp/libucm_p2p_transport.so.oldpatch.bak`；patch 原文 `/tmp/a2_hixl_host_sync_mock.patch` |
| 12 面板 JSON refId/指标名修复 | 宿主机 `/root/z009-monitoring/grafana/dashboards/drampool_e2e.json` | `/tmp/drampool_e2e.json.refidfix.bak` |
| 生成器修复（refId 自动分配、指标名） | 宿主机 `/root/z009-monitoring-setup.sh` | 无（git 未跟踪；改动为 2 处 Edit） |
| 官方 dashboard 导入 + `DS_PROMETHEUS→prom-a2` 绑定 | z009 Grafana uid `ucm-dram-metrics`（version 2） | 官方源文件在仓库 `examples/metrics/grafana_dram.json`（未改） |
| 派生副本删除 | z009 Grafana uid `ucm-dram-metrics-z009` 已删 | — |
| 宿主机 0821 仓库误操作还原 | `/root/z00984767/unified-cache-management` 4 个 hixl 文件 | 还原前备份 `/tmp/bak_{h.h,inst.cpp,inst.h,tr.cpp}` |
| promdata 权限 | 宿主机 `/root/z009-monitoring/promdata` → 777 | — |

**生产代码零修改**：所有改动限于容器内隔离 checkout 的 test-only patch、run 目录脚本、监控配置与库覆盖；未推送、未并入任何生产提交。
