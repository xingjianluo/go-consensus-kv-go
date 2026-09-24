# go-consensus-kv：本地可运行的 Raft 共识协调服务

用一条可复制状态机（Raft）实现的键值服务，**只依赖 Go 标准库**，无外部服务、
无真实凭据、无付费资源。提供两种本地运行方式：

1. **确定性模拟器**（`internal/sim`）：单进程内多个“节点”，可控时钟、可控
   网络分区 / 崩溃 / 重启 / 日志截断，用固定 seed 保证**可复现**。
2. **真实多进程**（`cmd/server`）：多个本地 TCP 进程 + 磁盘 JSON 持久化，
   支持 `kill -9` 后从数据目录恢复。

二者共用同一套共识核心 `internal/raft`，因此模拟器验证过的安全性质与真实
进程一致。

---

## 1. 目录结构

| 路径 | 职责 |
| --- | --- |
| `internal/raft` | Raft 核心：选举、日志复制、提交、快照安装、单节点成员变更、ReadIndex |
| `internal/raft/memstorage.go` | 模拟器用内存存储（崩溃时由模拟器保留，模拟磁盘） |
| `internal/raft/diskstorage.go` | 真实进程用磁盘存储（JSON + fsync + 原子 rename） |
| `internal/kvsm` | 键值状态机：会话、TTL、按 `(session, reqID)` 的幂等去重、快照序列化 |
| `internal/clock` | 可控时钟与确定性定时器队列 |
| `internal/sim` | 确定性集群：网络、分区、崩溃/重启、事件日志、确定性客户端 |
| `internal/nettransport` | TCP + 长度前缀 JSON 帧；raft 消息与客户端 RPC 共用 |
| `cmd/server` | 一个 raft 节点进程 |
| `cmd/client` | 本地演示 CLI（register/put/get/delete） |
| `cmd/sim` | 一键跑固定的确定性端到端场景并打印关键判定 |

---

## 2. 一致性语义（Consistency Semantics）

- **同一任期最多一个主节点**：term 单调递增；candidate 收到同任期合法 leader
  的 `AppendEntries` 立即退回 follower；更高 term 的消息才会触发降级。
- **多数派提交**：一条日志必须复制到**当前配置的多数派**，且其 term 等于
  leader 当前 term，才推进 `commitIndex`（Raft §5.4.2，避免直接提交旧 term
  条目）。
- **线性一致写**：写请求作为日志条目走完整提交流程，返回 `committed=true`
  时数据已在多数派持久化。
- **线性一致读（ReadIndex）**：leader 收到读请求后，必须
  ① 当前 term 的 no-op 已提交（证明自己未被取代），② 一轮心跳得到多数派
  确认，③ 本地状态机应用位置 ≥ 读屏障，才返回本地读结果。follower 直接以
  `not_leader` + leader 提示拒绝，绝不返回可能过期的数据。
- **已提交数据不回退**：冲突的 follower 后缀在 `AppendEntries` 时被截断；
  任期过期的节点只能作为 follower 追平，不能覆盖已提交前缀。
- **幂等**：每个写请求带 `(session, reqID)`。同一会话相同 `reqID` 只应用
  一次；重试返回 `Duplicate=true` 的缓存结果，日志中只有一条记录。
- **会话与 TTL**：先执行复制型 `register` 获得 session；`keepalive` 显式
  续期；普通写**不**自动续期。会话过期/未知时，写与读都得到**可区分的拒绝
  码**（`session_expired` / `session_unknown`），且不改动数据。

### 客户端结果码（可解释的失败结论）

| Code | 含义与调用方应采取的动作 |
| --- | --- |
| `not_leader` | 该节点不是主；携带 `Leader` 提示，应重定向重试 |
| `minority_no_quorum` | 少数派无法提交（本实现中表现为在途无确认） |
| `timeout_unknown_commit` | **提交状态未知**：可能已提交也可能没有。**不得**当作成功；用**相同 reqID** 重试即可安全判定 |
| `session_expired` / `session_unknown` | 会话过期/不存在，写读均拒绝，数据未改 |
| `stale_term` | 节点任期过期，已退回 follower 追平 |
| `pending_membership_change` | 上一个成员变更未提交完，串行执行期间拒绝新写 |
| `unknown_node` | 成员已在/不在配置中（变更幂等性结论） |
| `unsafe_read_rejected` | 非主节点读被拒，避免脏读 |
| `replayed_result` | 命中去重缓存（`Duplicate=true`） |

**关键原则：超时/连接失败永远不会返回成功。** 调用方只要看到非 OK 结果，
就不能认为已提交；用相同 `reqID` 重试，由服务端去重给出确定结论。

---

## 3. 故障处理规则（带可区分结论）

所有判定都会输出结构化事件 `Event{At, Node, Kind, Term, Role, Index, Detail}`，
中文 `Detail` 直接说明“判定依据”。`cmd/sim -v` 与测试都会打印。

### 3.1 分区
- 分区在网络层丢消息；跨组消息不会延迟送达（愈合后由心跳重发）。
- **少数派分区**：旧 leader 得不到多数派，既不推进提交也不向客户端返回成功，
  在途提案最终得到 `timeout_unknown_commit`。
- **多数派分区**：在随机化选举超时后选出更高 term 的新 leader 继续服务。
- 愈合后：旧 leader 收到更高 term 退回 follower，回滚未提交后缀，按
  `prevLogIndex/prevLogTerm` 一致性检查 + 冲突回退提示（`RejectHint`）补齐。

### 3.2 迟到 / 旧消息（安全忽略且可区分）
- 旧 term 的 `AppendEntries` / 响应 / 快照：拒绝或忽略，输出 `stale_message`。
- **已移除/从未加入节点的 `RequestVote` 被直接丢弃，且不采纳其 term**。
  这是 Raft 成员变更的“旧服务器问题”：否则被删节点会用不断升高的 term
  反复把合法 leader 拉下台。事件中明确记录“安全忽略非成员…不更新本地 term”。
- 旧快照（`lastIndex` 不新于本地）安全忽略并说明原因。

### 3.3 崩溃与重启
- 模拟器：`Crash` 只丢弃易失状态（角色、leader、进度表、定时器），保留
  `MemStorage` 与应用快照；`Restart` 从持久状态重建。
- 真实进程：硬状态（term/vote）、日志、快照落盘（临时文件 + rename +
  目录 fsync）；重新启动不带 `-bootstrap` 即从数据目录恢复。
- 全部节点同时崩溃后重启，已提交前缀在所有节点上一致（见
  `TestCrashAllRestartConverges`）。

### 3.4 快照与日志截断
- 应用位置距上次快照超过阈值（模拟器 `SnapshotEntries`，真实进程 200）时
  自动生成快照并截断日志。
- 落后节点的 `nextIndex ≤ snapshotLastIndex` 时走 `InstallSnapshot`：
  安装快照 → 恢复应用数据 → 以快照配置与位置继续追日志。
- 日志截断后崩溃重启的节点，要么从本地快照恢复，要么从 leader 安装快照，
  最终达到与多数派一致的状态。

---

## 4. 成员变更（加入 / 退出）

- 采用**一次只变一个节点**的单成员变更（Raft 论文的单节点安全变更）：
  新旧配置在一次单节点增减时法定人数必然重叠，因此不需要 joint consensus
  也不会出现双主。
- 变更本身是一条 `EntryConf` 日志，必须多数派提交才生效；提交前旧配置
  继续生效，期间拒绝其它写（`pending_membership_change`），杜绝重叠变更。
- **加入**：新节点先以“未配置”身份启动（不发起选举），leader 主动向其
  补日志/快照；它追平后配置条目在其与旧多数派之间提交生效。
- **退出**：配置提交后，被移除节点若在配置中发现自己已退出，立即停止
  选举与提交并静默；其余节点忽略它的投票请求且不采纳其 term。
- 已提交数据在变更前后都不会回退；变更期间的分区/崩溃组合见
  `TestPartitionDuringAddThenCrash`。

---

## 5. 本地验证方法

### 5.1 一键确定性场景（无需起进程）

```bash
go run ./cmd/sim -seed 42          # 打印 10 步结论
go run ./cmd/sim -seed 42 -v       # 同时打印每个关键事件（节点/任期/角色/位置/依据）
```

更换 `-seed` 会改变随机选举超时从而改变调度；**同一 seed 重复运行输出逐行
一致**（`TestDeterministicReplay` 会比较三次运行的完整事件序列）。

### 5.2 运行全部测试

```bash
go test ./...                 # 单元 + 确定性集成测试
go test -race ./...           # 竞态检测（并发客户端场景）
go test ./internal/sim -v     # 查看每个场景打印的关键事件与判定依据
```

覆盖的组合场景：

| 测试 | 覆盖内容 |
| --- | --- |
| `TestMinorityPartitionCannotCommit` | 少数派不能提交/不返回成功，愈合后多数派值保留 |
| `TestStaleTermLeaderDoesNotOverwrite` | 旧 term  leader 未提交后缀回滚，不覆盖已提交值 |
| `TestCrashLeaderReplay` / `TestCrashAllRestartConverges` | 崩溃/全崩后重启一致性、无重复应用 |
| `TestSnapshotTruncatedRestart` | 日志截断后落后节点经快照恢复一致 |
| `TestIdempotentRetry` | 相同 reqID 只提交一次，重试 `Duplicate` |
| `TestExpiredSessionRejected` | 过期会话写/读得到可区分拒绝 |
| `TestConcurrentClients` | 多 goroutine 客户端，成功即已提交且可读 |
| `TestReadFromNonLeaderRejectedExplainably` | follower 读拒绝并给 leader 提示 |
| `TestAddMember` / `TestRemoveMember` | 加入补数、退出缩容、无双主、数据不回退 |
| `TestPartitionDuringAddThenCrash` | 成员变更 + 分区 + 崩溃/重启组合 |
| `TestLateStaleMessagesSafelyIgnored` | 构造旧 term/非成员消息，安全忽略且可区分 |
| `TestDeterministicReplay` | 同 seed 三次运行事件序列逐行一致 |

### 5.3 真实多进程（TCP + 磁盘）

```bash
go build -o /tmp/rsrv ./cmd/server
go build -o /tmp/rcli ./cmd/client
P="n1=127.0.0.1:9101,n2=127.0.0.1:9102,n3=127.0.0.1:9103"

# 首次启动用 -bootstrap 初始化；数据写入 /tmp/rdata/nX
for i in 1 2 3; do
  /tmp/rsrv -id n$i -listen 127.0.0.1:$((9100+i)) -peers "$P" \
      -datadir /tmp/rdata/n$i -bootstrap n1,n2,n3 &
done

/tmp/rcli register -server 127.0.0.1:9101 -peers "$P" -ttl 300s -reqid s1   # Value 返回会话 id
/tmp/rcli put    -server 127.0.0.1:9101 -peers "$P" -session s1 -key k -value v -reqid p1
/tmp/rcli get    -server 127.0.0.1:9103 -peers "$P" -session s1 -key k -reqid g1   # 经 follower 重定向
kill -9 <n1 pid>          # 硬杀
/tmp/rsrv -id n1 -listen 127.0.0.1:9101 -peers "$P" -datadir /tmp/rdata/n1 &  # 不带 -bootstrap，从磁盘恢复
```

幂等重试：对同一写再次执行并保持相同 `-reqid`，返回 `Duplicate:true`，
不会应用新 value。

---

## 6. 已知边界（Scope / Limitations）

- **单成员串行变更**：一次只能增/删一个投票节点；不支持一次多节点的
  joint consensus，也不支持 learner/non-voting 角色的显式区分（加入中的
  节点以“未配置不竞选”的形式近似 learner）。
- **日志复制为全量 AppendEntries + 整快照 InstallSnapshot**：未实现
  快照分块传输与日志 pipeline 批量优化；适合本地演示与小规模数据。
- **真实进程的持久化是 JSON 文件**：清晰可审计，但不是高性能存储；
  写入采用原子 rename + fsync，满足崩溃一致性但不追求吞吐。
- **网络模型**：模拟器不模拟消息重复（只模拟丢失/分区/延迟），Raft 对
  重复消息本就幂等；真实 TCP 层单连接、无 TLS、无鉴权（仅本地使用）。
- **时钟**：模拟器为单驱动循环的逻辑时钟；真实进程用墙上时钟，选举超时
  含随机抖动。
- **ReadIndex 不落日志**：读的可重试验证依赖会话存活与多数派心跳确认；
  不提供 LeaseRead（租约会依赖时钟上界假设）。
- **去重缓存有界**：每个会话保留最近若干条 reqID 结果（模拟器/状态机中
  `maxCachedPerSession=64`）；超出后更老的 reqID 重试可能再次执行，
  客户端应在会话内复用近期 reqID。日志中的条目本身始终完整。
- 仅用于教学/本地验证，未做生产级的内存上限、压缩并发控制与成员变更期间
  的客户端完整重定向治理。
