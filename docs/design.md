# 本地共识协调服务 — 设计与一致性说明

一个可在本机运行的、基于 Raft 的复制型 KV 协调服务。目标是用多个本地进程和
**可控故障注入**模拟真实集群，使节点在崩溃、网络分区、重启与成员变更后的状态
结论一致且**可解释**。只依赖 Go 标准库，无外部服务、无凭据、无付费资源。

- 共识核心：`internal/raft`（单节点状态机，存储/传输无关）
- 应用状态机（会话、幂等、快照）：`internal/kv`
- 确定性模拟（假时钟、可注入故障的网络、集群驱动器）：`internal/sim`
- 本地真实多进程传输：`internal/transport`（loopback TCP，长度前缀 JSON）
- 可执行程序：`cmd/server`、`cmd/client`

---

## 1. 一致性语义（Safety）

### 1.1 领导选举与任期（term）
- 任一时钟周期（term）内**最多只有一个 leader**：候选人必须得到当前配置的
  严格多数票（`Majorize(n)=n/2+1`）才能成为 leader。
- 每个节点在转入更高任期时**先持久化 term/vote 再行动**；收到更高任期的
  RPC 会立即退回 follower（`RejectTermStale`）。
- 候选人在**同一任期**收到来自合法 leader 的 AppendEntries/快照时会退回
  follower，避免同任期内两个领导并存。
- 选举超时带有**按节点身份 + 选举次数哈希的抖动**（确定性），既避免活锁又
  保证测试可复现。

### 1.2 日志复制与提交
- 日志按连续 index 复制；follower 通过 `prevLogIndex/prevLogTerm` 校验，
  冲突时截断本地冲突后缀后再追加（绝不会在分歧之上拼接）。
- 提交只需严格多数确认；提交点只进不退。
- **少数派分区无法提交**：被隔离的 leader 拿不到多数 ack，`maybeCommit` 不
  推进，也不会向客户端返回成功。
- **旧任期条目不得直接提交**（Raft §8）：leader 当选后先提交一条本任期
  no-op；只有“多数复制到的、且属于当前任期”的 index 才能推进提交点。这保证
  任期过期的节点无法覆盖已提交数据。
- 落后节点的补全是**可解释的**：每条提交都有 index/term；follower 冲突回退
  时返回 `RejectLogOlder` 及回退提示 index；需要快照时走 InstallSnapshot。

### 1.3 读（线性一致）
- 读走 **ReadIndex**：leader 必须在当前任期收到多数心跳确认（领导权栅栏）
  后，才在不小于该 index 的状态上应答读。
- 拿不到多数（例如被隔离）时读**不会**返回，避免返回可能已过期的数据。
- 单节点集群的栅栏由自身立即满足。

### 1.4 幂等与客户端语义
- 客户端先建立**会话（session）**，每个写请求带会话内唯一 `requestID`。
- 状态机对 `(sessionID, requestID)` 保存**首个结论（reply cache）**：
  - 同一请求重复提交（含崩溃后重放日志）只会应用一次，重复得到
    `VerdictDuplicate` 并返回**原始结论与原始 index**。
  - 同一进程内并发重复请求共享同一个在途提案（原子 join-or-register）。
- 会话有空闲 TTL：过期/已关闭会话的读、写都被**显式拒绝**
  （`session-expired`，并带 `no-such-session/closed/idle-ttl` 原因）。
- **超时不等于成功**：写在限定时间内未拿到多数确认时返回
  `timeout-unknown`，调用方不得据此认为已提交；可用相同
  `(sessionID, requestID)` 重试，由 reply cache 收敛到真实结论。

### 1.5 多键原子批量写与条件写（v2 新增）

批量写是**一条** raft 日志条目（`CmdBatch`），携带多个更新项；它与单键
`CmdPut` 走完全相同的提案、提交、reply cache 与恢复路径，不改变任何已有
单键/会话语义。

**原子性（全有或全无）**
- 整个批量在状态机的**同一个应用步骤、同一把锁**内完成“校验 → 条件判定 →
  写入”，因此任何读都不可能看到只改了一半的状态；本节点应用成功也意味着
  该日志在多数派已提交。
- 任一更新项的前置条件不满足时，整批拒绝、一个键都不改；结论里逐项给出
  结果，其余项标记为 `not-applied`。

**版本规则（防覆盖 / CAS 的基础）**
- 每个键带单调递增的整数版本：首次写入后版本为 `1`，此后每次被提交的写
  （单键 `put` 或批量项）命中该键都恰好 `+1`；键不存在记为版本 `0`。
- `get` 的结论与批量失败时的逐项结果都会回传观察到的版本，调用方据此构造
  下一次条件写。
- 每个更新项可声明前置条件，三种形式：
  - 不声明（`check_version=false`）：无条件覆盖；
  - `check_version=true, expected_version=0`：**键必须尚不存在**（首次创建
    /防覆盖）；
  - `check_version=true, expected_version=N(N≥1)`：键必须存在且当前版本
    恰为 `N`（乐观锁 CAS）。
- 条件在**提交时刻**对最新已提交状态判定。两个客户端基于同一旧版本并发
  条件更新时，由于状态机按日志顺序串行应用，**最多一个成功**；失败方得到
  确定的 `precondition-failed`，并在 `reason` 中指明是哪个键
  （`version-mismatch:<key>` 或 `must-not-exist-but-exists:<key>`）及观察到
  的当前版本——它与“超时后结果未知”是**两种不同结论**，不可混用。

**形状/大小校验（确定性拒绝，绝不静默挑选一个值）**
| reason | 含义 |
|---|---|
| `batch-empty` | 批量不含任何更新项 |
| `batch-too-many-items` | 更新项数超过 `MaxBatchItems`（256） |
| `batch-duplicate-key` | 同一批量内出现重复键 |
| `batch-empty-key` | 存在空键 |
| `key-too-long` | 单键长度超过 `MaxKeyLength`（64 KiB） |
| `value-too-large` | 单值超过 `MaxValueSize`（1 MiB） |
| `batch-too-large` | 键+值总载荷超过 `MaxBatchBytes`（4 MiB） |
| `bad-expected-version` | 期望版本为负数 |

形状校验与节点状态无关、每个副本结论一致；服务层在提案前用同一套规则快速
拒绝，状态机在应用时再次执行（权威路径，也覆盖直接重放日志的副本）。

**幂等与恢复**
- 批量同样以 `(sessionID, requestID)` 缓存**首个完整结论**：提交位置
  （index）、整体 verdict、每个键的逐项结果与生效后版本。
- 即使首次已提交而调用方没收到响应、之后别的写入又改了这些键，用相同
  requestID 重试也返回**第一次的原始结论**（verdict 为 `duplicate`，但
  `items/index` 是首次内容），**不会按新状态重新判定条件，也不会只应用
  其中一部分**。
- 拒绝（形状错误或前置条件失败）也是已提交的确定性结论，同样被缓存与
  重放；前置条件失败的原始结论不会因后来状态“变得满足”而翻案。
- reply cache 与键版本都包含在应用快照内，压缩、快照恢复、崩溃重启后依然
  能回答重复请求；旧格式（无版本字节串）快照可恢复为版本 1 的条目。
- 超时或领导者切换期间，调用方只能把结果当作**未知**，并用同一 requestID
  安全重试，直到拿到 `ok / duplicate / rejected / precondition-failed /
  session-expired / not-leader` 之一的确定结论。

**与已有能力的兼容范围**
- 单键 `put/get`、会话打开/关闭与 TTL、线性一致读、成员变更的协议与结论
  保持不变；`put/get` 结论额外携带 `version` 字段（纯增量，旧字段语义
  不变）。
- 批量与单键写共享同一版本空间与同一 reply cache：单键写会推进版本，批量
  条件写会看到它，反之亦然。

**HTTP 接口**（`POST /kv/batch`，管理端口）：
```
请求: {"session":"s-...","request":"b42","items":[
        {"key":"a","value":"1","check_version":true,"expected_version":0},
        {"key":"b","value":"2"}]}
成功: {"verdict":"ok","index":7,"items":[
        {"key":"a","verdict":"ok","version":1},
        {"key":"b","verdict":"ok","version":1}]}
条件失败: HTTP 409 {"verdict":"precondition-failed",
        "reason":"version-mismatch:a","items":[
        {"key":"a","verdict":"precondition-failed","version":3},
        {"key":"b","verdict":"not-applied","reason":"not-applied-precondition-failed"}]}
形状错误: HTTP 409 {"verdict":"rejected","reason":"batch-duplicate-key",...}
超时:   HTTP 504 {"verdict":"timeout-unknown", ...}  # 结果未知，同 requestID 重试
```
状态码约定：`ok/duplicate` 为 200，形状/条件拒绝为 409，`timeout-unknown`
为 504，`not-leader` 为 307（跟随 `leader` 字段或换节点重试）。

---

## 2. 故障处理规则（以及如何区分）

所有拒绝/丢弃都带机器可读原因（`raft.RejectReason`），便于断言与排障：

| 原因 | 触发条件 | 结论 |
|---|---|---|
| `not-leader` | 向非 leader 提案/读 | 拒绝，并给出已知 leader |
| `term-stale` | 收到旧任期消息 | 安全忽略；对方会带着更高 term 退回 |
| `log-conflict` | prevLog index/term 不符或对端更旧 | 拒绝并给出回退提示 index |
| `config-change-in-flight` | 已有未提交的成员变更 | 拒绝重叠变更 |
| `node-already-member` / `node-not-member` | 增/删目标非法 | 拒绝 |
| `cannot-remove-leader` | 试图删除当前 leader | 拒绝（应先转移/删别人） |
| `not-a-voter` | 非成员发起/回应，或已删除成员的迟到消息 | 安全忽略，不更新进度、不影响提交 |
| `dropped-partition` | 注入的分区规则命中（投递时判定） | 消息未送达 |
| `dropped-node-down` | 目标已崩溃（投递时判定） | 消息未送达 |
| `duplicate` | 同请求在途合并 | 不重复应用，等待原提案结论 |
| `precondition-failed` | 批量项的期望版本不满足（提交时刻判定） | 整批确定拒绝，键名为 `version-mismatch:<key>` / `must-not-exist-but-exists:<key>`，附观察版本 |
| `rejected (batch-*)` | 批量形状/大小不合法 | 整批确定拒绝，不应用任何项 |
| `timeout-unknown` | 超时未确认 | 结果未知，**禁止当作已提交** |
| `closing` | 节点关闭中 | 拒绝 |

关键不变量：
- 故障在**投递时刻**判定，因此“消息在飞行中被分区/宕机”能被真实模拟。
- 已删除成员的迟到响应到达 leader 时只产生 `not-a-voter` 丢弃记录，不修改
  任何 match 进度，也不移动提交点。
- InstallSnapshot 是幂等的；快照丢失（节点当时宕机）时 leader 会在后续心跳
  **重发**，follower 对更旧的快照给出可区分的拒绝。

---

## 3. 快照与日志截断

- 应用应用到某 index 且活跃日志长度超过阈值时，raft 发出
  `EvtSnapshotRequired(index)`；应用导出状态字节后调用 `SnapshotBuilt`，
  raft 截断日志、安装快照（含当时配置）。
- follower 落后到“leader 已把它缺的日志压缩掉”时，收到 InstallSnapshot：
  - 存储层用 `InstallSnapshot` **整体替换**日志（允许快照 index 领先于本地
    lastIndex），冲突后缀被丢弃；
  - 应用先 `Restore` 快照状态，再继续应用其后的条目。
- **截断后重启**：`FileStorage` 用 fsync 的 WAL（append/hard/compact/install
  四类帧）+ 快照重放；崩溃留下的**半帧**在再次打开时被安全截断，恢复到
  一致状态。

---

## 4. 成员变更与恢复

- 仅支持**一次一个节点**的增/删（单成员变更）。旧配置与新配置的多数派必然
  重叠，因此不需要联合共识（joint consensus）也不会出现双主或提交回退。
- 变更写成一条 `EntryConf`：
  - 提出即按新配置复制/计票，**提交后**才算生效（`pendingConf` 在提交前
    阻止重叠变更）。
  - 加节点：新节点先以“无选民”身份运行（不竞选、不投票），日志/快照追平后
    随配置提交成为选民。
  - 删节点：被删节点在应用到不含自己的配置后退回被动 follower，不再提供
    服务；其持久化数据保留供检查，由管理员决定是否清理。
- 不允许删除当前 leader；也不允许把集群删到无法形成多数。

恢复流程：
1. 崩溃进程以**相同参数**重启，从 `-data` 重放 WAL + 快照恢复 hard state、
   日志与配置。
2. 节点以 follower 身份联系 leader；缺日志则增量补，缺太多则收快照。
3. 若任期落后，先因更高 term 退回，再接受当前 leader 的复制。

---

## 5. 本地验证

### 5.1 确定性模拟测试（推荐，最快）
```
go test ./...
go test -race ./...
go test ./internal/sim/ -run TestPartitionCrashMembershipRestartCombined -v
```
模拟使用**手动推进的时钟**与按 `(到期时间, 入队序号)` 排序的确定性调度；相同
输入重复运行得到相同的 leader 序列、任期与最终 index。测试日志会打印关键
事件、任期、日志位置与判定依据（见各 `acceptance_*_test.go` 中的 `t.Logf`
与 `dumpKeyEvents`）。

覆盖的组合场景：
- `TestMinorityPartitionCannotCommit` / `TestMinorityWriteRejectedNotSuccess`
- `TestCrashRestartFollowerCatchup` / `TestCrashLeaderStaleSuffixTruncated`
- `TestSnapshotInstallAndConverge`
- `TestAddThenRemoveMember` / `TestRejectInvalidConfChanges`
- `TestLateMessageFromRemovedMemberIgnored` /
  `TestOldTermMessageCannotRollBackTerm`
- `TestIdempotentDuplicateWriteAppliedOnce` /
  `TestExpiredSessionRejectedExplicitly` / `TestProposalOnFollowerRejected`
- 批量与条件写（本轮新增，确定性、可重复）：
  - `TestBatchAtomicAllOrNothing`（多键成功与条件失败的全有或全无）
  - `TestBatchStructuralRejectionCommitted`（重复键/空批等形状拒绝）
  - `TestBatchConcurrentConditionalRace`（并发 CAS 恰一个成功、失败方可区分）
  - `TestBatchDuplicateAfterLeaderChange`（换 leader 后重试返回原始结论）
  - `TestBatchRetryAfterPartitionConverges`（分区后同 requestID 重试收敛）
  - `TestBatchVerdictSurvivesSnapshot`（版本与逐项结论跨快照保留）
  - 状态机/服务级用例见 `internal/kv/batch_test.go`
- `TestLinearizableReadBarrier` / `TestReadFailsWithoutQuorum`
- `TestPartitionCrashMembershipRestartCombined`（分区+崩溃+重启+成员变更）
- 存储：`TestFileStorageRestartReplays`、
  `TestFileStorageSnapshotAcrossRestart`、`TestFileStorageTornTailFrame`

### 5.2 真实多进程（loopback TCP）
开三个终端：
```
go run ./cmd/server -id n1 -addr 127.0.0.1:9101 -data ./tmp/n1 -bootstrap \
  -peers n1=127.0.0.1:9101,n2=127.0.0.1:9102,n3=127.0.0.1:9103
go run ./cmd/server -id n2 -addr 127.0.0.1:9102 -data ./tmp/n2 \
  -peers n1=127.0.0.1:9101,n2=127.0.0.1:9102,n3=127.0.0.1:9103
go run ./cmd/server -id n3 -addr 127.0.0.1:9103 -data ./tmp/n3 \
  -peers n1=127.0.0.1:9101,n2=127.0.0.1:9102,n3=127.0.0.1:9103
```
每个进程在 raft 端口 +1000 上暴露管理 HTTP（如 `127.0.0.1:10101`）：
```
S=127.0.0.1:10101,127.0.0.1:10102,127.0.0.1:10103
go run ./cmd/client -servers $S status
SID=$(go run ./cmd/client -servers $S session-open | sed -n 's/.*"session": "\([^"]*\)".*/\1/p')
go run ./cmd/client -servers $S put  $SID r1 color blue
go run ./cmd/client -servers $S get  $SID r2 color     # 结论里带 "version"
# 原子批量：key=value[@期望版本]，@0 表示键必须不存在；整批成功或整批不动
go run ./cmd/client -servers $S batch $SID b1 color=blue@0 shape=round@0
go run ./cmd/client -servers $S get   $SID b1g shape   # 读到 version=1
# 两个客户端都基于 version=1 做 CAS，最多一个成功；失败方是
# precondition-failed/version-mismatch:<key>，不是 timeout-unknown
go run ./cmd/client -servers $S batch $SID b2 shape=square@1
# 杀掉一个进程后重放同一条 batch（相同 b1）观察 duplicate + 原始 items/index
# kill -9 后用相同命令重启，观察从 WAL/快照恢复
go run ./cmd/client -servers $S member-add n4 127.0.0.1:9104
go run ./cmd/client -servers $S member-remove n3
```
服务端日志会打印 `role/term/leader`、提交 index、丢弃原因与配置变更事件。

---

## 6. 已知边界

- **单成员变更**：不支持一次增删多个节点、也未实现联合共识；重叠变更被
  `config-change-in-flight` 拒绝。
- **不支持删除当前 leader**（返回 `cannot-remove-leader`）；要删 leader 请
  先删其它节点或先关停它。
- **被删节点不会自动发现自己被删**：若它一直收不到任何含新配置的消息，可能
  周期性以候选人身份空转（拿不到任何在任成员的票，故绝不可能成为 leader，
  也不会影响提交）。生产中通常配合下线流程。
- **未实现**：日志重定向的流水线限流/批量大快照分片、租约读（LeaseRead）、
  PreVote、领导权转移（消息类型已预留 `MsgTimeoutNow`）、WAL 垃圾回收与
  快照文件分离（当前快照内嵌在 WAL 帧中）。
- 传输面向**本机可信 loopback**：无 TLS、无鉴权、无访问控制。
- 客户端需要自行持久化/复用 `(sessionID, requestID)` 才能享受跨重试幂等；
  `timeout-unknown` 后必须用同一 requestID 重试。
- 真实进程路径用墙上时钟（非确定性）；**可复现性由 sim 测试保证**。
