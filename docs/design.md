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
- **并发同号重试的等待者管理（service 层）**：
  - 同一 `(session, request)` 的多个调用方挂在同一个 pending 上，提案到达终态
    （提交关联 / 显式拒绝）时一次性向所有等待者投递**同一个**结论。
  - 某个调用方自己的确认窗口超时，**只摘除它自己那一个等待者**；pending 与
    其它等待者保留——后提交成功时其余调用方照常拿到确定结论，不会被一个超时
    者"连坐"成未知。最后一个等待者离开后 pending 才删除，之后的重发才会发新
    提案（raft 与 reply cache 保证仍只生效一次）。
  - raft 对"原条目**已提交后**的重复 propose"会回 `PropCommitted +
    RejectDuplicate` 事件；service 据此从 reply cache 取出原始结论投递给等待者，
    而不是把它当作"处理中"丢弃、让调用方等到超时。若该事件先于本节点应用条目的
    Ready 批次到达（事件是在 raft 锁内同步发出的，而应用在另一条通路上），事件
    会被暂存并在每个 Ready 批次应用后重试，直到能从缓存读到原始结论。
- 会话有空闲 TTL：过期/已关闭会话的读、写都被**显式拒绝**
  （`session-expired`，并带 `no-such-session/closed/idle-ttl` 原因）。
  注意服务层对"本节点 store 还没追上会话条目"会先快速回
  `session-expired/session-unknown-or-closed`：这在新当选 leader 刚追平时是
  **可重试**响应，客户端用同一编号重试即可，不代表会话终态失效。
- **超时不等于成功**：写在限定时间内未拿到多数确认时返回
  `timeout-unknown`，调用方不得据此认为已提交；可用相同
  `(sessionID, requestID)` 重试，由 reply cache 收敛到真实结论。

### 1.5 原子批量写与条件更新
批量请求（`CmdBatch`）是一条普通 raft 日志条目，因此天然继承单键写的
全部保证：多数派提交、旧任期不可覆盖、崩溃重放确定、reply cache 幂等。

- **全有或全无**：状态机先对全部项求值前置条件（phase 1），全部满足后才
  一次性改 map（phase 2）；读路径与提交路径共用同一把锁，任何读都看不到
  只改了一半的中间状态。失败时不写任何键、不推进任何键版本。
- **结构性拒绝**（不读状态、各节点确定一致）返回 `invalid-batch`：
  空批次（`empty-batch`）、空键（`empty-key`）、键过长（`key-too-long`）、
  值过长（`value-too-long`）、批次内重复键（`duplicate-key`，绝不悄悄选
  一个值）、项数超限（`too-many-items`）、总载荷超限
  （`payload-too-large`）、非法前置版本（`bad-expect-version`）。
  阈值见 `MaxBatchItems/MaxKeyLen/MaxValueLen/MaxBatchPayload`。
- **版本规则与前置条件**：
  - 每个键有单调版本；首次成功写入为 1，之后每生效一次（单键 put 或
    批量项）+1。失败/被拒的请求不改版本；版本随快照持久化。
  - `expect_version`：省略=无条件覆盖写；`0`=键**必须不存在**
    （create-if-absent）；`v>0`=键必须存在且当前版本**恰好**为 v。
  - 不满足时整体返回 `precondition-failed`，原因可区分为
    `key-already-exists`（要求不存在但存在）、`key-not-exists`
    （要求某版本但不存在）、`version-mismatch`（存在但版本不符），
    并在结论里给出 `failed_key`、`expect_version`、`actual_version`。
  - 两个客户端并发基于同一旧版本更新时，由于条件在**串行化的日志应用点**
    求值，最多一个成功；失败方拿到上述确定原因。它与领导者切换/超时导致
    的 `timeout-unknown` 是**不同的结论**，调用方不会把"输了竞争"误判为
    "结果未知"。
- **幂等与恢复**：批量的完整首个结论（含提交位置 `applied_at`、每键结果、
  每键生效版本、失败详情）原样存入 reply cache 并随快照保存。用同一个
  `(sessionID, requestID)` 重发时——无论首次是成功、结构性拒绝还是条件
  失败，也无论之后别的写入是否已改动这些键——都返回 `duplicate` +
  **第一次的原始结论**，绝不按新状态重新判断条件，也绝不只应用一部分。
- **与单键能力的兼容**：单键 `put/get` 的报文、结论与 TTL 语义不变，只是
  响应额外带 `version`（读也带当前版本）；版本表是快照中的附加字段，旧
  快照恢复时按现存键补版本 1。
- **分区恢复 / 换主后的同号重试如何收敛**：用同一个
  `(sessionID, requestID)` 重发时，结论恰好是以下两者之一，且二者都唯一确定：
  1. **返回第一次已提交的原始结论**：旧条目在恢复过程中（或任何时刻）已经被
     多数提交——重发命中 reply cache（或 raft 的"已提交去重"快速通道），得到
     `duplicate` 与原始 `applied_at`、每键原始版本、原始失败详情；
  2. **作为一次全新的干净提交生效**：旧条目从未提交（少数派 leader 被换、
     未提交后缀被新任期覆盖）——重发追加为新条目，得到一次 `ok`，相关键恰好
     受这一次影响，版本只前进一次。

  无论重试多少次、中途落到哪个节点，相关键的最终值/版本与提交位置都与这唯一
  结论一致；前置条件**不会**按最新状态重新判断（条件失败的一方在任何重试下都
  拿到同一份确定失败原因，不会和超时后的未知混淆）。
- **什么时候仍然只能当成未知**：只有在"本次调用"层面拿不到终态时——
  - `timeout-unknown`：确认窗口内未得到多数确认，条目可能已提交也可能没有；
    继续用同一编号重试即按上面两种结果收敛；
  - `not-leader`（含 `term-stale`、`not-a-voter`）：当前联系的节点给不了结论，
    按返回的 leader 提示重定向后用同一编号重试。

  这两者都**不会**被当成"已经完成"回给调用方；而一旦条目已提交，任何重试都
  立即得到确定结论，调用方不会一直干等。在途合并的第二次调用会等待原提案的
  终态（提交/拒绝），那个终态本身仍是上述确定结论之一。

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
| `duplicate` | 同请求重复：在途则合并等待原提案终态；已提交则回原始结论 | 不重复应用；两种情况调用方都拿到确定结论 |
| `timeout-unknown` | 超时未确认 | 结果未知，**禁止当作已提交** |
| `invalid-batch` | 批量结构非法（空批次/空键/重复键/超限） | 确定拒绝，无任何键生效；原因细分 |
| `precondition-failed` | 某项期望版本与当前不符 | 确定拒绝，整体不生效；带 `failed_key` 与期望/实际版本 |
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

**重试收敛稳定性的反复验证**（本地连续大量重跑不应出现任何失败）：
```
# 确定性 sim：手动时钟，单轮毫秒级，适合几百次重跑
go test ./internal/sim/ -run 'BatchRetry|RepeatedSameRequest|ConditionalFailureRetry' -count=200
# 真实 3 节点 Service（墙上时钟 + 内存故障注入网络）：换主/恢复 + 并发同号重试
go test ./internal/kv/ -run 'RetryAfter|ConcurrentSameRequest' -count=50
go test -race ./internal/kv/ -run 'RetryAfter|ConcurrentSameRequest' -count=10
```
集成测试用短写超时（`SetWriteTimeout`，默认 2s）驱动，因此分区中每个"未知"
响应只等约 0.1s；客户端循环按"未知/not-leader/leader 尚未追平会话→同号重试，
其它结论→终态"推进，正是真实客户端的收敛姿势。
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
- `TestLinearizableReadBarrier` / `TestReadFailsWithoutQuorum`
- 批量与条件写（`internal/sim/acceptance_batch_test.go`、
  `internal/kv/batch_test.go`、`internal/kv/batch_service_test.go`）：
  - `TestBatchMultiKeyCommitsAndConverges`（多键原子提交并收敛）
  - `TestBatchPreconditionFailureConvergesAllOrNothing`（条件失败全有或全无）
  - `TestBatchDuplicateAfterInterveningWrites`（重复请求返回原始结论）
  - `TestBatchRetryAfterPartitionConverges`（分区/领导切换后同 request 重试收敛）
  - `TestBatchRepeatedSameRequestDuringPartitionNeverCommits`（分区内反复同号
    重试绝不提交；恢复后跨换主重试收敛到唯一结论）
  - `TestBatchConditionalFailureRetryKeepsOriginalDetail`（条件失败的同号重试
    返回原始失败详情与原始 actual 版本，不按新状态重判）
  - `TestBatchLeaderCrashRestartKeepsVerdict`（崩溃重启后结论与版本保留）
  - `TestBatchConcurrentConditionalRace`（并发条件更新恰一个成功，失败可区分）
  - 真实 3 节点 `kv.Service` + 故障注入网络的重试收敛套件
    （`internal/kv/retry_convergence_test.go`，墙上时钟、`-count` 可大量重跑）：
    - `TestRetryAfterPartitionConvergesToOneCommit`（分区未知→恢复后同号重试
      收敛到原始结论或一次干净提交，后续重试恒为同一 index 的 duplicate）
    - `TestRetryAfterLeaderChangeReturnsOriginalVerdict`（条目已提交后换主，
      同号条件批重试返回原始结论，不按新版本重判、不重复生效）
    - `TestRetriedConditionalFailureKeepsOriginalReason`（条件失败重试恒返回
      同一份确定原因，不与 timeout-unknown 混淆）
    - `TestConcurrentSameRequestRetriesAllDecisive`（分区/恢复/换主前后多路
      并发同号重试，每个调用方拿到同一确定结论与同一提交位置，无人拿到未知、
      无人干等）
    - `TestConcurrentSameRequestNoFaultStress`（无故障同任期高并发同号请求，
      专门压"刚提交后的重复提案"快速通道）
  - `TestBatchStructuralValidation`（重复键/空键/超限）/
    `TestBatchCreateIfAbsent` / `TestBatchSnapshotRestorePreservesVersionsAndReplies`
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
go run ./cmd/client -servers $S get  $SID r2 color
# 原子批量（一个 request 编号；@后是期望版本：0=必须不存在，N=必须当前是 vN）
go run ./cmd/client -servers $S batch $SID b1 cfg/a=1 cfg/b=2
go run ./cmd/client -servers $S batch $SID b2 cfg/c=new@0
go run ./cmd/client -servers $S batch $SID b3 cfg/a=99@1
# 若超时/换主，用完全相同的 b1 重发：得到第一次的原始结论（duplicate），不重复生效
# 杀掉一个进程后重放同一条 put（相同 r1）观察 duplicate/同一结论
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
