# go-consensus-kv

一个**仅依赖 Go 标准库**、可在本机运行的共识协调服务：用多个本地进程与可控
故障注入模拟 Raft 集群，覆盖崩溃、网络分区、重启、快照与成员变更，并给出
一致且可解释的结论。

- 共识、日志复制、选举、快照、成员变更：`internal/raft`
- 会话 / 幂等写 / 线性一致读 / 应用快照：`internal/kv`
- 确定性时钟、可注入分区与宕机的网络、集群驱动器：`internal/sim`
- loopback TCP 传输：`internal/transport`
- 进程与命令行：`cmd/server`、`cmd/client`

## 快速开始

```
go test ./...           # 确定性模拟测试（秒级，可重复）
go test -race ./...     # 含竞态检测
```

跑真实多进程集群与客户端见 [`docs/design.md`](docs/design.md#5-本地验证)。

## 结论约定

- 写成功 ⇒ 已被多数提交；写返回 `timeout-unknown` ⇒ **结果未知，不代表成功**，
  须用相同 `(session, request)` 重试。
- 同一 `(session, request)` 恰好应用一次；重复得到 `duplicate` 与原始结论。
- 少数派不能提交、不能返回成功；旧任期节点不能覆盖已提交数据。
- 每条拒绝/丢弃都有可区分原因（`not-leader / term-stale / log-conflict /
  not-a-voter / dropped-partition / dropped-node-down ...`）。

## 批量与条件写

- `batch`：一条请求带多个键值，作为**一条日志**原子提交，要么全部生效、要么
  全部不生效；读永远看不到半完成状态。重复键/空键/超限以 `invalid-batch`
  明确拒绝（原因如 `duplicate-key / empty-key / value-too-long /
  payload-too-large / too-many-items`）。
- 每个更新项可带前置版本：省略=无条件；`expect_version=0`=键必须不存在；
  `expect_version=v`=键必须恰好处于版本 v。不满足时整体返回
  `precondition-failed`，并给出 `failed_key` 与期望/实际版本；并发基于同一
  旧版本的更新最多一个成功，失败方得到确定原因（与 `timeout-unknown` 可区分）。
- **版本规则**：键首次写入为版本 1，此后每次生效的写入（单键或批量）+1；
  失败的请求不改版本。版本随快照持久化。
- 批量沿用会话幂等：同一 `(session, request)` 重发，即使第一次已提交且之后
  其他写入改了这些键，也返回**第一次的完整原始结论**（提交位置、每键结果、
  生效版本），不会按新状态重新判断条件。

## 换主 / 分区恢复后重试的结论

用同一个请求编号重发一笔之前没拿到确认的写时，系统只会收敛到两种确定结论，
不会停留在"说不清成没成"：

1. **返回第一次已经提交的原始结论**：旧条目在恢复瞬间/新 leader 处已提交，
   reply cache 命中，得到 `duplicate` + 原始提交位置、每键版本与失败详情。
2. **作为一次全新的干净提交生效**：旧条目确认从未提交（少数派 leader 被换
   掉、旧任期条目被覆盖），重试在新任期作为新条目提交一次。

什么时候**只能当成未知**：调用方自己的等待窗口到期（`timeout-unknown`）、
或此刻联系不上任何 leader（`not-leader`）。这两者都是**传输层瞬时结论**，
不代表成败；客户端应跟随更高任期的 leader、用相同 `(session, request)`
继续重试，直到拿到上面两种确定结论之一。

- 并发同编号重试（含分区前后落在不同节点、换主窗口）：所有调用方共享同一
  在途提案，最终都拿到**同一个**确定结论与同一个提交位置；超时只摘除
  调用方自己的等待，不会让其他调用方干等，也不会把"处理中/重复"当成"已
  完成"回给调用方。超时返回前会先与 reply cache 对账：若其实已经提交，
  直接返回原始结论而不是未知。
- 会话过期/关闭后，对**已有结论**请求的重试仍返回原始结论（reply cache
  优先于会话存活判断）；只有全新请求才会收到 `session-expired`。
- 条件失败是一种**确定失败**（带 `failed_key` 与期望/实际版本），重试不会
  按最新状态重新判断条件，因此它永远不会与超时未知混淆。

稳定性可重复验证（可连续大量重跑）：
```
go test -race ./internal/sim/ -run TestBatchRetryConvergenceAcrossPartitionAndLeaderChange -count=20
KVBATCH_RETRY_ITERS=25 go test -race ./internal/sim/   # 每轮内置的分区/换主/并发迭代次数
```


HTTP：`POST /kv/batch`；CLI：
```
go run ./cmd/client -servers $S batch $SID b1 cfg/a=1 cfg/b=2
go run ./cmd/client -servers $S batch $SID b2 cfg/c=new@0      # 必须不存在
go run ./cmd/client -servers $S batch $SID b3 cfg/a=99@1      # 必须当前是 v1
```

一致性语义、故障规则、成员变更与恢复流程、已知边界详见
[`docs/design.md`](docs/design.md)。
