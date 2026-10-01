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

换主/恢复后重试收敛的稳定性可以本地大量重跑验证：

```
# 确定性 sim（手动时钟，极快，适合 -count 大量重跑）
go test ./internal/sim/ -run 'BatchRetry|RepeatedSameRequest|ConditionalFailureRetry' -count=200 -v
# 真实 3 节点 kv.Service（墙上时钟 + 故障注入网络），覆盖并发同号重试
go test ./internal/kv/ -run 'RetryAfter|ConcurrentSameRequest' -count=50 -v
go test -race ./internal/kv/ -run 'RetryAfter|ConcurrentSameRequest' -count=10
```

跑真实多进程集群与客户端见 [`docs/design.md`](docs/design.md#5-本地验证)。

## 结论约定

- 写成功 ⇒ 已被多数提交；写返回 `timeout-unknown` ⇒ **结果未知，不代表成功**，
  须用相同 `(session, request)` 重试。
- 同一 `(session, request)` 恰好应用一次；重复得到 `duplicate` 与原始结论。
- 少数派不能提交、不能返回成功；旧任期节点不能覆盖已提交数据。
- **分区恢复 / 换主后的重试收敛**：用同一请求编号重发时，要么拿到第一次已提交的
  原始结论（`duplicate`，带原始提交位置），要么作为一次全新的干净提交生效（`ok`）。
  两种结论都唯一确定；在途合并、刚提交后的重复提案不会被当成"处理中/未知"返回，
  也不会让调用方干等。同一请求编号多路并发重试（跨越换主/恢复前后）的每个调用方
  都拿到**同一个确定结论、同一提交位置**；某个调用方超时只摘除它自己的等待，不
  影响其它共享该提案的调用方。
- 重试期间只有两种情况仍只能当"未知"：（1）本次调用在确认窗口内超时
  （`timeout-unknown`，条目可能已提交，继续用同一编号重试即可收敛）；
  （2）当时联系不到法定人数 leader（`not-leader`，重定向后重试）。
  会话本身未知/关闭的 `session-expired` 是终态拒绝，不属此类。
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

HTTP：`POST /kv/batch`；CLI：
```
go run ./cmd/client -servers $S batch $SID b1 cfg/a=1 cfg/b=2
go run ./cmd/client -servers $S batch $SID b2 cfg/c=new@0      # 必须不存在
go run ./cmd/client -servers $S batch $SID b3 cfg/a=99@1      # 必须当前是 v1
```

一致性语义、故障规则、成员变更与恢复流程、已知边界详见
[`docs/design.md`](docs/design.md)。
