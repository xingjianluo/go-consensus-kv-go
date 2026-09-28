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
- **多键原子批量写**（`batch`）：整批在同一提交位置生效，要么全改要么都不改，
  读看不到半成品；重复键/空键/超限以 `batch-*` 原因确定拒绝。
- **条件写 / 防覆盖**：每项可声明期望版本（`@0`=键必须不存在，`@N`=CAS 到
  版本 N）。并发同版本竞争最多一个成功，失败方得到可区分的
  `precondition-failed`（指明键与观察到的版本），与超时未知严格分开。
- 批量同样幂等：重试返回**首次完整结论**（提交 index、逐项结果、生效版本），
  不按新状态重判条件；结论与版本随快照/重启保留。
- 少数派不能提交、不能返回成功；旧任期节点不能覆盖已提交数据。
- 每条拒绝/丢弃都有可区分原因（`not-leader / term-stale / log-conflict /
  not-a-voter / dropped-partition / dropped-node-down ...`）。

一致性语义、故障规则、成员变更与恢复流程、已知边界详见
[`docs/design.md`](docs/design.md)。
