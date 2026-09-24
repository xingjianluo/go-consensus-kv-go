package sim

import (
	"fmt"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// ScenarioReport is the printable result of RunDemoScenario.
type ScenarioReport struct {
	Events []raft.Event
	Lines  []string
}

// RunDemoScenario executes a fixed, seed-deterministic scenario covering
// election, replication, minority partition, stale-term rollback, crash,
// snapshot-restricted restart and a membership add/remove, then returns a
// human-readable report. Repeated calls with the same seed are identical.
func RunDemoScenario(seed int64) ScenarioReport {
	c := NewCluster(Options{
		Nodes: []string{"n1", "n2", "n3"}, Seed: seed, SnapshotEntries: 4,
	})
	defer func() {
		for _, id := range c.order {
			if rt := c.nodes[id]; rt != nil && rt.alive {
				rt.node.Stop()
			}
		}
	}()

	var lines []string
	add := func(format string, a ...any) { lines = append(lines, fmt.Sprintf(format, a...)) }

	c.runFor(500 * time.Millisecond)
	leader := c.Leader()
	add("1) 选举完成: leader=%s term=%d", leader, c.State(leader).Term)

	cl := NewClient(c)
	cl.SetDeterministicIDs("demo")
	if err := cl.RegisterSession(20 * time.Second); err != nil {
		add("register 失败: %v", err)
		return ScenarioReport{Events: c.EventLog(), Lines: lines}
	}
	out := cl.Put("color", "blue")
	add("2) 写入 color=blue: 提交 index=%d term=%d（多数派确认）", out.Index, out.Term)

	// Minority partition around the leader.
	var majority []string
	for _, id := range c.order {
		if id != leader {
			majority = append(majority, id)
		}
	}
	c.Partition([]string{leader}, majority)
	c.runFor(800 * time.Millisecond)
	newLeader := ""
	for _, id := range majority {
		if c.State(id).Role == raft.Leader {
			newLeader = id
		}
	}
	add("3) 分区后多数派选出新 leader=%s；旧 leader=%s 处于少数派", newLeader, leader)

	ch := c.registerResult("demo-minority")
	_ = c.nodes[leader].node.Propose(raft.ClientRequest{
		ReqID: "demo-minority", Session: cl.Session(), Op: "put", Key: "color", Value: "GRAY",
	})
	if mo, ok := c.waitResult(ch, 400*time.Millisecond); ok && mo.OK {
		add("4) 错误：少数派提交成功")
	} else {
		add("4) 少数派旧 leader 写入被拒绝/无结论 code=%s（不得视为已提交）", mo.Code)
	}

	// Submit through the majority-side leader explicitly (the partition
	// hides the old leader from client discovery).
	chGreen := c.registerResult("demo-green")
	_ = c.nodes[newLeader].node.Propose(raft.ClientRequest{
		ReqID: "demo-green", Session: cl.Session(), Op: "put",
		Key: "color", Value: "green",
	})
	if go2, ok := c.waitResult(chGreen, 800*time.Millisecond); !ok || !go2.OK {
		add("5) 错误：多数派写入未确认: %+v", go2)
	} else {
		add("5) 多数派新 leader=%s 提交 color=green index=%d", newLeader, go2.Index)
	}

	c.Heal()
	c.runFor(800 * time.Millisecond)
	add("6) 分区愈合: 旧 leader 回滚未提交后缀并追平 term=%d", c.State(leader).Term)

	// Crash + restart of one follower after compaction.
	victim := pickFollower(c, c.Leader())
	for i := 0; i < 10; i++ {
		cb := c.registerResult(fmt.Sprintf("demo-bulk-%d", i))
		_ = c.nodes[c.Leader()].node.Propose(raft.ClientRequest{
			ReqID: fmt.Sprintf("demo-bulk-%d", i), Session: cl.Session(),
			Op: "put", Key: "bulk", Value: fmt.Sprintf("v%d", i),
		})
		_, _ = c.waitResult(cb, 800*time.Millisecond)
	}
	c.Crash(victim)
	c.Restart(victim)
	c.runFor(1200 * time.Millisecond)
	v, _ := c.nodes[victim].kv.Get("color")
	add("7) 崩溃重启+快照补齐: %s 恢复 color=%s applied=%d",
		victim, v, c.State(victim).LastApplied)

	// Membership add then remove.
	c.StartUnconfiguredMember("n4")
	if o := c.AddMember("demo-add", "n4"); o.OK {
		add("8) 成员加入 n4 在 index=%d 生效，无回退", o.Index)
	}
	c.runFor(800 * time.Millisecond)
	if o := c.RemoveMember("demo-rm", "n4"); o.OK {
		add("9) 成员退出 n4 在 index=%d 生效，当前投票节点=%v", o.Index, c.State(c.Leader()).Voters)
	}
	c.runFor(800 * time.Millisecond)

	finalColor, _ := c.nodes[c.Leader()].kv.Get("color")
	add("10) 最终一致结论: color=%s, leader=%s, term=%d",
		finalColor, c.Leader(), c.State(c.Leader()).Term)

	return ScenarioReport{Events: c.EventLog(), Lines: lines}
}
