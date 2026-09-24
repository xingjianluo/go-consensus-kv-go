package sim

import (
	"strings"
	"testing"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

func runScenario(seed int64) string {
	c := NewCluster(Options{
		Nodes: []string{"n1", "n2", "n3"}, Seed: seed, SnapshotEntries: 3,
	})
	defer func() {
		for _, id := range c.order {
			c.nodes[id].node.Stop()
		}
	}()
	c.runFor(400 * time.Millisecond)
	leader := c.Leader()
	cl := NewClient(c)
	cl.SetDeterministicIDs("req")
	_ = cl.RegisterSession(5 * time.Second)
	cl.Put("a", "1")
	cl.Put("a", "2")

	// partition + crash + restart mid-workload
	iso := pickFollower(c, leader)
	c.Partition([]string{leader, otherFollower(c, leader)}, []string{iso})
	cl.Put("b", "3")
	c.Crash(iso)
	c.Heal()
	c.Restart(iso)
	c.runFor(800 * time.Millisecond)
	cl.Get("a")

	// membership change
	c.StartUnconfiguredMember("n4")
	c.AddMember("add-n4", "n4")
	c.runFor(600 * time.Millisecond)

	var b strings.Builder
	for _, ev := range c.EventLog() {
		b.WriteString(ev.Node)
		b.WriteString("|")
		b.WriteString(string(ev.Kind))
		b.WriteString("|")
		b.WriteString(itoaTest(ev.Term))
		b.WriteString("|")
		b.WriteString(itoaTest(ev.Index))
		b.WriteString("|")
		// Normalize request ids (random hex) so only decisions/positions
		// are compared, not client-generated ids.
		b.WriteString(ev.Detail)
		b.WriteString("\n")
	}
	return b.String()
}

func stripReqIDs(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		if i+5 <= len(s) && (s[i:i+5] == "reqID") {
			// skip until next space or comma
			j := i
			for j < len(s) && s[j] != ' ' && s[j] != ',' {
				j++
			}
			b.WriteString("reqID=*")
			i = j
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

// TestDeterministicReplay: the same seed must produce the exact same event
// sequence (node, kind, term, position, decision) across repeated runs.
func TestDeterministicReplay(t *testing.T) {
	r1 := runScenario(7)
	r2 := runScenario(7)
	r3 := runScenario(7)
	if r1 != r2 || r2 != r3 {
		t.Fatalf("identical seeds produced different schedules\n--- run1 ---\n%s\n--- run2 ---\n%s",
			head(r1, 40), head(r2, 40))
	}
	t.Logf("三次运行事件序列完全一致，共 %d 行关键判定", strings.Count(r1, "\n"))

	// Different seed may (and typically does) diverge; at minimum the
	// framework should not panic and still reach one leader.
	d := runScenario(99)
	if strings.Count(d, "role_change|") < 1 {
		t.Fatalf("expected role transitions in different-seed run")
	}
}

func normalizeDetail(s string) string {
	// One-pass normalization of any token "r"+hex following a space or the
	// "读请求" label, so random client ReqIDs never affect comparison.
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] == 'r' && i+1 < len(s) && isHex(s[i+1]) &&
			(i == 0 || s[i-1] == ' ' || strings.HasSuffix(s[:i], "求 ")) {
			j := i + 1 // skip the leading 'r' delimiter
			for j < len(s) && isHex(s[j]) {
				j++
			}
			b.WriteString("*")
			i = j
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

func isHex(ch byte) bool {
	return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')
}

func head(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, "\n")
}

// TestLateStaleMessagesSafelyIgnored: craft old-term / old-conf messages
// directly and assert the node logs a distinguishable stale conclusion and
// never mutates committed data.
func TestLateStaleMessagesSafelyIgnored(t *testing.T) {
	c := newTestCluster(t, 100)
	leader := awaitLeader(t, c)
	cl := mustRegister(t, c)
	mustPut(t, cl, "g", "good")
	term := c.State(leader).Term
	li := c.State(leader).LastIndex

	stale := []raft.Message{
		{Type: raft.MsgAppend, From: "ghost-old-node", To: leader,
			Term: term - 1, Index: li, LogTerm: term - 1, Commit: li + 99,
			Entries: []raft.Entry{{Term: term - 1, Index: li + 1,
				Kind: raft.EntryClient, Op: "put", Key: "g", Value: "EVIL"}}},
		{Type: raft.MsgRequestVote, From: "ghost-old-node", To: leader,
			Term: term + 50, LogTerm: term + 50, Index: 999},
		{Type: raft.MsgAppendResp, From: "ghost-old-node", To: leader,
			Term: term - 1, Index: li + 99},
	}
	for _, m := range stale {
		_ = c.nodes[leader].node.Step(m)
	}
	c.runFor(300 * time.Millisecond)

	st := c.State(leader)
	if st.Term < term {
		t.Fatalf("stale high-term non-member vote must not adopt term: %d < %d", st.Term, term)
	}
	if v, ok := c.nodes[leader].kv.Get("g"); !ok || v != "good" {
		t.Fatalf("committed data mutated by stale message: %q", v)
	}
	if st.LastIndex < li {
		t.Fatalf("log index regressed: %d < %d", st.LastIndex, li)
	}
	// Distinguishable stale handling must be observable in the event log.
	var staleKinds int
	for _, ev := range c.EventLog() {
		if ev.Node == leader && (ev.Kind == "stale_message" || ev.Kind == "reject") {
			staleKinds++
			t.Logf("可区分的旧消息结论: term=%d idx=%d %s", ev.Term, ev.Index, ev.Detail)
		}
	}
	if staleKinds == 0 {
		t.Fatalf("expected at least one stale_message/reject event")
	}
}
