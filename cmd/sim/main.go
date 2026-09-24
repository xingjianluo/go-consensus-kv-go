// Command sim runs a deterministic end-to-end scenario in-process and
// prints the key events, terms, log positions and decisions. Repeated runs
// with the same -seed print identical output.
package main

import (
	"flag"
	"fmt"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
	"github.com/xingjianluo/go-consensus-kv-go/internal/sim"
)

func main() {
	seed := flag.Int64("seed", 42, "deterministic seed")
	verbose := flag.Bool("v", false, "print every key event")
	flag.Parse()

	rep := sim.RunDemoScenario(*seed)
	fmt.Println("===== 场景结论 =====")
	for _, l := range rep.Lines {
		fmt.Println(l)
	}
	if *verbose {
		fmt.Println("\n===== 关键事件（节点/任期/角色/位置/判定依据） =====")
		for _, ev := range rep.Events {
			if !verboseKind(ev.Kind) {
				continue
			}
			fmt.Printf("%-11s term=%-2d %-9s idx=%-3d %s\n",
				ev.Node, ev.Term, ev.Role, ev.Index, ev.Detail)
		}
	}
}

func verboseKind(k raft.EventKind) bool {
	switch k {
	case "role_change", "election", "proposal", "apply", "reject",
		"stale_message", "stale_term", "snapshot_install", "conf_change",
		"session", "read", "timeout", "replicate":
		return true
	}
	return false
}
