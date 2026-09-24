package sim

import (
	"strings"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

func filterEvents(evs []raft.Event, kinds ...raft.EventKind) string {
	want := map[raft.EventKind]bool{}
	for _, k := range kinds {
		want[k] = true
	}
	var b strings.Builder
	for _, ev := range evs {
		if want[ev.Kind] {
			b.WriteString("  ")
			b.WriteString(ev.Node)
			b.WriteString(" ")
			b.WriteString(ev.Detail)
			b.WriteString("\n")
		}
	}
	return b.String()
}
