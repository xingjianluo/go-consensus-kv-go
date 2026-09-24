// Command server runs one local consensus KV node.
//
// Start a 3-node cluster in separate terminals (loopback TCP, files
// under ./tmp, no external services):
//
//	go run ./cmd/server -id n1 -addr 127.0.0.1:9101 \
//	  -data ./tmp/n1 -peers n1=127.0.0.1:9101,n2=127.0.0.1:9102,n3=127.0.0.1:9103
//	go run ./cmd/server -id n2 -addr 127.0.0.1:9102 \
//	  -data ./tmp/n2 -peers n1=127.0.0.1:9101,n2=127.0.0.1:9102,n3=127.0.0.1:9103
//	go run ./cmd/server -id n3 -addr 127.0.0.1:9103 \
//	  -data ./tmp/n3 -peers n1=127.0.0.1:9101,n2=127.0.0.1:9102,n3=127.0.0.1:9103
//
// Kill a process with Ctrl-C (or kill -9) and restart it with the same
// flags: it recovers hard state + log (+ snapshot) from -data.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
	"github.com/xingjianluo/go-consensus-kv-go/internal/transport"
)

type peerFlag map[raft.NodeID]string

func (p peerFlag) String() string { return fmt.Sprint(map[raft.NodeID]string(p)) }
func (p peerFlag) Set(v string) error {
	for _, item := range strings.Split(v, ",") {
		kv := strings.SplitN(item, "=", 2)
		if len(kv) != 2 {
			return fmt.Errorf("bad peer %q", item)
		}
		p[raft.NodeID(kv[0])] = kv[1]
	}
	return nil
}

func main() {
	var (
		id        = flag.String("id", "", "node id")
		addr      = flag.String("addr", "127.0.0.1:9101", "listen address")
		dataDir   = flag.String("data", "./tmp/node", "data directory")
		bootstrap = flag.Bool("bootstrap", false, "initialize a fresh cluster (first/initial members only)")
		tickMS    = flag.Int("tick", 50, "raft tick interval in ms")
		peers     = peerFlag{}
	)
	flag.Var(peers, "peers", "initial peers id=addr,id=addr")
	flag.Parse()

	if *id == "" {
		log.Fatal("-id is required")
	}
	nodeID := raft.NodeID(*id)

	st, err := raft.NewFileStorage(*dataDir)
	if err != nil {
		log.Fatalf("storage: %v", err)
	}
	if *bootstrap {
		ids := make([]raft.NodeID, 0, len(peers))
		for id := range peers {
			ids = append(ids, id)
		}
		sortNodeIDs(ids)
		if err := raft.Bootstrap(st, nodeID, ids); err != nil &&
			err != raft.ErrAlreadyBootstrapped {
			log.Fatalf("bootstrap: %v", err)
		}
	}

	logger := log.New(os.Stdout, fmt.Sprintf("[%s] ", *id), log.LstdFlags|log.Lmsgprefix)

	tr, err := transport.NewTCPTransport(nodeID, *addr)
	if err != nil {
		log.Fatalf("transport: %v", err)
	}
	for pid, paddr := range peers {
		if pid != nodeID {
			tr.UpdatePeer(pid, paddr)
		}
	}

	store := kv.NewStore()
	node := raft.NewNode(raft.Options{
		ID:                nodeID,
		Storage:           st,
		ElectionTimeout:   20, // ~1s with 50ms tick
		HeartbeatTick:     2,  // ~100ms
		SnapshotThreshold: 4096,
		Events: func(e raft.Event) {
			switch e.Type {
			case raft.EvtRoleChange:
				logger.Printf("role=%s term=%d leader=%s", e.Role, e.Term, e.Leader)
			case raft.EvtProposalResult:
				logger.Printf("proposal req=%s status=%s reason=%s term=%d",
					e.ReqID, e.Status, e.Reason, e.Term)
			case raft.EvtEntriesCommitted:
				logger.Printf("commit advanced to index=%d term=%d", e.Index, e.Term)
			case raft.EvtMessageDropped:
				logger.Printf("message dropped reason=%s term=%d", e.Reason, e.Term)
			case raft.EvtConfChange:
				logger.Printf("config change event index=%d reason=%s", e.Index, e.Reason)
			case raft.EvtSnapshotRequired:
				logger.Printf("snapshot required at index=%d", e.Index)
			}
		},
	})
	svc := kv.NewService(node, store)
	go svc.Run()

	// One driver goroutine: ticks and pumps Ready/transport.
	stop := make(chan struct{})
	go drive(node, tr, store, svc, logger, time.Duration(*tickMS)*time.Millisecond, stop)

	// Simple local admin API on a second port for manual verification.
	go serveAdmin(*addr, nodeID, node, store, tr, peers, svc, logger)

	logger.Printf("node %s listening on %s, data=%s", *id, tr.LocalAddr(), *dataDir)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	close(stop)
	_ = tr.Close()
	logger.Printf("shut down cleanly")
}

func sortNodeIDs(ids []raft.NodeID) {
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
}
