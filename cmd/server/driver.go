package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
	"github.com/xingjianluo/go-consensus-kv-go/internal/transport"
)

// drive is the single goroutine that ticks raft and feeds inbound
// transport messages into the node. Outbound messages are handed to the
// transport by the Service's MessageHook; committed entries are applied
// to the KV store inside Service.Run.
func drive(node *raft.Node, tr *transport.TCPTransport, svc *kv.Store,
	service *kv.Service, logger *log.Logger, tick time.Duration, stop <-chan struct{}) {

	service.MessageHook = func(m raft.Message) {
		if err := tr.Send(m); err != nil {
			// Unknown peer: learn addresses from the active config and
			// retry once. A still-failing send is a genuine delivery
			// failure; raft re-sends on the next heartbeat.
			for _, v := range node.ActiveConfig().Voters {
				if v.Addr != "" {
					tr.UpdatePeer(v.ID, v.Addr)
				}
			}
			_ = tr.Send(m)
		}
	}
	service.OnConfCommit = func(e raft.Entry) {
		var conf raft.Config
		if err := json.Unmarshal(e.Data, &conf); err != nil {
			return
		}
		for _, v := range conf.Voters {
			if v.Addr != "" {
				tr.UpdatePeer(v.ID, v.Addr)
			}
		}
		logger.Printf("config committed at index=%d voters=%d", e.Index, len(conf.Voters))
	}

	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			node.Tick()
		case m := <-tr.Recv():
			node.Step(m)
		}
	}
}
