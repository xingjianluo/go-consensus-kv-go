// Command server runs one local raft node over TCP.
//
// Example (3 nodes on one machine):
//
//	go run ./cmd/server -id n1 -listen 127.0.0.1:9101 \
//	    -peers n1=127.0.0.1:9101,n2=127.0.0.1:9102,n3=127.0.0.1:9103 \
//	    -datadir ./data/n1 -bootstrap n1,n2,n3
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/xingjianluo/go-consensus-kv-go/internal/nettransport"
)

func main() {
	var (
		id        = flag.String("id", "", "node id")
		listen    = flag.String("listen", "127.0.0.1:9101", "listen address")
		peersFlag = flag.String("peers", "", "id=host:port pairs, comma separated")
		dataDir   = flag.String("datadir", "./data/node", "data directory")
		bootstrap = flag.String("bootstrap", "", "comma separated initial voter ids (first start only)")
	)
	flag.Parse()
	if *id == "" {
		log.Fatal("-id is required")
	}
	peers := map[string]string{}
	if *peersFlag != "" {
		for _, pair := range strings.Split(*peersFlag, ",") {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) != 2 {
				log.Fatalf("bad peer %q", pair)
			}
			peers[kv[0]] = kv[1]
		}
	}
	var boot []string
	if *bootstrap != "" {
		boot = strings.Split(*bootstrap, ",")
	}
	srv, err := nettransport.NewServer(nettransport.Config{
		ID: *id, Listen: *listen, Peers: peers, DataDir: *dataDir,
	}, boot)
	if err != nil {
		log.Fatalf("new server: %v", err)
	}
	if err := srv.Start(); err != nil {
		log.Fatalf("start: %v", err)
	}
	fmt.Printf("node %s listening on %s, datadir=%s\n", *id, *listen, *dataDir)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Println("shutting down ...")
	_ = srv.Close()
}
