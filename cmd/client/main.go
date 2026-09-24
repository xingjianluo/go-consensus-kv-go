// Command client is the local demo CLI for the raft KV service.
//
// Subcommands:
//
//	register -server host:port -ttl 10s
//	put      -session SID -key K -value V
//	get      -session SID -key K
//	delete   -session SID -key K
//
// Every invocation prints the structured outcome; a timeout never claims
// a commit, and replays with the same -reqid are deduplicated server side.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/nettransport"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	server := fs.String("server", "127.0.0.1:9101", "node address")
	peersFlag := fs.String("peers", "", "id=host:port pairs for leader redirect")
	session := fs.String("session", "", "session id")
	key := fs.String("key", "", "key")
	value := fs.String("value", "", "value")
	ttl := fs.Duration("ttl", 10*time.Second, "session TTL on register/keepalive")
	reqID := fs.String("reqid", "", "explicit idempotency ReqID (optional)")
	_ = fs.Parse(os.Args[2:])

	peers := map[string]string{}
	if *peersFlag != "" {
		for _, pair := range strings.Split(*peersFlag, ",") {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) == 2 {
				peers[kv[0]] = kv[1]
			}
		}
	}
	rid := *reqID
	if rid == "" {
		rid = fmt.Sprintf("cli-%d", time.Now().UnixNano())
	}
	req := nettransport.ClientRequest{
		ReqID: rid, Session: *session, Key: *key, Value: *value,
		TTLNs: int64(*ttl),
	}
	switch cmd {
	case "register":
		req.Op = "register"
	case "put":
		req.Op = "put"
	case "get":
		req.Op = "read"
	case "delete":
		req.Op = "delete"
	default:
		usage()
	}

	cl, err := nettransport.Dial(*server)
	if err != nil {
		log.Fatalf("dial %s: %v", *server, err)
	}
	defer cl.Close()
	out, err := cl.Call(req, peers)
	if err != nil {
		log.Fatalf("rpc: %v", err)
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
	if !out.OK {
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: client {register|put|get|delete} [flags]")
	os.Exit(2)
}
