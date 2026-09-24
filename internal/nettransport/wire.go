package nettransport

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// Wire message envelopes: raft traffic and client RPC share one framing.
type frame struct {
	Kind string         `json:"kind"` // "raft" | "client"
	Msg  *raft.Message  `json:"msg,omitempty"`
	Req  *ClientRequest `json:"req,omitempty"`
	Out  *raft.Outcome  `json:"out,omitempty"`
}

// ClientRequest is the JSON form for the demo CLI.
type ClientRequest struct {
	ReqID   string `json:"req_id"`
	Session string `json:"session"`
	Op      string `json:"op"`
	Key     string `json:"key"`
	Value   string `json:"value"`
	TTLNs   int64  `json:"ttl_ns"`
}

type connPool struct {
	mu    sync.Mutex
	conns map[string]net.Conn
	w     map[string]*bufio.Writer
	r     map[string]*bufio.Reader
}

func newConnPool() *connPool {
	return &connPool{
		conns: map[string]net.Conn{},
		w:     map[string]*bufio.Writer{},
		r:     map[string]*bufio.Reader{},
	}
}

// dial returns a buffered writer for an addr connection. Caller must hold
// no pool lock; send/lock are managed by the caller.
// dialLocked assumes the caller holds p.mu.
func (p *connPool) dialLocked(addr string) (*bufio.Writer, *bufio.Reader, error) {
	if w, ok := p.w[addr]; ok {
		return w, p.r[addr], nil
	}
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return nil, nil, err
	}
	w := bufio.NewWriter(conn)
	p.conns[addr] = conn
	p.w[addr] = w
	p.r[addr] = bufio.NewReader(conn)
	return w, p.r[addr], nil
}

func (p *connPool) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
}

func writeFrameWriter(w *bufio.Writer, f frame) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "%d\n", len(b)); err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	return w.Flush()
}

func writeFrame(writer io.Writer, f frame) error {
	w, ok := writer.(*bufio.Writer)
	if !ok {
		w = bufio.NewWriter(writer)
	}
	if err := writeFrameWriter(w, f); err != nil {
		return err
	}
	return w.Flush()
}

func readFrame(r *bufio.Reader) (frame, error) {
	var n int
	if _, err := fmt.Fscanf(r, "%d\n", &n); err != nil {
		return frame{}, err
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return frame{}, err
	}
	var f frame
	if err := json.Unmarshal(b, &f); err != nil {
		return frame{}, err
	}
	return f, nil
}
