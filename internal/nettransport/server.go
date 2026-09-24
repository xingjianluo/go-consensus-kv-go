package nettransport

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/clock"
	"github.com/xingjianluo/go-consensus-kv-go/internal/kvsm"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// Config addresses one node and its peers.
type Config struct {
	ID      string
	Listen  string
	Peers   map[string]string // id -> host:port
	DataDir string
}

// Server runs one raft node with a disk store and JSON RPC over TCP.
type Server struct {
	cfg   Config
	store *raft.DiskStorage
	node  *raft.Node
	kv    *kvsm.KV
	ln    net.Listener
	pool  *connPool

	mu      sync.Mutex
	waiters map[string]chan raft.Outcome
	events  []raft.Event
}

// liveEnv implements raft.Env for a real process.
type liveEnv struct {
	s *Server
}

func (e liveEnv) Now() time.Time { return time.Now() }

func (e liveEnv) Send(m raft.Message) {
	addr, ok := e.s.cfg.Peers[m.To]
	if !ok {
		return
	}
	e.s.pool.mu.Lock()
	defer e.s.pool.mu.Unlock()
	w, _, err := e.s.pool.dialLocked(addr)
	if err != nil {
		return // peer down; raft retries on the next heartbeat
	}
	_ = writeFrameWriter(w, frame{Kind: "raft", Msg: &m})
}

func (e liveEnv) Emit(ev raft.Event) {
	e.s.mu.Lock()
	e.s.events = append(e.s.events, ev)
	if len(e.s.events) > 2000 {
		e.s.events = e.s.events[len(e.s.events)-1000:]
	}
	e.s.mu.Unlock()
	fmt.Printf("[%s] t=%d %-9s idx=%d %s\n",
		ev.Node, ev.Term, ev.Role, ev.Index, ev.Detail)
}

func (e liveEnv) Respond(reqID string, out raft.Outcome) {
	e.s.mu.Lock()
	ch := e.s.waiters[reqID]
	if ch != nil {
		delete(e.s.waiters, reqID)
	}
	e.s.mu.Unlock()
	if ch != nil {
		select {
		case ch <- out:
		default:
		}
	}
}

func (e liveEnv) ApplySnapshotData(data []byte) error {
	return e.s.kv.Restore(data)
}

// liveTimers uses wall-clock time.
type liveTimers struct {
	s       *Server
	mu      sync.Mutex
	stop    chan struct{}
	running bool
}

func (t *liveTimers) ResetElection(term int)  { t.arm(term, "election") }
func (t *liveTimers) ResetHeartbeat(term int) { t.arm(term, "heartbeat") }

func (t *liveTimers) arm(term int, tag string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stop != nil {
		close(t.stop)
	}
	t.stop = make(chan struct{})
	stop := t.stop
	var d time.Duration
	if tag == "election" {
		d = t.s.node.ElectionTimeout()
	} else {
		d = 200 * time.Millisecond
	}
	go func() {
		select {
		case <-time.After(d):
			_ = t.s.node.Tick(tag)
		case <-stop:
		}
	}()
}

func (t *liveTimers) StopTimers() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stop != nil {
		close(t.stop)
		t.stop = nil
	}
}

// NewServer constructs (but does not start) a server.
// bootstrap lists the initial voter ids; an empty/absent bootstrap on an
// existing data dir means the node restores its prior state.
func NewServer(cfg Config, bootstrap []string) (*Server, error) {
	store, err := raft.OpenDiskStorage(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	kv := kvsm.New()
	s := &Server{
		cfg:     cfg,
		store:   store,
		kv:      kv,
		pool:    newConnPool(),
		waiters: map[string]chan raft.Outcome{},
	}
	env := liveEnv{s: s}
	nodeCfg := raft.Config{
		ID:                cfg.ID,
		ElectionMin:       250 * time.Millisecond,
		ElectionMax:       500 * time.Millisecond,
		Heartbeat:         100 * time.Millisecond,
		RNGSeed:           time.Now().UnixNano(),
		SnapshotThreshold: 200,
	}
	s.node = raft.NewNode(nodeCfg, store, kv, env, &liveTimers{s: s}, clock.WallClock{})
	if len(bootstrap) > 0 {
		if conf, ok := raft.LoadSnapshotConf(cfg.DataDir); !ok {
			if err := s.node.Bootstrap(raft.ConfState{Voters: bootstrap}); err != nil {
				return nil, err
			}
		} else {
			_ = conf
		}
	}
	s.node.StartTimers()
	return s, nil
}

// Start serves RPCs and raft traffic.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return err
	}
	s.ln = ln
	go s.acceptLoop()
	return nil
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.serveConn(conn)
	}
}

func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		f, err := readFrame(r)
		if err != nil {
			return
		}
		switch f.Kind {
		case "raft":
			if f.Msg != nil {
				_ = s.node.Step(*f.Msg)
			}
		case "client":
			if f.Req != nil {
				s.handleClient(conn, f)
			}
		}
	}
}

func (s *Server) handleClient(conn net.Conn, f frame) {
	req := f.Req
	out := s.submit(req)
	_ = writeFrame(conn, frame{Kind: "client", Out: &out})
}

func (s *Server) submit(req *ClientRequest) raft.Outcome {
	rr := raft.ClientRequest{
		ReqID: req.ReqID, Session: req.Session, Op: req.Op,
		Key: req.Key, Value: req.Value, TTLNs: req.TTLNs,
		ReadOnly: req.Op == "read",
	}
	if req.ReqID == "" {
		req.ReqID = fmt.Sprintf("c-%d", time.Now().UnixNano())
		rr.ReqID = req.ReqID
	}
	ch := make(chan raft.Outcome, 1)
	s.mu.Lock()
	s.waiters[req.ReqID] = ch
	s.mu.Unlock()
	_ = s.node.Propose(rr)
	select {
	case o := <-ch:
		return o
	case <-time.After(2 * time.Second):
		return raft.Outcome{OK: false, Code: raft.ErrTimeout,
			Reason: "超时：提交状态未知，请用相同 ReqID 重试，勿假定已提交", Committed: false}
	}
}

// Close stops the node and the listener.
func (s *Server) Close() error {
	s.node.Stop()
	s.pool.closeAll()
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}
