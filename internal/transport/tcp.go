// Package transport implements a length-prefixed JSON transport over
// (loopback) TCP so multiple real local processes form a cluster with
// no external services or credentials.
package transport

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// TCPTransport exchanges raft messages between processes. Each peer has
// a lazily dialed persistent connection; a delivery failure is returned
// to the caller, which must treat the message as not acknowledged.
type TCPTransport struct {
	mu     sync.Mutex
	id     raft.NodeID
	ln     net.Listener
	peers  map[raft.NodeID]string
	sendq  map[raft.NodeID]chan raft.Message
	recv   chan raft.Message
	closed chan struct{}
	wg     sync.WaitGroup
}

// NewTCPTransport creates a transport and starts listening on addr.
func NewTCPTransport(id raft.NodeID, addr string) (*TCPTransport, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	t := &TCPTransport{
		id: id, ln: ln,
		peers:  map[raft.NodeID]string{},
		sendq:  map[raft.NodeID]chan raft.Message{},
		recv:   make(chan raft.Message, 256),
		closed: make(chan struct{}),
	}
	t.wg.Add(1)
	go t.acceptLoop()
	return t, nil
}

// LocalAddr returns the bound address.
func (t *TCPTransport) LocalAddr() string { return t.ln.Addr().String() }

// Recv returns the channel of inbound messages.
func (t *TCPTransport) Recv() <-chan raft.Message { return t.recv }

// UpdatePeer remembers a member's dial address and starts/reuses its
// send loop.
func (t *TCPTransport) UpdatePeer(id raft.NodeID, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if old, ok := t.peers[id]; ok && old == addr {
		return
	}
	t.peers[id] = addr
	if _, ok := t.sendq[id]; !ok {
		q := make(chan raft.Message, 128)
		t.sendq[id] = q
		t.wg.Add(1)
		go t.sendLoop(id, addr, q)
	}
}

func (t *TCPTransport) acceptLoop() {
	defer t.wg.Done()
	for {
		c, err := t.ln.Accept()
		if err != nil {
			return
		}
		t.wg.Add(1)
		go t.readLoop(c)
	}
}

func (t *TCPTransport) readLoop(c net.Conn) {
	defer t.wg.Done()
	defer c.Close()
	r := newFrameReader(c)
	for {
		var m raft.Message
		if err := r.read(&m); err != nil {
			return
		}
		m.To = t.id
		select {
		case t.recv <- m:
		case <-t.closed:
			return
		}
	}
}

func (t *TCPTransport) sendLoop(id raft.NodeID, addr string, q <-chan raft.Message) {
	defer t.wg.Done()
	var c net.Conn
	for {
		select {
		case <-t.closed:
			return
		case m := <-q:
			if c == nil {
				var err error
				c, err = dialRepeatedly(addr, t.closed)
				if err != nil {
					return // shutting down
				}
			}
			if err := writeFrame(c, m); err != nil {
				_ = c.Close()
				c = nil
				// Best-effort requeue; message loss surfaces as proposal
				// timeout rather than a false success.
				if rq := t.requeue(id, m); rq != nil {
					select {
					case rq <- m:
					default:
					}
				}
			}
		}
	}
}

func dialRepeatedly(addr string, stop <-chan struct{}) (net.Conn, error) {
	for {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			return c, nil
		}
		select {
		case <-stop:
			return nil, io.ErrClosedPipe
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// requeue puts a failed message back on the peer queue if still present.
func (t *TCPTransport) requeue(id raft.NodeID, m raft.Message) chan<- raft.Message {
	t.mu.Lock()
	q := t.sendq[id]
	t.mu.Unlock()
	if q == nil {
		return nil
	}
	return q
}

// Send enqueues one message. Unknown peers produce an error so the host
// can learn the address from membership state first.
func (t *TCPTransport) Send(m raft.Message) error {
	t.mu.Lock()
	q, ok := t.sendq[m.To]
	t.mu.Unlock()
	if !ok {
		return errors.New("transport: unknown peer " + string(m.To))
	}
	select {
	case q <- m:
		return nil
	case <-t.closed:
		return io.ErrClosedPipe
	default:
		return errors.New("transport: send queue full")
	}
}

// Close shuts down listener and workers.
func (t *TCPTransport) Close() error {
	select {
	case <-t.closed:
	default:
		close(t.closed)
	}
	err := t.ln.Close()
	t.wg.Wait()
	return err
}

// --- length-prefixed JSON framing ---

const maxFrameBytes = 16 << 20

func writeFrame(w io.Writer, m raft.Message) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if len(b) > maxFrameBytes {
		return errors.New("frame too large")
	}
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(b)))
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

type frameReader struct {
	r   io.Reader
	buf []byte
}

func newFrameReader(r io.Reader) *frameReader { return &frameReader{r: r, buf: make([]byte, 4)} }

func (f *frameReader) read(m *raft.Message) error {
	if _, err := io.ReadFull(f.r, f.buf[:4]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(f.buf[:4])
	if n > maxFrameBytes {
		return errors.New("frame too large")
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(f.r, payload); err != nil {
		return err
	}
	return json.Unmarshal(payload, m)
}
