package sim

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// DefaultRequestWait bounds one simulated request attempt.
const DefaultRequestWait = 3 * time.Second

// Client is a deterministic simulated client bound to a cluster.
// Reads and writes are retried with the same ReqID when a leader redirect
// or timeout occurs, which is safe because of idempotent replay.
type Client struct {
	c       *Cluster
	session string
	hint    string // last known leader
	idFn    func() string
}

// NewClient creates a client not yet attached to a session.
func NewClient(c *Cluster) *Client {
	cl := &Client{c: c}
	cl.idFn = cl.randomID
	return cl
}

// SetDeterministicIDs makes this client emit sequential ReqIDs, used by
// reproducibility tests so identical seeds yield byte-identical event logs.
func (cl *Client) SetDeterministicIDs(prefix string) {
	n := 0
	cl.idFn = func() string {
		n++
		return prefix + "-" + itoaN(int64(n))
	}
}

func itoaN(i int64) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

func (cl *Client) randomID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "r" + hex.EncodeToString(b[:])
}

func (cl *Client) newReqID() string { return cl.idFn() }

// RegisterSession opens a replicated session with an explicit TTL.
func (cl *Client) RegisterSession(ttl time.Duration) error {
	id := cl.newReqID()
	out := cl.do(raft.ClientRequest{
		ReqID: id, Op: "register", TTLNs: int64(ttl),
	})
	if !out.OK {
		return &raft.ClientError{Code: out.Code, Reason: out.Reason}
	}
	cl.session = out.Value
	if cl.session == "" {
		cl.session = id
	}
	return nil
}

// Session returns the registered session id.
func (cl *Client) Session() string { return cl.session }

// Put replicates a write.
func (cl *Client) Put(key, value string) raft.Outcome {
	return cl.do(raft.ClientRequest{
		ReqID: cl.newReqID(), Session: cl.session, Op: "put", Key: key, Value: value,
	})
}

// PutWithID replays with an explicit ReqID (idempotency tests).
func (cl *Client) PutWithID(reqID, key, value string) raft.Outcome {
	return cl.do(raft.ClientRequest{
		ReqID: reqID, Session: cl.session, Op: "put", Key: key, Value: value,
	})
}

// Delete replicates a delete.
func (cl *Client) Delete(key string) raft.Outcome {
	return cl.do(raft.ClientRequest{
		ReqID: cl.newReqID(), Session: cl.session, Op: "delete", Key: key,
	})
}

// Get performs a linearizable read through the current leader.
func (cl *Client) Get(key string) raft.Outcome {
	return cl.do(raft.ClientRequest{
		ReqID: cl.newReqID(), Session: cl.session, Op: "read", Key: key, ReadOnly: true,
	})
}

// Keepalive extends the session deadline.
func (cl *Client) Keepalive(ttl time.Duration) raft.Outcome {
	return cl.do(raft.ClientRequest{
		ReqID: cl.newReqID(), Session: cl.session, Op: "keepalive", TTLNs: int64(ttl),
	})
}

func (cl *Client) target() string {
	if cl.hint != "" {
		if rt := cl.c.nodes[cl.hint]; rt != nil && rt.alive {
			return cl.hint
		}
	}
	return cl.c.mustLeader()
}

// do runs a request with a bounded number of redirect/timeout retries,
// always preserving the same ReqID.
func (cl *Client) do(req raft.ClientRequest) raft.Outcome {
	for attempt := 0; attempt < 6; attempt++ {
		to := cl.target()
		out, ok := cl.c.submit(to, req, DefaultRequestWait)
		if out.Leader != "" {
			cl.hint = out.Leader
		}
		if !ok {
			// Timed out: do NOT assume commit; retry same ReqID.
			continue
		}
		if out.OK {
			return out
		}
		switch out.Code {
		case raft.ErrNotLeader:
			cl.hint = out.Leader
			continue
		case raft.ErrTimeout:
			// commit status unknown: safe to retry with identical ReqID
			continue
		default:
			return out // definitive explainable rejection
		}
	}
	return raft.Outcome{OK: false, Code: raft.ErrTimeout,
		Reason: "多次尝试后仍无确定结论；调用方不得将其视为已提交", Committed: false}
}
