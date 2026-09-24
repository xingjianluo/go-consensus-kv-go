package nettransport

import (
	"bufio"
	"net"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
)

// Client is a length-prefixed JSON RPC client for the demo CLI.
type Client struct {
	addr string
	conn net.Conn
	w    *bufio.Writer
	r    *bufio.Reader
}

// Dial connects to a node.
func Dial(addr string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return nil, err
	}
	return &Client{
		addr: addr, conn: conn,
		w: bufio.NewWriter(conn), r: bufio.NewReader(conn),
	}, nil
}

// roundTrip sends one request and reads the outcome.
func (c *Client) roundTrip(req ClientRequest) (raft.Outcome, error) {
	if err := writeFrameWriter(c.w, frame{Kind: "client", Req: &req}); err != nil {
		return raft.Outcome{}, err
	}
	f, err := readFrame(c.r)
	if err != nil {
		return raft.Outcome{}, err
	}
	if f.Out == nil {
		return raft.Outcome{}, errEmpty
	}
	return *f.Out, nil
}

// Call performs a request, following a not_leader hint by redirecting to
// known peer addresses supplied in peers. It preserves the ReqID on retry.
func (c *Client) Call(req ClientRequest, peers map[string]string) (raft.Outcome, error) {
	var last raft.Outcome
	for attempt := 0; attempt < 4; attempt++ {
		out, err := c.roundTrip(req)
		if err != nil {
			// connection broken: try to redial the same address once
			c.reconnect()
			last = raft.Outcome{OK: false, Code: raft.ErrTimeout,
				Reason: "连接失败，提交状态未知；请用相同 ReqID 重试"}
			continue
		}
		last = out
		if out.OK {
			return out, nil
		}
		if out.Code == raft.ErrNotLeader && out.Leader != "" {
			if addr, ok := peers[out.Leader]; ok && addr != c.addr {
				if err := c.redirect(addr); err == nil {
					continue
				}
			}
		}
		return out, nil
	}
	return last, nil
}

func (c *Client) redirect(addr string) error {
	_ = c.conn.Close()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return err
	}
	c.addr = addr
	c.conn = conn
	c.w = bufio.NewWriter(conn)
	c.r = bufio.NewReader(conn)
	return nil
}

func (c *Client) reconnect() {
	if err := c.redirect(c.addr); err != nil {
		c.conn = nil
	}
}

// Close closes the connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Addr returns the currently connected address.
func (c *Client) Addr() string { return c.addr }

var errEmpty = &raft.ClientError{Code: raft.ErrTimeout, Reason: "空响应"}
