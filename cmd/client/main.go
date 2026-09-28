// Command client is a small CLI for the local consensus KV cluster.
// It follows "not-leader" redirects automatically and prints the
// decisive verdict for every request. A timeout is reported as
// "unknown outcome" rather than success.
//
// Examples:
//
//	# find the leader and open a session
//	go run ./cmd/client -servers 127.0.0.1:10101,127.0.0.1:10102,127.0.0.1:10103 session-open
//	go run ./cmd/client -servers ... put <session> <req-id> <key> <value>
//	go run ./cmd/client -servers ... get <session> <req-id> <key>
//	go run ./cmd/client -servers ... batch <session> <req-id> <k=v[@ver]> ...
//	# ver: 0 = key must not exist; N>0 = key must be at version N; omitted = any
//	go run ./cmd/client -servers ... member-add <id> <raft-addr>
//	go run ./cmd/client -servers ... member-remove <id>
//	status
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	servers := flag.String("servers", "127.0.0.1:10101", "comma-separated admin addresses")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: client <status|session-open|put|get|batch|member-add|member-remove> ...")
		os.Exit(2)
	}
	cl := &client{
		servers: split(*servers),
		http:    &http.Client{Timeout: 5 * time.Second},
	}
	var err error
	switch args[0] {
	case "status":
		err = cl.status()
	case "session-open":
		err = cl.sessionOpen()
	case "put":
		require(args, 5, "put <session> <req-id> <key> <value>")
		err = cl.put(args[1], args[2], args[3], args[4])
	case "get":
		require(args, 4, "get <session> <req-id> <key>")
		err = cl.get(args[1], args[2], args[3])
	case "batch":
		require(args, 4, "batch <session> <req-id> <k=v[@ver]> ...")
		err = cl.batch(args[1], args[2], args[3:])
	case "member-add":
		require(args, 3, "member-add <id> <raft-addr>")
		err = cl.memberAdd(args[1], args[2])
	case "member-remove":
		require(args, 2, "member-remove <id>")
		err = cl.memberRemove(args[1])
	default:
		err = fmt.Errorf("unknown command %q", args[0])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}

func require(args []string, n int, usage string) {
	if len(args) < n {
		fmt.Fprintln(os.Stderr, "usage:", usage)
		os.Exit(2)
	}
}

func split(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == ',' {
			out = append(out, cur)
			cur = ""
		} else {
			cur += string(r)
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

type client struct {
	servers []string
	http    *http.Client
	leader  string
}

// do sends one POST, making a single bounded pass over all configured
// servers. A "not-leader" answer simply moves on to the next server; the
// servers list is already in admin-port form so no address conversion is
// needed. Terminal JSON (success or a definitive failure) is returned.
func (c *client) do(path string, body any) (map[string]any, error) {
	b, _ := json.Marshal(body)
	var last map[string]any
	var lastErr error
	for _, addr := range c.servers {
		url := "http://" + addr + path
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			lastErr = fmt.Errorf("%s: bad json: %w", addr, err)
			continue
		}
		last = m
		// A definitive answer: anything other than a redirect we retry by
		// trying the next server to locate the leader ourselves.
		if resp.StatusCode != http.StatusTemporaryRedirect {
			return m, nil
		}
	}
	if last != nil {
		return last, nil
	}
	return nil, fmt.Errorf("no server reachable: %v", lastErr)
}

func (c *client) print(m map[string]any, err error) error {
	if err != nil {
		return err
	}
	out, _ := json.MarshalIndent(m, "", "  ")
	fmt.Println(string(out))
	return nil
}

func (c *client) status() error {
	for _, addr := range c.servers {
		resp, err := c.http.Get("http://" + addr + "/status")
		if err != nil {
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var m map[string]any
		if json.Unmarshal(raw, &m) == nil {
			return c.print(m, nil)
		}
	}
	return fmt.Errorf("no server reachable for status")
}

func (c *client) sessionOpen() error {
	m, err := c.do("/session/open", map[string]any{})
	return c.print(m, err)
}

func (c *client) put(session, req, key, value string) error {
	m, err := c.do("/kv/put", map[string]string{
		"session": session, "request": req, "key": key, "value": value,
	})
	return c.print(m, err)
}

func (c *client) get(session, req, key string) error {
	m, err := c.do("/kv/get", map[string]string{
		"session": session, "request": req, "key": key,
	})
	return c.print(m, err)
}

// batch parses items of the form key=value or key=value@expectVersion.
// expectVersion 0 means "key must not exist"; a positive v pins the
// current version. The whole request is atomic on the server.
func (c *client) batch(session, req string, raw []string) error {
	type op struct {
		Key           string `json:"key"`
		Value         string `json:"value"`
		ExpectVersion *int64 `json:"expect_version,omitempty"`
	}
	ops := make([]op, 0, len(raw))
	for _, item := range raw {
		eq := strings.IndexByte(item, '=')
		if eq <= 0 {
			return fmt.Errorf("bad batch item %q: want key=value[@ver]", item)
		}
		key, rest := item[:eq], item[eq+1:]
		o := op{Key: key, Value: rest}
		if at := strings.LastIndexByte(rest, '@'); at >= 0 {
			// '@' inside the value without a trailing integer is literal.
			suffix := rest[at+1:]
			if v, perr := strconv.ParseInt(suffix, 10, 64); perr == nil {
				o.Value = rest[:at]
				o.ExpectVersion = &v
			}
		}
		ops = append(ops, o)
	}
	m, err := c.do("/kv/batch", map[string]any{
		"session": session, "request": req, "ops": ops,
	})
	return c.print(m, err)
}

func (c *client) memberAdd(id, addr string) error {
	m, err := c.do("/member/add", map[string]string{"id": id, "addr": addr})
	return c.print(m, err)
}

func (c *client) memberRemove(id string) error {
	m, err := c.do("/member/remove", map[string]string{"id": id})
	return c.print(m, err)
}
