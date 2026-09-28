package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/xingjianluo/go-consensus-kv-go/internal/kv"
	"github.com/xingjianluo/go-consensus-kv-go/internal/raft"
	"github.com/xingjianluo/go-consensus-kv-go/internal/transport"
)

type adminState struct {
	id     raft.NodeID
	node   *raft.Node
	store  *kv.Store
	tr     *transport.TCPTransport
	svc    *kv.Service
	peers  map[raft.NodeID]string
	logger *log.Logger
}

func serveAdmin(raftAddr string, id raft.NodeID, node *raft.Node, store *kv.Store,
	tr *transport.TCPTransport, peers map[raft.NodeID]string, svc *kv.Service,
	logger *log.Logger) {
	httpAddr := adminAddr(raftAddr)
	st := &adminState{id: id, node: node, store: store, tr: tr, svc: svc,
		peers: peers, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("/status", st.status)
	mux.HandleFunc("/session/open", st.sessionOpen)
	mux.HandleFunc("/session/close", st.sessionClose)
	mux.HandleFunc("/kv/put", st.kvPut)
	mux.HandleFunc("/kv/batch", st.kvBatch)
	mux.HandleFunc("/kv/get", st.kvGet)
	mux.HandleFunc("/member/add", st.memberAdd)
	mux.HandleFunc("/member/remove", st.memberRemove)
	srv := &http.Server{Addr: httpAddr, Handler: mux, ReadTimeout: 10 * time.Second}
	logger.Printf("admin API on http://%s", httpAddr)
	_ = srv.ListenAndServe()
}

func adminAddr(addr string) string {
	i := strings.LastIndex(addr, ":")
	if i < 0 {
		return "127.0.0.1:10001"
	}
	port, err := strconv.Atoi(addr[i+1:])
	if err != nil {
		return "127.0.0.1:10001"
	}
	return addr[:i+1] + strconv.Itoa(port+1000)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (st *adminState) status(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{
		"id":     st.id,
		"role":   st.node.Role().String(),
		"term":   st.node.Term(),
		"leader": st.node.Leader(),
		"commit": st.node.CommitIndex(),
		"voters": st.node.ActiveConfig().Voters,
	})
}

func (st *adminState) sessionOpen(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TTLMillis int64 `json:"ttl_ms"`
	}
	body, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(body, &req)
	id, res := st.svc.OpenSession(req.TTLMillis)
	writeJSON(w, httpStatus(res), map[string]any{
		"session": id, "verdict": res.Verdict.String(),
		"reason": res.Reason, "leader": res.Leader, "index": res.Index,
	})
}

func (st *adminState) sessionClose(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Session string `json:"session"`
	}
	body, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(body, &req)
	res := st.svc.CloseSession(req.Session)
	writeJSON(w, httpStatus(res), map[string]any{
		"verdict": res.Verdict.String(), "reason": res.Reason, "leader": res.Leader,
	})
}

func (st *adminState) kvPut(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Session string `json:"session"`
		Request string `json:"request"`
		Key     string `json:"key"`
		Value   string `json:"value"`
	}
	body, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(body, &req)
	res := st.svc.Put(req.Session, req.Request, req.Key, []byte(req.Value))
	writeJSON(w, httpStatus(res), map[string]any{
		"verdict": res.Verdict.String(), "reason": res.Reason,
		"leader": res.Leader, "index": res.Index, "version": res.Version,
		"note": timeoutNote(res),
	})
}

// kvBatch applies an atomic multi-key write. Request body:
//
//	{
//	  "session": "s-...", "request": "req-42",
//	  "items": [
//	    {"key":"a","value":"1","check_version":true,"expected_version":0},
//	    {"key":"b","value":"2"}
//	  ]
//	}
//
// A successful reply includes the commit index and per-key resulting
// versions; a structural rejection / precondition failure is definitive
// (HTTP 409), a timeout is 504 and MUST be retried with the same request.
func (st *adminState) kvBatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Session string `json:"session"`
		Request string `json:"request"`
		Items   []struct {
			Key             string `json:"key"`
			Value           string `json:"value"`
			CheckVersion    bool   `json:"check_version"`
			ExpectedVersion int64  `json:"expected_version"`
		} `json:"items"`
	}
	body, _ := io.ReadAll(r.Body)
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"verdict": kv.VerdictRejected.String(), "reason": "bad-json",
		})
		return
	}
	items := make([]kv.BatchItem, len(req.Items))
	for i, it := range req.Items {
		items[i] = kv.BatchItem{Key: it.Key, Value: []byte(it.Value),
			CheckVersion: it.CheckVersion, ExpectedVersion: it.ExpectedVersion}
	}
	res := st.svc.Batch(req.Session, req.Request, items)
	out := map[string]any{
		"verdict": res.Verdict.String(), "reason": res.Reason,
		"leader": res.Leader, "index": res.Index,
		"note": timeoutNote(res),
	}
	if len(res.Items) > 0 {
		type ko struct {
			Key     string `json:"key"`
			Verdict string `json:"verdict"`
			Reason  string `json:"reason,omitempty"`
			Version int64  `json:"version"`
		}
		outs := make([]ko, len(res.Items))
		for i, o := range res.Items {
			outs[i] = ko{Key: o.Key, Verdict: o.Verdict.String(),
				Reason: o.Reason, Version: o.Version}
		}
		out["items"] = outs
	}
	writeJSON(w, httpStatus(res), out)
}

func (st *adminState) kvGet(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Session string `json:"session"`
		Request string `json:"request"`
		Key     string `json:"key"`
	}
	body, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(body, &req)
	res := st.svc.Get(req.Session, req.Request, req.Key)
	out := map[string]any{
		"verdict": res.Verdict.String(), "reason": res.Reason,
		"leader": res.Leader, "index": res.Index, "version": res.Version,
	}
	if len(res.Value) > 0 {
		out["value"] = string(res.Value)
	}
	writeJSON(w, httpStatus(res), out)
}

func (st *adminState) memberAdd(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID   string `json:"id"`
		Addr string `json:"addr"`
	}
	body, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(body, &req)
	reason, ok := st.svc.AddMember(raft.NodeID(req.ID), req.Addr)
	if !ok {
		writeJSON(w, http.StatusConflict, map[string]any{"accepted": false, "reason": reason.String()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true})
}

func (st *adminState) memberRemove(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID string `json:"id"`
	}
	body, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(body, &req)
	reason, ok := st.svc.RemoveMember(raft.NodeID(req.ID))
	if !ok {
		writeJSON(w, http.StatusConflict, map[string]any{"accepted": false, "reason": reason.String()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true})
}

func httpStatus(res kv.Result) int {
	switch res.Verdict {
	case kv.VerdictOK, kv.VerdictDuplicate:
		return http.StatusOK
	case kv.VerdictTimeout:
		return http.StatusGatewayTimeout
	case kv.VerdictNotLeader:
		return http.StatusTemporaryRedirect
	default:
		return http.StatusConflict
	}
}

func timeoutNote(res kv.Result) string {
	if res.Verdict == kv.VerdictTimeout {
		return "outcome unknown: do NOT assume committed; retry with same session+request id"
	}
	return ""
}
