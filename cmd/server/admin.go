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
	mux.HandleFunc("/kv/get", st.kvGet)
	mux.HandleFunc("/kv/batch", st.kvBatch)
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
	out := map[string]any{
		"verdict": res.Verdict.String(), "reason": res.Reason,
		"leader": res.Leader, "index": res.Index,
		"note": timeoutNote(res),
	}
	if res.Version > 0 {
		out["version"] = res.Version
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
		"leader": res.Leader, "index": res.Index,
	}
	if res.Verdict == kv.VerdictOK || res.Verdict == kv.VerdictDuplicate {
		out["value"] = string(res.Value)
		out["version"] = res.Version
	}
	writeJSON(w, httpStatus(res), out)
}

// batchItemJSON is the wire form of one batch update item. ExpectVersion
// semantics: omitted = unconditional; 0 = key must not exist; v>0 = key
// must be at exactly version v.
type batchItemJSON struct {
	Key           string `json:"key"`
	Value         string `json:"value"`
	ExpectVersion *int64 `json:"expect_version,omitempty"`
}

func (st *adminState) kvBatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Session string          `json:"session"`
		Request string          `json:"request"`
		Ops     []batchItemJSON `json:"ops"`
	}
	body, _ := io.ReadAll(r.Body)
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"verdict": kv.VerdictInvalidBatch.String(), "reason": "bad-json"})
		return
	}
	items := make([]kv.BatchItem, len(req.Ops))
	for i, op := range req.Ops {
		items[i] = kv.BatchItem{Key: op.Key, Value: []byte(op.Value),
			ExpectVersion: op.ExpectVersion}
	}
	res := st.svc.Batch(req.Session, req.Request, items)
	out := map[string]any{
		"verdict": res.Verdict.String(), "reason": res.Reason,
		"leader": res.Leader, "index": res.Index,
		"note": timeoutNote(res),
	}
	if res.Batch != nil {
		out["applied_at"] = res.Batch.AppliedAt
		out["failed_key"] = res.Batch.FailedKey
		if res.Batch.FailedKey != "" {
			out["expect_version"] = res.Batch.ExpectVersion
			out["actual_version"] = res.Batch.ActualVersion
		}
		ops := make([]map[string]any, 0, len(res.Batch.Ops))
		for _, op := range res.Batch.Ops {
			ops = append(ops, map[string]any{
				"key": op.Key, "index": op.Index, "version": op.Version,
			})
		}
		out["ops"] = ops
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
