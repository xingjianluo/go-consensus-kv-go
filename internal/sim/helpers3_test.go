package sim

import "github.com/xingjianluo/go-consensus-kv-go/internal/raft"

func readReq(id, key string) raft.ClientRequest {
	return raft.ClientRequest{ReqID: id, Op: "read", Key: key, ReadOnly: true}
}
