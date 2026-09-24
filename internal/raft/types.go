// Package raft implements a deterministic, single-node-state Raft
// consensus engine. It is transport- and storage-agnostic: the caller
// drives time via Tick, delivers network input via Step, and consumes
// emitted messages / state changes via a Ready channel.
package raft

// NodeID identifies a cluster member.
type NodeID string

// Term is a Raft election term (monotonic per node, persisted).
type Term uint64

// Index is a 1-based log position. Index 0 denotes "before the log".
type Index uint64

// Role of a node in the Raft state machine.
type Role uint8

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

func (r Role) String() string {
	switch r {
	case RoleFollower:
		return "follower"
	case RoleCandidate:
		return "candidate"
	case RoleLeader:
		return "leader"
	default:
		return "unknown"
	}
}

// ReqID is a client-assigned globally unique request identifier used for
// at-most-once (idempotent) application of proposals.
type ReqID string

// EntryKind distinguishes ordinary data entries from membership-change
// entries. The payload of an EntryConf entry is an encoded ConfChange.
type EntryKind uint8

const (
	EntryData EntryKind = iota
	EntryConf
)

// Entry is one log entry.
type Entry struct {
	Term  Term
	Index Index
	Kind  EntryKind
	Data  []byte
}

// ConfChangeType is the kind of a single-node membership change.
type ConfChangeType uint8

const (
	ConfAddNode ConfChangeType = iota
	ConfRemoveNode
)

// ConfChange requests adding or removing exactly one node.
// Only one-node-at-a-time changes are permitted; overlapping changes
// are rejected.
type ConfChange struct {
	Type ConfChangeType
	Node NodeID
	Addr string // contact address, used for AddNode
}

// Voter is a cluster member identity plus its contact address.
type Voter struct {
	ID   NodeID
	Addr string
}

// Config is the set of voters at some point in time. Membership changes
// add or remove exactly one Voter at a time.
type Config struct {
	Voters []Voter
}

// MsgType enumerates Raft wire message types.
type MsgType uint8

const (
	MsgHup  MsgType = iota // local: election timeout
	MsgBeat                // local: heartbeat tick
	MsgProp                // local: client proposal
	MsgRead                // local: linearizable read request
	MsgApp                 // AppendEntries (replication)
	MsgAppResp
	MsgVote // RequestVote
	MsgVoteResp
	MsgSnap // InstallSnapshot
	MsgSnapResp
	MsgJoin       // join request from a brand-new node
	MsgTimeoutNow // leader transfers leadership
)

// RejectReason explains why a request or message was refused or dropped.
// Zero value RejectNone means "not rejected".
type RejectReason uint8

const (
	RejectNone             RejectReason = iota
	RejectNotLeader                     // node is not leader / leader unknown
	RejectTermStale                     // stale term, step down
	RejectLogOlder                      // peer log older / conflict
	RejectConfigBusy                    // another membership change in flight
	RejectNodeExists                    // add of an existing voter
	RejectNodeMissing                   // remove of an unknown voter
	RejectRemoveLeader                  // removing the leader is not allowed
	RejectNotVoter                      // sender is not a member of the active config
	RejectDroppedPartition              // dropped by injected network partition
	RejectDroppedQueue                  // dropped: receive queue full / node down
	RejectSessionExpired                // application session expired (kv layer)
	RejectDuplicate                     // duplicate request, original result returned
	RejectTimeout                       // timed out before commit/confirmation
	RejectClosing                       // node shutting down
)

func (r RejectReason) String() string {
	switch r {
	case RejectNone:
		return "ok"
	case RejectNotLeader:
		return "not-leader"
	case RejectTermStale:
		return "term-stale"
	case RejectLogOlder:
		return "log-conflict"
	case RejectConfigBusy:
		return "config-change-in-flight"
	case RejectNodeExists:
		return "node-already-member"
	case RejectNodeMissing:
		return "node-not-member"
	case RejectRemoveLeader:
		return "cannot-remove-leader"
	case RejectNotVoter:
		return "not-a-voter"
	case RejectDroppedPartition:
		return "dropped-partition"
	case RejectDroppedQueue:
		return "dropped-node-down"
	case RejectSessionExpired:
		return "session-expired"
	case RejectDuplicate:
		return "duplicate"
	case RejectTimeout:
		return "timeout"
	case RejectClosing:
		return "closing"
	default:
		return "unknown"
	}
}

// Message is the unit of communication consumed/produced by the state
// machine. Local messages (Hup/Beat/Prop/Read) have From == node id.
type Message struct {
	Type     MsgType
	From     NodeID
	To       NodeID
	Term     Term
	Index    Index // for MsgApp: prevLogIndex; MsgAppResp: conflict/acked index
	LogTerm  Term  // prevLogTerm for MsgApp
	Entries  []Entry
	Commit   Index // leaderCommit
	Reject   bool
	RejectAt RejectReason
	Snapshot *Snapshot // MsgSnap
	ReqID    ReqID     // MsgProp / MsgRead correlation
	Data     []byte    // MsgProp / MsgRead payload
	Success  bool      // MsgSnapResp
	Granted  bool      // MsgVoteResp
	Addr     string    // MsgJoin contact address
}

// Snapshot is a compacted view of the state machine.
type Snapshot struct {
	Index Index
	Term  Term
	Conf  Config
	Data  []byte // application-defined snapshot bytes
}

// EventType enumerates observability events emitted by a Node.
type EventType uint8

const (
	EvtRoleChange EventType = iota
	EvtTermChange
	EvtProposalResult
	EvtReadResult
	EvtEntriesCommitted
	EvtSnapshotRequired // application must build a snapshot at Index
	EvtSnapshotApplied
	EvtConfChange
	EvtMessageDropped
)

// ProposalStatus is the terminal verdict for a client proposal.
type ProposalStatus uint8

const (
	PropCommitted ProposalStatus = iota // committed and applied
	PropRejected                        // explicitly rejected, never committed
	PropTimeout                         // outcome unknown; must not be presumed committed
)

func (s ProposalStatus) String() string {
	switch s {
	case PropCommitted:
		return "committed"
	case PropRejected:
		return "rejected"
	case PropTimeout:
		return "timeout-unknown"
	default:
		return "unknown"
	}
}

// Event is an observability record emitted by a Node.
type Event struct {
	Type          EventType
	Term          Term
	Role          Role
	Index         Index
	ReqID         ReqID
	Status        ProposalStatus
	Reason        RejectReason
	Leader        NodeID
	Entries       []Entry
	SnapshotIndex Index
}
