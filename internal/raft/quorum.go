package raft

// Majorize returns the smallest strict-majority size for n members.
func Majorize(n int) int { return n/2 + 1 }

// HasMajority reports whether votes contains a strict majority of the
// given member ids.
func HasMajority(ids []NodeID, votes map[NodeID]bool) bool {
	need := Majorize(len(ids))
	got := 0
	for _, id := range ids {
		if votes[id] {
			got++
		}
	}
	return got >= need
}
