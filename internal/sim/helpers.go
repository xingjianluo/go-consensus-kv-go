package sim

// pickFollower returns any member different from leader.
func pickFollower(c *Cluster, leader string) string {
	for _, id := range c.order {
		if id != leader {
			return id
		}
	}
	return ""
}

// otherFollower returns the second member different from leader and skip.
func otherFollower(c *Cluster, leader string) string {
	skip := pickFollower(c, leader)
	for _, id := range c.order {
		if id != leader && id != skip {
			return id
		}
	}
	return ""
}

func containsStr(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
