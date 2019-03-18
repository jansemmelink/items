package items

import "time"

//IRev describes a revision ...
type IRev interface {
	Nr() int
	Timestamp() time.Time
}

//Rev info
func Rev(nr int, ts time.Time) IRev {
	return rev{nr: nr, ts: ts}
}

type rev struct {
	nr int
	ts time.Time
}

func (r rev) Nr() int {
	return r.nr
}

func (r rev) Timestamp() time.Time {
	return r.ts
}
