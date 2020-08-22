package util

// non-thread-safe set
type Set map[string]bool

func NewSet(items ...string) Set {
	set := make(Set)
	for _, s := range items {
		set[s] = true
	}
	return set
}

func NewSetFromSlice(slice []string) Set {
	return NewSet(slice...)
}

func (set Set) Add(s string) {
	set[s] = true
}

func (set Set) Remove(s string) {
	delete(set, s)
}
