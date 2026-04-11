package observability

import "sync"

type Readiness struct {
	mu     sync.RWMutex
	checks map[string]bool
}

func NewReadiness(checkNames ...string) *Readiness {
	checks := make(map[string]bool, len(checkNames))
	for _, name := range checkNames {
		checks[name] = false
	}
	return &Readiness{checks: checks}
}

func (r *Readiness) Set(name string, ready bool) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks[name] = ready
}

func (r *Readiness) Snapshot() map[string]bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	snapshot := make(map[string]bool, len(r.checks))
	for name, ready := range r.checks {
		snapshot[name] = ready
	}
	return snapshot
}

func (r *Readiness) Ready() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.checks) == 0 {
		return true
	}
	for _, ready := range r.checks {
		if !ready {
			return false
		}
	}
	return true
}
