// Package listentest provides an in-memory listen.Sink with the same dedup rules as sql/events.sql, for tests of
// agents built on package listen.
package listentest

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/atrabilis/relay-events/listen"
)

// MemSink mimics the postgres_events table: value rows are unique by (epoch, entry_id, ref); LastState reads the
// last row's epoch and the last value row's EntryID in that epoch.
type MemSink struct {
	mu       sync.Mutex
	rows     []listen.Event
	keys     map[string]bool
	FailNext int // number of Write calls to fail
	writes   int
	States   []listen.State // states returned by LastState, in order
}

func NewMemSink() *MemSink { return &MemSink{keys: map[string]bool{}} }

func (s *MemSink) LastState(context.Context) (listen.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var st listen.State
	if n := len(s.rows); n > 0 {
		st.Epoch = s.rows[n-1].Epoch
	}
	for i := len(s.rows) - 1; i >= 0; i-- {
		r := s.rows[i]
		if r.Kind == listen.KindValue && r.EntryID != "" && r.Epoch == st.Epoch {
			st.EntryID = r.EntryID
			break
		}
	}
	s.States = append(s.States, st)
	return st, nil
}

func (s *MemSink) Write(_ context.Context, events []listen.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes++
	if s.FailNext > 0 {
		s.FailNext--
		return errors.New("database unavailable")
	}
	for _, e := range events {
		if e.Kind == listen.KindValue {
			k := fmt.Sprintf("%d|%s|%s", e.Epoch, e.EntryID, e.Ref)
			if e.EntryID == "" { // second unique index of sql/postgres_events.sql; rows without time stamp are not deduplicated
				k = fmt.Sprintf("noentry|%d|%s|%v|%s|%d", e.Epoch, e.Ref, e.DeviceTS, e.Value, len(s.rows))
				if e.DeviceTS != nil {
					k = fmt.Sprintf("noentry|%d|%s|%s|%s", e.Epoch, e.Ref, e.DeviceTS.UTC(), e.Value)
				}
			}
			if s.keys[k] {
				continue
			}
			s.keys[k] = true
		}
		s.rows = append(s.rows, e)
	}
	return nil
}

// Snapshot returns a copy of the stored rows.
func (s *MemSink) Snapshot() []listen.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]listen.Event(nil), s.rows...)
}

// Count returns the number of stored rows of a kind.
func (s *MemSink) Count(kind string) int {
	n := 0
	for _, r := range s.Snapshot() {
		if r.Kind == kind {
			n++
		}
	}
	return n
}
