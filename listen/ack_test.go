package listen_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	. "github.com/atrabilis/relay-events/listen"
	"github.com/atrabilis/relay-events/listentest"
)

// memSource is an in-process Source: each session delivers the given lines, records the acks and then waits for Stop.
type memSource struct {
	lines []string
	mu    sync.Mutex
	acks  []bool
}

type memSession struct {
	items chan Item
	stop  chan struct{}
	once  sync.Once
}

func (m *memSource) Start(context.Context, State) (Session, error) {
	s := &memSession{items: make(chan Item), stop: make(chan struct{})}
	go func() {
		defer close(s.items)
		for _, raw := range m.lines {
			l, err := ParseLine([]byte(raw))
			done := make(chan bool, 1)
			select {
			case s.items <- Item{Line: l, Err: err, Ack: func(ok bool) { done <- ok }}:
			case <-s.stop:
				return
			}
			ok := <-done
			m.mu.Lock()
			m.acks = append(m.acks, ok)
			m.mu.Unlock()
		}
		<-s.stop
	}()
	return s, nil
}

func (s *memSession) Items() <-chan Item { return s.items }
func (s *memSession) Stop()              { s.once.Do(func() { close(s.stop) }) }
func (s *memSession) Kill()              { s.Stop() }
func (s *memSession) Wait() int          { return ExitOK }

func (m *memSource) got() []bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]bool(nil), m.acks...)
}

type flakySink struct {
	*listentest.MemSink
	failReports bool
}

func (f *flakySink) Write(ctx context.Context, ev []Event) error {
	if f.failReports && len(ev) > 0 && ev[0].Kind == KindValue {
		return errors.New("database unavailable")
	}
	return f.MemSink.Write(ctx, ev)
}

func TestAckOnlyAfterStore(t *testing.T) {
	report, _ := json.Marshal(map[string]any{"type": "report", "received_at": "2026-11-23T10:00:00.000Z",
		"values": []map[string]any{{"i": 0, "ref": "BI 5 SigOpen", "reason": []string{"event"}, "value": true}}})
	for _, tc := range []struct {
		name string
		fail bool
		want bool
	}{{"stored", false, true}, {"store failed", true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			src := &memSource{lines: []string{`{"type":"enabled","at":"2026-11-23T10:00:00.000Z"}`, string(report)}}
			sink := &flakySink{MemSink: listentest.NewMemSink(), failReports: tc.fail}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				_ = Run(ctx, Options{Source: src, SourceName: "dnp3:3", BackoffMin: time.Hour}, sink)
				close(done)
			}()
			deadline := time.Now().Add(5 * time.Second)
			for len(src.got()) < 2 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			cancel()
			<-done
			acks := src.got()
			if len(acks) < 2 || !acks[0] || acks[1] != tc.want {
				t.Fatalf("acks = %v, want [true %v]", acks, tc.want)
			}
		})
	}
}
