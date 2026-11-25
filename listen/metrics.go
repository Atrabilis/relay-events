package listen

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Metrics of one listener, optionally written as a Prometheus textfile (node_exporter textfile collector).
type Metrics struct {
	mu     sync.Mutex
	labels string
	path   string
	last   time.Time
	Counters
}

// Counters are the values behind the metrics.
type Counters struct {
	Up             bool
	Busy           bool // last attempt found the RCB enabled by another client
	LastExit       int
	Sessions       int64
	Reports        int64
	Gaps           int64
	BadLines       int64
	UnsyncedStamps int64 // values whose time stamp says the device clock was not synchronized or failed
	NoEntryIDs     bool  // the device does not put EntryIDs in its reports: sessions cannot resume
	LastReport     time.Time
	LastHeartbeat  time.Time
}

// NewMetrics returns metrics labelled with the device and event source; path "" keeps them in memory only.
func NewMetrics(path, device, source string) *Metrics {
	esc := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return &Metrics{path: path, labels: fmt.Sprintf(`device="%s",source="%s"`, esc.Replace(device), esc.Replace(source))}
}

// set applies f under the lock and writes the file at most every 5 seconds (always when Up changes). Safe on nil.
func (m *Metrics) set(f func(*Counters)) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	wasUp := m.Up
	f(&m.Counters)
	if m.path != "" && (wasUp != m.Up || time.Since(m.last) >= 5*time.Second) {
		m.last = time.Now()
		if err := m.writeLocked(); err != nil {
			fmt.Fprintf(os.Stderr, "metrics: %v\n", err)
		}
	}
}

// Snapshot returns a copy of the counters.
func (m *Metrics) Snapshot() Counters {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Counters
}

func (m *Metrics) writeLocked() error {
	b2i := func(b bool) int {
		if b {
			return 1
		}
		return 0
	}
	unix := func(t time.Time) int64 {
		if t.IsZero() {
			return 0
		}
		return t.Unix()
	}
	var sb strings.Builder
	line := func(name string, v any) { fmt.Fprintf(&sb, "relay_events_%s{%s} %v\n", name, m.labels, v) }
	line("up", b2i(m.Up))
	line("channel_busy", b2i(m.Busy))
	line("last_exit_status", m.LastExit)
	line("sessions_total", m.Sessions)
	line("reports_total", m.Reports)
	line("gaps_total", m.Gaps)
	line("bad_lines_total", m.BadLines)
	line("unsynced_timestamps_total", m.UnsyncedStamps)
	line("reports_without_entry_id", b2i(m.NoEntryIDs))
	line("last_report_timestamp_seconds", unix(m.LastReport))
	line("last_heartbeat_timestamp_seconds", unix(m.LastHeartbeat))
	tmp := filepath.Join(filepath.Dir(m.path), "."+filepath.Base(m.path)+".tmp")
	if err := os.WriteFile(tmp, []byte(sb.String()), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, m.path)
}
