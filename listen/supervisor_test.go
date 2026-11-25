package listen_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/atrabilis/relay-events/listen"
	"github.com/atrabilis/relay-events/listentest"
)

// fakeListener writes a shell script that stands in for iec61850-listen. Invocation N runs scenarios[N-1] (the last
// one repeats); its arguments are saved to args_N. On SIGTERM it prints an end line and exits 0, like the real one.
func fakeListener(t *testing.T, scenarios ...string) (path, dir string) {
	t.Helper()
	dir = t.TempDir()
	for i, s := range scenarios {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("s_%d", i+1)), []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	script := fmt.Sprintf(`#!/bin/sh
d=%q
n=$(cat "$d/count" 2>/dev/null || echo 0); n=$((n+1)); echo $n > "$d/count"
echo "$@" > "$d/args_$n"
trap 'kill $SP 2>/dev/null; echo "{\"type\":\"end\",\"reason\":\"stopped\",\"reports\":0,\"exit\":0}"; exit 0' TERM
s="$d/s_$n"; [ -f "$s" ] || s="$d/s_%d"
emit() { printf '%%s\n' "$1"; }
. "$s"
`, dir, len(scenarios))
	path = filepath.Join(dir, "iec61850-listen")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path, dir
}

func invocations(dir string) int {
	b, _ := os.ReadFile(filepath.Join(dir, "count"))
	var n int
	_, _ = fmt.Sscanf(strings.TrimSpace(string(b)), "%d", &n)
	return n
}

func argsOf(t *testing.T, dir string, n int) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("args_%d", n)))
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

const (
	hello   = `emit '{"type":"hello","ied":"SIMIED","rcb":"LD0/LLN0.BR.BRCB_Status","rpt_id":"RP","buffered":true,"rpt_ena":false,"conf_rev":1,"dataset":"LD0/LLN0$Status","members":[{"ref":"LD0/PTRC1.Str[ST]","typed":true}]}'`
	enabled = `emit '{"type":"enabled","at":"2026-11-20T14:00:00.000Z"}'`
	// The sleep does not hold stdout: a SIGTERM that arrives before SP is set must not keep the pipe open.
	hang = `sleep 30 >/dev/null 2>&1 & SP=$!; wait`
)

func reportLine(entry string, ovfl bool, tq int) string {
	return fmt.Sprintf(`emit '{"type":"report","received_at":"2026-11-20T14:00:01.000Z","entry_id":"%s","seq_num":1,"buf_ovfl":%v,"values":[{"i":0,"ref":"LD0/PTRC1.Str[ST]","reason":["dchg"],"value":{"general":true,"q":"0000000000000","t":{"ts":"2026-11-20T14:00:00.900Z","tq":%d}}}]}'`, entry, ovfl, tq)
}

func scenario(lines ...string) string { return strings.Join(lines, "\n") + "\n" }

// execArgs builds iec61850-listen style arguments (the fake listener only records them).
func execArgs(from State) []string {
	args := []string{"-h", "127.0.0.1", "-r", "LD0/LLN0.BR.BRCB_Status", "-w"}
	if from.EntryID != "" {
		args = append(args, "-e", from.EntryID)
	}
	return args
}

func testOptions(path string) Options {
	return Options{
		Source: &ExecSource{Path: path, Args: execArgs}, SourceName: "LD0/LLN0.BR.BRCB_Status",
		Watchdog: 10 * time.Second, StopGrace: 5 * time.Second,
		BackoffMin: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond, BusyBackoff: 100 * time.Millisecond,
		StableAfter: time.Hour, Metrics: NewMetrics("", "dev", "rcb"), Logf: testLogf,
	}
}

// runUntil runs the supervisor until cond holds (then cancels) or the deadline passes.
func runUntil(t *testing.T, o Options, sink Sink, cond func() bool) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var err error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); err = Run(ctx, o, sink) }()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	ok := cond()
	cancel()
	wg.Wait()
	if !ok {
		t.Fatal("condition not reached before the deadline")
	}
	return err
}

func TestSessionStoresReportsAndStopsCleanly(t *testing.T) {
	path, dir := fakeListener(t, scenario(hello, enabled, reportLine("0a", false, 138), "emit 'garbage'", hang))
	sink := listentest.NewMemSink()
	o := testOptions(path)
	err := runUntil(t, o, sink, func() bool { return sink.Count(KindValue) == 1 })
	if err != nil {
		t.Fatal(err)
	}
	if sink.Count(KindSession) != 1 {
		t.Errorf("want one session row, got %d", sink.Count(KindSession))
	}
	args := argsOf(t, dir, 1)
	if !strings.Contains(args, "-w") || strings.Contains(args, "-e") {
		t.Errorf("first session must take the RCB without resuming: %q", args)
	}
	m := o.Metrics.Snapshot()
	if m.Reports != 1 || m.Sessions != 1 || m.BadLines != 1 {
		t.Errorf("metrics: %+v", m)
	}
}

func TestResumesFromLastStoredEntryAndDedups(t *testing.T) {
	// Session 1 stores entry 0a and loses the link; session 2 must resume after 0a, and the device re-sending 0a must
	// not create a second row.
	path, dir := fakeListener(t,
		scenario(hello, enabled, reportLine("0a", false, 138), "exit 2"),
		scenario(hello, `emit '{"type":"resume","entry_id":"0a","accepted":true,"error":0}'`, enabled,
			reportLine("0a", false, 138), reportLine("0b", false, 138), hang))
	sink := listentest.NewMemSink()
	if err := runUntil(t, testOptions(path), sink, func() bool { return sink.Count(KindValue) == 2 && invocations(dir) == 2 }); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(argsOf(t, dir, 2), "-e 0a") {
		t.Errorf("second session did not resume after 0a: %q", argsOf(t, dir, 2))
	}
	if sink.Count(KindGap) != 0 {
		t.Error("an accepted resume is not a gap")
	}
}

func TestResumeRefusedOpensNewEpochAndGap(t *testing.T) {
	path, _ := fakeListener(t,
		scenario(hello, enabled, reportLine("0a", false, 138), "exit 2"),
		scenario(hello, `emit '{"type":"resume","entry_id":"0a","accepted":false,"error":11}'`, enabled,
			reportLine("0a", false, 138), hang))
	sink := listentest.NewMemSink()
	if err := runUntil(t, testOptions(path), sink, func() bool { return sink.Count(KindValue) == 2 }); err != nil {
		t.Fatal(err)
	}
	rows := sink.Snapshot()
	var gap *Event
	epochs := map[int64]bool{}
	for i := range rows {
		if rows[i].Kind == KindGap {
			gap = &rows[i]
		}
		if rows[i].Kind == KindValue {
			epochs[rows[i].Epoch] = true
		}
	}
	if gap == nil || gap.Detail["reason"] != "resume_refused" || gap.Epoch != 2 {
		t.Fatalf("want a resume_refused gap in epoch 2, got %+v", gap)
	}
	// Same EntryID after the device restarted is a different event: both rows are kept, in different epochs.
	if !epochs[1] || !epochs[2] {
		t.Errorf("value rows must be in epochs 1 and 2: %v", epochs)
	}
}

func TestBufferOverflowIsAGapOnlyWhenHistoryWasKnown(t *testing.T) {
	// First report of a first session carries the overflow flag (the device has no history for us): not a gap. Once a
	// report without the flag arrived, a later flag means the device dropped events: a gap.
	path, _ := fakeListener(t, scenario(hello, enabled, reportLine("0b", true, 138), reportLine("0c", false, 138),
		reportLine("0d", true, 138), hang))
	sink := listentest.NewMemSink()
	o := testOptions(path)
	if err := runUntil(t, o, sink, func() bool { return sink.Count(KindValue) == 3 }); err != nil {
		t.Fatal(err)
	}
	if sink.Count(KindGap) != 1 || o.Metrics.Snapshot().Gaps != 1 {
		t.Errorf("want exactly one gap (for 0d), got %d", sink.Count(KindGap))
	}
	for _, e := range sink.Snapshot() {
		if e.Kind == KindGap && e.EntryID != "0d" {
			t.Errorf("gap on the wrong report: %s", e.EntryID)
		}
	}
}

func TestOverflowAfterAcceptedResumeIsAGap(t *testing.T) {
	path, _ := fakeListener(t,
		scenario(hello, enabled, reportLine("0a", false, 138), "exit 2"),
		scenario(hello, `emit '{"type":"resume","entry_id":"0a","accepted":true,"error":0}'`, enabled,
			reportLine("0e", true, 138), hang))
	sink := listentest.NewMemSink()
	if err := runUntil(t, testOptions(path), sink, func() bool { return sink.Count(KindValue) == 2 }); err != nil {
		t.Fatal(err)
	}
	if sink.Count(KindGap) != 1 {
		t.Errorf("overflow after a resumed session must be a gap, got %d", sink.Count(KindGap))
	}
}

func TestUnsynchronizedStampsAreCounted(t *testing.T) {
	path, _ := fakeListener(t, scenario(hello, enabled, reportLine("0d", false, TQClockNotSynchronized), hang))
	sink := listentest.NewMemSink()
	o := testOptions(path)
	if err := runUntil(t, o, sink, func() bool { return sink.Count(KindValue) == 1 }); err != nil {
		t.Fatal(err)
	}
	if o.Metrics.Snapshot().UnsyncedStamps != 1 {
		t.Error("value stamped while the device clock was not synchronized was not counted")
	}
}

func TestBusyWaitsAndRetries(t *testing.T) {
	path, dir := fakeListener(t,
		scenario(hello, `emit '{"type":"end","reason":"rcb_in_use","reports":0,"exit":4}'`, "exit 4"),
		scenario(hello, enabled, hang))
	sink := listentest.NewMemSink()
	o := testOptions(path)
	start := time.Now()
	if err := runUntil(t, o, sink, func() bool { return sink.Count(KindSession) == 1 }); err != nil {
		t.Fatal(err)
	}
	if invocations(dir) != 2 || time.Since(start) < o.BusyBackoff {
		t.Errorf("busy must wait BusyBackoff before retrying (invocations %d)", invocations(dir))
	}
}

func TestMismatchStopsForGood(t *testing.T) {
	path, dir := fakeListener(t, scenario(hello, `emit '{"type":"end","reason":"conf_rev_mismatch","reports":0,"exit":5}'`, "exit 5"))
	err := Run(context.Background(), testOptions(path), listentest.NewMemSink())
	if !errors.Is(err, ErrMismatch) {
		t.Fatalf("want ErrMismatch, got %v", err)
	}
	if invocations(dir) != 1 {
		t.Errorf("a mismatch must not be retried (%d invocations)", invocations(dir))
	}
}

func TestStoreFailureStopsAndResumesWithoutLoss(t *testing.T) {
	// The database fails on report 0b: the listener is stopped (the device keeps buffering) and the next session
	// resumes after 0a, so 0b arrives again and is stored.
	path, dir := fakeListener(t,
		scenario(hello, enabled, reportLine("0a", false, 138), reportLine("0b", false, 138), hang),
		scenario(hello, `emit '{"type":"resume","entry_id":"0a","accepted":true,"error":0}'`, enabled,
			reportLine("0b", false, 138), hang))
	sink := listentest.NewMemSink()
	writes := 0
	wrapped := &failingOn{MemSink: sink, fail: func() bool { writes++; return writes == 3 }} // session, 0a, then 0b fails
	if err := runUntil(t, testOptions(path), wrapped, func() bool { return sink.Count(KindValue) == 2 }); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(argsOf(t, dir, 2), "-e 0a") {
		t.Errorf("after the failure the session must resume after the last stored entry: %q", argsOf(t, dir, 2))
	}
}

type failingOn struct {
	*listentest.MemSink
	fail func() bool
}

func (f *failingOn) Write(ctx context.Context, ev []Event) error {
	if f.fail() {
		return errors.New("database unavailable")
	}
	return f.MemSink.Write(ctx, ev)
}

func TestWatchdogKillsASilentListener(t *testing.T) {
	path, dir := fakeListener(t, scenario(hello, "trap '' TERM", "exec sleep 30"), scenario(hello, enabled, hang))
	sink := listentest.NewMemSink()
	o := testOptions(path)
	o.Watchdog = 300 * time.Millisecond
	o.StopGrace = 500 * time.Millisecond // SIGKILL follows SIGTERM after this; the first child ignores SIGTERM
	if err := runUntil(t, o, sink, func() bool { return invocations(dir) >= 2 && sink.Count(KindSession) == 1 }); err != nil {
		t.Fatal(err)
	}
}

func TestMetricsFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "listen.prom")
	m := NewMetrics(p, `dev"x`, "LD0/LLN0.BR.B")
	SetForTest(m, func(c *Counters) { c.Up = true; c.Reports = 3 })
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{`relay_events_up{device="dev\"x",source="LD0/LLN0.BR.B"} 1`, "relay_events_reports_total", "relay_events_gaps_total"} {
		if !strings.Contains(s, want) {
			t.Errorf("metrics file lacks %q:\n%s", want, s)
		}
	}
	var nilMetrics *Metrics
	SetForTest(nilMetrics, func(c *Counters) { c.Up = true }) // must not panic
}

// testLogf prints supervisor logs when IEC61850_TEST_LOG is set.
func testLogf(format string, args ...any) {
	if os.Getenv("IEC61850_TEST_LOG") != "" {
		fmt.Fprintf(os.Stderr, "  log: "+format+"\n", args...)
	}
}

func TestDeviceWithoutEntryIDsResendsAndDedups(t *testing.T) {
	// The device kept OptFlds without EntryID: reports carry entry_id null. Nothing can be resumed, so the next
	// session starts without -e and the device re-sends its buffer; rows with a device time stamp are not stored twice.
	noEntry := `emit '{"type":"enabled","at":"2026-11-20T14:00:00.000Z","opt_flds":31,"trg_ops":19,"entry_id_in_reports":false}'`
	report := `emit '{"type":"report","received_at":"2026-11-20T14:00:01.000Z","entry_id":null,"seq_num":1,"buf_ovfl":false,"values":[{"i":0,"ref":"LD0/PTRC1.Str[ST]","reason":["dchg"],"value":{"general":true,"q":"0000000000000","t":{"ts":"2026-11-20T14:00:00.900Z","tq":138}}}]}'`
	path, dir := fakeListener(t, scenario(hello, noEntry, report, "exit 2"), scenario(hello, noEntry, report, hang))
	sink := listentest.NewMemSink()
	o := testOptions(path)
	if err := runUntil(t, o, sink, func() bool { return sink.Count(KindSession) == 2 }); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(argsOf(t, dir, 2), "-e") {
		t.Errorf("no EntryID was stored, so there is nothing to resume from: %q", argsOf(t, dir, 2))
	}
	if n := sink.Count(KindValue); n != 1 {
		t.Errorf("re-sent report stored %d times, want once", n)
	}
	if !o.Metrics.Snapshot().NoEntryIDs {
		t.Error("metric reports_without_entry_id not set")
	}
}

func TestResumeRefusedByGuardIsAGap(t *testing.T) {
	path, _ := fakeListener(t,
		scenario(hello, enabled, reportLine("0a", false, 138), "exit 2"),
		scenario(hello, `emit '{"type":"resume","entry_id":"0a","accepted":false,"error":-1,"reason":"entry_id_ahead_of_device"}'`, enabled, hang))
	sink := listentest.NewMemSink()
	if err := runUntil(t, testOptions(path), sink, func() bool { return sink.Count(KindSession) == 2 }); err != nil {
		t.Fatal(err)
	}
	for _, e := range sink.Snapshot() {
		if e.Kind == KindGap && e.Detail["why"] == "entry_id_ahead_of_device" && e.Epoch == 2 {
			return
		}
	}
	t.Errorf("guard refusal not stored as a gap in a new epoch: %+v", sink.Snapshot())
}
