package postgres_events

// Runs only with RELAY_EVENTS_E2E_PG set to a PostgreSQL URL (a throw-away database: the test creates and drops its
// own table in schema edge), e.g. postgres://postgres:e2e@127.0.0.1:5432/postgres.

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/atrabilis/relay-events/listen"
	"github.com/atrabilis/relay-events/storage/pgconn"
)

func setup(t *testing.T) (*Writer, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("RELAY_EVENTS_E2E_PG")
	if dsn == "" {
		t.Skip("RELAY_EVENTS_E2E_PG not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	ddl, err := os.ReadFile("../../sql/events.sql")
	if err != nil {
		t.Fatal(err)
	}
	stmt := strings.ReplaceAll(string(ddl), "example_relay_events", "test_relay_events")
	if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS edge.test_relay_events"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, stmt); err != nil {
		t.Fatalf("applying sql/events.sql: %v", err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS edge.test_relay_events") })

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	pw, _ := u.User.Password()
	for k, v := range map[string]string{"T_HOST": u.Hostname(), "T_PORT": u.Port(), "T_USER": u.User.Username(),
		"T_PASS": pw, "T_DB": strings.TrimPrefix(u.Path, "/")} {
		t.Setenv(k, v)
	}
	w, err := New(ctx, "test", pgconn.Config{HostEnv: "T_HOST", PortEnv: "T_PORT", UserEnv: "T_USER",
		PasswordEnv: "T_PASS", DatabaseEnv: "T_DB", Schema: "edge", Table: "test_relay_events"},
		"p1", "recloser", "LD0/LLN0.BR.BRCB_Status")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	return w, pool
}

func value(epoch int64, entry, ref string, ts time.Time) listen.Event {
	tq := 138
	seq := int64(1)
	return listen.Event{Kind: listen.KindValue, Epoch: epoch, EntryID: entry, SeqNum: &seq, Ref: ref, Reasons: []string{"dchg"},
		DeviceTS: &ts, TimeQuality: &tq, Quality: "0000000000000", Value: json.RawMessage(`{"general":true}`),
		ReceivedAt: ts.Add(20 * time.Millisecond)}
}

func TestWriterDedupEpochAndLastState(t *testing.T) {
	w, pool := setup(t)
	ctx := context.Background()
	t0 := time.Date(2026, 11, 20, 14, 0, 0, 0, time.UTC)

	st, err := w.LastState(ctx)
	if err != nil || st.EntryID != "" || st.Epoch != 0 {
		t.Fatalf("empty table state: %+v %v", st, err)
	}
	if err := w.Write(ctx, []listen.Event{
		{Kind: listen.KindSession, Epoch: 1, ReceivedAt: t0, Detail: map[string]any{"resumed": false}},
		value(1, "0a", "LD0/PTRC1.Str[ST]", t0),
		value(1, "0a", "LD0/XCBR1.Pos[ST]", t0),
	}); err != nil {
		t.Fatal(err)
	}
	// The device re-sends 0a after a resume: skipped. 0b is new.
	if err := w.Write(ctx, []listen.Event{value(1, "0a", "LD0/PTRC1.Str[ST]", t0), value(1, "0b", "LD0/PTRC1.Str[ST]", t0.Add(time.Second))}); err != nil {
		t.Fatal(err)
	}
	st, err = w.LastState(ctx)
	if err != nil || st.EntryID != "0b" || st.Epoch != 1 {
		t.Fatalf("state after 0b: %+v %v", st, err)
	}

	// Resume refused: gap opens epoch 2; the device's EntryIDs start over, so 0a in epoch 2 is a new row.
	if err := w.Write(ctx, []listen.Event{{Kind: listen.KindGap, Epoch: 2, ReceivedAt: t0.Add(time.Minute),
		Detail: map[string]any{"reason": "resume_refused"}}}); err != nil {
		t.Fatal(err)
	}
	st, err = w.LastState(ctx)
	if err != nil || st.Epoch != 2 || st.EntryID != "" {
		t.Fatalf("after a refused resume the next session must start without EntryID in epoch 2: %+v %v", st, err)
	}
	if err := w.Write(ctx, []listen.Event{value(2, "0a", "LD0/PTRC1.Str[ST]", t0.Add(2*time.Minute))}); err != nil {
		t.Fatal(err)
	}

	var values, gaps, sessions int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE kind='value'), count(*) FILTER (WHERE kind='gap'),
count(*) FILTER (WHERE kind='session') FROM edge.test_relay_events`).Scan(&values, &gaps, &sessions); err != nil {
		t.Fatal(err)
	}
	if values != 4 || gaps != 1 || sessions != 1 {
		t.Errorf("rows: values=%d gaps=%d sessions=%d, want 4/1/1", values, gaps, sessions)
	}
	var tq int
	var q string
	var ts time.Time
	if err := pool.QueryRow(ctx, `SELECT time_quality, quality, device_ts FROM edge.test_relay_events
WHERE entry_id='0b'`).Scan(&tq, &q, &ts); err != nil || tq != 138 || q != "0000000000000" || !ts.Equal(t0.Add(time.Second)) {
		t.Errorf("stored fields: tq=%d q=%q ts=%v err=%v", tq, q, ts, err)
	}
}

func TestWriterIsAtomicPerReport(t *testing.T) {
	w, pool := setup(t)
	ctx := context.Background()
	t0 := time.Now().UTC()
	bad := value(1, "0c", "LD0/PTRC1.Op[ST]", t0)
	bad.Kind = "bogus" // violates the kind CHECK: the whole report must be rolled back
	if err := w.Write(ctx, []listen.Event{value(1, "0c", "LD0/PTRC1.Str[ST]", t0), bad}); err == nil {
		t.Fatal("expected the CHECK constraint to fail")
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edge.test_relay_events`).Scan(&n); err != nil || n != 0 {
		t.Errorf("a failed report left %d rows (err %v)", n, err)
	}
	st, _ := w.LastState(ctx)
	if st.EntryID != "" {
		t.Errorf("LastState must not point at a report that was not stored: %+v", st)
	}
}

func TestWriterDedupWithoutEntryID(t *testing.T) {
	// Devices that send no EntryID re-send their whole buffer each session: rows are matched by member, device time
	// stamp and value. A member without its own time stamp cannot be matched and is stored again.
	w, pool := setup(t)
	ctx := context.Background()
	t0 := time.Date(2026, 11, 20, 15, 0, 0, 0, time.UTC)
	noStamp := listen.Event{Kind: listen.KindValue, Epoch: 1, Ref: "LD0/GGIO1.Ind1.stVal[ST]", Value: json.RawMessage(`true`), ReceivedAt: t0}
	for i := 0; i < 2; i++ {
		if err := w.Write(ctx, []listen.Event{value(1, "", "LD0/PTRC1.Str[ST]", t0), noStamp}); err != nil {
			t.Fatal(err)
		}
	}
	var stamped, unstamped int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE device_ts IS NOT NULL), count(*) FILTER (WHERE device_ts IS NULL)
FROM edge.test_relay_events`).Scan(&stamped, &unstamped); err != nil {
		t.Fatal(err)
	}
	if stamped != 1 || unstamped != 2 {
		t.Errorf("stamped=%d (want 1), unstamped=%d (want 2)", stamped, unstamped)
	}
	st, err := w.LastState(ctx)
	if err != nil || st.EntryID != "" {
		t.Errorf("without EntryIDs there is nothing to resume from: %+v %v", st, err)
	}
}
