package listen_test

import (
	"testing"
	"time"

	. "github.com/atrabilis/relay-events/listen"
)

func TestParseLine(t *testing.T) {
	for _, bad := range []string{"", "not json", `{"type":"surprise"}`, `[1,2]`} {
		if _, err := ParseLine([]byte(bad)); err == nil {
			t.Errorf("ParseLine(%q) accepted", bad)
		}
	}
	l, err := ParseLine([]byte(`{"type":"end","reason":"rcb_in_use","reports":0,"exit":4}`))
	if err != nil || l.Reason != "rcb_in_use" || l.Exit != 4 {
		t.Fatalf("end line: %+v %v", l, err)
	}
}

const report = `{"type":"report","received_at":"2026-11-20T14:29:55.500Z","entry_id":"000001a1f95d2b5f","seq_num":2,
"time_of_entry":"2026-11-20T14:29:55.450Z","conf_rev":1,"buf_ovfl":false,"values":[
{"i":1,"ref":"LD0/PTRC1.Str[ST]","reason":["dchg"],"value":{"general":true,"dirGeneral":0,"q":"0000000000000","t":{"ts":"2026-11-20T14:29:55.418Z","tq":138}}},
{"i":4,"ref":"LD0/GGIO1.Ind1.stVal[ST]","reason":["dchg"],"value":true},
{"i":2,"ref":"LD0/PTRC1.Op[ST]","reason":["gi"],"value":{"general":false,"q":"0000000000000","t":{"ts":"2026-11-20T14:29:50.000Z","tq":32}}}]}`

func TestFromReport(t *testing.T) {
	l, err := ParseLine([]byte(report))
	if err != nil {
		t.Fatal(err)
	}
	ev, err := FromReport(l, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(ev) != 3 {
		t.Fatalf("got %d events, want 3", len(ev))
	}
	str := ev[0]
	if str.Kind != KindValue || str.Epoch != 3 || str.EntryID != "000001a1f95d2b5f" || *str.SeqNum != 2 || str.Ref != "LD0/PTRC1.Str[ST]" {
		t.Errorf("event fields: %+v", str)
	}
	want := time.Date(2026, 11, 20, 14, 29, 55, 418e6, time.UTC)
	if str.DeviceTS == nil || !str.DeviceTS.Equal(want) || *str.TimeQuality != 138 || str.Quality != "0000000000000" {
		t.Errorf("stamp/quality: %v %v %q", str.DeviceTS, str.TimeQuality, str.Quality)
	}
	if str.ReportTS == nil || str.ReportTS.Format(time.RFC3339Nano) != "2026-11-20T14:29:55.45Z" {
		t.Errorf("report ts: %v", str.ReportTS)
	}
	// A member that is a single attribute has no time stamp or quality of its own.
	if ev[1].DeviceTS != nil || ev[1].TimeQuality != nil || ev[1].Quality != "" || string(ev[1].Value) != "true" {
		t.Errorf("single attribute member: %+v", ev[1])
	}
	if ev[2].Reasons[0] != "gi" || *ev[2].TimeQuality&TQClockNotSynchronized == 0 {
		t.Errorf("gi member: %+v", ev[2])
	}
}

func TestFromReportOverflowAndMissingFields(t *testing.T) {
	l, err := ParseLine([]byte(`{"type":"report","received_at":"2026-11-20T14:29:55.500Z","entry_id":null,"buf_ovfl":true,
"time_of_entry":"garbage","values":[{"i":0,"ref":"LD0/XCBR1.Pos[ST]","reason":[],"value":{"stVal":"01","t":"not an object"}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	ev, err := FromReport(l, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(ev) != 2 || ev[0].Kind != KindGap || ev[0].Detail["reason"] != "buffer_overflow" {
		t.Fatalf("overflow must come first as a gap row: %+v", ev)
	}
	if ev[1].EntryID != "" || ev[1].ReportTS != nil || ev[1].DeviceTS != nil {
		t.Errorf("missing fields must stay empty: %+v", ev[1])
	}
	if _, err := FromReport(&Line{Type: "report", ReceivedAt: "yesterday"}, 1); err == nil {
		t.Error("bad received_at accepted")
	}
}
