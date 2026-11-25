// Package listen records protection events from a device's buffered event channel (an IEC 61850 buffered report
// control block, a DNP3 outstation's event buffer...). A Source opens sessions and delivers JSON-shaped Lines; Run
// stores them through a Sink, restarts sessions after failures and resumes from the last stored state, so nothing
// the device buffered is lost or stored twice, and records gaps when the device says events were lost.
package listen

import (
	"encoding/json"
	"fmt"
	"time"
)

// Exit statuses every Source follows (the contract of iec61850-listen, bridge/iec61850_listen.c in iec61850-agent).
const (
	ExitOK       = 0
	ExitUsage    = 1 // usage or connection error
	ExitLost     = 2 // connection lost or heartbeat read failed
	ExitBusy     = 4 // RCB enabled by another client
	ExitMismatch = 5 // RCB not found, not buffered, unexpected ConfRev, data set unreadable
	ExitEnable   = 6 // the device refused to enable the RCB
	ExitDisabled = 7 // the RCB was disabled from outside while we held it
)

// Line is one JSON line of iec61850-listen. Only the fields of its Type are set.
type Line struct {
	Type string          `json:"type"`
	Raw  json.RawMessage `json:"-"` // the line as received, for logs

	// hello
	IED      string   `json:"ied"`
	RCB      string   `json:"rcb"`
	RptID    string   `json:"rpt_id"`
	Buffered bool     `json:"buffered"`
	RptEna   bool     `json:"rpt_ena"`
	Source   string   `json:"source"`
	Dataset  string   `json:"dataset"`
	Members  []Member `json:"members"`

	// resume
	Accepted bool `json:"accepted"`
	Error    int  `json:"error"`

	// report
	ReceivedAt  string       `json:"received_at"`
	EntryID     *string      `json:"entry_id"`
	SeqNum      *int64       `json:"seq_num"`
	TimeOfEntry string       `json:"time_of_entry"`
	BufOvfl     bool         `json:"buf_ovfl"`
	Values      []ValueEntry `json:"values"`

	// hello and report
	ConfRev *int64 `json:"conf_rev"`

	// heartbeat, enabled
	At      string `json:"at"`
	Reports int64  `json:"reports"`

	// enabled: what the device applied after our writes (-1 = could not be read back)
	OptFlds          *int  `json:"opt_flds"`
	TrgOps           *int  `json:"trg_ops"`
	EntryIDInReports *bool `json:"entry_id_in_reports"`

	// end; resume: ok, refused_by_device or entry_id_ahead_of_device
	Reason string `json:"reason"`
	Exit   int    `json:"exit"`
}

// Member is one data set member as announced in hello.
type Member struct {
	Ref   string `json:"ref"`
	Typed bool   `json:"typed"`
}

// ValueEntry is one data set member included in a report.
type ValueEntry struct {
	Index   int             `json:"i"`
	Ref     string          `json:"ref"`
	Reasons []string        `json:"reason"`
	Value   json.RawMessage `json:"value"`
}

// ParseLine decodes one line.
func ParseLine(b []byte) (*Line, error) {
	var l Line
	if err := json.Unmarshal(b, &l); err != nil {
		return nil, fmt.Errorf("bad line from listener: %w", err)
	}
	l.Raw = json.RawMessage(append([]byte(nil), b...))
	switch l.Type {
	case "hello", "resume", "enabled", "report", "heartbeat", "end":
		return &l, nil
	default:
		return nil, fmt.Errorf("unknown line type %q", l.Type)
	}
}

// Kinds of stored events.
const (
	KindValue   = "value"   // one data set member of one report
	KindSession = "session" // a listening session started (the RCB was taken)
	KindGap     = "gap"     // events may be missing: buffer overflow on the device, or resume refused
)

// Event is one stored row.
type Event struct {
	Kind        string
	Epoch       int64 // increases when the device no longer knows our EntryID (buffer lost); part of the dedup key
	EntryID     string
	SeqNum      *int64
	Ref         string
	Reasons     []string
	DeviceTS    *time.Time // the member's own time stamp ("t"), set by the device when the value changed
	TimeQuality *int       // time quality byte of that time stamp (0x20 clock not synchronized, 0x40 clock failure)
	Quality     string     // "q" bit string, as the device sent it
	Value       json.RawMessage
	ReportTS    *time.Time // TimeofEntry of the report
	ConfRev     *int64
	ReceivedAt  time.Time
	Detail      map[string]any // session and gap rows
}

// Time quality flags (IEC 61850-8-1 TimeQuality).
const (
	TQClockNotSynchronized = 0x20
	TQClockFailure         = 0x40
)

// FromReport turns one report line into value events (plus a gap event when the device reports a buffer overflow).
func FromReport(l *Line, epoch int64) ([]Event, error) {
	received, err := parseTime(l.ReceivedAt)
	if err != nil {
		return nil, fmt.Errorf("report received_at: %w", err)
	}
	var reportTS *time.Time
	if l.TimeOfEntry != "" {
		if t, err := parseTime(l.TimeOfEntry); err == nil {
			reportTS = &t
		}
	}
	entry := ""
	if l.EntryID != nil {
		entry = *l.EntryID
	}
	var out []Event
	if l.BufOvfl {
		out = append(out, Event{
			Kind: KindGap, Epoch: epoch, EntryID: entry, ReceivedAt: received, ReportTS: reportTS,
			Detail: map[string]any{"reason": "buffer_overflow", "seq_num": l.SeqNum},
		})
	}
	for _, v := range l.Values {
		e := Event{
			Kind: KindValue, Epoch: epoch, EntryID: entry, SeqNum: l.SeqNum, Ref: v.Ref, Reasons: v.Reasons,
			Value: v.Value, ReportTS: reportTS, ConfRev: l.ConfRev, ReceivedAt: received,
		}
		e.DeviceTS, e.TimeQuality, e.Quality = stampAndQuality(v.Value)
		out = append(out, e)
	}
	return out, nil
}

// stampAndQuality extracts "t" ({"ts","tq"}) and "q" from a data object value; members that are a single attribute
// (e.g. stVal alone) have neither.
func stampAndQuality(raw json.RawMessage) (*time.Time, *int, string) {
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) != nil {
		return nil, nil, ""
	}
	var ts *time.Time
	var tq *int
	if rawT, ok := obj["t"]; ok {
		var t struct {
			TS string `json:"ts"`
			TQ *int   `json:"tq"`
		}
		if json.Unmarshal(rawT, &t) == nil && t.TS != "" {
			if parsed, err := parseTime(t.TS); err == nil {
				ts = &parsed
			}
			tq = t.TQ
		}
	}
	q := ""
	if rawQ, ok := obj["q"]; ok {
		_ = json.Unmarshal(rawQ, &q) // a non-string q is left empty
	}
	return ts, tq, q
}

func parseTime(s string) (time.Time, error) {
	return time.Parse("2006-01-02T15:04:05.000Z07:00", s)
}
