package listen

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// State is what the sink remembers between sessions.
type State struct {
	EntryID string // last stored EntryID of a value row ("" when nothing was stored yet, or the device sends none)
	Epoch   int64  // epoch of the last stored row (0 when nothing was stored yet)
}

// Sink stores events. Write must be atomic per call: either every event of a report is stored or none is, so the
// next session resumes from the last report that is really in the database.
type Sink interface {
	LastState(ctx context.Context) (State, error)
	Write(ctx context.Context, events []Event) error
}

// Options for Run.
type Options struct {
	Source     Source
	SourceName string        // e.g. an RCB reference or a DNP3 outstation address; logs and session rows
	Watchdog   time.Duration // no line for this long stops the session (default 2m); sources send heartbeats
	StopGrace  time.Duration // time a session gets to release the device after Stop before Kill (default 15s)

	BackoffMin  time.Duration // default 5s
	BackoffMax  time.Duration // default 5m
	BusyBackoff time.Duration // wait after "channel in use by another client" (default 5m)
	StableAfter time.Duration // a session longer than this resets the backoff (default 2m)

	Logf    func(format string, args ...any)
	Metrics *Metrics // optional
}

// ErrMismatch means the device or its event channel is not what the configuration expects; retrying will not help.
var ErrMismatch = errors.New("device event channel does not match the configuration")

func (o *Options) defaults() {
	if o.Watchdog <= 0 {
		o.Watchdog = 2 * time.Minute
	}
	if o.StopGrace <= 0 {
		o.StopGrace = 15 * time.Second
	}
	if o.BackoffMin <= 0 {
		o.BackoffMin = 5 * time.Second
	}
	if o.BackoffMax <= 0 {
		o.BackoffMax = 5 * time.Minute
	}
	if o.BusyBackoff <= 0 {
		o.BusyBackoff = 5 * time.Minute
	}
	if o.StableAfter <= 0 {
		o.StableAfter = 2 * time.Minute
	}
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
}

// Run listens until ctx is cancelled (returns nil) or the event channel does not match the configuration (returns an
// error wrapping ErrMismatch). Every other failure is retried with a backoff.
func Run(ctx context.Context, o Options, sink Sink) error {
	o.defaults()
	backoff := o.BackoffMin
	for {
		start := time.Now()
		res := session(ctx, o, sink)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(res.err, ErrMismatch) {
			return res.err
		}
		wait := backoff
		switch {
		case res.exit == ExitBusy:
			wait = o.BusyBackoff
			o.Metrics.set(func(m *Counters) { m.Busy = true })
		case time.Since(start) >= o.StableAfter:
			backoff = o.BackoffMin
			wait = backoff
		default:
			backoff *= 2
			if backoff > o.BackoffMax {
				backoff = o.BackoffMax
			}
		}
		o.Logf("listener stopped (%v, exit %d); retrying in %s", res.err, res.exit, wait)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
}

type sessionResult struct {
	exit int
	err  error
}

// session runs one source session from start to end.
func session(ctx context.Context, o Options, sink Sink) sessionResult {
	state, err := sink.LastState(ctx)
	if err != nil {
		return sessionResult{exit: -1, err: fmt.Errorf("reading last state: %w", err)}
	}
	epoch := state.Epoch
	if epoch == 0 {
		epoch = 1
	}
	s, err := o.Source.Start(ctx, state)
	if err != nil {
		return sessionResult{exit: -1, err: err}
	}

	ended := make(chan struct{})
	stopped := false
	stop := func(reason string) {
		if stopped {
			return
		}
		stopped = true
		o.Logf("stopping listener: %s", reason)
		stopAfter(s, o.StopGrace, ended)
	}

	var sessionErr error
	ss := &sessState{last: state, epoch: epoch}
	watchdog := time.NewTimer(o.Watchdog)
	defer watchdog.Stop()

loop:
	for {
		select {
		case <-ctx.Done():
			stop("shutdown")
			ctx = context.Background() // keep draining until the session ends
		case <-watchdog.C:
			sessionErr = fmt.Errorf("no output from listener for %s", o.Watchdog)
			stop("watchdog")
		case it, ok := <-s.Items():
			if !ok {
				break loop
			}
			if !watchdog.Stop() {
				select {
				case <-watchdog.C:
				default:
				}
			}
			watchdog.Reset(o.Watchdog)
			if stopped {
				ack(it, false) // discard: anything after stop is re-sent by the device next session
				continue
			}
			if it.Err != nil {
				o.Logf("%v", it.Err)
				o.Metrics.set(func(m *Counters) { m.BadLines++ })
				ack(it, false)
				continue
			}
			err := handle(ctx, o, sink, it.Line, ss)
			ack(it, err == nil)
			if err != nil {
				sessionErr = err
				stop(err.Error())
			}
		}
	}

	exit := s.Wait()
	close(ended)
	o.Metrics.set(func(m *Counters) { m.Up = false; m.LastExit = exit })
	if exit == ExitMismatch {
		return sessionResult{exit: exit, err: fmt.Errorf("%w (see the hello/end lines in the log)", ErrMismatch)}
	}
	if sessionErr == nil {
		sessionErr = fmt.Errorf("listener exited with status %d", exit)
	}
	return sessionResult{exit: exit, err: sessionErr}
}

func ack(it Item, ok bool) {
	if it.Ack != nil {
		it.Ack(ok)
	}
}

// sessState is what one session learns as its lines arrive.
type sessState struct {
	last  State // stored state the session started from
	epoch int64
	// historyKnown: the device's event stream continues what we already stored. False at the start of a session that
	// did not resume (first session ever, or after a refused resume); true once the device accepted our EntryID or
	// sent a report without the overflow flag. Devices raise the overflow flag while history is unknown (IEC 61850-7-2
	// BufOvfl at start-up and after an unknown EntryID; a DNP3 outstation that nobody polled), so the flag is only a
	// new gap when history was known.
	historyKnown bool
	resumed      bool
}

// handle processes one line. A returned error stops the session (the device keeps buffering meanwhile).
func handle(ctx context.Context, o Options, sink Sink, l *Line, ss *sessState) error {
	now := time.Now().UTC()
	state := ss.last
	switch l.Type {
	case "hello":
		o.Logf("hello from %s: %s", o.SourceName, l.Raw)
		for _, m := range l.Members {
			if !m.Typed {
				o.Logf("member %s: type unknown, its value is stored with positional names", m.Ref)
			}
		}
	case "resume":
		if l.Accepted {
			ss.resumed, ss.historyKnown = true, true
			return nil
		}
		// The device no longer knows our EntryID (it restarted or its buffer wrapped): what happened in between may be
		// lost, and its EntryIDs may start over, so later rows go to a new epoch to keep the dedup key unique.
		ss.epoch++
		o.Logf("device refused to resume after EntryID %s (error %d): possible gap, new epoch %d", state.EntryID, l.Error, ss.epoch)
		o.Metrics.set(func(m *Counters) { m.Gaps++ })
		return sink.Write(ctx, []Event{{Kind: KindGap, Epoch: ss.epoch, ReceivedAt: now,
			Detail: map[string]any{"reason": "resume_refused", "why": l.Reason, "last_entry_id": state.EntryID, "error": l.Error}}})
	case "enabled":
		noEntry := l.EntryIDInReports != nil && !*l.EntryIDInReports
		if noEntry {
			// Reports without EntryID cannot be resumed after an outage (the device re-sends what it still holds); they
			// are deduplicated by member, device time stamp and value instead.
			o.Logf("device does not include EntryID in reports (OptFlds %v): sessions cannot resume", deref(l.OptFlds))
		}
		o.Metrics.set(func(m *Counters) { m.Up = true; m.Busy = false; m.Sessions++; m.NoEntryIDs = noEntry })
		return sink.Write(ctx, []Event{{Kind: KindSession, Epoch: ss.epoch, ReceivedAt: now,
			Detail: map[string]any{"resumed_from": state.EntryID, "resumed": ss.resumed, "source": o.SourceName,
				"opt_flds": deref(l.OptFlds), "trg_ops": deref(l.TrgOps), "entry_id_in_reports": deref(l.EntryIDInReports)}}})
	case "report":
		all, err := FromReport(l, ss.epoch)
		if err != nil {
			o.Logf("%v", err)
			o.Metrics.set(func(m *Counters) { m.BadLines++ })
			return nil
		}
		events := make([]Event, 0, len(all))
		for _, e := range all {
			if e.Kind == KindGap {
				if !ss.historyKnown {
					// Expected flag: no history to lose (first session) or already recorded (resume refused).
					o.Logf("report %s: overflow flag while history is unknown (start of session), not a new gap", e.EntryID)
					continue
				}
				o.Logf("device reports a buffer overflow (entry %s): events were lost on the device", e.EntryID)
				o.Metrics.set(func(m *Counters) { m.Gaps++ })
			}
			events = append(events, e)
		}
		if !l.BufOvfl {
			ss.historyKnown = true
		}
		for _, e := range events {
			if e.TimeQuality != nil && *e.TimeQuality&(TQClockNotSynchronized|TQClockFailure) != 0 {
				o.Metrics.set(func(m *Counters) { m.UnsyncedStamps++ })
			}
		}
		if err := sink.Write(ctx, events); err != nil {
			return fmt.Errorf("storing report %s: %w", deref(l.EntryID), err)
		}
		o.Metrics.set(func(m *Counters) { m.Reports++; m.LastReport = now })
	case "heartbeat":
		o.Metrics.set(func(m *Counters) { m.LastHeartbeat = now })
	case "end":
		o.Logf("listener ended: %s after %d reports (exit %d)", l.Reason, l.Reports, l.Exit)
	}
	return nil
}

func deref[T any](p *T) any {
	if p == nil {
		return nil
	}
	return *p
}
