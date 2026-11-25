package listen

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"syscall"
	"time"
)

// A Source opens sessions against one device. Each session takes the device's event channel (a buffered report
// control block, a DNP3 master association...), resumes after the given stored state when it can, delivers Lines
// until it ends, and ends with an exit status from the contract below (ExitOK, ExitLost, ExitBusy, ...).
type Source interface {
	Start(ctx context.Context, from State) (Session, error)
}

// Session is one running session.
type Session interface {
	// Items delivers the session's lines in order; it is closed when the session has ended.
	Items() <-chan Item
	// Stop asks the session to release the device and end. It may be called more than once.
	Stop()
	// Kill ends the session at once (after Stop did not work).
	Kill()
	// Wait returns the exit status once Items is closed.
	Wait() int
}

// Item is one line, or a line that could not be decoded.
type Item struct {
	Line *Line
	Err  error
	// Ack, when set, is called once the supervisor is done with the line: ok is true only if everything the line
	// carried is stored. In-process sources use it to confirm events to the device only after they are stored (DNP3
	// application confirmation); sources that resume by EntryID leave it nil.
	Ack func(ok bool)
}

// ExecSource runs a listener program that prints JSON lines on stdout and follows the exit status contract (for
// example iec61850-listen). SIGTERM asks it to release the device and stop.
type ExecSource struct {
	Path string
	// Args builds the command line for a session that resumes after `from`.
	Args func(from State) []string
	Logf func(format string, args ...any)
}

// Start runs the program.
func (s *ExecSource) Start(_ context.Context, from State) (Session, error) {
	logf := s.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	cmd := exec.Command(s.Path, s.Args(from)...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting %s: %w", s.Path, err)
	}
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			logf("listener: %s", sc.Text())
		}
	}()
	es := &execSession{cmd: cmd, items: make(chan Item, 64), done: make(chan struct{})}
	go func() {
		defer close(es.items)
		r := bufio.NewReaderSize(stdout, 1<<20)
		for {
			b, err := r.ReadBytes('\n')
			if len(b) > 0 {
				l, perr := ParseLine(b)
				es.items <- Item{Line: l, Err: perr}
			}
			if err != nil {
				if !errors.Is(err, io.EOF) {
					logf("reading listener output: %v", err)
				}
				return
			}
		}
	}()
	return es, nil
}

type execSession struct {
	cmd   *exec.Cmd
	items chan Item
	done  chan struct{}
}

func (e *execSession) Items() <-chan Item { return e.items }
func (e *execSession) Stop()              { _ = e.cmd.Process.Signal(syscall.SIGTERM) }
func (e *execSession) Kill()              { _ = e.cmd.Process.Kill() } // harmless if it already exited

func (e *execSession) Wait() int {
	err := e.cmd.Wait()
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	if err != nil {
		return -1
	}
	return 0
}

// stopAfter asks a session to stop and kills it if it has not ended after grace.
func stopAfter(s Session, grace time.Duration, ended <-chan struct{}) {
	s.Stop()
	go func() {
		select {
		case <-ended:
		case <-time.After(grace):
			s.Kill()
		}
	}()
}
