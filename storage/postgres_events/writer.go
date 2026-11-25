// Package postgres_events stores recorded protection events, one row per reported value (table layout in
// sql/events.sql). It implements listen.Sink for one device and event source.
package postgres_events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/atrabilis/relay-events/listen"
	"github.com/atrabilis/relay-events/storage/pgconn"
)

// Writer is one postgres_events output, bound to one plant, device and event source.
type Writer struct {
	name, fqn, plant, device, source string
	pool                             *pgxpool.Pool
}

// New connects using the environment variables named in cfg. It does not create tables.
func New(ctx context.Context, name string, cfg pgconn.Config, plant, device, source string) (*Writer, error) {
	pool, fqn, err := pgconn.Connect(ctx, name, cfg)
	if err != nil {
		return nil, err
	}
	return &Writer{name: name, fqn: fqn, plant: plant, device: device, source: source, pool: pool}, nil
}

// LastState returns the epoch of the last row and the EntryID of the last value row of this device and source.
func (w *Writer) LastState(ctx context.Context) (listen.State, error) {
	var s listen.State
	err := w.pool.QueryRow(ctx, fmt.Sprintf(`SELECT epoch FROM %s WHERE plant = $1 AND device_name = $2 AND source = $3
ORDER BY id DESC LIMIT 1`, w.fqn), w.plant, w.device, w.source).Scan(&s.Epoch)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return s, fmt.Errorf("output %q: %w", w.name, err)
	}
	var entry *string
	err = w.pool.QueryRow(ctx, fmt.Sprintf(`SELECT entry_id FROM %s
WHERE plant = $1 AND device_name = $2 AND source = $3 AND kind = 'value' AND entry_id IS NOT NULL AND epoch = $4
ORDER BY id DESC LIMIT 1`, w.fqn), w.plant, w.device, w.source, s.Epoch).Scan(&entry)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return s, fmt.Errorf("output %q: %w", w.name, err)
	}
	if entry != nil {
		s.EntryID = *entry
	}
	return s, nil
}

// Write stores all events in one transaction. Value rows already stored are skipped, so a report the device sends
// again after a resume is stored once: same epoch, EntryID and member; or, for devices that send no EntryID, same
// epoch, member, device time stamp and value.
func (w *Writer) Write(ctx context.Context, events []listen.Event) error {
	if len(events) == 0 {
		return nil
	}
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("output %q: %w", w.name, err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after Commit

	insert := fmt.Sprintf(`INSERT INTO %s (plant, device_name, source, epoch, kind, entry_id, seq_num, ref, reasons,
  device_ts, time_quality, quality, value, report_ts, conf_rev, received_at, detail)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)`, w.fqn)
	// No conflict target: either dedup index applies (with EntryID, or without it when the device does not send one).
	insertValue := insert + `
ON CONFLICT DO NOTHING`

	batch := &pgx.Batch{}
	for _, e := range events {
		var detail, value any
		if e.Detail != nil {
			b, err := json.Marshal(e.Detail)
			if err != nil {
				return fmt.Errorf("output %q: encoding detail: %w", w.name, err)
			}
			detail = json.RawMessage(b)
		}
		if len(e.Value) > 0 {
			value = e.Value
		}
		q := insert
		if e.Kind == listen.KindValue {
			q = insertValue
		}
		batch.Queue(q, w.plant, w.device, w.source, e.Epoch, e.Kind, nullString(e.EntryID), e.SeqNum, nullString(e.Ref),
			e.Reasons, e.DeviceTS, e.TimeQuality, nullString(e.Quality), value, e.ReportTS, e.ConfRev,
			e.ReceivedAt.UTC(), detail)
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("output %q: %w", w.name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("output %q: %w", w.name, err)
	}
	return nil
}

// Close releases the pool.
func (w *Writer) Close() { w.pool.Close() }

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
