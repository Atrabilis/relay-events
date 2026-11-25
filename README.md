# relay-events

Protocol-neutral core for recording protection events (pickups, trips, breaker position, reclose...) from protection
relays and reclosers into PostgreSQL, without losing or duplicating events across outages. Protocol agents build on it:
an IEC 61850 agent (buffered report control blocks) and a DNP3 agent (outstation event buffers).

## What it provides

| Package | What it does |
|---|---|
| `listen` | `Run` supervises sessions of a `Source` against one device: stores every reported value through a `Sink`, restarts sessions with a backoff, resumes from the last stored state, records gaps when the device says events were lost, and writes Prometheus textfile metrics |
| `listen.ExecSource` | A `Source` that runs a listener program printing JSON lines (for listeners written in other languages) |
| `storage/postgres_events` | The `Sink` for PostgreSQL, one transaction per report; table layout and indexes in [`sql/events.sql`](sql/events.sql) |
| `storage/pgconn` | PostgreSQL connection from environment variables named in the configuration |
| `listentest` | An in-memory `Sink` with the same dedup rules as the table, for tests |

## Source contract

A session delivers lines in order (`hello`, optionally `resume`, `enabled`, then `report` and `heartbeat`, last `end`)
and ends with an exit status: 0 stopped on request, 1 connection error, 2 connection lost, 4 event channel in use by
another client, 5 channel does not match the configuration (`Run` stops for good), 6 the device refused to enable the
channel, 7 the channel was disabled from outside. A `report` carries `entry_id`, `seq_num`, `time_of_entry`,
`buf_ovfl` and `values` (`ref`, `reason`, `value`, where `value.t` may hold the device time stamp and its time quality).

In-process sources can set `Item.Ack`: the supervisor calls it with `true` only after everything in the line is
stored, so a source can confirm events to the device (DNP3 application confirmation) only once they are safe.

## Failure handling

- Each report is stored atomically; a session resumes after the last stored EntryID, and reports sent again are
  skipped by the unique key (epoch, EntryID, member), or (epoch, member, device time stamp, value) when the device
  sends no EntryID.
- A resume the device refuses opens a new epoch and a `gap` row; an overflow flag is a gap only when history was known.
- A session that stops sending lines (sources send heartbeats) is stopped and restarted; a store failure stops the
  session so the device keeps buffering.

## License

MIT
