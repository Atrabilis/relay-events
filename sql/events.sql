-- Target table for recorded protection events (listen.Sink, storage/postgres_events): one row per reported value (an
-- IEC 61850 data set member, a DNP3 point), plus "session" rows (a listening session started) and "gap" rows (events
-- may be missing). Agents do not create tables.
--
-- epoch: increases when the device refuses to resume from our last EntryID (it restarted or its buffer wrapped).
-- Its EntryIDs may then start over, so the dedup key includes the epoch.
--
-- Indexes, chosen from the queries that use the table:
--   * PRIMARY KEY (id): logical replication identity, and insertion order.
--   * events_dedup: reports re-sent after a resume (same epoch, EntryID and member) are skipped by ON CONFLICT.
--   * events_dedup_noentry: same for devices that keep their OptFlds without EntryID: member, device time stamp and
--     value (members without their own time stamp cannot be deduplicated this way: NULLs are distinct).
--   * events_last: on start, the agent looks up the last row and the last value row of its device and source
--     (ORDER BY id DESC LIMIT 1).
--   * events_device_ts: rebuilding binary channels for a fault record reads the values of one device in a time
--     window of the device clock, plus the last value of each member before it.
--   * events_received: monitoring (what arrived recently, delays between device and edge clocks).

CREATE SCHEMA IF NOT EXISTS edge;

CREATE TABLE IF NOT EXISTS edge.example_relay_events (
  id           BIGSERIAL   PRIMARY KEY,
  plant        TEXT        NOT NULL,
  device_name  TEXT        NOT NULL,
  source       TEXT        NOT NULL,          -- event channel: an RCB (LD0/LLN0.BR.BRCB_Status), a DNP3 address...
  epoch        INTEGER     NOT NULL,
  kind         TEXT        NOT NULL CHECK (kind IN ('value', 'session', 'gap')),
  entry_id     TEXT,                          -- report EntryID, hex (IEC 61850); NULL for DNP3
  seq_num      BIGINT,
  ref          TEXT,                          -- data set member (LD0/XCBR1.Pos[ST]) or point (BI 5 SigOpen)
  reasons      TEXT[],                        -- dchg, qchg, dupd, integrity, gi (IEC 61850); event, static (DNP3)
  device_ts    TIMESTAMPTZ,                   -- the member's own time stamp (t), set by the device on change
  time_quality SMALLINT,                      -- 0x20 clock not synchronized, 0x40 clock failure
  quality      TEXT,                          -- q bit string
  value        JSONB,                         -- the member as reported (stVal, q, t, ...)
  report_ts    TIMESTAMPTZ,                   -- TimeofEntry of the report
  conf_rev     BIGINT,
  received_at  TIMESTAMPTZ NOT NULL,          -- edge clock
  detail       JSONB,                         -- session and gap rows
  ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS example_relay_events_dedup
  ON edge.example_relay_events (plant, device_name, source, epoch, entry_id, ref) WHERE kind = 'value';
CREATE UNIQUE INDEX IF NOT EXISTS example_relay_events_dedup_noentry
  ON edge.example_relay_events (plant, device_name, source, epoch, ref, device_ts, value)
  WHERE kind = 'value' AND entry_id IS NULL;
CREATE INDEX IF NOT EXISTS example_relay_events_last
  ON edge.example_relay_events (plant, device_name, source, id);
CREATE INDEX IF NOT EXISTS example_relay_events_device_ts
  ON edge.example_relay_events (plant, device_name, device_ts) WHERE kind = 'value';
CREATE INDEX IF NOT EXISTS example_relay_events_received
  ON edge.example_relay_events (plant, device_name, received_at);

-- Values of one device in a window, e.g. around a fault record:
--   SELECT device_ts, ref, value #>> '{general}' AS general, reasons, time_quality
--   FROM edge.example_relay_events
--   WHERE plant = 'p' AND device_name = 'recloser' AND kind = 'value'
--     AND device_ts BETWEEN '2026-11-19 18:21:51.6Z' AND '2026-11-19 18:21:52.7Z'
--   ORDER BY device_ts, id;
-- Gaps and sessions:
--   SELECT received_at, kind, epoch, detail FROM edge.example_relay_events
--   WHERE kind <> 'value' ORDER BY id DESC LIMIT 20;
