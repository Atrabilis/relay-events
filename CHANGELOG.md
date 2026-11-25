# Changelog

## v0.1.0 (2026-11-25)

- First version, extracted from iec61850-agent's report listener and made protocol neutral: `listen` (supervisor,
  `Source` interface, `ExecSource`, line protocol, metrics `relay_events_*`), `storage/postgres_events` (column
  `source` instead of `rcb`), `storage/pgconn`, `listentest`, and per-line `Ack` for sources that confirm events to the
  device only after storing them.
