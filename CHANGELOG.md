# Changelog

## 0.2.0 (2026-11-03)

Kafka topics and the default message format are unchanged. Upgrading from 0.1
is an ordinary restart (routers reconnect once); from 0.2.0 on, use
`bongsudae -upgrade`.

### New

- **Hitless upgrade** : `bongsudae -upgrade` (or SIGUSR2) hands the router
  connections, peer state and route tables to the new binary under the same pid.
  Routers do not reconnect. With 77 million routes the routers paused 5.5 - 9.5 s;
  the route tables load in the background. If anything fails, the old process
  keeps running.
- **JSON output** : `kafka.format = json` (`-format json`) writes one JSON object
  per line instead of the OpenBMP TSV, with the same topics, keys and message
  splitting.
- **Prometheus** : `GET /metrics` (no client library).
- **settled** : peers that are synced, or have been up for 5 minutes with no new
  prefix for 5 minutes - progress for routers that do not forward End-of-RIB
  (`settled` / `peers_settled` in the API).
- **Kafka key buckets** : `kafka.key_buckets = N` spreads the rows of a big peer
  over N keys by prefix, keeping the order per router + peer + prefix.
- Topic settings `kafka.partitions`, `kafka.replication`, `kafka.retention`.
- `bmp.rib = false` : forward only, no route tables (much less memory).
- BMP Statistics Reports in the peer detail; per-router one-minute statistics
  (`/api/stats?by_router=1`).
- systemd watchdog (`WatchdogSec=180` in the example unit file).

### Changed

- On SIGTERM : stop accepting, close the router connections (one router term
  65534 `Closed by collector: shutdown` per router), then flush Kafka.
- A new connection from a router that is already connected replaces the old one.
- A Kafka topic that is deleted or re-created is picked up again without a restart.
- `origin_as` of a path ending in an AS_SET of exactly one AS is that AS
  (`{64500}` -> `64500`); sets of two or more ASes stay as text.
- RFC fixes : trailing bits after the NLRI prefix length are cleared (RFC 4271 4.3);
  AS4_AGGREGATOR from 2-octet AS peers (RFC 6793 4.2.3).
- Row building rewritten : about 40 % less processing time and 60 % fewer
  allocations, byte-identical output.
- `/api/stats` topic counts use the keys `msgs` / `rows` (were `Msgs` / `Rows`).
- Invalid `limit`, `as` or `state` query values return 400 instead of being ignored.
- Logs, API errors and command line help are in English.

## 0.1.0 (2026-11-01)

First public release.
