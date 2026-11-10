# Changelog

## 0.3.0 (2026-11-11)

Existing topics (`unicast_prefix`, `peer`, `router`, `bmp_raw`) and their
message format are unchanged. Upgrading from 0.2.0 is hitless
(`bongsudae -upgrade`), and so is going back from 0.3.0 to 0.2.0 unless a peer
is received in two or more views.

### New

- **Release binaries** : static (`CGO_ENABLED=0`) Linux amd64 and arm64 archives
  with a `SHA256SUMS` file on the GitHub Releases page. They run on any Linux
  distribution; no Go toolchain needed. The arm64 binary is built and vetted
  but not yet tested on real hardware - please report how it works for you.
- **Address families** (one Kafka topic each, created when the first route
  arrives):
  - IPv4 / IPv6 labeled unicast (SAFI 4, 6PE) -> `labeled_unicast_prefix`,
    the 32 fields of `unicast_prefix` with field 28 `labels` filled.
  - VPNv4 / VPNv6 (SAFI 128) -> `l3vpn` (OpenBMP schema).
  - IPv4 / IPv6 multicast (SAFI 2) -> `multicast_prefix` (32 fields).
  - EVPN (AFI 25 / SAFI 70) -> `evpn` (OpenBMP schema, 40 fields).
  - FlowSpec (IPv4 / IPv6 / VPN, SAFI 133 / 134) -> `flowspec`, a format of
    our own (43 fields, OpenBMP has none).
  - Other families (BGP-LS, MVPN, RT Constraint ...) are counted per family
    and dropped (`other_families` in the stat line, `/api/stats`, `/metrics`).
- **Views** : every BMP view gets its own route table - Adj-RIB-In pre / post
  policy, Adj-RIB-Out pre / post policy (RFC 8671) and Loc-RIB (RFC 9069).
  API `view` / `views` / `view=`. Loc-RIB rows are the rows whose `peer_ip`
  is `0.0.0.0`.
- **Add-Path** (RFC 7911) : one route per path ID in the route tables, API
  `path_id`, `paths_v4` / `paths_v6`.
- `bmp.rib` (`-rib`) is a list of families to keep route tables for, e.g.
  `ipv4-unicast, ipv6-unicast, vpnv4, vpnv6, ipv4-flowspec, ipv6-flowspec`
  (`none` = forward only). The 0.2 values `true` / `false` still work.
- API `family=`, `family_routes` (route tables of the other families) and
  `rule=` (FlowSpec rules).
- Connections from addresses outside `bmp.allow` are logged (once a minute per
  address).

### Changed

- **`bmp.allow` default is any address (`0.0.0.0/0, ::/0`)**, was
  `127.0.0.1, ::1`. Routers connect from outside, so the old default never
  fitted. Restrict access with `bmp.allow` or a firewall.
- Connection slots are split: connections that have not sent Initiation yet
  get 4 per address and 64 in total; routers (after Initiation) get 2,000.
  Port scanners can no longer fill the slots a reconnecting router needs.
- The first message must arrive within 10 s (was 30 s).
- Large Initiation / Peer Up information TLVs : linear time, at most 4 KB per
  TLV type (a 1 MB Initiation used to take 21 s of CPU).
- API protection : one prefix lookup collects at most 2 million rows (413
  beyond that), a limited number of concurrent table scans (503 with
  Retry-After when busy), read / write deadlines, POST needs
  `Content-Type: application/json` (415).

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
