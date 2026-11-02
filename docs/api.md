# Bongsudae HTTP API

The HTTP API served by Bongsudae, a BMP (RFC 7854) collector.
It shows the state of routers and peers, lets you search the per-peer route tables
the collector keeps in memory, and manages the Kafka forwarding rules.

The running collector serves this document itself at `GET /api/doc`
(the copy built into the binary, so it always matches the API that is running).

- Base URL: set with `-http` (config `http.listen`), for example `http://127.0.0.1:5001` (local only)
  or `:5001` (all interfaces). The API is off when this is empty.
- Access control: use a host firewall. In addition, `-http-allow` (config `http.allow`) can list allowed
  addresses / prefixes (comma separated); requests from anywhere else get 403. Empty = allow all.
- Responses: JSON (UTF-8, indented). Errors are a single line of `text/plain` plus a status code.
- Times: RFC 3339 in the server's local time zone, e.g. `2026-10-29T14:52:36+09:00`. Missing times are `""`.
- Addresses: IPv4 `192.0.2.20`, IPv6 in canonical form `2001:db8::20:63`. Put them in the path as they are
  (`:` does not need escaping).
- At most 4 requests are processed at once; further requests wait. Large lists can take a few seconds.

Contents: 1. Concepts, 2. Routers and collector status, 3. Peers, 4. Routes (prefixes),
5. Kafka forwarding rules, 6. Errors, 7. Command-line client, 8. Notes for integrators.

## 1. Concepts

- **router**: a router that sends BMP to the collector. Key = router IP.
- **peer**: a BGP neighbor of a router. Key = (router IP, peer IP).
  - BMP only reports peers that reach Established. A peer that is configured but has never come up is invisible.
  - Peers that go down stay in the list (until the collector restarts).
- **route counts (routes_v4 / routes_v6)** = number of prefixes currently alive.
  - Several adds of the same prefix count as 1; a withdraw makes it 0. When the peer goes down, sends a new
    Peer Up, or the router connection ends, the peer's count becomes 0.
  - For a peer with `peer_up: false` the collector did not see the initial table transfer, so the count may be
    too low (common with `-input kafka`, which starts in the middle of a stream; shown in `warn`).
- **Kafka forwarding rules**: which topics (zero or more) receive the unicast_prefix messages. See section 5.

## 2. Routers and collector status

### GET /api/routers
All routers, sorted by address. Array.

```json
[
 {
  "ip": "192.0.2.20",
  "name": "edge1",
  "descr": "Cisco IOS XR Software ...",
  "state": "up",
  "since": "2026-10-29T14:36:04+09:00",
  "last_msg": "2026-10-29T16:05:34+09:00",
  "last_msg_ago_s": 0,
  "connects": 3,
  "msgs": {"route_mon": 222178, "peer_up": 9, "init": 1, "stats": 120},
  "errors": 0,
  "peer_up_no_open": 0,
  "peers_up": 9,
  "peers_down": 0,
  "routes_v4": 30,
  "routes_v6": 37960,
  "peers_synced": 9,
  "peers_settled": 9,
  "kafka": ["unicast_prefix"],
  "kafka_from": "default",
  "warn": ["reconnected 2 times"]
 }
]
```

| Field | Meaning |
|---|---|
| ip | Router IP (the key) |
| name / descr | sysName / sysDescr from the BMP Initiation message. Empty if the collector has not seen an Initiation (common with `-input kafka`). `descr` is omitted when empty |
| state | `up` / `down` (the BMP session) |
| since | When the router became up or down |
| last_msg, last_msg_ago_s | Time of the last BMP message, and seconds since then |
| connects | Number of Initiation messages since the collector started (reconnects = connects - 1) |
| msgs | Count per BMP message type (`route_mon` `stats` `peer_down` `peer_up` `init` `term` `route_mirror` `unknown`; zero counts are omitted) |
| errors | Number of messages that failed to parse |
| peer_up_no_open | Number of Peer Up messages without the OPEN messages (an RFC 7854 violation on the router side) |
| peers_up / peers_down | Number of peers in each state |
| routes_v4 / routes_v6 | Sum of the route counts of all peers of this router |
| peers_synced | Number of up peers whose initial table transfer is complete (see `synced` below). Progress after a reconnect / reset: peers_synced / peers_up |
| peers_settled | Number of up peers that are `settled` (see below). Progress for routers that do not forward End-of-RIB |
| last_reset, last_reset_why | Time and reason of the last time the collector closed the session (reset API). Omitted if none |
| last_term | Why the session last ended, e.g. `term 0 ...`, `connection closed`. Omitted if none |
| kafka, kafka_from | unicast_prefix topics for this router (router level) and where that comes from: `router` / `default`. `[]` = not sent |
| warn | Warning strings (omitted if none): `down`, `silent N min` (up but no message for more than 10 minutes), `reconnected N times`, `errors N`, `peer up without OPEN N`, `peers down N` |

### GET /api/routers/{ip}
One router object as above, plus `"peers": [...]` (peer objects as in section 3, without `detail`).
404 if the router is unknown.

### GET /api/stats
Is the collector healthy? The 1-minute statistics (the same numbers as the `stat ...` lines in the log) for the
last 60 minutes, plus release, memory and Kafka state. History is lost when the collector restarts
(a hitless upgrade keeps it, and `started`).

```json
{"release": "0.2.0", "version": "1a2b3c4", "started": "2026-10-30T20:58:01+09:00", "uptime_s": 3600,
 "heap_mb": 9800, "sys_mb": 14500, "gc_cycles": 210, "goroutines": 60,
 "kafka": {"fails_total": 0, "last_ok": "2026-10-30T21:58:01+09:00"},
 "handover": {"supported": true, "count": 1, "last_at": "2026-11-05T10:00:03+09:00", "from": "0.2.0 1a2b3c4",
              "pause_ms": 5400, "rib_loading": false, "rib_load_ms": 12400},
 "minutes": [
  {"time": "2026-10-30T21:58:01+09:00", "routers": 25,
   "counts": {"route_mon": 51232, "stats": 66, "errors": 0, "others": 0, "add": 75008, "del": 15124, "msgs": 51232,
              "router_msgs": 0, "peer_msgs": 0, "not_sent": 0, "peer_up_no_open": 0},
   "raw": 51232, "kafka_fail_total": 0, "topics": {"unicast_prefix": {"msgs": 51232, "rows": 90132}}}
 ]}
```

| Field | Meaning |
|---|---|
| release | Release number of the binary, e.g. `0.2.0` (same as the Git tag `v0.2.0`; development builds end in `-dev`) |
| version | git revision of the binary (with `-modified` if the tree had local changes at build time) |
| started, uptime_s | Start time of the collector, seconds since then |
| heap_mb / sys_mb | Go heap / memory obtained from the OS (close to RSS) |
| gc_cycles, goroutines | Go runtime counters |
| kafka | Produce failures since start (`fails_total`), time of the last success (`last_ok`) and last failure (`last_fail`, omitted if none). Absent with file input |
| minutes | Oldest first, at most 60 entries. `time` = end of that minute, `routers` = known routers (including down) |
| counts | BMP type counts (`route_mon`, `peer_up`, ...) are omitted when zero; the other keys are always present |
| raw | Messages sent to the bmp_raw topic in that minute (tcp input only) |
| kafka_fail_total | Produce failures since start, as of that minute |
| topics | Messages / rows per unicast_prefix topic (forwarding rules): `{"msgs": N, "rows": N}` (0.1 used the keys `Msgs` / `Rows`) |
| handover | (0.2.0+, tcp input only = this release can do a hitless upgrade; missing = 0.1, `-upgrade` will not work) supported, count (upgrades since the last restart), last_at / from (time of the last upgrade and the release it came from), pause_ms (how long reading from routers was paused), rib_loading (route tables still loading in the background: route counts / lookups are incomplete), rib_load_ms, last_error / last_error_at (why the last attempt gave up; the old process kept running). `bongsudae -upgrade` reads this |
| by_router | (0.2.0+, only with `?by_router=1`) router address -> counts in the same form, only for routers that had messages or errors in that minute |

What to watch: whether `counts.errors` or `kafka.fails_total` grows, whether `kafka.last_ok` has stopped for
more than a minute, and `raw` ≈ `counts.route_mon` + the other BMP message counts.

### GET /metrics

The same numbers in the Prometheus text exposition format 0.0.4 (`Content-Type: text/plain; version=0.0.4`), without a client library.
It is outside `/api/` but served on the same address, with the same `http.allow` and concurrency limit.
Scrape config example: `scrape_configs: [{job_name: bongsudae, static_configs: [{targets: ["collector.example:5001"]}]}]`

| Name | Type | Labels | Meaning |
|---|---|---|---|
| bongsudae_info | gauge | release, version | always 1 |
| bongsudae_start_time_seconds | gauge | | Unix time of the first start (kept across `-upgrade`) |
| bongsudae_router_up | gauge | router, name | 1 if the BMP session is up |
| bongsudae_router_last_message_age_seconds | gauge | router | seconds since the last BMP message (-1 = none yet) |
| bongsudae_router_connects_total | counter | router | Initiation messages received |
| bongsudae_router_bmp_messages_total | counter | router, type | BMP messages by type (`route_mon`, `peer_up`, ...) |
| bongsudae_router_errors_total | counter | router | messages that could not be parsed |
| bongsudae_router_peer_up_no_open_total | counter | router | Peer Up messages without the OPENs |
| bongsudae_peers | gauge | router, state | state = `up` / `down` / `synced` / `settled` |
| bongsudae_routes | gauge | router, afi | prefixes in the route tables (`4` / `6`) |
| bongsudae_rows_total | counter | action | unicast_prefix rows (`add` / `del`) |
| bongsudae_rows_not_sent_total | counter | | rows not sent because of forwarding rules |
| bongsudae_kafka_messages_total | counter | kind | produced messages (`unicast_prefix`, `router`, `peer`, `bmp_raw`) |
| bongsudae_other_afi_total | counter | | MP_REACH / MP_UNREACH of other address families (counted, dropped) |
| bongsudae_kafka_fails_total | counter | | Kafka produce failures (absent with file input) |
| bongsudae_kafka_last_ok_time_seconds | gauge | | Unix time of the last successful produce |
| go_memstats_heap_alloc_bytes, go_memstats_sys_bytes, go_gc_cycles_total, go_goroutines | | | Go runtime (same names as the Prometheus Go client) |

- Per-router values are computed per request (same as `/api/routers`). Global totals are sums of the one-minute statistics, so they grow once a minute.
- After a restart or `-upgrade` the `_total` counters start from 0 (Prometheus `rate()` handles counter resets).
- Alert examples: `bongsudae_router_up == 0`, `increase(bongsudae_router_errors_total[10m]) > 0`, `time() - bongsudae_kafka_last_ok_time_seconds > 120`

## 3. Peers

### GET /api/peers
Peers of all routers (by router, then by peer address). Array. Query parameters (all optional):

| Parameter | Meaning |
|---|---|
| router=IP | Only this router |
| state=up / down | Only this state (any other value is a 400) |
| as=N | Only this peer AS (not a number: 400) |
| warn=1 | Only peers with a warning |

```json
[
 {
  "router": "192.0.2.50",
  "router_name": "edge2",
  "peer": "198.51.100.170",
  "rd": "0:0",
  "as": 64501,
  "bgp_id": "198.51.100.36",
  "type": "global",
  "state": "up",
  "since": "2026-10-29T14:54:21+09:00",
  "peer_up": true,
  "synced": true,
  "eor_v4": "2026-10-29T14:54:40+09:00",
  "routes_v4": 4779,
  "routes_v6": 0,
  "last_update": "2026-10-29T15:53:14+09:00",
  "adds": 4787,
  "dels": 1,
  "kafka": ["unicast_prefix"],
  "kafka_from": "default"
 }
]
```

| Field | Meaning |
|---|---|
| router, router_name | Router IP, its sysName |
| peer, rd, as, bgp_id | Peer IP, route distinguisher (`0:0` for global), peer AS, BGP identifier |
| type | Per-peer header type: `global` / `rd` / `local` / `loc-rib` |
| state, since | `up` / `down`, and when it became so |
| peer_up | Whether a Peer Up was received in the current BMP session (if true, the route count is exact from the start) |
| synced | Initial table transfer complete = an End-of-RIB was received for every unicast family (IPv4 / IPv6) negotiated by both OPENs. False without a Peer Up or without the OPENs |
| settled | `synced`, or the peer has been up for 5 minutes and no new prefix arrived for 5 minutes (changes and withdrawals do not count). For routers that do not forward End-of-RIB over BMP. Only up peers with a Peer Up. Right after `-upgrade`, peers up for more than 5 minutes are settled at once (the time of the last new prefix is not handed over) |
| eor_v4 / eor_v6 | When End-of-RIB was received (omitted if not). Observed: 19 s (3,937 messages) and 225 s (5,525 messages) after Peer Up |
| routes_v4 / routes_v6 | Number of prefixes currently alive |
| last_update | Time of the last UPDATE (omitted if none) |
| adds / dels | add / del rows received since this peer came up (not the same as the route count) |
| down_reason | Why the peer went down (may be omitted), e.g. `reason 1 (local NOTIFICATION), notification 6/6 Other Configuration Change`, `router connection closed`, `new BMP session` |
| kafka, kafka_from | unicast_prefix topics for this peer and where that comes from: `peer` / `router` / `default` |
| warn | Omitted if none: `down`, `no Peer Up (route count may be incomplete)`, `Peer Up without OPEN` |

### GET /api/routers/{ip}/peers/{peer}
One peer plus `"detail"`. 404 if the router or peer is unknown.
(If the same peer address exists in several route distinguishers, the global one, `0:0`, is chosen.)

```json
 "detail": {
  "pre_policy": true,
  "adj_rib_in": true,
  "as4": true,
  "local_ip": "198.51.100.171",
  "local_port": 179,
  "remote_port": 45618,
  "hold_sent": 90,
  "hold_recv": 45,
  "sent_caps": "MPBGP (1) : afi=1 safi=1 : Unicast IPv4, Route Refresh Old (128), Route Refresh (2), Graceful Restart (64), 4 Octet ASN (65)",
  "recv_caps": "4 Octet ASN (65), MPBGP (1) : afi=1 safi=1 : Unicast IPv4, Graceful Restart (64)",
  "history": [
   "2026-10-29T14:54:21+09:00 up (Peer Up)",
   "2026-10-29T14:54:40+09:00 End-of-RIB IPv4 (routes 4779)"
  ]
 }
```
- `pre_policy`, `adj_rib_in`, `as4`: from the per-peer header flags.
- `sent_caps` = capabilities in the OPEN the router sent, `recv_caps` = capabilities in the OPEN the peer sent.
  Empty if no Peer Up was received (ports and hold times are then 0).
- `history`: the last 10 events, oldest first: `up (Peer Up)`, `seen in route monitoring (no Peer Up)`,
  `End-of-RIB IPv4 (routes N)`, `End-of-RIB IPv6 (routes N)`, `down : ...`.
- `bmp_stats` / `bmp_stats_at` (0.2.0+, inside `detail`, omitted if none received): the last BMP Statistics Report (RFC 7854 4.8) the router sent for this peer. Names follow the IANA types (`rejected_prefixes`, `duplicate_prefix_advertisements`, `adj_rib_in_routes`, `loc_rib_routes`, `updates_treat_as_withdraw`, ...); per address family values get `_<afi>_<safi>` (e.g. `adj_rib_in_routes_1_1` = IPv4 unicast); unknown types are `type_N`. Only for known peers (a Stats Report never creates a peer).

### POST /api/routers/{ip}/reset (key required)
The collector closes the router's BMP session (TCP). The router reconnects and sends Initiation, Peer Up for
every peer, and the full table again. Use it when you want to reload a router's routes from scratch
(for example when the route counts look wrong).

- Headers: `X-API-Key` (the key, see section 5) and `X-Actor` (recommended). No body.
- **tcp input only.** With `-input kafka` or `-input file` the answer is 409.
- 404 if the router is not connected. **Once per 10 minutes per router**: otherwise 429 with a
  `Retry-After: <seconds>` header.
- On close, a router term message goes to Kafka `<prefix>.router` (code 65534, reason
  `Closed by collector: api reset actor=<X-Actor>`), all peers of the router go down (route count 0),
  and a line is written to the collector log.
- When the router reconnects depends on **the router's configuration** (one router in testing reconnected about
  14 s after the close, then retried every 5 minutes). Until then there are no routes from that router; while it
  re-sends its table, its add rows go to Kafka in a burst.
- Follow progress with `state` (up), `peers_up` and `peers_synced` of `/api/routers/{ip}`.
  A user interface should ask for confirmation and show a warning before calling this.

```json
{"router": "192.0.2.20", "closed": true, "at": "2026-10-29T20:21:29+09:00",
 "why": "Closed by collector: api reset actor=alice", "note": "..."}
```
`note` is a human-readable explanation.

Reloading a single neighbor is not possible through BMP (BMP is one-way: the router ignores anything the collector
sends). A soft refresh on the router (`clear bgp ... <neighbor> soft in`) makes the router re-send that neighbor's
routes (the peer's `recv_caps` should include `Route Refresh (2)`; check per platform). That command has to be
issued on the router by your own tooling; the collector has no access to the routers.

## 4. Routes (prefixes)

One route row (JSON):

```json
{
 "router": "192.0.2.28",
 "router_name": "edge3",
 "peer": "198.51.100.66",
 "peer_as": 64501,
 "prefix": "203.0.113.0/24",
 "as_path": "64501 64502 64503",
 "origin_as": 64503,
 "nexthop": "198.51.100.66",
 "origin": "igp",
 "med": 5148,
 "local_pref": 0,
 "communities": "64501:420 64501:1008 64501:2000",
 "ext_communities": "",
 "large_communities": "",
 "cluster_list": "",
 "originator_id": "",
 "aggregator": "64503 203.0.113.1",
 "since": "2026-10-29T14:53:02+09:00"
}
```
- Fields with empty strings may be left out of the JSON (communities through aggregator).
- `as_path` is the AS path text without the leading space used in the Kafka messages.
- If the path ends with an AS_SET of two or more ASes: `origin_as` is 0 and `origin_as_set` holds the set text (`"{64510,64511}"`). A set of one AS (`{64512}`) gives `origin_as` 64512 and no `origin_as_set` (0.2.0+).
  The set in `as_path` is written `{64510,64511}` as well (RFC style; openbmpd wrote `{ a b }`).
  In `format=text` the origin_as column contains the set text. For the origin filter the origin is "unknown"
  (such rows are kept by `not_origin`).
- `since` = when this route was last added.
- Order: by router, peer, then prefix.

### Lists: GET /api/routers/{ip}/peers/{peer}/prefixes, GET /api/routers/{ip}/prefixes
All routes of one peer / of one router (all its peers). **Can be very large** (a full-table peer is about
1.2 million rows). 404 if the router / peer is unknown.

| Parameter | Meaning |
|---|---|
| afi=4 / 6 | IPv4 / IPv6 only |
| p= / match= | Filter by prefix / address, with the same meaning as in "Search" below (if p is a prefix the default match is exact, if an address it is longest; `longest` gives one row per peer). match without p is 400 |
| origin_as=N[,N...] / not_origin=N[,N...] | Origin filter (see "Origin filter" below) |
| path_as=N[,N...] | Rows whose as_path contains one of these ASes. ASes inside an AS_SET (`{a,b}`) and the origin are part of as_path and match. Rows with an empty as_path (iBGP) never match |
| offset=N | Skip the first N rows after filtering (order unchanged) |
| limit=N | Then return at most N rows (a user interface should always set this) |
| format=text | Tab-separated text (first line = header). For downloads; smaller and faster than JSON |
| distinct=1 | (Router list only) Distinct prefixes and the number of peers that have each: `[{"prefix": "203.0.113.0/24", "peers": 2}]`, in text `prefix\tpeers`. Rows are selected by the filters above (afi, p, origin, path_as), grouped by prefix, and offset / limit apply to the grouped prefixes. X-Total = number of distinct prefixes; X-Applied ends with `distinct` |

- Filter order: afi -> p (match) -> origin_as / not_origin -> path_as -> offset -> limit.
- Response headers (both JSON and text):
  - `X-Total` = number of rows after filtering (afi, p, origin, path_as), before offset / limit.
    There is a next page if offset + rows received < X-Total.
  - `X-Applied` = names of the parameters applied to this request, comma separated
    (e.g. `p,match,path_as,offset,limit,format`). Unknown parameters are silently ignored, so a name missing here
    means this version does not know that parameter.
- Invalid values (negative or non-numeric offset / limit, bad p, match, path_as, origin_as / not_origin) give 400.
- With limit, the table is scanned once (memory = limit rows). Without limit, rows are counted first and then
  scanned again for output, so UPDATEs arriving in between can make X-Total differ slightly from the rows received.
- Paging with offset reads the table again for each request: if routes change between pages, rows can shift or
  repeat.

JSON is an array of route rows (one object per line). Text columns:
`router router_name peer peer_as prefix as_path origin_as nexthop origin med local_pref communities ext_communities large_communities since`

### Search: GET /api/prefix (all routers), GET /api/routers/{ip}/prefix (one router)

| Parameter | Meaning |
|---|---|
| p= | Prefix `203.0.113.0/24` or address `203.0.113.10` (an address is taken as /32 or /128 and the default match is longest). Required (missing or invalid = 400) |
| match= | `exact` (default) that prefix / `longest` the longest prefix covering it (one per peer) / `covering` all prefixes covering it (the prefix itself and shorter ones) / `more` the prefix and all more-specific prefixes inside it |
| origin_as=N[,N...] | Only these origins |
| not_origin=N[,N...] | Exclude these origins (hijack check) |
| limit=N | First N rows only (applied after the filters). Not a number: 400 |
| format=text | Tab-separated text, same columns as the lists. total is in the `X-Total` response header |

```json
{"query": "203.0.113.0/24", "match": "more", "not_origin": [64500], "count": 500, "total": 12345, "routes": [ route rows ... ]}
```
- `count` = rows returned (after limit), `total` = rows after the filters, before limit (count < total means truncated).
- `origin_as` / `not_origin` appear in the response only when given.
- Hijack check example: `p=<your prefix>&match=more&not_origin=<your AS>` = rows inside your prefix whose origin
  is not your AS.
- 404 if the router (`/api/routers/{ip}/prefix`) is unknown. `/api/prefix` with no routers yet returns an empty result.

### Origin filter (search, lists and batch search)
- `origin_as` and `not_origin` cannot be used together (400). ASes are decimal (0 to 4294967295), comma separated.
  Bad format = 400.
- The filter is applied first and limit afterwards, so suspicious rows cannot hide behind the limit.
- How the origin of a row is decided:
  - Normally: the last AS in as_path (= the origin_as field).
  - as_path ends with an AS_SET of two or more ASes (`... {a,b}`; a single-AS set `{a}` means a): **origin unknown** -> not excluded by not_origin (kept as a
    suspicious row), never matches origin_as.
  - as_path is empty (iBGP, a route originated inside the local AS; origin_as field is 0): origin = the peer's AS (peer_as).

### Batch search: POST /api/prefix (all routers), POST /api/routers/{ip}/prefix (one router)
Many prefixes at once, for example to check all of your registered prefixes. It is a read, so no key is needed.
It counts as one request against the concurrency limit (4).

```json
{"p": ["203.0.113.0/24", "2001:db8::/32", "192.0.2.10"], "match": "more", "limit": 100, "not_origin": [64500]}
```
| Field | Meaning |
|---|---|
| p | List of prefixes or addresses, 1 to 5000 (otherwise 400). If any entry is invalid the answer is 400, naming the entry: index (from 0), position (from 1) and the value |
| match | Same match for all. If absent, each entry uses its default (prefix exact, address longest) |
| limit | First N rows per prefix (0 / absent = all) |
| origin_as / not_origin | As in the origin filter above (arrays of numbers) |

Unknown fields in the body, or a body that is not valid JSON, give 400. The body may be at most 2 MB.

Response (`results` in request order; a prefix given twice appears twice):
```json
{"match": "more", "not_origin": [64500], "count": 3,
 "results": [
  {"query": "203.0.113.0/24", "match": "more", "not_origin": [64500], "count": 3, "total": 3, "routes": [ route rows ... ]},
  {"query": "2001:db8::/32", "match": "more", "not_origin": [64500], "count": 0, "total": 0, "routes": []}
 ]}
```
- Top-level `count` = sum of the route rows in all results. Each result's `count` / `total` mean the same as in GET.
  Top-level `match` is omitted if it was not given.
- Each peer's table is scanned only once, for all prefixes together.
- Speed (synthetic test, 4 full-table peers = 4.8 million routes): 5000 prefixes with `more` 1.7 s, `exact` 0.04 s,
  `longest` 0.05 s. A single GET with `more` takes 0.1 to 0.2 s. During a search, processing for each peer waits
  briefly (about 0.4 s for one full-table peer), so run batch searches a few minutes apart at most
  (do not run your whole prefix list every time a page is opened).

## 5. Kafka forwarding rules

With many routers, Kafka and its consumers can become a burden. The rules choose, per router / per peer, which
topics receive the unicast_prefix messages. router / peer messages (topics `<prefix>.router`, `<prefix>.peer`) are
always sent, regardless of the rules.

- Levels: **peer rule -> router rule -> default** (the most specific wins).
- Value = list of topic names. A name starts with `unicast_prefix` followed by up to 64 characters from
  `A-Z a-z 0-9 _ . -`. The real topic is `<prefix>.<name>` (prefix = `-prefix`, config `kafka.prefix`,
  default `openbmp.parsed`). A topic used for the first time is created by the collector
  (8 partitions, replication 1, retention 90 minutes). Duplicate names in a list are removed.
- With several topics, the same message is sent to each.
- `[]` = **do not send** (a rule exists). **No rule** (null / DELETE) = follow the level above.
- Deleting a router rule keeps that router's peer rules. The default cannot be deleted (PUT only).
- Changes apply to UPDATEs that arrive afterwards. The current table is not re-sent to a new topic.
- Rules file: `-rules` (config `paths.rules`, default `conf/kafka_rules.json`). It is read at start and written on
  every change. If the file was edited by hand and not yet reloaded, PUT / DELETE give 409.

### GET /api/kafka/rules
```json
{
 "file": "conf/kafka_rules.json",
 "loaded": "2026-10-29T14:36:05+09:00",
 "prefix": "openbmp.parsed",
 "rules": {
  "default": ["unicast_prefix"],
  "routers": {
   "192.0.2.31": {"topics": []},
   "192.0.2.95": {
    "topics": ["unicast_prefix1", "unicast_prefix2"],
    "peers": {"2001:db8::20:63": ["unicast_prefix2"]}
   },
   "192.0.2.48": {"peers": {"198.51.100.1": []}}
  }
 },
 "topics": [
  {"topic": "unicast_prefix", "msgs_1m": 67248, "rows_1m": 118063, "in_rules": true}
 ]
}
```
- If `rules.routers.{ip}.topics` is **absent**, there is no router rule (192.0.2.48 above has peer rules only).
- `topics`: messages / rows sent per topic in the last minute. `in_rules: false` = no longer in the rules but
  something was sent to it in the last minute.

### GET /api/kafka/rules/default, /api/kafka/rules/routers/{ip}, /api/kafka/rules/routers/{ip}/peers/{peer}
```json
{"router": "192.0.2.95", "peer": "2001:db8::20:63", "rule": null, "effective": ["unicast_prefix1", "unicast_prefix2"], "from": "router"}
```
- `rule` = the value written at that level (null if none), `effective` = what actually applies,
  `from` = `peer` / `router` / `default`.
- A router query also includes `"peers": {peer: [...]}` (that router's peer rules). A router / peer with no rule
  still returns 200 (`rule` null). An invalid address in the path is 400.

### Changing rules (key required)
Header `X-API-Key: <key>` is required. The key is the content of the token file (`-token`, config `paths.token`,
default `conf/api_token`; surrounding whitespace ignored; at least 16 characters). The file is read on every
request, so a new key takes effect at once. If the file is missing or shorter than 16 characters, all changing
requests are refused with 403.

| Request | Body | Meaning |
|---|---|---|
| PUT /api/kafka/rules/default | `{"topics": [...]}` | Change the default |
| PUT /api/kafka/rules/routers/{ip} | `{"topics": [...]}` | Add / change a router rule |
| PUT /api/kafka/rules/routers/{ip}/peers/{peer} | `{"topics": [...]}` | Add / change a peer rule |
| DELETE /api/kafka/rules/routers/{ip} | - | Delete a router rule (the router follows the default) |
| DELETE /api/kafka/rules/routers/{ip}/peers/{peer} | - | Delete a peer rule (the peer follows the router) |
| POST /api/kafka/rules/reload | - | Re-read the rules file -> `{"changes": ["~ router 192.0.2.95 : a -> b", "+ ...", "- ..."]}` |

- The response to PUT / DELETE is the rule after the change (same shape as the GET above).
- DELETE of a rule that does not exist is 404.
- If reload fails (bad file, missing file), the answer is 400 and the previous rules stay in effect.
- Header `X-Actor: <name>` (optional): the caller's user name. It is recorded in the collector log line for the
  change as `actor=<value>`, e.g. `rules: router 192.0.2.31 PUT none (from 203.0.113.14:42396 actor=alice)`.
  Printable ASCII without spaces, at most 64 characters; otherwise 400.
- A PUT without `"topics"` is 400 (so that "do not send" never happens by mistake). To stop sending use
  `{"topics": []}`. Unknown fields in the body are 400.
- Every change is logged by the collector with the request address and X-Actor. Send X-Actor so the log can be
  matched against your own records. A wrong key is logged as well.

## 6. Errors

| Code | Meaning |
|---|---|
| 400 | Invalid value (address, prefix, match, origin_as / not_origin, path_as, offset / limit, as / state of /api/peers, topic name, X-Actor, body, number of p entries), or a failed rules reload |
| 403 | Wrong key / key file missing or too short. Client address outside `http.allow` |
| 404 | Unknown router / peer, DELETE of a rule that does not exist, reset of a router that is not connected |
| 409 | Route list / prefix lookup while route tables are off (`-rib=false`, 0.2.0+). The rules file was edited by hand and not yet reloaded -> reload first. Reset called when the input is not tcp |
| 429 | Reset of the same router again within 10 minutes (see the Retry-After header) |
| 500 | Writing the rules file / creating a topic failed |

The body is a one-line, human-readable reason (it can be shown to users as is).

## 7. Command-line client

The same binary calls the API. The arguments map one to one onto the API path after `/api/`, `name=value`
arguments become query parameters, and the option selects the HTTP method. Options (`-api`, `-json`, `-topics`,
`-token`) must come before the path. The default server is `-api http://127.0.0.1:5001`.

```
bongsudae -status routers                                    = GET /api/routers (as a table; the default without a path)
bongsudae -status routers 192.0.2.20                         = GET /api/routers/192.0.2.20 (router + peer table)
bongsudae -status -json peers state=down                     = GET /api/peers?state=down (raw JSON)
bongsudae -status routers 192.0.2.20 peers 198.51.100.33     = peer detail
bongsudae -status routers 192.0.2.20 prefixes distinct=1     = routes (prefix lists are fetched as format=text)
bongsudae -status prefix p=203.0.113.0/24 match=more not_origin=64500 limit=50   = suspicious rows
bongsudae -status kafka rules                                = rules table
bongsudae -set -topics unicast_prefix1 kafka rules routers 192.0.2.95   = PUT
bongsudae -set -topics none kafka rules routers 192.0.2.31   = PUT {"topics": []} (do not send)
bongsudae -del kafka rules routers 192.0.2.95                = DELETE
bongsudae -reload                                            = POST /api/kafka/rules/reload
bongsudae -post routers 192.0.2.20 reset                     = POST /api/routers/192.0.2.20/reset
```
- `-set` requires `-topics` (comma separated, `none` = `[]`).
- Changing requests (`-set`, `-del`, `-post`, `-reload`) send the key from the `-token` file and
  `X-Actor: cli:<local user>`. To use another server's collector, copy its key file and pass `-token`.

## 8. Notes for integrators

- Do not call the API from a browser directly: call it from your server and pass the results on. There is no CORS
  support, and the key must never reach a browser.
- Keep the key in a file on your server (mode 600, outside any web root). Protect rule changes with your own
  login / permissions and record who made each change (send it as X-Actor).
- Always set `limit` on route lists shown on screen; offer the full list as a `format=text` download.
