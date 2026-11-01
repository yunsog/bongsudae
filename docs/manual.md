# Bongsudae User Manual

Bongsudae is a BMP collector. It receives BMP (BGP Monitoring Protocol) from routers, parses it,
publishes OpenBMP v1.7 format messages to Kafka, and serves routers, peers and the routing table through an HTTP API.

> The Bongsudae was a beacon tower of old Korea, relaying news from hill to hill. This one relays news from your routers.

| Item | Details |
|---|---|
| Release | Bongsudae 0.1 (`GET /api/stats` returns `release` = `0.1` and `version` = git revision) |
| Binary | A single Go binary, `bongsudae`. It receives, parses, publishes to Kafka, keeps the routing table and serves the HTTP API. The only external library is the Kafka client (franz-go), vendored |
| Requirements | Linux, Kafka (tested with Apache Kafka 4.x in KRaft mode), enough memory for the routing table (section 6.2) |
| Standards | BMP RFC 7854; BGP RFC 4271, 4760 (multiprotocol), 6793 (4-byte AS), 7911 (Add-Path); communities RFC 1997, 4360, 5668, 8092 |
| Output format | OpenBMP Message Bus API v1.7 (same topics and fields as openbmpd 2.2.3). Known openbmpd bugs are not reproduced; Bongsudae follows the RFCs instead (see docs/kafka.md) |
| Scope | IPv4 / IPv6 unicast. VPN, EVPN, BGP-LS and other address families are counted and dropped |

---

## 1. Overview

```
 router (BMP, TCP 5000) ──┐                         ┌─> Kafka <prefix>.unicast_prefix  (route add / del)
 router ──────────────────┼─> bongsudae ────────────┼─> Kafka <prefix>.peer / .router (BGP session / router up, down)
 router ──────────────────┘   (one goroutine         ├─> Kafka openbmp.bmp_raw (received BMP, unchanged)
                               per router)           └─> HTTP API (e.g. 5001): routers / peers / prefix lookup / rules
                                  │
                                  └─ routing table (in memory, per peer)
```

- **Connections**: one router = one TCP connection = one goroutine. Routers are processed in parallel (multiple cores); messages from one router are processed in the order received.
- **Processing**: one BMP message -> parse -> Kafka messages (one BGP UPDATE = one unicast_prefix message with one or more rows) + routing table update.
- **Resync**: when Bongsudae restarts, routers reconnect and **send their full tables again**. With many full-table peers this can take a long time (tens of minutes) and produce a burst of Kafka messages, so plan restarts.
- **Configuration**: command-line options plus a config file (`/etc/bongsudae/bongsudae.conf`). The command line wins. `systemctl reload` re-reads configuration while running.

---

## 2. Terms

| Term | Meaning |
|---|---|
| BMP | BGP Monitoring Protocol (RFC 7854). The router sends BGP state and received routes to the collector (one direction only) |
| Initiation / Termination | Start of a BMP session (router name sysName, description sysDescr) / end of it |
| Peer Up / Peer Down | One BGP session (neighbor) on the router came up / went down. A Peer Up carries the two OPEN messages |
| Route Monitoring | A BGP UPDATE received from a neighbor, wrapped in BMP (route add / del) |
| End-of-RIB (EoR) | Marks that a neighbor has finished sending its initial table. Bongsudae uses it to mark a peer as `synced` |
| router hash | md5(router address + C_HASH_ID). Identifies one router |
| peer hash | md5(neighbor address + RD + router hash). Identifies one **(router, BGP neighbor) pair**. Kafka key of unicast_prefix and peer messages |
| resync | The router reconnects and sends Peer Up and its full table again (Bongsudae restart, reset API, router-side restart) |
| bmp_raw | Received BMP messages published to Kafka unchanged. Input for other collectors or for testing a new release |
| forwarding rules | Per-router / per-peer choice of the Kafka topics unicast_prefix rows go to (including "do not send"). File `conf/kafka_rules.json` |
| shadow run | A second instance that reads bmp_raw from Kafka and writes to test topics (`openbmp.test.*`). Used to test a new release next to production |

---

## 3. Installation

### 3.1 Fresh install
The example below uses the working directory `/var/lib/bongsudae`, the binary `/usr/local/bin/bongsudae` and the system user `bongsudae`.
Example files are in `deploy/` (`bongsudae.conf.example`, `bongsudae.service.example`).

```bash
# 1) Build (Go 1.26 or later; no network needed, dependencies are in vendor/)
go build -o bongsudae . && go test .
sudo install -m 755 bongsudae /usr/local/bin/bongsudae

# 2) User and working directory
sudo useradd --system --home-dir /var/lib/bongsudae --shell /usr/sbin/nologin bongsudae
sudo install -d -o bongsudae -g bongsudae -m 750 /var/lib/bongsudae /var/lib/bongsudae/conf /var/lib/bongsudae/run

# 3) Config and service
sudo install -d -m 755 /etc/bongsudae
sudo install -m 644 deploy/bongsudae.conf.example /etc/bongsudae/bongsudae.conf      # edit for your network (section 5)
sudo install -m 644 deploy/bongsudae.service.example /etc/systemd/system/bongsudae.service
sudo systemctl daemon-reload

# 4) API key for changes (at least 16 characters, mode 600): needed for rule changes and reset
sudo -u bongsudae sh -c 'head -c 24 /dev/urandom | base64 > /var/lib/bongsudae/conf/api_token && chmod 600 /var/lib/bongsudae/conf/api_token'

# 5) Check, start, verify
bongsudae -t                                  # check config file + rules file
sudo systemctl enable --now bongsudae
journalctl -u bongsudae -f                    # "start bongsudae 0.1 ...", "conn <ip>: initiation" per router, "stat ..." every minute
bongsudae -status                             # router table
```

- **Set `bmp.allow`** to your routers' source addresses (usually the loopback range). The default `127.0.0.1,::1` accepts only local connections.
- Firewall: allow BMP (TCP 5000) only from the routers, and the API port only from the hosts that call it. This is a second layer on top of Bongsudae's own `bmp.allow` / `http.allow`.
- Router side: configure the collector address and port 5000 (route monitoring, pre-policy Adj-RIB-In).

### 3.2 Files

| Path (example layout) | Contents | Written by |
|---|---|---|
| `/usr/local/bin/bongsudae` | Binary | install |
| `/etc/bongsudae/bongsudae.conf` | Config file | operator |
| `/var/lib/bongsudae/conf/kafka_rules.json` | Kafka forwarding rules (default if missing: everything to `unicast_prefix`) | API / operator |
| `/var/lib/bongsudae/conf/api_token` | API key for changes (mode 600) | operator |
| `/var/lib/bongsudae/run/bongsudae.lock` | Lock file (contains the pid): prevents two instances | bongsudae |
| log | Standard output / error. Under systemd: the journal, or a file if the unit uses `StandardOutput=append:` | bongsudae |

The relative defaults `conf/kafka_rules.json` and `conf/api_token` are resolved against the working directory.
Prefer absolute paths in the config file so the result does not depend on `WorkingDirectory=`.

Suggested systemd settings (see the example unit): `Restart=always` with `RestartSec=10` (Bongsudae exits on its own when Kafka is down
for 2 minutes and expects to be restarted), `StartLimitIntervalSec=0`, `ExecReload=/bin/kill -HUP $MAINPID`, `TimeoutStopSec=60`
(on SIGTERM it flushes buffered messages), write access only to the working directory (`ProtectSystem=strict`, `ReadWritePaths=`),
and optionally `MemoryMax=` sized for your routing table.

---

## 4. Running and command-line options

### 4.1 Modes
Selected by `-input` and command options. With none of them Bongsudae runs in **production** mode (`-input tcp`).

| Mode | Command | What it does |
|---|---|---|
| Production | `bongsudae [-c FILE] [options]` | Receives BMP from routers, publishes to Kafka, serves the API. Reads the config file |
| Shadow | `bongsudae -input kafka -prefix openbmp.test [-src openbmp.bmp_raw] [-group bmp-test-NAME]` | Reads bmp_raw from Kafka in real time and writes to test topics. Does not read the config file. `-prefix openbmp.parsed` (and `openbmp`) is refused; the group must start with `bmp-test-` |
| File | `bongsudae -input file -in DUMP -out PREFIX\|none [-http 127.0.0.1:5002]` | bmp_raw dump file -> one dump file per message type (no Kafka). With `-http`, keeps the API open after processing (rule changes disabled) |
| Check | `bongsudae -t` | Checks the config file and the rules file, then exits |
| API client | `bongsudae -status / -set / -del / -post / -reload ...` | Calls the HTTP API of a running Bongsudae (section 8.3). Reads the config file to find the key file |

### 4.2 All options
Options with a "Config key" can also be set in the config file (section 5). "Reload" = whether `systemctl reload` applies a change.

**Files**

| Option | Default | Config key | Reload | Meaning |
|---|---|---|---|---|
| `-c` | (empty = `/etc/bongsudae/bongsudae.conf` if it exists) | - | - | Config file. Production and API client only (an error with `-input kafka` / `file`) |
| `-rules` | `conf/kafka_rules.json` | `paths.rules` | restart (contents are re-read on every reload) | Kafka forwarding rules file |
| `-token` | `conf/api_token` | `paths.token` | restart (contents are read on every request) | API key file. Server: checks the key; API client: sends it |
| `-lock` | (empty = no lock) | `paths.lock` | restart | Lock file. If another instance holds it, logs one line and exits |

**Receiving BMP (`-input tcp`)**

| Option | Default | Config key | Reload | Meaning |
|---|---|---|---|---|
| `-listen` | `:5000` | `bmp.listen` | restart | Address to receive BMP on |
| `-allow` | `127.0.0.1,::1` | `bmp.allow` | live | Allowed addresses / prefixes (comma separated), normally your router loopback range. Others are closed immediately. On reload, current connections from routers no longer in the list are closed |
| `-raw` | `true` | `bmp.raw` | live | Publish received BMP unchanged to `<prefix without .parsed>.bmp_raw` |
| `-chash` | (empty = md5(hostname)) | `bmp.chash` | restart | C_HASH_ID in message headers (32 hex digits; also used for the router hash). Empty = md5 of the host name, as openbmpd does when `admin_id` is not set. Set it explicitly to keep hashes stable if the host name changes |

**Kafka**

| Option | Default | Config key | Reload | Meaning |
|---|---|---|---|---|
| `-kafka` | `127.0.0.1:9092` | `kafka.brokers` | restart | Kafka bootstrap address |
| `-prefix` | `openbmp.parsed` | `kafka.prefix` | restart | Output topic prefix (`<prefix>.unicast_prefix / .router / .peer`). For tests use `openbmp.test` |
| `-src` | `openbmp.bmp_raw` | - | - | Shadow: topic to read |
| `-group` | `bmp-test-shadow` | - | - | Shadow: consumer group (must be `bmp-test-*`) |
| `-from-start` | `false` | - | - | Shadow: if the group is new, start from the beginning of the topic (replay tests) |

**HTTP API**

| Option | Default | Config key | Reload | Meaning |
|---|---|---|---|---|
| `-http` | (empty = off) | `http.listen` | restart | API address (`:5001` = all interfaces, `127.0.0.1:5001` = local only) |
| `-http-allow` | (empty = everyone) | `http.allow` | live | Allowed addresses / prefixes; others get 403. Empty = rely on the firewall only |

**File mode / check / API client (command line only)**

| Option | Meaning |
|---|---|
| `-input` | `tcp` (default) / `kafka` / `file` |
| `-in`, `-out` | File mode: input dump / output file prefix (`<out>.<type>.dump`; `none` = discard) |
| `-t` | Check only |
| `-status`, `-set`, `-del`, `-post`, `-reload` | API client: GET / PUT / DELETE / POST / reload rules (section 8.3) |
| `-topics` | Topics for `-set` (comma separated, `none` = do not send) |
| `-json` | Print the received JSON instead of a table |
| `-api` | Server to call (default `http://127.0.0.1:5001`) |

Value formats: lists are comma separated (`192.0.2.0/24, 2001:db8::1`); a single address means /32 (IPv6 /128); booleans are `true` / `false`.

---

## 5. Config file

### 5.1 Rules
- Location: `-c FILE`, otherwise `/etc/bongsudae/bongsudae.conf` if it exists. **Read only in production mode and by the API client** (shadow and file mode never read it, so production values cannot leak into a test).
- Precedence: **command-line option > config file > built-in default**.
- Format
  - `key = value` lines under `[section]` headers
  - Everything after `#` is a comment (anywhere on a line)
  - Lists are comma separated. **A value ending in a comma continues on the next line** (one line is also fine)
  - An empty value (`allow =`) sets the option to empty
- Errors: unknown key, the same key twice, an invalid value, or a list that ends with a comma with no following value
  - at startup -> Bongsudae does not start (the log shows the line number and the reason)
  - on reload -> nothing is changed; the current values stay

### 5.2 All keys (example)
```ini
[paths]
rules = /var/lib/bongsudae/conf/kafka_rules.json   # (restart) contents re-read on every reload
token = /var/lib/bongsudae/conf/api_token          # (restart) contents read on every request
lock  = /var/lib/bongsudae/run/bongsudae.lock      # (restart) empty = no lock

[bmp]
listen = :5000                       # (restart)
allow  = 192.0.2.0/24,               # your router loopback range(s)
         2001:db8:100::/48
raw    = true
chash  =                             # (restart) empty = md5(hostname)

[kafka]
brokers = 127.0.0.1:9092             # (restart)
prefix  = openbmp.parsed             # (restart)

[http]
listen = 127.0.0.1:5001              # (restart) empty = API off
allow  =                             # empty = everyone (firewall only)
```
`(restart)` = not applied by reload; needs a restart (section 6.6). A restart means a full resync from all routers.

---

## 6. Behaviour

### 6.1 Connections and protection
| Case | Behaviour |
|---|---|
| Source address not in `bmp.allow` | Closed immediately (not logged: internet scanners are common) |
| First message does not arrive within 30 s, or is not an Initiation | That connection is closed |
| Length in the BMP header over 1 MB, or version not 3 | That connection is closed |
| Message that cannot be parsed | Only that message is dropped; the connection stays up (counted in `errors`, logged up to 100 lines per router per minute) |
| Peer Up without OPEN messages (RFC 7854 violation) | Connection kept; a peer up with empty capabilities is published, plus one log line |
| Programming error (panic) | Affects only that message (processing) / that connection (reading) |
| More than 200 concurrent connections | New connections are closed |
| Idle TCP | No read timeout (BMP can be silent), TCP keepalive 60 s |

### 6.2 Message processing
- One goroutine per router reads, parses, updates the routing table and hands messages to Kafka, in that order (order within a router = order received).
- One UPDATE -> **one add message and one del message** at most (adds and dels are never mixed in one message; no message if there are no rows).
- Some routers send IPv4 routes in MP_REACH; both forms are handled. 4-byte AS is taken from the A flag of the per-peer header.
- No del rows are generated on Peer Down (same as openbmpd). When a router disconnects only a router term message is sent; consumers should treat it as "all peers of that router are down".
- Routing table: live prefixes per peer (the same add several times = 1, del removes). Peer down, a new Peer Up, or router disconnect resets it to 0. Path attributes are shared between routes.
- Memory: about 65 bytes per route, so a full table of ~1M routes uses about 80 MB per peer. Many full-table peers add up: roughly 100 million routes in total needs a resident size of 11–15 GB.

### 6.3 Kafka output and failure handling
- No compression, record batches up to 4 MB, producer buffer of 200,000 records.
- **When the buffer is full, reading from that router pauses** -> TCP flow control slows the router down (nothing is dropped).
- Kafka errors are logged as a summary: one line per error type per minute.
- A short Kafka restart (tens of seconds): the client reconnects on its own.
- **No successful produce for 2 minutes** (or, in shadow mode, only read errors for 2 minutes): Bongsudae exits -> systemd restarts it (= resync). This covers, for example, a topic that was deleted and re-created with a new topic ID.
- `kill` / `systemctl stop` (SIGTERM): sends what is left in the buffer, then exits.

### 6.4 Forwarding rules (unicast_prefix only; router / peer messages are always sent)
- Order: **peer rule -> router rule -> default**. A value is a list of topic names (each starting with `unicast_prefix`; the real topic is `<prefix>.<name>`, created on first use).
- `[]` = do not send (a rule exists); deleting a rule = fall back to the next level. The default cannot be deleted.
- Changes apply to UPDATEs received after the change (the current table is not replayed into a new topic).
- How to change: the API (`-set`, `-del`, section 8.3) writes the file immediately; or edit the file by hand -> `bongsudae -t` -> `systemctl reload bongsudae` or `bongsudae -reload`.
- If the file was edited by hand and not yet reloaded, API changes are refused with 409 (so the manual edit is not overwritten). On reload, a missing file is an error (protects against accidental deletion). A missing file at startup means the default rules.

Rules file format:
```json
{
  "default": ["unicast_prefix"],
  "routers": {
    "192.0.2.20": { "topics": ["unicast_prefix", "unicast_prefix_core"] },
    "192.0.2.95": { "topics": [], "peers": { "198.51.100.33": ["unicast_prefix"] } }
  }
}
```

### 6.5 Start and restart (resync)
- Startup: config -> rules file -> check / create topics -> API -> listen for BMP -> routers connect.
- When a router connects: Initiation -> Peer Up (per neighbor) -> full table -> EoR.
- Prefix counts are exact from the first full table after Peer Up. A restart forgets previous state (list of down peers, per-minute statistics).
- Consumers see lag build up during a resync. Inform consumer owners before restarting.

### 6.6 Reloading configuration (`systemctl reload bongsudae`)
Re-reads the config file and the rules file while running (not a restart: connections and the routing table stay).

1. Re-read the config file used at startup with the same rules (the command line still wins; a key removed from the file = default value).
2. Validate all new values first. If anything is invalid or the file is missing, **nothing changes**.
3. If a restart-only key changed, the current value is kept and a warning is logged.
4. Apply the rest at once; the log lists only changed keys as `key "old" -> "new"`.
5. Then re-read the rules file (this happens even if the config file was invalid).

| Applied live | From when |
|---|---|
| `bmp.allow` | Next connection. Current connections from routers no longer allowed are closed at once (router term 65534 `Closed by collector: not in bmp.allow (config reload)`) |
| `bmp.raw` | Next message (when turned on, the topic is created if missing) |
| `http.allow` | Next request |
| rules file contents | Next UPDATE |

| Needs restart | Why |
|---|---|
| `bmp.listen`, `http.listen` | The socket must be re-opened |
| `kafka.brokers`, `kafka.prefix`, `bmp.chash` | Changing destination or hashes would confuse consumers |
| `paths.*` | Moving files while running is risky |

---

## 7. Kafka output (details: docs/kafka.md)

| Topic | Contents | Key |
|---|---|---|
| `<prefix>.unicast_prefix` | Route add / del (32 tab-separated fields, several rows per message) | peer hash |
| `<prefix>.peer` | BGP session up / down (31 fields) | peer hash |
| `<prefix>.router` | Router init / term (12 fields) | router hash |
| `<prefix without .parsed>.bmp_raw` (default `openbmp.bmp_raw`) | Received BMP unchanged (`bmp.raw`, production mode only) | router hash |

- Message = header (`V: 1.7`, `C_HASH_ID`, `T`, `L`, `R`) + empty line + rows. bmp_raw headers are `V`, `C_HASH_ID`, `R_HASH`, `R_IP`, `L`.
- **Same key = same partition = order preserved**: unicast_prefix and peer messages for one neighbor arrive in the order sent. There is no ordering across topics.
- Unlike openbmpd, bmp_raw also carries messages that failed to parse, and forwarding rules do not apply to it.
- Forwarding rules can send unicast_prefix rows to other topics, or not at all (6.4).

---

## 8. HTTP API (details: docs/api.md, also served by the binary at `GET /api/doc`)

### 8.1 Address and access
- `http.listen`. Clients outside `http.allow` get 403. At most 4 requests are processed at a time (others wait).
- Reads (GET) are open to everyone allowed. Changes (rule PUT / DELETE / reload, reset) need the header `X-API-Key` = contents of the `paths.token` file (at least 16 characters; without the file, changes are disabled). The optional header `X-Actor` (printable ASCII, up to 64 characters) is written to the log.

### 8.2 Main endpoints
| Task | Endpoint |
|---|---|
| Health (per-minute stats for 60 minutes, release / version, memory, Kafka) | `GET /api/stats` |
| Routers / peers | `GET /api/routers`, `/api/routers/{ip}`, `/api/peers`, `/api/routers/{ip}/peers/{peer}` |
| Route lists | `GET /api/routers/{ip}/peers/{peer}/prefixes`, `/api/routers/{ip}/prefixes` (`?format=text`, `?distinct=1`) |
| Prefix / address lookup | `GET /api/prefix?p=...&match=exact\|longest\|covering\|more`, `/api/routers/{ip}/prefix` |
| Bulk lookup (up to 5000) | `POST /api/prefix`, `POST /api/routers/{ip}/prefix` |
| Filter by origin | `origin_as=`, `not_origin=` on lookups and lists |
| Forwarding rules | `GET /api/kafka/rules[...]`, `PUT / DELETE ...` (key), `POST /api/kafka/rules/reload` (key) |
| Resync one router | `POST /api/routers/{ip}/reset` (key, production mode only, once per 10 minutes per router) |

### 8.3 Command-line client
Arguments = path after `/api/`, `name=value` = query parameters. Known paths are shown as tables.
```bash
bongsudae -status                                       # router table (= GET /api/routers)
bongsudae -status routers 192.0.2.20 peers              # peers of that router
bongsudae -status -json peers state=down                # raw JSON
bongsudae -status kafka rules                           # forwarding rules
bongsudae -set -topics unicast_prefix1 kafka rules routers 192.0.2.95                     # change a rule (key)
bongsudae -set -topics none kafka rules routers 192.0.2.95 peers 198.51.100.33            # do not send
bongsudae -del kafka rules routers 192.0.2.95           # delete a rule (fall back to the next level)
bongsudae -reload                                       # re-read the rules file
bongsudae -post routers 192.0.2.20 reset                # resync one router (key)
bongsudae -api http://203.0.113.10:5001 -status         # another server
```
A reset closes the BMP session (router term 65534 `Closed by collector: api reset actor=...`); the router reconnects and sends everything again.
Resyncing a single BGP neighbor is not possible through BMP (BMP is one-way); use a soft refresh on the router.

---

## 9. Logs

Bongsudae logs to standard output / error with microsecond timestamps. Under systemd this goes to the journal (`journalctl -u bongsudae`)
or to the file named in `StandardOutput=append:`; in the latter case rotate it with `copytruncate`.

In release 0.1 some log messages are in Korean. The table lists the fixed parts of each line.

| When | Line (example) | Meaning |
|---|---|---|
| start | `설정 /etc/bongsudae/bongsudae.conf : paths.rules=... bmp.allow=...` | Config file read and values applied (a key overridden on the command line shows `(명령줄 -option)` = "command line") |
| start | `start bongsudae 0.1 (<rev>) input=tcp prefix=openbmp.parsed http="127.0.0.1:5001" rules=...` | Release, git revision, input, topics, API, rules file |
| start | `rules: default unicast_prefix, routers 0` | Rules loaded |
| start | `http api ...`, `bmp_raw -> openbmp.bmp_raw`, `listen :5000 allow [192.0.2.0/24]` | Listening addresses, allowed ranges |
| start | `lock ... : 이미 돌고 있음 (pid N), 끝남` | Another instance holds the lock ("already running, exiting"); systemd retries every `RestartSec` |
| connection | `conn 192.0.2.20: initiation` / `conn ... closed after N msgs: ...` | Router connected / disconnected |
| connection | `conn ...: first message type N (not initiation), drop`, `too many connections, drop ...` | Closed by protection (6.1) |
| processing | `router ...: peer_up ... without OPEN (RFC 7854 violation) ...` | Router violates the standard; the message is still processed |
| every minute | `stat routers=3 route_mon=47346 errors=0 add=63722 del=11101 msgs=47346 ... raw=47344 kafka_fail=0 topics: unicast_prefix=47343/74820` | See the table below |
| Kafka | `... : N 번 (지난 1 분)` | Kafka error summary: N times in the last minute (one line per error type) |
| Kafka | `kafka 보내기가 2m0s 넘게 안 됨 : 끝냄 (systemd 가 다시 띄움)` | No successful produce for 2 minutes, exiting (6.3) |
| API | `reset: router ... closed (from ... actor=...)`, `rules: ... (from ... actor=...)`, `rules: key 틀림 ...` | Who changed what / wrong key |
| reload | `설정 다시 읽음 (..., SIGHUP) : bmp.allow "..." -> "..." (빠진 라우터 연결 N 개 끊음)` | Config reloaded: changed keys (N connections of removed routers closed) |
| reload | `설정 다시 읽음 : 재시작해야 바뀌는 항목 (지금 값 유지) : ...` | Restart-only keys changed; current values kept |
| reload | `설정 다시 읽기 실패, 지금 값 유지 : ...` | Config reload failed; nothing changed |
| reload | `rules reload (SIGHUP): N 바뀜` / `rules   ...` | Rules file re-read: N changes, then the changed rules |
| stop | `signal terminated: flush and exit` | stop / restart |

**Per-minute statistics (`stat`)**: zero values are omitted.

| Name | Meaning |
|---|---|
| `routers` | Routers currently connected |
| `route_mon`, `peer_up`, `peer_down`, `stats`, `init`, `term`, `route_mirror`, `unknown` | Received BMP messages by type |
| `errors` | Messages that failed to parse (normally 0) |
| `others` | Non-unicast routes (VPN etc., dropped) |
| `add`, `del` | Route rows generated |
| `msgs`, `router_msgs`, `peer_msgs` | Kafka messages sent (of which router / peer) |
| `peer_up_no_open` | Peer Up without OPEN |
| `not_sent` | Rows not sent because of a "do not send" rule |
| `raw` | bmp_raw messages sent |
| `kafka_fail` | Failed Kafka produces (normally 0) |
| `topics: name=messages/rows` | Per unicast_prefix topic |

---

## 10. Operations

| Task | How |
|---|---|
| Check status | `systemctl status bongsudae`, `bongsudae -status`, `curl -s 127.0.0.1:5001/api/stats`, the log |
| Change configuration | Edit the file -> `bongsudae -c FILE -t` -> install to `/etc/bongsudae/bongsudae.conf` -> `sudo systemctl reload bongsudae` for live keys, otherwise restart (resync) |
| Add a router | Configure BMP on the router. If its address is outside `bmp.allow`, add it and reload; open the firewall for port 5000 too |
| Remove a router | Remove the BMP config on the router (or remove it from `bmp.allow` and reload = disconnected at once) |
| Allow a new API client | Firewall for the API port; if `http.allow` is used, add it there too and reload |
| Send routes to other topics / not at all | Forwarding rules (6.4, 8.3) |
| Resync one router | `bongsudae -post routers <ip> reset` (once per 10 minutes) |
| Upgrade | Test the new build (`go test .`, a shadow run, or file mode on a bmp_raw dump, comparing output with the old release) -> inform consumers -> keep the old binary (e.g. `bongsudae.prev`) -> install the new binary -> `sudo systemctl restart bongsudae` |
| Roll back | Put the previous binary back and `sudo systemctl restart bongsudae` |
| Test a new release in shadow | `./bongsudae.new -input kafka -prefix openbmp.test -group bmp-test-NAME -http 127.0.0.1:5003` (next to production, writing to test topics), then compare its topics with production |
| Test the API on a full table | `bongsudae -input file -in <bmp_raw dump> -out none -http 127.0.0.1:5002` |

Notes on the shadow run:
- It takes router hashes from the `R_HASH` header of bmp_raw, so its output matches production.
- It has no routes from before it started (prefix counts in its API are lower until routers resync).
- Use a separate API port and a separate lock file (or none); if it holds the production API port, the production instance fails to start.

---

## 11. Troubleshooting

| Symptom | Where to look / what to do |
|---|---|
| Does not start | `systemctl status bongsudae` and the log: config error (line number), `-http ...: address already in use` (another process holds the API port), `lock ... 이미 돌고 있음` (an instance started by hand is running) |
| A router does not connect | Router BMP config, firewall port 5000, `bmp.allow` (outside = closed without a log line; the default allows only localhost), `first message type` in the log |
| `routers=` drops | `conn ... closed after` in the log (router restart / link). When it reconnects, only that router resyncs |
| `errors=` is not 0 | `router ...: ...` lines in the log (up to 100 per router per minute). Only that message is dropped. Reproduce with file mode from a bmp_raw dump |
| `kafka_fail=` is not 0 | Kafka status and the Kafka summary lines. After 2 minutes without success Bongsudae restarts itself |
| Low prefix count / `synced` false | Shortly after start a resync is still running. A neighbor that never sends EoR is never marked synced (its routes still arrive) |
| Large consumer lag | Normal right after a resync. If it stays high, check the consumers |
| API 403 | Client outside `http.allow`, or wrong / missing key (the body gives the reason) |
| API 409 | The rules file was edited by hand and not reloaded -> run `bongsudae -reload` first |
| API 429 | Reset of the same router again within 10 minutes (see `Retry-After`) |
| Memory | See 6.2. If systemd `MemoryMax=` is exceeded the process is killed and restarted (= resync) |

---

## Appendix A. Option / config key map

| Config key | Option | Reload |
|---|---|---|
| `paths.rules` | `-rules` | restart (contents on reload) |
| `paths.token` | `-token` | restart (contents on every request) |
| `paths.lock` | `-lock` | restart |
| `bmp.listen` | `-listen` | restart |
| `bmp.allow` | `-allow` | live |
| `bmp.raw` | `-raw` | live |
| `bmp.chash` | `-chash` | restart |
| `kafka.brokers` | `-kafka` | restart |
| `kafka.prefix` | `-prefix` | restart |
| `http.listen` | `-http` | restart |
| `http.allow` | `-http-allow` | live |

Command-line only options: `-c`, `-input`, `-src`, `-group`, `-from-start`, `-in`, `-out`, `-t`,
`-status`, `-set`, `-del`, `-post`, `-reload`, `-topics`, `-json`, `-api`.
