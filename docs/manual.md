# Bongsudae User Manual

Bongsudae is a BMP collector. It receives BMP (BGP Monitoring Protocol) from routers, parses it,
publishes OpenBMP v1.7 format messages to Kafka, and serves routers, peers and the routing table through an HTTP API.

> The Bongsudae was a beacon tower of old Korea, relaying news from hill to hill. This one relays news from your routers.

| Item | Details |
|---|---|
| Release | Bongsudae 0.2.0 (`GET /api/stats` returns `release` = `0.2.0` and `version` = git revision) |
| Binary | A single Go binary, `bongsudae`. It receives, parses, publishes to Kafka, keeps the routing table and serves the HTTP API. The only external library is the Kafka client (franz-go), vendored |
| Requirements | Linux, Kafka (tested with Apache Kafka 4.x in KRaft mode), enough memory for the routing table (section 6.2) |
| Standards | BMP RFC 7854; BGP RFC 4271, 4760 (multiprotocol), 6793 (4-byte AS), 7911 (Add-Path); communities RFC 1997, 4360, 5668, 8092 |
| Output format | OpenBMP Message Bus API v1.7 (same topics and fields as openbmpd 2.2.3). Where openbmpd differs from the RFCs, Bongsudae follows the RFCs (differences in docs/kafka.md) |
| Scope | IPv4 / IPv6 unicast, (0.3.0+) labeled unicast, L3VPN, multicast, EVPN, FlowSpec. BGP-LS and other address families are counted and dropped |

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
# 1) Binary : download bongsudae_<version>_linux_<amd64|arm64>.tar.gz + SHA256SUMS from GitHub Releases (0.3.0+, static)
tar xzf bongsudae_0.3.0_linux_amd64.tar.gz && sha256sum -c --ignore-missing SHA256SUMS
cp bongsudae_0.3.0_linux_amd64/bongsudae .
#    or build (Go 1.26 or later; go downloads the one dependency, franz-go, checked by go.sum)
go install github.com/yunsog/bongsudae@latest      # -> $(go env GOPATH)/bin/bongsudae
#    or in a clone : go build -o bongsudae . && go test ./...
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
journalctl -u bongsudae -f                    # "start bongsudae 0.2.0 ...", "conn <ip>: initiation" per router, "stat ..." every minute
bongsudae -status                             # router table
```

- `bmp.allow` defaults to any address (`0.0.0.0/0,::/0`), since routers connect from remote networks. If your router loopbacks are in known ranges, list them to narrow it.
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
| Upgrade | `bongsudae -upgrade` | (0.2.0+) Replaces the running collector with the new binary at the same path. Router sessions stay up (6.7) |
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
| `-allow` | `0.0.0.0/0,::/0` | `bmp.allow` | live | Allowed addresses / prefixes (comma separated). Default = any address; narrow to your router loopback ranges if you can. Others are closed immediately. On reload, current connections from routers no longer in the list are closed |
| `-raw` | `true` | `bmp.raw` | live | Publish received BMP unchanged to `<prefix without .parsed>.bmp_raw` |
| `-chash` | (empty = md5(hostname)) | `bmp.chash` | restart | C_HASH_ID in message headers (32 hex digits; also used for the router hash). Empty = md5 of the host name, as openbmpd does when `admin_id` is not set. Set it explicitly to keep hashes stable if the host name changes |

**Kafka**

| Option | Default | Config key | Reload | Meaning |
|---|---|---|---|---|
| `-kafka` | `127.0.0.1:9092` | `kafka.brokers` | restart | Kafka bootstrap address |
| `-partitions` | `8` | `kafka.partitions` | restart | Partitions of topics the collector creates (existing topics are not changed) |
| `-replication` | `1` | `kafka.replication` | restart | Replication factor of topics the collector creates |
| `-retention` | `90m` | `kafka.retention` | restart | Retention (retention.ms) of topics the collector creates, at least 1m |
| `-rib` | `ipv4-unicast,ipv6-unicast` | `bmp.rib` | restart | (0.3.0+: a list of address families) Address families to keep route tables for: `ipv4-unicast,ipv6-unicast,vpnv4 ...`, `all` (every family with route tables: unicast, labeled, vpnv4 / vpnv6, multicast, the four FlowSpec families), `none` (forward only, much less memory). The 0.2 values still work: `true` = ipv4 / ipv6-unicast, `false` = none. Kafka output is the same; families without a table are missing from route counts, lists and lookups (all off: those APIs answer 409; one off: peer warning `route table off for ...`). A family this release does not parse yet is accepted with a log line |
| `-key-buckets` | `0` | `kafka.key_buckets` | restart | Split the unicast_prefix Kafka key (0 = one key per peer, N = N buckets by prefix, key `<peer hash>.<bucket>`, 0 to 64). See docs/kafka.md section 1 |
| `-format` | `openbmp` | `kafka.format` | restart | Message format of unicast_prefix / router / peer: `openbmp` (v1.7, tab separated) or `json` (one JSON object per line). bmp_raw is unchanged. kafka.md 2.1 |
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
| `-upgrade` | Hitless upgrade (6.7): sends SIGUSR2 to the pid in the lock file and waits for the result in `/api/stats` |
| `-resume`, `-handover-check`, `-handover-formats` | Internal (the handover exec / checking the new binary: `-handover-check` prints "handover state 1" for 0.2, `-handover-formats` lists the state formats it reads). Never needed by hand |

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
- Errors: unknown key, the same key twice, an invalid value, or a list that ends with a comma with no following value (so a typo never runs silently on defaults)
- Removed keys: a key or section that existed in an official release (0.1, 0.2, 0.3, ...) and is removed is still accepted for that one official release, with a warning line
  (`config line N : [section] key ignored (...)`); from the next official release it is an unknown-key error. Keys that only existed in development builds (-dev) are errors at once
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
format  = openbmp                    # (restart) openbmp | json

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
| First message does not arrive within 10 s, or is not an Initiation | That connection is closed (routers send it right after connecting) |
| Length in the BMP header over 1 MB, or version not 3 | That connection is closed |
| Message that cannot be parsed | Only that message is dropped; the connection stays up (counted in `errors`, logged up to 100 lines per router per minute) |
| Peer Up without OPEN messages (RFC 7854 violation) | Connection kept; a peer up with empty capabilities is published, plus one log line |
| Programming error (panic) | Affects only that message (processing) / that connection (reading) |
| More than 4 connections from one address, or 64 in total, still waiting for Initiation | New connections are closed at once (so a scanner cannot take all slots; logged once per address per minute) |
| More than 2,000 connections that have sent Initiation (routers) | That connection is closed |
| Very many information TLVs of the same type (sysName etc.) | Up to 4 KB per type is joined; the rest is dropped |
| Idle TCP | No read timeout (BMP can be silent), TCP keepalive 60 s |

### 6.2 Message processing
- One goroutine per router reads, parses, updates the routing table and hands messages to Kafka, in that order (order within a router = order received).
- One UPDATE -> **one add message and one del message** at most (adds and dels are never mixed in one message; no message if there are no rows).
- Some routers send IPv4 routes in MP_REACH; both forms are handled. 4-byte AS is taken from the A flag of the per-peer header.
- No del rows are generated on Peer Down (same as openbmpd). When a router disconnects only a router term message is sent; consumers should treat it as "all peers of that router are down".
- Routing table: live prefixes per peer (the same add several times = 1, del removes). Peer down, a new Peer Up, or router disconnect resets it to 0. Path attributes are shared between routes.
- Views (0.3.0+): when a router sends the same neighbor in several views (Adj-RIB-In pre / post-policy, Adj-RIB-Out pre / post, Loc-RIB; L / O flags and Peer Type 3 of the per-peer header),
  each view has its own route table. The API shows the peer's main view (normally in-pre) and takes `view=`. Peer Down / a new Peer Up clears all views. Kafka rows are unchanged (isPrePolicy / isAdjRibIn).
- Address families (0.3.0+): MP_REACH / MP_UNREACH are parsed per AFI / SAFI by a registered parser (family.go): ipv4 / ipv6-unicast and **labeled unicast, L3VPN (VPNv4 / VPNv6), multicast, EVPN, FlowSpec (IPv4 / IPv6 / VPN)**
  (famroute.go / evpn.go / flowspec.go: Kafka topics `labeled_unicast_prefix`, `l3vpn`, `multicast_prefix`, `evpn`, `flowspec`, created when the first route arrives; row formats in kafka.md 3.2 - 3.4; flowspec is Bongsudae's own 43-field format).
  Route tables for these families (famrib.go) are kept only for families listed in `bmp.rib` (off by default): L3VPN routes are keyed by RD + prefix, labels are kept as a route attribute, FlowSpec rules are keyed by (RD, rule) and looked up by destination prefix (no route tables for EVPN). API: peer `family_routes`, list / lookup `family=`. Other families are counted per family and dropped
  (`other_families:` in the stat line, /api/stats, /metrics `bongsudae_other_family_total`). The peer detail `families` lists the families announced in both OPENs.
- Add-Path (0.3.0+, RFC 7911): for sessions that negotiated Add-Path (OPEN messages in the Peer Up), each path ID of a prefix is kept separately (a route with the same ID replaces only that path; a withdrawal removes only that ID).
  API rows carry `path_id`, peers `paths_v4 / v6`. Other sessions keep the same tables and memory. Kafka rows are unchanged (path_id field since 0.1).
- Done yet? `synced` = End-of-RIB received. `settled` = synced, or up for 5 minutes with no new prefix for 5 minutes
  (for routers that do not forward End-of-RIB over BMP; API `settled` / `peers_settled`, SYNCED column of `-status` = yes / settled / no).
- Allocation: rows are written straight into reused per-router buffers, with the parts shared by all rows of an UPDATE (router, peer, attributes) built once;
  each BMP message is allocated once when read (bmp_raw uses the same bytes). Benchmark: `BONGSUDAE_BENCH_DUMP=<bmp_raw dump> go test -run '^$' -bench Process -benchmem .`
- Memory: about 65 bytes per route, so a full table of ~1M routes uses about 80 MB per peer. Many full-table peers add up: roughly 100 million routes in total needs a resident size of 11–15 GB.

### 6.3 Kafka output and failure handling
- No compression, record batches up to 4 MB, producer buffer of 200,000 records.
- **When the buffer is full, reading from that router pauses** -> TCP flow control slows the router down (nothing is dropped).
- Kafka errors are logged as a summary: one line per error type per minute.
- A short Kafka restart (tens of seconds): the client reconnects on its own.
- **A topic deleted or re-created with a new topic ID** (0.2.0+): when a produce / fetch error is UNKNOWN_TOPIC_ID or UNKNOWN_TOPIC_OR_PARTITION, the client drops what it knows about that topic and reads it again (creating it if missing), at most every 30 s per topic. Log: `kafka topic <topic> : unknown topic (deleted / re-created?), re-reading topic metadata`. Only the records of that topic buffered at the time (about 15 s) are lost; there is no restart and no resync.
- **No successful produce for 2 minutes** (or, in shadow mode, only read errors for 2 minutes): Bongsudae exits -> systemd restarts it (= resync). This is for Kafka being down altogether.
- Topics the collector creates (first use, new topics in the rules) get `kafka.partitions` / `kafka.replication` / `kafka.retention` (default 8 / 1 / 90m). Existing topics are never changed.
- **watchdog** (0.2.0+, unit file `WatchdogSec=180`, `NotifyAccess=main`): after each 1-minute statistics pass (which takes every router lock in turn) Bongsudae sends `WATCHDOG=1` to systemd. If it is alive but stuck (e.g. a deadlock), systemd restarts it after 180 s. It also sends `READY=1` at start and `STOPPING=1` on shutdown.
- `kill` / `systemctl stop` (SIGTERM, 0.2.0+): stops accepting, closes every router connection (one router term 65534 `Closed by collector: shutdown` per router), waits up to 10 s for the router goroutines, sends what is left in the Kafka buffer (up to 30 s), then exits. A SIGTERM during start-up follows the same steps.
- (0.2.0+) A new connection from a router that is already connected replaces the old one; the old one is closed and the log says `conn <ip>: new connection replaces the old one (closed)`. Messages the old connection had already read are dropped. If this line repeats, the router probably has two BMP sessions to this collector.

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
- From 0.2.0 a new release is installed without a restart (6.7). A restart (resync) is only needed when the hitless upgrade cannot be used (upgrading from 0.1, a release with a new state format, a new listen address).

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
| `kafka.brokers`, `kafka.prefix`, `kafka.key_buckets`, `kafka.format`, `bmp.chash` | Changing destination, keys, format or hashes would confuse consumers |
| `paths.*` | Moving files while running is risky |

### 6.7 Hitless upgrade (0.2.0+, `bongsudae -upgrade`)
The router TCP sessions and all state are handed over to the new binary, so routers do not reconnect (no resync, no consumer lag).
Like nginx's binary upgrade: **the same pid** execs the new file, so for systemd nothing happens (it is not a restart).

| Step | Old process | New process |
|---|---|---|
| 1 | Checks the new binary: picks the newest state format it reads (`-handover-formats`; a 0.2 binary has only `-handover-check` = format 1) that this process can write, and `-t` (accepts the current config). Stops here if not | |
| 2 | Pause: stops accepting and stops reading from every router at a message boundary (bytes of a half-read message are handed over). Routers wait on TCP | |
| 3 | Flushes everything pending to Kafka | |
| 4 | Writes the state file `<lock file without .lock>.state` (e.g. `run/bongsudae.state`): routers / peers (Add-Path, negotiated families, EoR, history ...), row sequence number, connections (fd, number, pending bytes), 1-minute statistics, route tables | |
| 5 | exec (same pid, same options + `-resume <state file>`) | Reads the config and rules files again, connects to Kafka, restores routers / peers |
| 6 | | Takes the connections over and continues reading from the pending bytes = **end of the pause** |
| 7 | | Loads the route tables in the background. UPDATEs arriving meanwhile are queued per peer and applied after its table is loaded. The state file is removed at the end |

- Pause: about 0.4 s with 8 million routes (test); 5.5 to 6.4 s with a full table of 77 million routes (2.4 GB state file). Mostly step 4 (about 4 s writing the route tables) and the start of the new process (about 2 s, Kafka connection etc.)
- Kafka output does not depend on the route tables, so it does not wait for step 7. During step 7 (about 9 s at full size) route counts and prefix lookups are incomplete (`handover.rib_loading` in `/api/stats`)
- Row sequence numbers and peer / router message numbers continue. No extra router / peer messages are sent (consumers see nothing)
- On failure in steps 1 to 5 (including a failed exec) the old process simply continues reading the paused connections (log `handover: aborted ...`, `handover.last_error` in `/api/stats`).
  If the new process cannot read the state after exec, it exits and systemd starts it again: the sessions drop and routers reconnect (the same as a restart, never worse)
- Configuration: the new process reads the config file from scratch, so "restart" keys change too. Exception: `bmp.listen` and `http.listen` keep the inherited sockets (a restart is needed; one log line).
  Do not change `kafka.prefix`, `kafka.key_buckets` or `bmp.chash` together with an upgrade (topics / keys would change for consumers mid-stream)
- The process handing over must already have this feature: upgrading from 0.1 to 0.2.0 is the last ordinary restart. Refused while the route tables of the previous handover are still loading (about a minute)
- State formats (0.3.0+): 1 = 0.2 (one route table per peer), 2 = 0.3 (route tables and counters per view). 0.3 reads both and writes the newest one the new binary reads,
  so 0.2 -> 0.3 and a rollback 0.3 -> 0.2 are both hitless (the rollback writes format 1; it is refused if a peer has two or more views, which format 1 cannot hold).
- Tested with 25 simulated routers sending continuously while the collector was upgraded every 6 s (31 times, alternating old and new releases): the Kafka output matched a single uninterrupted run row by row

## 7. Kafka output (details: docs/kafka.md)

| Topic | Contents | Key |
|---|---|---|
| `<prefix>.unicast_prefix` | Route add / del (32 tab-separated fields, several rows per message) | peer hash |
| `<prefix>.peer` | BGP session up / down (31 fields) | peer hash |
| `<prefix>.router` | Router init / term (12 fields) | router hash |
| `<prefix without .parsed>.bmp_raw` (default `openbmp.bmp_raw`) | Received BMP unchanged (`bmp.raw`, production mode only) | router hash |

- Message = header (`V: 1.7`, `C_HASH_ID`, `T`, `L`, `R`) + empty line + rows; with `kafka.format = json`, one JSON object per line and no header (kafka.md 2.1). bmp_raw headers are `V`, `C_HASH_ID`, `R_HASH`, `R_IP`, `L`.
- **Same key = same partition = order preserved**: unicast_prefix and peer messages for one neighbor arrive in the order sent. There is no ordering across topics.
- Unlike openbmpd, bmp_raw also carries messages that failed to parse, and forwarding rules do not apply to it.
- Forwarding rules can send unicast_prefix rows to other topics, or not at all (6.4).

---

## 8. HTTP API (details: docs/api.md, also served by the binary at `GET /api/doc`)

### 8.1 Address and access
- `http.listen`. Clients outside `http.allow` get 403. **In production, restrict `http.allow` to the hosts that call the API**
  (empty = everyone = only the firewall protects it).
- Protection against unauthenticated requests stopping the collector:
  - Concurrency: 4 requests that scan route tables (`.../prefixes`, `.../prefix`) and 16 others. No slot within 5 seconds = 503.
  - The request (headers and body) must arrive within 30 seconds; a client that reads nothing of the response for 1 minute
    is disconnected (downloads that keep reading finish).
  - A prefix search holds at most 2,000,000 rows (limit given: trimmed as it goes; 413 beyond, e.g. `0.0.0.0/0` more without limit). Identical queries are searched once.
  - POST search requires `Content-Type: application/json` (415), so it cannot be forged through a browser.
- Reads (GET) are open to everyone allowed. Changes (rule PUT / DELETE / reload, reset) need the header `X-API-Key` = contents of the `paths.token` file (at least 16 characters; without the file, changes are disabled). The optional header `X-Actor` (printable ASCII, up to 64 characters) is written to the log.

### 8.2 Main endpoints
| Task | Endpoint |
|---|---|
| Health (per-minute stats for 60 minutes, release / version, memory, Kafka) | `GET /api/stats` |
| Prometheus | `GET /metrics` (the same numbers in the Prometheus text format, api.md section 2) |
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

The table lists the fixed parts of each line.

| When | Line (example) | Meaning |
|---|---|---|
| start | `config /etc/bongsudae/bongsudae.conf : paths.rules=... bmp.allow=...` | Config file read and values applied (a key overridden on the command line shows `(command line -option)`) |
| start | `start bongsudae 0.2.0 (<rev>) input=tcp prefix=openbmp.parsed http="127.0.0.1:5001" rules=...` | Release, git revision, input, topics, API, rules file |
| start | `rules: default unicast_prefix, routers 0` | Rules loaded |
| start | `http api ...`, `bmp_raw -> openbmp.bmp_raw`, `listen :5000 allow [192.0.2.0/24]` | Listening addresses, allowed ranges |
| start | `lock ... : already running (pid N), exiting` | Another instance holds the lock; systemd retries every `RestartSec` |
| connection | `conn 192.0.2.20: initiation` / `conn ... closed after N msgs: ...` | Router connected / disconnected |
| connection | `conn 203.0.113.5 refused : not in bmp.allow (N more since last line)` | (0.3.0+) Closed because the address is outside bmp.allow. One line per address per minute; the count of further refusals goes on the next line. If a router does not connect, check bmp.allow here |
| connection | `conn 203.0.113.5 refused : 4 connections from this address already waiting for initiation` / `... 64 connections already waiting for initiation (all addresses)` | No free slot for connections waiting for Initiation (many scanner connections). A router gets in again within 10 s |
| connection | `conn 203.0.113.5: initiation, but already 2000 routers connected (maxRouters), drop` | Router connection limit |
| connection | `conn ...: first message type N (not initiation), drop`, `too many connections, drop ...` | Closed by protection (6.1) |
| processing | `router ...: peer_up ... without OPEN (RFC 7854 violation) ...` | Router violates the standard; the message is still processed |
| every minute | `stat routers=3 route_mon=47346 errors=0 add=63722 del=11101 msgs=47346 ... raw=47344 kafka_fail=0 topics: unicast_prefix=47343/74820` | See the table below. (0.3.0+) `family_rows=N` when address family rows were produced; `other_families: bgp-ls=3,rt-constraint=1` at the end when families were dropped |
| family topic | `kafka topic openbmp.parsed.l3vpn : first l3vpn route, topic ready` | (0.3.0+) the first route of that family created its topic (once per process) |
| Kafka | `... : N times (last minute)` | Kafka error summary: N times in the last minute (one line per error type) |
| Kafka | `kafka produce failing for more than 2m0s : exiting (systemd restarts)` | No successful produce for 2 minutes, exiting (6.3) |
| API | `reset: router ... closed (from ... actor=...)`, `rules: ... (from ... actor=...)`, `rules: wrong key ...` | Who changed what / wrong key |
| reload | `config reloaded (..., SIGHUP) : bmp.allow "..." -> "..." (closed N connections of removed routers)` | Config reloaded: changed keys (N connections of removed routers closed) |
| reload | `config reloaded : restart needed for (running values kept) : ...` | Restart-only keys changed; current values kept (0.3.0+: durations compare by length, `90m` equals `1h30m0s`) |
| reload | `config reload failed, running values kept : ...` | Config reload failed; nothing changed |
| reload | `rules reload (SIGHUP): N changed` / `rules   ...` | Rules file re-read: N changes, then the changed rules |
| Kafka | `kafka topic ... : unknown topic (deleted / re-created?), re-reading topic metadata` (shadow: `re-subscribing`) | A topic was re-created; recovered without restart (6.3) |
| stop | `signal : stop accepting, close router connections`, `shutdown : N router connections closed`, `shutdown : flush kafka and exit` | stop / restart |
| conn | `conn <ip>: new connection replaces the old one (closed)` | A new connection from the same router replaced the old one |
| upgrade | `handover: start (SIGUSR2), checking /var/lib/bongsudae/bongsudae` | Old process: start (6.7) |
| upgrade | `handover: paused 25 connections in 4ms, kafka flushed in 1ms, state run/bongsudae.state (2434 MB, 76937952 routes) written in ...: exec ...` | Old process: paused, flushed, state written, now exec |
| upgrade | `start bongsudae 0.2.1 (...) (handover) ...` then `handover: resumed from 0.2.0 ... : 25 connections, 25 routers, 1331 peers, routers paused ...` | New process: took over, how long routers were paused |
| upgrade | `handover: route tables loaded : N routes in ...` | New process: route tables loaded |
| upgrade | `handover: aborted, old process keeps running : ...` | Gave up, the old process keeps running. Reason: new binary check, connections that did not pause, Kafka, state file, exec |
| upgrade | `handover: bmp.listen ... ignored, keeping the inherited socket (needs a restart)` | The listen address in the config changed: only a restart changes it |

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
| Upgrade | Test the new build (`go test .`, a shadow run, or file mode on a bmp_raw dump, comparing output with the old release) -> inform consumers -> keep the old binary (e.g. `bongsudae.prev`) -> install the new binary at the same path -> **`bongsudae -upgrade`** (0.2.0+, 6.7, no sudo needed). Only when 0.1 is running: `sudo systemctl restart bongsudae` |
| Roll back | Put the previous binary back and `bongsudae -upgrade` (works when the previous binary reads a state format this one writes, e.g. 0.3 -> 0.2), otherwise `sudo systemctl restart bongsudae` |
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
| Does not start | `systemctl status bongsudae` and the log: config error (line number), `-http ...: address already in use` (another process holds the API port), `lock ... already running` (an instance started by hand is running) |
| A router does not connect | Router BMP config, firewall port 5000, `bmp.allow` (outside = closed without a log line; the default allows only localhost), `first message type` in the log |
| `routers=` drops | `conn ... closed after` in the log (router restart / link). When it reconnects, only that router resyncs |
| `errors=` is not 0 | `router ...: ...` lines in the log (up to 100 per router per minute). Only that message is dropped. Reproduce with file mode from a bmp_raw dump |
| `kafka_fail=` is not 0 | Kafka status and the Kafka summary lines. After 2 minutes without success Bongsudae restarts itself |
| Low prefix count / `synced` false | Shortly after start a resync is still running. A neighbor that never sends EoR is never marked synced (its routes still arrive); look at `settled` (no new prefix for 5 minutes) |
| Large consumer lag | Normal right after a resync. If it stays high, check the consumers |
| API 403 | Client outside `http.allow`, or wrong / missing key (the body gives the reason) |
| API 409 | The rules file was edited by hand and not reloaded -> run `bongsudae -reload` first |
| API 429 | Reset of the same router again within 10 minutes (see `Retry-After`) |
| `-upgrade` says `aborted` | The reason is printed (also `handover.last_error` in `/api/stats` and the log). The old process keeps running. If the new binary failed `-handover-check` / `-t`, fix that binary; `still loading` = try again in a minute |
| `-upgrade` says `cannot hand over` | The running release is 0.1 (no hitless upgrade): restart once with `sudo systemctl restart bongsudae` |
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
| `kafka.key_buckets` | `-key-buckets` | restart |
| `kafka.format` | `-format` | restart |
| `kafka.partitions` | `-partitions` | restart |
| `kafka.replication` | `-replication` | restart |
| `kafka.retention` | `-retention` | restart |
| `bmp.rib` | `-rib` | restart |
| `http.listen` | `-http` | restart |
| `http.allow` | `-http-allow` | live |

Command-line only options: `-c`, `-input`, `-src`, `-group`, `-from-start`, `-in`, `-out`, `-t`,
`-status`, `-set`, `-del`, `-post`, `-reload`, `-topics`, `-json`, `-api`, `-upgrade`, `-resume`, `-handover-check`, `-handover-formats`.
