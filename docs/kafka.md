# Bongsudae Kafka messages

This document describes the messages Bongsudae (release 0.2) writes to Kafka.
The format is the OpenBMP Message Bus API **v1.7**, the same as the output of openbmpd 2.2.3,
so consumers written for openbmpd can read Bongsudae output without changes.
Where openbmpd does not follow the RFCs, Bongsudae follows the RFCs; those cases are listed in section 7.

See also: the web API reference (`api.md`) and the user manual (`manual.md`).

---

## 1. Topics

| Topic (default prefix) | Content | Key |
|---|---|---|
| `openbmp.parsed.unicast_prefix` | IPv4 / IPv6 unicast route lines (add / del) | peer hash |
| `openbmp.parsed.peer` | peer up / down | peer hash |
| `openbmp.parsed.router` | router init / term (BMP session start / end) | router hash |
| `openbmp.bmp_raw` | BMP messages exactly as received from the router (not parsed) | router hash |

- **Same key = same partition = order is kept.** The unicast_prefix and peer messages of one peer are read in the order they were sent.
  Order across topics is not guaranteed (for example, a peer down and that peer's unicast_prefix lines).
- **Splitting the unicast_prefix key (0.2.0+, config `kafka.key_buckets` / option `-key-buckets`, default 0 = one key per peer)** :
  with N > 0 every row goes to bucket FNV-1a("prefix/length") % N, rows of one bucket form one message, and the key is `<peer hash>.<bucket>`.
  One prefix of one router + peer always gets the same key, so its order is kept, while a big full-table peer is spread over several partitions.
  One UPDATE then becomes several messages (one per bucket, adds before deletes as before); the rows themselves (all 32 fields, seq included) do not change.
  Order across different prefixes of one peer is no longer guaranteed. Changing it needs a restart; coordinate with consumers, since one prefix can briefly sit in two partitions.
  - **Bongsudae chooses the partition itself** instead of leaving it to Kafka's murmur2(key): for a key `<peer hash>.<bucket>` the partition is (murmur2(peer hash) + bucket) % number of partitions,
    so the buckets of one peer sit on neighbouring partitions, one each. Other keys (peer, router, bmp_raw messages) use Kafka's default murmur2(key), i.e. the same partition as without buckets.
    Measured on a full routing table (78.9 M rows, 8 partitions), the largest / smallest partition share was 1.73 with one key per peer, 1.21 with bucket keys hashed by Kafka, and 1.00 with this method.
- The topic prefix is set with `-prefix` (config key `kafka.prefix`); the default is `openbmp.parsed`.
  For a test instance use e.g. `-prefix openbmp.test` (topics `openbmp.test.unicast_prefix`, ...).
- The bmp_raw topic name is the prefix with a trailing `.parsed` removed, plus `.bmp_raw`
  (`openbmp.parsed` -> `openbmp.bmp_raw`, `openbmp.test` -> `openbmp.test.bmp_raw`).
- **Forwarding rules** (section 1.1) can send unicast_prefix messages of a given router / peer to other topics, or not at all.
  peer, router and bmp_raw messages are always sent, regardless of the rules.
- No compression (so consumers can read without a compression library).
- openbmpd's `collector`, `bmp_stat`, `base_attribute`, `l3vpn`, `evpn` and `ls_*` topics are not produced.
  Routes that are not IPv4 / IPv6 unicast (VPN, EVPN, BGP-LS, ...) are counted and dropped.

### 1.1 Forwarding rules (unicast_prefix only)

Rules choose the topic(s) for unicast_prefix messages. They are looked up most specific first:

```
peer rule (router IP + peer IP)  ->  router rule (router IP)  ->  default
```

- A rule value is a list of topic names. Each name must start with `unicast_prefix`
  (pattern `^unicast_prefix[A-Za-z0-9_.-]{0,64}$`); the actual topic is `<prefix>.<name>`, e.g. `openbmp.parsed.unicast_prefix1`.
  A topic is created the first time it is used.
- An empty list `[]` means "do not send" (the rule exists). Deleting a rule means "no rule" (the next level applies).
  Deleting a router rule keeps that router's peer rules. The default rule cannot be deleted.
- Without a rules file the default is `["unicast_prefix"]`, i.e. every line goes to `<prefix>.unicast_prefix`.
- A changed rule applies to UPDATEs received after the change; the current RIB is not re-sent to a new topic.
- Rules are stored in a JSON file (`paths.rules`) and changed through the web API (`/api/kafka/rules`, see `api.md`)
  or by editing the file and reloading. File shape:

```json
{
  "default": ["unicast_prefix"],
  "routers": {
    "192.0.2.1": { "topics": ["unicast_prefix1"], "peers": { "198.51.100.7": ["unicast_prefix2"] } },
    "192.0.2.2": { "topics": [] }
  }
}
```

---

## 2. Message layout (parsed topics)

```
V: 1.7
C_HASH_ID: 5d0c8e2a4b7f19c3e6a1d4f80b2c9e57
T: unicast_prefix
L: 385
R: 1

<line 1>
<line 2>
...
```

| Header | Meaning |
|---|---|
| V | format version (`1.7`) |
| C_HASH_ID | collector hash: option `-chash` (config key `bmp.chash`, 32 hex digits). Empty (default) = md5 of the host name, as openbmpd does when `admin_id` is not set |
| T | message type: `unicast_prefix` / `peer` / `router` |
| L | number of bytes of the body (after the empty line) |
| R | number of lines in the body |

- The header ends at the first empty line (`\n\n`). Each body line ends with `\n`; fields are separated by tab (`\t`).
- Inside a field, `\r` is removed, a newline becomes the two characters `\n`, and a tab becomes a space (e.g. router description).
- Timestamps are UTC with microseconds (`2026-01-15 05:05:36.749433`).
  unicast_prefix and peer use the time in the BMP per-peer header; if that is 0, the time the message was received.
  router messages use the time the message was received.
- With `-input kafka` (re-processing a bmp_raw topic), C_HASH_ID and the router hash are taken from the bmp_raw record header.

### 2.1 JSON format (option `-format json`, config `kafka.format = json`, default `openbmp`)

For new consumers that do not read the openbmp format. Topic names, Kafka keys and message splitting (one UPDATE = one add message + one del message, `kafka.key_buckets`)
are the same as above; **only the message value differs**: no header (V / C_HASH_ID / T / L / R), one JSON object per line (NDJSON, each line ends with `\n`). The topic tells the type.

```
{"action":"add","seq":2830337,"hash":"01f2...","router_hash":"e306...","router_ip":"192.0.2.162","base_attr_hash":"4f35...","peer_hash":"ec4a...","peer_ip":"198.51.100.20","peer_asn":64500,"timestamp":"2026-11-01 09:42:10.123456","prefix":"203.0.113.0","prefix_len":24,"is_ipv4":true,"origin":"igp","as_path":"64500 64510","as_path_count":2,"origin_as":64510,"nexthop":"198.51.100.20","med":0,"local_pref":100,"aggregator":"","community_list":"64500:1","ext_community_list":"","cluster_list":"","is_atomic_agg":false,"is_nexthop_ipv4":true,"originator_id":"","path_id":0,"labels":"","is_pre_policy":true,"is_adj_rib_in":true,"large_community_list":""}
{"action":"del","seq":2830338,"hash":"54f4...","router_hash":"e306...","router_ip":"192.0.2.162","peer_hash":"ec4a...","peer_ip":"198.51.100.20","peer_asn":64500,"timestamp":"2026-11-01 09:42:10.123456","prefix":"198.51.100.0","prefix_len":24,"is_ipv4":true,"path_id":0,"labels":"","is_pre_policy":true,"is_adj_rib_in":true}
```

- Field names are the names in the tables of sections 3, 4 and 5, in the same order (peer flags in snake_case: `is_l3vpn`, `is_pre_policy`, `is_ipv4`, `is_loc_rib`, `is_loc_rib_filtered`).
- Values are the openbmp text with a type: numeric fields are JSON numbers, 0 / 1 flags are `true` / `false`, `origin_as` is a number
  (or the text `"{a,b}"` when the path ends with an AS_SET of two or more ASes), `as_path` / `cluster_list` have no leading or trailing space. Other fields (communities etc.) are the same text as in the openbmp format.
- unicast_prefix `del` lines have no attribute fields (base_attr_hash, origin ... originator_id, large_community_list).
- router / peer: empty numeric or flag fields are omitted (e.g. `local_asn` of a Peer Up without OPENs, `bmp_reason` of an up). Empty text fields are `""`.
- Quotes, backslashes and control characters are escaped as JSON requires; invalid UTF-8 becomes `\ufffd`. A newline in a router description is the two characters `\n` as in the openbmp format.
- bmp_raw is unchanged (BMP bytes). Changing the format needs a restart; with `-upgrade` the new format starts at that moment, so coordinate with consumers.
- Message splitting is the same, so the ordering guarantee per router + peer + prefix is the same.

---

## 3. unicast_prefix (32 fields)

One BGP UPDATE produces **at most one add message and one del message**. add and del lines are never mixed in one message;
the add message comes first. Within a message, IPv4 lines come before IPv6 lines.

| # | Name | Example | Meaning |
|---|---|---|---|
| 0 | action | `add` | `add` (announce) / `del` (withdraw) |
| 1 | seq | `1052` | line number, increases by 1 per line across the whole collector, starts from 1 after a restart |
| 2 | hash | `8f44...` | md5(prefix + prefix length as 1 byte + peer_hash) |
| 3 | router_hash | `7f64...` | router hash (section 8) |
| 4 | router_ip | `192.0.2.49` | router address (BMP session source) |
| 5 | base_attr_hash | `c882...` | hash of the path attributes (section 8) |
| 6 | peer_hash | `84c3...` | md5(peer_ip + peer_rd + router_hash) |
| 7 | peer_ip | `2001:db8::20:63` | peer address |
| 8 | peer_asn | `64500` | peer AS |
| 9 | timestamp | `2026-01-15 05:05:36.749433` | UTC |
| 10 | prefix | `2001:db8:100::` | prefix address |
| 11 | prefix_len | `48` | prefix length |
| 12 | isIPv4 | `0` | 1 = IPv4, 0 = IPv6 |
| 13 | origin | `igp` | `igp` / `egp` / `incomplete` |
| 14 | as_path | ` 64496 64497 64498 64511` | **leading space**, one space before each AS. AS_SET: ` {64510,64511}` (braces, comma inside). AS_CONFED_SEQUENCE: ` (65001 65002)`. AS_CONFED_SET: ` [65001,65002]` |
| 15 | as_path_count | `4` | path length per RFC 4271 9.1.2.2: an AS_SET counts as 1; confederation segments count as 0 (RFC 5065) |
| 16 | origin_as | `64511` or `{64510,64511}` | last AS of the last AS_SEQUENCE. **If the path ends with an AS_SET of two or more ASes, the AS_SET text** (RFC 6811: the origin cannot be determined), so this field is not always a number. A set of one AS (`{64512}`) gives that AS (`64512`, 0.2.0+) |
| 17 | nexthop | `2001:db8::20:234` | next hop. IPv4 routes use NEXT_HOP if present, otherwise the MP_REACH next hop. For IPv6, the global address (a link-local second address is not shown) |
| 18 | med | `1000` | MULTI_EXIT_DISC (`0` if absent) |
| 19 | local_pref | `70` | LOCAL_PREF (`0` if absent) |
| 20 | aggregator | `64499 192.0.2.9` | `AS address` |
| 21 | community_list | `64500:8300 64500:8400` | `high:low`, space separated |
| 22 | ext_community_list | `rt=64500:10 soo=64500:600` | space separated, format in 3.1 |
| 23 | cluster_list | `192.0.2.63 192.0.2.61 ` | **trailing space** after each address |
| 24 | isAtomicAgg | `0` | always 0 (as openbmpd) |
| 25 | isNexthopIPv4 | `0` | 1 if the next hop is an IPv4 address |
| 26 | originator_id | `192.0.2.28` | ORIGINATOR_ID |
| 27 | path_id | `0` | ADD-PATH path identifier (RFC 7911) when Add-Path was negotiated in the Peer Up OPENs, otherwise 0 |
| 28 | labels | | always empty |
| 29 | isPrePolicy | `1` | 1 unless the per-peer header L flag (post-policy) is set |
| 30 | isAdjRibIn | `1` | 1 unless the per-peer header O flag (Adj-RIB-Out) is set or the peer type is Loc-RIB |
| 31 | large_community_list | `64500:1:2` | `global:local1:local2`, space separated |

- **del lines**: fields 5 and 13 ~ 26 and 31 are empty (a withdrawal carries no attributes). path_id, isPrePolicy and isAdjRibIn are filled.
- **No del lines are sent when a peer goes down.** On a peer `down` (or a router `term`, section 5) the consumer must remove all
  routes of that peer itself (same as openbmpd).
- AS4_PATH is merged into AS_PATH when the session uses 2-byte AS numbers (RFC 6793).

Example (one add line; hashes shortened, fields separated by tabs):

```
add	1052	8f44...	7f64...	192.0.2.49	c882...	84c3...	2001:db8::20:63	64500	2026-01-15 05:05:36.749433	2001:db8:100::	48	0	igp	 64496 64497 64498 64511	4	64511	2001:db8::20:234	1000	70		64500:8300 64500:8400		192.0.2.63 192.0.2.61 	0	0	192.0.2.28	0		1	1	
```

### 3.1 ext_community_list format (RFC 4360, 5668, 7153, IANA registry)

Each 8-byte extended community becomes one token; tokens are separated by one space.
The names (`rt=`, `soo=`, ...) are the same as openbmpd's. Non-transitive types (0x40 bit of the type byte) are formatted
the same way as the transitive ones.

| Kind | Text | Example |
|---|---|---|
| route target / route origin | `rt=AS:N`, `soo=AS:N` (IPv4 type: `rt=address:N`) | `rt=64500:10 soo=64500:600` |
| OSPF domain id / router id / route type (RFC 4577, including the old 0x8000 / 0x8001 / 0x8005 types) | `ospf-did=AS:N`, `ospf-rid=address:N`, `ospf-rt=area-N:TYPE:OPTIONS` (TYPE = O, IA, E, N or a number) | `ospf-did=0:66048 ospf-rt=area-0:E:1 ospf-rid=192.0.2.238:0` |
| connected-to / source AS / VRF route import (RFC 6514) | `colc=AS:N`, `sas=AS:N`, `import=address:N` | `sas=64501:0 import=192.0.2.245:0` |
| L2VPN id / Cisco VPN id | `l2vpn-id=G:0xHEX`, `vpn-id=G:0xHEX` | |
| P2MP next hop | `p2mp-nh=G:N` | |
| link bandwidth (2-byte AS type) | `link-bw=AS:bytes-per-second` (value decoded as IEEE 754 float) | `link-bw=64502:250000` (= 2 Mbit/s) |
| origin validation state (RFC 8097) | `ov=valid` / `ov=not-found` / `ov=invalid` | |
| cost community | `cost=POI:ID:VALUE` (POI = abs, igp, ext, bgp_id or a number) | |
| CP-ORF / default gateway | `cp-orf`, `default-gw` | |
| color / encapsulation (RFC 9012) | `color=N`, `encap=TUNNEL-TYPE` (2 bytes) | `color=1000`, `encap=8` |
| EVPN (RFC 7432) | `mac-mob=FLAGS:SEQ`, `esi-label=FLAGS:LABEL`, `es_import=MAC`, `router_mac=MAC` | `router_mac=00:1a:0b:c0:ff:01` |
| flow spec (RFC 8955) | `flow-rate=AS:bytes-per-second` (IEEE float), `flow-act=` + `S` / `T`, `flow-redir=G:N`, `flow-remark=DSCP` | |
| layer 2 info (RFC 4761) | `l2info=ENCAP:FLAGS:mtu:MTU` | |
| anything else (unknown) | `0xTTSS:` + the 6 value bytes in hex (TT = type, SS = sub-type) | `0x0043:006400000001` |

---

## 4. peer (31 fields)

| # | Name | up example | Meaning |
|---|---|---|---|
| 0 | action | `up` | `up` / `down` |
| 1 | seq | `0` | message number, starts from 0 for each BMP session of the router |
| 2 | hash | `437e...` | peer hash (= peer_hash of unicast_prefix, the Kafka key) |
| 3 | router_hash | `51fc...` | |
| 4 | name | (empty) | always empty (section 7) |
| 5 | remote_bgp_id | `203.0.113.34` | peer BGP identifier (from the per-peer header) |
| 6 | router_ip | `192.0.2.100` | |
| 7 | timestamp | `2026-01-15 23:46:22.487471` | UTC |
| 8 | remote_asn | `64501` | |
| 9 | remote_ip | `198.51.100.99` | peer address |
| 10 | peer_rd | `0:0` | route distinguisher (`0:0` for a global instance peer). Type 0 `AS:N`, type 1 `address:N`, type 2 `AS4:N` |
| 11 | remote_port | `179` | up only |
| 12 | local_asn | `64500` | up only (router side AS, from the router's OPEN) |
| 13 | local_ip | `198.51.100.98` | up only |
| 14 | local_port | `30719` | up only |
| 15 | local_bgp_id | `192.0.2.100` | up only (from the router's OPEN) |
| 16 | info_data | | always empty |
| 17 | adv_cap | `MPBGP (1) : afi=1 safi=1 : Unicast IPv4, Route Refresh (2), ...` | capabilities in the OPEN sent by the router (up only), `, ` separated |
| 18 | recv_cap | `Graceful Restart (64), MPBGP (1) : ...` | capabilities in the OPEN received from the peer (up only) |
| 19 | remote_holddown | `90` | hold time in the peer's OPEN (up only) |
| 20 | adv_holddown | `180` | hold time in the router's OPEN (up only) |
| 21 | bmp_reason | `4` | down only (RFC 7854 4.9): 1 local NOTIFICATION, 2 local close without NOTIFICATION, 3 remote NOTIFICATION, 4 remote close without NOTIFICATION, 5 peer de-configured |
| 22 | bgp_error_code | `6` | down only. NOTIFICATION error code for bmp_reason 1 / 3; `0` for bmp_reason 2 / 4 |
| 23 | bgp_error_sub_code | `6` | down only. NOTIFICATION sub-code; `0` for bmp_reason 2 / 4 |
| 24 | error_text | `Other configuration change` | down only, see below |
| 25 | isL3VPN | `0` | 1 if the per-peer header peer type is RD instance (1) |
| 26 | isPrePolicy | `1` | 1 unless the L flag is set |
| 27 | isIPv4 | `1` | address family of the peer address |
| 28 | isLocRIB | `0` | 1 if the peer type is Loc-RIB (3) |
| 29 | isLocRIBFiltered | `0` | 1 if Loc-RIB and the F flag (0x80) is set |
| 30 | table_name | | always empty |

error_text (down):

- bmp_reason 1 / 3: the NOTIFICATION error name (RFC 4271), e.g. `Hold timer expired`. For code 6 (Cease) the sub-code name
  (RFC 4486), e.g. `Administratively shutdown`, `Peer de-configured`, `Other configuration change`; `Cease` if the sub-code is unknown.
  Unknown codes: `Unknown <code>/<sub>`.
- bmp_reason 2: `Local (<router_ip>) closed peer (<peer_ip>) session: fsm_event=<FSM event>, No BGP notify message.`
- bmp_reason 4: `Remote peer (<peer_ip>) closed local (<router_ip>) session: No BGP notify message.`

These texts follow openbmpd. The bmp_reason 4 text is taken from the openbmpd source and was not seen in openbmpd output.

Notes:

- **Peer Up without OPEN messages.** Some routers (seen with IOS XR 6.2.3) send a Peer Up that ends after the ports and contains
  neither OPEN message (a violation of RFC 7854 4.10). Bongsudae keeps the session and sends a peer `up` with remote_port, local_ip
  and local_port filled, and local_asn, local_bgp_id, adv_cap, recv_cap and both hold times empty (see section 7).
- **When a router goes away (router `term`), no peer down messages are sent** (same as openbmpd).
  A consumer must treat all peers of that router as down.

---

## 5. router (12 fields)

| # | Name | init example | term example |
|---|---|---|---|
| 0 | action | `init` | `term` |
| 1 | seq | `0` | `1` (per BMP session of the router) |
| 2 | name | `rtr1` (sysName from the Initiation message) | `rtr1` (the same sysName) |
| 3 | hash | `51fc...` (router hash, the Kafka key) | same |
| 4 | ip | `192.0.2.100` | same |
| 5 | description | `Cisco IOS XR Software, Version 6.2.3[Default]\nCopyright ...` (sysDescr; newlines as the characters `\n`) | empty |
| 6 | term_code | `0` | see table below |
| 7 | term_reason | empty | see table below |
| 8 | init_data | Initiation String TLV (type 0) | empty |
| 9 | term_data | empty | Termination String TLV (type 0) |
| 10 | timestamp | `2026-01-15 23:46:29.595604` | time received |
| 11 | bgp_id | empty | empty |

If an Initiation / Termination message carries several TLVs of the same type, their values are joined with a space.

| term_code | term_reason | When |
|---|---|---|
| 0 ~ 4 | RFC 7854 4.5 names: `Session administratively closed`, `Unspecified reason`, `Out of resources`, `Redundant connection`, `Session permanently administratively closed` | the router sent a Termination message |
| 65534 | `Closed by collector: api reset` (+ ` actor=<X-Actor header>` if given) | the connection was closed by the web API reset |
| 65534 | `Closed by collector: not in bmp.allow (config reload)` | after a configuration reload, the router is no longer in the allowed address list |
| 65534 | `Closed by collector: shutdown` | (0.2.0+) Bongsudae is stopping (stop / restart); one per connected router. 0.1 sent no term on shutdown |
| 65535 | `Connection closed` | the TCP connection closed without a Termination message |

- (0.2.0+) When a router opens a new connection (Initiation) while an old one is still open, the old one is closed without a term: consumers see two `init` messages, the new session takes over.

---

## 6. bmp_raw

```
key   = router hash
value = V: 1.7\n
        C_HASH_ID: <collector hash>\n
        R_HASH: <router hash>\n
        R_IP: 192.0.2.38\n
        L: <number of BMP bytes>\n
        \n
        <one BMP message, bytes exactly as received>
```

- One record = one complete BMP message (never a fragment), in the order the router sent them
  (the key is the router hash, so one router stays in one partition).
- Uses: feeding the same stream to another BMP collector; testing a new Bongsudae build in parallel
  (`bongsudae -input kafka -src openbmp.bmp_raw -prefix openbmp.test`).
- Messages that fail to parse are sent too. Forwarding rules do not apply (all routers).
- Produced only with TCP input, when enabled (option `-raw`, config key `bmp.raw`, default on; can be switched by a configuration reload).
- Maximum BMP message size is 1 MB (plus the header); larger messages are treated as broken.

---

## 7. Differences from openbmpd

Bongsudae keeps openbmpd's message format (topics, headers, field order), but where openbmpd does not follow the standards,
Bongsudae follows the RFCs. Consumers that compare text with openbmpd output will see the differences below.

### 7.1 Extended communities (RFC 4360, 7153, IANA registry)

| Case | openbmpd | Bongsudae |
|---|---|---|
| unknown types | dropped (empty token, e.g. `'a  b'`) | kept as `0xTTSS:<value hex>`, e.g. `0x0043:006400000001` |
| non-transitive types (0x40 bit) | all dropped (the bit is not cleared because of C integer promotion) | formatted like the transitive type |
| old OSPF types 0x8000 / 0x8001 / 0x8005 (RFC 4577) | dropped (e.g. `'ospf-did=0:66048  '`) | `ospf-rt=`, `ospf-rid=`, `ospf-did=` |
| link bandwidth | IEEE float bits printed as an integer (`link-bw=64502:1215570944`) | decoded float, bytes per second (`link-bw=64502:250000`) |
| flow spec traffic-rate | integer converted to float | decoded IEEE float |
| IPv4-specific type: colc / sas / link-bw numbers | byte order reversed (little-endian) | network byte order |
| 4-byte AS type `import=` | split as if the global part were a 2-byte AS | `AS4:N` |
| L2VPN id (sub-type 0x0a) | printed as `vpn-id=` (same as the Cisco VPN id) | `l2vpn-id=` |
| EVPN MAC mobility / ESI label | contains a space (looks like two tokens); flags printed as a raw byte character | one token, flags as a number |
| encapsulation (RFC 9012) | only 1 byte of the tunnel type | 2-byte tunnel type |

### 7.2 AS_SET (RFC 4271, RFC 6811)

| Field | openbmpd | Bongsudae |
|---|---|---|
| as_path | ` { 64510 64511 }` | ` {64510,64511}` (one token when split on spaces) |
| as_path_count | each AS inside the set is counted | one AS_SET counts as 1 (RFC 4271 9.1.2.2); confederation segments count as 0 (RFC 5065) |
| origin_as when the path ends with an AS_SET | the last AS in the set (`{64510,64511}` -> `64511`) | the set text (`{64510,64511}`), because the origin cannot be determined (RFC 6811). A set of one AS (`{64512}`) gives `64512` (0.2.0+; 0.1 gave `{64512}`) |

### 7.3 Other differences

| Item | openbmpd | Bongsudae |
|---|---|---|
| peer name (peer field 4) | reverse DNS name of the peer address | always empty (no rDNS lookups) |
| router name in `term` | reverse DNS name of the router address | the sysName from the router's Initiation message (no rDNS lookups) |
| Peer Up without OPEN messages (RFC 7854 violation by the router) | fails to parse it, closes the whole router connection and emits router `term` 65534; no routes from that router are processed | keeps the connection, emits a peer `up` with empty capability / AS / hold time fields (section 4), logs one line and counts it in the statistics (`peer_up_no_open`) |
| router_hash | computed by a method that could not be reproduced from its inputs | md5(router IP + C_HASH_ID). Because peer_hash, hash, base_attr_hash and the Kafka keys are derived from it, these values differ from openbmpd's for the same router |
| bmp_raw | messages that fail to parse are not sent | every received message is sent |
| (0.2.0+) NLRI trailing bits after the prefix length (RFC 4271 4.3) | not checked | cleared, e.g. `10.1.1` with length 23 becomes `10.1.0.0/23` |
| (0.2.0+) AGGREGATOR / AS4_AGGREGATOR from a 2-octet AS peer (RFC 6793 4.2.3) | | AGGREGATOR AS 23456 : aggregator = AS4_AGGREGATOR; any other AS : AS4_AGGREGATOR and AS4_PATH are ignored |

With these rules applied to openbmpd output, Bongsudae's output was compared line by line with openbmpd's on the same input;
no other differences were found in unicast_prefix lines. peer messages are identical except for seq.

---

## 8. Hashes

All hashes are lower-case hex MD5 of the concatenated inputs (strings as text, no separators).

| Hash | Input |
|---|---|
| router_hash | router IP + C_HASH_ID (with `-input kafka`: the R_HASH of the bmp_raw record) |
| peer_hash | peer IP + peer RD text (e.g. `0:0`) + router_hash |
| hash (route line) | prefix address text + prefix length as 1 byte + peer_hash |
| base_attr_hash | as_path + nexthop + aggregator + origin + med + local_pref + community_list + ext_community_list + peer_hash, where med and local_pref are 4-byte little-endian integers (not text) |
| C_HASH_ID | option `-chash`; if empty, md5(host name) |

peer_hash, hash and base_attr_hash use the same method as openbmpd (checked against openbmpd output); only router_hash differs.
