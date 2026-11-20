<p align="center"><img src="docs/logo/bongsudae_logo.png" alt="Bongsudae" width="320"></p>

# Bongsudae

The Bongsudae was a beacon tower of old Korea, relaying news from hill to hill.
This one relays news from your routers.

Bongsudae is a BMP (BGP Monitoring Protocol, RFC 7854) collector written in Go.
Routers send BMP to it over TCP; it keeps the routes of every peer in memory,
publishes them to Kafka in the OpenBMP v1.7 message format (or JSON) and answers
questions about them over an HTTP API.

It is a single binary meant as a drop-in replacement for the OpenBMP
collector (`openbmpd`) for consumers that read the `openbmp.parsed.*` topics.
Where openbmpd's output disagrees with the RFCs, Bongsudae follows the RFC
and documents the difference
([docs/kafka.md](docs/kafka.md)).

Bongsudae runs in production with a few dozen routers and about 1,700 peers
(77 million routes). It is new software : please compare it with your current
collector before relying on it. See [CHANGELOG.md](CHANGELOG.md) for what changed
in each release.

## Features

- **BMP v3** : Initiation, Termination, Peer Up / Down, Route Monitoring
  (all views), Statistics Reports (shown per peer). 4-octet AS,
  Add-Path, MP_REACH / MP_UNREACH (IPv4 routes in MP_REACH too), AS4_PATH,
  AS4_AGGREGATOR, End-of-RIB.
- **Address families** : IPv4 / IPv6 unicast, labeled unicast, multicast,
  VPNv4 / VPNv6, EVPN, FlowSpec, BGP-LS, VPLS, MVPN, SR Policy and RT Constraint,
  each in its own Kafka topic; any other family goes to `other_family` as NLRI hex.
  Nothing is dropped. Routing tables (for the API) cover unicast, labeled unicast,
  multicast, VPN and FlowSpec.
- **Views** : Adj-RIB-In pre / post policy, Adj-RIB-Out (RFC 8671) and Loc-RIB
  (RFC 9069), each with its own routing table; Add-Path (one route per path ID).
- **Kafka output** : OpenBMP v1.7 `unicast_prefix`, `peer`, `router` messages
  (same headers, field order and hashes as openbmpd), or the same data as JSON
  lines (`kafka.format = json`), plus `bmp_raw` = every received BMP message
  unchanged. The messages of one peer stay in order; `kafka.key_buckets` spreads
  a big peer over several partitions while keeping the order per prefix.
- **Forwarding rules** : choose the `unicast_prefix` topics per router or per
  peer (or none), changed at run time through the API.
- **HTTP API** : routers, peers, per-peer routing tables, prefix lookup
  (exact / longest / covering / more specific, origin AS filters, many prefixes
  in one POST), one-minute statistics, BMP session reset, initial-table progress
  (`synced` by End-of-RIB, `settled` for routers that do not forward it).
  Prometheus metrics at `/metrics`. The command line client is built in
  (`bongsudae -status ...`).
- **Hitless upgrade** : `bongsudae -upgrade` hands the router sessions and the
  routing tables to the new binary (same pid). Routers do not reconnect and do not
  resend their tables; if anything fails the old process keeps running.
- **Robust input** : allow list, first message must be Initiation within 10 s,
  limited connections waiting for Initiation (per address and in total),
  1 MB message limit, a bad message affects only itself (never the session or
  the process), one goroutine per router, a new session from a router replaces
  the old one. Port scanners and broken BMP do not stop the collector.
- **Testing a new release** : a second instance can read `openbmp.bmp_raw`
  (`-input kafka`) and write to test topics, or replay a dump file
  (`-input file`), so two releases can be compared on the same input.
- **Operations** : configuration file with `systemctl reload`, systemd watchdog,
  one stat line per minute, recovers from Kafka restarts and re-created topics,
  routing tables per family are off by default and turned on per family (`bmp.rib`), to save memory.
- **Dependencies** : Go standard library and [franz-go](https://github.com/twmb/franz-go)
  (Kafka client).

## Install

Linux (the hitless upgrade and systemd integration are Linux only).

**Binary** (0.3.0+) : download `bongsudae_<version>_linux_amd64.tar.gz` or
`..._linux_arm64.tar.gz` from [Releases](https://github.com/yunsog/bongsudae/releases).
Static binaries (no C library), so they run on any Linux distribution.
The arm64 binary is not yet tested on real hardware.

```
tar xzf bongsudae_0.3.0_linux_amd64.tar.gz
sha256sum -c --ignore-missing SHA256SUMS          # SHA256SUMS from the same release
sudo install -m 755 bongsudae_0.3.0_linux_amd64/bongsudae /usr/local/bin/bongsudae
```

The archive also has README, CHANGELOG, LICENSE, NOTICE and the example configuration
and systemd unit.

**From source** : Go 1.26 or later.

```
CGO_ENABLED=0 go install github.com/yunsog/bongsudae@latest     # binary in $(go env GOPATH)/bin/bongsudae
```

or from a clone :

```
git clone https://github.com/yunsog/bongsudae.git
cd bongsudae
CGO_ENABLED=0 go build -o bongsudae .   # static : libc updates do not restart it (needrestart)
go test ./...
```

## Quick start

```
# Kafka on 127.0.0.1:9092, routers in 192.0.2.0/24, HTTP API on 127.0.0.1:5001
./bongsudae -allow 192.0.2.0/24 -kafka 127.0.0.1:9092 -http 127.0.0.1:5001 \
            -rib ipv4-unicast,ipv6-unicast

# on the router : BMP to <this host> port 5000

# what is connected
./bongsudae -status routers
./bongsudae -status routers 192.0.2.1
./bongsudae -status prefix p=203.0.113.0/24 match=covering

# Prometheus (Grafana dashboard example: deploy/grafana/bongsudae.json)
curl -s 127.0.0.1:5001/metrics
```

Output topics : `openbmp.parsed.unicast_prefix`, `openbmp.parsed.peer`,
`openbmp.parsed.router`, one topic per other address family (`l3vpn`, `evpn`,
`flowspec`, `ls_node` ...) and `openbmp.bmp_raw`. Routing tables are off by
default : turn them on with `-rib` or `bmp.rib` to use the route lookups.

For a systemd installation see `deploy/bongsudae.service.example`,
`deploy/bongsudae.conf.example` and the manual. To move to a new release
without a resend : build it at the same path, then `./bongsudae -upgrade`.

## Documentation

- [User manual](docs/manual.md) : install, options, configuration, operation, upgrade, troubleshooting
- [HTTP API](docs/api.md) (also served by the collector at `/api/doc`), including `/metrics`
- [Kafka messages](docs/kafka.md) : topics, fields, JSON format, hashes, differences from openbmpd
- Introduction slides : [English](docs/bongsudae_v0.4_en.pdf), [Korean](docs/bongsudae_v0.4_ko.pdf)

Source code comments are in Korean; command line help, log messages and API errors are in English.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
Releases 0.3.0 and earlier were published under the ISC license and remain so.

The Kafka message format and the text form of attributes (for example the
extended community names `rt=`, `soo=`) follow OpenBMP so that existing
consumers keep working. Bongsudae contains no OpenBMP code.
