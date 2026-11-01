<p align="center"><img src="docs/logo/bongsudae_logo.png" alt="Bongsudae" width="320"></p>

# Bongsudae

The Bongsudae was a beacon tower of old Korea, relaying news from hill to hill.
This one relays news from your routers.

Bongsudae is a BMP (BGP Monitoring Protocol, RFC 7854) collector written in Go.
Routers send BMP to it over TCP; it keeps the routes of every peer in memory,
publishes them to Kafka in the OpenBMP v1.7 message format and answers
questions about them over an HTTP API.

It is a single static binary meant as a drop-in replacement for the OpenBMP
collector (`openbmpd`) for consumers that read the `openbmp.parsed.*` topics,
without copying openbmpd's bugs : where openbmpd's output disagrees with the
RFCs, Bongsudae follows the RFC and documents the difference
([docs/kafka.md](docs/kafka.md)).

Bongsudae 0.1 runs in production with a few dozen routers and full-table peers.
It is new software : please compare it with your current collector before
relying on it.

## Features

- **BMP v3** : Initiation, Termination, Peer Up / Down, Route Monitoring
  (pre-policy Adj-RIB-In), Statistics counted. 4-octet AS, Add-Path,
  MP_REACH / MP_UNREACH (IPv4 routes in MP_REACH too), AS4_PATH, End-of-RIB.
- **Scope** : IPv4 / IPv6 unicast. Other address families (VPN, EVPN, BGP-LS ...)
  are counted and dropped.
- **Kafka output** : OpenBMP v1.7 `unicast_prefix`, `peer`, `router` messages
  (same headers, field order and hashes as openbmpd), plus `bmp_raw` = every
  received BMP message unchanged. Kafka key = peer hash, so the messages of one
  peer stay in order.
- **Forwarding rules** : choose the `unicast_prefix` topics per router or per
  peer (or none), changed at run time through the API.
- **HTTP API** : routers, peers, per-peer route tables, prefix lookup
  (exact / longest / covering / more specific, origin AS filters, many prefixes
  in one POST), one-minute statistics, BMP session reset. The command line client
  is built in (`bongsudae -status ...`).
- **Robust input** : allow list, first message must be Initiation within 30 s,
  1 MB message limit, a bad message affects only itself (never the session or
  the process), one goroutine per router. Port scanners and broken BMP do not
  stop the collector.
- **Testing a new release** : a second instance can read `openbmp.bmp_raw`
  (`-input kafka`) and write to test topics, or replay a dump file
  (`-input file`), so two releases can be compared on the same input.
- **Operations** : configuration file with `systemctl reload`, one stat line per
  minute, exits on purpose when Kafka is gone for 2 minutes (systemd restarts it).
- **Dependencies** : Go standard library and [franz-go](https://github.com/twmb/franz-go)
  (Kafka client).

## Install

Go 1.26 or later.

```
go install github.com/yunsog/bongsudae@latest     # binary in $(go env GOPATH)/bin/bongsudae
```

or from a clone :

```
git clone https://github.com/yunsog/bongsudae.git
cd bongsudae
go build -o bongsudae .
go test ./...
```

## Quick start

```
# Kafka on 127.0.0.1:9092, routers in 192.0.2.0/24, HTTP API on 127.0.0.1:5001
./bongsudae -allow 192.0.2.0/24 -kafka 127.0.0.1:9092 -http 127.0.0.1:5001

# on the router : BMP to <this host> port 5000

# what is connected
./bongsudae -status routers
./bongsudae -status routers 192.0.2.1
./bongsudae -status prefix p=203.0.113.0/24 match=covering
```

Output topics : `openbmp.parsed.unicast_prefix`, `openbmp.parsed.peer`,
`openbmp.parsed.router` and `openbmp.bmp_raw`.

For a systemd installation see `deploy/bongsudae.service.example`,
`deploy/bongsudae.conf.example` and the manual.

## Documentation

- [User manual](docs/manual.md) : install, options, configuration, operation, troubleshooting
- [HTTP API](docs/api.md) (also served by the collector at `/api/doc`)
- [Kafka messages](docs/kafka.md) : topics, fields, hashes, differences from openbmpd
- Introduction slides : [English](docs/bongsudae_v0.1_en.pdf), [Korean](docs/bongsudae_v0.1_ko.pdf)

Source code comments, command line help and log messages are in Korean.

## License

ISC. See [LICENSE](LICENSE).

The Kafka message format and the text form of attributes (for example the
extended community names `rt=`, `soo=`) follow OpenBMP so that existing
consumers keep working. Bongsudae contains no OpenBMP code.
