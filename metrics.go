// metrics.go : GET /metrics (0.2.0) = Prometheus 글자 꼴 (text exposition format 0.0.4), 라이브러리 없이.
//
// Prometheus 가 주기적으로 (보통 15 초 ~ 1 분) 이 주소를 불러 수를 모으고, Grafana 그래프 / 경보를 만든다.
// 내용은 /api/stats 와 /api/routers 의 수와 같음 :
//
//	bongsudae_*_total      시작 뒤 누적 (counter). 줄 / Kafka 수는 1 분 통계에서 더하므로 1 분마다 늘어남
//	그 밖                  지금 값 (gauge)
//
// 재시작 / 판 올리기 (-upgrade) 뒤에는 누적이 0 부터 = Prometheus 는 counter reset 으로 알아서 처리.
// 라우터 표시 (label) router = 주소, name = sysName. 라우터 수에 비례 (라우터 25 대 약 25 KB).
package main

import (
	"bufio"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// routerSnap : 라우터 하나의 지금 수 (r.mu 안에서 복사)
type routerSnap struct {
	ip, name           string
	up                 bool
	connects           int
	typeCount          [bmpTypeCount + 1]uint64
	errors, noOpen     uint64
	peersUp, peersDown int
	synced, settled    int
	routesV4, routesV6 int
	lastMsgAgo         float64 // 초 (-1 = 받은 적 없음)
}

func (c *collector) routerSnaps(now time.Time) []routerSnap {
	var out []routerSnap
	for _, r := range c.sortedRouters() {
		r.mu.RLock()
		s := routerSnap{ip: r.ip, name: r.sysName, up: r.up, connects: r.connects, typeCount: r.typeCount,
			errors: r.errors, noOpen: r.noOpen, lastMsgAgo: -1}
		if !r.lastMsg.IsZero() {
			s.lastMsgAgo = now.Sub(r.lastMsg).Seconds()
		}
		for _, p := range r.peers {
			if p.up {
				s.peersUp++
			} else {
				s.peersDown++
			}
			if p.synced() {
				s.synced++
			}
			if p.settled(now) {
				s.settled++
			}
		}
		s.routesV4, s.routesV6 = r.routes()
		r.mu.RUnlock()
		out = append(out, s)
	}
	return out
}

// promWriter : 한 metric 의 머리 (HELP / TYPE) 와 줄들
type promWriter struct{ w *bufio.Writer }

func (p promWriter) head(name, typ, help string) {
	fmt.Fprintf(p.w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
}

// val : name{labels} value. labels = "이름", "값", ... 차례로
func (p promWriter) val(name string, v float64, labels ...string) {
	p.w.WriteString(name)
	if len(labels) > 0 {
		p.w.WriteByte('{')
		for i := 0; i+1 < len(labels); i += 2 {
			if i > 0 {
				p.w.WriteByte(',')
			}
			fmt.Fprintf(p.w, "%s=\"%s\"", labels[i], promEscape(labels[i+1]))
		}
		p.w.WriteByte('}')
	}
	p.w.WriteByte(' ')
	p.w.Write(strconv.AppendFloat(nil, v, 'f', -1, 64)) // 정수는 정수 글자로 (8.9e+06 대신 8934954)
	p.w.WriteByte('\n')
}

// promEscape : label 값 안의 \ " 줄바꿈 (sysName 등)
func promEscape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func (c *collector) apiMetrics(w http.ResponseWriter, req *http.Request) {
	now := time.Now()
	rs := c.routerSnaps(now)
	cum, raw, p := hist.totals()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	bw := bufio.NewWriterSize(w, 1<<16)
	defer bw.Flush()
	m := promWriter{bw}

	m.head("bongsudae_info", "gauge", "Release and git revision (value is always 1).")
	m.val("bongsudae_info", 1, "release", release, "version", version())
	m.head("bongsudae_start_time_seconds", "gauge", "Unix time the collector first started (kept across -upgrade).")
	m.val("bongsudae_start_time_seconds", float64(hist.started.Unix()))

	// 라우터마다
	m.head("bongsudae_router_up", "gauge", "1 if the router's BMP session is up.")
	for _, r := range rs {
		m.val("bongsudae_router_up", b2f(r.up), "router", r.ip, "name", r.name)
	}
	m.head("bongsudae_router_last_message_age_seconds", "gauge", "Seconds since the last BMP message from the router (-1 = none yet).")
	for _, r := range rs {
		m.val("bongsudae_router_last_message_age_seconds", r.lastMsgAgo, "router", r.ip)
	}
	m.head("bongsudae_router_connects_total", "counter", "BMP sessions (Initiation messages) since start.")
	for _, r := range rs {
		m.val("bongsudae_router_connects_total", float64(r.connects), "router", r.ip)
	}
	m.head("bongsudae_router_bmp_messages_total", "counter", "BMP messages received, by message type.")
	for _, r := range rs {
		for i, n := range r.typeCount {
			if n == 0 {
				continue
			}
			name := "unknown"
			if i < bmpTypeCount {
				name = bmpTypeName[i]
			}
			m.val("bongsudae_router_bmp_messages_total", float64(n), "router", r.ip, "type", name)
		}
	}
	m.head("bongsudae_router_errors_total", "counter", "BMP messages that could not be parsed.")
	for _, r := range rs {
		m.val("bongsudae_router_errors_total", float64(r.errors), "router", r.ip)
	}
	m.head("bongsudae_router_peer_up_no_open_total", "counter", "Peer Up messages without the OPEN messages (RFC 7854 violation).")
	for _, r := range rs {
		m.val("bongsudae_router_peer_up_no_open_total", float64(r.noOpen), "router", r.ip)
	}
	m.head("bongsudae_peers", "gauge", "Known peers by state. synced = End-of-RIB received, settled = synced or no new prefix for 5 minutes.")
	for _, r := range rs {
		m.val("bongsudae_peers", float64(r.peersUp), "router", r.ip, "state", "up")
		m.val("bongsudae_peers", float64(r.peersDown), "router", r.ip, "state", "down")
		m.val("bongsudae_peers", float64(r.synced), "router", r.ip, "state", "synced")
		m.val("bongsudae_peers", float64(r.settled), "router", r.ip, "state", "settled")
	}
	m.head("bongsudae_routes", "gauge", "Prefixes in the route tables (all peers of the router).")
	for _, r := range rs {
		m.val("bongsudae_routes", float64(r.routesV4), "router", r.ip, "afi", "4")
		m.val("bongsudae_routes", float64(r.routesV6), "router", r.ip, "afi", "6")
	}

	// 전체 (1 분 통계의 누적)
	m.head("bongsudae_rows_total", "counter", "unicast_prefix rows made (updated once a minute).")
	m.val("bongsudae_rows_total", float64(cum.add), "action", "add")
	m.val("bongsudae_rows_total", float64(cum.del), "action", "del")
	m.head("bongsudae_rows_not_sent_total", "counter", "Rows not sent to Kafka because of the forwarding rules.")
	m.val("bongsudae_rows_not_sent_total", float64(cum.dropped))
	m.head("bongsudae_kafka_messages_total", "counter", "Kafka messages produced, by kind (updated once a minute).")
	m.val("bongsudae_kafka_messages_total", float64(cum.msgs-cum.routerMsgs-cum.peerMsgs), "kind", "unicast_prefix")
	m.val("bongsudae_kafka_messages_total", float64(cum.routerMsgs), "kind", "router")
	m.val("bongsudae_kafka_messages_total", float64(cum.peerMsgs), "kind", "peer")
	m.val("bongsudae_kafka_messages_total", float64(raw), "kind", "bmp_raw")
	m.head("bongsudae_other_afi_total", "counter", "MP_REACH / MP_UNREACH of address families other than IPv4 / IPv6 unicast (counted, not forwarded).")
	m.val("bongsudae_other_afi_total", float64(cum.others))
	if p != nil {
		m.head("bongsudae_kafka_fails_total", "counter", "Kafka produce failures since start.")
		m.val("bongsudae_kafka_fails_total", float64(p.fails.Load()))
		m.head("bongsudae_kafka_last_ok_time_seconds", "gauge", "Unix time of the last successful Kafka produce (0 = none).")
		m.val("bongsudae_kafka_last_ok_time_seconds", float64(p.lastOK.Load())/1e9)
	}

	// Go 실행기 (이름은 Prometheus Go 라이브러리와 같게)
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	m.head("go_memstats_heap_alloc_bytes", "gauge", "Bytes of allocated heap objects.")
	m.val("go_memstats_heap_alloc_bytes", float64(ms.HeapAlloc))
	m.head("go_memstats_sys_bytes", "gauge", "Bytes of memory obtained from the OS.")
	m.val("go_memstats_sys_bytes", float64(ms.Sys))
	m.head("go_gc_cycles_total", "counter", "Completed GC cycles.")
	m.val("go_gc_cycles_total", float64(ms.NumGC))
	m.head("go_goroutines", "gauge", "Number of goroutines.")
	m.val("go_goroutines", float64(runtime.NumGoroutine()))
}
