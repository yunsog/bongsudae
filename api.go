// 웹 API : 라우터 / peer 현황, 경로표 조회 (GET 만, JSON 기본, 큰 목록은 ?format=text 로 탭 구분 글자)
//
//	/api/routers                                   라우터 전체
//	/api/routers/{ip}                              라우터 하나 + 그 peer 들
//	/api/peers?router=&state=up|down&as=&warn=1    peer 목록 (걸러 보기)
//	/api/routers/{ip}/peers/{peer}                 peer 하나 상세 (능력, 이력)
//	/api/routers/{ip}/peers/{peer}/prefixes?afi=4|6&p=&match=&origin_as=|not_origin=&path_as=&offset=&limit=
//	/api/routers/{ip}/prefixes?(위와 같음)&distinct=1     응답 머리 X-Total (거른 뒤 줄 수), X-Applied (적용한 질의)
//	/api/routers/{ip}/prefix?p=&match=             라우터 안에서 특정 프리픽스 (find.go)
//	/api/prefix?p=&match=&limit=                   모든 라우터에서 특정 프리픽스, POST = 여러 개 (find.go)
//	POST /api/routers/{ip}/reset                   BMP 세션을 끊음 (key, tcp 입력만) -> 라우터가 다시 접속해 전체를 다시 보냄
//	/api/doc                                       이 API 의 설명 (docs/api.md)
//
// match : exact (기본) | longest (가장 긴 일치 하나) | covering (그 프리픽스와 덮는 것들) | more (그 안의 더 구체적인 것들)
// p 에 주소만 주면 (예 1.11.56.10) /32 (/128) 로 보고, match 기본은 longest.
//
// 주소 : -http 127.0.0.1:5001 (로컬) | :5001 (글로벌, 접근 제어는 방화벽). 외부 라이브러리 없이 net/http 만.
// 잠금 : 라우터마다 r.mu (collector.go). 큰 목록은 peer 하나씩 읽기 잠금 -> 복사 -> 풀고 나서 내보냄
// (그 라우터의 처리만 잠깐 기다림, 다른 라우터는 영향 없음)
package main

import (
	"bufio"
	"cmp"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unique"
)

// apiDoc : docs/api.md (빌드할 때 실행 파일에 넣음 = 도는 API 와 같은 판)
//
//go:embed docs/api.md
var apiDoc []byte

const (
	silentWarn = 10 * time.Minute // up 인데 이만큼 메시지가 없으면 경고
	apiConc    = 4                // 동시에 처리하는 API 요청 수
)

// startAPI : 웹 API 를 goroutine 으로 시작
// httpAllow : API 허용 대역 (설정 http.allow / -http-allow, reload 로 바꿈). nil = 모두 (접근 제어는 방화벽)
var httpAllow atomic.Pointer[[]netip.Prefix]

// httpAllowed : 요청한 주소가 허용 대역 안인지
func httpAllowed(remote string) bool {
	list := httpAllow.Load()
	if list == nil || len(*list) == 0 {
		return true
	}
	ap, err := netip.ParseAddrPort(remote)
	return err == nil && allowed(*list, ap.Addr())
}

// parseHTTPAllow : 빈칸 = 모두 허용, 아니면 parseAllow 와 같음 (주소 하나는 /32, /128 로)
func parseHTTPAllow(s string) ([]netip.Prefix, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	return parseAllow(s)
}

func startAPI(addr string, c *collector) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/routers", c.apiRouters)
	mux.HandleFunc("GET /api/routers/{ip}", c.apiRouter)
	mux.HandleFunc("GET /api/peers", c.apiPeers)
	mux.HandleFunc("GET /api/stats", c.apiStats) // 1 분 통계 60 분, 판, 메모리, Kafka (stats_api.go)
	mux.HandleFunc("GET /api/routers/{ip}/peers/{peer}", c.apiPeer)
	mux.HandleFunc("GET /api/routers/{ip}/peers/{peer}/prefixes", c.apiPeerPrefixes)
	mux.HandleFunc("GET /api/routers/{ip}/prefixes", c.apiRouterPrefixes)
	mux.HandleFunc("GET /api/routers/{ip}/prefix", c.apiPrefix)
	mux.HandleFunc("GET /api/prefix", c.apiPrefix)
	mux.HandleFunc("POST /api/prefix", c.apiPrefixBatch)
	mux.HandleFunc("POST /api/routers/{ip}/prefix", c.apiPrefixBatch)
	mux.HandleFunc("GET /api/doc", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
		w.Write(apiDoc)
	})
	if c.rules != nil {
		c.addRuleAPI(mux)
		mux.HandleFunc("POST /api/routers/{ip}/reset", c.auth(c.apiReset)) // key 필요 (rules_api.go 의 auth)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("-http %s: %w", addr, err)
	}
	sem := make(chan struct{}, apiConc)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !httpAllowed(r.RemoteAddr) {
			http.Error(w, "허용 대역 밖 (http.allow)", http.StatusForbidden)
			return
		}
		sem <- struct{}{}
		defer func() { <-sem }()
		mux.ServeHTTP(w, r)
	})
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: time.Minute}
	log.Printf("http api %s", addr)
	go func() { log.Printf("http api: %v", srv.Serve(ln)) }()
	return nil
}

// ---- JSON 모양 ----

type routerJSON struct {
	IP         string            `json:"ip"`
	Name       string            `json:"name"`
	Descr      string            `json:"descr,omitempty"`
	State      string            `json:"state"` // up | down
	Since      string            `json:"since"`
	LastMsg    string            `json:"last_msg"`
	LastMsgAgo int64             `json:"last_msg_ago_s"`
	Connects   int               `json:"connects"`
	Msgs       map[string]uint64 `json:"msgs"`
	Errors     uint64            `json:"errors"`
	NoOpen     uint64            `json:"peer_up_no_open"`
	PeersUp    int               `json:"peers_up"`
	PeersDown  int               `json:"peers_down"`
	RoutesV4   int               `json:"routes_v4"`
	RoutesV6   int               `json:"routes_v6"`
	Synced     int               `json:"peers_synced"` // up peer 중 처음 전체 전송을 다 받은 수 (End-of-RIB)
	LastTerm   string            `json:"last_term,omitempty"`
	LastReset  string            `json:"last_reset,omitempty"` // collector 가 끊은 (reset) 시각과 이유
	ResetWhy   string            `json:"last_reset_why,omitempty"`
	Kafka      []string          `json:"kafka"`      // unicast_prefix 를 보내는 토픽 (라우터 단계, [] = 안 보냄)
	KafkaFrom  string            `json:"kafka_from"` // router | default
	Warn       []string          `json:"warn,omitempty"`
	Peers      []peerJSON        `json:"peers,omitempty"` // /api/routers/{ip} 만
}

type peerJSON struct {
	Router     string   `json:"router"`
	RouterName string   `json:"router_name"`
	Peer       string   `json:"peer"`
	RD         string   `json:"rd"`
	AS         uint32   `json:"as"`
	BGPID      string   `json:"bgp_id"`
	Type       string   `json:"type"`
	State      string   `json:"state"`
	Since      string   `json:"since"`
	PeerUp     bool     `json:"peer_up"` // 이번 세션의 Peer Up 을 받음 (경로 수가 정확)
	Synced     bool     `json:"synced"`  // 처음 전체 전송을 다 받음 (협상된 종류마다 End-of-RIB)
	EoRV4      string   `json:"eor_v4,omitempty"`
	EoRV6      string   `json:"eor_v6,omitempty"`
	RoutesV4   int      `json:"routes_v4"`
	RoutesV6   int      `json:"routes_v6"`
	LastUpdate string   `json:"last_update,omitempty"`
	Adds       uint64   `json:"adds"`
	Dels       uint64   `json:"dels"`
	DownReason string   `json:"down_reason,omitempty"`
	Kafka      []string `json:"kafka"`      // 이 peer 의 unicast_prefix 토픽 ([] = 안 보냄)
	KafkaFrom  string   `json:"kafka_from"` // peer | router | default
	Warn       []string `json:"warn,omitempty"`
	// 상세 (/api/routers/{ip}/peers/{peer}) 만
	Detail *peerDetail `json:"detail,omitempty"`
}

type peerDetail struct {
	PrePolicy  bool     `json:"pre_policy"`
	AdjRibIn   bool     `json:"adj_rib_in"`
	AS4        bool     `json:"as4"`
	LocalIP    string   `json:"local_ip"`
	LocalPort  uint16   `json:"local_port"`
	RemotePort uint16   `json:"remote_port"`
	HoldSent   uint16   `json:"hold_sent"`
	HoldRecv   uint16   `json:"hold_recv"`
	SentCaps   string   `json:"sent_caps"`
	RecvCaps   string   `json:"recv_caps"`
	History    []string `json:"history"`
}

type routeJSON struct {
	Router       string `json:"router"`
	RouterName   string `json:"router_name"`
	Peer         string `json:"peer"`
	PeerAS       uint32 `json:"peer_as"`
	Prefix       string `json:"prefix"`
	ASPath       string `json:"as_path"`
	OriginAS     uint32 `json:"origin_as"`
	OriginSet    string `json:"origin_as_set,omitempty"` // 경로가 AS_SET 으로 끝나면 "{a,b}" (origin_as 는 0)
	NextHop      string `json:"nexthop"`
	Origin       string `json:"origin"`
	MED          uint32 `json:"med"`
	LocalPref    uint32 `json:"local_pref"`
	Communities  string `json:"communities,omitempty"`
	ExtComm      string `json:"ext_communities,omitempty"`
	LargeComm    string `json:"large_communities,omitempty"`
	ClusterList  string `json:"cluster_list,omitempty"`
	OriginatorID string `json:"originator_id,omitempty"`
	Aggregator   string `json:"aggregator,omitempty"`
	Since        string `json:"since"`
}

// ---- 글자 ----

// tstr : 시각 글자 (서버 지역 시각 + 시간대). 0 이면 빈칸
func tstr(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Local().Format(time.RFC3339)
}

func peerTypeName(t uint8) string {
	switch t {
	case 0:
		return "global"
	case 1:
		return "rd"
	case 2:
		return "local"
	case 3:
		return "loc-rib"
	}
	return strconv.Itoa(int(t))
}

func stateName(up bool) string {
	if up {
		return "up"
	}
	return "down"
}

// ---- 현황 만들기 (읽기 잠금 안에서 부름) ----

func (c *collector) routerView(r *router, now time.Time) routerJSON {
	j := routerJSON{IP: r.ip, Name: r.sysName, Descr: r.sysDescr, State: stateName(r.up), Since: tstr(r.since),
		LastMsg: tstr(r.lastMsg), Connects: r.connects, Errors: r.errors, NoOpen: r.noOpen, LastTerm: r.lastTerm,
		Msgs: map[string]uint64{}}
	if !r.lastMsg.IsZero() {
		j.LastMsgAgo = int64(now.Sub(r.lastMsg).Seconds())
	}
	for i, n := range r.typeCount {
		if n > 0 {
			name := "unknown"
			if i < bmpTypeCount {
				name = bmpTypeName[i]
			}
			j.Msgs[name] = n
		}
	}
	j.LastReset, j.ResetWhy = tstr(r.lastReset), r.lastResetWhy
	for _, p := range r.peers {
		if p.synced() {
			j.Synced++
		}
		if p.up {
			j.PeersUp++
		} else {
			j.PeersDown++
		}
	}
	j.RoutesV4, j.RoutesV6 = r.routes()
	j.Kafka, j.KafkaFrom = c.rules.effective(r.ip, "")
	if !r.up {
		j.Warn = append(j.Warn, "down")
	} else if !r.lastMsg.IsZero() && now.Sub(r.lastMsg) > silentWarn {
		j.Warn = append(j.Warn, fmt.Sprintf("silent %d min", int(now.Sub(r.lastMsg).Minutes())))
	}
	if r.connects > 1 {
		j.Warn = append(j.Warn, fmt.Sprintf("reconnected %d times", r.connects-1))
	}
	if r.errors > 0 {
		j.Warn = append(j.Warn, fmt.Sprintf("errors %d", r.errors))
	}
	if r.noOpen > 0 {
		j.Warn = append(j.Warn, fmt.Sprintf("peer up without OPEN %d", r.noOpen))
	}
	if j.PeersDown > 0 {
		j.Warn = append(j.Warn, fmt.Sprintf("peers down %d", j.PeersDown))
	}
	return j
}

func (c *collector) peerView(r *router, p *peer, detail bool) peerJSON {
	j := peerJSON{Router: r.ip, RouterName: r.sysName, Peer: p.ip.String(), RD: p.rd, AS: p.as, BGPID: p.bgpID,
		Type: peerTypeName(p.typ), State: stateName(p.up), Since: tstr(p.since), PeerUp: p.gotUp,
		RoutesV4: len(p.rib4), RoutesV6: len(p.rib6), LastUpdate: tstr(p.lastUpdate), Adds: p.adds, Dels: p.dels,
		DownReason: p.downReason, Synced: p.synced(), EoRV4: tstr(p.eorV4), EoRV6: tstr(p.eorV6)}
	j.Kafka, j.KafkaFrom = c.rules.effective(r.ip, p.ip.Unmap().String())
	if !p.up {
		j.Warn = append(j.Warn, "down")
	} else if !p.gotUp {
		j.Warn = append(j.Warn, "no Peer Up (route count may be incomplete)")
	}
	if p.noOpen {
		j.Warn = append(j.Warn, "Peer Up without OPEN")
	}
	if detail {
		d := &peerDetail{PrePolicy: p.flags&flagL == 0, AdjRibIn: p.flags&flagO == 0 && p.typ != peerTypeLocRIB,
			AS4: p.flags&flagA == 0, LocalIP: p.localIP, LocalPort: p.localPort, RemotePort: p.rmtPort,
			HoldSent: p.holdSent, HoldRecv: p.holdRecv, SentCaps: p.sentCaps, RecvCaps: p.recvCaps}
		for _, e := range p.hist {
			d.History = append(d.History, tstr(e.t)+" "+e.text)
		}
		j.Detail = d
	}
	return j
}

// sortedRouters : 라우터를 주소 순으로
// (c.mu 를 잠깐 잡음 : 이것을 부를 때는 c.mu 를 잡고 있지 않아야 함)
func (c *collector) sortedRouters() []*router {
	c.mu.RLock()
	defer c.mu.RUnlock()
	rs := make([]*router, 0, len(c.routers))
	for _, r := range c.routers {
		rs = append(rs, r)
	}
	slices.SortFunc(rs, func(a, b *router) int { return cmpAddr(a.ip, b.ip) })
	return rs
}

// sortedPeers : peer 를 주소 순으로
func sortedPeers(r *router) []*peer {
	ps := make([]*peer, 0, len(r.peers))
	for _, p := range r.peers {
		ps = append(ps, p)
	}
	slices.SortFunc(ps, func(a, b *peer) int {
		return cmp.Or(a.ip.Compare(b.ip), strings.Compare(a.rd, b.rd))
	})
	return ps
}

func cmpAddr(a, b string) int {
	x, _ := netip.ParseAddr(a)
	y, _ := netip.ParseAddr(b)
	return x.Compare(y)
}

// findPeer : 라우터 안에서 peer 주소로 (같은 주소가 여럿이면 rd 0:0 = global 을 먼저)
func findPeer(r *router, ip string) *peer {
	a, err := netip.ParseAddr(ip)
	if err != nil {
		return nil
	}
	var found *peer
	for _, p := range r.peers {
		if p.ip == a && (found == nil || p.rd == "0:0") {
			found = p
		}
	}
	return found
}

// ---- 응답 ----

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	e := json.NewEncoder(w)
	e.SetIndent("", " ")
	e.Encode(v)
}

func httpErr(w http.ResponseWriter, code int, f string, a ...any) {
	http.Error(w, fmt.Sprintf(f, a...), code)
}

// ---- 처리기 ----

func (c *collector) apiRouters(w http.ResponseWriter, req *http.Request) {
	now := time.Now()
	out := []routerJSON{}
	for _, r := range c.sortedRouters() {
		r.mu.RLock()
		out = append(out, c.routerView(r, now))
		r.mu.RUnlock()
	}
	writeJSON(w, out)
}

func (c *collector) apiRouter(w http.ResponseWriter, req *http.Request) {
	r := c.getRouter(req.PathValue("ip"))
	if r == nil {
		httpErr(w, 404, "router %s not found", req.PathValue("ip"))
		return
	}
	r.mu.RLock()
	j := c.routerView(r, time.Now())
	for _, p := range sortedPeers(r) {
		j.Peers = append(j.Peers, c.peerView(r, p, false))
	}
	r.mu.RUnlock()
	writeJSON(w, j)
}

func (c *collector) apiPeers(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	asn, _ := strconv.ParseUint(q.Get("as"), 10, 32)
	out := []peerJSON{}
	for _, r := range c.sortedRouters() {
		if q.Get("router") != "" && q.Get("router") != r.ip {
			continue
		}
		r.mu.RLock()
		for _, p := range sortedPeers(r) {
			if (q.Get("state") != "" && q.Get("state") != stateName(p.up)) || (asn != 0 && uint32(asn) != p.as) {
				continue
			}
			j := c.peerView(r, p, false)
			if q.Get("warn") == "1" && len(j.Warn) == 0 {
				continue
			}
			out = append(out, j)
		}
		r.mu.RUnlock()
	}
	writeJSON(w, out)
}

func (c *collector) apiPeer(w http.ResponseWriter, req *http.Request) {
	r := c.getRouter(req.PathValue("ip"))
	if r == nil {
		httpErr(w, 404, "router %s not found", req.PathValue("ip"))
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	p := findPeer(r, req.PathValue("peer"))
	if p == nil {
		httpErr(w, 404, "peer %s not found on router %s", req.PathValue("peer"), r.ip)
		return
	}
	writeJSON(w, c.peerView(r, p, true))
}

// ---- 경로 목록 ----

// ribRow : 경로 하나 (잠금 밖으로 가져가는 복사본)
type ribRow struct {
	router, routerName, peer string
	peerAS                   uint32
	pfx                      netip.Prefix
	rt                       route
}

func (x *ribRow) json() routeJSON {
	a := x.rt.a.Value()
	return routeJSON{Router: x.router, RouterName: x.routerName, Peer: x.peer, PeerAS: x.peerAS, Prefix: x.pfx.String(),
		ASPath: a.asPath, OriginAS: a.originAS, OriginSet: a.originSet, NextHop: a.nextHop, Origin: a.origin, MED: a.med, LocalPref: a.localPref,
		Communities: a.community, ExtComm: a.extComm, LargeComm: a.largeComm, ClusterList: a.clusterList,
		OriginatorID: a.originatorID, Aggregator: a.aggregator, Since: tstr(time.Unix(int64(x.rt.t), 0))}
}

var textHead = "router\trouter_name\tpeer\tpeer_as\tprefix\tas_path\torigin_as\tnexthop\torigin\tmed\tlocal_pref\tcommunities\text_communities\tlarge_communities\tsince\n"

func (x *ribRow) text() string {
	j := x.json()
	origin := u32(j.OriginAS)
	if j.OriginSet != "" {
		origin = j.OriginSet
	}
	return strings.Join([]string{j.Router, j.RouterName, j.Peer, u32(j.PeerAS), j.Prefix, j.ASPath, origin,
		j.NextHop, j.Origin, u32(j.MED), u32(j.LocalPref), j.Communities, j.ExtComm, j.LargeComm, j.Since}, "\t") + "\n"
}

// rowWriter : JSON 배열 또는 탭 글자를 한 줄씩 내보냄 (큰 목록을 메모리에 다 만들지 않음)
type rowWriter struct {
	w     *bufio.Writer
	text  bool
	n     int
	limit int
}

func newRowWriter(w http.ResponseWriter, req *http.Request) *rowWriter {
	rw := &rowWriter{w: bufio.NewWriterSize(w, 1<<16), text: req.URL.Query().Get("format") == "text"}
	rw.limit, _ = strconv.Atoi(req.URL.Query().Get("limit"))
	if rw.text {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		rw.w.WriteString(textHead)
	} else {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		rw.w.WriteString("[")
	}
	return rw
}

// full : limit 에 닿았는가
func (rw *rowWriter) full() bool { return rw.limit > 0 && rw.n >= rw.limit }

func (rw *rowWriter) row(x *ribRow) {
	if rw.full() {
		return
	}
	if rw.text {
		rw.w.WriteString(x.text())
	} else {
		if rw.n > 0 {
			rw.w.WriteString(",")
		}
		b, _ := json.Marshal(x.json())
		rw.w.WriteString("\n")
		rw.w.Write(b)
	}
	rw.n++
}

func (rw *rowWriter) end() {
	if !rw.text {
		rw.w.WriteString("\n]\n")
	}
	rw.w.Flush()
}

// copyRIB : peer 하나의 경로표를 복사 (읽기 잠금 안에서), 프리픽스 순
func copyRIB(r *router, p *peer, afi string) []ribRow {
	rows := make([]ribRow, 0, len(p.rib4)+len(p.rib6))
	base := ribRow{router: r.ip, routerName: r.sysName, peer: p.ip.String(), peerAS: p.as}
	if afi != "6" {
		for k, v := range p.rib4 {
			x := base
			x.pfx, x.rt = k.prefix(), v
			rows = append(rows, x)
		}
	}
	if afi != "4" {
		for k, v := range p.rib6 {
			x := base
			x.pfx, x.rt = k.prefix(), v
			rows = append(rows, x)
		}
	}
	return rows
}

func sortRows(rows []ribRow) {
	slices.SortFunc(rows, func(a, b ribRow) int {
		return cmp.Or(cmpAddr(a.router, b.router), strings.Compare(a.peer, b.peer), a.pfx.Addr().Compare(b.pfx.Addr()),
			a.pfx.Bits()-b.pfx.Bits())
	})
}

// routerPeer : (라우터, peer) 짝
type routerPeer struct {
	r *router
	p *peer
}

// peersOf : (라우터, peer) 목록. ip 가 비면 모든 라우터. peer 는 지워지지 않으므로 잠금 밖에서 들고 있어도 됨
// (peer 안을 읽을 때는 그 라우터의 r.mu.RLock)
func (c *collector) peersOf(ip, peerIP string) (list []routerPeer, ok bool) {
	ok = ip == "" // 모든 라우터 : 라우터가 아직 없어도 404 가 아님 (빈 결과)
	for _, r := range c.sortedRouters() {
		if ip != "" && r.ip != ip {
			continue
		}
		ok = true
		r.mu.RLock()
		if peerIP != "" {
			if p := findPeer(r, peerIP); p != nil {
				list = append(list, routerPeer{r, p})
			} else {
				ok = false
			}
		} else {
			for _, p := range sortedPeers(r) {
				list = append(list, routerPeer{r, p})
			}
		}
		r.mu.RUnlock()
	}
	return
}

// listFilter : 경로 목록의 질의 . 순서 afi -> p -> origin -> path_as -> offset -> limit
type listFilter struct {
	afi     string        // "4" / "6" / "" (모두)
	p       netip.Prefix  // p= (없으면 IsValid() == false)
	how     string        // exact | longest | covering | more
	origin  *originFilter // origin_as / not_origin (nil = 거르지 않음)
	pathAS  map[uint32]bool
	offset  int
	limit   int
	applied []string // 응답 머리 X-Applied

	pathHit map[unique.Handle[pathAttr]]bool // path_as 결과를 속성 묶음마다 기억 (같은 속성을 여러 줄이 씀)
	noSort  bool                             // distinct : 프리픽스로 묶기만 하므로 peer 안 정렬이 필요 없음
}

// newListFilter : 질의 -> listFilter. 잘못된 값은 오류 (400)
func newListFilter(q url.Values) (*listFilter, error) {
	f := &listFilter{afi: q.Get("afi")}
	var err error
	if f.afi == "4" || f.afi == "6" {
		f.applied = append(f.applied, "afi")
	}
	if s := q.Get("p"); s != "" {
		if f.p, f.how, err = parseQuery(s); err != nil {
			return nil, fmt.Errorf("p=%q : 프리픽스 또는 주소", s)
		}
		f.applied = append(f.applied, "p")
		if m := q.Get("match"); m != "" {
			if !validMatch(m) {
				return nil, fmt.Errorf("match=%q : exact | longest | covering | more", m)
			}
			f.how = m
			f.applied = append(f.applied, "match")
		}
	} else if q.Get("match") != "" {
		return nil, fmt.Errorf("match 는 p 와 함께")
	}
	if f.origin, err = filterFromQuery(q); err != nil {
		return nil, err
	}
	if f.origin != nil {
		f.applied = append(f.applied, map[bool]string{false: "origin_as", true: "not_origin"}[f.origin.not])
	}
	list, err := parseASList("path_as", q.Get("path_as"))
	if err != nil {
		return nil, err
	}
	if len(list) > 0 {
		f.pathAS, f.pathHit = map[uint32]bool{}, map[unique.Handle[pathAttr]]bool{}
		for _, as := range list {
			f.pathAS[as] = true
		}
		f.applied = append(f.applied, "path_as")
	}
	for _, k := range []struct {
		name string
		v    *int
	}{{"offset", &f.offset}, {"limit", &f.limit}} {
		if s := q.Get(k.name); s != "" {
			if *k.v, err = strconv.Atoi(s); err != nil || *k.v < 0 {
				return nil, fmt.Errorf("%s=%q : 0 이상의 정수", k.name, s)
			}
			f.applied = append(f.applied, k.name)
		}
	}
	if q.Get("format") == "text" {
		f.applied = append(f.applied, "format")
	}
	return f, nil
}

// keep : afi, p (more), origin, path_as 를 모두 통과하는가 (exact / longest / covering 은 peerRows 가 match 로 찾음)
func (f *listFilter) keep(x *ribRow) bool {
	if (f.afi == "4" && !x.pfx.Addr().Is4()) || (f.afi == "6" && x.pfx.Addr().Is4()) {
		return false
	}
	if f.p.IsValid() && f.how == "more" && (x.pfx.Bits() < f.p.Bits() || !f.p.Contains(x.pfx.Addr())) {
		return false
	}
	if !f.origin.keep(x) {
		return false
	}
	if f.pathAS != nil {
		hit, ok := f.pathHit[x.rt.a]
		if !ok {
			hit = pathHas(x.rt.a.Value().asPath, f.pathAS)
			f.pathHit[x.rt.a] = hit
		}
		return hit
	}
	return true
}

// pathHas : as_path 글자 (" 1299 4788 {23736,38044}") 에 set 의 AS 가 있는가 (AS_SET 안도)
func pathHas(asPath string, set map[uint32]bool) bool {
	for _, t := range strings.FieldsFunc(asPath, func(r rune) bool { return r == ' ' || r == ',' || r == '{' || r == '}' }) {
		if n, err := strconv.ParseUint(t, 10, 32); err == nil && set[uint32(n)] {
			return true
		}
	}
	return false
}

// kv4 / kv6 : 정렬용 작은 복사본 (열쇠 + 경로 = 16 / 32 바이트). 열쇠 순 = 프리픽스 순 (주소, 그다음 길이)
type kv4 struct {
	k  key4
	rt route
}

type kv6 struct {
	k  key6
	rt route
}

// peerScan : peer 하나의 걸러진 경로를 프리픽스 순으로 훑음 (잠금 -> 열쇠만 복사 -> 풀고 정렬, 거름).
// 걸러진 줄 번호 (0 부터) 가 [lo, hi) 인 줄만 each 로 넘기고 (줄은 그때만 만듦), 걸러진 줄 수를 돌려줌.
// each 가 받는 *ribRow 는 다음 줄에 다시 쓰임 -> 들고 있으려면 복사
func (f *listFilter) peerScan(rp routerPeer, lo, hi int, each func(*ribRow)) int {
	r, p := rp.r, rp.p
	want := hi > 0 && lo < hi // 내보낼 줄이 있는가 (없으면 세기만, 정렬 안 함)
	sortNeeded := want && !f.noSort
	n := 0
	visit := func(x *ribRow) {
		if f.keep(x) {
			if want && n >= lo && n < hi {
				each(x)
			}
			n++
		}
	}
	if f.p.IsValid() && f.how != "more" { // exact / longest / covering : 몇 줄뿐 (길이마다 한 번씩 찾음)
		r.mu.RLock()
		rows := match(r, p, f.p, f.how)
		r.mu.RUnlock()
		slices.SortFunc(rows, func(a, b ribRow) int { return a.pfx.Bits() - b.pfx.Bits() }) // 같은 주소 : 짧은 것부터
		for i := range rows {
			visit(&rows[i])
		}
		return n
	}
	var v4 []kv4
	var v6 []kv6
	r.mu.RLock()
	base := ribRow{router: r.ip, routerName: r.sysName, peer: p.ip.String(), peerAS: p.as}
	if f.afi != "6" && (!f.p.IsValid() || f.p.Addr().Is4()) {
		v4 = make([]kv4, 0, len(p.rib4))
		for k, v := range p.rib4 {
			v4 = append(v4, kv4{k, v})
		}
	}
	if f.afi != "4" && (!f.p.IsValid() || f.p.Addr().Is6()) {
		v6 = make([]kv6, 0, len(p.rib6))
		for k, v := range p.rib6 {
			v6 = append(v6, kv6{k, v})
		}
	}
	r.mu.RUnlock()
	if sortNeeded {
		slices.SortFunc(v4, func(a, b kv4) int { return cmp.Compare(a.k, b.k) })
		slices.SortFunc(v6, func(a, b kv6) int {
			return cmp.Or(cmp.Compare(a.k.hi, b.k.hi), cmp.Compare(a.k.lo, b.k.lo), cmp.Compare(a.k.n, b.k.n))
		})
	}
	x := base
	for _, e := range v4 { // v4 다음 v6 (예전 정렬과 같음)
		x.pfx, x.rt = e.k.prefix(), e.rt
		visit(&x)
	}
	for _, e := range v6 {
		x.pfx, x.rt = e.k.prefix(), e.rt
		visit(&x)
	}
	return n
}

// streamPeers : 걸러진 줄의 offset 부터 limit 줄을 내보냄. 머리 X-Total (거른 뒤 offset / limit 전 줄 수), X-Applied.
// 머리는 본문보다 먼저 보내야 하므로 :
//
//	limit 있음 : 한 번 훑으며 total 을 세고 [offset, offset+limit) 줄만 들고 있다가 내보냄 (메모리 = limit 줄)
//	limit 없음 : 먼저 세기만 하고 (정렬 없이), 다시 훑으며 내보냄 (그 사이 바뀐 경로가 있으면 total 과 줄 수가 조금 다를 수 있음)
func (c *collector) streamPeers(w http.ResponseWriter, req *http.Request, list []routerPeer, f *listFilter) {
	total := 0
	var win []ribRow
	for _, rp := range list {
		if f.limit > 0 {
			total += f.peerScan(rp, f.offset-total, f.offset+f.limit-total, func(x *ribRow) { win = append(win, *x) })
		} else {
			total += f.peerScan(rp, 0, 0, nil)
		}
	}
	w.Header().Set("X-Total", strconv.Itoa(total))
	w.Header().Set("X-Applied", strings.Join(f.applied, ","))
	rw := newRowWriter(w, req)
	if f.limit > 0 {
		for i := range win {
			rw.row(&win[i])
		}
	} else {
		done := 0
		for _, rp := range list {
			done += f.peerScan(rp, f.offset-done, math.MaxInt, rw.row)
		}
	}
	rw.end()
}

func (c *collector) apiPeerPrefixes(w http.ResponseWriter, req *http.Request) {
	f, err := newListFilter(req.URL.Query())
	if err != nil {
		httpErr(w, 400, "%v", err)
		return
	}
	list, ok := c.peersOf(req.PathValue("ip"), req.PathValue("peer"))
	if !ok || len(list) == 0 {
		httpErr(w, 404, "router %s / peer %s not found", req.PathValue("ip"), req.PathValue("peer"))
		return
	}
	c.streamPeers(w, req, list, f)
}

func (c *collector) apiRouterPrefixes(w http.ResponseWriter, req *http.Request) {
	f, err := newListFilter(req.URL.Query())
	if err != nil {
		httpErr(w, 400, "%v", err)
		return
	}
	list, ok := c.peersOf(req.PathValue("ip"), "")
	if !ok {
		httpErr(w, 404, "router %s not found", req.PathValue("ip"))
		return
	}
	if req.URL.Query().Get("distinct") == "1" {
		c.distinct(w, req, list, f)
		return
	}
	c.streamPeers(w, req, list, f)
}

// distinct : 라우터의 서로 다른 프리픽스와 그것을 가진 peer 수. 목록과 같은 필터 (afi, p, origin, path_as) 로 줄을 고른 뒤
// 프리픽스로 묶고, offset / limit 은 묶은 프리픽스에. X-Total = 서로 다른 프리픽스 수 (10-30 부터)
func (c *collector) distinct(w http.ResponseWriter, req *http.Request, list []routerPeer, f *listFilter) {
	count := map[netip.Prefix]int{}
	f.noSort = true // 묶기만 하므로 peer 마다 정렬할 필요 없음
	for _, rp := range list {
		f.peerScan(rp, 0, math.MaxInt, func(x *ribRow) { count[x.pfx]++ })
	}
	keys := make([]netip.Prefix, 0, len(count))
	for k := range count {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b netip.Prefix) int { return cmp.Or(a.Addr().Compare(b.Addr()), a.Bits()-b.Bits()) })
	w.Header().Set("X-Total", strconv.Itoa(len(keys)))
	w.Header().Set("X-Applied", strings.Join(append(f.applied, "distinct"), ","))
	keys = keys[min(f.offset, len(keys)):]
	if f.limit > 0 && len(keys) > f.limit {
		keys = keys[:f.limit]
	}
	bw := bufio.NewWriterSize(w, 1<<16)
	defer bw.Flush()
	if req.URL.Query().Get("format") == "text" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		bw.WriteString("prefix\tpeers\n")
		for _, k := range keys {
			fmt.Fprintf(bw, "%s\t%d\n", k, count[k])
		}
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	bw.WriteString("[")
	for i, k := range keys {
		if i > 0 {
			bw.WriteString(",")
		}
		fmt.Fprintf(bw, "\n{\"prefix\": %q, \"peers\": %d}", k.String(), count[k])
	}
	bw.WriteString("\n]\n")
}

// ---- 특정 프리픽스 찾기 ----

// parseQuery : p (프리픽스 또는 주소) -> 프리픽스, 기본 match
func parseQuery(s string) (netip.Prefix, string, error) {
	if !strings.Contains(s, "/") {
		a, err := netip.ParseAddr(s)
		if err != nil {
			return netip.Prefix{}, "", err
		}
		a = a.Unmap()
		return netip.PrefixFrom(a, a.BitLen()), "longest", nil
	}
	p, err := netip.ParsePrefix(s)
	if err != nil {
		return p, "", err
	}
	return netip.PrefixFrom(p.Addr().Unmap(), p.Bits()).Masked(), "exact", nil
}

// lookup : peer 하나에서 길이 n 으로 자른 q 가 있는가
func lookup(p *peer, q netip.Prefix, n int) (route, netip.Prefix, bool) {
	m, _ := q.Addr().Prefix(n)
	var rt route
	var ok bool
	if q.Addr().Is4() {
		rt, ok = p.rib4[k4(m.Addr(), n)]
	} else {
		rt, ok = p.rib6[k6(m.Addr(), n)]
	}
	return rt, m, ok
}

// match : peer 하나에서 exact / longest / covering 찾기 (길이마다 한 번씩 찾아봄 = 빠름)
func match(r *router, p *peer, q netip.Prefix, how string) []ribRow {
	base := ribRow{router: r.ip, routerName: r.sysName, peer: p.ip.String(), peerAS: p.as}
	var out []ribRow
	add := func(pfx netip.Prefix, rt route) {
		x := base
		x.pfx, x.rt = pfx, rt
		out = append(out, x)
	}
	switch how {
	case "exact":
		if rt, m, ok := lookup(p, q, q.Bits()); ok {
			add(m, rt)
		}
	case "longest", "covering":
		for n := q.Bits(); n >= 0; n-- {
			if rt, m, ok := lookup(p, q, n); ok {
				add(m, rt)
				if how == "longest" {
					break
				}
			}
		}
	} // more 는 find.go 의 moreMany (경로표를 한 번 훑어 여러 프리픽스를 함께)
	return out
}

// ---- BMP 세션 끊기 ----

type resetJSON struct {
	Router string `json:"router"`
	Closed bool   `json:"closed"`
	At     string `json:"at"`
	Why    string `json:"why"`
	Note   string `json:"note"`
}

// apiReset : POST /api/routers/{ip}/reset. 연결을 닫으면 라우터가 다시 접속해 Initiation, Peer Up, 경로 전체를 보냄
func (c *collector) apiReset(w http.ResponseWriter, req *http.Request) {
	c.mu.RLock()
	closeRouter := c.closeRouter
	c.mu.RUnlock()
	if closeRouter == nil {
		httpErr(w, 409, "tcp 입력에서만 (지금은 %s 입력 : 끊을 라우터 연결이 없음)", *optInput)
		return
	}
	ip, err := normIP(req.PathValue("ip"))
	if err != nil {
		httpErr(w, 400, "router %s : 주소가 아님", req.PathValue("ip"))
		return
	}
	why := "Closed by collector: api reset" + actor(req)
	left, err := closeRouter(ip, why)
	switch err {
	case errNotConnected:
		httpErr(w, 404, "router %s : %v", ip, err)
		return
	case errTooSoon:
		w.Header().Set("Retry-After", strconv.Itoa(int(left.Seconds())+1))
		httpErr(w, 429, "router %s : %s 에 한 번까지, %d 초 뒤에", ip, resetGap, int(left.Seconds())+1)
		return
	}
	log.Printf("reset: router %s closed (from %s%s)", ip, req.RemoteAddr, actor(req))
	writeJSON(w, resetJSON{Router: ip, Closed: true, At: tstr(time.Now()), Why: why,
		Note: "라우터가 다시 접속하면 Initiation, Peer Up, 경로 전체를 다시 보냄 (재접속 간격은 라우터 설정). 진행은 peers_synced"})
}
