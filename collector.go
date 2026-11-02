// collector : BMP 메시지 하나를 받아 unicast_prefix / router / peer 메시지들로 바꿈
//
// 라우터마다, peer 마다 상태 (해시, Add-Path, 현황, 경로표 = rib.go) 를 들고 있음.
// 입력 (TCP / Kafka bmp_raw / 파일) 과 출력 (Kafka / 파일) 은 이 파일 밖에서.
//
// 여러 코어로 (10-29) : 라우터마다 goroutine 하나가 그 라우터의 메시지를 차례로 처리 (해석, 줄 만들기, 경로표).
// 서로 다른 라우터는 동시에. 같은 라우터 (= 같은 peer) 의 메시지 순서는 그대로.
//
// 잠금 (C 의 pthread_mutex 와 같음, 잡는 순서는 늘 c.mu -> r.mu -> rules.mu) :
//
//	c.mu   라우터 목록 (routers map), closeRouter. 라우터를 찾거나 만들 때만 잠깐
//	r.mu   라우터 하나의 모든 것 (peer, 경로표, 번호, 통계). 처리 = 쓰기 잠금, 웹 API = 읽기 잠금
//	c.seq  줄 번호 : 원자적 덧셈 (메시지마다 번호 묶음을 받음)
//
// 입력 쪽은 handle / connStart / connClosed 를 부름 (잠금 포함).
package main

import (
	"fmt"
	"log"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// router : BMP 를 보내는 라우터 하나
type router struct {
	mu         sync.RWMutex // 아래 모든 것 (ip, hash 는 만든 뒤 안 바뀜)
	ip         string
	hash       string
	peers      map[string]*peer // peer hash -> peer (down 도 남음, API 가 씀)
	byID       map[peerID]*peer // 주소 + RD -> 같은 peer (메시지마다 찾기, rib.go peerGet)
	routerSeq  uint32           // router 메시지 번호 (연결마다 0 부터)
	peerSeq    uint32           // peer 메시지 번호 (연결마다 0 부터)
	terminated bool             // term 을 이미 냄
	connGen    uint64           // TCP 연결 번호 : 늦게 처리된 이전 연결의 끊김을 무시하려고
	stat       stats            // 1 분 통계 (logStat 이 모아서 0 으로)
	sink       rowSink          // unicast_prefix 줄 버퍼 (UPDATE 마다 다시 씀, row.go)
	enc        rowEnc

	// 현황 (웹 API)
	sysName, sysDescr string                   // Initiation
	up                bool                     // BMP 세션이 살아 있음
	since             time.Time                // up / down 이 된 시각
	lastMsg           time.Time                // 마지막 메시지 받은 시각
	connects          int                      // Initiation 받은 수 (collector 시작 뒤)
	typeCount         [bmpTypeCount + 1]uint64 // BMP 종류별 메시지 수
	errors, noOpen    uint64                   // 해석 오류, OPEN 없는 Peer Up
	lastTerm          string                   // 마지막으로 끊긴 이유
	lastReset         time.Time                // collector 가 끊은 (reset) 마지막 시각
	lastResetWhy      string
}

// reset : 새 BMP 세션 - 남아 있던 peer 는 down (경로표 비움), 번호를 처음으로
func (r *router) reset(t time.Time) {
	for _, p := range r.peers {
		p.markDown(t, "new BMP session")
	}
	r.routerSeq, r.peerSeq, r.terminated = 0, 0, false
}

// collector : 전체 상태 + 통계
type collector struct {
	mu    sync.RWMutex // routers 목록 (라우터 안은 r.mu)
	cHash string       // C_HASH_ID (collector 해시, openbmpd 설정 값 그대로)
	// keyBuckets : unicast_prefix 메시지를 프리픽스 칸으로 나눔 (Kafka key 1-B, -key-buckets). 0 = 나누지 않음 (key = peer hash)
	keyBuckets int
	// noRIB : 경로표를 들지 않음 (-rib=false, 0.2.0). 줄 / Kafka 는 같고, 경로 수 / 프리픽스 찾기 API 만 못 씀 (메모리 대비)
	noRIB bool
	// json : Kafka 메시지를 JSON 으로 (-format json, 0.2.0, jsonrow.go). false = openbmp v1.7 (기본)
	json    bool
	seq     atomic.Uint64 // 줄 번호 (줄마다 1 씩, 여러 goroutine 이 함께 씀)
	routers map[string]*router

	statMu sync.Mutex // stat : 라우터를 모르는 오류 (Kafka bmp_raw 머리 등)
	stat   stats

	rules *rules // Kafka 전달 규칙 (nil = 파일 입력 : 모두 unicast_prefix)
	// closeRouter : 라우터 연결 닫기 (reset API). tcp 입력만 (server.go 가 넣음), 아니면 nil
	closeRouter func(ip, why string) (time.Duration, error)
	prefix      string // 토픽 앞부분 (API 에 보여 줌)
	tokenPath   string // 규칙 변경 API 의 key 파일
}

func newCollector(cHash string) *collector {
	return &collector{cHash: cHash, routers: map[string]*router{}}
}

// lockRouter : 라우터를 찾거나 만들고 쓰기 잠금을 잡아 돌려줌 (부른 쪽이 r.mu.Unlock)
func (c *collector) lockRouter(ip, hash string) *router {
	c.mu.RLock()
	r := c.routers[ip]
	c.mu.RUnlock()
	if r == nil {
		c.mu.Lock()
		r = c.routerFor(ip, hash)
		c.mu.Unlock()
	}
	r.mu.Lock()
	return r
}

// getRouter : 라우터 하나 (없으면 nil). 안을 읽으려면 r.mu.RLock
func (c *collector) getRouter(ip string) *router {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.routers[ip]
}

// routerFor : 라우터 상태 (없으면 만듦, c.mu 쓰기 잠금 안에서). hash 가 비면 md5(ip + cHash)
func (c *collector) routerFor(ip, hash string) *router {
	r := c.routers[ip]
	if r == nil {
		if hash == "" {
			hash = md5hex(ip, c.cHash)
		}
		r = &router{ip: ip, hash: hash, peers: map[string]*peer{}, byID: map[peerID]*peer{}}
		c.routers[ip] = r
	}
	return r
}

// process : BMP 메시지 하나 (공통 머리부터) -> 보낼 메시지들. now = 받은 시각
func (c *collector) process(r *router, msg []byte, now time.Time) ([]outMsg, error) {
	n, typ, err := bmpHeader(msg)
	if err != nil {
		return nil, err
	}
	if n != len(msg) {
		return nil, fmt.Errorf("bmp length %d != %d", n, len(msg))
	}
	t := int(typ)
	if t > bmpTypeCount {
		t = bmpTypeCount // 모르는 종류
	}
	r.stat.bmpType[t]++
	r.typeCount[t]++
	r.lastMsg = now
	if !r.up && r.lastTerm == "" && typ != bmpInit {
		// Initiation 을 못 본 라우터 (그림자 실행 시작 전에 붙은 연결) : 메시지가 오니 up 으로
		r.up, r.since = true, now
	}
	body := msg[bmpHdrLen:]
	switch typ {
	case bmpRouteMon:
		return c.routeMon(r, body, now)
	case bmpPeerUp:
		return c.peerUp(r, body, now)
	case bmpPeerDown:
		return c.peerDown(r, body, now)
	case bmpInit:
		return c.initiation(r, body, now), nil
	case bmpTerm:
		return c.termination(r, body, now), nil
	case bmpStats: // peer 상세 API 에만 (Kafka 출력 없음, bmp_stats.go)
		return nil, c.statsReport(r, body, now)
	}
	return nil, nil // Route Mirroring : 출력 없음
}

// peerInfo : 줄 공통 정보 (peer hash 와 주소 글자는 peer 에 이미 있음)
func (c *collector) peerInfo(r *router, p *peer, h *peerHdr, now time.Time) rowPeer {
	return rowPeer{
		routerHash: r.hash,
		routerIP:   r.ip,
		peerHash:   p.key,
		peerIP:     p.ipStr,
		peerAS:     h.as,
		ts:         tsString(h.sec, h.usec, now),
		prePolicy:  h.flags&flagL == 0,
		adjRibIn:   h.flags&flagO == 0 && h.typ != peerTypeLocRIB,
	}
}

// routeMon : per-peer 머리 + BGP UPDATE
func (c *collector) routeMon(r *router, b []byte, now time.Time) ([]outMsg, error) {
	h, err := parsePeerHdr(b)
	if err != nil {
		return nil, err
	}
	typ, body, _, err := bgpMsg(b[peerHdrLen:])
	if err != nil {
		return nil, err
	}
	if typ != 2 {
		return nil, fmt.Errorf("route_mon bgp type %d", typ)
	}
	p := r.peerGet(&h)
	rp := c.peerInfo(r, p, &h, now)
	if !p.up {
		// Peer Up 없이 경로가 옴 (그림자 실행 / 그 전에 맺어진 세션) : up 으로 보되 경로 수는 부정확
		p.up, p.gotUp, p.since, p.downReason = true, false, now, ""
		p.addEvent(now, "seen in route monitoring (no Peer Up)")
	}
	o := pathOpts{as4: h.flags&flagA == 0, addPathV4: p.addPathV4, addPathV6: p.addPathV6}
	u, err := parseUpdate(body, o)
	if err != nil {
		return nil, err
	}
	r.stat.others += u.others
	if c.noRIB {
		p.countOnly(u, now)
	} else {
		p.apply(u, now)
	}
	p.gotEoR(u, now)
	return c.updateMsgs(r, &rp, u), nil
}

// updateMsgs : add 메시지 하나 + del 메시지 하나 (openbmpd 처럼 섞지 않음)
// 줄 번호 : 이 UPDATE 의 줄 수만큼 묶음으로 받음 (다른 라우터 goroutine 과 겹치지 않음)
func (c *collector) updateMsgs(r *router, rp *rowPeer, u *update) []outMsg {
	nAdd, nDel := len(u.v4Add)+len(u.v6Add), len(u.v4Del)+len(u.v6Del)
	r.stat.add += nAdd
	r.stat.del += nDel
	seq := c.seq.Add(uint64(nAdd+nDel)) - uint64(nAdd+nDel) // 이 묶음의 첫 번호 - 1
	topics := c.rules.topics(rp.routerIP, rp.peerIP)
	if len(topics) == 0 {
		r.stat.dropped += nAdd + nDel // 규칙 = 보내지 않음 : 줄을 만들지 않음
		return nil
	}
	// 줄 번호는 칸과 상관없이 원래 줄 순서대로 (칸을 나눠도 seq 는 나누지 않을 때와 같음)
	var out []outMsg
	s, e := &r.sink, &r.enc // 라우터마다 다시 쓰는 버퍼 (r.mu 안)
	s.reset(c.keyBuckets)
	e.reset(false, c.json, rp, &u.attrs)
	for _, p := range u.v4Add {
		seq++
		s.add(e, seq, p, true)
	}
	for _, p := range u.v6Add {
		seq++
		s.add(e, seq, p, false)
	}
	out = s.msgs(out, c, rp.peerHash, topics)
	s.reset(c.keyBuckets)
	e.reset(true, c.json, rp, nil)
	for _, p := range u.v4Del {
		seq++
		s.add(e, seq, p, true)
	}
	for _, p := range u.v6Del {
		seq++
		s.add(e, seq, p, false)
	}
	out = s.msgs(out, c, rp.peerHash, topics)
	e.reset(false, false, nil, nil) // UPDATE 를 붙잡고 있지 않게
	return out
}

// peerUp : per-peer 머리 + 로컬 주소 16 + 로컬 포트 2 + 원격 포트 2 + 보낸 OPEN + 받은 OPEN (+ 정보 TLV)
func (c *collector) peerUp(r *router, b []byte, now time.Time) ([]outMsg, error) {
	h, err := parsePeerHdr(b)
	if err != nil {
		return nil, err
	}
	b = b[peerHdrLen:]
	if len(b) < 20 {
		return nil, errShort
	}
	local := netip.AddrFrom16([16]byte(b[:16]))
	if h.addr.Is4() {
		local = netip.AddrFrom4([4]byte(b[12:16]))
	}
	lport, rport := be16(b[16:18]), be16(b[18:20])
	if len(b) == 20 {
		// OPEN 이 둘 다 없음 (RFC 7854 4.10 위반). 192.0.2.100 (IOS XR 6.2.3) 의 peer 198.51.100.103 이 이렇게 보냄.
		// openbmpd 는 여기서 라우터 연결 전체를 끊음 -> 우리는 주소 / 포트만으로 peer up 을 내고 기록을 남김
		log.Printf("router %s: peer_up %s AS %d without OPEN (RFC 7854 violation), sent peer up with empty capabilities",
			r.ip, h.addr, h.as)
		r.stat.noOpen++
		r.noOpen++
		p := c.peerSession(r, &h, local, lport, rport, nil, nil, now)
		p.noOpen = true
		return []outMsg{c.peerUpMsg(r, &h, local, lport, rport, nil, nil, now)}, nil
	}
	typ1, sentBody, rest, err := bgpMsg(b[20:])
	if err != nil {
		return nil, fmt.Errorf("peer_up sent open: %w", err)
	}
	typ2, recvBody, _, err := bgpMsg(rest)
	if err != nil {
		return nil, fmt.Errorf("peer_up recv open: %w", err)
	}
	if typ1 != 1 || typ2 != 1 {
		return nil, fmt.Errorf("peer_up bgp types %d %d (not open)", typ1, typ2)
	}
	sent, err1 := parseOpen(sentBody)
	recv, err2 := parseOpen(recvBody)
	if err1 != nil || err2 != nil {
		return nil, fmt.Errorf("peer_up open: %v %v", err1, err2)
	}
	c.peerSession(r, &h, local, lport, rport, &sent, &recv, now)
	return []outMsg{c.peerUpMsg(r, &h, local, lport, rport, &sent, &recv, now)}, nil
}

// peerDown : 상태를 지우고 peer down 메시지 (openbmpd 처럼 del 줄은 만들지 않음)
func (c *collector) peerDown(r *router, b []byte, now time.Time) ([]outMsg, error) {
	h, err := parsePeerHdr(b)
	if err != nil {
		return nil, err
	}
	m := c.peerDownMsg(r, &h, b[peerHdrLen:], now)
	r.peerGet(&h).markDown(now, downText(m))
	return []outMsg{m}, nil
}

// peerSession : Peer Up -> peer 현황 (새 세션 : 경로표를 비우고 처음부터). sent / recv 가 nil 이면 OPEN 없음
func (c *collector) peerSession(r *router, h *peerHdr, local netip.Addr, lport, rport uint16, sent, recv *openMsg, now time.Time) *peer {
	p := r.peerGet(h)
	p.clearRIB()
	p.up, p.gotUp, p.noOpen, p.since, p.downReason = true, true, false, now, ""
	p.localIP, p.localPort, p.rmtPort = local.String(), lport, rport
	p.holdSent, p.holdRecv, p.sentCaps, p.recvCaps = 0, 0, "", ""
	p.addPathV4, p.addPathV6, p.famV4, p.famV6 = false, false, false, false
	if sent != nil && recv != nil {
		p.famV4 = famOn(*sent, *recv, afiIPv4, safiUnicast)
		p.famV6 = famOn(*sent, *recv, afiIPv6, safiUnicast)
		p.holdSent, p.holdRecv = sent.hold, recv.hold
		p.sentCaps, p.recvCaps = strings.Join(sent.caps, ", "), strings.Join(recv.caps, ", ")
		p.addPathV4 = addPathOn(*sent, *recv, afiIPv4, safiUnicast)
		p.addPathV6 = addPathOn(*sent, *recv, afiIPv6, safiUnicast)
	}
	p.addEvent(now, "up (Peer Up)")
	return p
}

// downText : peer down 메시지 (탭 구분 31 필드) 에서 이유 글자 (21 bmp_reason, 22/23 코드, 24 글자)
func downText(m outMsg) string {
	_, rows, _ := strings.Cut(string(m.value), "\n\n")
	f := strings.Split(strings.TrimSuffix(rows, "\n"), "\t")
	if len(f) < 25 {
		return "?"
	}
	s := "reason " + f[21] + " " + downReasonName(f[21])
	if f[22] != "" {
		s += ", notification " + f[22] + "/" + f[23] + " " + f[24]
	}
	return s
}

// downReasonName : Peer Down 이유 번호 (RFC 7854 4.9, RFC 9069)
func downReasonName(code string) string {
	switch code {
	case "1":
		return "(local NOTIFICATION)"
	case "2":
		return "(local, no NOTIFICATION)"
	case "3":
		return "(remote NOTIFICATION)"
	case "4":
		return "(remote closed, no NOTIFICATION)"
	case "5":
		return "(peer de-configured)"
	case "6":
		return "(local system closed)"
	}
	return ""
}

// handle : 입력 쪽에서 부르는 곳 (잠금 포함). BMP 메시지 하나 -> 보낼 메시지들.
// 라우터마다 한 goroutine 이 부름 -> 다른 라우터와는 동시에, 같은 라우터는 차례로
func (c *collector) handle(ip, hash string, msg []byte, now time.Time) []outMsg {
	r := c.lockRouter(ip, hash)
	defer r.mu.Unlock()
	return c.safeProcess(r, msg, now)
}

// handleConn : tcp 입력의 메시지 하나. gen 이 지금 연결 번호가 아니면 (새 연결이 이어 받음) 처리하지 않고 false
func (c *collector) handleConn(ip string, gen uint64, msg []byte, now time.Time) ([]outMsg, bool) {
	r := c.lockRouter(ip, "")
	defer r.mu.Unlock()
	if gen != r.connGen {
		return nil, false
	}
	return c.safeProcess(r, msg, now), true
}

// connStart : TCP 새 연결 (첫 메시지 = Initiation 직전). 돌려주는 것 = 연결 번호 (connClosed 에 다시 줌)
func (c *collector) connStart(ip string, now time.Time) uint64 {
	r := c.lockRouter(ip, "")
	defer r.mu.Unlock()
	r.reset(now)
	r.connGen++
	return r.connGen
}

// connClosed : TCP 끊김 -> Termination 이 없었으면 router term 메시지. why = collector 가 끊은 이유 (reset)
// gen 이 지금 연결 번호가 아니면 (새 연결이 이미 시작됨) 아무것도 안 함
func (c *collector) connClosed(ip, why string, gen uint64, now time.Time) []outMsg {
	r := c.lockRouter(ip, "")
	defer r.mu.Unlock()
	if gen != r.connGen {
		return nil
	}
	return c.routerClosed(r, why, now)
}

// validIP : 라우터 주소 글자 검사 (Kafka 머리의 R_IP 등)
func validIP(s string) bool {
	_, err := netip.ParseAddr(s)
	return err == nil
}

// safeProcess : process + 오류 기록. panic (코드 실수, 깨진 입력) 도 이 메시지만 버리고 계속
func (c *collector) safeProcess(r *router, msg []byte, now time.Time) (out []outMsg) {
	defer func() {
		if e := recover(); e != nil {
			r.stat.errors++
			r.errors++
			c.logErr(r, fmt.Errorf("panic: %v", e))
			out = nil
		}
	}()
	out, err := c.process(r, msg, now)
	if err != nil {
		r.stat.errors++
		r.errors++
		c.logErr(r, err)
	}
	r.stat.msgs += len(out)
	return out
}

// logErr : 오류 기록 (라우터마다 1 분에 100 줄까지 : 로그가 커지지 않게)
func (c *collector) logErr(r *router, err error) {
	if r.stat.errors <= 100 {
		log.Printf("router %s: %v", r.ip, err)
	}
}

// logStat : 1 분 통계 한 줄을 남기고 0 으로
func (c *collector) logStat(p *producer) {
	kafkaErrs.flush()
	fails, raw := int64(0), ""
	m := minuteStat{Time: tstr(time.Now())}
	if p != nil {
		fails = p.fails.Load()
		if p.rawOn.Load() {
			n := p.rawN.Swap(0)
			raw, m.Raw = fmt.Sprintf(" raw=%d", n), &n
		}
	}
	m.KafkaFail = fails
	topics := ""
	if c.rules != nil {
		topics = " topics: " + c.rules.rotate()
		m.Topics = c.rules.lastMinute()
	}
	var total stats
	c.statMu.Lock()
	total.addAll(&c.stat)
	c.stat = stats{}
	c.statMu.Unlock()
	rs := c.sortedRouters()
	m.ByRouter = map[string]map[string]int{}
	for _, r := range rs {
		r.mu.Lock()
		total.addAll(&r.stat)
		if r.stat.msgs > 0 || r.stat.errors > 0 || r.stat.bmpTotal() > 0 {
			m.ByRouter[r.ip] = r.stat.counts()
		}
		r.stat = stats{}
		r.mu.Unlock()
	}
	log.Printf("stat routers=%d %s%s kafka_fail=%d%s", len(rs), total.String(), raw, fails, topics)
	m.Routers, m.Counts = len(rs), total.counts()
	hist.add(m, &total, p) // GET /api/stats (stats_api.go), /metrics (metrics.go)
}

// statErr : 라우터를 모르는 오류 하나 (Kafka bmp_raw 머리가 깨짐 등)
func (c *collector) statErr() {
	c.statMu.Lock()
	c.stat.errors++
	c.statMu.Unlock()
}
