// 라우터 / peer 현황과 peer 별 경로표 (RIB) : 웹 API (api.go) 가 읽음
//
// 경로 수 = 지금 살아 있는 프리픽스 수 (add / del 횟수가 아님) :
//
//	add (새 프리픽스)      넣음 +1
//	add (이미 있음)        속성만 바꿈 (수 그대로, 100 번 와도 1 개)
//	del (있음)             뺌 -1
//	del (없음)             무시
//	Peer Down / 새 Peer Up / 라우터 끊김 -> 그 peer 경로표를 비움 (0)
//
// 열쇠 = (프리픽스, 길이). Add-Path 의 path id 는 넣지 않음 (쓰는 곳 없음, 10-28 표본).
// 같은 속성 묶음은 unique.Make 로 한 번만 저장 (어느 경로도 안 쓰면 GC 가 지움).
//
// 잠금 : collector.mu 하나. 처리 goroutine 은 메시지마다 쓰기 잠금, API 는 읽기 잠금.
package main

import (
	"encoding/binary"
	"fmt"
	"net/netip"
	"strings"
	"time"
	"unique"
)

// pathAttr : 경로 속성 묶음 (API 에 보여 줄 것만)
type pathAttr struct {
	asPath       string
	originAS     uint32
	originSet    string // 경로가 AS_SET 으로 끝나면 "{a,b}" (originAS 는 0)
	nextHop      string
	origin       string
	med          uint32
	localPref    uint32
	community    string
	extComm      string
	largeComm    string
	clusterList  string
	originatorID string
	aggregator   string
}

// route : 경로표의 값 하나 = 속성 (공유) + 마지막 add 시각
type route struct {
	a unique.Handle[pathAttr]
	t uint32 // unix 초
}

// 열쇠 : v4 = 주소 32 비트 << 8 | 길이, v6 = 주소 128 비트 + 길이
type key4 uint64
type key6 struct {
	hi, lo uint64
	n      uint8
}

func k4(a netip.Addr, n int) key4 {
	b := a.Unmap().As4()
	return key4(uint64(binary.BigEndian.Uint32(b[:]))<<8 | uint64(n))
}

func k6(a netip.Addr, n int) key6 {
	b := a.As16()
	return key6{binary.BigEndian.Uint64(b[:8]), binary.BigEndian.Uint64(b[8:]), uint8(n)}
}

func (k key4) prefix() netip.Prefix {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(k>>8))
	return netip.PrefixFrom(netip.AddrFrom4(b), int(k&0xff))
}

func (k key6) prefix() netip.Prefix {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], k.hi)
	binary.BigEndian.PutUint64(b[8:], k.lo)
	return netip.PrefixFrom(netip.AddrFrom16(b), int(k.n))
}

// event : peer 이력 한 줄
type event struct {
	t    time.Time
	text string
}

const histMax = 10 // peer 마다 남기는 이력 수

// peer : 라우터 하나의 peer 하나 (down 이 되어도 목록에 남음, collector 를 다시 시작하면 잊음)
type peer struct {
	key      string // peer hash
	ip       netip.Addr
	ipStr    string // ip 글자 (줄마다 쓰므로 한 번만 만듦)
	rd       string
	typ      uint8 // per-peer 머리 종류 (0 global, 1 rd, 2 local, 3 loc-rib)
	flags    uint8
	as       uint32
	bgpID    string
	bgpIDRaw [4]byte // bgpID 의 바이트 (바뀌었는지 보려고)

	bmpStats   map[string]uint64 // 마지막 BMP Stats Report (bmp_stats.go, 0.2.0). nil = 받은 적 없음
	bmpStatsAt time.Time

	up     bool
	gotUp  bool // 이번 BMP 세션에서 Peer Up 을 받음 = 경로표가 처음부터 정확
	noOpen bool // Peer Up 에 OPEN 이 없었음 (RFC 7854 위반)
	since  time.Time

	// Peer Up 에서
	localIP              string
	localPort, rmtPort   uint16
	holdSent, holdRecv   uint16
	sentCaps, recvCaps   string
	addPathV4, addPathV6 bool
	famV4, famV6         bool // 협상된 unicast 종류 (OPEN 둘 다 알린 것) = End-of-RIB 를 기다릴 것

	eorV4, eorV6 time.Time // End-of-RIB 받은 시각 (이번 세션)

	lastUpdate time.Time
	grewAt     uint32 // 경로표에 새 프리픽스가 마지막으로 들어온 unix 초 (settled, 0.2.0). -rib=false 면 add 를 받은 때
	adds, dels uint64 // 이번 up 이후 받은 add / del 줄 수
	downReason string
	hist       []event

	rib4 map[key4]route
	rib6 map[key6]route

	// 무중단 판 올리기 (0.2.0, handover.go) 뒤 경로표를 뒤에서 읽는 동안 : 경로표 바꾸기는 ribLog 에 모았다가
	// 읽은 경로표에 차례로 (ribLoaded). 그 사이 경로표를 비우면 (Peer Up / Down) 기다림을 끝냄 = 읽은 것은 버림
	ribWait bool
	ribLog  []ribOp
}

// ribOp : 경로표를 읽는 동안 미뤄 둔 UPDATE 하나
type ribOp struct {
	u *update
	t uint32
}

func (p *peer) addEvent(t time.Time, text string) {
	if len(p.hist) >= histMax {
		p.hist = p.hist[1:]
	}
	p.hist = append(p.hist, event{t, text})
}

// clearRIB : 경로표를 비움 (nil 로 : 메모리를 돌려줌)
func (p *peer) clearRIB() {
	p.rib4, p.rib6 = nil, nil
	p.ribWait, p.ribLog = false, nil
	p.adds, p.dels, p.grewAt = 0, 0, 0
	p.eorV4, p.eorV6 = time.Time{}, time.Time{}
}

// synced : 이번 세션의 처음 전체 전송을 다 받음 (협상된 종류마다 End-of-RIB).
// Peer Up 을 못 받았거나 (그림자) OPEN 이 없으면 무엇을 기다릴지 몰라 false
func (p *peer) synced() bool {
	if !p.up || !p.gotUp || (!p.famV4 && !p.famV6) {
		return false
	}
	return (!p.famV4 || !p.eorV4.IsZero()) && (!p.famV6 || !p.eorV6.IsZero())
}

// settleAfter : Peer Up 뒤 이만큼 새 프리픽스가 늘지 않으면 settled (End-of-RIB 를 BMP 로 안 넘기는 라우터용)
const settleAfter = 5 * time.Minute

// settled : 처음 전체 전송이 끝난 것으로 봄 = synced (End-of-RIB) 이거나, Peer Up 뒤 settleAfter 가 지났고
// 그동안 settleAfter 넘게 새 프리픽스가 없음. synced 와 같이 Peer Up 을 받은 up peer 만
func (p *peer) settled(now time.Time) bool {
	if p.synced() {
		return true
	}
	if !p.up || !p.gotUp || now.Sub(p.since) < settleAfter {
		return false
	}
	return now.Unix()-int64(p.grewAt) >= int64(settleAfter/time.Second)
}

// gotEoR : End-of-RIB 를 기록 (처음 한 번만 이력에)
func (p *peer) gotEoR(u *update, now time.Time) {
	if u.eorV4 && p.eorV4.IsZero() {
		p.eorV4 = now
		p.addEvent(now, fmt.Sprintf("End-of-RIB IPv4 (routes %d)", len(p.rib4)))
	}
	if u.eorV6 && p.eorV6.IsZero() {
		p.eorV6 = now
		p.addEvent(now, fmt.Sprintf("End-of-RIB IPv6 (routes %d)", len(p.rib6)))
	}
}

// markDown : down 으로 (경로 수 0)
func (p *peer) markDown(t time.Time, why string) {
	if !p.up {
		return
	}
	p.up, p.gotUp = false, false
	p.since, p.downReason = t, why
	p.clearRIB()
	p.addEvent(t, "down : "+why)
}

// attrOf : UPDATE 속성 -> 경로표 속성 (next hop 고르는 법은 addRow 와 같음)
func attrOf(a *attrs, v4 bool) pathAttr {
	nh := a.mpNextHop
	if v4 && a.nextHop != "" {
		nh = a.nextHop
	}
	return pathAttr{
		asPath: strings.TrimSpace(a.asPath), originAS: a.originAS, originSet: a.originSet, nextHop: nh, origin: a.origin,
		med: a.med, localPref: a.localPref, community: a.community, extComm: a.extComm, largeComm: a.largeComm,
		clusterList: strings.TrimSpace(a.clusterList), originatorID: a.originatorID, aggregator: a.aggregator,
	}
}

// apply : UPDATE 하나를 경로표에
// countOnly : 경로표 없이 (-rib=false) 수와 시각만
func (p *peer) countOnly(u *update, now time.Time) {
	p.adds += uint64(len(u.v4Add) + len(u.v6Add))
	p.dels += uint64(len(u.v4Del) + len(u.v6Del))
	p.lastUpdate = now
	if len(u.v4Add)+len(u.v6Add) > 0 { // 경로표가 없어 새 프리픽스인지 모름 : add 를 받은 때로
		p.grewAt = uint32(now.Unix())
	}
}

func (p *peer) apply(u *update, now time.Time) {
	p.adds += uint64(len(u.v4Add) + len(u.v6Add))
	p.dels += uint64(len(u.v4Del) + len(u.v6Del))
	p.lastUpdate = now
	t := uint32(now.Unix())
	if p.ribWait {
		p.ribLog = append(p.ribLog, ribOp{u, t})
		return
	}
	p.ribApply(u, t)
}

// ribApply : UPDATE 하나를 경로표에 (t = unix 초)
func (p *peer) ribApply(u *update, t uint32) {
	n := len(p.rib4) + len(p.rib6)
	if len(u.v4Add) > 0 {
		h := unique.Make(attrOf(&u.attrs, true))
		if p.rib4 == nil {
			p.rib4 = map[key4]route{}
		}
		for _, x := range u.v4Add {
			p.rib4[k4(x.addr, x.length)] = route{h, t}
		}
	}
	if len(u.v6Add) > 0 {
		h := unique.Make(attrOf(&u.attrs, false))
		if p.rib6 == nil {
			p.rib6 = map[key6]route{}
		}
		for _, x := range u.v6Add {
			p.rib6[k6(x.addr, x.length)] = route{h, t}
		}
	}
	if len(p.rib4)+len(p.rib6) > n { // 새 프리픽스가 들어옴 (지우기 전에 셈 : settled)
		p.grewAt = t
	}
	for _, x := range u.v4Del {
		delete(p.rib4, k4(x.addr, x.length)) // nil map 에 delete 는 그냥 지나감
	}
	for _, x := range u.v6Del {
		delete(p.rib6, k6(x.addr, x.length))
	}
}

// ribLoaded : 뒤에서 읽은 경로표를 넣고 미뤄 둔 UPDATE 를 차례로 (r.mu 쓰기 잠금 안에서).
// 기다림이 이미 끝났으면 (그 사이 경로표를 비움) 읽은 것은 버리고 false
func (p *peer) ribLoaded(rib4 map[key4]route, rib6 map[key6]route) bool {
	if !p.ribWait {
		return false
	}
	p.rib4, p.rib6 = rib4, rib6
	for _, op := range p.ribLog {
		p.ribApply(op.u, op.t)
	}
	p.ribWait, p.ribLog = false, nil
	return true
}

// peerID : peer 를 찾는 열쇠 = per-peer 머리의 주소 + RD 바이트 (비교 가능한 구조체 = C 의 memcmp 열쇠).
// peer hash (md5 글자) 는 peer 를 처음 만들 때 한 번만 계산 (0.2.0 : 전에는 메시지마다 두 번)
type peerID struct {
	addr netip.Addr
	rd   [8]byte
}

// peerGet : per-peer 머리로 peer 를 찾음 (없으면 만듦). AS, BGP ID 는 머리의 최신 값
func (r *router) peerGet(h *peerHdr) *peer {
	id := peerID{h.addr, h.rd}
	p := r.byID[id]
	if p == nil {
		ip, rd := h.addr.String(), rdString(h.rd)
		key := md5hex(ip, rd, r.hash)
		if p = r.peers[key]; p == nil {
			p = &peer{key: key, ip: h.addr, ipStr: ip, rd: rd, typ: h.typ}
			r.peers[key] = p
		}
		r.byID[id] = p
	}
	p.flags, p.as = h.flags, h.as
	if p.bgpID == "" || p.bgpIDRaw != h.bgpID { // 글자는 바뀔 때만 만듦
		p.bgpIDRaw, p.bgpID = h.bgpID, netip.AddrFrom4(h.bgpID).String()
	}
	return p
}

// routerDown : 라우터 연결이 끝남 -> 모든 peer down (경로 수 0)
func (r *router) routerDown(t time.Time, why string) {
	r.up, r.since, r.lastTerm = false, t, why
	for _, p := range r.peers {
		p.markDown(t, "router "+why)
	}
}

// routes : 라우터의 경로 수 합 (v4, v6)
func (r *router) routes() (n4, n6 int) {
	for _, p := range r.peers {
		n4 += len(p.rib4)
		n6 += len(p.rib6)
	}
	return
}
