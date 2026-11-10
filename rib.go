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
	labels       string // 라벨 "16001,3" (경로 종류 표만, 0.3.0 famrib.go. unicast 는 늘 빈칸)
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
	famV4, famV6         bool     // 협상된 unicast 종류 (OPEN 둘 다 알린 것) = End-of-RIB 를 기다릴 것
	fams                 []family // 협상된 경로 종류 모두 (0.3.0, API detail.families)
	addPathFams          []family // 그중 Add-Path 로 받는 것 (API detail.add_path)

	downReason string
	hist       []event

	// 보기마다 경로표와 수 (0.3.0) : nil = 그 보기로 받은 적 없음 (이번 세션). 보통 In pre 하나만
	views [nViews]*ribView

	// 무중단 판 올리기 (0.2.0, handover.go) 뒤 경로표를 뒤에서 읽는 동안 : 경로표 바꾸기는 ribLog 에 모았다가
	// 읽은 경로표에 차례로 (ribLoaded). 그 사이 경로표를 비우면 (Peer Up / Down) 기다림을 끝냄 = 읽은 것은 버림
	ribWait bool
	ribLog  []ribOp
}

// ---- 보기 (0.3.0) ----
//
// BMP 는 같은 네이버의 경로를 여러 단계로 보낼 수 있음 : per-peer 머리의 Peer Type 과 깃발 L (post-policy), O (Adj-RIB-Out).
// 단계마다 다른 경로 목록이므로 경로표를 따로 둠 (같은 표에 섞으면 서로 덮어쓰고 지움). Kafka 줄은 줄마다 isPrePolicy /
// isAdjRibIn 으로 이미 구분해 냄 (이것은 경로표 / API 만의 일)
const (
	viewInPre   = iota // Adj-RIB-In pre-policy  : 네이버가 보낸 그대로 (RFC 7854)
	viewInPost         // Adj-RIB-In post-policy : import 정책 뒤
	viewOutPre         // Adj-RIB-Out pre-policy : 보낼 후보 (RFC 8671)
	viewOutPost        // Adj-RIB-Out post-policy : 실제로 보낸 것
	viewLocRIB         // Loc-RIB : 라우터가 고른 최선 경로 (RFC 9069, Peer Type 3 = 네이버가 아니라 라우터 자신)
	nViews
)

var viewNames = [nViews]string{"in-pre", "in-post", "out-pre", "out-post", "loc-rib"}

// viewOf : per-peer 머리 -> 보기
func viewOf(typ, flags uint8) int {
	if typ == peerTypeLocRIB {
		return viewLocRIB
	}
	v := viewInPre
	if flags&flagO != 0 {
		v = viewOutPre
	}
	if flags&flagL != 0 {
		v++ // post-policy
	}
	return v
}

// parseView : API 질의 view= 글자 -> 보기 (없으면 -1 = 그 peer 의 주 보기)
func parseView(s string) (int, error) {
	if s == "" {
		return -1, nil
	}
	for i, n := range viewNames {
		if n == s {
			return i, nil
		}
	}
	return 0, fmt.Errorf("view=%q : in-pre | in-post | out-pre | out-post | loc-rib", s)
}

// ribView : 보기 하나의 경로표와 수 (이번 세션)
type ribView struct {
	rib4 ribTable[key4] // IPv4 unicast 경로표 (ribtable.go)
	rib6 ribTable[key6] // IPv6 unicast

	eorV4, eorV6 time.Time // End-of-RIB 받은 시각
	lastUpdate   time.Time
	grewAt       uint32 // 새 프리픽스가 마지막으로 들어온 unix 초 (settled, 0.2.0). 경로표가 없으면 add 를 받은 때
	adds, dels   uint64 // 이번 up 이후 받은 add / del 줄 수

	fam map[family]famTable // unicast 밖 종류의 경로표 (0.3.0 famrib.go, bmp.rib 에 있는 종류만. 보통 nil)
}

// emptyView : 받은 적 없는 보기를 읽을 때 (쓰지 않음)
var emptyView = &ribView{}

// view : 보기 v 를 읽음 (v < 0 = 주 보기). 없으면 빈 것 (읽기만)
func (p *peer) view(v int) *ribView {
	if v < 0 {
		v = p.mainView()
	}
	if p.views[v] == nil {
		return emptyView
	}
	return p.views[v]
}

// viewFor : 보기 v 에 씀 (없으면 만듦)
func (p *peer) viewFor(v int) *ribView {
	if p.views[v] == nil {
		p.views[v] = &ribView{}
	}
	return p.views[v]
}

// mainView : API 의 기본 보기 = 받은 보기 중 번호가 가장 작은 것 (보통 In pre). 없으면 Loc-RIB peer 는 Loc-RIB, 나머지는 In pre
func (p *peer) mainView() int {
	for v := range nViews {
		if p.views[v] != nil {
			return v
		}
	}
	if p.typ == peerTypeLocRIB {
		return viewLocRIB
	}
	return viewInPre
}

// viewCount : 받은 보기 수
func (p *peer) viewCount() int {
	n := 0
	for _, rv := range p.views {
		if rv != nil {
			n++
		}
	}
	return n
}

// routes : 보기 하나의 프리픽스 수 (v4, v6)
func (rv *ribView) routes() (int, int) { return rv.rib4.size(), rv.rib6.size() }

// addPathCounts : Add-Path 를 협상한 종류만 경로 수 (API paths_v4 / v6), 아니면 0 (= 칸을 뺌)
func (p *peer) addPathCounts(rv *ribView) (n4, n6 int) {
	if p.addPathV4 {
		n4 = rv.rib4.paths()
	}
	if p.addPathV6 {
		n6 = rv.rib6.paths()
	}
	return
}

// ribOp : 경로표를 읽는 동안 미뤄 둔 UPDATE 하나
type ribOp struct {
	v  int // 보기
	u  *update
	t  uint32
	on ribOn
}

func (p *peer) addEvent(t time.Time, text string) {
	if len(p.hist) >= histMax {
		p.hist = p.hist[1:]
	}
	p.hist = append(p.hist, event{t, text})
}

// clearRIB : 보기 모두의 경로표와 수를 버림 (세션 하나 = 보기 모두. 메모리를 돌려줌)
func (p *peer) clearRIB() {
	p.views = [nViews]*ribView{}
	p.ribWait, p.ribLog = false, nil
}

// synced : 주 보기에서 이번 세션의 처음 전체 전송을 다 받음 (협상된 종류마다 End-of-RIB).
// Peer Up 을 못 받았거나 (그림자) OPEN 이 없으면 무엇을 기다릴지 몰라 false
func (p *peer) synced() bool { return p.syncedIn(p.view(-1)) }

// syncedIn : 보기 rv 에서 synced
func (p *peer) syncedIn(rv *ribView) bool {
	if !p.up || !p.gotUp || (!p.famV4 && !p.famV6) {
		return false
	}
	return (!p.famV4 || !rv.eorV4.IsZero()) && (!p.famV6 || !rv.eorV6.IsZero())
}

// settleAfter : Peer Up 뒤 이만큼 새 프리픽스가 늘지 않으면 settled (End-of-RIB 를 BMP 로 안 넘기는 라우터용)
const settleAfter = 5 * time.Minute

// settled : 처음 전체 전송이 끝난 것으로 봄 = synced (End-of-RIB) 이거나, Peer Up 뒤 settleAfter 가 지났고
// 그동안 settleAfter 넘게 새 프리픽스가 없음. synced 와 같이 Peer Up 을 받은 up peer 만
func (p *peer) settled(now time.Time) bool { return p.settledIn(p.view(-1), now) }

// settledIn : 보기 rv 에서 settled
func (p *peer) settledIn(rv *ribView, now time.Time) bool {
	if p.syncedIn(rv) {
		return true
	}
	if !p.up || !p.gotUp || now.Sub(p.since) < settleAfter {
		return false
	}
	return now.Unix()-int64(rv.grewAt) >= int64(settleAfter/time.Second)
}

// gotEoR : 보기 v 의 End-of-RIB 를 기록 (처음 한 번만 이력에. In pre 가 아니면 보기 이름을 붙임)
func (p *peer) gotEoR(v int, u *update, now time.Time) {
	if !u.eorV4 && !u.eorV6 {
		return
	}
	rv := p.viewFor(v)
	name := ""
	if v != viewInPre {
		name = " " + viewNames[v]
	}
	if u.eorV4 && rv.eorV4.IsZero() {
		rv.eorV4 = now
		p.addEvent(now, fmt.Sprintf("End-of-RIB IPv4%s (routes %d)", name, rv.rib4.size()))
	}
	if u.eorV6 && rv.eorV6.IsZero() {
		rv.eorV6 = now
		p.addEvent(now, fmt.Sprintf("End-of-RIB IPv6%s (routes %d)", name, rv.rib6.size()))
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

// count : 보기 v 의 수와 시각 (경로표와 상관없이 늘 셈)
func (rv *ribView) count(u *update, now time.Time) {
	rv.adds += uint64(len(u.v4Add) + len(u.v6Add))
	rv.dels += uint64(len(u.v4Del) + len(u.v6Del))
	rv.lastUpdate = now
}

// countOnly : 경로표 없이 (-rib=false) 수와 시각만
func (p *peer) countOnly(v int, u *update, now time.Time) {
	rv := p.viewFor(v)
	rv.count(u, now)
	if len(u.v4Add)+len(u.v6Add) > 0 { // 경로표가 없어 새 프리픽스인지 모름 : add 를 받은 때로
		rv.grewAt = uint32(now.Unix())
	}
}

// apply : UPDATE 하나를 보기 v 의 경로표에 (on = 경로표를 드는 종류, -rib)
func (p *peer) apply(v int, u *update, now time.Time, on ribOn) {
	rv := p.viewFor(v)
	rv.count(u, now)
	t := uint32(now.Unix())
	if p.ribWait {
		p.ribLog = append(p.ribLog, ribOp{v, u, t, on})
		return
	}
	rv.ribApply(u, t, p.addPathV4, p.addPathV6, on, p.addPathFams)
}

// ribApply : UPDATE 하나를 경로표에 (t = unix 초). ap4 / ap6 = 이 세션이 그 종류에 Add-Path 를 협상함
// (그러면 NLRI 마다 path ID 가 있고, 넣기 / 빼기가 그 path ID 의 경로만 : ribtable.go)
func (rv *ribView) ribApply(u *update, t uint32, ap4, ap6 bool, on ribOn, apFams []family) {
	if u.fams != nil && len(on.fam) > 0 { // unicast 밖 종류 (famrib.go)
		rv.applyFams(u, t, on.fam, apFams)
	}
	if (!on.v4 && len(u.v4Add) > 0) || (!on.v6 && len(u.v6Add) > 0) { // 경로표가 꺼진 종류 : 새 경로인지 몰라 add 를 받은 때로 (settled)
		rv.grewAt = t
	}
	n := rv.rib4.paths() + rv.rib6.paths()
	if len(u.v4Add) > 0 && on.v4 {
		h := unique.Make(attrOf(&u.attrs, true))
		for _, x := range u.v4Add {
			if ap4 {
				rv.rib4.setPath(k4(x.addr, x.length), x.pathID, route{h, t})
			} else {
				rv.rib4.set(k4(x.addr, x.length), route{h, t})
			}
		}
	}
	if len(u.v6Add) > 0 && on.v6 {
		h := unique.Make(attrOf(&u.attrs, false))
		for _, x := range u.v6Add {
			if ap6 {
				rv.rib6.setPath(k6(x.addr, x.length), x.pathID, route{h, t})
			} else {
				rv.rib6.set(k6(x.addr, x.length), route{h, t})
			}
		}
	}
	if rv.rib4.paths()+rv.rib6.paths() > n { // 새 경로가 들어옴 (지우기 전에 셈 : settled)
		rv.grewAt = t
	}
	for _, x := range u.v4Del {
		if !on.v4 {
			break
		}
		if ap4 {
			rv.rib4.delPath(k4(x.addr, x.length), x.pathID)
		} else {
			rv.rib4.del(k4(x.addr, x.length)) // 없으면 그냥 지나감
		}
	}
	for _, x := range u.v6Del {
		if !on.v6 {
			break
		}
		if ap6 {
			rv.rib6.delPath(k6(x.addr, x.length), x.pathID)
		} else {
			rv.rib6.del(k6(x.addr, x.length))
		}
	}
}

// ribLoaded : 뒤에서 읽은 경로표 (보기마다, nil = 그 보기 없음) 를 넣고 미뤄 둔 UPDATE 를 차례로 (r.mu 쓰기 잠금 안에서).
// 수 / 시각 (adds, eor ...) 은 상태 파일의 peer 부분에서 이미 옴 : 경로표만 넣음.
// 기다림이 이미 끝났으면 (그 사이 경로표를 비움) 읽은 것은 버리고 false
// on = 이 판의 -rib : 옛 판이 넘긴 경로표 중 꺼진 종류는 버림
func (p *peer) ribLoaded(tabs [nViews]*ribView, on ribOn) bool {
	if !p.ribWait {
		return false
	}
	for v, t := range tabs {
		if t != nil {
			rv := p.viewFor(v)
			if on.v4 {
				rv.rib4 = t.rib4
			}
			if on.v6 {
				rv.rib6 = t.rib6
			}
			for f, ft := range t.fam {
				if on.fam[f] {
					if rv.fam == nil {
						rv.fam = map[family]famTable{}
					}
					rv.fam[f] = ft
				}
			}
		}
	}
	for _, op := range p.ribLog {
		p.viewFor(op.v).ribApply(op.u, op.t, p.addPathV4, p.addPathV6, op.on, p.addPathFams)
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

// routes : 라우터의 경로 수 합 (v4, v6) = peer 마다 주 보기
func (r *router) routes() (n4, n6 int) {
	for _, p := range r.peers {
		a, b := p.view(-1).routes()
		n4 += a
		n6 += b
	}
	return
}
