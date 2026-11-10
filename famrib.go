// famrib.go : unicast 밖 경로 종류의 경로표 (0.3.0 : labeled unicast, L3VPN, multicast. FlowSpec 표는 flowspec.go 의 fsTable)
//
// 같은 ribTable[K] (ribtable.go) 를 쓰고 열쇠만 종류마다 : labeled / multicast = key4 / key6 (프리픽스),
// L3VPN = keyVPN4 / keyVPN6 (RD + 프리픽스 : 같은 프리픽스도 RD 가 다르면 다른 경로). 라벨은 경로 속성에 (pathAttr.labels).
// 보기 (ribView) 마다 종류 -> 표 (fam). 설정 bmp.rib 에 그 종류가 있을 때만 (기본 끔)
package main

import (
	"cmp"
	"encoding/binary"
	"net/netip"
	"slices"
	"unique"
)

// keyVPN4 / keyVPN6 : RD + 프리픽스
type keyVPN4 struct {
	rd [8]byte
	k  key4
}

type keyVPN6 struct {
	rd [8]byte
	k  key6
}

func (k keyVPN4) prefix() netip.Prefix { return k.k.prefix() }
func (k keyVPN6) prefix() netip.Prefix { return k.k.prefix() }

// famRow : 종류 표에서 꺼낸 경로 하나 (API / 상태 파일)
type famRow struct {
	pfx  netip.Prefix // FlowSpec 은 목적지 프리픽스 (없으면 !IsValid)
	rd   [8]byte
	vpn  bool
	p    pathOf
	rule bool // FlowSpec : 속성의 labels 칸에 규칙 글자
}

// famTable : 종류 하나의 경로표 (종류마다 열쇠가 달라 interface : C 의 함수 포인터 표)
type famTable interface {
	size() int  // 프리픽스 (RD + 프리픽스) 수
	paths() int // 경로 수
	apply(fu *famUpdate, a *attrs, t uint32, addPath bool)
	each(fn func(famRow) bool) // 모든 경로 (순서 없음)
	encode(b []byte, num func(unique.Handle[pathAttr]) uint64) []byte
	decode(d *dec, handle func() unique.Handle[pathAttr])
}

// famTab : 열쇠 K 의 famTable
type famTab[K ribKey] struct {
	t      ribTable[K]
	key    func(r *famRoute) K
	rd     func(k K) ([8]byte, bool) // 열쇠의 RD (L3VPN)
	encKey func(b []byte, k K) []byte
	decKey func(d *dec) K
}

// newFamTable : 종류 f 의 빈 표 (경로표를 들 수 없는 종류면 nil)
func newFamTable(f family) famTable {
	if _, ok := fsFams[f]; ok { // FlowSpec (flowspec.go)
		return &fsTable{f: f}
	}
	sp, ok := famSpecs[f]
	if !ok {
		return nil
	}
	noRD4 := func(key4) ([8]byte, bool) { return [8]byte{}, false }
	noRD6 := func(key6) ([8]byte, bool) { return [8]byte{}, false }
	switch {
	case sp.vpn && sp.afi == afiIPv4:
		return &famTab[keyVPN4]{
			key:    func(r *famRoute) keyVPN4 { return keyVPN4{r.rd, k4(r.pfx.addr, r.pfx.length)} },
			rd:     func(k keyVPN4) ([8]byte, bool) { return k.rd, true },
			encKey: func(b []byte, k keyVPN4) []byte { return encKey4(append(b, k.rd[:]...), k.k) },
			decKey: func(d *dec) keyVPN4 { var k keyVPN4; copy(k.rd[:], d.take(8)); k.k = decKey4(d); return k },
		}
	case sp.vpn:
		return &famTab[keyVPN6]{
			key:    func(r *famRoute) keyVPN6 { return keyVPN6{r.rd, k6(r.pfx.addr, r.pfx.length)} },
			rd:     func(k keyVPN6) ([8]byte, bool) { return k.rd, true },
			encKey: func(b []byte, k keyVPN6) []byte { return encKey6(append(b, k.rd[:]...), k.k) },
			decKey: func(d *dec) keyVPN6 { var k keyVPN6; copy(k.rd[:], d.take(8)); k.k = decKey6(d); return k },
		}
	case sp.afi == afiIPv4:
		return &famTab[key4]{key: func(r *famRoute) key4 { return k4(r.pfx.addr, r.pfx.length) }, rd: noRD4, encKey: encKey4, decKey: decKey4}
	default:
		return &famTab[key6]{key: func(r *famRoute) key6 { return k6(r.pfx.addr, r.pfx.length) }, rd: noRD6, encKey: encKey6, decKey: decKey6}
	}
}

func (t *famTab[K]) size() int  { return t.t.size() }
func (t *famTab[K]) paths() int { return t.t.paths() }

// apply : 종류 묶음 하나를 표에 (a = 이 종류의 next hop 을 넣은 속성)
func (t *famTab[K]) apply(fu *famUpdate, a *attrs, tm uint32, addPath bool) {
	v4 := famSpecs[fu.f].afi == afiIPv4
	for i := range fu.add {
		r := &fu.add[i]
		pa := attrOf(a, v4)
		pa.labels = string(labelText(nil, r.labels))
		rt := route{unique.Make(pa), tm}
		if addPath {
			t.t.setPath(t.key(r), r.pfx.pathID, rt)
		} else {
			t.t.set(t.key(r), rt)
		}
	}
	for i := range fu.del {
		r := &fu.del[i]
		if addPath {
			t.t.delPath(t.key(r), r.pfx.pathID)
		} else {
			t.t.del(t.key(r))
		}
	}
}

func (t *famTab[K]) each(fn func(famRow) bool) {
	for k, p := range t.t.all() {
		rd, vpn := t.rd(k)
		if !fn(famRow{pfx: k.prefix(), rd: rd, vpn: vpn, p: p}) {
			return
		}
	}
}

// encode : 상태 파일 (형식 2) = 경로 수 + 경로마다 (열쇠, Add-Path 표시 1 바이트, [path ID], 속성 번호, 시각)
func (t *famTab[K]) encode(b []byte, num func(unique.Handle[pathAttr]) uint64) []byte {
	b = binary.AppendUvarint(b, uint64(t.t.paths()))
	for k, p := range t.t.all() {
		b = t.encKey(b, k)
		if p.addPath {
			b = binary.AppendUvarint(append(b, 1), uint64(p.id))
		} else {
			b = append(b, 0)
		}
		b = binary.AppendUvarint(b, num(p.rt.a))
		b = binary.LittleEndian.AppendUint32(b, p.rt.t)
	}
	return b
}

func (t *famTab[K]) decode(d *dec, handle func() unique.Handle[pathAttr]) {
	for range d.count(8) {
		k := t.decKey(d)
		ap := d.u8() == 1
		var id uint32
		if ap {
			id = d.u32()
		}
		rt := route{handle(), d.le32()}
		if d.err != nil {
			return
		}
		if ap {
			t.t.setPath(k, id, rt)
		} else {
			t.t.set(k, rt)
		}
	}
}

func encKey4(b []byte, k key4) []byte {
	b = binary.LittleEndian.AppendUint32(b, uint32(k>>8))
	return append(b, byte(k))
}

func decKey4(d *dec) key4 {
	a := d.le32()
	return key4(uint64(a)<<8 | uint64(d.u8()))
}

func encKey6(b []byte, k key6) []byte {
	b = binary.LittleEndian.AppendUint64(b, k.hi)
	b = binary.LittleEndian.AppendUint64(b, k.lo)
	return append(b, k.n)
}

func decKey6(d *dec) key6 { return key6{hi: d.le64(), lo: d.le64(), n: d.u8()} }

// ---- 보기 안의 종류 표 ----

// famFor : 보기 rv 의 종류 f 표 (없으면 만듦)
func (rv *ribView) famFor(f family) famTable {
	if rv.fam == nil {
		rv.fam = map[family]famTable{}
	}
	t := rv.fam[f]
	if t == nil {
		t = newFamTable(f)
		rv.fam[f] = t
	}
	return t
}

// applyFams : UPDATE 의 경로 종류 묶음을 표에 (on = 경로표를 드는 종류, apFams = Add-Path 로 받는 종류)
func (rv *ribView) applyFams(u *update, tm uint32, on map[family]bool, apFams []family) {
	for i := range u.fams {
		fu := &u.fams[i]
		if !on[fu.f] || fu.count() == 0 {
			continue
		}
		t := rv.famFor(fu.f)
		if t == nil {
			continue
		}
		a := u.attrs
		a.mpNextHop, a.nextHop = fu.nextHop, ""
		t.apply(fu, &a, tm, slices.Contains(apFams, fu.f))
	}
}

// famCounts : 종류마다 경로 수 (API family_routes, 없으면 nil)
func (rv *ribView) famCounts() map[string]int {
	if len(rv.fam) == 0 {
		return nil
	}
	m := map[string]int{}
	for f, t := range rv.fam {
		m[f.String()] = t.paths()
	}
	return m
}

// famRows : 보기 rv 의 종류 f 경로 모두, (RD, 프리픽스, path ID) 순
func (rv *ribView) famRows(f family) []famRow {
	t := rv.fam[f]
	if t == nil {
		return nil
	}
	rows := make([]famRow, 0, t.paths())
	t.each(func(r famRow) bool { rows = append(rows, r); return true })
	slices.SortFunc(rows, func(a, b famRow) int {
		return cmp.Or(slices.Compare(a.rd[:], b.rd[:]), a.pfx.Addr().Compare(b.pfx.Addr()), a.pfx.Bits()-b.pfx.Bits(), cmp.Compare(a.p.id, b.p.id))
	})
	return rows
}

// famMatch : peer 하나 (보기 v) 의 종류 f 표에서 q 와 맞는 경로 줄 (q 가 없으면 모두). how = exact / longest / covering / more.
// 표를 한 번 훑음 (종류 표는 unicast 보다 작음). longest 는 RD 마다 가장 긴 것. 잠금은 부르는 쪽 (r.mu 읽기)
func famMatch(r *router, p *peer, v int, f family, q netip.Prefix, how string) []ribRow {
	base := ribRow{router: r.ip, routerName: r.sysName, peer: p.ip.String(), peerAS: p.as}
	keep := func(x netip.Prefix) bool {
		if !q.IsValid() {
			return true
		}
		if !x.IsValid() || x.Addr().Is4() != q.Addr().Is4() { // FlowSpec 규칙에 목적지가 없으면 프리픽스로 찾을 수 없음
			return false
		}
		switch how {
		case "exact":
			return x == q
		case "more":
			return x.Bits() >= q.Bits() && q.Contains(x.Addr())
		default: // longest / covering : q 를 덮는 것
			return x.Bits() <= q.Bits() && x.Contains(q.Addr())
		}
	}
	var hits []famRow
	best := map[[8]byte]int{} // longest : RD 마다 가장 긴 길이
	for _, fr := range p.view(v).famRows(f) {
		if keep(fr.pfx) {
			hits = append(hits, fr)
			best[fr.rd] = max(best[fr.rd], fr.pfx.Bits())
		}
	}
	var out []ribRow
	for _, fr := range hits {
		if how == "longest" && q.IsValid() && fr.pfx.Bits() != best[fr.rd] {
			continue
		}
		x := base
		x.pfx = fr.pfx
		x.setPath(fr.p)
		x.rule = fr.rule
		if fr.vpn {
			x.rd = rdString(fr.rd)
		}
		out = append(out, x)
	}
	return out
}
