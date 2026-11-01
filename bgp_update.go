// BGP UPDATE 해석 (RFC 4271, 4760, 6793, 7911) : 경로 속성 + NLRI
//
// 속성 글자 모양은 openbmpd v1.7 출력과 같게 만듦 (표본으로 확인).
package main

import (
	"fmt"
	"net/netip"
	"strings"
)

// 경로 속성 번호
const (
	attrOrigin      = 1
	attrASPath      = 2
	attrNextHop     = 3
	attrMED         = 4
	attrLocalPref   = 5
	attrAtomicAgg   = 6
	attrAggregator  = 7
	attrCommunity   = 8
	attrOriginator  = 9
	attrClusterList = 10
	attrMPReach     = 14
	attrMPUnreach   = 15
	attrExtComm     = 16
	attrAS4Path     = 17
	attrAS4Agg      = 18
	attrLargeComm   = 32
)

// prefix : NLRI 하나
type prefix struct {
	addr   netip.Addr
	length int
	pathID uint32
}

// attrs : 줄에 들어가는 속성 (이미 글자로 바꾼 것)
type attrs struct {
	origin       string
	asPath       string // " 1299 3257 { 1 2 }" (AS 마다 앞 공백)
	asCount      int    // 경로 길이 (RFC 4271 : AS_SET = 1, CONFED = 0)
	originAS     uint32 // 마지막 AS_SEQUENCE 의 마지막 AS (경로가 AS_SET 으로 끝나면 0)
	originSet    string // 경로가 AS_SET 으로 끝나면 그 묶음 "{38266,45271}" (origin_as 칸에 이것)
	segs         []asSeg
	nextHop      string // NEXT_HOP (IPv4)
	mpNextHop    string // MP_REACH 의 next hop
	med          uint32
	localPref    uint32
	aggregator   string // "AS IP"
	atomic       bool
	community    string // "a:b c:d"
	extComm      string // "rt=1:2 soo=3:4"
	clusterList  string // "1.2.3.4 5.6.7.8 " (뒤 공백)
	originatorID string
	largeComm    string // "a:b:c"
}

// update : UPDATE 하나를 나눈 결과
type update struct {
	attrs  attrs
	v4Add  []prefix
	v4Del  []prefix
	v6Add  []prefix
	v6Del  []prefix
	others int // unicast 가 아닌 MP_REACH / UNREACH 수 (VPN, EVPN 등 : 세고 버림)

	eorV4, eorV6 bool // End-of-RIB (RFC 4724) : 처음 전체 전송이 끝났다는 표시
}

// pathOpts : peer 상태에서 오는 해석 옵션
type pathOpts struct {
	as4       bool // AS_PATH 의 AS 가 4 바이트
	addPathV4 bool // IPv4 unicast NLRI 앞에 path id
	addPathV6 bool
}

// parseUpdate : UPDATE 몸통 (BGP 머리 뒤)
func parseUpdate(b []byte, o pathOpts) (*update, error) {
	u := &update{}
	if len(b) < 4 {
		return nil, errShort
	}
	if len(b) == 4 && be32(b) == 0 { // 철회 0 + 속성 0 + NLRI 없음 = IPv4 unicast End-of-RIB
		u.eorV4 = true
		return u, nil
	}
	// 1. 철회된 IPv4 경로
	wl := int(be16(b))
	if 2+wl+2 > len(b) {
		return nil, fmt.Errorf("withdrawn length %d", wl)
	}
	var err error
	if u.v4Del, err = parseNLRI(b[2:2+wl], afiIPv4, o.addPathV4); err != nil {
		return nil, fmt.Errorf("withdrawn: %w", err)
	}
	b = b[2+wl:]
	// 2. 경로 속성
	al := int(be16(b))
	if 2+al > len(b) {
		return nil, fmt.Errorf("attr length %d", al)
	}
	if err = parseAttrs(b[2:2+al], o, u); err != nil {
		return nil, err
	}
	// 3. 나머지 = 새 IPv4 경로 (MP_REACH AFI 1 로 온 것 뒤에 이어 붙임)
	p, err := parseNLRI(b[2+al:], afiIPv4, o.addPathV4)
	if err != nil {
		return nil, fmt.Errorf("nlri: %w", err)
	}
	u.v4Add = append(u.v4Add, p...)
	return u, nil
}

// parseNLRI : [path id 4] + 길이 1 + 주소 (길이 만큼 비트) 를 되풀이
func parseNLRI(b []byte, afi int, addPath bool) ([]prefix, error) {
	var out []prefix
	maxBits := 32
	if afi == afiIPv6 {
		maxBits = 128
	}
	for len(b) > 0 {
		var p prefix
		if addPath {
			if len(b) < 4 {
				return nil, errShort
			}
			p.pathID = be32(b)
			b = b[4:]
		}
		if len(b) < 1 {
			return nil, errShort
		}
		p.length = int(b[0])
		nb := (p.length + 7) / 8
		if p.length > maxBits || 1+nb > len(b) {
			return nil, fmt.Errorf("prefix length %d", p.length)
		}
		var a [16]byte
		copy(a[:], b[1:1+nb])
		if afi == afiIPv6 {
			p.addr = netip.AddrFrom16(a)
		} else {
			p.addr = netip.AddrFrom4([4]byte(a[:4]))
		}
		out = append(out, p)
		b = b[1+nb:]
	}
	return out, nil
}

// parseAttrs : 속성 = flag 1 + 종류 1 + 길이 (1 또는 extended 면 2) + 값
func parseAttrs(b []byte, o pathOpts, u *update) error {
	var as4path []byte // 2 바이트 AS peer 의 AS4_PATH (나중에 합침)
	for len(b) > 0 {
		if len(b) < 3 {
			return fmt.Errorf("attr: %w", errShort)
		}
		flags, typ := b[0], b[1]
		hl, n := 3, int(b[2])
		if flags&0x10 != 0 { // extended length
			if len(b) < 4 {
				return fmt.Errorf("attr: %w", errShort)
			}
			hl, n = 4, int(be16(b[2:4]))
		}
		if hl+n > len(b) {
			return fmt.Errorf("attr %d length %d", typ, n)
		}
		v := b[hl : hl+n]
		b = b[hl+n:]
		if typ == attrAS4Path {
			as4path = v
			continue
		}
		if err := parseAttr(typ, v, o, u); err != nil {
			return fmt.Errorf("attr %d: %w", typ, err)
		}
	}
	if as4path != nil && !o.as4 {
		mergeAS4Path(as4path, &u.attrs)
	}
	return nil
}

// parseAttr : 속성 하나
func parseAttr(typ uint8, v []byte, o pathOpts, u *update) error {
	a := &u.attrs
	switch typ {
	case attrOrigin:
		if len(v) < 1 {
			return errShort
		}
		a.origin = [...]string{"igp", "egp", "incomplete"}[min(int(v[0]), 2)]
	case attrASPath:
		return parseASPath(v, o.as4, a)
	case attrNextHop:
		if len(v) == 4 {
			a.nextHop = netip.AddrFrom4([4]byte(v)).String()
		}
	case attrMED:
		if len(v) == 4 {
			a.med = be32(v)
		}
	case attrLocalPref:
		if len(v) == 4 {
			a.localPref = be32(v)
		}
	case attrAtomicAgg:
		a.atomic = true
	case attrAggregator:
		a.aggregator = aggregatorString(v)
	case attrCommunity:
		a.community = communityString(v)
	case attrOriginator:
		if len(v) == 4 {
			a.originatorID = netip.AddrFrom4([4]byte(v)).String()
		}
	case attrClusterList:
		var sb strings.Builder
		for i := 0; i+4 <= len(v); i += 4 {
			sb.WriteString(netip.AddrFrom4([4]byte(v[i : i+4])).String())
			sb.WriteByte(' ')
		}
		a.clusterList = sb.String()
	case attrMPReach:
		return parseMPReach(v, o, u)
	case attrMPUnreach:
		return parseMPUnreach(v, o, u)
	case attrExtComm:
		a.extComm = extCommString(v)
	case attrLargeComm:
		a.largeComm = largeCommString(v)
	}
	return nil
}

// ---- AS_PATH (RFC 4271, 5065, 6793) ----
//
// 세그먼트 단위로 해석한 뒤 (asSeg) 글자 / 개수 / origin 을 표준대로 계산 (10-30, openbmpd 의 버릇 대신) :
//
//	글자      : AS_SEQUENCE " 1299 3257", AS_SET " {38266,45271}", CONFED_SEQUENCE " (65001 65002)", CONFED_SET " [65001,65002]"
//	            (AS 마다 앞 공백은 openbmpd 와 같은 꼴. 묶음은 Cisco / Juniper 표시처럼 중괄호, 안은 쉼표 = 공백으로 나눠도 한 덩어리)
//	개수      : RFC 4271 9.1.2.2 = AS_SET 은 묶음 하나를 1, CONFED 는 0 (RFC 5065) (openbmpd : 묶음 안 AS 를 하나씩 셈)
//	origin    : 마지막 세그먼트가 AS_SEQUENCE 면 그 마지막 AS. AS_SET 이면 origin 을 하나로 정할 수 없음 (RFC 6811 : NONE)
//	            -> originSet = 묶음 글자 "{38266,45271}" 을 origin_as 칸에 그대로 (openbmpd : 묶음의 마지막 AS 를 고름)

// asSeg : AS_PATH 세그먼트 하나 (typ 1 SET, 2 SEQUENCE, 3 CONFED_SEQUENCE, 4 CONFED_SET)
type asSeg struct {
	typ  uint8
	asns []uint32
}

// parseSegs : AS_PATH / AS4_PATH 바이트 -> 세그먼트들 (as4 = AS 가 4 바이트)
func parseSegs(v []byte, as4 bool) ([]asSeg, error) {
	w := 2
	if as4 {
		w = 4
	}
	var segs []asSeg
	for len(v) >= 2 {
		seg, cnt := v[0], int(v[1])
		if 2+cnt*w > len(v) {
			return nil, fmt.Errorf("as_path segment %d x %d", cnt, w)
		}
		s := asSeg{typ: seg, asns: make([]uint32, cnt)}
		for i := 0; i < cnt; i++ {
			p := v[2+i*w:]
			if as4 {
				s.asns[i] = be32(p)
			} else {
				s.asns[i] = uint32(be16(p))
			}
		}
		segs = append(segs, s)
		v = v[2+cnt*w:]
	}
	return segs, nil
}

// parseASPath : AS_PATH 속성 -> attrs (세그먼트, 글자, 개수, origin)
func parseASPath(v []byte, as4 bool, a *attrs) error {
	segs, err := parseSegs(v, as4)
	if err != nil {
		return err
	}
	a.setPath(segs)
	return nil
}

// segLen : 경로 길이에 셈하는 수 (RFC 4271 9.1.2.2, RFC 5065)
func segLen(s asSeg) int {
	switch s.typ {
	case 1:
		return 1 // AS_SET : 묶음 하나 = 1
	case 2:
		return len(s.asns)
	}
	return 0 // CONFED
}

// setPath : 세그먼트로 글자 / 개수 / origin 을 계산
func (a *attrs) setPath(segs []asSeg) {
	a.segs = segs
	var sb strings.Builder
	a.asCount, a.originAS, a.originSet = 0, 0, ""
	for _, s := range segs {
		a.asCount += segLen(s)
		switch s.typ {
		case 2:
			for _, n := range s.asns {
				sb.WriteString(" " + u32(n))
			}
			if len(s.asns) > 0 {
				a.originAS, a.originSet = s.asns[len(s.asns)-1], ""
			}
		case 1:
			set := setText(s.asns, "{", "}")
			sb.WriteString(" " + set)
			a.originAS, a.originSet = 0, set
		case 3:
			sb.WriteString(" (")
			for i, n := range s.asns {
				if i > 0 {
					sb.WriteByte(' ')
				}
				sb.WriteString(u32(n))
			}
			sb.WriteString(")")
		case 4:
			sb.WriteString(" " + setText(s.asns, "[", "]"))
		}
	}
	a.asPath = sb.String()
}

// setText : "{38266,45271}"
func setText(asns []uint32, open, close string) string {
	parts := make([]string, len(asns))
	for i, n := range asns {
		parts[i] = u32(n)
	}
	return open + strings.Join(parts, ",") + close
}

// mergeAS4Path : 2 바이트 AS peer 의 AS_PATH + AS4_PATH (RFC 6793 4.2.3)
// AS_PATH 의 앞에서 (AS_PATH 길이 - AS4_PATH 길이) 만큼 남기고 AS4_PATH 를 이어 붙임. AS4_PATH 가 더 길면 무시
func mergeAS4Path(v []byte, a *attrs) {
	segs4, err := parseSegs(v, true)
	if err != nil {
		return
	}
	n4 := 0
	for _, s := range segs4 {
		n4 += segLen(s)
	}
	keep := a.asCount - n4
	if keep < 0 {
		return
	}
	var out []asSeg
	for _, s := range a.segs {
		switch {
		case s.typ == 3 || s.typ == 4: // CONFED 는 세지 않고 그대로
			out = append(out, s)
		case keep == 0:
		case s.typ == 1:
			out = append(out, s)
			keep--
		default:
			k := min(keep, len(s.asns))
			out = append(out, asSeg{typ: 2, asns: s.asns[:k]})
			keep -= k
		}
	}
	a.setPath(append(out, segs4...))
}

// aggregatorString : "AS IP" (AS 2 또는 4 바이트)
func aggregatorString(v []byte) string {
	switch len(v) {
	case 6:
		return fmt.Sprintf("%d %s", be16(v), netip.AddrFrom4([4]byte(v[2:6])))
	case 8:
		return fmt.Sprintf("%d %s", be32(v), netip.AddrFrom4([4]byte(v[4:8])))
	}
	return ""
}

// communityString : 4 바이트마다 "높은 2:낮은 2", 공백 구분
func communityString(v []byte) string {
	var s []string
	for i := 0; i+4 <= len(v); i += 4 {
		s = append(s, fmt.Sprintf("%d:%d", be16(v[i:]), be16(v[i+2:])))
	}
	return strings.Join(s, " ")
}

// largeCommString : 12 바이트마다 "a:b:c"
func largeCommString(v []byte) string {
	var s []string
	for i := 0; i+12 <= len(v); i += 12 {
		s = append(s, fmt.Sprintf("%d:%d:%d", be32(v[i:]), be32(v[i+4:]), be32(v[i+8:])))
	}
	return strings.Join(s, " ")
}

// parseMPReach : afi 2 + safi 1 + next hop 길이 1 + next hop + 예약 1 + NLRI
func parseMPReach(v []byte, o pathOpts, u *update) error {
	if len(v) < 5 {
		return errShort
	}
	afi, safi, nl := be16(v), v[2], int(v[3])
	if 4+nl+1 > len(v) {
		return fmt.Errorf("mp_reach next hop length %d", nl)
	}
	if safi != safiUnicast || (afi != afiIPv4 && afi != afiIPv6) {
		u.others++
		return nil
	}
	nh := v[4 : 4+nl]
	switch {
	case nl >= 16: // IPv6 global (+ link-local 16) : 앞 16 바이트
		u.attrs.mpNextHop = netip.AddrFrom16([16]byte(nh[:16])).String()
	case nl >= 4:
		u.attrs.mpNextHop = netip.AddrFrom4([4]byte(nh[:4])).String()
	}
	nlri := v[4+nl+1:]
	var err error
	if afi == afiIPv6 {
		var p []prefix
		p, err = parseNLRI(nlri, afiIPv6, o.addPathV6)
		u.v6Add = append(u.v6Add, p...)
	} else {
		var p []prefix
		p, err = parseNLRI(nlri, afiIPv4, o.addPathV4)
		u.v4Add = append(u.v4Add, p...)
	}
	return err
}

// parseMPUnreach : afi 2 + safi 1 + 철회 NLRI
func parseMPUnreach(v []byte, o pathOpts, u *update) error {
	if len(v) < 3 {
		return errShort
	}
	afi, safi := be16(v), v[2]
	if safi != safiUnicast || (afi != afiIPv4 && afi != afiIPv6) {
		u.others++
		return nil
	}
	if len(v) == 3 { // 철회할 것이 없는 MP_UNREACH = 그 종류의 End-of-RIB
		u.eorV4, u.eorV6 = u.eorV4 || afi == afiIPv4, u.eorV6 || afi == afiIPv6
		return nil
	}
	var err error
	if afi == afiIPv6 {
		var p []prefix
		p, err = parseNLRI(v[3:], afiIPv6, o.addPathV6)
		u.v6Del = append(u.v6Del, p...)
	} else {
		var p []prefix
		p, err = parseNLRI(v[3:], afiIPv4, o.addPathV4)
		u.v4Del = append(u.v4Del, p...)
	}
	return err
}
