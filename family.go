// family.go : 경로 종류 (address family = AFI / SAFI, 0.3.0)
//
// 경로 종류를 더하는 자리 : NLRI 해석기 등록표 (nlriParsers). 등록된 종류만 해석하고, 나머지 MP_REACH / MP_UNREACH 는
// 종류마다 세고 버림 (1 분 통계 other_families, /api/stats, /metrics). 지금 등록 = IPv4 / IPv6 unicast.
// 새 종류 = 해석기 + 경로표 열쇠 (ribtable.go) + 출력기 (토픽) 를 더함 (docs/todo.md 2 장)
package main

import (
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"strings"
)

// family : AFI << 8 | SAFI (OPEN 능력 표의 열쇠와 같은 꼴)
type family uint32

func famOf(afi uint16, safi uint8) family { return family(uint32(afi)<<8 | uint32(safi)) }

func (f family) afi() uint16 { return uint16(f >> 8) }
func (f family) safi() uint8 { return uint8(f) }

// 이름 (설정 / API / 통계 글자). 표에 없으면 "afi/safi"
var familyNames = map[family]string{
	famOf(afiIPv4, 1): "ipv4-unicast", famOf(afiIPv6, 1): "ipv6-unicast",
	famOf(afiIPv4, 2): "ipv4-multicast", famOf(afiIPv6, 2): "ipv6-multicast",
	famOf(afiIPv4, 4): "ipv4-labeled", famOf(afiIPv6, 4): "ipv6-labeled",
	famOf(afiIPv4, 5): "ipv4-mvpn", famOf(afiIPv6, 5): "ipv6-mvpn",
	famOf(afiIPv4, 73): "ipv4-sr-policy", famOf(afiIPv6, 73): "ipv6-sr-policy",
	famOf(afiIPv4, 128): "vpnv4", famOf(afiIPv6, 128): "vpnv6",
	famOf(afiIPv4, 132): "rt-constraint",
	famOf(afiIPv4, 133): "ipv4-flowspec", famOf(afiIPv6, 133): "ipv6-flowspec",
	famOf(afiIPv4, 134): "vpnv4-flowspec", famOf(afiIPv6, 134): "vpnv6-flowspec",
	famOf(25, 65): "l2vpn-vpls", famOf(25, 70): "l2vpn-evpn",
	famOf(16388, 71): "bgp-ls", famOf(16388, 72): "bgp-ls-vpn",
}

func (f family) String() string {
	if n, ok := familyNames[f]; ok {
		return n
	}
	return strconv.Itoa(int(f.afi())) + "/" + strconv.Itoa(int(f.safi()))
}

// parseFamily : 이름 또는 "afi/safi" -> family
func parseFamily(s string) (family, error) {
	s = strings.TrimSpace(s)
	for f, n := range familyNames {
		if n == s {
			return f, nil
		}
	}
	if a, b, ok := strings.Cut(s, "/"); ok {
		afi, err1 := strconv.ParseUint(a, 10, 16)
		safi, err2 := strconv.ParseUint(b, 10, 8)
		if err1 == nil && err2 == nil {
			return famOf(uint16(afi), uint8(safi)), nil
		}
	}
	return 0, fmt.Errorf("unknown address family %q (e.g. ipv4-unicast, vpnv4 or 1/128)", s)
}

// familyList : family 들을 이름 순서 (afi, safi) 로 글자 목록
func familyList(fs []family) []string {
	fs = slices.Clone(fs)
	slices.Sort(fs)
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.String()
	}
	return out
}

// ---- NLRI 해석기 등록표 ----

// nlriParser : 경로 종류 하나의 MP_REACH / MP_UNREACH 해석.
// reach : next hop 바이트, NLRI 바이트 -> u 에 더함. unreach : 철회 NLRI (빈 것 = 그 종류의 End-of-RIB)
type nlriParser struct {
	reach   func(u *update, nh, nlri []byte, o pathOpts) error
	unreach func(u *update, nlri []byte, o pathOpts) error
}

var nlriParsers = map[family]nlriParser{
	famOf(afiIPv4, safiUnicast): {reachUnicast(afiIPv4), unreachUnicast(afiIPv4)},
	famOf(afiIPv6, safiUnicast): {reachUnicast(afiIPv6), unreachUnicast(afiIPv6)},
}

// reachUnicast : IPv4 / IPv6 unicast MP_REACH (IPv4 도 MP 로 오는 라우터가 있음)
func reachUnicast(afi int) func(u *update, nh, nlri []byte, o pathOpts) error {
	return func(u *update, nh, nlri []byte, o pathOpts) error {
		u.attrs.mpNextHop = nextHopText(nh)
		if afi == afiIPv6 {
			p, err := parseNLRI(nlri, afiIPv6, o.addPathV6)
			u.v6Add = append(u.v6Add, p...)
			return err
		}
		p, err := parseNLRI(nlri, afiIPv4, o.addPathV4)
		u.v4Add = append(u.v4Add, p...)
		return err
	}
}

// unreachUnicast : IPv4 / IPv6 unicast MP_UNREACH. 빈 것 = End-of-RIB
func unreachUnicast(afi int) func(u *update, nlri []byte, o pathOpts) error {
	return func(u *update, nlri []byte, o pathOpts) error {
		if len(nlri) == 0 {
			if afi == afiIPv6 {
				u.eorV6 = true
			} else {
				u.eorV4 = true
			}
			return nil
		}
		if afi == afiIPv6 {
			p, err := parseNLRI(nlri, afiIPv6, o.addPathV6)
			u.v6Del = append(u.v6Del, p...)
			return err
		}
		p, err := parseNLRI(nlri, afiIPv4, o.addPathV4)
		u.v4Del = append(u.v4Del, p...)
		return err
	}
}

// nextHopText : MP_REACH next hop 바이트 -> 글자. IPv6 global (+ link-local 16) 이면 앞 16 바이트
func nextHopText(nh []byte) string {
	switch {
	case len(nh) >= 16:
		return netip.AddrFrom16([16]byte(nh[:16])).String()
	case len(nh) >= 4:
		return netip.AddrFrom4([4]byte(nh[:4])).String()
	}
	return ""
}

// ---- 경로표를 들 종류 (설정 bmp.rib / 옵션 -rib, 0.3.0) ----

// defaultRIB : 기본 = 지금 운영과 같음 (IPv4 / IPv6 unicast)
const defaultRIB = "ipv4-unicast,ipv6-unicast"

// parseRIBFamilies : "ipv4-unicast,vpnv4" -> 종류 모음. 0.2 의 true = unicast 둘, false / none = 없음 (넘기기만),
// all = 해석기가 있는 종류 모두. 해석기가 아직 없는 종류도 받음 (그 종류를 만드는 판부터 쓰임)
func parseRIBFamilies(s string) (map[family]bool, error) {
	out := map[family]bool{}
	switch strings.TrimSpace(s) {
	case "true", "":
		s = defaultRIB
	case "false", "none":
		return out, nil
	case "all":
		for f := range ribSupported {
			out[f] = true
		}
		return out, nil
	}
	for _, x := range strings.Split(s, ",") {
		f, err := parseFamily(x)
		if err != nil {
			return nil, fmt.Errorf("-rib : %w (or true, false, none, all)", err)
		}
		out[f] = true
	}
	return out, nil
}

// ribSupported : 경로표를 들 수 있는 종류 (0.3.0 : unicast + famrib.go 의 labeled unicast / L3VPN / multicast)
var ribSupported = map[family]bool{famOf(afiIPv4, safiUnicast): true, famOf(afiIPv6, safiUnicast): true}

func init() {
	for f := range famSpecs {
		ribSupported[f] = true
	}
}

// ribOn : 경로표를 드는 종류 : unicast 둘 (빠른 길) + 그 밖 종류 (famrib.go)
type ribOn struct {
	v4, v6 bool
	fam    map[family]bool
}
