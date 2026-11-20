// SPDX-License-Identifier: Apache-2.0

// mvpn.go : MCAST-VPN (AFI 1, 2 / SAFI 5, RFC 6514) = 토픽 <prefix>.mvpn (0.4.0, 봉수대 설계, docs/kafka.md 3-6).
//
// NLRI = 경로 종류 1 + 길이 1 + 값. 주소 길이 칸은 비트 (32 / 128, 0 = 와일드카드 "*" RFC 6625)
//
//	1 Intra-AS I-PMSI A-D   RD, originating router IP
//	2 Inter-AS I-PMSI A-D   RD, source AS
//	3 S-PMSI A-D            RD, source, group, originating router IP
//	4 Leaf A-D              route key (안의 MVPN 경로를 글자로), originating router IP
//	5 Source Active A-D     RD, source, group
//	6 / 7 C-multicast       RD, source AS, source (6 = C-RP), group
//
// 모르는 종류 (8 ~ ) 는 other_family 로
package main

import (
	"fmt"
	"strconv"
	"strings"
)

const kindMVPN = "mvpn"

var mvpnSchema = ourSchema(kindMVPN, "is_ipv4?", "route_type#", "rd", "originating_router_ip", "source_as#", "source", "group", "route_key")

func init() {
	regX(famOf(afiIPv4, 5), parseMVPN, false)
	regX(famOf(afiIPv6, 5), parseMVPN, false)
}

func parseMVPN(f family, b []byte, addPath bool) ([]xRoute, int, error) {
	var out []xRoute
	raw := 0
	for len(b) > 0 {
		id, rest, err := cutPathID(b, addPath)
		if err != nil || len(rest) < 2 || 2+int(rest[1]) > len(rest) {
			return out, raw, fmt.Errorf("mvpn nlri length")
		}
		one := rest[:2+int(rest[1])]
		b = rest[2+int(rest[1]):]
		if one[0] < 1 || one[0] > 7 {
			out = append(out, otherRoute(f, id, one, strconv.Itoa(int(one[0]))))
			raw++
			continue
		}
		r := mvpnSchema.newRoute(id, one)
		r.set("is_ipv4", b01(f.afi() == afiIPv4))
		r.set("route_type", strconv.Itoa(int(one[0])))
		if err := mvpnFields(one[0], one[2:], r.set); err != nil {
			return out, raw, fmt.Errorf("mvpn route type %d : %w", one[0], err)
		}
		out = append(out, r)
	}
	return out, raw, nil
}

// mvpnFields : 종류마다 값 -> 칸 (set). 종류 4 의 route key 는 안의 경로를 "이름=값" 글자로
func mvpnFields(typ byte, v []byte, set func(name, v string)) error {
	d := &dec{b: v}
	addr := func(bits int) string { // 길이가 비트로 주어진 주소
		switch bits {
		case 0:
			return "*"
		case 32, 128:
			return ipText(d.take(bits / 8))
		}
		d.fail()
		return ""
	}
	rd := func() { set("rd", rdString([8]byte(d.take(8)))) }
	switch typ {
	case 1:
		if len(v) < 12 {
			return errShort
		}
		rd()
		set("originating_router_ip", ipText(d.b))
	case 2:
		if len(v) != 12 {
			return errShort
		}
		rd()
		set("source_as", strconv.FormatUint(uint64(be32(d.take(4))), 10))
	case 3, 5:
		if len(v) < 10 {
			return errShort
		}
		rd()
		set("source", addr(int(d.u8())))
		set("group", addr(int(d.u8())))
		if typ == 3 && d.err == nil {
			set("originating_router_ip", ipText(d.b))
		}
	case 4: // route key = 종류 1 + 길이 1 + 값, 뒤 = originating router IP
		if len(v) < 2 || 2+int(v[1]) > len(v) {
			return errShort
		}
		kt, kv := v[0], v[2:2+int(v[1])]
		set("originating_router_ip", ipText(v[2+int(v[1]):]))
		var parts []string
		if kt >= 1 && kt <= 7 && kt != 4 {
			parts = append(parts, "type="+strconv.Itoa(int(kt)))
			err := mvpnFields(kt, kv, func(name, val string) { parts = append(parts, name+"="+val) })
			if err == nil {
				set("route_key", strings.Join(parts, " "))
				return nil
			}
		}
		set("route_key", "type="+strconv.Itoa(int(kt))+" "+fmt.Sprintf("%x", kv)) // 다른 경로 종류 (예 : FlowSpec 쪽 키)
		return nil
	case 6, 7:
		if len(v) < 14 {
			return errShort
		}
		rd()
		set("source_as", strconv.FormatUint(uint64(be32(d.take(4))), 10))
		set("source", addr(int(d.u8())))
		set("group", addr(int(d.u8())))
	}
	if d.err != nil {
		return d.err
	}
	return nil
}
