// SPDX-License-Identifier: Apache-2.0

// otherfam.go : 다른 토픽이 없는 경로 종류 = 토픽 <prefix>.other_family (0.4.0, 봉수대 설계, docs/kafka.md 3-9).
//
// 경로 하나 = 줄 하나. NLRI 바이트는 늘 nlri 칸에 hex 로, 풀 수 있는 것만 rd / prefix / labels / detail 을 채움.
//
//	종류 (AFI/SAFI)                         풀기
//	Encapsulation (1, 2/7, RFC 5512)       prefix = 끝점 주소
//	Tunnel (1, 2/64)                       detail = tunnel id, prefix
//	MDT (1/66, RFC 6037)                   rd, prefix = 원 주소 /32, detail = group
//	Classful Transport (1, 2/76, RFC 9832) labels, rd, prefix (VPN 과 같은 모양)
//	VPN multicast (1, 2/129, RFC 6513)     labels, rd, prefix (VPN 과 같은 모양)
//	CAR / VPN-CAR (1, 2/83, 84)            nlri_type, (종류 1) rd, prefix, detail = color
//	MUP (1, 2/85, RFC 9433 계열)           nlri_type = 구조 / 경로 종류, rd
//	그 밖 / 모르는 종류                    nlri = MP_REACH / UNREACH 의 NLRI 통째로 (경로를 나눌 수 없음, other_families 로 셈)
//
// 다른 해석기 (BGP-LS, MVPN) 가 모르는 경로 종류 (route type) 도 이 토픽에 nlri_type 과 hex 로 냄
package main

import (
	"encoding/hex"
	"fmt"
	"net/netip"
	"strconv"
)

const kindOther = "other_family"

var otherSchema = ourSchema(kindOther, "afi#", "safi#", "family", "nlri_type", "rd", "prefix", "labels", "detail", "nlri")

func init() {
	for _, afi := range []uint16{afiIPv4, afiIPv6} {
		regX(famOf(afi, 7), parsePrefixLike(0), false)
		regX(famOf(afi, 64), parsePrefixLike(2), false)
		regX(famOf(afi, 76), parseLabeledVPN, true)
		regX(famOf(afi, 129), parseLabeledVPN, true)
		regX(famOf(afi, 83), parseCAR(false), false)
		regX(famOf(afi, 84), parseCAR(true), true)
		regX(famOf(afi, 85), parseMUP, false)
	}
	regX(famOf(afiIPv4, 66), parseMDT, false)
	for f, n := range map[family]string{
		famOf(afiIPv4, 7): "ipv4-encap", famOf(afiIPv6, 7): "ipv6-encap",
		famOf(afiIPv4, 64): "ipv4-tunnel", famOf(afiIPv6, 64): "ipv6-tunnel",
		famOf(afiIPv4, 66): "ipv4-mdt",
		famOf(afiIPv4, 76): "ipv4-classful-transport", famOf(afiIPv6, 76): "ipv6-classful-transport",
		famOf(afiIPv4, 129): "ipv4-vpn-multicast", famOf(afiIPv6, 129): "ipv6-vpn-multicast",
		famOf(afiIPv4, 83): "ipv4-car", famOf(afiIPv6, 83): "ipv6-car",
		famOf(afiIPv4, 84): "ipv4-vpn-car", famOf(afiIPv6, 84): "ipv6-vpn-car",
		famOf(afiIPv4, 85): "ipv4-mup", famOf(afiIPv6, 85): "ipv6-mup",
		famOf(16388, 80): "bgp-ls-spf",
	} {
		familyNames[f] = n
	}
}

// otherRoute : other_family 줄 하나 (afi / safi / family / nlri 칸을 채움)
func otherRoute(f family, pathID uint32, nlri []byte, typ string) xRoute {
	r := otherSchema.newRoute(pathID, nlri)
	r.set("afi", strconv.Itoa(int(f.afi())))
	r.set("safi", strconv.Itoa(int(f.safi())))
	r.set("family", f.String())
	r.set("nlri_type", typ)
	r.set("nlri", hex.EncodeToString(nlri))
	return r
}

// rawParser : 등록표에 없는 종류 : NLRI 통째로 줄 하나 (bgp_update.go). 빈 철회 = End-of-RIB
func rawParser(f family) nlriParser {
	return xParser(f, func(f family, nlri []byte, _ bool) ([]xRoute, int, error) {
		return []xRoute{otherRoute(f, 0, nlri, "")}, 1, nil
	}, false)
}

// cutPathID : Add-Path 면 앞 4 바이트 path id
func cutPathID(b []byte, addPath bool) (uint32, []byte, error) {
	if !addPath {
		return 0, b, nil
	}
	if len(b) < 4 {
		return 0, nil, errShort
	}
	return be32(b), b[4:], nil
}

// addrPrefix : 주소 바이트 (모자라면 0 으로 채움) + 길이 -> 프리픽스 글자
func addrPrefix(b []byte, bits int, v6 bool) (string, error) {
	maxBits := 32
	if v6 {
		maxBits = 128
	}
	if bits > maxBits || len(b) < (bits+7)/8 {
		return "", fmt.Errorf("prefix length %d", bits)
	}
	var a [16]byte
	copy(a[:], b[:(bits+7)/8])
	if x := bits % 8; x != 0 {
		a[(bits+7)/8-1] &= 0xff << (8 - x)
	}
	if v6 {
		return netip.PrefixFrom(netip.AddrFrom16(a), bits).String(), nil
	}
	return netip.PrefixFrom(netip.AddrFrom4([4]byte(a[:4])), bits).String(), nil
}

// ipText : 4 / 16 바이트 주소 글자 (그 밖 길이는 hex)
func ipText(b []byte) string {
	switch len(b) {
	case 4:
		return netip.AddrFrom4([4]byte(b)).String()
	case 16:
		return netip.AddrFrom16([16]byte(b)).String()
	}
	return hex.EncodeToString(b)
}

// parsePrefixLike : [path id] + 길이 (비트) + [앞 head 바이트 = tunnel id] + 프리픽스
func parsePrefixLike(head int) xParseFn {
	return func(f family, b []byte, addPath bool) ([]xRoute, int, error) {
		var out []xRoute
		for len(b) > 0 {
			id, rest, err := cutPathID(b, addPath)
			if err != nil || len(rest) < 1 {
				return out, 0, errShort
			}
			bits := int(rest[0])
			n := (bits + 7) / 8
			if 1+n > len(rest) || bits < head*8 {
				return out, 0, fmt.Errorf("nlri length %d bits", bits)
			}
			v := rest[1 : 1+n]
			b = rest[1+n:]
			r := otherRoute(f, id, rest[:1+n], "")
			if head == 2 {
				r.set("detail", "tunnel_id="+strconv.Itoa(int(be16(v))))
			}
			p, err := addrPrefix(v[head:], bits-head*8, f.afi() == afiIPv6)
			if err != nil {
				return out, 0, err
			}
			r.set("prefix", p)
			out = append(out, r)
		}
		return out, 0, nil
	}
}

// parseLabeledVPN : 라벨 스택 + RD + 프리픽스 (L3VPN 과 같은 모양, famroute.go 의 parseFamNLRI)
func parseLabeledVPN(f family, b []byte, addPath bool) ([]xRoute, int, error) {
	sp := famSpec{afi: int(f.afi()), labeled: true, vpn: true}
	var out []xRoute
	for len(b) > 0 { // 경로 하나씩 잘라 (hex 칸) parseFamNLRI 에 넘김
		id, rest, err := cutPathID(b, addPath)
		if err != nil || len(rest) < 1 {
			return out, 0, errShort
		}
		n := (int(rest[0]) + 7) / 8
		if 1+n > len(rest) {
			return out, 0, fmt.Errorf("nlri length")
		}
		one := rest[:1+n]
		b = rest[1+n:]
		rs, err := parseFamNLRI(one, sp, false, false)
		if err != nil || len(rs) != 1 {
			return out, 0, fmt.Errorf("labeled vpn nlri : %v", err)
		}
		r := otherRoute(f, id, one, "")
		r.set("rd", rdString(rs[0].rd))
		r.set("prefix", rs[0].pfx.addr.String()+"/"+strconv.Itoa(rs[0].pfx.length))
		r.set("labels", string(labelText(nil, rs[0].labels)))
		out = append(out, r)
	}
	return out, 0, nil
}

// parseMDT : 길이 (비트, 128) + RD 8 + 원 주소 4 + group 4 (RFC 6037 3)
func parseMDT(f family, b []byte, addPath bool) ([]xRoute, int, error) {
	var out []xRoute
	for len(b) > 0 {
		id, rest, err := cutPathID(b, addPath)
		if err != nil || len(rest) < 17 || rest[0] != 128 {
			return out, 0, fmt.Errorf("mdt nlri")
		}
		v := rest[1:17]
		b = rest[17:]
		r := otherRoute(f, id, rest[:17], "")
		r.set("rd", rdString([8]byte(v[:8])))
		r.set("prefix", ipText(v[8:12])+"/32")
		r.set("detail", "group="+ipText(v[12:16]))
		out = append(out, r)
	}
	return out, 0, nil
}

// parseCAR : 길이 1 + 열쇠 길이 1 + NLRI 종류 1 + 열쇠 + TLV (draft-ietf-idr-bgp-car).
// 종류 1 (Color-Aware Route) 열쇠 = [RD 8 (VPN-CAR)] + 프리픽스 길이 1 + 프리픽스 + color 4
func parseCAR(vpn bool) xParseFn {
	return func(f family, b []byte, addPath bool) ([]xRoute, int, error) {
		var out []xRoute
		for len(b) > 0 {
			id, rest, err := cutPathID(b, addPath)
			if err != nil || len(rest) < 3 || rest[0] < 2 || 1+int(rest[0]) > len(rest) {
				return out, 0, fmt.Errorf("car nlri length")
			}
			one := rest[:1+int(rest[0])]
			b = rest[1+int(rest[0]):]
			typ := one[2]
			r := otherRoute(f, id, one, strconv.Itoa(int(typ)))
			key := one[3:]
			if kl := int(one[1]); typ == 1 && kl <= len(key) {
				key = key[:kl]
				if vpn && len(key) >= 8 {
					r.set("rd", rdString([8]byte(key[:8])))
					key = key[8:]
				}
				if len(key) >= 1 {
					bits := int(key[0])
					n := (bits + 7) / 8
					if 1+n+4 == len(key) {
						if p, err := addrPrefix(key[1:1+n], bits, f.afi() == afiIPv6); err == nil {
							r.set("prefix", p)
							r.set("detail", "color="+strconv.FormatUint(uint64(be32(key[1+n:])), 10))
						}
					}
				}
			}
			out = append(out, r)
		}
		return out, 0, nil
	}
}

// parseMUP : 구조 종류 1 + 경로 종류 2 + 길이 1 + 값 (값은 RD 8 로 시작). nlri_type = "구조/경로"
func parseMUP(f family, b []byte, addPath bool) ([]xRoute, int, error) {
	var out []xRoute
	for len(b) > 0 {
		id, rest, err := cutPathID(b, addPath)
		if err != nil || len(rest) < 4 || 4+int(rest[3]) > len(rest) {
			return out, 0, fmt.Errorf("mup nlri length")
		}
		one := rest[:4+int(rest[3])]
		b = rest[4+int(rest[3]):]
		r := otherRoute(f, id, one, strconv.Itoa(int(one[0]))+"/"+strconv.Itoa(int(be16(one[1:3]))))
		if len(one) >= 12 {
			r.set("rd", rdString([8]byte(one[4:12])))
		}
		out = append(out, r)
	}
	return out, 0, nil
}
