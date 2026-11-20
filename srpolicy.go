// SPDX-License-Identifier: Apache-2.0

// srpolicy.go : SR Policy (AFI 1, 2 / SAFI 73, RFC 9830) = 토픽 <prefix>.sr_policy (0.4.0, 봉수대 설계, docs/kafka.md 3-7).
//
// NLRI = 길이 (비트 : 96 IPv4, 192 IPv6) + distinguisher 4 + color 4 + endpoint 4 / 16.
// 후보 경로의 내용은 Tunnel Encapsulation 속성 (종류 23, RFC 9012) 의 tunnel 종류 15 (SR Policy) 에 :
//
//	sub-TLV 12 preference, 13 binding SID (4 = 라벨, 16 = SRv6), 20 SRv6 binding SID, 128 segment list,
//	129 candidate path name, 130 policy name (RFC 9830 2.4)
//
// segment_lists 칸 = 목록마다 "w=<weight> <segment> <segment> ..." 를 "; " 로. segment = 라벨 값, SRv6 SID (주소),
// 그 밖 종류는 "type<N>"
package main

import (
	"fmt"
	"strconv"
	"strings"
)

const kindSRPolicy = "sr_policy"

var srSchema = ourSchema(kindSRPolicy, "is_ipv4?", "distinguisher#", "color#", "endpoint",
	"preference#", "binding_sid", "policy_name", "candidate_path_name", "segment_lists")

func init() {
	srSchema.attrFn = srAttrCols
	for _, afi := range []uint16{afiIPv4, afiIPv6} {
		regX(famOf(afi, 73), parseSRPolicy, false)
	}
}

func parseSRPolicy(f family, b []byte, addPath bool) ([]xRoute, int, error) {
	var out []xRoute
	for len(b) > 0 {
		id, rest, err := cutPathID(b, addPath)
		if err != nil || len(rest) < 1 {
			return out, 0, errShort
		}
		n := (int(rest[0]) + 7) / 8
		if (n != 12 && n != 24) || 1+n > len(rest) {
			return out, 0, fmt.Errorf("sr policy nlri length %d bits", rest[0])
		}
		v := rest[1 : 1+n]
		b = rest[1+n:]
		r := srSchema.newRoute(id, rest[:1+n])
		r.set("is_ipv4", b01(n == 12))
		r.set("distinguisher", strconv.FormatUint(uint64(be32(v)), 10))
		r.set("color", strconv.FormatUint(uint64(be32(v[4:])), 10))
		r.set("endpoint", ipText(v[8:]))
		out = append(out, r)
	}
	return out, 0, nil
}

// tlvs : Tunnel Encap 의 TLV (종류 2 + 길이 2) 또는 sub-TLV (종류 1 + 길이 1, 종류 128 이상은 길이 2) 를 차례로
func encapTLVs(b []byte, sub bool, fn func(typ int, v []byte)) {
	for len(b) > 0 {
		var typ, n, h int
		switch {
		case !sub && len(b) >= 4:
			typ, n, h = int(be16(b)), int(be16(b[2:])), 4
		case sub && len(b) >= 2 && b[0] < 128:
			typ, n, h = int(b[0]), int(b[1]), 2
		case sub && len(b) >= 3:
			typ, n, h = int(b[0]), int(be16(b[1:])), 3
		default:
			return
		}
		if h+n > len(b) {
			return
		}
		fn(typ, b[h:h+n])
		b = b[h+n:]
	}
}

// srAttrCols : add 줄에서 Tunnel Encap 속성 -> 칸
func srAttrCols(u *update, r *xRoute) {
	encapTLVs(u.tunEncap, false, func(typ int, v []byte) {
		if typ != 15 { // tunnel 종류 15 = SR Policy
			return
		}
		var lists []string
		encapTLVs(v, true, func(st int, sv []byte) {
			switch st {
			case 12: // flags 1 + 예약 1 + preference 4
				if len(sv) == 6 {
					r.set("preference", strconv.FormatUint(uint64(be32(sv[2:])), 10))
				}
			case 13, 20: // flags 1 + 예약 1 + SID (없음 / 라벨 4 / SRv6 16)
				if len(sv) == 6 {
					r.set("binding_sid", strconv.FormatUint(uint64(be32(sv[2:])>>12), 10))
				} else if len(sv) >= 18 {
					r.set("binding_sid", ipText(sv[2:18]))
				}
			case 128:
				lists = append(lists, segList(sv))
			case 129, 130: // flags 1 + 예약 1 + 이름 (RFC 9830 2.4.8 / 2.4.9)
				name := "candidate_path_name"
				if st == 130 {
					name = "policy_name"
				}
				if len(sv) > 2 {
					r.set(name, string(sv[2:]))
				}
			}
		})
		r.set("segment_lists", strings.Join(lists, "; "))
	})
}

// segList : segment list sub-TLV = 예약 1 + sub-TLV 들 (9 weight, 1 종류 A 라벨, 13 종류 B SRv6 SID, ...)
func segList(v []byte) string {
	if len(v) < 1 {
		return ""
	}
	w := "w=0"
	var segs []string
	encapTLVs(v[1:], true, func(t int, sv []byte) {
		switch {
		case t == 9 && len(sv) == 6: // flags 1 + 예약 1 + weight 4
			w = "w=" + strconv.FormatUint(uint64(be32(sv[2:])), 10)
		case t == 1 && len(sv) == 6: // 종류 A : flags 1 + 예약 1 + 라벨 칸 4 (위 20 비트)
			segs = append(segs, strconv.FormatUint(uint64(be32(sv[2:])>>12), 10))
		case t == 13 && len(sv) >= 18: // 종류 B : flags 1 + 예약 1 + SRv6 SID 16
			segs = append(segs, ipText(sv[2:18]))
		default:
			segs = append(segs, "type"+strconv.Itoa(t))
		}
	})
	return strings.TrimSpace(w + " " + strings.Join(segs, " "))
}
