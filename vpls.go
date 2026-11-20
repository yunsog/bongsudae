// SPDX-License-Identifier: Apache-2.0

// vpls.go : L2VPN VPLS (AFI 25 / SAFI 65) = 토픽 <prefix>.l2vpn_vpls (0.4.0, 봉수대 설계, docs/kafka.md 3-5).
//
// NLRI = 길이 2 바이트 + 값. 값 17 바이트 = RFC 4761 3.2.2 (RD 8, VE ID 2, VE block offset 2, VE block size 2, label base 3),
// 12 바이트 = RFC 6074 3.2.2 BGP 자동 찾기 (RD 8, PE 주소 4). 그 밖 길이는 other_family 로
package main

import (
	"fmt"
	"strconv"
)

const kindVPLS = "l2vpn_vpls"

var vplsSchema = ourSchema(kindVPLS, "rd", "ve_id#", "ve_block_offset#", "ve_block_size#", "label_base#", "pe_address")

func init() {
	regX(famOf(25, 65), parseVPLS, false)
}

func parseVPLS(f family, b []byte, addPath bool) ([]xRoute, int, error) {
	var out []xRoute
	raw := 0
	for len(b) > 0 {
		id, rest, err := cutPathID(b, addPath)
		if err != nil || len(rest) < 2 || 2+int(be16(rest)) > len(rest) {
			return out, raw, fmt.Errorf("vpls nlri length")
		}
		n := int(be16(rest))
		v := rest[2 : 2+n]
		b = rest[2+n:]
		switch n {
		case 17:
			r := vplsSchema.newRoute(id, v)
			r.set("rd", rdString([8]byte(v[:8])))
			r.set("ve_id", strconv.Itoa(int(be16(v[8:]))))
			r.set("ve_block_offset", strconv.Itoa(int(be16(v[10:]))))
			r.set("ve_block_size", strconv.Itoa(int(be16(v[12:]))))
			r.set("label_base", strconv.Itoa(int(uint32(v[14])<<12|uint32(v[15])<<4|uint32(v[16])>>4)))
			out = append(out, r)
		case 12:
			r := vplsSchema.newRoute(id, v)
			r.set("rd", rdString([8]byte(v[:8])))
			r.set("pe_address", ipText(v[8:12]))
			out = append(out, r)
		default:
			out = append(out, otherRoute(f, id, rest[:2+n], "len="+strconv.Itoa(n)))
			raw++
		}
	}
	return out, raw, nil
}
