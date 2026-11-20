// SPDX-License-Identifier: Apache-2.0

// rtc.go : Route Target Constraint (AFI 1 / SAFI 132, RFC 4684) = 토픽 <prefix>.rt_constraint (0.4.0, 봉수대 설계, docs/kafka.md 3-8).
//
// NLRI = 길이 (비트, 0 또는 32 ~ 96) + origin AS 4 + route target (앞 길이-32 비트). 길이 0 = 모든 RT (기본 경로)
package main

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

const kindRTC = "rt_constraint"

var rtcSchema = ourSchema(kindRTC, "prefix_len#", "rt_origin_as#", "route_target")

func init() {
	regX(famOf(afiIPv4, 132), parseRTC, false)
}

func parseRTC(f family, b []byte, addPath bool) ([]xRoute, int, error) {
	var out []xRoute
	for len(b) > 0 {
		id, rest, err := cutPathID(b, addPath)
		if err != nil || len(rest) < 1 {
			return out, 0, errShort
		}
		bits := int(rest[0])
		n := (bits + 7) / 8
		if bits > 96 || (bits > 0 && bits < 32) || 1+n > len(rest) {
			return out, 0, fmt.Errorf("rt constraint length %d", bits)
		}
		v := rest[1 : 1+n]
		b = rest[1+n:]
		r := rtcSchema.newRoute(id, rest[:1+n])
		r.set("prefix_len", strconv.Itoa(bits))
		if bits >= 32 {
			r.set("rt_origin_as", strconv.FormatUint(uint64(be32(v)), 10))
			r.set("route_target", rtText(v[4:], bits-32))
		}
		out = append(out, r)
	}
	return out, 0, nil
}

// rtText : RT 8 바이트면 ext community 글자 (extcomm.go, "rt=64496:1"), 모자라면 "hex/비트"
func rtText(v []byte, bits int) string {
	if bits == 64 && len(v) == 8 {
		return strings.TrimSpace(extCommString(v))
	}
	if bits == 0 {
		return ""
	}
	return hex.EncodeToString(v) + "/" + strconv.Itoa(bits)
}
