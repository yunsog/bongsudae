// bmp_stats.go : BMP Statistics Report (종류 1, RFC 7854 4.8) -> peer 상세 API 의 bmp_stats (0.2.0)
//
// 라우터가 peer 마다 주기적으로 보내는 수 (거부한 프리픽스, 중복, 경로 수 등). Kafka 로는 내지 않음 (읽는 소비자 없음).
// 모양 : per-peer 머리 + 개수 4 + TLV (종류 2, 길이 2, 값). 값 = 32 비트 수 (4), 64 비트 수 (8),
// 또는 AFI 2 + SAFI 1 + 64 비트 (11, 주소 종류마다)
package main

import (
	"fmt"
	"time"
)

// statNames : IANA "BMP Statistics Types" (RFC 7854, 8671, 9069 일부)
var statNames = map[uint16]string{
	0:  "rejected_prefixes",
	1:  "duplicate_prefix_advertisements",
	2:  "duplicate_withdraws",
	3:  "invalid_cluster_list_loop",
	4:  "invalid_as_path_loop",
	5:  "invalid_originator_id",
	6:  "invalid_as_confed_loop",
	7:  "adj_rib_in_routes",
	8:  "loc_rib_routes",
	9:  "adj_rib_in_routes",
	10: "loc_rib_routes",
	11: "updates_treat_as_withdraw",
	12: "prefixes_treat_as_withdraw",
	13: "duplicate_update_messages",
	14: "adj_rib_out_pre_policy_routes",
	15: "adj_rib_out_post_policy_routes",
	16: "adj_rib_out_pre_policy_routes",
	17: "adj_rib_out_post_policy_routes",
}

// statName : TLV 이름. 주소 종류마다인 것은 뒤에 _<afi>_<safi> (예 adj_rib_in_routes_1_1 = IPv4 unicast). 모르는 종류 = type_N
func statName(typ uint16, afi uint16, safi uint8, perAFI bool) string {
	n, ok := statNames[typ]
	if !ok {
		n = fmt.Sprintf("type_%d", typ)
	}
	if perAFI {
		n += fmt.Sprintf("_%d_%d", afi, safi)
	}
	return n
}

// statsReport : Stats Report 하나 -> 그 peer 의 bmpStats (지난 값을 통째로 바꿈)
func (c *collector) statsReport(r *router, b []byte, now time.Time) error {
	h, err := parsePeerHdr(b)
	if err != nil {
		return err
	}
	b = b[peerHdrLen:]
	if len(b) < 4 {
		return errShort
	}
	count := be32(b)
	b = b[4:]
	m := map[string]uint64{}
	for i := uint32(0); i < count && len(b) > 0; i++ {
		if len(b) < 4 {
			return errShort
		}
		typ, n := be16(b), int(be16(b[2:]))
		if 4+n > len(b) {
			return fmt.Errorf("stats tlv %d length %d", typ, n)
		}
		v := b[4 : 4+n]
		b = b[4+n:]
		switch n {
		case 4:
			m[statName(typ, 0, 0, false)] = uint64(be32(v))
		case 8:
			m[statName(typ, 0, 0, false)] = uint64(be32(v))<<32 | uint64(be32(v[4:]))
		case 11:
			m[statName(typ, be16(v), v[2], true)] = uint64(be32(v[3:]))<<32 | uint64(be32(v[7:]))
		default: // 모르는 꼴 : 길이만 넘어감
		}
	}
	// 아는 peer 에만 (Stats 때문에 새 peer 를 만들지 않음 : 0.1 은 Stats 를 무시했으므로 peer 목록이 바뀌지 않게)
	if p := r.byID[peerID{h.addr, h.rd}]; p != nil {
		p.bmpStats, p.bmpStatsAt = m, now
	}
	return nil
}
