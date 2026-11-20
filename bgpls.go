// SPDX-License-Identifier: Apache-2.0

// bgpls.go : BGP-LS (AFI 16388 / SAFI 71, VPN 72, BGP-LS-SPF 80 : RFC 9552, 9085, 9086, 9815) (0.4.0).
// 토픽 <prefix>.ls_node / ls_link / ls_prefix = openbmp 스키마 칸 그대로 (27 / 46 / 34 칸) + 끝에 rd 한 칸 (VPN 72, 그 밖은 빈칸).
//
// NLRI = 종류 2 + 길이 2 + [RD 8 (72)] + protocol 1 + identifier 8 + TLV (종류 2 + 길이 2) 들 :
//
//	1 node           256 local node descriptors
//	2 link           256, 257 remote node descriptors, 258 link local / remote id, 259 / 260 IPv4 iface / nbr, 261 / 262 IPv6, 263 MT
//	3 / 4 prefix     256, 263 MT, 264 OSPF route type, 265 IP reachability
//
// node descriptor sub-TLV : 512 AS, 513 BGP-LS id, 514 OSPF area, 515 IGP router id, 516 BGP router id (EPE).
// 그 밖 NLRI 종류 (6 SRv6 SID 등) 는 other_family 로.
// 속성 (BGP-LS 속성 종류 29) 은 줄을 만들 때 풂 (add 줄만) : node 1024 ~ 1034, link 1028 ~ 1103, prefix 1152 ~ 1158.
//
// hash : node = md5(peer hash, protocol + identifier + RD + local node descriptors).
// link 의 local_node_hash / remote_node_hash, prefix 의 local_node_hash 는 같은 계산이라 ls_node 의 hash 와 이어짐.
// link / prefix 의 hash = md5(peer hash, 종류, NLRI 바이트, path id)
package main

import (
	"encoding/hex"
	"fmt"
	"math"
	"net/netip"
	"strconv"
	"strings"
)

var (
	lsNodeSchema = newSchema("ls_node", "action", "seq", "hash", "base_attr_hash", "router_hash", "router_ip",
		"peer_hash", "peer_ip", "peer_asn", "timestamp", "igp_router_id", "router_id", "routing_id#", "ls_id",
		"mt_id", "ospf_area_id", "isis_area_id", "protocol", "flags", "as_path", "local_pref", "med", "nexthop",
		"name", "is_pre_policy", "is_adj_rib_in", "sr_capabilities", "rd")
	lsLinkSchema = newSchema("ls_link", "action", "seq", "hash", "base_attr_hash", "router_hash", "router_ip",
		"peer_hash", "peer_ip", "peer_asn", "timestamp", "igp_router_id", "router_id", "routing_id#", "ls_id",
		"ospf_area_id", "isis_area_id", "protocol", "as_path", "local_pref", "med", "nexthop", "mt_id",
		"local_link_id#", "remote_link_id#", "interface_ip", "neighbor_ip", "igp_metric#", "admin_group#",
		"max_link_bw#", "max_resv_bw#", "unreserved_bw", "te_default_metric#", "link_protection", "mpls_proto_mask",
		"srlg", "link_name", "remote_node_hash", "local_node_hash", "remote_igp_router_id", "remote_router_id",
		"local_node_asn#", "remote_node_asn#", "peer_node_sid", "is_pre_policy", "is_adj_rib_in", "adj_segment_id", "rd")
	lsPrefixSchema = newSchema("ls_prefix", "action", "seq", "hash", "base_attr_hash", "router_hash", "router_ip",
		"peer_hash", "peer_ip", "peer_asn", "timestamp", "igp_router_id", "router_id", "routing_id#", "ls_id",
		"ospf_area_id", "isis_area_id", "protocol", "as_path", "local_pref", "med", "nexthop", "local_node_hash",
		"mt_id", "ospf_route_type", "igp_flags", "route_tag", "ext_route_tag", "ospf_fwd_addr", "igp_metric#",
		"prefix", "prefix_len#", "is_pre_policy", "is_adj_rib_in", "prefix_sid", "rd")
)

func init() {
	lsNodeSchema.attrFn = lsAttrCols
	lsLinkSchema.attrFn = lsAttrCols
	lsPrefixSchema.attrFn = lsAttrCols
	regX(famOf(16388, 71), parseLS, false)
	regX(famOf(16388, 72), parseLS, true)
	regX(famOf(16388, 80), parseLS, false)
}

// lsProtocol : protocol-ID (RFC 9552 5.2, 9086, 9552 IANA) -> openbmp 글자
func lsProtocol(p byte) string {
	switch p {
	case 1:
		return "IS-IS_L1"
	case 2:
		return "IS-IS_L2"
	case 3:
		return "OSPFv2"
	case 4:
		return "Direct"
	case 5:
		return "Static"
	case 6:
		return "OSPFv3"
	case 7:
		return "BGP"
	case 8:
		return "RSVP-TE"
	case 9:
		return "Segment-Routing"
	}
	return "unknown-" + strconv.Itoa(int(p))
}

// lsTLVs : TLV (종류 2 + 길이 2) 들을 차례로. 길이가 넘치면 오류
func lsTLVs(b []byte, fn func(typ int, v []byte)) error {
	for len(b) > 0 {
		if len(b) < 4 || 4+int(be16(b[2:])) > len(b) {
			return fmt.Errorf("bgp-ls tlv length")
		}
		n := int(be16(b[2:]))
		fn(int(be16(b)), b[4:4+n])
		b = b[4+n:]
	}
	return nil
}

// igpRouterID : 515 값 -> 글자. 4 = OSPF router id, 6 = IS-IS system id, 7 = + pseudonode, 8 = OSPF DR + 인터페이스 주소
func igpRouterID(v []byte) string {
	sys := func(b []byte) string { return fmt.Sprintf("%04x.%04x.%04x", be16(b), be16(b[2:]), be16(b[4:])) }
	switch len(v) {
	case 4:
		return ipText(v)
	case 6:
		return sys(v)
	case 7:
		return sys(v) + fmt.Sprintf(".%02x", v[6])
	case 8:
		return ipText(v[:4]) + ":" + ipText(v[4:])
	}
	return hex.EncodeToString(v)
}

// nodeDesc : node descriptor 의 칸들
type nodeDesc struct {
	asn, lsID, area, igp, bgpID string
}

func parseNodeDesc(v []byte) (nodeDesc, error) {
	var d nodeDesc
	err := lsTLVs(v, func(t int, x []byte) {
		switch {
		case t == 512 && len(x) == 4:
			d.asn = strconv.FormatUint(uint64(be32(x)), 10)
		case t == 513 && len(x) == 4:
			d.lsID = fmt.Sprintf("%08x", be32(x))
		case t == 514 && len(x) == 4:
			d.area = ipText(x)
		case t == 515:
			d.igp = igpRouterID(x)
		case t == 516 && len(x) == 4:
			d.bgpID = ipText(x)
		}
	})
	return d, err
}

// parseLS : NLRI 들 -> ls_node / ls_link / ls_prefix 경로 (모르는 종류는 other_family)
func parseLS(f family, b []byte, addPath bool) ([]xRoute, int, error) {
	var out []xRoute
	raw := 0
	vpn := f.safi() == 72
	for len(b) > 0 {
		id, rest, err := cutPathID(b, addPath)
		if err != nil || len(rest) < 4 || 4+int(be16(rest[2:])) > len(rest) {
			return out, raw, fmt.Errorf("bgp-ls nlri length")
		}
		typ, n := be16(rest), int(be16(rest[2:]))
		one := rest[:4+n]
		b = rest[4+n:]
		r, ok, err := lsRoute(typ, one[4:], id, one, vpn)
		if err != nil {
			return out, raw, fmt.Errorf("bgp-ls nlri type %d : %w", typ, err)
		}
		if !ok {
			out = append(out, otherRoute(f, id, one, strconv.Itoa(int(typ))))
			raw++
			continue
		}
		out = append(out, r)
	}
	return out, raw, nil
}

// lsRoute : NLRI 하나 (종류, 값). ok false = 모르는 종류
func lsRoute(typ uint16, v []byte, pathID uint32, whole []byte, vpn bool) (xRoute, bool, error) {
	var s *xSchema
	switch typ {
	case 1:
		s = lsNodeSchema
	case 2:
		s = lsLinkSchema
	case 3, 4:
		s = lsPrefixSchema
	default:
		return xRoute{}, false, nil
	}
	head := 9
	if vpn {
		head = 17
	}
	if len(v) < head {
		return xRoute{}, false, errShort
	}
	rd := ""
	if vpn {
		rd = rdString([8]byte(v[:8]))
		v = v[8:]
	}
	r := s.newRoute(pathID, whole)
	r.set("rd", rd)
	r.set("protocol", lsProtocol(v[0]))
	r.set("routing_id", strconv.FormatUint(uint64(be32(v[1:]))<<32|uint64(be32(v[5:])), 10))
	base := string(v[:9]) + rd + "|" // node hash 재료 = protocol + identifier + RD + node descriptor 바이트
	var mt []string
	err := lsTLVs(v[9:], func(t int, x []byte) {
		switch t {
		case 256:
			d, err := parseNodeDesc(x)
			if err != nil {
				return
			}
			r.set("igp_router_id", d.igp)
			r.set("router_id", d.bgpID)
			r.set("ls_id", d.lsID)
			r.set("ospf_area_id", d.area)
			r.set("local_node_asn", d.asn)
			if typ == 1 {
				r.hash = base + string(x)
			} else {
				r.refs = append(r.refs, xRef{s.idxOf["local_node_hash"], base + string(x)})
			}
		case 257:
			d, err := parseNodeDesc(x)
			if err != nil {
				return
			}
			r.set("remote_igp_router_id", d.igp)
			r.set("remote_router_id", d.bgpID)
			r.set("remote_node_asn", d.asn)
			r.refs = append(r.refs, xRef{s.idxOf["remote_node_hash"], base + string(x)})
		case 258:
			if len(x) == 8 {
				r.set("local_link_id", strconv.FormatUint(uint64(be32(x)), 10))
				r.set("remote_link_id", strconv.FormatUint(uint64(be32(x[4:])), 10))
			}
		case 259, 261:
			r.set("interface_ip", ipText(x))
		case 260, 262:
			r.set("neighbor_ip", ipText(x))
		case 263:
			for i := 0; i+2 <= len(x); i += 2 {
				mt = append(mt, strconv.FormatUint(uint64(be16(x[i:])&0x0fff), 16))
			}
		case 264:
			if len(x) == 1 && x[0] >= 1 && x[0] <= 6 {
				r.set("ospf_route_type", [...]string{"", "Intra", "Inter", "Ext-1", "Ext-2", "NSSA-1", "NSSA-2"}[x[0]])
			}
		case 265:
			if len(x) >= 1 {
				p, err := addrPrefix(x[1:], int(x[0]), typ == 4)
				if err == nil {
					pp, _ := netip.ParsePrefix(p)
					r.set("prefix", pp.Addr().String())
					r.set("prefix_len", strconv.Itoa(pp.Bits()))
				}
			}
		}
	})
	if len(mt) > 0 {
		r.set("mt_id", strings.Join(mt, ","))
	}
	return r, true, err
}

// ---- BGP-LS 속성 (종류 29) ----

// sidText : SID / 라벨 칸 (3 바이트 = 라벨 아래 20 비트, 4 바이트 = index, 16 바이트 = SRv6 SID)
func sidText(b []byte) string {
	switch len(b) {
	case 3:
		return strconv.FormatUint(uint64(uint32(b[0])<<16|uint32(b[1])<<8|uint32(b[2]))&0xfffff, 10)
	case 4:
		return "idx" + strconv.FormatUint(uint64(be32(b)), 10)
	case 16:
		return ipText(b)
	}
	return hex.EncodeToString(b)
}

// bwKbit : IEEE float32 (바이트 / 초) -> Kbit / 초 (openbmp 단위)
func bwKbit(b []byte) string {
	if len(b) != 4 {
		return ""
	}
	v := math.Float32frombits(be32(b))
	return strconv.FormatUint(uint64(float64(v)*8/1000), 10)
}

// bitNames : 1 바이트 flags 를 이름으로 (위 비트부터)
func bitNames(v byte, names string) string {
	var s []byte
	for i := 0; i < len(names) && i < 8; i++ {
		if v&(0x80>>i) != 0 {
			s = append(s, names[i])
		}
	}
	return string(s)
}

// lsAttrCols : add 줄에서 BGP-LS 속성 -> 칸 (그 토픽에 없는 칸은 set 이 무시)
func lsAttrCols(u *update, r *xRoute) {
	var adj, psid []string
	_ = lsTLVs(u.lsAttr, func(t int, x []byte) {
		switch t {
		case 1024: // node flag bits : Overload, aTtached, External, aBr, Router, V6
			if len(x) >= 1 {
				r.set("flags", bitNames(x[0], "OTEBRV"))
			}
		case 1026:
			r.set("name", string(x))
		case 1027:
			r.set("isis_area_id", hex.EncodeToString(x))
		case 1028, 1029: // local router id (node / link)
			if r.get("router_id") == "" {
				r.set("router_id", ipText(x))
			}
		case 1030, 1031:
			if r.get("remote_router_id") == "" {
				r.set("remote_router_id", ipText(x))
			}
		case 1034: // SR capabilities : flags 1 + 예약 1 + (범위 3 + SID/라벨 sub-TLV 1161) 들
			if len(x) >= 2 {
				var rs []string
				y := x[2:]
				for len(y) >= 7 && 7+int(be16(y[5:])) <= len(y) {
					n := int(be16(y[5:]))
					rng := uint32(y[0])<<16 | uint32(y[1])<<8 | uint32(y[2])
					rs = append(rs, strconv.FormatUint(uint64(rng), 10)+" "+sidText(y[7:7+n]))
					y = y[7+n:]
				}
				r.set("sr_capabilities", fmt.Sprintf("%02x ", x[0])+strings.Join(rs, ","))
			}
		case 1088:
			if len(x) == 4 {
				r.set("admin_group", strconv.FormatUint(uint64(be32(x)), 10))
			}
		case 1089:
			r.set("max_link_bw", bwKbit(x))
		case 1090:
			r.set("max_resv_bw", bwKbit(x))
		case 1091:
			var bw []string
			for i := 0; i+4 <= len(x); i += 4 {
				bw = append(bw, bwKbit(x[i:i+4]))
			}
			r.set("unreserved_bw", strings.Join(bw, ","))
		case 1092:
			if len(x) >= 3 && len(x) <= 4 {
				r.set("te_default_metric", strconv.FormatUint(beN(x), 10))
			}
		case 1093:
			if len(x) >= 1 {
				r.set("link_protection", flagWords(x[0], "", "", "Enhanced", "Dedicated1+1", "Dedicated1:1", "Shared", "Unprotected", "ExtraTraffic"))
			}
		case 1094:
			if len(x) >= 1 {
				r.set("mpls_proto_mask", flagWords(x[0], "LDP", "RSVP-TE"))
			}
		case 1095, 1155: // IGP metric (link 1095 = 1 ~ 3 바이트, prefix 1155 = 4 바이트)
			if (t == 1095) == (r.s == lsLinkSchema) && len(x) >= 1 && len(x) <= 4 {
				r.set("igp_metric", strconv.FormatUint(beN(x), 10))
			}
		case 1096:
			var s []string
			for i := 0; i+4 <= len(x); i += 4 {
				s = append(s, strconv.FormatUint(uint64(be32(x[i:])), 16))
			}
			r.set("srlg", strings.Join(s, ","))
		case 1098:
			r.set("link_name", string(x))
		case 1099: // Adj-SID : flags 1 + weight 1 + 예약 2 + SID
			if len(x) >= 7 {
				adj = append(adj, fmt.Sprintf("%02x %d %s", x[0], x[1], sidText(x[4:])))
			}
		case 1101: // PeerNode SID (EPE, RFC 9086) : flags (V 0x80, L 0x40) 1 + weight 1 + 예약 2 + SID
			if len(x) >= 7 {
				l := ""
				if x[0]&0x40 != 0 {
					l = "L "
				}
				r.set("peer_node_sid", fmt.Sprintf("%s%d %s", l, x[1], sidText(x[4:])))
			}
		case 1152: // IGP flags : Down (IS-IS), No unicast, Local address, Propagate NSSA
			if len(x) >= 1 {
				r.set("igp_flags", bitNames(x[0], "DNLP"))
			}
		case 1153:
			var s []string
			for i := 0; i+4 <= len(x); i += 4 {
				s = append(s, strconv.FormatUint(uint64(be32(x[i:])), 10))
			}
			r.set("route_tag", strings.Join(s, ","))
		case 1154:
			var s []string
			for i := 0; i+8 <= len(x); i += 8 {
				s = append(s, fmt.Sprintf("%x", uint64(be32(x[i:]))<<32|uint64(be32(x[i+4:]))))
			}
			r.set("ext_route_tag", strings.Join(s, ","))
		case 1156:
			r.set("ospf_fwd_addr", ipText(x))
		case 1158: // Prefix-SID : flags 1 + algorithm 1 + 예약 2 + SID
			if len(x) >= 7 {
				psid = append(psid, fmt.Sprintf("%02x %d %s", x[0], x[1], sidText(x[4:])))
			}
		}
	})
	if len(adj) > 0 {
		r.set("adj_segment_id", strings.Join(adj, ","))
	}
	if len(psid) > 0 {
		r.set("prefix_sid", strings.Join(psid, ","))
	}
}

// beN : 1 ~ 8 바이트 big-endian
func beN(b []byte) uint64 {
	var v uint64
	for _, c := range b {
		v = v<<8 | uint64(c)
	}
	return v
}

// flagWords : 1 바이트 flags 를 이름 목록 (위 비트부터 names[0])
func flagWords(v byte, names ...string) string {
	var s []string
	for i, n := range names {
		if n != "" && v&(0x80>>i) != 0 {
			s = append(s, n)
		}
	}
	return strings.Join(s, ",")
}
