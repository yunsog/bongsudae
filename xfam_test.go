// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// xAttr : 경로 속성 하나 (flags, 종류, 값 hex). 255 바이트 넘으면 확장 길이
func xAttr(flags, typ byte, v string) string {
	n := len(v) / 2
	if n > 255 {
		return fmt.Sprintf("%02x%02x%04x", flags|0x10, typ, n) + v
	}
	return fmt.Sprintf("%02x%02x%02x", flags, typ, n) + v
}

// xUpdateHex : ORIGIN, AS_PATH 64496, 더 붙일 속성, MP_REACH (afi, safi, next hop) + NLRI
func xUpdateHex(afi uint16, safi byte, nh, nlri, extra string) string {
	mp := fmt.Sprintf("%04x%02x%02x", afi, safi, len(nh)/2) + nh + "00" + nlri
	attrs := "40010100" + "400206020100" + "00fbf0" + extra + xAttr(0x80, 14, mp)
	return "0000" + fmt.Sprintf("%04x", len(attrs)/2) + attrs
}

// xUnreachHex : MP_UNREACH 만
func xUnreachHex(afi uint16, safi byte, nlri string) string {
	attrs := xAttr(0x80, 15, fmt.Sprintf("%04x%02x", afi, safi)+nlri)
	return "0000" + fmt.Sprintf("%04x", len(attrs)/2) + attrs
}

// xRun : UPDATE -> Kafka 메시지 (TSV)
func xRun(t *testing.T, hexs string) []outMsg {
	t.Helper()
	u, err := parseUpdate(mustHex(t, hexs), pathOpts{as4: true})
	if err != nil {
		t.Fatalf("parse : %v", err)
	}
	c, r, rp := famTestPeer()
	return c.famMsgs(r, rp, u, nil)
}

// col : 칸 이름으로 TSV 줄의 값
func col(t *testing.T, s *xSchema, f []string, name string) string {
	t.Helper()
	if len(f) != len(s.cols) {
		t.Fatalf("%s : %d fields, want %d : %q", s.kind, len(f), len(s.cols), f)
	}
	for i, c := range s.cols {
		if c.name == name {
			return f[i]
		}
	}
	t.Fatalf("%s : no column %s", s.kind, name)
	return ""
}

const xRD = "0000fbf000000064" // 64496:100

// VPLS : RFC 4761 (17 바이트), RFC 6074 (12 바이트), 모르는 길이 -> other_family
func TestVPLS(t *testing.T) {
	nlri := "0011" + xRD + "0001" + "0001" + "000a" + "003e81" + "000c" + xRD + "c0000201" + "0002" + "abcd"
	out := xRun(t, xUpdateHex(25, 65, "c0000209", nlri, ""))
	if len(out) != 2 || out[0].kind != kindVPLS || out[0].rows != 2 || out[1].kind != kindOther {
		t.Fatalf("%+v", out)
	}
	rows := famRows(t, out[0])
	s := vplsSchema
	if len(s.cols) != 29 || col(t, s, rows[0], "rd") != "64496:100" || col(t, s, rows[0], "ve_id") != "1" ||
		col(t, s, rows[0], "ve_block_size") != "10" || col(t, s, rows[0], "label_base") != "1000" ||
		col(t, s, rows[0], "nexthop") != "192.0.2.9" || col(t, s, rows[0], "as_path") != " 64496" {
		t.Fatalf("%q", rows[0])
	}
	if col(t, s, rows[1], "pe_address") != "192.0.2.1" || col(t, s, rows[1], "ve_id") != "" {
		t.Fatalf("%q", rows[1])
	}
	o := famRows(t, out[1])[0]
	if col(t, otherSchema, o, "family") != "l2vpn-vpls" || col(t, otherSchema, o, "nlri") != "0002abcd" {
		t.Fatalf("%q", o)
	}
	// 철회 : del 줄, 속성 빈칸
	out = xRun(t, xUnreachHex(25, 65, "0011"+xRD+"0001"+"0001"+"000a"+"003e81"))
	f := famRows(t, out[0])[0]
	if col(t, s, f, "action") != "del" || col(t, s, f, "as_path") != "" || col(t, s, f, "ve_id") != "1" {
		t.Fatalf("%q", f)
	}
}

// RT Constraint : origin AS + RT 64496:100, 기본 경로 (길이 0)
func TestRTConstraint(t *testing.T) {
	out := xRun(t, xUpdateHex(1, 132, "c0000209", "60"+"0000fbf0"+"0002fbf000000064"+"00", ""))
	rows := famRows(t, out[0])
	s := rtcSchema
	if out[0].kind != kindRTC || len(rows) != 2 || col(t, s, rows[0], "rt_origin_as") != "64496" ||
		!strings.Contains(col(t, s, rows[0], "route_target"), "64496:100") || col(t, s, rows[1], "prefix_len") != "0" {
		t.Fatalf("%q", rows)
	}
}

// MVPN : 종류 1, 3, 4 (route key = 종류 3), 7, 모르는 종류 9 -> other_family
func TestMVPN(t *testing.T) {
	t1 := "01" + "0c" + xRD + "c0000201"
	t3v := xRD + "20" + "c6336401" + "20" + "e8010101" + "c0000201"
	t3 := "03" + fmt.Sprintf("%02x", len(t3v)/2) + t3v
	t4v := t3 + "c0000202"
	t4 := "04" + fmt.Sprintf("%02x", len(t4v)/2) + t4v
	t7 := "07" + "16" + xRD + "0000fbf0" + "20" + "c6336401" + "20" + "e8010101"
	t9 := "09" + "01" + "00"
	out := xRun(t, xUpdateHex(1, 5, "c0000209", t1+t3+t4+t7+t9, ""))
	if len(out) != 2 || out[0].kind != kindMVPN || out[0].rows != 4 || out[1].kind != kindOther {
		t.Fatalf("%+v", out)
	}
	s := mvpnSchema
	rows := famRows(t, out[0])
	if col(t, s, rows[0], "originating_router_ip") != "192.0.2.1" || col(t, s, rows[0], "rd") != "64496:100" ||
		col(t, s, rows[1], "source") != "198.51.100.1" || col(t, s, rows[1], "group") != "232.1.1.1" ||
		col(t, s, rows[2], "originating_router_ip") != "192.0.2.2" ||
		col(t, s, rows[2], "route_key") != "type=3 rd=64496:100 source=198.51.100.1 group=232.1.1.1 originating_router_ip=192.0.2.1" ||
		col(t, s, rows[3], "source_as") != "64496" || col(t, s, rows[3], "is_ipv4") != "1" {
		t.Fatalf("%q", rows)
	}
	if o := famRows(t, out[1])[0]; col(t, otherSchema, o, "nlri_type") != "9" || col(t, otherSchema, o, "family") != "ipv4-mvpn" {
		t.Fatalf("%q", o)
	}
}

// SR Policy : NLRI + Tunnel Encap (preference, binding SID, segment list, policy name)
func TestSRPolicy(t *testing.T) {
	sub := "0c06" + "0000" + "00000064" + // preference 100
		"0d06" + "0000" + "05dc0000" + // binding SID 라벨 24000
		"800019" + "00" + "0906" + "0000" + "00000002" + "0106" + "0000" + "03e81000" + "0106" + "0000" + "03e82000" + // weight 2, 16001 16002
		"820006" + "0000" + "706f6c31" // policy name "pol1"
	te := "000f" + fmt.Sprintf("%04x", len(sub)/2) + sub
	out := xRun(t, xUpdateHex(1, 73, "c0000209", "60"+"00000001"+"00000064"+"c0000202", xAttr(0xc0, 23, te)))
	f := famRows(t, out[0])[0]
	s := srSchema
	if out[0].kind != kindSRPolicy || col(t, s, f, "color") != "100" || col(t, s, f, "endpoint") != "192.0.2.2" ||
		col(t, s, f, "preference") != "100" || col(t, s, f, "binding_sid") != "24000" ||
		col(t, s, f, "segment_lists") != "w=2 16001 16002" || col(t, s, f, "policy_name") != "pol1" {
		t.Fatalf("%q", f)
	}
}

// BGP-LS : node, link, IPv4 prefix 를 한 UPDATE 에 + BGP-LS 속성. node hash 가 link / prefix 의 local_node_hash 와 같음
func TestBGPLS(t *testing.T) {
	tlv := func(typ uint16, v string) string { return fmt.Sprintf("%04x%04x", typ, len(v)/2) + v }
	head := "02" + "0000000000000000" // IS-IS L2, identifier 0
	local := tlv(256, tlv(512, "0000fbf0")+tlv(515, "000000000001"))
	remote := tlv(257, tlv(512, "0000fbf0")+tlv(515, "000000000002"))
	node := tlv(1, head+local)
	link := tlv(2, head+local+remote+tlv(259, "c0000201")+tlv(260, "c0000202"))
	pfx := tlv(3, head+local+tlv(265, "18cb0071"))
	srv6 := tlv(6, head+local)
	attr := tlv(1024, "80") + tlv(1026, "7231") + tlv(1095, "00000a") + tlv(1089, "4cee6b28") +
		tlv(1155, "00000014") + tlv(1158, "00000000"+"00000064")
	out := xRun(t, xUpdateHex(16388, 71, "c0000209", node+link+pfx+srv6, xAttr(0x80, 29, attr)))
	if len(out) != 4 || out[0].kind != "ls_node" || out[1].kind != "ls_link" || out[2].kind != "ls_prefix" || out[3].kind != kindOther {
		t.Fatalf("%+v", out)
	}
	n := famRows(t, out[0])[0]
	l := famRows(t, out[1])[0]
	p := famRows(t, out[2])[0]
	if col(t, lsNodeSchema, n, "igp_router_id") != "0000.0000.0001" || col(t, lsNodeSchema, n, "protocol") != "IS-IS_L2" ||
		col(t, lsNodeSchema, n, "name") != "r1" || col(t, lsNodeSchema, n, "flags") != "O" || len(n) != 28 {
		t.Fatalf("node %q", n)
	}
	nh := col(t, lsNodeSchema, n, "hash")
	if col(t, lsLinkSchema, l, "local_node_hash") != nh || col(t, lsLinkSchema, l, "remote_node_hash") == nh ||
		col(t, lsLinkSchema, l, "remote_igp_router_id") != "0000.0000.0002" || col(t, lsLinkSchema, l, "interface_ip") != "192.0.2.1" ||
		col(t, lsLinkSchema, l, "igp_metric") != "10" || col(t, lsLinkSchema, l, "max_link_bw") != "1000000" || len(l) != 47 {
		t.Fatalf("link %q", l)
	}
	if col(t, lsPrefixSchema, p, "local_node_hash") != nh || col(t, lsPrefixSchema, p, "prefix") != "203.0.113.0" ||
		col(t, lsPrefixSchema, p, "prefix_len") != "24" || col(t, lsPrefixSchema, p, "igp_metric") != "20" ||
		col(t, lsPrefixSchema, p, "prefix_sid") != "00 0 idx100" || len(p) != 35 {
		t.Fatalf("prefix %q", p)
	}
	// 철회한 node 의 hash 도 같음
	out = xRun(t, xUnreachHex(16388, 71, node))
	if d := famRows(t, out[0])[0]; col(t, lsNodeSchema, d, "hash") != nh || col(t, lsNodeSchema, d, "action") != "del" || col(t, lsNodeSchema, d, "name") != "" {
		t.Fatalf("del %q", d)
	}
}

// other_family : MDT, Classful Transport, MUP
func TestOtherFamilyShapes(t *testing.T) {
	s := otherSchema
	f := famRows(t, xRun(t, xUpdateHex(1, 66, "c0000209", "80"+xRD+"c0000201"+"e8010101", ""))[0])[0]
	if col(t, s, f, "family") != "ipv4-mdt" || col(t, s, f, "prefix") != "192.0.2.1/32" || col(t, s, f, "detail") != "group=232.1.1.1" {
		t.Fatalf("mdt %q", f)
	}
	f = famRows(t, xRun(t, xUpdateHex(1, 76, "0000000000000000c0000209", "70"+"03e801"+xRD+"cb0071", ""))[0])[0]
	if col(t, s, f, "labels") != "16000" || col(t, s, f, "rd") != "64496:100" || col(t, s, f, "prefix") != "203.0.113.0/24" ||
		col(t, s, f, "nexthop") != "192.0.2.9" {
		t.Fatalf("ct %q", f)
	}
	f = famRows(t, xRun(t, xUpdateHex(1, 85, "c0000209", "01"+"0001"+"0c"+xRD+"c0000201", ""))[0])[0]
	if col(t, s, f, "nlri_type") != "1/1" || col(t, s, f, "rd") != "64496:100" {
		t.Fatalf("mup %q", f)
	}
	// JSON : 숫자 칸은 숫자, 빈 칸은 빠짐
	u, _ := parseUpdate(mustHex(t, xUpdateHex(1, 66, "c0000209", "80"+xRD+"c0000201"+"e8010101", "")), pathOpts{as4: true})
	c, r, rp := famTestPeer()
	c.json = true
	var o map[string]any
	if err := json.Unmarshal(c.famMsgs(r, rp, u, nil)[0].value, &o); err != nil || o["safi"] != 66.0 || o["is_pre_policy"] != true ||
		o["origin_as"] != 64496.0 || o["labels"] != nil || o["family"] != "ipv4-mdt" {
		t.Fatalf("%v %v", err, o)
	}
}
