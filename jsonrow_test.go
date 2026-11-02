package main

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"
	"testing"
)

// 32 필드 이름 (docs/kafka.md 3 장) : JSON 의 이름과 openbmp 꼴의 칸 번호
var prefixFields = []string{"action", "seq", "hash", "router_hash", "router_ip", "base_attr_hash", "peer_hash", "peer_ip", "peer_asn",
	"timestamp", "prefix", "prefix_len", "is_ipv4", "origin", "as_path", "as_path_count", "origin_as", "nexthop", "med", "local_pref",
	"aggregator", "community_list", "ext_community_list", "cluster_list", "is_atomic_agg", "is_nexthop_ipv4", "originator_id",
	"path_id", "labels", "is_pre_policy", "is_adj_rib_in", "large_community_list"}

// jsonLines : 메시지 값 (한 줄에 객체 하나) -> 객체들. 줄마다 올바른 JSON 이어야 함
func jsonLines(t *testing.T, v []byte) []map[string]any {
	var out []map[string]any
	if len(v) == 0 || v[len(v)-1] != '\n' {
		t.Fatalf("must end with newline : %q", v)
	}
	for _, l := range strings.Split(strings.TrimSuffix(string(v), "\n"), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("%v : %s", err, l)
		}
		out = append(out, m)
	}
	return out
}

// jsonText : JSON 값을 openbmp 칸 글자로 (true -> 1, 숫자 그대로)
func jsonText(v any) string {
	switch x := v.(type) {
	case bool:
		return b01(x)
	case float64:
		return fmt.Sprintf("%.0f", x)
	case string:
		return x
	}
	return fmt.Sprint(v)
}

// JSON 줄 = openbmp 줄과 같은 값 (형만 다름, as_path / cluster_list 는 앞뒤 공백 없음, del 은 속성 칸 없음)
func TestJSONRows(t *testing.T) {
	pfx := func(s string) prefix {
		x := netip.MustParsePrefix(s)
		return prefix{addr: x.Addr(), length: x.Bits(), pathID: 7}
	}
	var a attrs
	a.setPath([]asSeg{{2, []uint32{64496, 64497}}, {1, []uint32{64500, 64501}}}) // AS_SET 으로 끝남 : origin_as 글자
	a.origin, a.nextHop, a.mpNextHop, a.med, a.localPref = "igp", "192.0.2.1", "2001:db8::1", 10, 100
	a.community, a.extComm, a.largeComm, a.clusterList = "64496:1 64496:2", "rt=64496:1", "64496:1:2", "192.0.2.9 "
	a.aggregator, a.originatorID = "64496 192.0.2.5", "192.0.2.6"
	a.community += " \"q\\" // 따옴표 / 역빗금도 올바른 JSON 으로 (탭은 TSV 칸을 깨므로 속성 글자에 나오지 않음)
	u := &update{attrs: a, v4Add: []prefix{pfx("198.51.100.0/24")}, v6Add: []prefix{pfx("2001:db8:1::/48")},
		v4Del: []prefix{pfx("203.0.113.0/24")}}
	rp := &rowPeer{routerHash: "r", routerIP: "192.0.2.1", peerHash: "p", peerIP: "198.51.100.1", peerAS: 64496,
		ts: "2026-11-01 00:00:00.000000", prePolicy: true, adjRibIn: false}
	run := func(js bool) []outMsg {
		c := newCollector("x")
		c.json = js
		return c.updateMsgs(&router{}, rp, u)
	}
	tsv, js := run(false), run(true)
	if len(tsv) != len(js) {
		t.Fatalf("msgs %d / %d", len(tsv), len(js))
	}
	n := 0
	for i := range tsv {
		if tsv[i].key != js[i].key || tsv[i].rows != js[i].rows {
			t.Fatalf("msg %d key / rows", i)
		}
		body := strings.SplitN(string(tsv[i].value), "\n\n", 2)[1]
		lines := strings.Split(strings.TrimSuffix(body, "\n"), "\n")
		objs := jsonLines(t, js[i].value)
		if len(objs) != len(lines) {
			t.Fatalf("rows %d / %d", len(objs), len(lines))
		}
		for k, l := range lines {
			f, o := strings.Split(l, "\t"), objs[k]
			for j, name := range prefixFields {
				v, ok := o[name]
				if !ok {
					if f[0] == "del" && f[j] == "" {
						continue
					}
					t.Fatalf("%s : no %s", f[0], name)
				}
				if want := strings.TrimSpace(f[j]); jsonText(v) != want {
					t.Errorf("%s %s : json %q tsv %q", f[0], name, jsonText(v), want)
				}
			}
			if f[0] == "del" && len(o) != 16 { // 32 - 속성 16 칸
				t.Errorf("del fields %d : %v", len(o), o)
			}
			n++
		}
	}
	if n != 3 {
		t.Fatalf("rows %d", n)
	}
	if o := jsonLines(t, js[0].value)[0]; o["origin_as"] != "{64500,64501}" || o["as_path"] != "64496 64497 {64500,64501}" {
		t.Fatalf("origin_as %v as_path %v", o["origin_as"], o["as_path"])
	}
}

// router / peer : 줄 하나 = 객체 하나, 빈 숫자 칸은 뺌
func TestJSONRouterPeer(t *testing.T) {
	c := newCollector("x")
	c.json = true
	r := c.routerFor("192.0.2.100", "")
	init := c.initiation(r, []byte("\x00\x02\x00\x02r1\x00\x01\x00\x03a\nb"), testNow)
	o := jsonLines(t, init[0].value)[0]
	if o["action"] != "init" || o["name"] != "r1" || o["description"] != `a\nb` || o["term_code"] != 0.0 || o["seq"] != 0.0 {
		t.Fatalf("router %v", o)
	}
	out := c.safeProcess(r, mustHex(t, "03000000440300000000000000000000000000000000000000000000c63364670000fbf0c63364676ae222c300051812000000000000000000000000c0000264e2e800b3"), testNow)
	o = jsonLines(t, out[0].value)[0]
	if o["action"] != "up" || o["remote_asn"] != 64496.0 || o["remote_ip"] != "198.51.100.103" || o["is_ipv4"] != true {
		t.Fatalf("peer %v", o)
	}
	if _, ok := o["local_asn"]; ok { // OPEN 없는 Peer Up : 빈 숫자 칸
		t.Fatalf("local_asn must be omitted : %v", o)
	}
	if len(o) != len(peerFields)-6 { // local_asn, remote_holddown, adv_holddown + down 칸 (bmp_reason, 코드 2) 빠짐
		t.Errorf("peer fields %d : %v", len(o), o)
	}
}
