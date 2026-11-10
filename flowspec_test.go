package main

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
)

// fsUpdateHex : ORIGIN, AS_PATH 64496, ext community traffic-rate 0 (= 버림), MP_REACH (afi / safi, next hop 없음) + NLRI
func fsUpdateHex(afiSafi, nlri string) string {
	mp := afiSafi + "00" + "00" + nlri
	attrs := "40010100" + "400206020100" + "00fbf0" + "c01008" + "8006fbf000000000" + "800e" + fmt.Sprintf("%02x", len(mp)/2) + mp
	return "0000" + fmt.Sprintf("%04x", len(attrs)/2) + attrs
}

// IPv4 규칙 : dst 203.0.113.0/24, proto =6, dport =80,=443, tcp-flags syn&!ack -> flowspec 43 칸
func TestFlowSpec(t *testing.T) {
	rule := "0118cb0071" + "038106" + "05015091" + "01bb" + "090002c210"
	u, err := parseUpdate(mustHex(t, fsUpdateHex("000185", "13"+rule)), pathOpts{as4: true})
	if err != nil || len(u.fams) != 1 || len(u.fams[0].fsAdd) != 1 {
		t.Fatalf("%v %+v", err, u)
	}
	c, r, rp := famTestPeer()
	out := c.famMsgs(r, rp, u, nil)
	if len(out) != 1 || out[0].kind != kindFlowSpec {
		t.Fatalf("msgs %+v", out)
	}
	f := famRows(t, out[0])[0]
	if len(f) != 43 || f[10] != "1" || f[11] != "" || f[12] != "dst 203.0.113.0/24 proto =6 dport =80,=443 tcp-flags syn&!ack" ||
		f[13] != "203.0.113.0/24" || f[15] != "=6" || f[17] != "=80,=443" || f[21] != "syn&!ack" || f[26] != "0" ||
		!strings.Contains(f[38], "flow-rate=64496:0") || f[40] != "0" {
		t.Fatalf("%d %q", len(f), f)
	}
	// JSON : traffic_rate 는 숫자, rule 은 글자
	c.json = true
	var o map[string]any
	if err := json.Unmarshal(c.famMsgs(r, rp, u, nil)[0].value, &o); err != nil || o["traffic_rate"] != 0.0 || o["dst_port"] != "=80,=443" || o["is_ipv4"] != true {
		t.Fatalf("%v %v", err, o)
	}
}

// 연산자 글자 : 범위 (AND), IPv6 flow label / fragment, VPN RD, 모르는 성분은 그 규칙만 버림
func TestFlowSpecOps(t *testing.T) {
	// sport >=1024&<=2048 : 연산자 0x13 (gt|eq, 2 바이트) + 0400, 0xd5 (끝|and|lt|eq, 2 바이트) + 0800
	rs, bad, err := parseFS(mustHex(t, "07"+"0613040"+"0d50800"), famOf(afiIPv4, 133), false)
	if err != nil || bad != 0 || len(rs) != 1 || rs[0].comp[6] != ">=1024&<=2048" {
		t.Fatalf("%v %d %+v", err, bad, rs)
	}
	// IPv6 : dst 2001:db8::/32 (offset 0), frag =isf, flow-label =5
	rs, _, err = parseFS(mustHex(t, "0d"+"01200020010db8"+"0c8102"+"0d8105"), famOf(afiIPv6, 133), false)
	if err != nil || len(rs) != 1 || rs[0].ruleText() != "dst 2001:db8::/32 frag =isf flow-label =5" {
		t.Fatalf("%v %+v", err, rs)
	}
	// VPN : RD 64496:1 + dst
	rs, _, err = parseFS(mustHex(t, "0d"+"0000fbf000000001"+"0118cb0071"), famOf(afiIPv4, 134), false)
	if err != nil || len(rs) != 1 || rdString(rs[0].rd) != "64496:1" || rs[0].dst.String() != "203.0.113.0/24" {
		t.Fatalf("%v %+v", err, rs)
	}
	// 모르는 성분 (14) : 그 규칙만 버리고 다음 규칙은 읽음
	rs, bad, err = parseFS(mustHex(t, "03"+"0e8101"+"05"+"0118cb0071"), famOf(afiIPv4, 133), false)
	if err != nil || bad != 1 || len(rs) != 1 {
		t.Fatalf("%v %d %+v", err, bad, rs)
	}
	// 깨진 길이 : 오류 (panic 아님)
	if _, _, err := parseFS(mustHex(t, "20"+"0118"), famOf(afiIPv4, 133), false); err == nil {
		t.Fatal("short rule must fail")
	}
}

// 경로표 : bmp.rib 에 ipv4-flowspec 을 적으면 규칙을 들고, 찾기 = 목적지 프리픽스, 철회, 상태 파일 왕복
func TestFlowSpecRIB(t *testing.T) {
	c, r, p := vpnCollector("ipv4-unicast,ipv4-flowspec")
	rule := "0118cb0071" + "038106"
	u, _ := parseUpdate(mustHex(t, fsUpdateHex("000185", "08"+rule)), pathOpts{as4: true})
	p.apply(viewInPre, u, testNow, c.on)
	if j := c.peerView(r, p, false); j.FamRoutes["ipv4-flowspec"] != 1 {
		t.Fatalf("%+v", j.FamRoutes)
	}
	m := famMatch(r, p, -1, famOf(afiIPv4, 133), netip.MustParsePrefix("203.0.113.0/24"), "exact")
	b, _ := json.Marshal(m[0].json())
	if len(m) != 1 || !strings.Contains(string(b), `"rule":"dst 203.0.113.0/24 proto =6"`) || strings.Contains(string(b), `"labels"`) {
		t.Fatalf("%d %s", len(m), b)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "2.state")
	if _, n, err := writeHandover(path, &handoverState{Version: 2}, c); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	hs, _ := readHandover(path)
	c2, _, _ := vpnCollector("ipv4-unicast,ipv4-flowspec")
	c2.routers = map[string]*router{}
	hs.restore(c2)
	hs.loadRIBs(c2)
	q := c2.routers["192.0.2.1"].peers[p.key]
	if rows := q.view(-1).famRows(famOf(afiIPv4, 133)); len(rows) != 1 || rows[0].pfx.String() != "203.0.113.0/24" {
		t.Fatalf("state %+v", rows)
	}
	// 철회 (MP_UNREACH 1/133)
	un := "000185" + "08" + rule
	u, err := parseUpdate(mustHex(t, "0000"+fmt.Sprintf("%04x", 3+len(un)/2)+"800f"+fmt.Sprintf("%02x", len(un)/2)+un), pathOpts{as4: true})
	if err != nil {
		t.Fatal(err)
	}
	p.apply(viewInPre, u, testNow, c.on)
	if p.view(-1).fam[famOf(afiIPv4, 133)].paths() != 0 {
		t.Fatal("withdraw")
	}
	// 목적지 없는 규칙 (proto =17) : 목록에는 나오고 prefix 는 빈칸, 프리픽스 찾기에는 안 걸림
	u, _ = parseUpdate(mustHex(t, fsUpdateHex("000185", "03"+"038111")), pathOpts{as4: true})
	p.apply(viewInPre, u, testNow, c.on)
	all := famMatch(r, p, -1, famOf(afiIPv4, 133), netip.Prefix{}, "")
	b, _ = json.Marshal(all[0].json())
	if len(all) != 1 || !strings.Contains(string(b), `"prefix":"","rule":"proto =17"`) ||
		len(famMatch(r, p, -1, famOf(afiIPv4, 133), netip.MustParsePrefix("0.0.0.0/0"), "more")) != 0 {
		t.Fatalf("%d %s", len(all), b)
	}
}
