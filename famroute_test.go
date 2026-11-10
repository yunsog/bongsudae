package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// famRows : 메시지 값 (openbmp 꼴) -> 줄마다 칸
func famRows(t *testing.T, m outMsg) [][]string {
	parts := strings.SplitN(string(m.value), "\n\n", 2)
	if len(parts) != 2 || !strings.Contains(parts[0], "T: "+m.kind) {
		t.Fatalf("header %q", parts[0])
	}
	var out [][]string
	for _, l := range strings.Split(strings.TrimSuffix(parts[1], "\n"), "\n") {
		out = append(out, strings.Split(l, "\t"))
	}
	return out
}

func famTestPeer() (*collector, *router, *rowPeer) {
	c := newCollector("x")
	r := c.routerFor("192.0.2.1", "")
	rp := &rowPeer{routerHash: "r", routerIP: "192.0.2.1", peerHash: "p", peerIP: "198.51.100.1", peerAS: 64496,
		ts: "2026-11-03 00:00:00.000000", prePolicy: true, adjRibIn: true}
	return c, r, rp
}

// VPNv4 : 라벨 17 + RD 64496:1 + 203.0.113.0/24 -> l3vpn 34 칸. 철회 (호환 라벨 0x800000) -> del 줄, 라벨 없음
func TestL3VPN(t *testing.T) {
	// ORIGIN igp, AS_PATH 64496, MP_REACH vpnv4 : next hop RD 0 + 192.0.2.9, NLRI 112 비트 = 라벨 000111 (17, S) + RD + 203.0.113/24
	mp := "0001" + "80" + "0c" + "0000000000000000" + "c0000209" + "00" + "70" + "000111" + "0000fbf000000001" + "cb0071"
	u, err := parseUpdate(mustHex(t, "0000"+"0030"+"40010100"+"400206020100"+"00fbf0"+"800e20"+mp), pathOpts{as4: true})
	if err != nil || len(u.fams) != 1 || len(u.fams[0].add) != 1 || u.others != 0 {
		t.Fatalf("%v %+v", err, u)
	}
	c, r, rp := famTestPeer()
	out := c.famMsgs(r, rp, u, nil)
	if len(out) != 1 || out[0].kind != kindL3VPN || out[0].key != "p" {
		t.Fatalf("msgs %+v", out)
	}
	f := famRows(t, out[0])[0]
	if len(f) != 34 || f[0] != "add" || f[10] != "203.0.113.0" || f[11] != "24" || f[17] != "192.0.2.9" || f[25] != "1" ||
		f[28] != "17" || f[31] != "64496:1" || f[32] != "0" || f[16] != "64496" {
		t.Fatalf("%d fields %q", len(f), f)
	}
	if r.stat.famRows != 1 {
		t.Fatal(r.stat.famRows)
	}
	// 철회 : MP_UNREACH vpnv4, 라벨 칸 = 호환 값 800000
	un := "0001" + "80" + "70" + "800000" + "0000fbf000000001" + "cb0071"
	u, err = parseUpdate(mustHex(t, "0000"+"0015"+"800f12"+un), pathOpts{as4: true})
	if err != nil || len(u.fams) != 1 || len(u.fams[0].del) != 1 || len(u.fams[0].del[0].labels) != 0 {
		t.Fatalf("withdraw %v %+v", err, u)
	}
	f = famRows(t, c.famMsgs(r, rp, u, nil)[0])[0]
	if len(f) != 34 || f[0] != "del" || f[13] != "" || f[28] != "" || f[31] != "64496:1" {
		t.Fatalf("del %q", f)
	}
}

// 6PE (IPv6 labeled unicast) : 라벨 둘 (16001, 3) -> labeled_unicast_prefix 32 칸, labels "16001,3". multicast -> multicast_prefix
func TestLabeledAndMulticast(t *testing.T) {
	// MP_REACH 2/4 : next hop ::ffff:192.0.2.9 (16), NLRI 80 비트 = 03e810 (16001) + 000031 (3, S) + 2001:0db8 (/32)
	mp := "0002" + "04" + "10" + "00000000000000000000ffffc0000209" + "00" + "50" + "03e810" + "000031" + "20010db8"
	u, err := parseUpdate(mustHex(t, "0000"+"0030"+"40010100"+"400206020100"+"00fbf0"+"800e20"+mp), pathOpts{as4: true})
	if err != nil {
		t.Fatal(err)
	}
	c, r, rp := famTestPeer()
	out := c.famMsgs(r, rp, u, nil)
	if len(out) != 1 || out[0].kind != kindLabeled {
		t.Fatalf("msgs %+v", out)
	}
	f := famRows(t, out[0])[0]
	if len(f) != 32 || f[10] != "2001:db8::" || f[11] != "32" || f[12] != "0" || f[17] != "::ffff:192.0.2.9" || f[28] != "16001,3" {
		t.Fatalf("labeled %q", f)
	}
	// multicast 1/2 : 프리픽스만
	mc := "0001" + "02" + "04" + "c0000209" + "00" + "18" + "e00001"
	u, err = parseUpdate(mustHex(t, "0000"+"001d"+"40010100"+"400206020100"+"00fbf0"+"800e0d"+mc), pathOpts{as4: true})
	if err != nil {
		t.Fatal(err)
	}
	out = c.famMsgs(r, rp, u, nil)
	f = famRows(t, out[0])[0]
	if out[0].kind != kindMulticast || len(f) != 32 || f[10] != "224.0.1.0" || f[11] != "24" || f[28] != "" {
		t.Fatalf("multicast %s %q", out[0].kind, f)
	}
}

// JSON 꼴 : l3vpn 줄에 labels, rd, rd_type
func TestL3VPNJSON(t *testing.T) {
	mp := "0001" + "80" + "0c" + "0000000000000000" + "c0000209" + "00" + "70" + "000111" + "0000fbf000000001" + "cb0071"
	u, _ := parseUpdate(mustHex(t, "0000"+"0030"+"40010100"+"400206020100"+"00fbf0"+"800e20"+mp), pathOpts{as4: true})
	c, r, rp := famTestPeer()
	c.json = true
	out := c.famMsgs(r, rp, u, nil)
	var o map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(out[0].value))), &o); err != nil {
		t.Fatal(err, string(out[0].value))
	}
	if o["labels"] != "17" || o["rd"] != "64496:1" || o["rd_type"] != 0.0 || o["prefix"] != "203.0.113.0" || o["nexthop"] != "192.0.2.9" {
		t.Fatalf("%v", o)
	}
}

// 깨진 라벨 스택 / RD : 오류 (panic 아님)
func TestFamNLRIBroken(t *testing.T) {
	sp := famSpecs[famOf(afiIPv4, 128)]
	for _, s := range []string{"70000111", "18000110", "70000111000000", "ff" + strings.Repeat("00", 40)} {
		if _, err := parseFamNLRI(mustHex(t, s), sp, false, false); err == nil {
			t.Errorf("%s must fail", s)
		}
	}
}

// 처음부터 끝까지 : BMP Route Monitoring (VPNv4 + IPv4 unicast) 를 파일 입력으로 -> l3vpn 출력 파일이 처음 쓸 때 생기고, unicast 는 그대로
func TestFamilyFileMode(t *testing.T) {
	dir := t.TempDir()
	// BGP UPDATE : ORIGIN, AS_PATH 64496, NEXT_HOP 192.0.2.9, MP_REACH vpnv4, NLRI 198.51.100.0/24
	mp := "0001" + "80" + "0c" + "0000000000000000" + "c0000209" + "00" + "70" + "000111" + "0000fbf000000001" + "cb0071"
	body := "0000" + "0037" + "40010100" + "400206020100" + "00fbf0" + "400304c0000209" + "800e20" + mp + "18c63364"
	bgp := strings.Repeat("ff", 16) + fmt.Sprintf("%04x", 19+len(body)/2) + "02" + body
	// per-peer 머리 42 : 종류 0, 깃발 0, RD 0, 주소 (IPv4 = 끝 4 바이트) 198.51.100.1, AS 64496, BGP ID, 시각 0
	pp := "00" + "00" + "0000000000000000" + strings.Repeat("00", 12) + "c6336401" + "0000fbf0" + "c6336401" + "00000000" + "00000000"
	msg := mustHex(t, "03"+fmt.Sprintf("%08x", 6+len(pp)/2+len(bgp)/2)+"00"+pp+bgp)
	h := newRawHead("c0ffee", "deadbeef", "192.0.2.1")
	full := append(make([]byte, h.room), msg...)
	in := filepath.Join(dir, "in.dump")
	f, _ := os.Create(in)
	w := bufio.NewWriter(f)
	writeDump(w, &dumpRecord{ts: testNow, key: []byte("deadbeef"), value: h.value(full)})
	w.Flush()
	f.Close()
	out := filepath.Join(dir, "out")
	if _, err := runFile(in, out); err != nil {
		t.Fatal(err)
	}
	for kind, want := range map[string]string{kindL3VPN: "64496:1", kindPrefix: "198.51.100.0"} {
		b, err := os.ReadFile(out + "." + kind + ".dump")
		if err != nil || !strings.Contains(string(b), want) {
			t.Fatalf("%s : %v %q", kind, err, b)
		}
	}
	if _, err := os.Stat(out + "." + kindMulticast + ".dump"); err == nil {
		t.Fatal("unused family topic file must not be created")
	}
}
