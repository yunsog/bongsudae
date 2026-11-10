package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

const evpnRD = "0000fbf000000064" // 64496:100

// evpnUpdateHex : ORIGIN, AS_PATH 64496, [ext community], MP_REACH evpn (next hop 192.0.2.9) + NLRI
func evpnUpdateHex(nlri, ext string) string {
	mp := "0019" + "46" + "04" + "c0000209" + "00" + nlri
	attrs := "40010100" + "400206020100" + "00fbf0" + ext + "800e" + fmt.Sprintf("%02x", len(mp)/2) + mp
	return "0000" + fmt.Sprintf("%04x", len(attrs)/2) + attrs
}

// 종류 2 (MAC/IP), 3 (Inclusive Multicast), 5 (IP Prefix) -> evpn 40 칸
func TestEVPN(t *testing.T) {
	t2 := "02" + "25" + evpnRD + strings.Repeat("00", 10) + "00000000" + "30" + "001122334455" + "20" + "c000020a" + "000641"
	t3 := "03" + "11" + evpnRD + "00000064" + "20" + "c0000209"
	t5 := "05" + "22" + evpnRD + strings.Repeat("00", 10) + "00000000" + "18" + "cb007100" + "00000000" + "000641"
	u, err := parseUpdate(mustHex(t, evpnUpdateHex(t2+t3+t5, "")), pathOpts{as4: true})
	if err != nil || len(u.fams) != 1 || len(u.fams[0].eAdd) != 3 {
		t.Fatalf("%v %+v", err, u)
	}
	c, r, rp := famTestPeer()
	out := c.famMsgs(r, rp, u, nil)
	if len(out) != 1 || out[0].kind != kindEVPN || out[0].rows != 3 {
		t.Fatalf("msgs %+v", out)
	}
	rows := famRows(t, out[0])
	f := rows[0] // 종류 2
	if len(f) != 40 || f[14] != "192.0.2.9" || f[27] != "64496:100" || f[28] != "0" || f[31] != "00000000" || f[32] != "000000000000000000" ||
		f[33] != "48" || f[34] != "00:11:22:33:44:55" || f[35] != "32" || f[36] != "192.0.2.10" || f[37] != "100" || f[38] != "" {
		t.Fatalf("type 2 : %d %q", len(f), f)
	}
	f = rows[1] // 종류 3 : originating router ip, ESI / MAC 없음
	if f[29] != "32" || f[30] != "192.0.2.9" || f[31] != "00000064" || f[32] != "" || f[34] != "" {
		t.Fatalf("type 3 : %q", f)
	}
	f = rows[2] // 종류 5 : IP 프리픽스는 ip_len / ip
	if f[35] != "24" || f[36] != "203.0.113.0" || f[37] != "100" {
		t.Fatalf("type 5 : %q", f)
	}
	// JSON : route_type, gateway_ip 를 더함
	c.json = true
	var o map[string]any
	line := strings.Split(string(c.famMsgs(r, rp, u, nil)[0].value), "\n")[2]
	if err := json.Unmarshal([]byte(line), &o); err != nil || o["route_type"] != 5.0 || o["gateway_ip"] != "0.0.0.0" || o["ip"] != "203.0.113.0" {
		t.Fatalf("%v %v", err, o)
	}
}

// VXLAN (encap=8) : 라벨 칸 = VNI 24 비트. 철회 -> del 줄, 속성 빈칸
func TestEVPNVXLAN(t *testing.T) {
	t2 := "02" + "25" + evpnRD + strings.Repeat("00", 10) + "00000000" + "30" + "001122334455" + "20" + "c000020a" + "000064"
	u, err := parseUpdate(mustHex(t, evpnUpdateHex(t2, "c01008"+"030c"+"00000000"+"0008")), pathOpts{as4: true})
	if err != nil {
		t.Fatal(err)
	}
	c, r, rp := famTestPeer()
	f := famRows(t, c.famMsgs(r, rp, u, nil)[0])[0]
	if f[19] != "encap=8" || f[37] != "100" { // MPLS 로 읽으면 6
		t.Fatalf("vxlan %q %q", f[19], f[37])
	}
	un := "0019" + "46" + t2
	u, err = parseUpdate(mustHex(t, "0000"+fmt.Sprintf("%04x", 3+len(un)/2)+"800f"+fmt.Sprintf("%02x", len(un)/2)+un), pathOpts{as4: true})
	if err != nil {
		t.Fatal(err)
	}
	f = famRows(t, c.famMsgs(r, rp, u, nil)[0])[0]
	if f[0] != "del" || f[10] != "" || f[5] != "" || f[34] != "00:11:22:33:44:55" || len(f) != 40 {
		t.Fatalf("del %q", f)
	}
}

// 깨진 / 모르는 종류 : 오류 또는 건너뜀 (panic 아님)
func TestEVPNBroken(t *testing.T) {
	if rs, err := parseEVPN(mustHex(t, "0903aabbcc"), false); err != nil || len(rs) != 0 { // 종류 9 : 건너뜀
		t.Fatal(rs, err)
	}
	for _, s := range []string{"0225" + evpnRD, "02", "0205" + evpnRD[:10], "0511" + evpnRD + "0000000000"} {
		if _, err := parseEVPN(mustHex(t, s), false); err == nil {
			t.Errorf("%s must fail", s)
		}
	}
}
