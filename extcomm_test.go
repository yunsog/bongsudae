package main

import (
	"strings"
	"testing"
)

// TestExtCommStandard : 표준대로의 글자 (실제 망에서 본 바이트 + openbmpd 가 틀리던 것)
func TestExtCommStandard(t *testing.T) {
	for _, c := range []struct {
		hex  string
		want string
	}{
		{"000934b200000000", "sas=13490:0"},                   // 10-29 실시간 (.247, AS1299)
		{"010b1835a8eb0000", "import=24.53.168.235:0"},        // 10-29 실시간
		{"00021d9a0000000a", "rt=7578:10"},                    // 보통
		{"0102e3e50001" + "0064", "rt=227.229.0.1:100"},       // IPv4 rt
		{"00045ba048742400", "link-bw=23456:250000"},          // 실시간 (.247) : IEEE float 250000 바이트/초 (openbmpd : 1215570944)
		{"40045ba048742400", "link-bw=23456:250000"},          // non-transitive link bandwidth (openbmpd : 빈칸)
		{"8000000000000501", "ospf-rt=area-0:E:1"},            // 실시간 (.145) 옛 OSPF route type (openbmpd : 빈칸)
		{"800186a501ee0000", "ospf-rid=134.165.1.238:0"},      // 실시간 (.145) 옛 OSPF router id (openbmpd : 빈칸)
		{"0043006400000001", "0x0043:006400000001"},           // 실시간 (.145) 모르는 종류 : 바이트 그대로 (openbmpd : 빈칸)
		{"0109010000010005", "sas=1.0.0.1:5"},                 // IPv4 종류 : 바이트 순서 그대로 (openbmpd : 뒤집음)
		{"020b0000fde80007", "import=65000:7"},                // 4 바이트 AS import (openbmpd : 2 바이트 AS 꼴)
		{"060300 1a0bc0ff01", "router_mac=00:1a:0b:c0:ff:01"}, // 두 자리
		{"0600010000000005", "mac-mob=1:5"},                   // 공백 없이 (openbmpd : 'mac_mob_flags=\x01 mac_mob_seq_num=5')
		{"030c000000000008", "encap=8"},
		{"030c000000000108", "encap=264"},  // 2 바이트 (openbmpd : 8)
		{"4300000000000002", "ov=invalid"}, // origin validation state (RFC 8097)
	} {
		b := mustHex(t, strings.ReplaceAll(c.hex, " ", ""))
		if got := extCommOne(b); got != c.want {
			t.Errorf("%s : %q, want %q", c.hex, got, c.want)
		}
	}
	if s := extCommString(mustHex(t, "000934b200000000"+"0043006400000001")); s != "sas=13490:0 0x0043:006400000001" {
		t.Errorf("string %q", s)
	}
}
