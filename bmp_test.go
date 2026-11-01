// 시험 : go test .
// 표본 (data/, 10-28) 에서 꺼낸 실제 UPDATE 바이트로 openbmpd 출력과 같은지 확인
package main

import (
	"encoding/hex"
	"net/netip"
	"strings"
	"testing"
	"time"
)

var testNow = time.Unix(1793160000, 0)

// mustHex : 16 진 글자 -> 바이트
func mustHex(t *testing.T, s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// parseBGPUpdate : BGP 메시지 (marker 부터) -> update
func parseBGPUpdate(t *testing.T, s string) *update {
	_, body, _, err := bgpMsg(mustHex(t, s))
	if err != nil {
		t.Fatal(err)
	}
	u, err := parseUpdate(body, pathOpts{as4: true})
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// IPv4 경로가 MP_REACH (AFI 1) 로 옴 (192.0.2.95) : 일반 NLRI 가 비어도 경로가 남아야 함
func TestMPReachIPv4(t *testing.T) {
	u := parseBGPUpdate(t, "ffffffffffffffffffffffffffffffff0052020000003b900e000d00010104c000021700188289634001010040020602010000407d800404000003e840050400000064c0080cfbf4006efbf40c80fbf42328")
	if len(u.v4Add) != 1 || u.v4Add[0].addr.String() != "130.137.99.0" || u.v4Add[0].length != 24 {
		t.Fatalf("v4Add = %v", u.v4Add)
	}
	a := u.attrs
	if a.mpNextHop != "192.0.2.23" || a.asPath != " 16509" || a.med != 1000 || a.localPref != 100 {
		t.Fatalf("attrs = %+v", a)
	}
	if a.community != "64500:110 64500:3200 64500:9000" {
		t.Fatalf("community = %q", a.community)
	}
}

// IPv6 MP_REACH + MP_UNREACH
func TestMPIPv6(t *testing.T) {
	u := parseBGPUpdate(t, "ffffffffffffffffffffffffffffffff0025020000000e900f000a0002013026059cc00c0a")
	if len(u.v6Del) != 1 || u.v6Del[0].addr.String() != "2605:9cc0:c0a::" || u.v6Del[0].length != 48 {
		t.Fatalf("v6Del = %v", u.v6Del)
	}
}

// ext community : 모르는 종류도 빈 자리 (openbmpd : "ospf-did=0:66048  ")
func TestExtComm(t *testing.T) {
	v := mustHex(t, "0005000000010200"+"8000000000000000"+"8000000000000000")
	if s := extCommString(v); s != "ospf-did=0:66048 ospf-rt=area-0:0:0 ospf-rt=area-0:0:0" { // 옛 OSPF 0x8000 도 글자로 (openbmpd 는 빈칸)
		t.Fatalf("got %q", s)
	}
	v = mustHex(t, "0002000200000003"+"030b00000000000a")
	if s := extCommString(v); s != "rt=2:3 color=10" {
		t.Fatalf("got %q", s)
	}
}

// AS_SET : 표준대로 (RFC 4271 묶음 = 1, RFC 6811 origin 없음 -> origin_as 칸에 묶음)
func TestASSet(t *testing.T) {
	var a attrs
	v := mustHex(t, "0202"+"00000513"+"0000d872"+"0104"+"0000957a"+"0000b0d7"+"0000b0d7"+"0000d95c")
	if err := parseASPath(v, true, &a); err != nil {
		t.Fatal(err)
	}
	if a.asPath != " 1299 55410 {38266,45271,45271,55644}" || a.asCount != 3 || a.originAS != 0 || originText(&a) != "{38266,45271,45271,55644}" {
		t.Fatalf("%q %d %d %q", a.asPath, a.asCount, a.originAS, originText(&a))
	}
}

// 깨진 입력 : 오류로 돌아와야 함 (panic 아님)
func TestBroken(t *testing.T) {
	for _, s := range []string{"", "03", "0300000006", "0400000006", "03ffffffff00", "0300000030" + strings.Repeat("00", 43)} {
		c := newCollector("x")
		b, _ := hex.DecodeString(s)
		c.safeProcess(c.routerFor("1.2.3.4", ""), b, testNow)
	}
}

// OPEN 없는 Peer Up (192.0.2.100 -> peer 198.51.100.103, 10-29 tcpdump) : 오류가 아니라 빈 능력의 peer up 하나
func TestPeerUpNoOpen(t *testing.T) {
	c := newCollector("x")
	b := mustHex(t, "03000000440300000000000000000000000000000000000000000000c63364670000fbf0c63364676ae222c300051812000000000000000000000000c0000264e2e800b3")
	r := c.routerFor("192.0.2.100", "")
	out := c.safeProcess(r, b, testNow)
	if len(out) != 1 || r.stat.errors != 0 || r.stat.noOpen != 1 {
		t.Fatalf("out %d errors %d noOpen %d", len(out), r.stat.errors, r.stat.noOpen)
	}
	f := strings.Split(strings.SplitN(string(out[0].value), "\n\n", 2)[1], "\t")
	if f[0] != "up" || f[8] != "64496" || f[9] != "198.51.100.103" || f[13] != "192.0.2.100" || f[14] != "58088" || f[11] != "179" || f[17] != "" {
		t.Fatalf("fields %q", f)
	}
}

// Peer Down 이유 2 (NOTIFICATION 없이 라우터가 닫음, FSM event 2 바이트) : openbmpd 와 같은 코드 0 / 0 과 글자
func TestPeerDownLocal(t *testing.T) {
	c := newCollector("x")
	b := mustHex(t, "0300000033020000000000000000000000000000000000000000000001020305"+"00003b418efb31080000000000000000"+"020007")
	out := c.safeProcess(c.routerFor("1.2.3.4", ""), b, testNow)
	if len(out) != 1 {
		t.Fatalf("out %d", len(out))
	}
	f := strings.Split(strings.SplitN(string(out[0].value), "\n\n", 2)[1], "\t")
	want := "Local (1.2.3.4) closed peer (1.2.3.5) session: fsm_event=7, No BGP notify message."
	if f[0] != "down" || f[21] != "2" || f[22] != "0" || f[23] != "0" || f[24] != want {
		t.Fatalf("fields %q", f)
	}
}

// 경로 수 = 살아 있는 프리픽스 수 : 같은 프리픽스 add 여러 번 = 1, del = 0, 없는 것 del = 무시, peer down = 0
func TestRIBCount(t *testing.T) {
	r := &router{ip: "1.2.3.4", peers: map[string]*peer{}}
	h := peerHdr{addr: netip.MustParseAddr("10.0.0.1"), as: 65000}
	p := r.peerGet(&h)
	p.up = true
	pfx := func(s string) prefix {
		x := netip.MustParsePrefix(s)
		return prefix{addr: x.Addr(), length: x.Bits()}
	}
	add := &update{attrs: attrs{asPath: " 65000 1", nextHop: "10.0.0.1"}, v4Add: []prefix{pfx("1.0.0.0/24")},
		v6Add: []prefix{pfx("2001:db8::/32")}}
	for i := 0; i < 100; i++ {
		p.apply(add, testNow)
	}
	if len(p.rib4) != 1 || len(p.rib6) != 1 {
		t.Fatalf("100 add : v4 %d v6 %d (1, 1 이어야)", len(p.rib4), len(p.rib6))
	}
	p.apply(&update{v4Del: []prefix{pfx("9.9.9.0/24")}}, testNow) // 없는 것
	p.apply(&update{v4Del: []prefix{pfx("1.0.0.0/24")}}, testNow)
	if len(p.rib4) != 0 || len(p.rib6) != 1 {
		t.Fatalf("del : v4 %d v6 %d", len(p.rib4), len(p.rib6))
	}
	// 찾기 : 주소 하나로 longest, /16 으로 more
	p.apply(&update{v4Add: []prefix{pfx("1.0.0.0/16"), pfx("1.0.1.0/24")}}, testNow)
	q, how, _ := parseQuery("1.0.1.9")
	if m := match(r, p, q, how); len(m) != 1 || m[0].pfx.String() != "1.0.1.0/24" {
		t.Fatalf("longest %v", m)
	}
	q, _, _ = parseQuery("1.0.0.0/16")
	if m := newCollector("x").findMany([]routerPeer{{r, p}}, []findQuery{{p: q, how: "more"}}, 0, nil)[0]; m.total != 2 {
		t.Fatalf("more %v", m.rows)
	}
	if m := match(r, p, netip.MustParsePrefix("1.0.1.0/24"), "covering"); len(m) != 2 {
		t.Fatalf("covering %v", m)
	}
	r.routerDown(testNow, "connection closed")
	if p.up || len(p.rib4)+len(p.rib6) != 0 {
		t.Fatalf("router down : up %v routes %d", p.up, len(p.rib4)+len(p.rib6))
	}
}
