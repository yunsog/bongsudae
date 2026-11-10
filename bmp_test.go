// 시험 : go test .
// 표본 (data/, 10-28) 에서 꺼낸 실제 UPDATE 바이트로 openbmpd 출력과 같은지 확인
package main

import (
	"encoding/hex"
	"fmt"
	"net"
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
	// AS 하나뿐인 묶음 : 글자 / 개수는 묶음 그대로, origin_as 는 숫자
	v = mustHex(t, "0202"+"00000513"+"0000d872"+"0101"+"00002b83")
	if err := parseASPath(v, true, &a); err != nil {
		t.Fatal(err)
	}
	if a.asPath != " 1299 55410 {11139}" || a.asCount != 3 || a.originAS != 11139 || originText(&a) != "11139" {
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
	r := &router{ip: "1.2.3.4", peers: map[string]*peer{}, byID: map[peerID]*peer{}}
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
		p.apply(viewInPre, add, testNow, allOn)
	}
	if inPre(p).rib4.size() != 1 || inPre(p).rib6.size() != 1 {
		t.Fatalf("100 add : v4 %d v6 %d (1, 1 이어야)", inPre(p).rib4.size(), inPre(p).rib6.size())
	}
	p.apply(viewInPre, &update{v4Del: []prefix{pfx("9.9.9.0/24")}}, testNow, allOn) // 없는 것
	p.apply(viewInPre, &update{v4Del: []prefix{pfx("1.0.0.0/24")}}, testNow, allOn)
	if inPre(p).rib4.size() != 0 || inPre(p).rib6.size() != 1 {
		t.Fatalf("del : v4 %d v6 %d", inPre(p).rib4.size(), inPre(p).rib6.size())
	}
	// 찾기 : 주소 하나로 longest, /16 으로 more
	p.apply(viewInPre, &update{v4Add: []prefix{pfx("1.0.0.0/16"), pfx("1.0.1.0/24")}}, testNow, allOn)
	q, how, _ := parseQuery("1.0.1.9")
	if m := match(r, p, -1, q, how); len(m) != 1 || m[0].pfx.String() != "1.0.1.0/24" {
		t.Fatalf("longest %v", m)
	}
	q, _, _ = parseQuery("1.0.0.0/16")
	if ms, _ := newCollector("x").findMany([]routerPeer{{r, p}}, []findQuery{{p: q, how: "more"}}, 0, nil, -1, 0); ms[0].total != 2 {
		t.Fatalf("more %v", ms[0].rows)
	}
	if m := match(r, p, -1, netip.MustParsePrefix("1.0.1.0/24"), "covering"); len(m) != 2 {
		t.Fatalf("covering %v", m)
	}
	r.routerDown(testNow, "connection closed")
	if p.up || inPre(p).rib4.size()+inPre(p).rib6.size() != 0 {
		t.Fatalf("router down : up %v routes %d", p.up, inPre(p).rib4.size()+inPre(p).rib6.size())
	}
}

// Kafka key 1-B (-key-buckets) : 칸으로 나눠도 줄 (seq 포함) 은 모두 같고, 같은 프리픽스는 늘 같은 칸, key = peer hash.칸
func TestKeyBuckets(t *testing.T) {
	pfx := func(s string) prefix {
		x := netip.MustParsePrefix(s)
		return prefix{addr: x.Addr(), length: x.Bits()}
	}
	var adds, dels []prefix
	for i := 0; i < 200; i++ {
		adds = append(adds, pfx(fmt.Sprintf("10.%d.%d.0/24", i/250, i%250)))
	}
	dels = append(dels, pfx("192.0.2.0/24"), pfx("2001:db8::/32"))
	u := &update{attrs: attrs{asPath: " 64500", nextHop: "192.0.2.1"}, v4Add: adds, v4Del: dels[:1], v6Del: dels[1:]}
	rp := &rowPeer{routerHash: "r", routerIP: "192.0.2.1", peerHash: "p", peerIP: "198.51.100.1", ts: "2026-11-01 00:00:00.000000", prePolicy: true, adjRibIn: true}
	rows := func(n int) (map[string]string, []outMsg) { // 줄 -> key
		c := newCollector("x")
		c.keyBuckets = n
		out := c.updateMsgs(&router{}, rp, u)
		m := map[string]string{}
		for _, o := range out {
			body := strings.SplitN(string(o.value), "\n\n", 2)[1]
			for _, l := range strings.Split(strings.TrimSuffix(body, "\n"), "\n") {
				m[l] = o.key
			}
		}
		return m, out
	}
	plain, out0 := rows(0)
	if len(out0) != 2 || out0[0].key != "p" || len(plain) != 202 {
		t.Fatalf("n=0 : msgs %d key %q rows %d", len(out0), out0[0].key, len(plain))
	}
	split, out8 := rows(8)
	if len(split) != len(plain) {
		t.Fatalf("n=8 rows %d, n=0 rows %d", len(split), len(plain))
	}
	for l, k := range split { // 줄 글자 (seq 포함) 가 같아야 함
		if _, ok := plain[l]; !ok {
			t.Fatalf("n=8 row not in n=0 : %q", l)
		}
		f := strings.Split(l, "\t")
		p := pfx(f[10] + "/" + f[11])
		if want := fmt.Sprintf("p.%d", prefixBucket(p, 8)); k != want {
			t.Fatalf("%s key %q want %q", f[10], k, want)
		}
	}
	if len(out8) < 8 || len(out8) > 8+2 { // add 는 200 줄이라 8 칸 모두, del 은 2 줄 = 최대 2 칸
		t.Fatalf("n=8 msgs %d", len(out8))
	}
	if out8[len(out8)-1].value[0] != 'V' || !strings.Contains(string(out8[len(out8)-1].value), "\ndel\t") {
		t.Fatalf("del message must come last")
	}
}

// NLRI 끝 비트 (RFC 4271 4.3) : 길이 뒤의 남는 비트는 0 으로 (/23 에 10.1.1 -> 10.1.0.0/23)
func TestNLRITrailingBits(t *testing.T) {
	ps, err := parseNLRI([]byte{23, 10, 1, 1, 64, 0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0xff}, afiIPv4, false)
	if err == nil {
		t.Fatalf("v4 /64 must fail : %v", ps)
	}
	ps, err = parseNLRI([]byte{23, 10, 1, 1, 9, 0xff, 0xff}, afiIPv4, false)
	if err != nil || len(ps) != 2 || ps[0].addr.String() != "10.1.0.0" || ps[1].addr.String() != "255.128.0.0" {
		t.Fatalf("v4 %v %v", ps, err)
	}
	ps, err = parseNLRI([]byte{33, 0x20, 0x01, 0x0d, 0xb8, 0xff}, afiIPv6, false)
	if err != nil || len(ps) != 1 || ps[0].addr.String() != "2001:db8:8000::" {
		t.Fatalf("v6 %v %v", ps, err)
	}
}

// RFC 6793 4.2.3 : 2 바이트 AS peer 의 AGGREGATOR / AS4_AGGREGATOR / AS4_PATH
func TestAS4Aggregator(t *testing.T) {
	agg := func(as uint16) []byte { return []byte{byte(as >> 8), byte(as), 192, 0, 2, 1} }
	as4agg := []byte{0, 1, 0x00, 0x01, 198, 51, 100, 1} // AS 65537, 198.51.100.1
	as4path := mustHex(t, "0201"+"00010001")            // AS_SEQUENCE 65537
	path := func() attrs {
		var a attrs
		if err := parseASPath(mustHex(t, "0202"+"5ba0"+"5ba0"), false, &a); err != nil { // 2 바이트 : 23456 23456
			t.Fatal(err)
		}
		return a
	}
	a := path() // AGGREGATOR = AS_TRANS : AS4_AGGREGATOR 를 쓰고 AS4_PATH 를 합침
	mergeAS4(agg(asTrans), as4agg, as4path, &a)
	if a.aggregator != "65537 198.51.100.1" || a.asPath != " 23456 65537" {
		t.Fatalf("trans : agg %q path %q", a.aggregator, a.asPath)
	}
	a = path() // AGGREGATOR 가 AS_TRANS 가 아님 : AS4_* 모두 무시
	a.aggregator = "64500 192.0.2.1"
	mergeAS4(agg(64500), as4agg, as4path, &a)
	if a.aggregator != "64500 192.0.2.1" || a.asPath != " 23456 23456" {
		t.Fatalf("not trans : agg %q path %q", a.aggregator, a.asPath)
	}
	a = path() // AGGREGATOR 없음 : AS4_PATH 만
	mergeAS4(nil, as4agg, as4path, &a)
	if a.aggregator != "" || a.asPath != " 23456 65537" {
		t.Fatalf("no agg : agg %q path %q", a.aggregator, a.asPath)
	}
}

// 같은 라우터의 새 연결은 옛 연결을 닫음, closeAll 은 모두 닫음
func TestConnTableReplace(t *testing.T) {
	ct := newConnTable()
	a1, b1 := net.Pipe()
	a2, b2 := net.Pipe()
	defer b1.Close()
	defer b2.Close()
	old, cur := &routerConn{conn: a1}, &routerConn{conn: a2}
	ct.add("192.0.2.1", old)
	ct.add("192.0.2.1", cur)
	if _, err := a1.Write([]byte{1}); err == nil {
		t.Fatal("old connection must be closed")
	}
	if ct.remove("192.0.2.1", old) != "Closed by collector: replaced by new connection" || ct.m["192.0.2.1"] != cur {
		t.Fatalf("old why %q / table %v", old.why, ct.m)
	}
	if n := ct.closeAll("Closed by collector: shutdown"); n != 1 || cur.why != "Closed by collector: shutdown" {
		t.Fatalf("closeAll %d %q", n, cur.why)
	}
}

// BMP Stats Report (0.2.0) : 아는 peer 에만, 32 / 64 비트 / 주소 종류마다
func TestStatsReport(t *testing.T) {
	c := newCollector("x")
	r := c.lockRouter("192.0.2.1", "")
	defer r.mu.Unlock()
	h := fzPeer(0, 0, false)
	body := append(append([]byte{}, h...), 0, 0, 0, 3,
		0, 0, 0, 4, 0, 0, 0, 5, // rejected_prefixes = 5
		0, 7, 0, 8, 0, 0, 0, 1, 0, 0, 0, 2, // adj_rib_in_routes = 2^32 + 2
		0, 9, 0, 11, 0, 2, 1, 0, 0, 0, 0, 0, 0, 0, 9) // adj_rib_in_routes_2_1 = 9
	if err := c.statsReport(r, body, testNow); err != nil {
		t.Fatal(err)
	}
	if len(r.peers) != 0 {
		t.Fatalf("stats for an unknown peer must not create it : %d peers", len(r.peers))
	}
	hdr, _ := parsePeerHdr(h)
	p := r.peerGet(&hdr)
	if err := c.statsReport(r, body, testNow); err != nil {
		t.Fatal(err)
	}
	if p.bmpStats["rejected_prefixes"] != 5 || p.bmpStats["adj_rib_in_routes"] != 1<<32+2 || p.bmpStats["adj_rib_in_routes_2_1"] != 9 {
		t.Fatalf("stats %v", p.bmpStats)
	}
	if err := c.statsReport(r, body[:len(body)-3], testNow); err == nil {
		t.Fatal("short tlv must fail")
	}
}

// sdNotify (0.2.0) : NOTIFY_SOCKET 의 unixgram 소켓에 한 줄
func TestSdNotify(t *testing.T) {
	path := t.TempDir() + "/notify"
	ln, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	t.Setenv("NOTIFY_SOCKET", path)
	sdNotify("WATCHDOG=1")
	buf := make([]byte, 64)
	ln.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := ln.Read(buf)
	if err != nil || string(buf[:n]) != "WATCHDOG=1" {
		t.Fatalf("got %q %v", buf[:n], err)
	}
}

// settled : End-of-RIB 없이 Peer Up 뒤 5 분, 새 프리픽스 없이 5 분 (같은 프리픽스 add / del 은 늘어난 것이 아님)
func TestSettled(t *testing.T) {
	t0 := testNow
	p := &peer{up: true, gotUp: true, famV4: true, since: t0}
	pfx := func(s string) []prefix {
		x := netip.MustParsePrefix(s)
		return []prefix{{addr: x.Addr(), length: x.Bits()}}
	}
	p.apply(viewInPre, &update{v4Add: pfx("192.0.2.0/24")}, t0.Add(time.Minute), allOn)
	if p.settled(t0.Add(4 * time.Minute)) {
		t.Fatal("Peer Up 뒤 5 분 전")
	}
	if p.settled(t0.Add(5*time.Minute + 59*time.Second)) {
		t.Fatal("새 프리픽스 뒤 5 분 전")
	}
	if !p.settled(t0.Add(6 * time.Minute)) {
		t.Fatal("새 프리픽스 뒤 5 분")
	}
	p.apply(viewInPre, &update{v4Add: pfx("192.0.2.0/24")}, t0.Add(7*time.Minute), allOn) // 이미 있는 것 : 그대로
	p.apply(viewInPre, &update{v4Del: pfx("192.0.2.0/24")}, t0.Add(7*time.Minute), allOn)
	if !p.settled(t0.Add(8 * time.Minute)) {
		t.Fatal("바꾸기 / 지우기는 늘어난 것이 아님")
	}
	p.apply(viewInPre, &update{v4Add: pfx("198.51.100.0/24")}, t0.Add(9*time.Minute), allOn)
	if p.settled(t0.Add(10 * time.Minute)) {
		t.Fatal("새 프리픽스 뒤 1 분")
	}
	inPre(p).eorV4 = t0.Add(10 * time.Minute) // End-of-RIB 면 바로
	if !p.settled(t0.Add(10 * time.Minute)) {
		t.Fatal("synced")
	}
	p.markDown(t0.Add(11*time.Minute), "x")
	if p.settled(t0.Add(30 * time.Minute)) {
		t.Fatal("down")
	}
}

// bmp_raw 값 : 메시지 앞 빈 자리에 머리를 써 넣은 것 = 예전 (머리 + 메시지 복사) 과 같은 바이트, 해석용 slice 는 그대로
func TestRawValue(t *testing.T) {
	h := newRawHead("c0ffee", "deadbeef", "192.0.2.1")
	for _, n := range []int{6, 999, 1000, 1 << 20} {
		msg := make([]byte, n)
		for i := range msg {
			msg[i] = byte(i)
		}
		full := make([]byte, h.room+n) // msgReader.next 와 같은 모양
		copy(full[h.room:], msg)
		got := h.value(full)
		want := fmt.Sprintf("V: 1.7\nC_HASH_ID: c0ffee\nR_HASH: deadbeef\nR_IP: 192.0.2.1\nL: %d\n\n%s", n, msg)
		if string(got) != want || string(full[h.room:]) != string(msg) {
			t.Fatalf("n %d : %q", n, got[:60])
		}
	}
}
