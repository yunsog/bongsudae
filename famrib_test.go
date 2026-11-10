package main

import (
	"encoding/json"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
)

// vpnUpdate : VPNv4 경로 (RD 종류 0 = 64496:rdn, 라벨) 를 add 또는 del 로
func vpnUpdate(t *testing.T, rdn uint32, pfx string, label uint32, del bool) *update {
	x := netip.MustParsePrefix(pfx)
	var rd [8]byte
	rd[2], rd[3] = 0xfb, 0xf0 // AS 64496
	rd[4], rd[5], rd[6], rd[7] = byte(rdn>>24), byte(rdn>>16), byte(rdn>>8), byte(rdn)
	r := famRoute{pfx: prefix{addr: x.Addr(), length: x.Bits()}, rd: rd, labels: []uint32{label}}
	u := &update{attrs: attrs{asPath: " 64496 64500", originAS: 64500, origin: "igp"}}
	fu := famUpdate{f: famOf(afiIPv4, 128), nextHop: "192.0.2.9"}
	if del {
		fu.del = []famRoute{r}
	} else {
		fu.add = []famRoute{r}
	}
	u.fams = []famUpdate{fu}
	return u
}

func vpnCollector(rib string) (*collector, *router, *peer) {
	c := newCollector("x")
	fams, _ := parseRIBFamilies(rib)
	c.setRIB(fams)
	r := c.routerFor("192.0.2.1", "")
	p := r.peerGet(&peerHdr{addr: netip.MustParseAddr("198.51.100.1"), as: 64496})
	p.up, p.gotUp = true, true
	return c, r, p
}

// VPNv4 경로표 : 같은 프리픽스도 RD 가 다르면 따로, 철회는 그 RD 만, 라벨은 속성에. 기본 (vpnv4 꺼짐) 이면 넣지 않음
func TestFamRIB(t *testing.T) {
	c, r, p := vpnCollector("ipv4-unicast,ipv6-unicast")
	p.apply(viewInPre, vpnUpdate(t, 1, "203.0.113.0/24", 17, false), testNow, c.on)
	if p.view(-1).fam != nil {
		t.Fatal("vpnv4 off : must not keep")
	}
	c, r, p = vpnCollector("ipv4-unicast,vpnv4")
	p.apply(viewInPre, vpnUpdate(t, 1, "203.0.113.0/24", 17, false), testNow, c.on)
	p.apply(viewInPre, vpnUpdate(t, 2, "203.0.113.0/24", 18, false), testNow, c.on)
	p.apply(viewInPre, vpnUpdate(t, 1, "203.0.113.128/25", 19, false), testNow, c.on)
	p.apply(viewInPre, vpnUpdate(t, 2, "203.0.113.0/24", 0, true), testNow, c.on)
	rows := p.view(-1).famRows(famOf(afiIPv4, 128))
	if len(rows) != 2 || rdString(rows[0].rd) != "64496:1" || rows[0].p.rt.a.Value().labels != "17" || rows[1].pfx.String() != "203.0.113.128/25" {
		t.Fatalf("%+v", rows)
	}
	if j := c.peerView(r, p, false); j.FamRoutes["vpnv4"] != 2 || j.RoutesV4 != 0 {
		t.Fatalf("peer %+v", j.FamRoutes)
	}
	q := netip.MustParsePrefix("203.0.113.130/32")
	if m := famMatch(r, p, -1, famOf(afiIPv4, 128), q, "longest"); len(m) != 1 || m[0].pfx.Bits() != 25 || m[0].rd != "64496:1" {
		t.Fatalf("longest %+v", m)
	}
	if m := famMatch(r, p, -1, famOf(afiIPv4, 128), netip.MustParsePrefix("203.0.113.0/24"), "more"); len(m) != 2 {
		t.Fatalf("more %d", len(m))
	}
	b, _ := json.Marshal(famMatch(r, p, -1, famOf(afiIPv4, 128), netip.MustParsePrefix("203.0.113.0/24"), "exact")[0].json())
	if !strings.Contains(string(b), `"rd":"64496:1"`) || !strings.Contains(string(b), `"labels":"17"`) {
		t.Fatal(string(b))
	}
	// 목록 API (family=vpnv4) : peerScan
	f, err := newListFilter(url.Values{"family": {"vpnv4"}})
	if err != nil || f.fam != famOf(afiIPv4, 128) {
		t.Fatal(err)
	}
	n := f.peerScan(routerPeer{r, p}, 0, 100, func(*ribRow) {})
	if n != 2 {
		t.Fatalf("list %d", n)
	}
	if _, err := newListFilter(url.Values{"family": {"l2vpn-evpn"}}); err == nil {
		t.Fatal("family without tables must fail")
	}
	// 꺼진 종류 = 409
	c2, _, _ := vpnCollector("ipv4-unicast")
	w := httptest.NewRecorder()
	c2.apiPrefix(w, httptest.NewRequest("GET", "/api/prefix?p=203.0.113.0/24&family=vpnv4", nil))
	if w.Code != 409 {
		t.Fatalf("off family : %d %s", w.Code, w.Body)
	}
	// 글자 응답 (format=text) : family= 면 끝에 rd / labels / rule 열, unicast 는 그대로
	w = httptest.NewRecorder()
	c.apiPrefix(w, httptest.NewRequest("GET", "/api/prefix?p=203.0.113.0/24&match=more&family=vpnv4&format=text", nil))
	lines := strings.Split(strings.TrimSpace(w.Body.String()), "\n")
	if w.Code != 200 || len(lines) != 3 || !strings.HasSuffix(lines[0], "\tsince\trd\tlabels\trule") ||
		!strings.HasSuffix(lines[1], "\t64496:1\t17\t") || len(strings.Split(lines[1], "\t")) != len(strings.Split(lines[0], "\t")) {
		t.Fatalf("%d %q", w.Code, lines)
	}
	w = httptest.NewRecorder()
	c.apiPrefix(w, httptest.NewRequest("GET", "/api/prefix?p=203.0.113.0/24&format=text", nil))
	if head, _, _ := strings.Cut(w.Body.String(), "\n"); !strings.HasSuffix(head, "\tsince") {
		t.Fatalf("unicast head %q", head)
	}
}

// 상태 파일 형식 2 : 종류 표와 라벨이 왕복, 형식 1 은 못 담아 오류
func TestStateFamRIB(t *testing.T) {
	c, _, p := vpnCollector("ipv4-unicast,vpnv4")
	p.apply(viewInPre, vpnUpdate(t, 1, "203.0.113.0/24", 17, false), testNow, c.on)
	p.apply(viewInPre, vpnUpdate(t, 2, "198.51.100.0/24", 18, false), testNow, c.on)
	dir := t.TempDir()
	if _, _, err := writeHandover(filepath.Join(dir, "1.state"), &handoverState{Version: 1}, c); err == nil {
		t.Fatal("family tables in format 1 must fail")
	}
	path := filepath.Join(dir, "2.state")
	if _, n, err := writeHandover(path, &handoverState{Version: 2}, c); err != nil || n != 2 {
		t.Fatal(n, err)
	}
	hs, err := readHandover(path)
	if err != nil {
		t.Fatal(err)
	}
	c2, _, _ := vpnCollector("ipv4-unicast,vpnv4")
	c2.routers = map[string]*router{}
	hs.restore(c2)
	hs.loadRIBs(c2)
	rows := c2.routers["192.0.2.1"].peers[p.key].view(-1).famRows(famOf(afiIPv4, 128))
	if len(rows) != 2 || rows[0].p.rt.a.Value().labels != "17" || rdString(rows[1].rd) != "64496:2" {
		t.Fatalf("%+v", rows)
	}
	// 새 판에서 vpnv4 를 끄면 넘겨받은 표를 버림
	c3, _, _ := vpnCollector("ipv4-unicast")
	c3.routers = map[string]*router{}
	writeHandover(path, &handoverState{Version: 2}, c)
	hs, _ = readHandover(path)
	hs.restore(c3)
	hs.loadRIBs(c3)
	if c3.routers["192.0.2.1"].peers[p.key].view(-1).fam != nil {
		t.Fatal("off family must be dropped on load")
	}
}
