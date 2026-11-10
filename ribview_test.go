package main

import (
	"net/netip"
	"path/filepath"
	"testing"
	"time"
	"unique"
)

// allOn : 시험용 = 경로표를 모두 켬
var allOn = ribOn{v4: true, v6: true}

// inPre : 시험용 = peer 의 In pre-policy 보기 (없으면 만듦)
func inPre(p *peer) *ribView { return p.viewFor(viewInPre) }

// per-peer 머리 -> 보기
func TestViewOf(t *testing.T) {
	for _, c := range []struct {
		typ, flags uint8
		want       int
	}{
		{0, 0, viewInPre}, {0, flagL, viewInPost}, {0, flagO, viewOutPre}, {0, flagO | flagL, viewOutPost},
		{1, flagV | flagL, viewInPost}, {peerTypeLocRIB, 0, viewLocRIB}, {peerTypeLocRIB, 0x80, viewLocRIB}, // Loc-RIB 의 0x80 = F (filtered)
	} {
		if got := viewOf(c.typ, c.flags); got != c.want {
			t.Errorf("typ %d flags %#x : %s, want %s", c.typ, c.flags, viewNames[got], viewNames[c.want])
		}
	}
	if v, err := parseView("out-post"); err != nil || v != viewOutPost {
		t.Fatal(v, err)
	}
	if v, err := parseView(""); err != nil || v != -1 {
		t.Fatal(v, err)
	}
	if _, err := parseView("post"); err == nil {
		t.Fatal("bad view must fail")
	}
}

// 같은 네이버의 보기는 따로 : post-policy 에서 지워도 pre-policy 경로는 남고, 경로 수 / API 가 보기마다
func TestViewsSeparate(t *testing.T) {
	c := newCollector("x")
	r := c.routerFor("192.0.2.1", "")
	p := r.peerGet(&peerHdr{addr: netip.MustParseAddr("198.51.100.1"), as: 64500})
	p.up, p.gotUp, p.famV4 = true, true, true
	pfx := func(s string) []prefix {
		x := netip.MustParsePrefix(s)
		return []prefix{{addr: x.Addr(), length: x.Bits()}}
	}
	lp100 := &update{attrs: attrs{asPath: " 64500", originAS: 64500, nextHop: "198.51.100.1", localPref: 100}, v4Add: pfx("203.0.113.0/24")}
	lp200 := &update{attrs: attrs{asPath: " 64500", originAS: 64500, nextHop: "198.51.100.1", localPref: 200}, v4Add: pfx("203.0.113.0/24")}
	p.apply(viewInPre, lp100, testNow, allOn)
	p.apply(viewInPost, lp200, testNow, allOn)
	p.apply(viewInPost, &update{v4Del: pfx("203.0.113.0/24")}, testNow, allOn)
	if p.view(viewInPre).rib4.size() != 1 || p.view(viewInPost).rib4.size() != 0 || p.viewCount() != 2 {
		t.Fatalf("pre %d post %d views %d", p.view(viewInPre).rib4.size(), p.view(viewInPost).rib4.size(), p.viewCount())
	}
	rt, _ := p.view(viewInPre).rib4.get(k4(netip.MustParseAddr("203.0.113.0"), 24))
	if rt.a.Value().localPref != 100 {
		t.Fatalf("pre-policy route overwritten : local-pref %d", rt.a.Value().localPref)
	}
	p.gotEoR(viewInPost, &update{eorV4: true}, testNow)
	if p.synced() || !p.syncedIn(p.view(viewInPost)) { // 주 보기 (In pre) 는 아직 EoR 없음
		t.Fatal("synced must be per view")
	}
	// API : 주 보기 = in-pre, 보기가 둘이라 views 목록
	j := c.peerView(r, p, false)
	if j.View != "in-pre" || j.RoutesV4 != 1 || len(j.Views) != 2 || j.Views[1].View != "in-post" || !j.Views[1].Synced {
		t.Fatalf("peer json %+v", j)
	}
	// 찾기 : view 를 주면 그 보기만
	q := netip.MustParsePrefix("203.0.113.0/24")
	if n := len(match(r, p, -1, q, "exact")); n != 1 {
		t.Fatalf("main view : %d", n)
	}
	if n := len(match(r, p, viewInPost, q, "exact")); n != 0 {
		t.Fatalf("in-post : %d", n)
	}
	if n := len(match(r, p, viewOutPre, q, "exact")); n != 0 { // 받은 적 없는 보기 = 비어 있음 (만들지 않음)
		t.Fatalf("out-pre : %d", n)
	}
	if p.viewCount() != 2 {
		t.Fatal("reading a view must not create it")
	}
	// 세션이 끝나면 보기 모두 비움
	p.markDown(testNow, "x")
	if p.viewCount() != 0 {
		t.Fatal("views must be cleared on down")
	}
}

// 상태 파일 : 형식 2 는 보기마다 왕복, 형식 1 (0.2) 은 보기 하나 = 마지막 머리의 보기로 읽음, 보기가 둘인데 형식 1 로 쓰면 오류
func TestStateViews(t *testing.T) {
	c := newCollector("x")
	r := c.routerFor("192.0.2.1", "")
	a := attrs{asPath: " 64500", originAS: 64500, nextHop: "198.51.100.1"}
	one := func(s string) []prefix {
		x := netip.MustParsePrefix(s)
		return []prefix{{addr: x.Addr(), length: x.Bits()}}
	}
	p := r.peerGet(&peerHdr{addr: netip.MustParseAddr("198.51.100.1"), as: 64500})
	p.up = true
	p.apply(viewInPre, &update{attrs: a, v4Add: one("203.0.113.0/24")}, testNow, allOn)
	p.apply(viewOutPost, &update{attrs: a, v6Add: one("2001:db8::/32")}, testNow.Add(time.Minute), allOn)
	inPre(p).grewAt = 42
	dir := t.TempDir()

	// 형식 2 왕복
	path := filepath.Join(dir, "v2.state")
	if _, n, err := writeHandover(path, &handoverState{Version: 2}, c); err != nil || n != 2 {
		t.Fatal(n, err)
	}
	c2 := newCollector("x")
	hs, err := readHandover(path)
	if err != nil {
		t.Fatal(err)
	}
	hs.restore(c2)
	hs.loadRIBs(c2)
	q := c2.routers["192.0.2.1"].peers[p.key]
	if q.viewCount() != 2 || q.view(viewInPre).rib4.size() != 1 || q.view(viewOutPost).rib6.size() != 1 ||
		q.view(viewInPre).grewAt != 42 || !q.view(viewOutPost).lastUpdate.Equal(testNow.Add(time.Minute)) {
		t.Fatalf("format 2 : views %d pre %d out %d grew %d", q.viewCount(), q.view(viewInPre).rib4.size(), q.view(viewOutPost).rib6.size(), q.view(viewInPre).grewAt)
	}

	// 형식 1 로는 보기 둘을 못 씀
	if _, _, err := writeHandover(filepath.Join(dir, "x.state"), &handoverState{Version: 1}, c); err == nil {
		t.Fatal("two views in format 1 must fail")
	}

	// 형식 1 (0.2 가 쓴 것) : 보기 하나 -> 마지막 머리의 보기 (In pre)
	p.views[viewOutPost] = nil
	path1 := filepath.Join(dir, "v1.state")
	if _, n, err := writeHandover(path1, &handoverState{Version: 1}, c); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	c3 := newCollector("x")
	hs1, err := readHandover(path1)
	if err != nil {
		t.Fatal(err)
	}
	hs1.restore(c3)
	hs1.loadRIBs(c3)
	q = c3.routers["192.0.2.1"].peers[p.key]
	rt, ok := q.view(viewInPre).rib4.get(k4(netip.MustParseAddr("203.0.113.0"), 24))
	if q.viewCount() != 1 || !ok || rt.a != unique.Make(attrOf(&a, true)) || q.view(-1).adds != 1 {
		t.Fatalf("format 1 : views %d found %v adds %d", q.viewCount(), ok, q.view(-1).adds)
	}
}
