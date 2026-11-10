package main

import (
	"encoding/json"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
)

// Add-Path 표 : 같은 프리픽스에 path ID 마다, 같은 ID 는 바뀜, 철회는 그 ID 만, 마지막이 빠지면 프리픽스도
func TestAddPathTable(t *testing.T) {
	var tb ribTable[key4]
	k := k4(netip.MustParseAddr("203.0.113.0"), 24)
	tb.setPath(k, 1, route{t: 10})
	tb.setPath(k, 2, route{t: 20})
	tb.setPath(k, 1, route{t: 11}) // id 1 만 바뀜
	if tb.size() != 1 || tb.paths() != 2 {
		t.Fatalf("size %d paths %d", tb.size(), tb.paths())
	}
	ps := tb.lookup(k)
	if len(ps) != 2 || ps[0].id != 1 || ps[0].rt.t != 11 || ps[1].id != 2 || !ps[0].addPath {
		t.Fatalf("paths %+v", ps)
	}
	tb.delPath(k, 9) // 없는 id
	tb.delPath(k, 2)
	if tb.size() != 1 || tb.paths() != 1 {
		t.Fatalf("after del 2 : size %d paths %d", tb.size(), tb.paths())
	}
	tb.delPath(k, 1)
	if tb.size() != 0 || tb.paths() != 0 || tb.lookup(k) != nil {
		t.Fatalf("after del 1 : size %d paths %d", tb.size(), tb.paths())
	}
	n := 0
	tb.set(k4(netip.MustParseAddr("198.51.100.0"), 24), route{})
	tb.setPath(k, 5, route{})
	for range tb.all() {
		n++
	}
	if n != 2 {
		t.Fatalf("all %d", n)
	}
}

// 실제 UPDATE 바이트 (Add-Path NLRI : path ID 4 바이트 + 길이 + 프리픽스) -> peer 경로표 / API
func TestAddPathUpdate(t *testing.T) {
	// ORIGIN igp, AS_PATH 64496, NEXT_HOP 192.0.2.1, NLRI : id 1 203.0.113.0/24, id 2 203.0.113.0/24
	body := mustHex(t, "0000"+"0014"+"40010100"+"400206020100"+"00fbf0"+"400304c0000201"+
		"00000001"+"18cb0071"+"00000002"+"18cb0071")
	u, err := parseUpdate(body, pathOpts{as4: true, addPathV4: true})
	if err != nil || len(u.v4Add) != 2 || u.v4Add[1].pathID != 2 {
		t.Fatalf("parse %v %+v", err, u)
	}
	c := newCollector("x")
	r := c.routerFor("192.0.2.1", "")
	p := r.peerGet(&peerHdr{addr: netip.MustParseAddr("198.51.100.1"), as: 64496})
	p.up, p.addPathV4 = true, true
	p.apply(viewInPre, u, testNow, allOn)
	if rv := p.view(-1); rv.rib4.size() != 1 || rv.rib4.paths() != 2 {
		t.Fatalf("size %d paths %d", rv.rib4.size(), rv.rib4.paths())
	}
	rows := match(r, p, -1, netip.MustParsePrefix("203.0.113.0/24"), "exact")
	if len(rows) != 2 {
		t.Fatalf("rows %d", len(rows))
	}
	sortRows(rows)
	b, _ := json.Marshal(rows[1].json())
	if !strings.Contains(string(b), `"path_id":2`) {
		t.Fatalf("json %s", b)
	}
	if j := c.peerView(r, p, false); j.RoutesV4 != 1 || j.PathsV4 != 2 {
		t.Fatalf("peer routes %d paths %d", j.RoutesV4, j.PathsV4)
	}
	// 철회 id 1 : 프리픽스는 남음
	u.v4Del, u.v4Add = u.v4Add[:1], nil
	p.apply(viewInPre, u, testNow, allOn)
	if rv := p.view(-1); rv.rib4.size() != 1 || rv.rib4.paths() != 1 {
		t.Fatalf("after withdraw id 1 : size %d paths %d", rv.rib4.size(), rv.rib4.paths())
	}
	// Add-Path 아닌 peer 의 줄에는 path_id 가 없음
	q := r.peerGet(&peerHdr{addr: netip.MustParseAddr("198.51.100.2"), as: 64497})
	q.up = true
	q.apply(viewInPre, &update{attrs: u.attrs, v4Add: []prefix{{addr: netip.MustParseAddr("203.0.113.0"), length: 24}}}, testNow, allOn)
	b, _ = json.Marshal(match(r, q, -1, netip.MustParsePrefix("203.0.113.0/24"), "exact")[0].json())
	if strings.Contains(string(b), "path_id") || c.peerView(r, q, false).PathsV4 != 0 {
		t.Fatalf("non Add-Path : %s", b)
	}
}

// 상태 파일 : 형식 2 는 Add-Path 경로를 왕복, 형식 1 은 못 담아 오류
func TestStateAddPath(t *testing.T) {
	c := newCollector("x")
	r := c.routerFor("192.0.2.1", "")
	p := r.peerGet(&peerHdr{addr: netip.MustParseAddr("198.51.100.1"), as: 64496})
	p.up, p.addPathV4, p.addPathV6 = true, true, true
	a := attrs{asPath: " 64496", originAS: 64496, nextHop: "192.0.2.1"}
	pf := func(s string, id uint32) prefix {
		x := netip.MustParsePrefix(s)
		return prefix{addr: x.Addr(), length: x.Bits(), pathID: id}
	}
	p.apply(viewInPre, &update{attrs: a, v4Add: []prefix{pf("203.0.113.0/24", 1), pf("203.0.113.0/24", 7), pf("198.51.100.0/24", 0)},
		v6Add: []prefix{pf("2001:db8::/32", 3)}}, testNow, allOn)
	dir := t.TempDir()
	if _, _, err := writeHandover(filepath.Join(dir, "1.state"), &handoverState{Version: 1}, c); err == nil {
		t.Fatal("Add-Path routes in format 1 must fail")
	}
	path := filepath.Join(dir, "2.state")
	if _, n, err := writeHandover(path, &handoverState{Version: 2}, c); err != nil || n != 4 {
		t.Fatal(n, err)
	}
	hs, err := readHandover(path)
	if err != nil {
		t.Fatal(err)
	}
	c2 := newCollector("x")
	hs.restore(c2)
	hs.loadRIBs(c2)
	q := c2.routers["192.0.2.1"].peers[p.key].view(-1)
	ps := q.rib4.lookup(k4(netip.MustParseAddr("203.0.113.0"), 24))
	if q.rib4.size() != 2 || q.rib4.paths() != 3 || q.rib6.paths() != 1 || len(ps) != 2 || ps[1].id != 7 {
		t.Fatalf("size %d paths %d v6 %d %+v", q.rib4.size(), q.rib4.paths(), q.rib6.paths(), ps)
	}
}
