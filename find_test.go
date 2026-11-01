package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"
	"unique"
)

// TestFind : findMany (more 를 한 번에 훑기, limit 을 줄여 가며 셈) 가 하나씩 찾은 것과 같은지, origin 필터
func TestFind(t *testing.T) {
	c := newCollector("x")
	rnd := rand.New(rand.NewSource(1))
	attr := func(path string, origin uint32) unique.Handle[pathAttr] {
		set := ""
		if strings.HasSuffix(path, "}") { // AS_SET 으로 끝나는 경로 : origin 은 묶음
			set, origin = path[strings.Index(path, "{"):], 0
		}
		return unique.Make(pathAttr{asPath: path, originAS: origin, originSet: set})
	}
	var all []ribRow // 모든 경로 (하나씩 비교용)
	for ri := 0; ri < 3; ri++ {
		r := c.routerFor(fmt.Sprintf("192.0.2.%d", ri+1), "")
		for pi := 0; pi < 4; pi++ {
			h := peerHdr{addr: netip.AddrFrom4([4]byte{10, 0, byte(ri), byte(pi)}), as: 65000 + uint32(pi)}
			p := r.peerGet(&h)
			p.up = true
			p.rib4, p.rib6 = map[key4]route{}, map[key6]route{}
			for i := 0; i < 3000; i++ {
				n := 8 + rnd.Intn(17)
				a := netip.AddrFrom4([4]byte{byte(1 + rnd.Intn(3)), byte(rnd.Intn(256)), byte(rnd.Intn(256)), 0})
				pf, _ := a.Prefix(n)
				var at unique.Handle[pathAttr]
				switch rnd.Intn(10) {
				case 0:
					at = attr("", 0) // iBGP : origin = peer AS
				case 1:
					at = attr("3356 {64500,64496}", 64496) // AS_SET : 모름
				default:
					o := uint32(64500 + rnd.Intn(3))
					at = attr(fmt.Sprintf("3356 %d", o), o)
				}
				p.rib4[k4(pf.Addr(), n)] = route{a: at, t: uint32(i)}
			}
			v6, _ := netip.MustParseAddr("2001:db8::").Prefix(32)
			p.rib6[k6(v6.Addr(), 32)] = route{a: attr("64500", 64500)}
			for k, v := range p.rib4 {
				all = append(all, ribRow{router: r.ip, peer: p.ip.String(), peerAS: p.as, pfx: k.prefix(), rt: v})
			}
		}
	}
	list, _ := c.peersOf("", "")
	var qs []findQuery
	for _, s := range []string{"1.0.0.0/8", "2.16.0.0/12", "3.200.0.0/16", "1.2.3.0/24", "2001:db8::/32", "1.0.0.0/8"} {
		p, _, _ := parseQuery(s)
		qs = append(qs, findQuery{text: s, p: p, how: "more"})
	}
	f, _ := newOriginFilter(nil, []uint32{64500})
	start := time.Now()
	res := c.findMany(list, qs, 7, f)
	t.Logf("findMany %v", time.Since(start))
	for i, q := range qs {
		want := 0
		for _, x := range all {
			if x.pfx.Bits() >= q.p.Bits() && q.p.Contains(x.pfx.Addr()) && f.keep(&x) {
				want++
			}
		}
		if q.p.Addr().Is6() {
			want = 0 // 64500 은 빠짐
		}
		if res[i].total != want || len(res[i].rows) != min(7, want) {
			t.Errorf("%s : total %d rows %d, want %d", q.text, res[i].total, len(res[i].rows), want)
		}
		for _, x := range res[i].rows {
			if o, ok := rowOrigin(&x); ok && o == 64500 {
				t.Errorf("%s : 64500 not removed %v", q.text, x.pfx)
			}
		}
		t.Logf("%s : total %d", q.text, res[i].total)
	}
	// limit 없이 찾은 앞 7 줄 = limit 7 의 결과 (줄여 가며 세도 같음)
	full := c.findMany(list, qs[:1], 0, f)[0]
	for i := 0; i < 7; i++ {
		if full.rows[i].pfx != res[0].rows[i].pfx || full.rows[i].peer != res[0].rows[i].peer {
			t.Fatalf("row %d differs", i)
		}
	}
	// 필터 : AS_SET 은 not_origin 에 남고 origin_as 에는 안 맞음, 빈 as_path = peer AS
	set := ribRow{peerAS: 1, rt: route{a: attr("3356 {64500,64496}", 64496)}}
	ibgp := ribRow{peerAS: 64500, rt: route{a: attr("", 0)}}
	in, _ := newOriginFilter([]uint32{64496}, nil)
	if !f.keep(&set) || in.keep(&set) || f.keep(&ibgp) {
		t.Error("filter")
	}
	if _, err := newOriginFilter([]uint32{1}, []uint32{2}); err == nil {
		t.Error("both accepted")
	}
	if _, err := parseASList("not_origin", "64500,AS1"); err == nil {
		t.Error("bad AS accepted")
	}
}

// TestListFilter : 경로 목록의 offset / path_as / p + match / X-Total / X-Applied
func TestListFilter(t *testing.T) {
	c := newCollector("x")
	r := c.routerFor("192.0.2.1", "")
	a1 := unique.Make(pathAttr{asPath: " 3356 64500", originAS: 64500})
	a2 := unique.Make(pathAttr{asPath: " 3356 {64500,64496}", originSet: "{64500,64496}"})
	for pi := 0; pi < 2; pi++ {
		h := peerHdr{addr: netip.AddrFrom4([4]byte{10, 0, 0, byte(pi)}), as: 65000}
		p := r.peerGet(&h)
		p.rib4, p.rib6 = map[key4]route{}, map[key6]route{}
		for i := 0; i < 20; i++ {
			a := a1
			if i%5 == 0 {
				a = a2
			}
			p.rib4[k4(netip.AddrFrom4([4]byte{1, 2, byte(i), 0}), 24)] = route{a: a}
		}
		p.rib4[k4(netip.AddrFrom4([4]byte{1, 2, 0, 0}), 16)] = route{a: a1}
		p.rib6[k6(netip.MustParseAddr("2001:db8::"), 32)] = route{a: a1}
	}
	get := func(peer, q string) (int, []string, string, string) {
		path := "/api/routers/192.0.2.1/prefixes"
		if peer != "" {
			path = "/api/routers/192.0.2.1/peers/" + peer + "/prefixes"
		}
		req := httptest.NewRequest("GET", path+"?format=text&"+q, nil)
		req.SetPathValue("ip", "192.0.2.1")
		w := httptest.NewRecorder()
		if peer != "" {
			req.SetPathValue("peer", peer)
			c.apiPeerPrefixes(w, req)
		} else {
			c.apiRouterPrefixes(w, req)
		}
		lines := strings.Split(strings.TrimSuffix(w.Body.String(), "\n"), "\n")[1:] // 머리 줄 뺌
		var pfx []string
		for _, l := range lines {
			f := strings.Split(l, "\t")
			if len(f) > 4 {
				pfx = append(pfx, f[2]+" "+f[4])
			}
		}
		return w.Code, pfx, w.Header().Get("X-Total"), w.Header().Get("X-Applied")
	}
	// offset / limit 으로 나눠 받은 것을 이으면 전체와 같음 (peer 경계를 넘어도)
	_, all, total, _ := get("", "")
	if len(all) != 44 || total != "44" {
		t.Fatalf("all %d total %s", len(all), total)
	}
	var paged []string
	for off := 0; off < 50; off += 7 {
		_, rows, tot, _ := get("", fmt.Sprintf("offset=%d&limit=7", off))
		if tot != "44" {
			t.Fatalf("offset %d total %s", off, tot)
		}
		paged = append(paged, rows...)
	}
	if strings.Join(paged, ",") != strings.Join(all, ",") {
		t.Fatalf("paged differs")
	}
	if _, rows, tot, _ := get("", "offset=40"); len(rows) != 4 || tot != "44" {
		t.Fatalf("offset only : %d %s", len(rows), tot)
	}
	// path_as : AS_SET 안도 (64496 은 묶음 안에만)
	if _, rows, tot, ap := get("10.0.0.1", "path_as=64496"); len(rows) != 4 || tot != "4" || ap != "path_as,format" {
		t.Fatalf("path_as : %d %s %q", len(rows), tot, ap)
	}
	// p + match (peer 하나 안에서)
	for _, k := range []struct {
		q    string
		want int
	}{{"p=1.2.3.0/24", 1}, {"p=1.2.3.9", 1}, {"p=1.2.3.0/24&match=covering", 2}, {"p=1.2.0.0/16&match=more", 21},
		{"p=1.2.0.0/16&match=more&afi=6", 0}, {"p=9.9.9.9", 0}, {"p=1.2.0.0/16&match=more&not_origin=64500&offset=2", 2}} {
		if _, rows, tot, _ := get("10.0.0.0", k.q); len(rows) != k.want {
			t.Errorf("%s : %d rows (total %s), want %d", k.q, len(rows), tot, k.want)
		}
	}
	if _, _, _, ap := get("10.0.0.0", "afi=4&p=1.2.0.0/16&match=more&origin_as=64500&path_as=3356&offset=1&limit=2"); ap != "afi,p,match,origin_as,path_as,offset,limit,format" {
		t.Errorf("X-Applied %q", ap)
	}
	// distinct : 같은 필터로 고른 뒤 프리픽스로 묶음 (64496 = i%5==0 인 /24 넷, peer 둘 다), offset / limit 은 묶은 것에
	req := httptest.NewRequest("GET", "/api/routers/192.0.2.1/prefixes?distinct=1&format=text&path_as=64496&offset=1&limit=2", nil)
	req.SetPathValue("ip", "192.0.2.1")
	w := httptest.NewRecorder()
	c.apiRouterPrefixes(w, req)
	if body := w.Body.String(); body != "prefix\tpeers\n1.2.5.0/24\t2\n1.2.10.0/24\t2\n" || w.Header().Get("X-Total") != "4" ||
		w.Header().Get("X-Applied") != "path_as,offset,limit,format,distinct" {
		t.Errorf("distinct %q total %s applied %s", body, w.Header().Get("X-Total"), w.Header().Get("X-Applied"))
	}
	for _, q := range []string{"offset=-1", "offset=x", "match=more", "p=1.2.0.0/16&match=near", "path_as=AS1", "p=zzz"} {
		if code, _, _, _ := get("10.0.0.0", q); code != 400 {
			t.Errorf("%s : %d, want 400", q, code)
		}
	}
}

// TestStatsAPI : 1 분 통계가 /api/stats 의 minutes 에 쌓임
func TestStatsAPI(t *testing.T) {
	c := newCollector("x")
	c.safeProcess(c.routerFor("1.2.3.4", ""), mustHex(t, "0300000033020000000000000000000000000000000000000000000001020305"+"00003b418efb31080000000000000000"+"020007"), testNow)
	c.logStat(nil)
	w := httptest.NewRecorder()
	c.apiStats(w, httptest.NewRequest("GET", "/api/stats", nil))
	var j statsJSON
	if err := json.Unmarshal(w.Body.Bytes(), &j); err != nil || len(j.Minutes) == 0 {
		t.Fatalf("%v %s", err, w.Body.String())
	}
	m := j.Minutes[len(j.Minutes)-1]
	if m.Routers != 1 || m.Counts["peer_down"] != 1 || m.Counts["peer_msgs"] != 1 || m.Raw != nil || j.Kafka != nil {
		t.Errorf("%+v", m)
	}
}
