// 프리픽스 찾기 (웹 API) : 하나 (GET) / 여러 개 (POST), origin 필터, total (필터 뒤 limit 전 줄 수)
//
//	GET  /api/prefix?p=&match=&limit=&origin_as=&not_origin=&format=text
//	GET  /api/routers/{ip}/prefix?...                        라우터 하나
//	POST /api/prefix  {"p": [...], "match": "more", "limit": 100, "not_origin": [64500]}   (5000 개까지)
//	POST /api/routers/{ip}/prefix                            라우터 하나
//
// origin 필터 (목록 /prefixes 에도) :
//
//	origin_as=N,N   그 origin 만 / not_origin=N,N  그 origin 은 뺌 (하이재킹 확인). 둘 다 주면 400
//	origin 을 알 수 없는 줄 (as_path 가 AS_SET 으로 끝남) : not_origin 으로 빼지 않음 (의심 줄로 남김), origin_as 에는 안 맞음
//	as_path 가 빈 줄 (iBGP, 우리 AS 안에서 생김) : origin = peer AS
//
// limit 은 필터 뒤에 (프리픽스마다). limit 이 있으면 위 N 줄만 남겨 가며 셈 = 메모리는 limit 만큼, total 은 정확.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

const maxBatch = 5000 // POST 한 번에 찾는 프리픽스 수

// ---- origin 필터 ----

// originFilter : nil = 거르지 않음
type originFilter struct {
	not  bool            // true = not_origin (그 AS 를 뺌)
	set  map[uint32]bool // AS 들
	list []uint32        // 응답에 그대로 보여 줌
}

// newOriginFilter : origin_as / not_origin 중 하나 (둘 다면 오류, 둘 다 없으면 nil)
func newOriginFilter(in, not []uint32) (*originFilter, error) {
	if len(in) > 0 && len(not) > 0 {
		return nil, fmt.Errorf("origin_as 와 not_origin 은 함께 쓸 수 없음")
	}
	f := &originFilter{list: in}
	if len(not) > 0 {
		f.not, f.list = true, not
	}
	if len(f.list) == 0 {
		return nil, nil
	}
	f.set = map[uint32]bool{}
	for _, as := range f.list {
		f.set[as] = true
	}
	return f, nil
}

// parseASList : "64500,64496" -> [64500 64496] (10 진수, 0 ~ 4294967295)
func parseASList(name, s string) ([]uint32, error) {
	if s == "" {
		return nil, nil
	}
	var out []uint32
	for _, t := range strings.Split(s, ",") {
		n, err := strconv.ParseUint(strings.TrimSpace(t), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("%s=%q : AS 번호 (10 진수, 쉼표 구분)", name, s)
		}
		out = append(out, uint32(n))
	}
	return out, nil
}

// filterFromQuery : GET 질의의 origin_as / not_origin
func filterFromQuery(q url.Values) (*originFilter, error) {
	in, err := parseASList("origin_as", q.Get("origin_as"))
	if err != nil {
		return nil, err
	}
	not, err := parseASList("not_origin", q.Get("not_origin"))
	if err != nil {
		return nil, err
	}
	return newOriginFilter(in, not)
}

// rowOrigin : 줄의 origin AS 와 알 수 있는지
func rowOrigin(x *ribRow) (uint32, bool) {
	a := x.rt.a.Value()
	switch {
	case a.asPath == "":
		return x.peerAS, true // iBGP : 우리 AS 안에서 생긴 경로
	case a.originSet != "":
		return 0, false // AS_SET 으로 끝남 : origin 을 하나로 정할 수 없음
	}
	return a.originAS, true
}

// keep : 이 줄을 남기는가
func (f *originFilter) keep(x *ribRow) bool {
	if f == nil {
		return true
	}
	as, known := rowOrigin(x)
	if !known {
		return f.not // not_origin : 의심 줄로 남김, origin_as : 맞다고 할 수 없음
	}
	return f.set[as] != f.not
}

// ---- 찾기 ----

// findQuery : 찾을 프리픽스 하나
type findQuery struct {
	text string // 받은 글자 (응답의 query 는 정리한 프리픽스)
	p    netip.Prefix
	how  string // exact | longest | covering | more
}

// findResult : 프리픽스 하나의 결과
type findResult struct {
	rows  []ribRow
	total int // 필터 뒤, limit 전
}

func validMatch(how string) bool {
	return how == "exact" || how == "longest" || how == "covering" || how == "more"
}

// findMany : 여러 프리픽스를 (라우터, peer) 목록에서 찾음. peer 하나씩 읽기 잠금
func (c *collector) findMany(list []routerPeer, qs []findQuery, limit int, f *originFilter) []findResult {
	res := make([]findResult, len(qs))
	more := newMoreIndex(qs)
	for _, rp := range list {
		r, p := rp.r, rp.p
		start := make([]int, len(qs)) // 이번 peer 에서 더한 줄의 시작
		for i := range res {
			start[i] = len(res[i].rows)
		}
		r.mu.RLock() // 이 라우터의 처리만 잠깐 기다림
		for i, q := range qs {
			if q.how != "more" {
				res[i].rows = append(res[i].rows, match(r, p, q.p, q.how)...)
			}
		}
		more.scan(r, p, res)
		r.mu.RUnlock()
		for i := range res {
			res[i].keepNew(start[i], f, limit)
		}
	}
	for i := range res {
		sortRows(res[i].rows)
		if limit > 0 && len(res[i].rows) > limit {
			res[i].rows = res[i].rows[:limit]
		}
	}
	return res
}

// keepNew : 이번 peer 에서 더한 줄 (from 부터) 을 거르고 셈. limit 의 2 배가 넘으면 정렬해 위 limit 개만 남김
// (위 N 개 = (지금까지의 위 N 개 + 새 줄) 의 위 N 개 이므로 결과는 같음)
func (fr *findResult) keepNew(from int, f *originFilter, limit int) {
	n := from
	for i := from; i < len(fr.rows); i++ {
		if f.keep(&fr.rows[i]) {
			fr.rows[n] = fr.rows[i]
			n++
		}
	}
	fr.rows = fr.rows[:n]
	fr.total += n - from
	if limit > 0 && len(fr.rows) > 2*limit {
		sortRows(fr.rows)
		fr.rows = fr.rows[:limit]
	}
}

// moreIndex : match=more 인 프리픽스들 (프리픽스 -> 몇 번째 질의들), 들어 있는 길이들
type moreIndex struct {
	idx          map[netip.Prefix][]int
	lens4, lens6 []int
}

func newMoreIndex(qs []findQuery) *moreIndex {
	m := &moreIndex{idx: map[netip.Prefix][]int{}}
	for i, q := range qs {
		if q.how != "more" {
			continue
		}
		if m.idx[q.p] == nil {
			if q.p.Addr().Is4() {
				m.lens4 = append(m.lens4, q.p.Bits())
			} else {
				m.lens6 = append(m.lens6, q.p.Bits())
			}
		}
		m.idx[q.p] = append(m.idx[q.p], i)
	}
	slices.Sort(m.lens4)
	m.lens4 = slices.Compact(m.lens4)
	slices.Sort(m.lens6)
	m.lens6 = slices.Compact(m.lens6)
	return m
}

// scan : peer 의 경로표를 한 번 훑어, 경로마다 그것을 덮는 질의 프리픽스 (질의에 있는 길이만) 를 찾음
func (m *moreIndex) scan(r *router, p *peer, res []findResult) {
	if len(m.idx) == 0 {
		return
	}
	base := ribRow{router: r.ip, routerName: r.sysName, peer: p.ip.String(), peerAS: p.as}
	hit := func(x netip.Prefix, rt route, lens []int) {
		for _, n := range lens {
			if n > x.Bits() {
				break
			}
			up, _ := x.Addr().Prefix(n)
			for _, i := range m.idx[up] {
				row := base
				row.pfx, row.rt = x, rt
				res[i].rows = append(res[i].rows, row)
			}
		}
	}
	if len(m.lens4) > 0 {
		for k, v := range p.rib4 {
			hit(k.prefix(), v, m.lens4)
		}
	}
	if len(m.lens6) > 0 {
		for k, v := range p.rib6 {
			hit(k.prefix(), v, m.lens6)
		}
	}
}

// ---- 응답 모양 ----

type findJSON struct {
	Query     string      `json:"query"`
	Match     string      `json:"match"`
	OriginAS  []uint32    `json:"origin_as,omitempty"`
	NotOrigin []uint32    `json:"not_origin,omitempty"`
	Count     int         `json:"count"` // 돌려준 줄 수 (limit 뒤)
	Total     int         `json:"total"` // 필터 뒤, limit 전
	Routes    []routeJSON `json:"routes"`
}

type batchJSON struct {
	Match     string     `json:"match,omitempty"`
	OriginAS  []uint32   `json:"origin_as,omitempty"`
	NotOrigin []uint32   `json:"not_origin,omitempty"`
	Count     int        `json:"count"` // results 의 routes 줄 수 합
	Results   []findJSON `json:"results"`
}

// setFilter : 응답에 필터를 그대로 보여 줌
func setFilter(f *originFilter, in, not *[]uint32) {
	if f == nil {
		return
	}
	if f.not {
		*not = f.list
	} else {
		*in = f.list
	}
}

func (fr *findResult) json(q findQuery, f *originFilter) findJSON {
	j := findJSON{Query: q.p.String(), Match: q.how, Count: len(fr.rows), Total: fr.total, Routes: make([]routeJSON, len(fr.rows))}
	setFilter(f, &j.OriginAS, &j.NotOrigin)
	for i := range fr.rows {
		j.Routes[i] = fr.rows[i].json()
	}
	return j
}

// ---- 처리기 ----

// apiPrefix : GET /api/prefix (모든 라우터), /api/routers/{ip}/prefix (라우터 하나)
func (c *collector) apiPrefix(w http.ResponseWriter, req *http.Request) {
	qv := req.URL.Query()
	p, how, err := parseQuery(qv.Get("p"))
	if err != nil {
		httpErr(w, 400, "p=%q : prefix (1.11.56.0/21) or address (1.11.56.10) : %v", qv.Get("p"), err)
		return
	}
	if m := qv.Get("match"); m != "" {
		how = m
	}
	if !validMatch(how) {
		httpErr(w, 400, "match=%q : exact | longest | covering | more", how)
		return
	}
	f, err := filterFromQuery(qv)
	if err != nil {
		httpErr(w, 400, "%v", err)
		return
	}
	list, ok := c.peersOf(req.PathValue("ip"), "")
	if !ok {
		httpErr(w, 404, "router %s not found", req.PathValue("ip"))
		return
	}
	limit, _ := strconv.Atoi(qv.Get("limit"))
	q := findQuery{text: qv.Get("p"), p: p, how: how}
	fr := c.findMany(list, []findQuery{q}, limit, f)[0]
	if qv.Get("format") == "text" {
		w.Header().Set("X-Total", strconv.Itoa(fr.total)) // 글자 응답의 total
		rw := newRowWriter(w, req)
		for i := range fr.rows {
			rw.row(&fr.rows[i])
		}
		rw.end()
		return
	}
	writeJSON(w, fr.json(q, f))
}

// apiPrefixBatch : POST /api/prefix, /api/routers/{ip}/prefix (여러 프리픽스, 읽기라 key 없음)
func (c *collector) apiPrefixBatch(w http.ResponseWriter, req *http.Request) {
	var body struct {
		P         []string `json:"p"`
		Match     string   `json:"match"`
		Limit     int      `json:"limit"`
		OriginAS  []uint32 `json:"origin_as"`
		NotOrigin []uint32 `json:"not_origin"`
	}
	d := json.NewDecoder(io.LimitReader(req.Body, 2<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(&body); err != nil {
		httpErr(w, 400, "본문 : %v (모양 {\"p\": [...], \"match\": \"more\", \"limit\": 100, \"not_origin\": [64500]})", err)
		return
	}
	if len(body.P) == 0 || len(body.P) > maxBatch {
		httpErr(w, 400, "p : 1 ~ %d 개 (받은 것 %d 개)", maxBatch, len(body.P))
		return
	}
	if body.Match != "" && !validMatch(body.Match) {
		httpErr(w, 400, "match=%q : exact | longest | covering | more", body.Match)
		return
	}
	f, err := newOriginFilter(body.OriginAS, body.NotOrigin)
	if err != nil {
		httpErr(w, 400, "%v", err)
		return
	}
	qs := make([]findQuery, len(body.P))
	for i, s := range body.P {
		p, how, err := parseQuery(s)
		if err != nil {
			httpErr(w, 400, "p[%d] (%d 번째) %q : prefix (1.11.56.0/21) or address (1.11.56.10)", i, i+1, s)
			return
		}
		if body.Match != "" {
			how = body.Match
		}
		qs[i] = findQuery{text: s, p: p, how: how}
	}
	list, ok := c.peersOf(req.PathValue("ip"), "")
	if !ok {
		httpErr(w, 404, "router %s not found", req.PathValue("ip"))
		return
	}
	res := c.findMany(list, qs, body.Limit, f)
	out := batchJSON{Match: body.Match, Results: make([]findJSON, len(qs))}
	setFilter(f, &out.OriginAS, &out.NotOrigin)
	for i := range res {
		out.Results[i] = res[i].json(qs[i], f)
		out.Count += len(res[i].rows)
	}
	writeJSON(w, out)
}
