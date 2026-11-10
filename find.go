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
//
// (11-05) 인증 없는 요청 하나로 메모리를 다 쓰지 않게 :
//   - 요청 하나가 모아 두는 줄은 모든 질의를 합쳐 maxFindRows 까지. 넘으면 (limit 이 있으면 줄여 본 뒤) 바로 멈추고 413 (전체 경로 more 등)
//   - 같은 (프리픽스, match) 질의는 한 번만 찾음 (POST 에 같은 것 5000 개 = 메모리 5000 배였음)
//   - JSON 응답은 줄마다 써 나감 (전에는 응답 전체를 메모리에 만든 뒤 씀)
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

const maxBatch = 5000 // POST 한 번에 찾는 프리픽스 수

// maxFindRows : 찾기 요청 하나가 메모리에 모아 두는 줄 수의 상한 (모든 질의 합, limit 으로 줄이기 전 포함).
// 줄 하나 약 150 B, slice 를 늘리며 옮기는 것과 GC 를 더하면 요청 하나의 메모리는 이것의 약 3 배까지
// (11-05 운영 크기 시험 : 500 만이면 한 요청에 RSS +3.5 GB -> 동시 4 개면 MemoryMax 에 닿을 수 있어 200 만으로).
// limit 이 있으면 상한에 닿을 때마다 위 limit 줄만 남겨 자리를 만드므로 걸리지 않음 (화면이 쓰는 가장 큰 요청 = limit 20 만).
// 시험이 작게 바꾸므로 변수
var maxFindRows = 2_000_000

// errTooMany : 줄이 maxFindRows 를 넘음 (413)
func errTooMany() error {
	return httpError{http.StatusRequestEntityTooLarge, fmt.Sprintf("too many routes (over %d rows held): add limit, use a narrower prefix, "+
		"or download per router (/api/routers/{ip}/prefixes?format=text)", maxFindRows)}
}

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
		return nil, fmt.Errorf("origin_as and not_origin cannot be used together")
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
			return nil, fmt.Errorf("%s=%q : AS numbers (decimal, comma separated)", name, s)
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

// findMany : 여러 프리픽스를 (라우터, peer) 목록의 보기 view (-1 = peer 마다 주 보기) 에서 찾음. peer 하나씩 읽기 잠금.
// 같은 (프리픽스, match) 질의는 한 번만 찾고 결과를 나눠 줌 (rows 는 같은 slice 를 함께 읽기만 함).
// 모아 둔 줄이 maxFindRows 를 넘으면 errTooMany (413)
func (c *collector) findMany(list []routerPeer, qs []findQuery, limit int, f *originFilter, view int, fam family) ([]findResult, error) {
	type key struct {
		p   netip.Prefix
		how string
	}
	first := map[key]int{} // 질의 -> uq 안의 번호
	var uq []findQuery
	back := make([]int, len(qs))
	for i, q := range qs {
		k := key{q.p, q.how}
		j, ok := first[k]
		if !ok {
			j = len(uq)
			first[k] = j
			uq = append(uq, q)
		}
		back[i] = j
	}
	ures, err := c.findUnique(list, uq, limit, f, view, fam)
	if err != nil {
		return nil, err
	}
	res := make([]findResult, len(qs))
	for i, j := range back {
		res[i] = ures[j]
	}
	return res, nil
}

// findUnique : findMany 의 몸통 (질의는 서로 다름)
func (c *collector) findUnique(list []routerPeer, qs []findQuery, limit int, f *originFilter, view int, fam family) ([]findResult, error) {
	res := make([]findResult, len(qs))
	more := newMoreIndex(qs)
	held := 0 // 지금 모아 둔 줄 (모든 질의 합)
	for _, rp := range list {
		r, p := rp.r, rp.p
		start := make([]int, len(qs)) // 이번 peer 에서 더한 줄의 시작
		for i := range res {
			start[i] = len(res[i].rows)
		}
		room := maxFindRows - held // 이번 peer 에서 더 모을 수 있는 줄
		// fit : n 줄을 더할 자리가 있는가. 없고 limit 이 있으면 지금까지 모은 것을 거르고 위 limit 줄만 남겨 자리를 만듦
		// (그 정렬은 읽기 잠금 안 = 그 라우터 처리가 그만큼 기다림. 한 peer 가 상한보다 많은 경로를 가질 때만 생김)
		fit := func(n int) bool {
			if room >= n {
				room -= n
				return true
			}
			if limit <= 0 {
				return false
			}
			h := 0
			for i := range res {
				res[i].keepNew(start[i], f, limit)
				start[i] = len(res[i].rows)
				h += len(res[i].rows)
			}
			if room = maxFindRows - h; room < n {
				return false
			}
			room -= n
			return true
		}
		ok := true
		r.mu.RLock()  // 이 라우터의 처리만 잠깐 기다림
		if fam != 0 { // unicast 밖 종류 (famrib.go) : 질의마다 표를 훑음
			for i, q := range qs {
				rows := famMatch(r, p, view, fam, q.p, q.how)
				if ok = fit(len(rows)); !ok {
					break
				}
				res[i].rows = append(res[i].rows, rows...)
			}
		} else {
			for i, q := range qs {
				if q.how == "more" {
					continue
				}
				rows := match(r, p, view, q.p, q.how)
				if ok = fit(len(rows)); !ok {
					break
				}
				res[i].rows = append(res[i].rows, rows...)
			}
			ok = ok && more.scan(r, p, view, res, fit)
		}
		r.mu.RUnlock()
		if !ok {
			return nil, errTooMany()
		}
		held = 0
		for i := range res {
			res[i].keepNew(start[i], f, limit)
			held += len(res[i].rows)
		}
	}
	for i := range res {
		sortRows(res[i].rows)
		if limit > 0 && len(res[i].rows) > limit {
			res[i].rows = res[i].rows[:limit]
		}
	}
	return res, nil
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

// scan : peer 의 경로표를 한 번 훑어, 경로마다 그것을 덮는 질의 프리픽스 (질의에 있는 길이만) 를 찾음.
// 줄 하나를 더할 때마다 fit(1) 로 자리를 확인하고, 자리가 없으면 바로 멈추고 false (= 줄이 너무 많음)
func (m *moreIndex) scan(r *router, p *peer, view int, res []findResult, fit func(int) bool) bool {
	if len(m.idx) == 0 {
		return true
	}
	base := ribRow{router: r.ip, routerName: r.sysName, peer: p.ip.String(), peerAS: p.as}
	hit := func(x netip.Prefix, rt pathOf, lens []int) bool {
		for _, n := range lens {
			if n > x.Bits() {
				break
			}
			up, _ := x.Addr().Prefix(n)
			for _, i := range m.idx[up] {
				if !fit(1) {
					return false
				}
				row := base
				row.pfx = x
				row.setPath(rt)
				res[i].rows = append(res[i].rows, row)
			}
		}
		return true
	}
	rv := p.view(view)
	if len(m.lens4) > 0 {
		for k, v := range rv.rib4.all() {
			if !hit(k.prefix(), v, m.lens4) {
				return false
			}
		}
	}
	if len(m.lens6) > 0 {
		for k, v := range rv.rib6.all() {
			if !hit(k.prefix(), v, m.lens6) {
				return false
			}
		}
	}
	return true
}

// ---- 응답 모양 ----

// findHead : 프리픽스 하나의 결과에서 routes 앞부분 (writeFind 가 이것을 쓰고 routes 를 줄마다 이어 씀)
type findHead struct {
	Query     string   `json:"query"`
	Match     string   `json:"match"`
	OriginAS  []uint32 `json:"origin_as,omitempty"`
	NotOrigin []uint32 `json:"not_origin,omitempty"`
	Count     int      `json:"count"` // 돌려준 줄 수 (limit 뒤)
	Total     int      `json:"total"` // 필터 뒤, limit 전
}

// findJSON : 응답 전체 모양 (findHead 칸들 + routes). 응답은 writeFind 가 같은 글자로 써 나감, 이 형은 읽는 쪽 (시험)
type findJSON struct {
	findHead
	Routes []routeJSON `json:"routes"`
}

// batchHead : POST 응답에서 results 앞부분
type batchHead struct {
	Match     string   `json:"match,omitempty"`
	OriginAS  []uint32 `json:"origin_as,omitempty"`
	NotOrigin []uint32 `json:"not_origin,omitempty"`
	Count     int      `json:"count"` // results 의 routes 줄 수 합
}

// batchJSON : POST 응답 전체 모양 (읽는 쪽)
type batchJSON struct {
	batchHead
	Results []findJSON `json:"results"`
}

// openObj : 구조체를 JSON 으로 쓰되 끝 '}' 를 떼고 ,"name":[ 를 붙임 (뒤에 배열을 줄마다 이어 쓰려고)
func openObj(bw *bufio.Writer, v any, name string) {
	b, _ := json.Marshal(v)
	bw.Write(b[:len(b)-1])
	bw.WriteString(`,"` + name + `":[`)
}

// writeFind : 프리픽스 하나의 결과를 findJSON 모양으로 써 나감 (routes 를 한꺼번에 만들지 않음)
func writeFind(bw *bufio.Writer, q findQuery, fr *findResult, f *originFilter) {
	h := findHead{Query: q.p.String(), Match: q.how, Count: len(fr.rows), Total: fr.total}
	setFilter(f, &h.OriginAS, &h.NotOrigin)
	openObj(bw, h, "routes")
	for i := range fr.rows {
		if i > 0 {
			bw.WriteByte(',')
		}
		b, _ := json.Marshal(fr.rows[i].json())
		bw.WriteByte('\n')
		bw.Write(b)
	}
	bw.WriteString("]}")
}

// jsonOut : JSON 응답을 쓸 버퍼 (머리를 정함)
func jsonOut(w http.ResponseWriter) *bufio.Writer {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	return bufio.NewWriterSize(w, 1<<16)
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
	view, err := parseView(qv.Get("view"))
	if err != nil {
		httpErr(w, 400, "%v", err)
		return
	}
	fam, err := c.famQuery(qv.Get("family"))
	if err != nil {
		httpErr(w, err.(httpError).code, "%v", err)
		return
	}
	list, ok := c.peersOf(req.PathValue("ip"), "")
	if !ok {
		httpErr(w, 404, "router %s not found", req.PathValue("ip"))
		return
	}
	n, err := queryUint(qv, "limit", 1<<31-1) // 틀린 값은 400 (0.2.0 전에는 조용히 기본값)
	if err != nil {
		httpErr(w, 400, "%v", err)
		return
	}
	limit := int(n)
	q := findQuery{text: qv.Get("p"), p: p, how: how}
	res, err := c.findMany(list, []findQuery{q}, limit, f, view, fam)
	if err != nil {
		httpErr(w, err.(httpError).code, "%v", err)
		return
	}
	fr := &res[0]
	if qv.Get("format") == "text" {
		w.Header().Set("X-Total", strconv.Itoa(fr.total)) // 글자 응답의 total
		rw := newRowWriter(w, req)
		for i := range fr.rows {
			rw.row(&fr.rows[i])
		}
		rw.end()
		return
	}
	bw := jsonOut(w)
	writeFind(bw, q, fr, f)
	bw.WriteString("\n")
	bw.Flush()
}

// apiPrefixBatch : POST /api/prefix, /api/routers/{ip}/prefix (여러 프리픽스, 읽기라 key 없음)
func (c *collector) apiPrefixBatch(w http.ResponseWriter, req *http.Request) {
	var body struct {
		P         []string `json:"p"`
		Match     string   `json:"match"`
		Limit     int      `json:"limit"`
		OriginAS  []uint32 `json:"origin_as"`
		NotOrigin []uint32 `json:"not_origin"`
		View      string   `json:"view"`   // 0.3.0 : in-pre | in-post | out-pre | out-post | loc-rib (없으면 peer 마다 주 보기)
		Family    string   `json:"family"` // 0.3.0 : vpnv4 등 (없으면 unicast)
	}
	if !isJSONBody(req) {
		httpErr(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}
	d := json.NewDecoder(io.LimitReader(req.Body, 2<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(&body); err != nil {
		httpErr(w, 400, "body : %v (expected {\"p\": [...], \"match\": \"more\", \"limit\": 100, \"not_origin\": [64500]})", err)
		return
	}
	if body.Limit < 0 { // 11-05 : 전에는 음수 = 제한 없음 (GET 은 400)
		httpErr(w, 400, "limit : 0 or more (got %d)", body.Limit)
		return
	}
	if len(body.P) == 0 || len(body.P) > maxBatch {
		httpErr(w, 400, "p : 1 to %d entries (got %d)", maxBatch, len(body.P))
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
	view, err := parseView(body.View)
	if err != nil {
		httpErr(w, 400, "%v", err)
		return
	}
	fam, err := c.famQuery(body.Family)
	if err != nil {
		httpErr(w, err.(httpError).code, "%v", err)
		return
	}
	qs := make([]findQuery, len(body.P))
	for i, s := range body.P {
		p, how, err := parseQuery(s)
		if err != nil {
			httpErr(w, 400, "p[%d] (entry %d) %q : prefix (192.0.2.0/24) or address (192.0.2.1)", i, i+1, s)
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
	res, err := c.findMany(list, qs, body.Limit, f, view, fam)
	if err != nil {
		httpErr(w, err.(httpError).code, "%v", err)
		return
	}
	h := batchHead{Match: body.Match}
	setFilter(f, &h.OriginAS, &h.NotOrigin)
	for i := range res {
		h.Count += len(res[i].rows)
	}
	bw := jsonOut(w)
	openObj(bw, h, "results")
	for i := range res {
		if i > 0 {
			bw.WriteByte(',')
		}
		bw.WriteByte('\n')
		writeFind(bw, qs[i], &res[i], f)
	}
	bw.WriteString("\n]}\n")
	bw.Flush()
}

// isJSONBody : 본문의 Content-Type 이 application/json 인가 (11-05 : 아니면 415.
// 브라우저의 form / 단순 요청 (CSRF) 은 이 머리를 붙일 수 없어, 나쁜 웹 페이지가 찾기 POST 를 보내지 못함)
func isJSONBody(req *http.Request) bool {
	t, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	return err == nil && t == "application/json"
}

// httpError : 오류 + HTTP 상태 번호
type httpError struct {
	code int
	msg  string
}

func (e httpError) Error() string { return e.msg }

// famQuery : 찾기의 family= (0.3.0). 빈칸 / unicast = 0 (unicast 경로표), 경로표가 없는 종류 = 400, 꺼진 종류 = 409
func (c *collector) famQuery(s string) (family, error) {
	if s == "" {
		return 0, nil
	}
	f, err := parseFamily(s)
	if err != nil {
		return 0, httpError{400, err.Error()}
	}
	if f == famOf(afiIPv4, safiUnicast) || f == famOf(afiIPv6, safiUnicast) {
		return 0, nil // unicast 는 프리픽스 주소로 v4 / v6 가 정해짐
	}
	if !ribSupported[f] {
		return 0, httpError{400, "family=" + s + " : no route tables for this address family"}
	}
	if !c.rib[f] {
		return 0, httpError{409, "route tables are off for " + f.String() + " (-rib)"}
	}
	return f, nil
}
