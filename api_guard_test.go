package main

// (11-05) 인증 없는 API 요청으로 서비스가 멈추지 않게 한 것의 시험 :
// 찾기 줄 수 상한 (413), 같은 질의 합치기, 음수 limit / Content-Type, 자리 기다림 (503), 쓰기 기한

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"
	"unique"
)

// guardCollector : 라우터 하나, peer 둘, peer 마다 /24 1000 개 (1.0.0.0/24 ~ 1.3.231.0/24)
func guardCollector() *collector {
	c := newCollector("x")
	r := c.routerFor("192.0.2.1", "")
	a := unique.Make(pathAttr{asPath: " 64500", originAS: 64500})
	for pi := 0; pi < 2; pi++ {
		h := peerHdr{addr: netip.AddrFrom4([4]byte{198, 51, 100, byte(pi)}), as: 64496}
		p := r.peerGet(&h)
		p.up = true
		for i := 0; i < 1000; i++ {
			inPre(p).rib4.set(k4(netip.AddrFrom4([4]byte{1, byte(i >> 8), byte(i), 0}), 24), route{a: a})
		}
	}
	return c
}

func TestFindGuard(t *testing.T) {
	c := guardCollector()
	defer func(n int) { maxFindRows = n }(maxFindRows)
	get := func(q string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		c.apiPrefix(w, httptest.NewRequest("GET", "/api/prefix?"+q, nil))
		return w
	}
	post := func(body, ct string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/api/prefix", strings.NewReader(body))
		if ct != "" {
			req.Header.Set("Content-Type", ct)
		}
		w := httptest.NewRecorder()
		c.apiPrefixBatch(w, req)
		return w
	}

	// 상한 1500 : peer 둘 = 2000 줄 -> 413
	maxFindRows = 1500
	if w := get("p=0.0.0.0/0&match=more"); w.Code != 413 {
		t.Fatalf("no limit : %d %s", w.Code, w.Body.String())
	}
	// limit 이 있으면 상한에 닿을 때 위 limit 줄만 남겨 자리를 만듦 : peer 하나 (1000 줄) 가 상한 (500) 보다 커도 되고,
	// 결과는 상한 없이 찾은 앞 10 줄과 같음
	maxFindRows = 1 << 30
	w := get("p=0.0.0.0/0&match=more&limit=10")
	want := w.Body.String()
	maxFindRows = 500
	if w = get("p=0.0.0.0/0&match=more&limit=10"); w.Body.String() != want {
		t.Fatalf("compacted result differs :\n%.300s\n%.300s", w.Body.String(), want)
	}
	var one findJSON
	if err := json.Unmarshal(w.Body.Bytes(), &one); err != nil || w.Code != 200 {
		t.Fatalf("limit 10 : %d %v %s", w.Code, err, w.Body.String())
	}
	if one.Count != 10 || one.Total != 2000 || len(one.Routes) != 10 || one.Query != "0.0.0.0/0" || one.Match != "more" {
		t.Fatalf("limit 10 : %+v", one.findHead)
	}

	// 상한 안 : 줄마다 써 나간 JSON 이 바른 모양
	maxFindRows = 5000
	w = get("p=0.0.0.0/0&match=more&not_origin=64511")
	if err := json.Unmarshal(w.Body.Bytes(), &one); err != nil || one.Count != 2000 || len(one.Routes) != 2000 ||
		one.Routes[0].Prefix != "1.0.0.0/24" || len(one.NotOrigin) != 1 {
		t.Fatalf("all : %v %+v", err, one.findHead)
	}
	if w = get("p=9.9.9.0/24"); !strings.Contains(w.Body.String(), `"routes":[]`) {
		t.Fatalf("empty : %s", w.Body.String())
	}

	// 같은 질의 5000 개 : 한 번만 찾으므로 상한에 걸리지 않고, 결과는 질의마다
	maxFindRows = 1500
	ps := `"0.0.0.0/0"` + strings.Repeat(`,"0.0.0.0/0"`, maxBatch-1)
	w = post(`{"p":[`+ps+`],"match":"more","limit":1}`, "application/json")
	var b batchJSON
	if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil || w.Code != 200 {
		t.Fatalf("dup : %d %v %.200s", w.Code, err, w.Body.String())
	}
	if len(b.Results) != maxBatch || b.Count != maxBatch || b.Results[maxBatch-1].Total != 2000 || len(b.Results[0].Routes) != 1 {
		t.Fatalf("dup : results %d count %d", len(b.Results), b.Count)
	}
	// 서로 감싸는 질의 (/0, /1) 는 합칠 수 없음 -> limit 없으면 상한, limit 이 있으면 줄여 가며 됨
	if w = post(`{"p":["0.0.0.0/0","0.0.0.0/1"],"match":"more"}`, "application/json; charset=utf-8"); w.Code != 413 {
		t.Fatalf("nested : %d", w.Code)
	}
	if w = post(`{"p":["0.0.0.0/0","0.0.0.0/1"],"match":"more","limit":1}`, "application/json; charset=utf-8"); w.Code != 200 {
		t.Fatalf("nested limit 1 : %d", w.Code)
	}
	// 음수 limit, Content-Type
	maxFindRows = 5000
	if w = post(`{"p":["1.0.0.0/24"],"limit":-1}`, "application/json"); w.Code != 400 {
		t.Fatalf("limit -1 : %d", w.Code)
	}
	for _, ct := range []string{"", "text/plain", "application/x-www-form-urlencoded"} {
		if w = post(`{"p":["1.0.0.0/24"]}`, ct); w.Code != 415 {
			t.Fatalf("Content-Type %q : %d", ct, w.Code)
		}
	}
	if w = post(`{"p":["1.0.0.0/24","1.0.0.9"]}`, "application/json"); w.Code != 200 {
		t.Fatalf("ok : %d %s", w.Code, w.Body.String())
	}
	if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil || b.Count != 4 || len(b.Results) != 2 {
		t.Fatalf("ok : %v %s", err, w.Body.String())
	}
}

func TestTakeSlot(t *testing.T) {
	defer func(d time.Duration) { apiWait = d }(apiWait)
	apiWait = 50 * time.Millisecond
	sem := make(chan struct{}, 1)
	if !takeSlot(sem, httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil)) {
		t.Fatal("free slot")
	}
	// 자리가 없으면 apiWait 뒤 503
	w := httptest.NewRecorder()
	start := time.Now()
	if takeSlot(sem, w, httptest.NewRequest("GET", "/", nil)) || w.Code != 503 || w.Header().Get("Retry-After") == "" {
		t.Fatalf("busy : %d", w.Code)
	}
	if d := time.Since(start); d < apiWait || d > 2*time.Second {
		t.Fatalf("busy after %v", d)
	}
	// 상대가 끊으면 바로 false, 아무것도 쓰지 않음
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w = httptest.NewRecorder()
	if takeSlot(sem, w, httptest.NewRequest("GET", "/", nil).WithContext(ctx)) || w.Body.Len() != 0 {
		t.Fatal("canceled")
	}
	<-sem
	if !takeSlot(sem, httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil)) {
		t.Fatal("freed slot")
	}
}

// TestSlowGuard : 받아 가지 않는 상대는 apiWriteWait 뒤 끊기고, 천천히라도 받아 가면 apiWriteWait 보다 길어도 끝까지
func TestSlowGuard(t *testing.T) {
	defer func(d time.Duration) { apiWriteWait = d }(apiWriteWait)
	apiWriteWait = 300 * time.Millisecond
	done := make(chan error, 1)
	chunk := make([]byte, 64<<10) // 실제 응답도 bufio 64 KB 조각으로 씀 (기한은 Write 한 번마다)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g := newSlowGuard(w)
		n := 4096 // 받지 않는 상대 : 256 MB 는 소켓 버퍼에 다 들어가지 않음
		if r.URL.Path == "/slow" {
			n = 96 // 6 MB
		}
		for i := 0; i < n; i++ {
			if _, err := g.Write(chunk); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}))
	// 소켓 버퍼를 작게 : 리눅스는 보내는 버퍼가 크게 (수 MB) 비어야 쓰는 쪽을 깨우므로, 기한 300 ms 시험에서는 버퍼를 줄여야
	// "천천히 받아 감" 이 쓰기 사이 간격에 보임 (운영 기한 1 분은 넉넉함)
	srv.Config.ConnState = func(c net.Conn, s http.ConnState) {
		if tc, ok := c.(*net.TCPConn); ok && s == http.StateNew {
			tc.SetWriteBuffer(64 << 10)
		}
	}
	srv.Start()
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	// 받지 않음
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.Write([]byte("GET /stuck HTTP/1.1\r\nHost: x\r\n\r\n"))
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("stuck reader : all written")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stuck reader : write not cut")
	}

	// 천천히 받음 : 조각 사이는 apiWriteWait 보다 짧고 전체는 더 김
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	conn2.(*net.TCPConn).SetReadBuffer(64 << 10)
	conn2.Write([]byte("GET /slow HTTP/1.1\r\nHost: x\r\n\r\n"))
	start := time.Now()
	br := bufio.NewReader(conn2)
	buf := make([]byte, 64<<10)
	got := 0
	for got < 6<<20 {
		n, err := br.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("slow reader : %v after %d", err, got)
		}
		got += n
		time.Sleep(25 * time.Millisecond)
		if err == io.EOF {
			break
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("slow reader cut : %v", err)
	}
	if d := time.Since(start); d < 2*apiWriteWait {
		t.Fatalf("slow reader finished in %v : not longer than the deadline, extension not exercised", d)
	}
}
