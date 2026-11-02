// 무중단 판 올리기 시험 (handover.go). exec 를 거치는 시험은 시험 포트에서 (docs/release.md, tools/bmp_replay)
package main

import (
	"bytes"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

// router / peer 에 칸을 더하면 상태 파일 (saveRouter / savePeer) 에도 넣거나, 넘기지 않는 이유를 여기 적어야 함
func TestHandoverFields(t *testing.T) {
	check := func(v any, passed, skipped []string) {
		var want []string
		want = append(append(want, passed...), skipped...)
		var got []string
		ty := reflect.TypeOf(v)
		for i := range ty.NumField() {
			got = append(got, ty.Field(i).Name)
		}
		sort.Strings(want)
		sort.Strings(got)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s fields changed : update saveRouter / savePeer (handover.go) and this list\n got  %v\n want %v", ty.Name(), got, want)
		}
	}
	check(router{}, []string{"ip", "hash", "peers", "routerSeq", "peerSeq", "terminated", "connGen", "sysName", "sysDescr", "up", "since",
		"lastMsg", "connects", "typeCount", "errors", "noOpen", "lastTerm", "lastReset", "lastResetWhy"},
		[]string{"mu", "byID", "stat", "sink", "enc"}) // byID = 찾을 때 다시 채움, stat = 판 올리기 직전에 기록하고 0, sink / enc = 줄 버퍼
	check(peer{}, []string{"key", "ip", "ipStr", "rd", "typ", "flags", "as", "bgpID", "bgpIDRaw", "bmpStats", "bmpStatsAt", "up", "gotUp",
		"noOpen", "since", "localIP", "localPort", "rmtPort", "holdSent", "holdRecv", "sentCaps", "recvCaps", "addPathV4", "addPathV6",
		"famV4", "famV6", "eorV4", "eorV6", "lastUpdate", "adds", "dels", "downReason", "hist", "rib4", "rib6"},
		[]string{"ribWait", "ribLog", // 새 판에서만 쓰는 것
			"grewAt"}) // settled 용. 넘기면 상태 파일 판이 올라 0.2.0-dev (d52a3fd) 에서 -upgrade 를 못 함 -> 넘기지 않음 (새 판에서 0 = 5 분 넘게 up 이면 바로 settled)
}

// testRouterState : 칸마다 0 이 아닌 값 (빠뜨리면 왕복 비교에서 보임)
func testCollector() *collector {
	c := newCollector("x")
	now := time.Unix(1793160000, 123000).UTC()
	r := c.routerFor("192.0.2.1", "")
	r.routerSeq, r.peerSeq, r.terminated, r.connGen = 3, 4, true, 7
	r.sysName, r.sysDescr, r.up, r.since, r.lastMsg, r.connects = "r1", "desc", true, now, now, 2
	r.typeCount[0], r.typeCount[bmpTypeCount] = 10, 1
	r.errors, r.noOpen, r.lastTerm, r.lastReset, r.lastResetWhy = 5, 1, "term", now, "api"
	a := attrOf(&attrs{asPath: " 64496 64497", originAS: 64497, nextHop: "192.0.2.9", origin: "igp", med: 1, localPref: 100}, true)
	for i, ip := range []string{"198.51.100.1", "2001:db8::1"} {
		h := peerHdr{addr: netip.MustParseAddr(ip), as: 64500 + uint32(i), bgpID: [4]byte{192, 0, 2, byte(i)}, flags: 0x20}
		p := r.peerGet(&h)
		p.bmpStats, p.bmpStatsAt = map[string]uint64{"rejected_prefixes": 3}, now
		p.up, p.gotUp, p.noOpen, p.since = true, true, true, now
		p.localIP, p.localPort, p.rmtPort, p.holdSent, p.holdRecv = "192.0.2.1", 179, 1000, 90, 180
		p.sentCaps, p.recvCaps, p.addPathV4, p.addPathV6, p.famV4, p.famV6 = "a", "b", true, true, true, true
		p.eorV4, p.eorV6, p.lastUpdate, p.adds, p.dels, p.downReason = now, now, now, 9, 8, "why"
		p.addEvent(now, "up")
		u := &update{attrs: attrs{asPath: a.asPath, originAS: a.originAS, nextHop: a.nextHop, origin: a.origin, med: a.med, localPref: a.localPref}}
		u.v4Add = []prefix{{addr: netip.MustParseAddr("203.0.113.0"), length: 24}}
		u.v6Add = []prefix{{addr: netip.MustParseAddr("2001:db8:1::"), length: 48}}
		p.apply(u, now)
	}
	c.seq.Store(12345)
	return c
}

// 상태 파일 왕복 : 라우터 / peer / seq / 경로표가 같음, 경로표를 읽는 동안 온 UPDATE 는 읽은 뒤 적용
func TestHandoverStateRoundTrip(t *testing.T) {
	c := testCollector()
	old := c.routers["192.0.2.1"]
	wantPeers := allPeers(c)
	wantDigest, wantN := ribDigest(wantPeers)
	path := filepath.Join(t.TempDir(), "x.state")
	hs := &handoverState{Version: stateVersion, Release: "test", LockFd: 3, ListenFd: 4, HTTPFd: -1,
		Conns: []connState{{IP: "192.0.2.1", Fd: 5, Gen: 7, N: 100, Pending: []byte{3, 0, 0}}}}
	if _, _, err := writeHandover(path, hs, c); err != nil {
		t.Fatal(err)
	}
	got, err := readHandover(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Conns[0].Gen != 7 || !bytes.Equal(got.Conns[0].Pending, []byte{3, 0, 0}) || got.Seq != 12345 {
		t.Fatalf("state %+v", got)
	}
	c2 := newCollector("x")
	got.restore(c2)
	r2 := c2.routers["192.0.2.1"]
	if !reflect.DeepEqual(saveRouter(r2), saveRouter(old)) || c2.seq.Load() != 12345 {
		t.Fatalf("router\n got  %+v\n want %+v", saveRouter(r2), saveRouter(old))
	}
	ps := allPeers(c2)
	for i, p := range ps {
		if !reflect.DeepEqual(savePeer(p), savePeer(wantPeers[i])) || !p.ribWait {
			t.Fatalf("peer %d\n got  %+v\n want %+v", i, savePeer(p), savePeer(wantPeers[i]))
		}
	}
	// 경로표를 읽기 전에 온 UPDATE : 하나는 더하고 하나는 지움 -> 읽은 뒤에 적용되어야 함
	p0 := ps[0]
	u := &update{attrs: attrs{asPath: " 64511", originAS: 64511, origin: "igp"}}
	u.v4Add = []prefix{{addr: netip.MustParseAddr("198.51.100.0"), length: 24}}
	u.v4Del = []prefix{{addr: netip.MustParseAddr("203.0.113.0"), length: 24}}
	p0.apply(u, time.Unix(1793160100, 0))
	if len(p0.rib4) != 0 || len(p0.ribLog) != 1 {
		t.Fatalf("while loading : rib4 %d log %d", len(p0.rib4), len(p0.ribLog))
	}
	// 두 번째 peer 는 그 사이 새 세션 (Peer Up) : 읽은 경로표는 버려야 함
	ps[1].clearRIB()
	got.loadRIBs(c2)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("state file must be removed after loading")
	}
	if ribLoading.Load() || p0.ribWait || len(p0.rib4) != 1 || len(p0.rib6) != 1 || len(ps[1].rib4)+len(ps[1].rib6) != 0 {
		t.Fatalf("after load : p0 %d/%d wait %v, p1 %d", len(p0.rib4), len(p0.rib6), p0.ribWait, len(ps[1].rib4))
	}
	if _, ok := p0.rib4[k4(netip.MustParseAddr("198.51.100.0"), 24)]; !ok {
		t.Fatal("deferred add not applied")
	}
	// 미뤄 둔 것이 없는 경우는 원래와 똑같음
	c3 := testCollector()
	path3 := filepath.Join(t.TempDir(), "y.state")
	writeHandover(path3, &handoverState{Version: stateVersion}, c3)
	h3, _ := readHandover(path3)
	c4 := newCollector("x")
	h3.restore(c4)
	h3.loadRIBs(c4)
	if d, n := ribDigest(allPeers(c4)); d != wantDigest || n != wantN {
		t.Fatalf("route tables differ : %d vs %d routes", n, wantN)
	}
	// 틀린 파일 / 다른 형식 판
	os.WriteFile(path3, []byte("hello\n"), 0600)
	if _, err := readHandover(path3); err == nil {
		t.Fatal("bad magic must fail")
	}
	writeHandover(path3, &handoverState{Version: stateVersion + 1}, c3)
	if _, err := readHandover(path3); err == nil {
		t.Fatal("other version must fail")
	}
}

// 메시지 중간에서 멈춰도 (읽기 시간 제한) 읽은 바이트는 남고, 새 msgReader 가 이어 받아 같은 메시지를 냄
func TestMsgReaderPause(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	msg := fzBMP(4, []byte("0123456789"))                    // Initiation 꼴 (길이 16)
	go b.Write(append(append([]byte{}, msg...), msg[:9]...)) // 하나 반
	rd := newMsgReader(a, nil)
	got, err := rd.next()
	if err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("first : %v %v", got, err)
	}
	time.Sleep(50 * time.Millisecond) // 반쪽이 들어오기를
	a.SetReadDeadline(time.Now())
	_, err = rd.next()
	if !os.IsTimeout(err) {
		t.Fatalf("want timeout, got %v", err)
	}
	upg.mu.Lock() // 판 올리기 중이 아님 + Initiation 뒤 = 늦게 온 판 올리기의 시간 제한 -> 이어 읽음
	upg.pausing = false
	upg.mu.Unlock()
	cr := &connRun{conn: a, n: 1}
	if !upg.park(cr, err) {
		t.Fatal("late pause timeout after initiation must resume")
	}
	pend := rd.pending()
	if !bytes.Equal(pend, msg[:9]) {
		t.Fatalf("pending %v", pend)
	}
	rd2 := newMsgReader(bytes.NewReader(msg[9:]), pend)
	got, err = rd2.next()
	if err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("resumed : %v %v", got, err)
	}
	// 큰 메시지 (읽기 단위 64 KB 보다 큼)
	big := fzBMP(4, bytes.Repeat([]byte{'x'}, 200000))
	rd3 := newMsgReader(bytes.NewReader(append(append([]byte{}, big...), msg...)), nil)
	if g, err := rd3.next(); err != nil || !bytes.Equal(g, big) {
		t.Fatalf("big : %d %v", len(g), err)
	}
	if g, err := rd3.next(); err != nil || !bytes.Equal(g, msg) {
		t.Fatalf("after big : %v", err)
	}
}

// -resume 을 뺀 옵션 (판 올리기를 거듭해도 쌓이지 않게)
func TestBaseArgs(t *testing.T) {
	saved := os.Args
	defer func() { os.Args = saved }()
	os.Args = []string{"bongsudae", "-c", "x.conf", "-resume", "run/a.state", "-resume=run/b.state", "-http", ":5001"}
	if got := baseArgs(); !reflect.DeepEqual(got, []string{"-c", "x.conf", "-http", ":5001"}) {
		t.Fatalf("%v", got)
	}
}
