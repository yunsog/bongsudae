// 상태 파일 시험 (무중단 판 올리기, 0.2.0)
//
// 큰 측정 (운영 전체 RIB 덤프, 약 20 분 + 메모리 15 GB) 은 환경 변수가 있을 때만 :
//
//	BONGSUDAE_RIB_DUMP=data/rib/<덤프> BONGSUDAE_STATE_DIR=run go test -run TestStateRIBDump -timeout 0 -v .
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
	"unique"
)

// ribDigest : 경로표 전체의 요약 (순서와 상관없음 : 경로마다 해시를 더함). 속성은 값으로 비교
func ribDigest(ps []*peer) (sum [4]uint64, n int) {
	add := func(h [32]byte) {
		for i := range sum {
			sum[i] += binary.LittleEndian.Uint64(h[i*8:])
		}
		n++
	}
	for _, p := range ps {
		attrHash := map[unique.Handle[pathAttr]][]byte{} // 속성 바이트는 한 번만 만듦
		ab := func(h unique.Handle[pathAttr]) []byte {
			b, ok := attrHash[h]
			if !ok {
				b = appendAttr(nil, h.Value())
				attrHash[h] = b
			}
			return b
		}
		for k, rt := range p.rib4 {
			b := binary.LittleEndian.AppendUint64([]byte(p.key+"4"), uint64(k))
			b = binary.LittleEndian.AppendUint32(b, rt.t)
			add(sha256.Sum256(append(b, ab(rt.a)...)))
		}
		for k, rt := range p.rib6 {
			b := binary.LittleEndian.AppendUint64([]byte(p.key+"6"), k.hi)
			b = binary.LittleEndian.AppendUint64(b, k.lo)
			b = append(b, k.n)
			b = binary.LittleEndian.AppendUint32(b, rt.t)
			add(sha256.Sum256(append(b, ab(rt.a)...)))
		}
	}
	return
}

// 작은 경로표 왕복 : 쓰고 읽으면 같은 경로표 (빈 peer, v4 만, v6 만, 둘 다)
func TestStateRIBRoundTrip(t *testing.T) {
	a1 := unique.Make(pathAttr{asPath: "64496 64497", originAS: 64497, nextHop: "192.0.2.1", origin: "igp", med: 10, localPref: 100,
		community: "64496:1 64496:2", extComm: "rt=64496:1", largeComm: "64496:1:2", clusterList: "192.0.2.9", originatorID: "192.0.2.8", aggregator: "64497 192.0.2.7"})
	a2 := unique.Make(pathAttr{asPath: "64496 {64498,64499}", originSet: "{64498,64499}", nextHop: "2001:db8::1", origin: "incomplete"})
	mk := func(key string) *peer { return &peer{key: key, ipStr: key} }
	ps := []*peer{mk("empty"), mk("v4"), mk("v6"), mk("both")}
	pfx := netip.MustParsePrefix
	ps[1].rib4 = map[key4]route{}
	for i := range 1000 {
		a := netip.AddrFrom4([4]byte{198, 51, byte(i >> 8), byte(i)})
		ps[1].rib4[k4(a, 32)] = route{a1, uint32(1793160000 + i)}
	}
	ps[2].rib6 = map[key6]route{k6(pfx("2001:db8:1::/48").Addr(), 48): {a2, 7}}
	ps[3].rib4 = map[key4]route{k4(pfx("0.0.0.0/0").Addr(), 0): {a2, 1}, k4(pfx("203.0.113.0/24").Addr(), 24): {a1, 2}}
	ps[3].rib6 = map[key6]route{k6(pfx("::/0").Addr(), 0): {a1, 3}}
	want, wn := ribDigest(ps)

	path := filepath.Join(t.TempDir(), "state")
	f, _ := os.Create(path)
	if _, err := writeRIBs(f, ps); err != nil {
		t.Fatal(err)
	}
	f.Close()
	got := []*peer{mk("empty"), mk("v4"), mk("v6"), mk("both")}
	got[0].rib4 = map[key4]route{1: {}} // 읽기 전에 있던 것은 없어져야 함
	f, _ = os.Open(path)
	defer f.Close()
	if err := readRIBs(bufio.NewReader(f), len(got), decodeInto(got)); err != nil {
		t.Fatal(err)
	}
	if d, n := ribDigest(got); d != want || n != wn {
		t.Fatalf("digest differs: %d routes vs %d", n, wn)
	}
	if len(got[0].rib4) != 0 || got[1].rib4 == nil || got[1].rib6 != nil {
		t.Fatalf("maps: %v %v %v", got[0].rib4, got[1].rib4 == nil, got[1].rib6)
	}

	// 잘린 파일 : 어디서 잘려도 panic 없이 오류
	b, _ := os.ReadFile(path)
	for _, cut := range []int{1, 8, 9, 20, len(b) / 2, len(b) - 1} {
		err := readRIBs(bufio.NewReader(&cutReader{b[:cut]}), 4, decodeInto([]*peer{mk("a"), mk("b"), mk("c"), mk("d")}))
		if err == nil {
			t.Fatalf("cut %d: no error", cut)
		}
	}
}

// decodeInto : 덩어리 i -> ps[i] 경로표
func decodeInto(ps []*peer) func(int, []byte) error {
	return func(i int, b []byte) error { return ps[i].decodeRIB(b) }
}

type cutReader struct{ b []byte }

func (r *cutReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, os.ErrClosed
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}

// allPeers : 라우터 주소 순, peer hash 순 (상태 파일의 peer 순서와 같은 규칙)
func allPeers(c *collector) []*peer {
	var ps []*peer
	for _, r := range c.sortedRouters() {
		ps = append(ps, sortedPeers(r)...)
	}
	return ps
}

// 운영 전체 RIB 측정 : 덤프를 처리해 경로표를 채움 -> 상태 파일 쓰기 / 읽기 시간과 크기, 읽은 것이 같은지
func TestStateRIBDump(t *testing.T) {
	dump := os.Getenv("BONGSUDAE_RIB_DUMP")
	if dump == "" {
		t.Skip("BONGSUDAE_RIB_DUMP not set")
	}
	dir := os.Getenv("BONGSUDAE_STATE_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	t0 := time.Now()
	c, err := runFile(dump, "none")
	if err != nil {
		t.Fatal(err)
	}
	ps := allPeers(c)
	keys := make([]string, len(ps))
	for i, p := range ps {
		keys[i] = p.key
	}
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	t.Logf("load %s : routers %d, peers %d, heap %d MB", time.Since(t0).Round(time.Second), len(c.routers), len(ps), ms.HeapAlloc>>20)
	want, wn := ribDigest(ps)

	path := filepath.Join(dir, "state.measure")
	defer os.Remove(path)
	t1 := time.Now()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriterSize(f, 1<<20)
	size, err := writeRIBs(w, ps)
	if err == nil {
		err = w.Flush()
	}
	if err == nil {
		err = f.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	tw := time.Since(t1)
	t.Logf("write : %d routes, %d MB, %s", wn, size>>20, tw.Round(time.Millisecond))

	// 옛 경로표를 버리고 (새 프로세스처럼) 읽음
	c, ps = nil, nil
	runtime.GC()
	got := make([]*peer, len(keys))
	for i, k := range keys {
		got[i] = &peer{key: k, ipStr: k}
	}
	t2 := time.Now()
	f, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := readRIBs(bufio.NewReaderSize(f, 1<<20), len(got), decodeInto(got)); err != nil {
		t.Fatal(err)
	}
	tr := time.Since(t2)
	runtime.GC()
	runtime.ReadMemStats(&ms)
	t.Logf("read  : %s, heap %d MB", tr.Round(time.Millisecond), ms.HeapAlloc>>20)
	if d, n := ribDigest(got); d != want || n != wn {
		t.Fatalf("digest differs: %d routes vs %d", n, wn)
	}
	t.Logf("same route tables (%d routes)", wn)
}
