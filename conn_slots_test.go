package main

// (11-05) 연결 자리 (Initiation 전 / 라우터 나눔) 와 정보 TLV 글자 (제곱 -> 한 번 훑음) 시험

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

func TestConnSlots(t *testing.T) {
	s := &connSlots{byIP: map[string]int{}}
	cr := func(ip string) *connRun { return &connRun{ip: ip} }

	// 주소마다 Initiation 전 pendingPerIP 개
	var same []*connRun
	for i := 0; i < pendingPerIP; i++ {
		c := cr("192.0.2.1")
		if why := s.admit(c); why != "" {
			t.Fatalf("admit %d : %s", i, why)
		}
		same = append(same, c)
	}
	if why := s.admit(cr("192.0.2.1")); !strings.Contains(why, "from this address") {
		t.Fatalf("per address : %q", why)
	}
	if why := s.admit(cr("192.0.2.2")); why != "" { // 다른 주소는 됨
		t.Fatalf("other address : %s", why)
	}
	// Initiation 을 받으면 Initiation 전 자리가 풀려 같은 주소가 또 들어옴
	if !s.promote(same[0]) || same[0].slot != slotRouter {
		t.Fatal("promote")
	}
	if why := s.admit(cr("192.0.2.1")); why != "" {
		t.Fatalf("after promote : %s", why)
	}
	if p, r := s.counts(); p != pendingPerIP+1 || r != 1 {
		t.Fatalf("counts %d %d", p, r)
	}
	// 끝나면 돌려줌, 두 번 불러도 됨
	s.release(same[0])
	s.release(same[0])
	s.release(same[1])
	if p, r := s.counts(); p != pendingPerIP || r != 0 || s.byIP["192.0.2.1"] != pendingPerIP-1 {
		t.Fatalf("after release %d %d %v", p, r, s.byIP)
	}

	// 전체 Initiation 전 maxPending
	s = &connSlots{byIP: map[string]int{}}
	for i := 0; i < maxPending; i++ {
		if why := s.admit(cr(fmt.Sprintf("198.51.100.%d", i))); why != "" {
			t.Fatalf("admit %d : %s", i, why)
		}
	}
	if why := s.admit(cr("203.0.113.1")); !strings.Contains(why, "all addresses") {
		t.Fatalf("all : %q", why)
	}

	// 라우터 maxRouters : 그 뒤 Initiation 은 false, Initiation 전 자리는 그대로 (release 가 돌려줌)
	s = &connSlots{byIP: map[string]int{}}
	for i := 0; i < maxRouters; i++ {
		c := cr("203.0.113.9")
		s.admit(c)
		if !s.promote(c) {
			t.Fatalf("router %d", i)
		}
	}
	c := cr("203.0.113.10")
	s.admit(c)
	if s.promote(c) || c.slot != slotPending {
		t.Fatal("over maxRouters")
	}
	s.release(c)
	if p, r := s.counts(); p != 0 || r != maxRouters || len(s.byIP) != 0 {
		t.Fatalf("over : %d %d %v", p, r, s.byIP)
	}

	// 판 올리기로 이어 받은 연결 : 상한 없이 셈 (Initiation 전 / 뒤)
	s = &connSlots{byIP: map[string]int{}}
	a, b := cr("192.0.2.5"), &connRun{ip: "192.0.2.6", n: 3}
	s.resume(a)
	s.resume(b)
	if p, r := s.counts(); p != 1 || r != 1 || a.slot != slotPending || b.slot != slotRouter {
		t.Fatalf("resume %d %d", p, r)
	}
}

// oldTLVs : 11-05 전 tlvs (비교용)
func oldTLVs(b []byte) map[uint16]string {
	m := map[uint16]string{}
	for len(b) >= 4 {
		t, n := be16(b), int(be16(b[2:]))
		if 4+n > len(b) {
			break
		}
		v := string(b[4 : 4+n])
		if m[t] != "" {
			v = m[t] + " " + v
		}
		m[t] = v
		b = b[4+n:]
	}
	return m
}

func tlv(t uint16, v string) []byte {
	return append([]byte{byte(t >> 8), byte(t), byte(len(v) >> 8), byte(len(v))}, v...)
}

func TestTLVs(t *testing.T) {
	// 보통 크기 : 전과 같음 (빈 값, 같은 종류 여러 번, 끝이 잘린 TLV 포함)
	rnd := rand.New(rand.NewSource(1))
	for k := 0; k < 2000; k++ {
		var b []byte
		for i := rnd.Intn(8); i > 0; i-- {
			v := strings.Repeat("x", rnd.Intn(5))
			b = append(b, tlv(uint16(rnd.Intn(4)), v)...)
		}
		b = b[:len(b)-rnd.Intn(3)*min(len(b), 1)]
		got, want := tlvs(b), oldTLVs(b)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%x : %v, want %v", b, got, want)
		}
	}
	// 1 MB 에 1 바이트 sysName 20 만 개 : 한 번 훑음 (전에는 21 초), 글자는 tlvMax 까지
	big := bytes.Repeat(tlv(2, "a"), (1<<20)/5)
	start := time.Now()
	m := tlvs(big)
	if d := time.Since(start); d > time.Second {
		t.Fatalf("slow : %v", d)
	}
	if n := len(m[2]); n > tlvMax || n < tlvMax-2 {
		t.Fatalf("len %d", n)
	}
	// 첫 값은 tlvMax 보다 길어도 그대로
	long := strings.Repeat("d", tlvMax+100)
	if m := tlvs(append(tlv(1, long), tlv(1, "more")...)); m[1] != long {
		t.Fatalf("long first : %d", len(m[1]))
	}
}
