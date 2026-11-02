// 퍼징 (0.2.0) : 망가뜨린 BMP 입력을 수백만 번 넣어 panic (코드 실수) 을 찾음.
//
//	go test -run '^$' -fuzz FuzzProcess -fuzztime 10m .   (보통 go test 는 아래 씨앗만 한 번씩 돌림)
//
// safeProcess 는 panic 을 잡아 그 메시지만 버리므로 운영은 멈추지 않지만, 퍼징은 process 를 바로 불러 panic 을 실패로 봄.
// 찾은 입력은 testdata/fuzz/FuzzProcess/ 에 남고, 그 뒤로는 보통 go test 에서도 늘 돌아감 (되풀이 시험)
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"
	"time"
)

// fz : 씨앗 만들기 도우미
func fzBMP(typ byte, body []byte) []byte {
	b := []byte{3, 0, 0, 0, 0, typ}
	binary.BigEndian.PutUint32(b[1:5], uint32(6+len(body)))
	return append(b, body...)
}

// fzPeer : per-peer 머리 42 바이트 (v6 이면 flags 0x80, A flag 0x20 = 2 바이트 AS)
func fzPeer(typ, flags byte, v6 bool) []byte {
	h := make([]byte, 42)
	h[0], h[1] = typ, flags
	if v6 {
		h[1] |= 0x80
		copy(h[10:26], []byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
	} else {
		copy(h[22:26], []byte{198, 51, 100, 1})
	}
	binary.BigEndian.PutUint32(h[26:30], 64500)
	copy(h[30:34], []byte{198, 51, 100, 1})
	return h
}

// fzOpen : OPEN (4 바이트 AS, IPv4 / IPv6 unicast, Add-Path 둘 다 받고 보냄)
func fzOpen() []byte {
	caps := []byte{
		65, 4, 0, 0, 0xfb, 0xf4, // 4 바이트 AS 64500
		1, 4, 0, 1, 0, 1, // MP IPv4 unicast
		1, 4, 0, 2, 0, 1, // MP IPv6 unicast
		69, 8, 0, 1, 1, 3, 0, 2, 1, 3, // Add-Path IPv4 / IPv6 받고 보냄
	}
	opt := append([]byte{2, byte(len(caps))}, caps...)
	body := append([]byte{4, 0x5b, 0xa0, 0, 180, 198, 51, 100, 1, byte(len(opt))}, opt...)
	m := append(bytes.Repeat([]byte{0xff}, 16), 0, 0, 1)
	binary.BigEndian.PutUint16(m[16:18], uint16(19+len(body)))
	return append(m, body...)
}

func fzSeeds(f *testing.F) {
	upd4, _ := hex.DecodeString("ffffffffffffffffffffffffffffffff0052020000003b900e000d00010104c000021700188289634001010040020602010000407d800404000003e840050400000064c0080cfbf4006efbf40c80fbf42328")
	upd6, _ := hex.DecodeString("ffffffffffffffffffffffffffffffff0025020000000e900f000a0002013026059cc00c0a")
	eor, _ := hex.DecodeString("ffffffffffffffffffffffffffffffff001702000000")
	upNoOpen, _ := hex.DecodeString("03000000440300000000000000000000000000000000000000000000c63364670000fbf0c6336467" + "6ae222c3000518120000000000000000000000000000c0000264e2e800b3")
	down, _ := hex.DecodeString("0300000033020000000000000000000000000000000000000000000001020305" + "00003b418efb31080000000000000000" + "020007")
	initMsg := fzBMP(4, []byte{0, 2, 0, 3, 'r', 't', 'r', 0, 1, 0, 2, 'd', 'x'})
	open := fzOpen()
	up := append(fzPeer(0, 0, false), make([]byte, 20)...) // 로컬 주소 16 + 포트 2 + 2
	up = append(append(up, open...), open...)
	stats := append(fzPeer(0, 0, false), 0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 7, 0, 7, 0, 8, 0, 0, 0, 0, 0, 0, 0, 9)
	for _, s := range [][]byte{
		initMsg,
		fzBMP(0, append(fzPeer(0, 0, false), upd4...)),
		fzBMP(0, append(fzPeer(0, 0x20, false), upd4...)), // 2 바이트 AS peer
		fzBMP(0, append(fzPeer(0, 0, true), upd6...)),
		fzBMP(0, append(fzPeer(0, 0, false), eor...)),
		fzBMP(3, up),
		upNoOpen,
		down,
		fzBMP(1, stats),
		fzBMP(5, []byte{0, 1, 0, 2, 0, 1}),
	} {
		f.Add(s)
		f.Add(append(append(append([]byte{}, initMsg...), fzBMP(3, up)...), s...)) // Peer Up (Add-Path) 뒤에 이어서
	}
}

// FuzzProcess : 입력 = BMP 메시지 여러 개를 이은 것. 머리의 길이로 잘라 같은 라우터에 차례로 (상태가 쌓이는 경우도 봄)
func FuzzProcess(f *testing.F) {
	fzSeeds(f)
	now := time.Unix(1793160000, 0)
	f.Fuzz(func(t *testing.T, in []byte) {
		for _, buckets := range []int{0, 8} {
			c := newCollector("x")
			c.keyBuckets = buckets
			r := c.lockRouter("192.0.2.1", "")
			rd := newMsgReader(bytes.NewReader(nil), in) // 수집기가 TCP 에서 자르는 것과 같은 코드
			for {
				msg, err := rd.next()
				if err != nil {
					break
				}
				c.process(r, msg, now) // 오류는 괜찮음, panic 은 실패
			}
			r.mu.Unlock()
		}
	})
}

// FuzzExtComm : ext community 글자 만들기 (8 바이트 단위가 아닌 길이도)
func FuzzExtComm(f *testing.F) {
	f.Add([]byte{0, 2, 0, 2, 0, 0, 0, 3})
	f.Add([]byte{0x40, 4, 0, 1, 0x48, 0x74, 0x24, 0})
	f.Add([]byte{6, 0, 1, 0, 0, 0, 0, 5})
	f.Fuzz(func(t *testing.T, b []byte) {
		extCommString(b)
	})
}
