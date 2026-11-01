// BMP v3 해석 (RFC 7854, RFC 9069)
//
// 메시지 = 공통 머리 (6 바이트) + 몸통.
// Route Monitoring / Peer Up / Peer Down / Stats 는 몸통 앞에 per-peer 머리 (42 바이트) 가 있음.
package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
)

// BMP 메시지 종류 (RFC 7854 4.1)
const (
	bmpRouteMon    = 0
	bmpStats       = 1
	bmpPeerDown    = 2
	bmpPeerUp      = 3
	bmpInit        = 4
	bmpTerm        = 5
	bmpRouteMirror = 6
	bmpTypeCount   = 7
)

var bmpTypeName = [bmpTypeCount]string{"route_mon", "stats", "peer_down", "peer_up", "init", "term", "route_mirror"}

const (
	bmpHdrLen  = 6       // 공통 머리 : 버전 1 + 길이 4 + 종류 1
	peerHdrLen = 42      // per-peer 머리
	bmpMaxLen  = 1 << 20 // 이보다 긴 메시지는 깨진 것으로 봄 (BGP 최대 65535 x 2 + TLV 보다 넉넉히)
)

// per-peer 머리 flag (RFC 7854 4.2, RFC 8671)
const (
	flagV = 0x80 // peer 주소가 IPv6
	flagL = 0x40 // post-policy (0 이면 pre-policy)
	flagA = 0x20 // AS_PATH 가 2 바이트 AS
	flagO = 0x10 // Adj-RIB-Out (0 이면 Adj-RIB-In)
)

// peer 종류 (RFC 9069 : 3 = Loc-RIB)
const peerTypeLocRIB = 3

var errShort = errors.New("message too short")

// peerHdr : per-peer 머리 (C 의 struct 와 같음)
type peerHdr struct {
	typ   uint8
	flags uint8
	rd    [8]byte
	addr  netip.Addr
	as    uint32
	bgpID [4]byte
	sec   uint32 // 0 이면 라우터가 시각을 주지 않음
	usec  uint32
}

func be16(b []byte) uint16 { return binary.BigEndian.Uint16(b) }
func be32(b []byte) uint32 { return binary.BigEndian.Uint32(b) }

// bmpHeader : 공통 머리를 검사하고 (전체 길이, 종류) 를 돌려줌
func bmpHeader(b []byte) (length int, typ uint8, err error) {
	if len(b) < bmpHdrLen {
		return 0, 0, errShort
	}
	if b[0] != 3 {
		return 0, 0, fmt.Errorf("bmp version %d", b[0])
	}
	n := be32(b[1:5])
	if n < bmpHdrLen || n > bmpMaxLen {
		return 0, 0, fmt.Errorf("bmp length %d", n)
	}
	return int(n), b[5], nil
}

// parsePeerHdr : per-peer 머리 42 바이트
func parsePeerHdr(b []byte) (peerHdr, error) {
	var h peerHdr
	if len(b) < peerHdrLen {
		return h, errShort
	}
	h.typ = b[0]
	h.flags = b[1]
	copy(h.rd[:], b[2:10])
	// Loc-RIB 에서는 0x80 이 V 가 아니라 F (filtered) 이고 주소는 0
	if h.flags&flagV != 0 && h.typ != peerTypeLocRIB {
		h.addr = netip.AddrFrom16([16]byte(b[10:26]))
	} else {
		h.addr = netip.AddrFrom4([4]byte(b[22:26]))
	}
	h.as = be32(b[26:30])
	copy(h.bgpID[:], b[30:34])
	h.sec = be32(b[34:38])
	h.usec = be32(b[38:42])
	return h, nil
}

// rdString : route distinguisher 글자 (openbmpd 와 같게 "0:0" 꼴)
func rdString(rd [8]byte) string {
	switch be16(rd[0:2]) {
	case 1: // IPv4 주소 4 + 번호 2
		return fmt.Sprintf("%d.%d.%d.%d:%d", rd[2], rd[3], rd[4], rd[5], be16(rd[6:8]))
	case 2: // 4 바이트 AS + 번호 2
		return fmt.Sprintf("%d:%d", be32(rd[2:6]), be16(rd[6:8]))
	default: // 0 : 2 바이트 AS + 번호 4
		return fmt.Sprintf("%d:%d", be16(rd[2:4]), be32(rd[4:8]))
	}
}

// bgpMsg : BGP 메시지 하나 (머리 19 바이트 : marker 16 + 길이 2 + 종류 1) 를 잘라
// (종류, 몸통, 나머지) 를 돌려줌
func bgpMsg(b []byte) (typ uint8, body []byte, rest []byte, err error) {
	if len(b) < 19 {
		return 0, nil, nil, errShort
	}
	n := int(be16(b[16:18]))
	if n < 19 || n > len(b) {
		return 0, nil, nil, fmt.Errorf("bgp length %d (have %d)", n, len(b))
	}
	return b[18], b[19:n], b[n:], nil
}
