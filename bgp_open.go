// Peer Up 의 BGP OPEN 해석 : AS, hold time, BGP ID, 능력 (4 바이트 AS, Add-Path, 글자 목록)
package main

import (
	"fmt"
	"net/netip"
	"slices"
	"strconv"
)

// AFI / SAFI
const (
	afiIPv4     = 1
	afiIPv6     = 2
	safiUnicast = 1
)

// openMsg : OPEN 하나에서 뽑은 것
type openMsg struct {
	as      uint32 // 4 바이트 AS 능력이 있으면 그 값, 없으면 My AS (2 바이트)
	hold    uint16
	bgpID   string
	as4     bool
	addPath map[uint32]uint8 // (afi<<8 | safi) -> 1 받기, 2 보내기, 3 둘 다
	mp      map[uint32]bool  // Multiprotocol 능력 (afi<<8 | safi). 비면 IPv4 unicast 만 (RFC 4760)
	caps    []string         // 능력 글자 (openbmpd 의 adv_cap / recv_cap, 순서 그대로)
}

// parseOpen : OPEN 몸통 (BGP 머리 뒤)
// 버전 1, My AS 2, hold 2, BGP ID 4, 옵션 길이 1, 옵션들 (종류 1 + 길이 1 + 값, 종류 2 = 능력 목록)
func parseOpen(b []byte) (openMsg, error) {
	o := openMsg{addPath: map[uint32]uint8{}, mp: map[uint32]bool{}}
	if len(b) < 10 {
		return o, errShort
	}
	o.as = uint32(be16(b[1:3]))
	o.hold = be16(b[3:5])
	o.bgpID = netip.AddrFrom4([4]byte(b[5:9])).String()
	optLen := int(b[9])
	opt := b[10:]
	if optLen > len(opt) {
		return o, fmt.Errorf("open opt length %d", optLen)
	}
	opt = opt[:optLen]
	for len(opt) >= 2 {
		t, n := opt[0], int(opt[1])
		if 2+n > len(opt) {
			return o, fmt.Errorf("open param length %d", n)
		}
		if t == 2 {
			parseCaps(opt[2:2+n], &o)
		}
		opt = opt[2+n:]
	}
	return o, nil
}

// parseCaps : 능력 = 코드 1 + 길이 1 + 값
func parseCaps(b []byte, o *openMsg) {
	for len(b) >= 2 {
		code, n := b[0], int(b[1])
		if 2+n > len(b) {
			return
		}
		v := b[2 : 2+n]
		switch code {
		case 1: // Multiprotocol : afi 2 + 예약 1 + safi 1
			if len(v) >= 4 {
				o.mp[uint32(be16(v))<<8|uint32(v[3])] = true
			}
		case 65: // 4 바이트 AS (RFC 6793)
			o.as4 = true
			if len(v) == 4 {
				o.as = be32(v)
			}
		case 69: // Add-Path (RFC 7911) : afi 2 + safi 1 + 보내기/받기 1 을 되풀이
			for i := 0; i+4 <= len(v); i += 4 {
				o.addPath[uint32(be16(v[i:]))<<8|uint32(v[i+2])] = v[i+3]
			}
		}
		o.caps = append(o.caps, capString(code, v))
		b = b[2+n:]
	}
}

// capString : 능력 하나의 글자 (openbmpd 와 같게, 표본으로 확인한 것 + 모르는 것은 번호만)
func capString(code uint8, v []byte) string {
	switch code {
	case 1: // Multiprotocol : afi 2 + 예약 1 + safi 1
		if len(v) >= 4 {
			afi, safi := be16(v), v[3]
			return fmt.Sprintf("MPBGP (1) : afi=%d safi=%d : %s", afi, safi, afiSafiName(afi, safi))
		}
	case 2:
		return "Route Refresh (2)"
	case 64:
		return "Graceful Restart (64)"
	case 65:
		return "4 Octet ASN (65)"
	case 70:
		return "Route Refresh Enhanced (70)"
	case 128:
		return "Route Refresh Old (128)"
	}
	return strconv.Itoa(int(code)) // 표본 : 5, 6, 9, 71 은 번호만 나옴
}

// afiSafiName : 표본에서 본 것 (1/1, 2/1, 1/4, 2/4, 2/128) + 1/128 은 추정
func afiSafiName(afi uint16, safi uint8) string {
	fam := "IPv4"
	if afi == afiIPv6 {
		fam = "IPv6"
	}
	switch safi {
	case 1:
		return "Unicast " + fam
	case 4:
		return "Labeled Unicast " + fam
	case 128:
		return "MPLS-Labeled VPN " + fam
	}
	return fmt.Sprintf("Unknown %s", fam)
}

// famOn : 두 OPEN 이 그 주소 종류를 함께 알림 (MP 능력이 둘 다 없으면 IPv4 unicast 만)
func famOn(sent, recv openMsg, afi uint16, safi uint8) bool {
	k := uint32(afi)<<8 | uint32(safi)
	on := func(o openMsg) bool {
		if len(o.mp) == 0 {
			return afi == afiIPv4 && safi == safiUnicast
		}
		return o.mp[k]
	}
	return on(sent) && on(recv)
}

// famsOn : 두 OPEN 이 함께 알린 경로 종류 모두 (0.3.0, MP 능력이 둘 다 없으면 IPv4 unicast 만), 번호 순
func famsOn(sent, recv openMsg) []family {
	if len(sent.mp) == 0 && len(recv.mp) == 0 {
		return []family{famOf(afiIPv4, safiUnicast)}
	}
	var out []family
	for k := range sent.mp {
		f := family(k)
		if famOn(sent, recv, f.afi(), f.safi()) {
			out = append(out, f)
		}
	}
	slices.Sort(out)
	return out
}

// addPathOn : 라우터가 peer 에게서 받은 경로에 path id 가 붙는지.
// peer 가 보내기 (2) 를 알리고 라우터가 받기 (1) 를 알렸을 때만.
func addPathOn(sent, recv openMsg, afi uint16, safi uint8) bool {
	k := uint32(afi)<<8 | uint32(safi)
	return sent.addPath[k]&1 != 0 && recv.addPath[k]&2 != 0
}
