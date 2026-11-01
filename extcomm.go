// Extended community 글자 (RFC 4360, 5668, 7153, IANA "BGP Extended Communities" 등록부)
//
// 8 바이트마다 : 종류 1 + 하위 종류 1 + 값 6. 여럿이면 공백 하나로 이음.
// 이름 (rt=, soo= ...) 은 openbmpd 와 같게 두되 (소비자가 쓰는 글자), 값은 표준대로 정확히 (원칙 :
// openbmpd 의 버그는 흉내 내지 않고 고침). openbmpd 와 다른 점 (= openbmpd 의 버그를 고친 것) :
//
//   - 모르는 종류를 빈칸으로 버리지 않고 원래 바이트를 남김        0x0043:006400000001
//   - non-transitive 종류 (종류 바이트의 0x40 비트) 를 버리지 않음   (openbmpd : C 의 int 승격으로 위 비트를 못 지워 모두 빈칸)
//   - link-bw 의 대역폭은 IEEE float (바이트/초)                     link-bw=23456:250000  (openbmpd : 비트를 정수로 1215570944)
//   - flow-rate 도 IEEE float                                         (openbmpd : 정수를 float 로 바꿈)
//   - IPv4 종류의 colc / sas / link-bw 숫자는 바이트 순서 그대로      (openbmpd : little-endian 으로 뒤집음)
//   - 4 바이트 AS 종류의 import= 는 AS:번호                           (openbmpd : 2 바이트 AS 꼴로 잘못 나눔)
//   - L2VPN id (0x0a) 는 l2vpn-id= (openbmpd : Cisco vpn-id= 와 같은 이름)
//   - 옛 OSPF 꼬리표 (0x8000 / 0x8001 / 0x8005, RFC 4577) 를 버리지 않음
//   - EVPN MAC mobility / ESI label 은 공백 없는 하나의 글자, flags 는 숫자 (openbmpd : 공백이 들어가 꼬리표 둘로 보이고 flags 는 바이트 글자)
//   - encap (tunnel type) 은 2 바이트 (openbmpd : 1 바이트만)
//   - origin validation state (0x4300, RFC 8097) : ov=valid / not-found / invalid
package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"net/netip"
	"strconv"
	"strings"
)

// extCommString : 공백 구분
func extCommString(v []byte) string {
	var s []string
	for i := 0; i+8 <= len(v); i += 8 {
		s = append(s, extCommOne(v[i:i+8]))
	}
	return strings.Join(s, " ")
}

// extCommOne : 하나. 알면 이름=값, 모르면 원래 바이트 (0x종류하위:값 12 자리)
func extCommOne(e []byte) string {
	hi, sub, v := e[0], e[1], e[2:8]
	var s string
	if hi&0x80 == 0 { // IANA 가 정한 종류 (0x40 비트 = non-transitive, 값의 꼴은 같음)
		switch hi &^ 0x40 {
		case 0x00: // 2 바이트 AS
			s = extCommon(sub, extVals(v, false, false))
		case 0x01: // IPv4
			s = extCommon(sub, extVals(v, true, true))
		case 0x02: // 4 바이트 AS
			s = extCommon(sub, extVals(v, true, false))
		case 0x03:
			s = extOpaque(sub, v)
		case 0x06:
			s = extEVPN(sub, v)
		}
	} else { // 실험용 / 옛 종류 (0x80 ~ : flow spec, 옛 OSPF)
		switch hi {
		case 0x80:
			s = extGeneric(sub, v, extVals(v, false, false))
		case 0x81:
			s = extGeneric(sub, v, extVals(v, true, true))
		case 0x82:
			s = extGeneric(sub, v, extVals(v, true, false))
		}
	}
	if s == "" {
		s = fmt.Sprintf("0x%02x%02x:%x", hi, sub, v) // 모르는 종류 : 버리지 않고 바이트 그대로
	}
	return s
}

// extVal : 값 6 바이트 = global (2 바이트 AS / IPv4 / 4 바이트 AS) + local
type extVal struct {
	g4, ip bool   // global 이 4 바이트 (4 바이트 AS 또는 IPv4)
	global uint32 // 2 바이트 AS 이면 앞 2 바이트, 아니면 앞 4 바이트 (big-endian)
	local  uint32 // 2 바이트 AS 이면 뒤 4 바이트, 아니면 뒤 2 바이트
	addr   string // IPv4 종류의 주소 글자
}

func extVals(v []byte, g4, ip bool) extVal {
	x := extVal{g4: g4, ip: ip}
	if !g4 {
		x.global, x.local = uint32(be16(v[0:])), be32(v[2:])
		return x
	}
	x.global, x.local = be32(v[0:]), uint32(be16(v[4:]))
	if ip {
		x.addr = netip.AddrFrom4([4]byte(v[0:4])).String()
	}
	return x
}

// pair : "global:local" (IPv4 이면 주소:번호)
func (x extVal) pair() string {
	if x.ip {
		return x.addr + ":" + u32(x.local)
	}
	return u32(x.global) + ":" + u32(x.local)
}

// hexPair : vpn-id (local 을 0x 16 진수)
func (x extVal) hexPair() string {
	g := u32(x.global)
	if x.ip {
		g = x.addr
	}
	return g + ":0x" + strconv.FormatUint(uint64(x.local), 16)
}

// float32 바이트 (IEEE 754, big-endian) -> 글자 (정수면 소수점 없이)
func floatString(b []byte) string {
	return strconv.FormatFloat(float64(math.Float32frombits(binary.BigEndian.Uint32(b))), 'f', -1, 32)
}

// extCommon : 2 바이트 AS / IPv4 / 4 바이트 AS 종류의 하위 종류 (IANA 등록부)
func extCommon(sub uint8, x extVal) string {
	switch sub {
	case 0x02:
		return "rt=" + x.pair()
	case 0x03:
		return "soo=" + x.pair()
	case 0x04: // link bandwidth : 2 바이트 AS + 대역폭 (IEEE float, 바이트/초)
		if !x.g4 {
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, x.local)
			return "link-bw=" + u32(x.global) + ":" + floatString(b)
		}
	case 0x05:
		return "ospf-did=" + x.pair()
	case 0x07:
		return "ospf-rid=" + x.pair()
	case 0x08:
		return "colc=" + x.pair()
	case 0x09:
		return "sas=" + x.pair()
	case 0x0a:
		return "l2vpn-id=" + x.hexPair()
	case 0x0b:
		return "import=" + x.pair()
	case 0x10:
		return "vpn-id=" + x.hexPair()
	case 0x12:
		return "p2mp-nh=" + x.pair()
	}
	return ""
}

// extOpaque : 종류 0x03 / 0x43
func extOpaque(sub uint8, v []byte) string {
	switch sub {
	case 0x00: // origin validation state (RFC 8097)
		switch v[5] {
		case 0:
			return "ov=valid"
		case 1:
			return "ov=not-found"
		case 2:
			return "ov=invalid"
		}
	case 0x01: // cost community : 위치 1 + id 1 + 값 4
		poi := map[byte]string{128: "abs", 129: "igp", 130: "ext", 131: "bgp_id"}[v[0]]
		if poi == "" {
			poi = u32(uint32(v[0]))
		}
		return fmt.Sprintf("cost=%s:%d:%d", poi, v[1], be32(v[2:]))
	case 0x03:
		return "cp-orf"
	case 0x06: // OSPF route type (RFC 4577) : area 4 + 종류 1 + 옵션 1
		return ospfRT(v)
	case 0x0b: // color (RFC 9012) : flags 2 + color 4
		return fmt.Sprintf("color=%d", be32(v[2:]))
	case 0x0c: // encapsulation (RFC 9012) : 예약 4 + tunnel type 2
		return fmt.Sprintf("encap=%d", be16(v[4:]))
	case 0x0d:
		return "default-gw"
	}
	return ""
}

// ospfRT : OSPF route type 글자 (opaque 0x0306 과 옛 0x8000 이 같은 꼴)
func ospfRT(v []byte) string {
	return fmt.Sprintf("ospf-rt=area-%d:%s:%d", be32(v[0:]), ospfRouteType(v[4]), v[5])
}

// ospfRouteType : RFC 4577
func ospfRouteType(t uint8) string {
	switch t {
	case 1, 2:
		return "O"
	case 3:
		return "IA"
	case 5:
		return "E"
	case 7:
		return "N"
	}
	return u32(uint32(t))
}

// extEVPN : 종류 0x06 (RFC 7432 등)
func extEVPN(sub uint8, v []byte) string {
	switch sub {
	case 0x00: // MAC mobility : flags 1 + 예약 1 + 번호 4
		return fmt.Sprintf("mac-mob=%d:%d", v[0], be32(v[2:]))
	case 0x01: // ESI label : flags 1 + 예약 2 + label 3
		return fmt.Sprintf("esi-label=%d:%d", v[0], uint32(v[3])<<16|uint32(v[4])<<8|uint32(v[5]))
	case 0x02:
		return "es_import=" + macString(v)
	case 0x03:
		return "router_mac=" + macString(v)
	}
	return ""
}

// macString : aa:bb:cc:dd:ee:ff
func macString(v []byte) string {
	return fmt.Sprintf("%02x:%02x:%02x:%02x:%02x:%02x", v[0], v[1], v[2], v[3], v[4], v[5])
}

// extGeneric : 종류 0x80 / 0x81 / 0x82 (flow spec RFC 8955, 옛 OSPF RFC 4577)
func extGeneric(sub uint8, v []byte, x extVal) string {
	switch sub {
	case 0x00: // 옛 OSPF route type (0x8000)
		if !x.g4 {
			return ospfRT(v)
		}
	case 0x01: // 옛 OSPF router id (0x8001) : 주소 4 + 0
		if !x.g4 {
			return "ospf-rid=" + netip.AddrFrom4([4]byte(v[0:4])).String() + ":" + u32(uint32(be16(v[4:])))
		}
	case 0x05: // 옛 OSPF domain id (0x8005)
		return "ospf-did=" + x.pair()
	case 0x06: // traffic-rate : 2 바이트 AS + 속도 (IEEE float, 바이트/초)
		if !x.g4 {
			return "flow-rate=" + u32(x.global) + ":" + floatString(v[2:6])
		}
	case 0x07: // traffic-action : 마지막 바이트 S (0x02) / T (0x01)
		s := "flow-act="
		if v[5]&0x02 != 0 {
			s += "S"
		}
		if v[5]&0x01 != 0 {
			s += "T"
		}
		return s
	case 0x08: // redirect (rt 꼴)
		return "flow-redir=" + x.pair()
	case 0x09: // traffic-marking : DSCP 6 비트
		return fmt.Sprintf("flow-remark=%d", v[5]&0x3f)
	case 0x0a: // layer 2 info (RFC 4761) : encap 1 + flags 1 + MTU 2
		return fmt.Sprintf("l2info=%d:%d:mtu:%d", v[0], v[1], be16(v[2:]))
	}
	return ""
}
