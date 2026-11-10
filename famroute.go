// famroute.go : unicast 밖 경로 종류의 해석과 Kafka 줄 (0.3.0 1 차 : labeled unicast, L3VPN, multicast)
//
//	종류                         SAFI   NLRI                                         토픽 (<prefix>.)      줄
//	IPv4 / IPv6 labeled unicast   4      라벨 스택 + 프리픽스 (RFC 8277, 6PE = IPv6) labeled_unicast_prefix       unicast_prefix 32 칸, 28 labels
//	VPNv4 / VPNv6 (L3VPN)         128    라벨 스택 + RD 8 바이트 + 프리픽스 (RFC 4364, 4659) l3vpn          openbmp l3vpn 34 칸 (31 뒤에 RD, RD 종류)
//	IPv4 / IPv6 multicast         2      프리픽스 (unicast 와 같음)                   multicast_prefix      unicast_prefix 32 칸
//
// 토픽은 종류마다 고정 (전달 규칙은 unicast_prefix 만). 처음 쓸 때 만듦 (kafka.go). 경로표는 famrib.go. EVPN 은 evpn.go, FlowSpec 은 flowspec.go
// labels 칸 = 라벨 값 (20 비트) 을 쉼표로 (openbmp 스키마 "comma delimited list"). 철회의 호환 라벨 (0x800000, RFC 8277 2.4) 은 내지 않음
package main

import (
	"fmt"
	"net/netip"
	"strconv"
)

// 메시지 종류 (토픽 이름 끝)
const (
	kindLabeled   = "labeled_unicast_prefix"
	kindL3VPN     = "l3vpn"
	kindMulticast = "multicast_prefix"
)

// familyKinds : 경로 종류 토픽 (시작 때 만들지 않고 처음 쓸 때 만듦 : 우리 라우터가 보내지 않으면 생기지 않음)
var familyKinds = map[string]bool{kindLabeled: true, kindL3VPN: true, kindMulticast: true}

// famRoute : 경로 종류의 NLRI 하나
type famRoute struct {
	pfx    prefix
	rd     [8]byte  // L3VPN
	labels []uint32 // labeled unicast / L3VPN : 라벨 값 (20 비트), 위에서부터
}

// famUpdate : UPDATE 하나 안의 한 종류 (MP_REACH / MP_UNREACH)
type famUpdate struct {
	f        family
	add, del []famRoute
	nextHop  string // MP_REACH 의 next hop (VPN 은 RD 를 뗀 주소)
	eor      bool   // 그 종류의 End-of-RIB (빈 MP_UNREACH)

	eAdd, eDel   []evpnRoute // EVPN (evpn.go) : 프리픽스가 아니라 경로 종류마다 다른 칸
	fsAdd, fsDel []fsRule    // FlowSpec (flowspec.go) : 규칙
}

// count : 이 묶음의 경로 / 규칙 수
func (fu *famUpdate) count() int {
	return len(fu.add) + len(fu.del) + len(fu.eAdd) + len(fu.eDel) + len(fu.fsAdd) + len(fu.fsDel)
}

// famFor : 이 UPDATE 의 종류 f 묶음 (없으면 만듦)
func (u *update) famFor(f family) *famUpdate {
	for i := range u.fams {
		if u.fams[i].f == f {
			return &u.fams[i]
		}
	}
	u.fams = append(u.fams, famUpdate{f: f})
	return &u.fams[len(u.fams)-1]
}

// famSpec : 경로 종류 하나의 NLRI 모양과 토픽
type famSpec struct {
	afi     int
	labeled bool // 라벨 스택이 있음
	vpn     bool // RD 가 있음
	kind    string
}

var famSpecs = map[family]famSpec{
	famOf(afiIPv4, 4):   {afiIPv4, true, false, kindLabeled},
	famOf(afiIPv6, 4):   {afiIPv6, true, false, kindLabeled},
	famOf(afiIPv4, 128): {afiIPv4, true, true, kindL3VPN},
	famOf(afiIPv6, 128): {afiIPv6, true, true, kindL3VPN},
	famOf(afiIPv4, 2):   {afiIPv4, false, false, kindMulticast},
	famOf(afiIPv6, 2):   {afiIPv6, false, false, kindMulticast},
}

func init() { // 해석기 등록표 (family.go) 에 넣음
	for f, sp := range famSpecs {
		nlriParsers[f] = nlriParser{reachFam(f, sp), unreachFam(f, sp)}
	}
}

// reachFam : 종류 f 의 MP_REACH
func reachFam(f family, sp famSpec) func(u *update, nh, nlri []byte, o pathOpts) error {
	return func(u *update, nh, nlri []byte, o pathOpts) error {
		fu := u.famFor(f)
		if sp.vpn && len(nh) >= 12 { // VPN next hop = RD (늘 0) 8 바이트 + 주소 (RFC 4364 4.3.2, 4659 3.2)
			nh = nh[8:]
		}
		fu.nextHop = nextHopText(nh)
		rs, err := parseFamNLRI(nlri, sp, o.addPathFor(f), false)
		fu.add = append(fu.add, rs...)
		return err
	}
}

// unreachFam : 종류 f 의 MP_UNREACH (빈 것 = End-of-RIB)
func unreachFam(f family, sp famSpec) func(u *update, nlri []byte, o pathOpts) error {
	return func(u *update, nlri []byte, o pathOpts) error {
		fu := u.famFor(f)
		if len(nlri) == 0 {
			fu.eor = true
			return nil
		}
		rs, err := parseFamNLRI(nlri, sp, o.addPathFor(f), true)
		fu.del = append(fu.del, rs...)
		return err
	}
}

// maxLabels : 라벨 스택 최대 (틀린 입력 막기. RFC 8277 Multiple Labels 도 이 안)
const maxLabels = 16

// parseFamNLRI : [path ID] + 길이 (비트, 라벨 / RD 포함) + 라벨 스택 + [RD] + 프리픽스.
// 라벨 3 바이트 = 값 20 비트 + TC 3 + 맨 아래 표시 (S) 1. S 가 선 라벨까지. 철회의 0x800000 / 0 (호환 값) 은 한 칸으로 보고 내지 않음
func parseFamNLRI(b []byte, sp famSpec, addPath, withdraw bool) ([]famRoute, error) {
	maxBits := 32
	if sp.afi == afiIPv6 {
		maxBits = 128
	}
	var out []famRoute
	for len(b) > 0 {
		var r famRoute
		if addPath {
			if len(b) < 4 {
				return nil, errShort
			}
			r.pfx.pathID = be32(b)
			b = b[4:]
		}
		if len(b) < 1 {
			return nil, errShort
		}
		bits := int(b[0])
		total := (bits + 7) / 8
		if 1+total > len(b) {
			return nil, fmt.Errorf("nlri length %d bits", bits)
		}
		d := b[1 : 1+total]
		b = b[1+total:]
		if sp.labeled {
			for n := 0; ; n++ {
				if len(d) < 3 || bits < 24 || n >= maxLabels {
					return nil, fmt.Errorf("label stack")
				}
				v := uint32(d[0])<<16 | uint32(d[1])<<8 | uint32(d[2])
				d, bits = d[3:], bits-24
				if withdraw && (v == 0x800000 || v == 0) {
					break
				}
				r.labels = append(r.labels, v>>4)
				if v&1 != 0 { // S : 맨 아래
					break
				}
			}
		}
		if sp.vpn {
			if len(d) < 8 || bits < 64 {
				return nil, fmt.Errorf("route distinguisher")
			}
			copy(r.rd[:], d[:8])
			d, bits = d[8:], bits-64
		}
		if bits > maxBits || len(d) != (bits+7)/8 {
			return nil, fmt.Errorf("prefix length %d", bits)
		}
		var a [16]byte
		copy(a[:], d)
		if x := bits % 8; x != 0 { // 길이 뒤 남는 비트는 0 으로 (RFC 4271 4.3)
			a[len(d)-1] &= 0xff << (8 - x)
		}
		r.pfx.length = bits
		if sp.afi == afiIPv6 {
			r.pfx.addr = netip.AddrFrom16(a)
		} else {
			r.pfx.addr = netip.AddrFrom4([4]byte(a[:4]))
		}
		out = append(out, r)
	}
	return out, nil
}

// labelText : 라벨들 -> "16001,16002" (b 뒤에 붙임)
func labelText(b []byte, ls []uint32) []byte {
	for i, l := range ls {
		if i > 0 {
			b = append(b, ',')
		}
		b = strconv.AppendUint(b, uint64(l), 10)
	}
	return b
}

// famMsgs : UPDATE 의 경로 종류 묶음들 -> Kafka 메시지 (종류마다 add 메시지 + del 메시지, unicast 와 같은 짜임).
// 줄 번호 seq 는 unicast 와 같은 수열 (c.seq)
func (c *collector) famMsgs(r *router, rp *rowPeer, u *update, out []outMsg) []outMsg {
	for i := range u.fams {
		fu := &u.fams[i]
		if fu.f == famEVPN {
			out = c.evpnMsgs(r, rp, u, fu, out)
			continue
		}
		if _, ok := fsFams[fu.f]; ok {
			out = c.fsMsgs(r, rp, u, fu, out)
			continue
		}
		sp, ok := famSpecs[fu.f]
		if !ok || len(fu.add)+len(fu.del) == 0 {
			continue
		}
		n := len(fu.add) + len(fu.del)
		r.stat.famRows += n
		seq := c.seq.Add(uint64(n)) - uint64(n)
		a := u.attrs // 이 종류의 next hop (NEXT_HOP 속성은 IPv4 unicast 의 것)
		a.mpNextHop, a.nextHop = fu.nextHop, ""
		v4 := sp.afi == afiIPv4
		s, e := &r.sink, &r.enc
		for _, del := range []bool{false, true} {
			rs := fu.add
			if del {
				rs = fu.del
			}
			if len(rs) == 0 {
				continue
			}
			s.reset(c.keyBuckets)
			e.reset(del, c.json, rp, &a)
			e.ext, e.vpn = true, sp.vpn
			for _, x := range rs {
				seq++
				e.labels = labelText(e.labels[:0], x.labels)
				e.rd = x.rd
				s.add(e, seq, x.pfx, v4)
			}
			out = s.msgsKind(out, c, sp.kind, rp.peerHash, nil)
		}
		e.reset(false, false, nil, nil)
	}
	return out
}
