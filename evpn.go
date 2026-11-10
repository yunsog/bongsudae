// evpn.go : EVPN (AFI 25 / SAFI 70, RFC 7432, 8365, 9136) 해석과 Kafka 줄 (0.3.0 2 차). 토픽 <prefix>.evpn
//
// 줄 = openbmp 스키마 evpn 40 칸 (https://www.openbmp.org/api/kafka_message_schema.html) :
//
//	0 action 1 seq 2 hash 3 router_hash 4 router_ip 5 base_attr_hash 6 peer_hash 7 peer_ip 8 peer_asn 9 timestamp
//	10 origin 11 as_path 12 as_path_count 13 origin_as 14 nexthop 15 med 16 local_pref 17 aggregator 18 community_list
//	19 ext_community_list 20 cluster_list 21 isAtomicAgg 22 isNextHopIPv4 23 originator_id 24 path_id 25 isPrePolicy 26 isAdjIn
//	27 rd 28 rd_type 29 originating_router_ip_len 30 originating_router_ip 31 ethernet_tag_id (16 진) 32 esi
//	33 mac_len 34 mac 35 ip_len 36 ip 37 mpls_label1 38 mpls_label2 39 large_community_list
//
// 경로 종류 (route type) 칸은 스키마에 없음 : 1 Ethernet A-D, 2 MAC/IP, 3 Inclusive Multicast, 4 Ethernet Segment, 5 IP Prefix.
// 5 (RFC 9136) 의 IP 프리픽스는 ip_len / ip 에 (길이 / 주소), gateway 는 TSV 칸이 없음. JSON 에는 route_type, gateway_ip 를 더함.
// 라벨 : MPLS 라벨 값 (20 비트). encapsulation ext community 가 VXLAN / NVGRE / VXLAN-GPE (8 / 9 / 12, RFC 8365) 면 24 비트 VNI.
// 경로표는 아직 없음 (Kafka 줄만, bmp.rib 에 l2vpn-evpn 을 적어도 기록 한 줄)
package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

const kindEVPN = "evpn"

var famEVPN = famOf(25, 70)

func init() {
	familyKinds[kindEVPN] = true
	nlriParsers[famEVPN] = nlriParser{reachEVPN, unreachEVPN}
}

// evpnRoute : EVPN NLRI 하나
type evpnRoute struct {
	typ     uint8
	pathID  uint32
	rd      [8]byte
	esi     [10]byte
	hasESI  bool
	tag     uint32
	hasTag  bool
	macLen  uint8
	mac     [6]byte
	ipLen   uint8 // 비트 (0 / 32 / 128, 종류 5 는 프리픽스 길이)
	ip      netip.Addr
	orig    netip.Addr // Originating Router's IP (종류 3, 4)
	gw      netip.Addr // 종류 5 gateway
	labels  [2][3]byte // 라벨 칸 바이트 그대로 (MPLS 라벨이냐 VNI 냐는 encapsulation 으로)
	nLabels int
}

func reachEVPN(u *update, nh, nlri []byte, o pathOpts) error {
	fu := u.famFor(famEVPN)
	fu.nextHop = nextHopText(nh)
	rs, err := parseEVPN(nlri, o.addPathFor(famEVPN))
	fu.eAdd = append(fu.eAdd, rs...)
	return err
}

func unreachEVPN(u *update, nlri []byte, o pathOpts) error {
	fu := u.famFor(famEVPN)
	if len(nlri) == 0 {
		fu.eor = true
		return nil
	}
	rs, err := parseEVPN(nlri, o.addPathFor(famEVPN))
	fu.eDel = append(fu.eDel, rs...)
	return err
}

// parseEVPN : [path ID] + 종류 1 + 길이 1 + 값. 모르는 종류는 건너뜀
func parseEVPN(b []byte, addPath bool) ([]evpnRoute, error) {
	var out []evpnRoute
	for len(b) > 0 {
		var r evpnRoute
		if addPath {
			if len(b) < 4 {
				return nil, errShort
			}
			r.pathID = be32(b)
			b = b[4:]
		}
		if len(b) < 2 || 2+int(b[1]) > len(b) {
			return nil, fmt.Errorf("evpn nlri length")
		}
		r.typ = b[0]
		v := b[2 : 2+int(b[1])]
		b = b[2+int(b[1]):]
		if err := r.parse(v); err != nil {
			return nil, fmt.Errorf("evpn route type %d : %w", r.typ, err)
		}
		if r.typ >= 1 && r.typ <= 5 {
			out = append(out, r)
		}
	}
	return out, nil
}

// parse : 종류마다 값 (RFC 7432 7.1 ~ 7.4, RFC 9136 3)
func (r *evpnRoute) parse(v []byte) error {
	d := &dec{b: v}
	ip := func(bits int) netip.Addr {
		switch bits {
		case 32:
			if t := d.take(4); t != nil {
				return netip.AddrFrom4([4]byte(t))
			}
		case 128:
			if t := d.take(16); t != nil {
				return netip.AddrFrom16([16]byte(t))
			}
		case 0:
			return netip.Addr{}
		default:
			d.fail()
		}
		return netip.Addr{} // 모자라면 take 가 d.err 에 남김
	}
	label := func() {
		if t := d.take(3); t != nil && r.nLabels < 2 {
			copy(r.labels[r.nLabels][:], t)
			r.nLabels++
		}
	}
	switch r.typ {
	case 1, 2, 3, 4, 5:
	default:
		return nil // 모르는 종류 : 건너뜀
	}
	copy(r.rd[:], d.take(8))
	if r.typ != 3 {
		copy(r.esi[:], d.take(10))
		r.hasESI = true
	}
	if r.typ != 4 {
		r.tag, r.hasTag = d.le32be(), true
	}
	switch r.typ {
	case 1:
		label()
	case 2:
		r.macLen = d.u8()
		copy(r.mac[:], d.take(6))
		r.ipLen = d.u8()
		r.ip = ip(int(r.ipLen))
		label()
		if len(d.b) >= 3 {
			label()
		}
	case 3, 4:
		n := d.u8()
		r.orig = ip(int(n))
	case 5: // 길이 34 = IPv4, 58 = IPv6
		r.ipLen = d.u8()
		w := 4
		if len(v) == 58 {
			w = 16
		}
		a := d.take(w)
		g := d.take(w)
		if d.err == nil {
			if w == 4 {
				r.ip, r.gw = netip.AddrFrom4([4]byte(a)), netip.AddrFrom4([4]byte(g))
			} else {
				r.ip, r.gw = netip.AddrFrom16([16]byte(a)), netip.AddrFrom16([16]byte(g))
			}
			if int(r.ipLen) > r.ip.BitLen() {
				return fmt.Errorf("ip prefix length %d", r.ipLen)
			}
		}
		label()
	}
	return d.err
}

// le32be : 4 바이트 big-endian (dec 의 le32 는 little-endian 상태 파일용)
func (d *dec) le32be() uint32 {
	if b := d.take(4); b != nil {
		return be32(b)
	}
	return 0
}

// vxlanEncap : ext community 글자에 VXLAN / NVGRE / VXLAN-GPE encapsulation 이 있는가 (RFC 8365 : 라벨 칸 = VNI 24 비트)
func vxlanEncap(ext string) bool {
	for _, w := range strings.Fields(ext) {
		if w == "encap=8" || w == "encap=9" || w == "encap=12" {
			return true
		}
	}
	return false
}

// labelValue : 라벨 칸 3 바이트 -> 값 (MPLS = 위 20 비트, VNI = 24 비트)
func labelValue(b [3]byte, vni bool) uint32 {
	v := uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
	if vni {
		return v
	}
	return v >> 4
}

// esiText : ESI 10 바이트 -> 스키마 글자 (종류 0 = 값 16 진, 1 ~ 5 = 두 부분을 공백으로)
func esiText(e [10]byte) string {
	mac := func(b []byte) string { return macText([6]byte(b)) }
	switch e[0] {
	case 0:
		return hex.EncodeToString(e[1:])
	case 1, 2: // CE LACP / 루트 브리지 MAC + 포트 키 / 우선순위 2
		return mac(e[1:7]) + " " + strconv.Itoa(int(be16(e[7:9])))
	case 3: // 시스템 MAC + local discriminator 3
		return mac(e[1:7]) + " " + strconv.Itoa(int(e[7])<<16|int(e[8])<<8|int(e[9]))
	case 4: // router ID + local discriminator 4
		return netip.AddrFrom4([4]byte(e[1:5])).String() + " " + strconv.FormatUint(uint64(be32(e[5:9])), 10)
	case 5: // AS + local discriminator 4
		return strconv.FormatUint(uint64(be32(e[1:5])), 10) + " " + strconv.FormatUint(uint64(be32(e[5:9])), 10)
	}
	return hex.EncodeToString(e[:])
}

func macText(m [6]byte) string {
	return fmt.Sprintf("%02x:%02x:%02x:%02x:%02x:%02x", m[0], m[1], m[2], m[3], m[4], m[5])
}

// evpnFields : 경로 하나의 NLRI 칸 (27 ~ 38) 과 JSON 에 더할 것
func (r *evpnRoute) fields(vni bool) (f [12]string) {
	f[0], f[1] = rdString(r.rd), strconv.Itoa(int(be16(r.rd[:2])))
	if r.orig.IsValid() {
		f[2], f[3] = strconv.Itoa(r.orig.BitLen()), r.orig.String()
	}
	if r.hasTag {
		f[4] = fmt.Sprintf("%08x", r.tag)
	}
	if r.hasESI {
		f[5] = esiText(r.esi)
	}
	if r.typ == 2 {
		f[6], f[7] = strconv.Itoa(int(r.macLen)), macText(r.mac)
	}
	if r.ip.IsValid() {
		f[8], f[9] = strconv.Itoa(int(r.ipLen)), r.ip.String()
	}
	for i := 0; i < r.nLabels; i++ {
		f[10+i] = strconv.FormatUint(uint64(labelValue(r.labels[i], vni)), 10)
	}
	return
}

// hash : 스키마 = peer hash, mac, mac 길이, ip, ip 길이, ESI, RD, path ID (0 이 아니면), 라벨이 있으면 1
func (r *evpnRoute) hash(peerHash string) string {
	h := md5.New()
	h.Write([]byte(peerHash))
	h.Write(r.mac[:])
	h.Write([]byte{r.macLen})
	if r.ip.IsValid() {
		h.Write(r.ip.AsSlice())
	}
	h.Write([]byte{r.ipLen})
	h.Write(r.esi[:])
	h.Write(r.rd[:])
	h.Write([]byte{r.typ}) // 종류가 다르면 같은 칸이어도 다른 경로
	if r.pathID != 0 {
		h.Write([]byte{byte(r.pathID >> 24), byte(r.pathID >> 16), byte(r.pathID >> 8), byte(r.pathID)})
	}
	if r.nLabels > 0 {
		h.Write([]byte{'1'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// evpnMsgs : UPDATE 의 EVPN 경로 -> Kafka 메시지 (add 메시지 + del 메시지, key = peer hash)
func (c *collector) evpnMsgs(r *router, rp *rowPeer, u *update, fu *famUpdate, out []outMsg) []outMsg {
	n := len(fu.eAdd) + len(fu.eDel)
	if n == 0 {
		return out
	}
	r.stat.famRows += n
	seq := c.seq.Add(uint64(n)) - uint64(n)
	a := &u.attrs
	vni := vxlanEncap(a.extComm)
	for _, del := range []bool{false, true} {
		rs := fu.eAdd
		if del {
			rs = fu.eDel
		}
		if len(rs) == 0 {
			continue
		}
		var b []byte
		for i := range rs {
			seq++
			b = c.evpnRow(b, seq, rp, a, fu.nextHop, &rs[i], del, vni)
		}
		out = append(out, c.makeMsg(kindEVPN, rp.peerHash, b, len(rs)))
	}
	return out
}

// evpnRow : 줄 하나 (openbmp TSV 40 칸 또는 JSON)
func (c *collector) evpnRow(b []byte, seq uint64, rp *rowPeer, a *attrs, nh string, r *evpnRoute, del, vni bool) []byte {
	action, baseHash := "add", ""
	attr := make([]string, 14) // 10 ~ 23 (del 은 빈칸)
	large := ""
	if del {
		action = "del"
	} else {
		baseHash = md5hex(a.asPath, nh, a.aggregator, a.origin, le32(a.med), le32(a.localPref), a.community, a.extComm, rp.peerHash)
		attr = []string{a.origin, a.asPath, strconv.Itoa(a.asCount), originText(a), nh, u32(a.med), u32(a.localPref),
			a.aggregator, a.community, a.extComm, a.clusterList, "0", b01(strings.Contains(nh, ".")), a.originatorID}
		large = a.largeComm
	}
	nf := r.fields(vni)
	if !c.json {
		f := []string{action, strconv.FormatUint(seq, 10), r.hash(rp.peerHash), rp.routerHash, rp.routerIP, baseHash,
			rp.peerHash, rp.peerIP, u32(rp.peerAS), rp.ts}
		f = append(f, attr...)
		f = append(f, u32(r.pathID), b01(rp.prePolicy), b01(rp.adjRibIn))
		f = append(f, nf[:]...)
		f = append(f, large)
		return append(joinTab(b, false, f...), '\n')
	}
	b = append(b, `{"action":`...)
	b = appendJSONStr(b, action)
	b = jsonKN(b, "seq", seq)
	b = jsonKV(b, "hash", r.hash(rp.peerHash))
	b = jsonKV(b, "router_hash", rp.routerHash)
	b = jsonKV(b, "router_ip", rp.routerIP)
	if !del {
		b = jsonKV(b, "base_attr_hash", baseHash)
	}
	b = jsonKV(b, "peer_hash", rp.peerHash)
	b = jsonKV(b, "peer_ip", rp.peerIP)
	b = jsonKN(b, "peer_asn", uint64(rp.peerAS))
	b = jsonKV(b, "timestamp", rp.ts)
	if !del {
		b = jsonKV(b, "origin", a.origin)
		b = jsonKV(b, "as_path", strings.TrimSpace(a.asPath))
		b = jsonKN(b, "as_path_count", uint64(a.asCount))
		if a.originSet != "" {
			b = jsonKV(b, "origin_as", a.originSet)
		} else {
			b = jsonKN(b, "origin_as", uint64(a.originAS))
		}
		b = jsonKV(b, "nexthop", nh)
		b = jsonKN(b, "med", uint64(a.med))
		b = jsonKN(b, "local_pref", uint64(a.localPref))
		b = jsonKV(b, "aggregator", a.aggregator)
		b = jsonKV(b, "community_list", a.community)
		b = jsonKV(b, "ext_community_list", a.extComm)
		b = jsonKV(b, "cluster_list", strings.TrimSpace(a.clusterList))
		b = jsonKB(b, "is_atomic_agg", false)
		b = jsonKB(b, "is_nexthop_ipv4", strings.Contains(nh, "."))
		b = jsonKV(b, "originator_id", a.originatorID)
	}
	b = jsonKN(b, "path_id", uint64(r.pathID))
	b = jsonKB(b, "is_pre_policy", rp.prePolicy)
	b = jsonKB(b, "is_adj_rib_in", rp.adjRibIn)
	b = jsonKN(b, "route_type", uint64(r.typ))
	b = jsonKV(b, "rd", nf[0])
	b = jsonKN(b, "rd_type", uint64(be16(r.rd[:2])))
	names := []string{"originating_router_ip_len", "originating_router_ip", "ethernet_tag_id", "esi", "mac_len", "mac", "ip_len", "ip", "mpls_label1", "mpls_label2"}
	num := map[string]bool{"originating_router_ip_len": true, "mac_len": true, "ip_len": true, "mpls_label1": true, "mpls_label2": true}
	for i, name := range names {
		v := nf[2+i]
		if v == "" {
			continue
		}
		if num[name] {
			n, _ := strconv.ParseUint(v, 10, 64)
			b = jsonKN(b, name, n)
		} else {
			b = jsonKV(b, name, v)
		}
	}
	if r.gw.IsValid() {
		b = jsonKV(b, "gateway_ip", r.gw.String())
	}
	if !del {
		b = jsonKV(b, "large_community_list", large)
	}
	return append(b, '}', '\n')
}
