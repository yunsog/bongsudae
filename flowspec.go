// flowspec.go : FlowSpec (RFC 8955 IPv4, RFC 8956 IPv6, VPN = SAFI 134) 해석, Kafka 줄, 경로표 (0.3.0 2 차).
// 토픽 <prefix>.flowspec, 줄 = 봉수대가 정한 43 칸 (openbmp 스키마에 없음) : docs/kafka.md 3-4 장이 약속.
//
// NLRI 하나 = [path ID] + 길이 (1 바이트, 0xf0 이상이면 2 바이트 12 비트) + [RD 8 (VPN)] + 성분들 (종류 1 바이트 + 값).
// 성분 1 / 2 = 프리픽스 (IPv6 는 offset 이 있음), 3 ~ 13 = 연산자 목록 (연산자 1 바이트 + 값 1 / 2 / 4 / 8 바이트, 끝 비트까지).
// 모르는 성분이 있는 규칙은 그 규칙만 버림 (other_families 로 셈)
package main

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"unique"
)

const kindFlowSpec = "flowspec"

// FlowSpec 종류 (IPv4 / IPv6, VPN)
var fsFams = map[family]struct{ v4, vpn bool }{
	famOf(afiIPv4, 133): {true, false}, famOf(afiIPv6, 133): {false, false},
	famOf(afiIPv4, 134): {true, true}, famOf(afiIPv6, 134): {false, true},
}

func init() {
	familyKinds[kindFlowSpec] = true
	for f := range fsFams {
		nlriParsers[f] = nlriParser{reachFS(f), unreachFS(f)}
		ribSupported[f] = true
	}
}

// fsRule : 규칙 하나
type fsRule struct {
	pathID uint32
	rd     [8]byte
	raw    string     // 규칙 바이트 (RD 뒤 성분들 : 해시 / 경로표 열쇠)
	comp   [14]string // 성분 1 ~ 13 의 글자 (0 은 안 씀)
	dst    netip.Prefix
}

func reachFS(f family) func(u *update, nh, nlri []byte, o pathOpts) error {
	return func(u *update, nh, nlri []byte, o pathOpts) error {
		fu := u.famFor(f)
		fu.nextHop = nextHopText(nh)
		rs, bad, err := parseFS(nlri, f, o.addPathFor(f))
		fu.fsAdd = append(fu.fsAdd, rs...)
		for range bad {
			u.other(f)
		}
		return err
	}
}

func unreachFS(f family) func(u *update, nlri []byte, o pathOpts) error {
	return func(u *update, nlri []byte, o pathOpts) error {
		fu := u.famFor(f)
		if len(nlri) == 0 {
			fu.eor = true
			return nil
		}
		rs, bad, err := parseFS(nlri, f, o.addPathFor(f))
		fu.fsDel = append(fu.fsDel, rs...)
		for range bad {
			u.other(f)
		}
		return err
	}
}

// parseFS : NLRI 들 -> 규칙들. bad = 모르는 성분이 있어 버린 규칙 수. err = 길이가 틀림 (UPDATE 오류)
func parseFS(b []byte, f family, addPath bool) (out []fsRule, bad int, err error) {
	sp := fsFams[f]
	for len(b) > 0 {
		var r fsRule
		if addPath {
			if len(b) < 4 {
				return nil, 0, errShort
			}
			r.pathID = be32(b)
			b = b[4:]
		}
		if len(b) < 1 {
			return nil, 0, errShort
		}
		n, h := int(b[0]), 1
		if b[0] >= 0xf0 { // 2 바이트 길이 (12 비트)
			if len(b) < 2 {
				return nil, 0, errShort
			}
			n, h = int(b[0]&0x0f)<<8|int(b[1]), 2
		}
		if h+n > len(b) {
			return nil, 0, fmt.Errorf("flowspec length %d", n)
		}
		v := b[h : h+n]
		b = b[h+n:]
		if sp.vpn {
			if len(v) < 8 {
				return nil, 0, fmt.Errorf("flowspec route distinguisher")
			}
			copy(r.rd[:], v[:8])
			v = v[8:]
		}
		r.raw = string(v)
		if r.parse(v, sp.v4) != nil {
			bad++ // 모르는 / 깨진 성분 : 이 규칙만 버림
			continue
		}
		out = append(out, r)
	}
	return out, bad, nil
}

// parse : 성분들
func (r *fsRule) parse(v []byte, v4 bool) error {
	for len(v) > 0 {
		t := int(v[0])
		v = v[1:]
		if t < 1 || t > 13 || (v4 && t == 13) {
			return fmt.Errorf("component %d", t)
		}
		var err error
		switch {
		case t <= 2:
			var p netip.Prefix
			var txt string
			if p, txt, v, err = fsPrefix(v, v4); err != nil {
				return err
			}
			r.comp[t] = txt
			if t == 1 {
				r.dst = p
			}
		default:
			var txt string
			if txt, v, err = fsOps(v, t == 9 || t == 12, t); err != nil {
				return err
			}
			r.comp[t] = txt
		}
	}
	return nil
}

// fsPrefix : 성분 1 / 2. IPv4 = 길이 + 바이트, IPv6 = 길이 + offset + (길이 - offset) 비트
func fsPrefix(v []byte, v4 bool) (netip.Prefix, string, []byte, error) {
	if v4 {
		if len(v) < 1 || v[0] > 32 || 1+(int(v[0])+7)/8 > len(v) {
			return netip.Prefix{}, "", nil, errShort
		}
		n := int(v[0])
		var a [4]byte
		copy(a[:], v[1:1+(n+7)/8])
		p := netip.PrefixFrom(netip.AddrFrom4(a), n).Masked()
		return p, p.String(), v[1+(n+7)/8:], nil
	}
	if len(v) < 2 || v[0] > 128 || v[1] > v[0] {
		return netip.Prefix{}, "", nil, errShort
	}
	n, off := int(v[0]), int(v[1])
	nb := (n - off + 7) / 8
	if 2+nb > len(v) {
		return netip.Prefix{}, "", nil, errShort
	}
	var a [16]byte
	for i := 0; i < n-off; i++ { // pattern 비트를 offset 자리부터 놓음
		if v[2+i/8]&(0x80>>(i%8)) != 0 {
			j := off + i
			a[j/8] |= 0x80 >> (j % 8)
		}
	}
	p := netip.PrefixFrom(netip.AddrFrom16(a), n)
	txt := p.String()
	if off != 0 {
		txt += "@" + strconv.Itoa(off)
	}
	return p, txt, v[2+nb:], nil
}

var (
	tcpFlagNames  = []string{"fin", "syn", "rst", "psh", "ack", "urg", "ece", "cwr"}
	fragFlagNames = []string{"df", "isf", "ff", "lf"}
)

// fsOps : 연산자 목록 -> 글자 (docs/kafka.md 3-4). bits = 비트 성분 (9 TCP flags, 12 fragment)
func fsOps(v []byte, bits bool, t int) (string, []byte, error) {
	var sb strings.Builder
	for first := true; ; first = false {
		if len(v) < 1 {
			return "", nil, errShort
		}
		op := v[0]
		n := 1 << ((op >> 4) & 3)
		if 1+n > len(v) {
			return "", nil, errShort
		}
		var val uint64
		for _, c := range v[1 : 1+n] {
			val = val<<8 | uint64(c)
		}
		v = v[1+n:]
		if !first {
			if op&0x40 != 0 {
				sb.WriteByte('&')
			} else {
				sb.WriteByte(',')
			}
		}
		if bits {
			if op&0x02 != 0 {
				sb.WriteByte('!')
			}
			if op&0x01 != 0 {
				sb.WriteByte('=')
			}
			names := tcpFlagNames
			if t == 12 {
				names = fragFlagNames
			}
			sb.WriteString(flagText(val, names))
		} else {
			sb.WriteString([8]string{"false", "=", ">", ">=", "<", "<=", "!=", "true"}[op&0x07])
			if op&0x07 != 0 && op&0x07 != 7 {
				sb.WriteString(strconv.FormatUint(val, 10))
			}
		}
		if op&0x80 != 0 { // 끝
			return sb.String(), v, nil
		}
	}
}

// flagText : 비트 -> "syn|ack" (이름 없는 비트는 0x..)
func flagText(val uint64, names []string) string {
	var p []string
	for i, n := range names {
		if val&(1<<i) != 0 {
			p = append(p, n)
			val &^= 1 << i
		}
	}
	if val != 0 {
		p = append(p, fmt.Sprintf("0x%x", val))
	}
	if len(p) == 0 {
		return "0"
	}
	return strings.Join(p, "|")
}

// 성분 이름 (rule 칸)
var fsCompNames = [14]string{"", "dst", "src", "proto", "port", "dport", "sport", "icmp-type", "icmp-code", "tcp-flags", "len", "dscp", "frag", "flow-label"}

// JSON 이름 (13 ~ 25 칸 이름과 같음)
var fsCompJSON = [14]string{"", "dst_prefix", "src_prefix", "protocol", "port", "dst_port", "src_port", "icmp_type", "icmp_code", "tcp_flags", "packet_length", "dscp", "fragment", "flow_label"}

// ruleText : 규칙 전체 글자
func (r *fsRule) ruleText() string {
	var p []string
	for t := 1; t <= 13; t++ {
		if r.comp[t] != "" {
			p = append(p, fsCompNames[t]+" "+r.comp[t])
		}
	}
	return strings.Join(p, " ")
}

// hash : md5(isIPv4 + RD + 규칙 바이트 + peer hash + [path ID])
func (r *fsRule) hash(v4 bool, peerHash string) string {
	h := md5.New()
	h.Write([]byte(b01(v4)))
	h.Write(r.rd[:])
	h.Write([]byte(r.raw))
	h.Write([]byte(peerHash))
	if r.pathID != 0 {
		h.Write(binary.BigEndian.AppendUint32(nil, r.pathID))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// fsActions : ext community 글자에서 동작 (traffic_rate, traffic_action, redirect, traffic_marking)
func fsActions(ext string) (act [4]string) {
	for _, w := range strings.Fields(ext) {
		k, v, ok := strings.Cut(w, "=")
		if !ok {
			continue
		}
		switch k {
		case "flow-rate": // AS:속도
			if _, rate, ok := strings.Cut(v, ":"); ok {
				act[0] = rate
			}
		case "flow-act":
			act[1] = v
		case "flow-redir":
			act[2] = v
		case "flow-remark":
			act[3] = v
		}
	}
	return
}

// fsMsgs : UPDATE 의 FlowSpec 규칙 -> Kafka 메시지 (add 메시지 + del 메시지, key = peer hash)
func (c *collector) fsMsgs(r *router, rp *rowPeer, u *update, fu *famUpdate, out []outMsg) []outMsg {
	n := len(fu.fsAdd) + len(fu.fsDel)
	if n == 0 {
		return out
	}
	r.stat.famRows += n
	seq := c.seq.Add(uint64(n)) - uint64(n)
	sp := fsFams[fu.f]
	for _, del := range []bool{false, true} {
		rs := fu.fsAdd
		if del {
			rs = fu.fsDel
		}
		if len(rs) == 0 {
			continue
		}
		var b []byte
		for i := range rs {
			seq++
			b = c.fsRow(b, seq, rp, &u.attrs, fu.nextHop, &rs[i], sp.v4, sp.vpn, del)
		}
		out = append(out, c.makeMsg(kindFlowSpec, rp.peerHash, b, len(rs)))
	}
	return out
}

// fsRow : 줄 하나 (TSV 43 칸 또는 JSON)
func (c *collector) fsRow(b []byte, seq uint64, rp *rowPeer, a *attrs, nh string, r *fsRule, v4, vpn, del bool) []byte {
	action, baseHash, rd := "add", "", ""
	if vpn {
		rd = rdString(r.rd)
	}
	var act [4]string
	attr := make([]string, 10) // 30 ~ 39 (del 은 빈칸)
	if del {
		action = "del"
	} else {
		baseHash = md5hex(a.asPath, nh, a.aggregator, a.origin, le32(a.med), le32(a.localPref), a.community, a.extComm, rp.peerHash)
		act = fsActions(a.extComm)
		attr = []string{a.origin, a.asPath, strconv.Itoa(a.asCount), originText(a), nh, u32(a.med), u32(a.localPref),
			a.community, a.extComm, a.largeComm}
	}
	if !c.json {
		f := []string{action, strconv.FormatUint(seq, 10), r.hash(v4, rp.peerHash), rp.routerHash, rp.routerIP, baseHash,
			rp.peerHash, rp.peerIP, u32(rp.peerAS), rp.ts, b01(v4), rd, r.ruleText()}
		f = append(f, r.comp[1:]...)
		f = append(f, act[:]...)
		f = append(f, attr...)
		f = append(f, u32(r.pathID), b01(rp.prePolicy), b01(rp.adjRibIn))
		return append(joinTab(b, false, f...), '\n')
	}
	b = append(b, `{"action":`...)
	b = appendJSONStr(b, action)
	b = jsonKN(b, "seq", seq)
	b = jsonKV(b, "hash", r.hash(v4, rp.peerHash))
	b = jsonKV(b, "router_hash", rp.routerHash)
	b = jsonKV(b, "router_ip", rp.routerIP)
	if !del {
		b = jsonKV(b, "base_attr_hash", baseHash)
	}
	b = jsonKV(b, "peer_hash", rp.peerHash)
	b = jsonKV(b, "peer_ip", rp.peerIP)
	b = jsonKN(b, "peer_asn", uint64(rp.peerAS))
	b = jsonKV(b, "timestamp", rp.ts)
	b = jsonKB(b, "is_ipv4", v4)
	if rd != "" {
		b = jsonKV(b, "rd", rd)
	}
	b = jsonKV(b, "rule", r.ruleText())
	for t := 1; t <= 13; t++ {
		if r.comp[t] != "" {
			b = jsonKV(b, fsCompJSON[t], r.comp[t])
		}
	}
	for i, name := range []string{"traffic_rate", "traffic_action", "redirect", "traffic_marking"} {
		if act[i] == "" {
			continue
		}
		if n, err := strconv.ParseFloat(act[i], 64); err == nil && (i == 0 || i == 3) {
			b = append(b, ',', '"')
			b = append(b, name...)
			b = append(b, '"', ':')
			b = strconv.AppendFloat(b, n, 'f', -1, 64)
		} else {
			b = jsonKV(b, name, act[i])
		}
	}
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
		b = jsonKV(b, "community_list", a.community)
		b = jsonKV(b, "ext_community_list", a.extComm)
		b = jsonKV(b, "large_community_list", a.largeComm)
	}
	b = jsonKN(b, "path_id", uint64(r.pathID))
	b = jsonKB(b, "is_pre_policy", rp.prePolicy)
	b = jsonKB(b, "is_adj_rib_in", rp.adjRibIn)
	return append(b, '}', '\n')
}

// ---- 경로표 (bmp.rib 에 ipv4-flowspec 등을 적을 때) ----

// fsKey : (RD, 규칙 바이트) + 목적지 프리픽스 (찾기용, 규칙에서 정해짐)
type fsKey struct {
	rd   [8]byte
	rule string
	dst  netip.Prefix
}

func (k fsKey) prefix() netip.Prefix { return k.dst }

// fsTable : FlowSpec 종류 하나의 경로표 (famTable)
type fsTable struct {
	f family
	t ribTable[fsKey]
}

func (t *fsTable) size() int  { return t.t.size() }
func (t *fsTable) paths() int { return t.t.paths() }

func (t *fsTable) apply(fu *famUpdate, a *attrs, tm uint32, addPath bool) {
	v4 := fsFams[t.f].v4
	for i := range fu.fsAdd {
		r := &fu.fsAdd[i]
		pa := attrOf(a, v4)
		pa.labels = r.ruleText() // 규칙 글자를 속성에 (API 줄의 rule)
		rt := route{unique.Make(pa), tm}
		k := fsKey{r.rd, r.raw, r.dst}
		if addPath {
			t.t.setPath(k, r.pathID, rt)
		} else {
			t.t.set(k, rt)
		}
	}
	for i := range fu.fsDel {
		r := &fu.fsDel[i]
		k := fsKey{r.rd, r.raw, r.dst}
		if addPath {
			t.t.delPath(k, r.pathID)
		} else {
			t.t.del(k)
		}
	}
}

func (t *fsTable) each(fn func(famRow) bool) {
	vpn := fsFams[t.f].vpn
	for k, p := range t.t.all() {
		if !fn(famRow{pfx: k.dst, rd: k.rd, vpn: vpn, p: p, rule: true}) {
			return
		}
	}
}

// encode : 경로 수 + 경로마다 (RD 8, 규칙 길이 + 바이트, Add-Path 표시, [path ID], 속성 번호, 시각)
func (t *fsTable) encode(b []byte, num func(unique.Handle[pathAttr]) uint64) []byte {
	b = binary.AppendUvarint(b, uint64(t.t.paths()))
	for k, p := range t.t.all() {
		b = append(b, k.rd[:]...)
		b = appendStr(b, k.rule)
		if p.addPath {
			b = binary.AppendUvarint(append(b, 1), uint64(p.id))
		} else {
			b = append(b, 0)
		}
		b = binary.AppendUvarint(b, num(p.rt.a))
		b = binary.LittleEndian.AppendUint32(b, p.rt.t)
	}
	return b
}

func (t *fsTable) decode(d *dec, handle func() unique.Handle[pathAttr]) {
	v4 := fsFams[t.f].v4
	for range d.count(14) {
		var k fsKey
		copy(k.rd[:], d.take(8))
		k.rule = d.str()
		var r fsRule
		if r.parse([]byte(k.rule), v4) == nil {
			k.dst = r.dst
		}
		ap := d.u8() == 1
		var id uint32
		if ap {
			id = d.u32()
		}
		rt := route{handle(), d.le32()}
		if d.err != nil {
			return
		}
		if ap {
			t.t.setPath(k, id, rt)
		} else {
			t.t.set(k, rt)
		}
	}
}
