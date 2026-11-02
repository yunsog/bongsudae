// openbmp v1.7 unicast_prefix 줄과 메시지 만들기
//
// 메시지 = 머리 (V, C_HASH_ID, T, L, R) + 빈 줄 + 줄들 (탭 구분 32 필드, 끝 "\n")
// 해시는 openbmpd 와 같은 방식 (표본으로 확인) :
//
//	peer   = md5(peer_ip + rd 글자 + router_hash 글자)
//	줄     = md5(prefix 글자 + prefix 길이 1 바이트 + peer_hash 글자)
//	속성   = md5(as_path + nexthop + aggregator + origin + med + local_pref + community + ext_community + peer_hash)
//	         (med, local_pref 는 글자가 아니라 4 바이트 little-endian 숫자 = openbmpd 의 메모리 그대로)
package main

import (
	"crypto/md5"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

// outMsg : Kafka 로 보낼 메시지 하나
type outMsg struct {
	kind   string // kindPrefix / kindRouter / kindPeer (토픽 = prefix + "." + kind)
	key    string // peer hash (router 메시지는 router hash) : 같은 peer 는 같은 partition
	value  []byte
	rows   int
	topics []string // unicast_prefix 만 : 규칙으로 정한 토픽 이름들 (rules.go). 나머지는 kind 하나
}

// rowPeer : 줄마다 같은 peer / 라우터 정보
type rowPeer struct {
	routerHash string
	routerIP   string
	peerHash   string
	peerIP     string
	peerAS     uint32
	ts         string
	prePolicy  bool
	adjRibIn   bool
}

func md5hex(parts ...string) string {
	h := md5.New()
	for _, p := range parts {
		h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// tsString : per-peer 머리 시각 (0 이면 받은 시각 now), UTC 마이크로초
func tsString(sec, usec uint32, now time.Time) string {
	t := now
	if sec != 0 {
		t = time.Unix(int64(sec), int64(usec)*1000)
	}
	return t.UTC().Format("2006-01-02 15:04:05.000000")
}

func b01(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// ---- unicast_prefix 줄 (0.2.0 : 바이트에 바로 씀) ----
//
// 전에는 줄마다 필드 32 개를 글자로 만들어 strings.Join 으로 이었음 = 줄 하나에 할당 수십 번 (GC 가 CPU 의 약 30 %).
// 지금은 UPDATE 하나의 줄들이 같은 부분 (라우터 / peer / 속성 필드, 속성 해시) 을 UPDATE 마다 한 번 만들고 (rowEnc),
// 줄마다 다른 것 (seq, 줄 해시, 프리픽스, 길이, path id) 만 줄마다 써 넣음. 줄은 라우터마다 다시 쓰는 버퍼 (rowSink) 에 모으고,
// Kafka 로 보낼 메시지만 새로 할당 (보내기는 비동기라 버퍼를 다시 쓸 수 없음). 출력 바이트는 전과 같음 (regress).
//
// 줄 모양 (TSV, 필드 번호) :
//
//	0 action  1 seq  2 hash | segA[afi] = 3 ~ 9 | 10 prefix  11 len | segB[afi] = 12 ~ 26 | 27 path_id | segC = 28 ~ 31 + "\n"
//
// JSON (-format json) 도 같은 짜임 (jsonrow.go 의 rowEnc.initJSON)

// rowEnc : UPDATE 하나 (add 또는 del) 의 줄 만들기
type rowEnc struct {
	del    bool
	json   bool
	rp     *rowPeer
	a      *attrs
	segA   [2][]byte // afi 마다 (0 = IPv6, 1 = IPv4). 처음 쓸 때 만듦
	segB   [2][]byte
	segC   []byte
	ready  [2]bool
	hashIn []byte // 줄 해시 입력 (다시 씀)
}

func afiIdx(v4 bool) int {
	if v4 {
		return 1
	}
	return 0
}

// nextHopOf : 줄의 nexthop (IPv4 는 NEXT_HOP 속성, 없거나 IPv6 면 MP_REACH 의 next hop)
func nextHopOf(a *attrs, v4 bool) string {
	if v4 && a.nextHop != "" {
		return a.nextHop
	}
	return a.mpNextHop
}

// reset : 새 UPDATE (버퍼는 다시 씀)
func (e *rowEnc) reset(del, json bool, rp *rowPeer, a *attrs) {
	e.del, e.json, e.rp, e.a = del, json, rp, a
	e.ready = [2]bool{}
	e.segC = e.segC[:0]
}

// prepare : afi 의 같은 부분을 만듦 (UPDATE 마다 afi 마다 한 번)
func (e *rowEnc) prepare(v4 bool) {
	i := afiIdx(v4)
	if e.ready[i] {
		return
	}
	e.ready[i] = true
	if e.json {
		e.initJSON(v4)
		return
	}
	rp := e.rp
	baseHash := ""
	if !e.del {
		a := e.a
		baseHash = md5hex(a.asPath, nextHopOf(a, v4), a.aggregator, a.origin, le32(a.med), le32(a.localPref), a.community, a.extComm, rp.peerHash)
	}
	e.segA[i] = joinTab(e.segA[i][:0], true, rp.routerHash, rp.routerIP, baseHash, rp.peerHash, rp.peerIP, u32(rp.peerAS), rp.ts)
	if e.del {
		// 12 isIPv4, 13 ~ 26 빈칸 (openbmpd 처럼)
		e.segB[i] = append(append(append(e.segB[i][:0], b01(v4)...), "\t\t\t\t\t\t\t\t\t\t\t\t\t\t"...), '\t')
	} else {
		a := e.a
		nh := nextHopOf(a, v4)
		e.segB[i] = append(joinTab(e.segB[i][:0], false, b01(v4), a.origin, a.asPath, strconv.Itoa(a.asCount), originText(a), nh,
			u32(a.med), u32(a.localPref), a.aggregator, a.community, a.extComm, a.clusterList,
			"0", b01(strings.Contains(nh, ".")), a.originatorID), '\t') // isAtomicAgg : openbmpd 는 늘 0
	}
	if len(e.segC) == 0 {
		large := ""
		if !e.del {
			large = e.a.largeComm
		}
		e.segC = append(joinTab(e.segC, true, "", b01(rp.prePolicy), b01(rp.adjRibIn), large), '\n') // 28 labels 빈칸
	}
}

// joinTab : 글자들을 탭으로 이어 b 뒤에 (lead = 맨 앞에도 탭)
func joinTab(b []byte, lead bool, f ...string) []byte {
	for i, s := range f {
		if i > 0 || lead {
			b = append(b, '\t')
		}
		b = append(b, s...)
	}
	return b
}

// row : 줄 하나를 b 뒤에 씀
func (e *rowEnc) row(b []byte, seq uint64, p prefix, v4 bool) []byte {
	e.prepare(v4)
	i := afiIdx(v4)
	h := e.rowHash(p)
	if e.json {
		return e.rowJSON(b, seq, &h, p, i)
	}
	if e.del {
		b = append(b, "del\t"...)
	} else {
		b = append(b, "add\t"...)
	}
	b = strconv.AppendUint(b, seq, 10)
	b = append(b, '\t')
	b = append(b, h[:]...)
	b = append(b, e.segA[i]...)
	b = append(b, '\t')
	b = p.addr.AppendTo(b)
	b = append(b, '\t')
	b = strconv.AppendInt(b, int64(p.length), 10)
	b = append(b, '\t')
	b = append(b, e.segB[i]...)
	b = strconv.AppendUint(b, uint64(p.pathID), 10)
	return append(b, e.segC...)
}

// rowHash : 줄 해시 = md5(프리픽스 글자 + 길이 1 바이트 + peer hash), 글자 32 (할당 없이)
func (e *rowEnc) rowHash(p prefix) (h [32]byte) {
	in := p.addr.AppendTo(e.hashIn[:0])
	in = append(in, byte(p.length))
	in = append(in, e.rp.peerHash...)
	e.hashIn = in
	sum := md5.Sum(in)
	hex.Encode(h[:], sum[:])
	return
}

// rowSink : unicast_prefix 줄을 모아 메시지로 (add 또는 del 한 묶음). 라우터마다 하나를 다시 씀 (r.mu 안에서만).
// n = 0 : 메시지 하나, Kafka key = peer hash.
// n > 0 : 줄마다 칸 = FNV-1a("프리픽스/길이") % n, 칸마다 메시지 하나, key = "<peer hash>.<칸>" (Kafka key 1-B).
// 같은 router + peer + 프리픽스는 늘 같은 칸 = 같은 key = 같은 partition 이라 순서가 지켜지고,
// 큰 peer 하나의 줄이 여러 partition 으로 퍼짐. 메시지 순서 = 칸이 처음 나온 순서, 칸 안의 줄 순서 = 원래 순서
type rowSink struct {
	n     int
	buf   [][]byte // 칸마다 (n = 0 이면 하나). 길이만 0 으로 해서 다시 씀
	rows  []int    // 칸마다 줄 수
	order []int    // 처음 나온 칸 순서
	tmp   []byte   // prefixBucket 글자
}

// reset : 새 묶음 (n = 칸 수)
func (s *rowSink) reset(n int) {
	k := max(n, 1)
	if len(s.buf) < k {
		s.buf, s.rows = make([][]byte, k), make([]int, k)
	}
	for _, b := range s.order {
		s.buf[b], s.rows[b] = s.buf[b][:0], 0
	}
	s.n, s.order = n, s.order[:0]
}

// add : 줄 하나 (e 가 씀)
func (s *rowSink) add(e *rowEnc, seq uint64, p prefix, v4 bool) {
	b := 0
	if s.n > 0 {
		b = s.bucket(p)
	}
	if s.rows[b] == 0 {
		s.order = append(s.order, b)
	}
	s.rows[b]++
	s.buf[b] = e.row(s.buf[b], seq, p, v4)
}

// msgs : 모은 줄을 메시지로 만들어 out 뒤에 붙임
func (s *rowSink) msgs(out []outMsg, c *collector, peerHash string, topics []string) []outMsg {
	for _, b := range s.order {
		key := peerHash
		if s.n > 0 {
			key = peerHash + "." + strconv.Itoa(b)
		}
		m := c.makeMsg(kindPrefix, key, s.buf[b], s.rows[b])
		m.topics = topics
		out = append(out, m)
	}
	return out
}

// bucket : FNV-1a 32 ("1.2.3.0/24") % n. 고정 값 (partition 수와 무관, 판이 바뀌어도 같음)
func (s *rowSink) bucket(p prefix) int {
	t := p.addr.AppendTo(s.tmp[:0])
	t = append(t, '/')
	t = strconv.AppendInt(t, int64(p.length), 10)
	s.tmp = t
	return int(fnv1a(t) % uint32(s.n))
}

// prefixBucket : bucket 과 같음 (시험용)
func prefixBucket(p prefix, n int) int {
	s := rowSink{n: n}
	return s.bucket(p)
}

// fnv1a : FNV-1a 32 비트 (hash/fnv 와 같은 값, 할당 없이)
func fnv1a(b []byte) uint32 {
	h := uint32(2166136261)
	for _, c := range b {
		h ^= uint32(c)
		h *= 16777619
	}
	return h
}

// makeMsg : 줄들 (rows 바이트, 복사함) 로 메시지 하나. 할당은 값 하나뿐.
// openbmp 꼴 = 머리 (V, C_HASH_ID, T, L, R) + 빈 줄 + 줄들, JSON 꼴 = 줄들 그대로 (한 줄에 객체 하나)
func (c *collector) makeMsg(kind, key string, rows []byte, n int) outMsg {
	if c.json {
		return outMsg{kind: kind, key: key, value: append([]byte(nil), rows...), rows: n}
	}
	v := make([]byte, 0, 80+len(c.cHash)+len(kind)+len(rows))
	v = append(v, "V: 1.7\nC_HASH_ID: "...)
	v = append(v, c.cHash...)
	v = append(v, "\nT: "...)
	v = append(v, kind...)
	v = append(v, "\nL: "...)
	v = strconv.AppendInt(v, int64(len(rows)), 10)
	v = append(v, "\nR: "...)
	v = strconv.AppendInt(v, int64(n), 10)
	v = append(v, "\n\n"...)
	return outMsg{kind: kind, key: key, value: append(v, rows...), rows: n}
}

// originText : origin_as 칸. 경로가 AS 둘 이상인 AS_SET 으로 끝나면 그 묶음 "{38266,45271}" (RFC 6811 : origin 을 하나로 정할 수 없음). "{11139}" 은 11139
func originText(a *attrs) string {
	if a.originSet != "" {
		return a.originSet
	}
	return u32(a.originAS)
}

// maxKeyBuckets : -key-buckets 의 최대 (칸이 partition 수보다 많으면 고르게 퍼지는 효과는 더 없음)
const maxKeyBuckets = 64

func u32(v uint32) string { return strconv.FormatUint(uint64(v), 10) }

// le32 : 숫자를 4 바이트 little-endian 으로 (해시 입력용)
func le32(v uint32) string {
	return string([]byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)})
}
