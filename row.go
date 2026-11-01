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
	"fmt"
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

// addRow : 경로 추가 줄 (32 필드)
func addRow(sb *strings.Builder, seq uint64, rp *rowPeer, a *attrs, p prefix, v4 bool) {
	nh := a.mpNextHop
	if v4 && a.nextHop != "" {
		nh = a.nextHop
	}
	med := strconv.FormatUint(uint64(a.med), 10)
	lp := strconv.FormatUint(uint64(a.localPref), 10)
	baseHash := md5hex(a.asPath, nh, a.aggregator, a.origin, le32(a.med), le32(a.localPref), a.community, a.extComm, rp.peerHash)
	f := rowHead("add", seq, rp, p, v4)
	f = append(f, baseHash, rp.peerHash, rp.peerIP, u32(rp.peerAS), rp.ts, p.addr.String(), strconv.Itoa(p.length), b01(v4),
		a.origin, a.asPath, strconv.Itoa(a.asCount), originText(a), nh, med, lp,
		a.aggregator, a.community, a.extComm, a.clusterList, "0", b01(strings.Contains(nh, ".")), // isAtomicAgg : openbmpd 는 늘 0
		a.originatorID, u32(p.pathID), "", b01(rp.prePolicy), b01(rp.adjRibIn), a.largeComm)
	writeRow(sb, f)
}

// delRow : 경로 철회 줄 (속성 필드는 빈칸)
func delRow(sb *strings.Builder, seq uint64, rp *rowPeer, p prefix, v4 bool) {
	f := rowHead("del", seq, rp, p, v4)
	f = append(f, "", rp.peerHash, rp.peerIP, u32(rp.peerAS), rp.ts, p.addr.String(), strconv.Itoa(p.length), b01(v4))
	f = append(f, make([]string, 14)...) // 13 ~ 26
	f = append(f, u32(p.pathID), "", b01(rp.prePolicy), b01(rp.adjRibIn), "")
	writeRow(sb, f)
}

// rowHead : 필드 0 ~ 4 (action, seq, 줄 해시, router hash, router ip)
func rowHead(action string, seq uint64, rp *rowPeer, p prefix, v4 bool) []string {
	h := md5hex(p.addr.String(), string([]byte{byte(p.length)}), rp.peerHash)
	f := make([]string, 0, 32)
	return append(f, action, strconv.FormatUint(seq, 10), h, rp.routerHash, rp.routerIP)
}

func writeRow(sb *strings.Builder, f []string) {
	sb.WriteString(strings.Join(f, "\t"))
	sb.WriteByte('\n')
}

func u32(v uint32) string { return strconv.FormatUint(uint64(v), 10) }

// le32 : 숫자를 4 바이트 little-endian 으로 (해시 입력용)
func le32(v uint32) string {
	return string([]byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)})
}

// makeMsg : 줄들에 머리를 붙여 메시지로
func makeMsg(cHash, kind, key string, rows string, n int) outMsg {
	head := fmt.Sprintf("V: 1.7\nC_HASH_ID: %s\nT: %s\nL: %d\nR: %d\n\n", cHash, kind, len(rows), n)
	return outMsg{kind: kind, key: key, value: []byte(head + rows), rows: n}
}

// originText : origin_as 칸. 경로가 AS_SET 으로 끝나면 그 묶음 "{38266,45271}" (RFC 6811 : origin 을 하나로 정할 수 없음)
func originText(a *attrs) string {
	if a.originSet != "" {
		return a.originSet
	}
	return u32(a.originAS)
}
