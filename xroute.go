// SPDX-License-Identifier: Apache-2.0

// xroute.go : 0.4.0 에 더한 경로 종류의 공통 줄 짜임 (라우팅 테이블 없이 Kafka 줄만).
//
// 종류마다 칸 목록 (xSchema) 을 정하고, 해석기는 경로마다 NLRI 칸 글자 (xRoute.cols) 만 채움.
// 공통 칸 (action, seq, hash, router / peer, 속성, path_id ...) 은 줄을 만들 때 채움 (xRow).
//
//	토픽 (<prefix>.)                    종류                                       파일
//	ls_node, ls_link, ls_prefix         BGP-LS (16388/71, 72, 80)                  bgpls.go (openbmp 스키마)
//	l2vpn_vpls                          VPLS (25/65)                               vpls.go
//	mvpn                                MCAST-VPN (1, 2/5)                         mvpn.go
//	sr_policy                           SR Policy (1, 2/73)                        srpolicy.go
//	rt_constraint                       RT Constraint (1/132)                      rtc.go
//	other_family                        그 밖 모두 (MDT, MUP, CAR, 모르는 종류 ...) otherfam.go
//
// 칸 순서 = 토픽마다 docs/kafka.md 3-5 ~ 3-9 장. 봉수대가 정한 토픽은 앞 10 칸 + NLRI 칸 + 뒤 13 칸 (flowspec 과 같은 꼴)
package main

import (
	"crypto/md5"
	"encoding/hex"
	"strconv"
	"strings"
)

// xCol : 줄의 칸 하나
type xCol struct {
	name   string // 이름 (JSON 키)
	common bool   // 공통 칸 (xCommon 이 채움). 아니면 경로 칸 cols[idx]
	idx    int
	num    bool // JSON 에서 숫자
	flag   bool // JSON 에서 true / false (TSV 는 1 / 0)
}

// xSchema : 토픽 하나의 칸 목록
type xSchema struct {
	kind   string
	cols   []xCol
	nCols  int                        // 경로 칸 수
	attrFn func(u *update, r *xRoute) // add 줄에서 경로 속성으로 경로 칸을 채움 (BGP-LS 속성, Tunnel Encap). nil 이면 없음
	idxOf  map[string]int             // 경로 칸 이름 -> 번호
}

// 공통 칸 이름
var xCommonNames = map[string]bool{
	"action": true, "seq": true, "hash": true, "base_attr_hash": true, "router_hash": true, "router_ip": true,
	"peer_hash": true, "peer_ip": true, "peer_asn": true, "timestamp": true,
	"origin": true, "as_path": true, "as_path_count": true, "origin_as": true, "nexthop": true, "med": true,
	"local_pref": true, "community_list": true, "ext_community_list": true, "large_community_list": true,
	"path_id": true, "is_pre_policy": true, "is_adj_rib_in": true,
}

// 공통 칸 중 속성 (del 줄은 빈칸 / JSON 에서 빠짐)
var xAttrNames = map[string]bool{
	"base_attr_hash": true, "origin": true, "as_path": true, "as_path_count": true, "origin_as": true, "nexthop": true,
	"med": true, "local_pref": true, "community_list": true, "ext_community_list": true, "large_community_list": true,
}

var xNumNames = map[string]bool{"seq": true, "origin_as": true, "peer_asn": true, "as_path_count": true, "med": true, "local_pref": true, "path_id": true}
var xFlagNames = map[string]bool{"is_pre_policy": true, "is_adj_rib_in": true}

// 봉수대가 정한 토픽의 앞 10 칸 / 뒤 13 칸
var (
	xHead = []string{"action", "seq", "hash", "router_hash", "router_ip", "base_attr_hash", "peer_hash", "peer_ip", "peer_asn", "timestamp"}
	xTail = []string{"origin", "as_path", "as_path_count", "origin_as", "nexthop", "med", "local_pref",
		"community_list", "ext_community_list", "large_community_list", "path_id", "is_pre_policy", "is_adj_rib_in"}
)

// newSchema : 칸 이름들로 칸 목록. 공통 이름이 아니면 경로 칸 (이름 끝 '#' = 숫자, '?' = true / false)
func newSchema(kind string, names ...string) *xSchema {
	s := &xSchema{kind: kind, idxOf: map[string]int{}}
	for _, n := range names {
		c := xCol{name: n}
		switch {
		case strings.HasSuffix(n, "#"):
			c.name, c.num = n[:len(n)-1], true
		case strings.HasSuffix(n, "?"):
			c.name, c.flag = n[:len(n)-1], true
		}
		if xCommonNames[c.name] {
			c.common, c.num, c.flag = true, xNumNames[c.name], xFlagNames[c.name]
		} else {
			c.idx = s.nCols
			s.idxOf[c.name] = c.idx
			s.nCols++
		}
		s.cols = append(s.cols, c)
	}
	familyKinds[kind] = true
	return s
}

// ourSchema : 봉수대가 정한 토픽 = 앞 10 칸 + NLRI 칸 + 뒤 13 칸
func ourSchema(kind string, nlri ...string) *xSchema {
	names := append(append(append([]string{}, xHead...), nlri...), xTail...)
	return newSchema(kind, names...)
}

// xRoute : 경로 하나
type xRoute struct {
	s      *xSchema
	pathID uint32
	key    string   // 해시 재료 (NLRI 바이트)
	hash   string   // 정해 둔 해시 (BGP-LS node = node 열쇠). 비어 있으면 md5(peer hash, 종류, key, path id)
	cols   []string // 경로 칸
	refs   []xRef   // peer hash 로 만드는 칸 (BGP-LS 의 local / remote node hash)
}

// xRef : 칸 idx = nodeHash(peer hash, key)
type xRef struct {
	idx int
	key string
}

// newRoute : 칸이 빈 경로
func (s *xSchema) newRoute(pathID uint32, key []byte) xRoute {
	return xRoute{s: s, pathID: pathID, key: string(key), cols: make([]string, s.nCols)}
}

// set : 이름으로 경로 칸 채움 (없는 이름은 무시 : 토픽마다 칸이 조금씩 다름)
func (r *xRoute) set(name, v string) {
	if i, ok := r.s.idxOf[name]; ok {
		r.cols[i] = v
	}
}

func (r *xRoute) get(name string) string {
	if i, ok := r.s.idxOf[name]; ok {
		return r.cols[i]
	}
	return ""
}

// routeHash : 줄의 hash 칸
func (r *xRoute) routeHash(peerHash string) string {
	if r.hash != "" {
		return md5hex(peerHash, r.hash)
	}
	h := md5.New()
	h.Write([]byte(peerHash))
	h.Write([]byte(r.s.kind))
	h.Write([]byte(r.key))
	if r.pathID != 0 {
		h.Write([]byte{byte(r.pathID >> 24), byte(r.pathID >> 16), byte(r.pathID >> 8), byte(r.pathID)})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ---- 해석기 등록 ----

// xParseFn : NLRI 바이트 -> 경로들. raw = 모양을 몰라 hex 로만 낸 경로 수 (other_families 로 셈)
type xParseFn func(f family, nlri []byte, addPath bool) (rs []xRoute, raw int, err error)

// regX : 종류 f 의 해석기를 등록. stripRD = next hop 앞에 RD 8 바이트가 붙는 VPN 종류
func regX(f family, parse xParseFn, stripRD bool) {
	nlriParsers[f] = xParser(f, parse, stripRD)
}

func xParser(f family, parse xParseFn, stripRD bool) nlriParser {
	reach := func(u *update, nh, nlri []byte, o pathOpts) error {
		fu := u.famFor(f)
		if stripRD && (len(nh) == 12 || len(nh) == 24 || len(nh) == 48) {
			nh = nh[8:]
		}
		fu.nextHop = nextHopText(nh)
		rs, raw, err := parse(f, nlri, o.addPathFor(f))
		fu.xAdd = append(fu.xAdd, rs...)
		for range raw {
			u.other(f)
		}
		return err
	}
	unreach := func(u *update, nlri []byte, o pathOpts) error {
		fu := u.famFor(f)
		if len(nlri) == 0 {
			fu.eor = true
			return nil
		}
		rs, raw, err := parse(f, nlri, o.addPathFor(f))
		fu.xDel = append(fu.xDel, rs...)
		for range raw {
			u.other(f)
		}
		return err
	}
	return nlriParser{reach, unreach}
}

// ---- 줄 만들기 ----

// xMsgs : UPDATE 의 이 종류 경로 -> Kafka 메시지. 토픽마다 add 메시지 + del 메시지 (key = peer hash)
func (c *collector) xMsgs(r *router, rp *rowPeer, u *update, fu *famUpdate, out []outMsg) []outMsg {
	n := len(fu.xAdd) + len(fu.xDel)
	if n == 0 {
		return out
	}
	r.stat.famRows += n
	seq := c.seq.Add(uint64(n)) - uint64(n)
	for _, del := range []bool{false, true} {
		rs := fu.xAdd
		if del {
			rs = fu.xDel
		}
		// 토픽 순서는 처음 나온 순서 (BGP-LS 는 한 UPDATE 에 node / link / prefix 가 섞일 수 있음)
		var kinds []*xSchema
		for i := range rs {
			found := false
			for _, s := range kinds {
				found = found || s == rs[i].s
			}
			if !found {
				kinds = append(kinds, rs[i].s)
			}
		}
		for _, s := range kinds {
			var b []byte
			cnt := 0
			for i := range rs {
				if rs[i].s != s {
					continue
				}
				seq++
				cnt++
				b = c.xRow(b, seq, rp, u, fu.nextHop, &rs[i], del)
			}
			out = append(out, c.makeMsg(s.kind, rp.peerHash, b, cnt))
		}
	}
	return out
}

// xCommon : 공통 칸 글자
func xCommon(name string, seq uint64, rp *rowPeer, a *attrs, nh, hash, baseHash string, r *xRoute, del bool) string {
	if del && xAttrNames[name] {
		return ""
	}
	switch name {
	case "action":
		if del {
			return "del"
		}
		return "add"
	case "seq":
		return strconv.FormatUint(seq, 10)
	case "hash":
		return hash
	case "base_attr_hash":
		return baseHash
	case "router_hash":
		return rp.routerHash
	case "router_ip":
		return rp.routerIP
	case "peer_hash":
		return rp.peerHash
	case "peer_ip":
		return rp.peerIP
	case "peer_asn":
		return u32(rp.peerAS)
	case "timestamp":
		return rp.ts
	case "origin":
		return a.origin
	case "as_path":
		return a.asPath
	case "as_path_count":
		return strconv.Itoa(a.asCount)
	case "origin_as":
		return originText(a)
	case "nexthop":
		return nh
	case "med":
		return u32(a.med)
	case "local_pref":
		return u32(a.localPref)
	case "community_list":
		return a.community
	case "ext_community_list":
		return a.extComm
	case "large_community_list":
		return a.largeComm
	case "path_id":
		return u32(r.pathID)
	case "is_pre_policy":
		return b01(rp.prePolicy)
	case "is_adj_rib_in":
		return b01(rp.adjRibIn)
	}
	return ""
}

// xRow : 줄 하나 (TSV 또는 JSON). JSON 은 빈 경로 칸을 뺌
func (c *collector) xRow(b []byte, seq uint64, rp *rowPeer, u *update, nh string, r *xRoute, del bool) []byte {
	a := &u.attrs
	cols := r.cols
	if r.s.attrFn != nil && !del {
		cols = append([]string(nil), r.cols...) // 속성으로 채운 칸은 이 줄에만
		x := *r
		x.cols = cols
		r.s.attrFn(u, &x)
	}
	for _, ref := range r.refs {
		if cols[ref.idx] == "" && ref.key != "" {
			cols[ref.idx] = md5hex(rp.peerHash, ref.key)
		}
	}
	hash, baseHash := r.routeHash(rp.peerHash), ""
	if !del {
		baseHash = md5hex(a.asPath, nh, a.aggregator, a.origin, le32(a.med), le32(a.localPref), a.community, a.extComm, rp.peerHash)
	}
	if !c.json {
		f := make([]string, len(r.s.cols))
		for i, col := range r.s.cols {
			if col.common {
				f[i] = xCommon(col.name, seq, rp, a, nh, hash, baseHash, r, del)
			} else {
				f[i] = cols[col.idx]
			}
		}
		return append(joinTab(b, false, f...), '\n')
	}
	b = append(b, '{')
	first := true
	for _, col := range r.s.cols {
		var v string
		if col.common {
			if del && xAttrNames[col.name] {
				continue
			}
			v = xCommon(col.name, seq, rp, a, nh, hash, baseHash, r, del)
			if col.name == "as_path" {
				v = strings.TrimSpace(v)
			}
		} else {
			v = cols[col.idx]
			if v == "" {
				continue
			}
		}
		if !first {
			b = append(b, ',')
		}
		first = false
		b = appendJSONStr(b, col.name)
		b = append(b, ':')
		switch {
		case col.flag:
			b = append(b, map[bool]string{true: "true", false: "false"}[v == "1"]...)
		case col.num && isDigits(v):
			b = append(b, v...)
		default:
			b = appendJSONStr(b, v)
		}
	}
	return append(b, '}', '\n')
}

// isDigits : 0 ~ 9 만 (JSON 숫자로 써도 되는가. origin_as 의 "{1,2}" 는 글자로)
func isDigits(s string) bool {
	if s == "" || len(s) > 20 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
