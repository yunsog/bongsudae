// jsonrow.go : JSON 출력 (0.2.0, -format json / 설정 kafka.format = json)
//
// 토픽, Kafka key, 메시지 나누기 (UPDATE 하나 = add 메시지 + del 메시지, key_buckets) 는 openbmp 꼴과 같고
// 메시지 값만 다름 : 머리 (V / C_HASH_ID / T / L / R) 없이 한 줄에 JSON 객체 하나 (NDJSON, 줄마다 "\n").
// 필드 이름은 openbmp 스키마 이름 (snake_case), 값은 openbmp 꼴과 같은 글자를 형에 맞게 :
//
//	숫자 (seq, asn, 길이, med ...)  -> JSON 숫자          0 / 1 깃발 -> true / false
//	origin_as                       -> 숫자, AS_SET 이면 글자 "{a,b}"
//	as_path, cluster_list           -> 앞뒤 공백을 뗀 글자
//	del 줄                          -> 속성 필드 (base_attr_hash, origin ~ originator_id, large_community_list) 없음
//	router / peer                   -> 빈 숫자 / 깃발 필드는 뺌 (예 OPEN 없는 Peer Up 의 local_asn)
//
// bmp_raw 는 그대로 (BMP 바이트).
package main

import (
	"strconv"
	"strings"
	"unicode/utf8"
)

// appendJSONStr : 글자를 JSON 문자열로 ("..." , 제어 문자 / 잘못된 UTF-8 은 바꿈)
func appendJSONStr(b []byte, s string) []byte {
	b = append(b, '"')
	for i := 0; i < len(s); {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' && c < utf8.RuneSelf {
			b = append(b, c)
			i++
			continue
		}
		if c >= utf8.RuneSelf {
			r, n := utf8.DecodeRuneInString(s[i:])
			if r == utf8.RuneError && n == 1 {
				b = append(b, `�`...)
			} else {
				b = append(b, s[i:i+n]...)
			}
			i += n
			continue
		}
		switch c {
		case '"', '\\':
			b = append(b, '\\', c)
		case '\n':
			b = append(b, `\n`...)
		case '\t':
			b = append(b, `\t`...)
		case '\r':
			b = append(b, `\r`...)
		default:
			b = append(b, `\u00`...)
			b = append(b, "0123456789abcdef"[c>>4], "0123456789abcdef"[c&0xf])
		}
		i++
	}
	return append(b, '"')
}

// jsonKV : ,"이름":"값"
func jsonKV(b []byte, name, v string) []byte {
	b = append(b, ',', '"')
	b = append(b, name...)
	b = append(b, '"', ':')
	return appendJSONStr(b, v)
}

// jsonKN : ,"이름":숫자
func jsonKN(b []byte, name string, v uint64) []byte {
	b = append(b, ',', '"')
	b = append(b, name...)
	b = append(b, '"', ':')
	return strconv.AppendUint(b, v, 10)
}

// jsonKB : ,"이름":true|false
func jsonKB(b []byte, name string, v bool) []byte {
	b = append(b, ',', '"')
	b = append(b, name...)
	b = append(b, '"', ':')
	return strconv.AppendBool(b, v)
}

// initJSON : rowEnc.prepare 의 JSON 판 (afi 마다 같은 부분)
func (e *rowEnc) initJSON(v4 bool) {
	i := afiIdx(v4)
	rp := e.rp
	b := e.segA[i][:0]
	b = jsonKV(b, "router_hash", rp.routerHash)
	b = jsonKV(b, "router_ip", rp.routerIP)
	if !e.del {
		a := e.a
		b = jsonKV(b, "base_attr_hash", md5hex(a.asPath, nextHopOf(a, v4), a.aggregator, a.origin, le32(a.med), le32(a.localPref), a.community, a.extComm, rp.peerHash))
	}
	b = jsonKV(b, "peer_hash", rp.peerHash)
	b = jsonKV(b, "peer_ip", rp.peerIP)
	b = jsonKN(b, "peer_asn", uint64(rp.peerAS))
	b = jsonKV(b, "timestamp", rp.ts)
	e.segA[i] = append(b, `,"prefix":`...)

	b = jsonKB(e.segB[i][:0], "is_ipv4", v4)
	if !e.del {
		a := e.a
		nh := nextHopOf(a, v4)
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
		b = jsonKB(b, "is_atomic_agg", false) // openbmp 꼴과 같음 (늘 0)
		b = jsonKB(b, "is_nexthop_ipv4", strings.Contains(nh, "."))
		b = jsonKV(b, "originator_id", a.originatorID)
	}
	e.segB[i] = append(b, `,"path_id":`...)

	if len(e.segC) == 0 {
		b := jsonKV(e.segC, "labels", "")
		b = jsonKB(b, "is_pre_policy", rp.prePolicy)
		b = jsonKB(b, "is_adj_rib_in", rp.adjRibIn)
		if !e.del {
			b = jsonKV(b, "large_community_list", e.a.largeComm)
		}
		e.segC = append(b, '}', '\n')
	}
}

// rowJSON : 줄 하나 (JSON 객체 + "\n")
func (e *rowEnc) rowJSON(b []byte, seq uint64, h *[32]byte, p prefix, i int) []byte {
	if e.del {
		b = append(b, `{"action":"del","seq":`...)
	} else {
		b = append(b, `{"action":"add","seq":`...)
	}
	b = strconv.AppendUint(b, seq, 10)
	b = append(b, `,"hash":"`...)
	b = append(b, h[:]...)
	b = append(b, '"')
	b = append(b, e.segA[i]...)
	b = append(b, '"')
	b = p.addr.AppendTo(b)
	b = append(b, `","prefix_len":`...)
	b = strconv.AppendInt(b, int64(p.length), 10)
	b = append(b, e.segB[i]...)
	b = strconv.AppendUint(b, uint64(p.pathID), 10)
	return append(b, e.segC...)
}

// 필드 이름과 형 : s = 글자, n = 숫자, b = 0 / 1 깃발 (빈 n / b 는 뺌)
type jsonField struct{ name, typ string }

var routerFields = []jsonField{{"action", "s"}, {"seq", "n"}, {"name", "s"}, {"hash", "s"}, {"ip", "s"}, {"description", "s"},
	{"term_code", "n"}, {"term_reason", "s"}, {"init_data", "s"}, {"term_data", "s"}, {"timestamp", "s"}, {"bgp_id", "s"}}

var peerFields = []jsonField{{"action", "s"}, {"seq", "n"}, {"hash", "s"}, {"router_hash", "s"}, {"name", "s"},
	{"remote_bgp_id", "s"}, {"router_ip", "s"}, {"timestamp", "s"}, {"remote_asn", "n"}, {"remote_ip", "s"}, {"peer_rd", "s"},
	{"remote_port", "n"}, {"local_asn", "n"}, {"local_ip", "s"}, {"local_port", "n"}, {"local_bgp_id", "s"}, {"info_data", "s"},
	{"adv_cap", "s"}, {"recv_cap", "s"}, {"remote_holddown", "n"}, {"adv_holddown", "n"}, {"bmp_reason", "n"},
	{"bgp_error_code", "n"}, {"bgp_error_sub_code", "n"}, {"error_text", "s"}, {"is_l3vpn", "b"}, {"is_pre_policy", "b"},
	{"is_ipv4", "b"}, {"is_loc_rib", "b"}, {"is_loc_rib_filtered", "b"}, {"table_name", "s"}}

// lineMsg : router / peer 메시지 (줄 하나). f = openbmp 필드 글자들
func (c *collector) lineMsg(kind, key string, f []string) outMsg {
	var b []byte
	if !c.json {
		b = append(joinTab(nil, false, f...), '\n')
		return c.makeMsg(kind, key, b, 1)
	}
	spec := peerFields
	if kind == kindRouter {
		spec = routerFields
	}
	b = append(b, '{')
	first := true
	for i, fd := range spec {
		if i >= len(f) {
			break
		}
		v := f[i]
		if fd.typ != "s" && v == "" {
			continue
		}
		switch fd.typ {
		case "n":
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				b = jsonKV(b, fd.name, v) // 숫자가 아니면 글자 그대로 (없어야 함)
			} else {
				b = jsonKN(b, fd.name, n)
			}
		case "b":
			b = jsonKB(b, fd.name, v == "1")
		default:
			b = jsonKV(b, fd.name, v)
		}
		if first { // jsonK* 는 앞에 쉼표를 붙임 : 첫 필드는 "{," -> "{"
			b = append(b[:1], b[2:]...)
			first = false
		}
	}
	b = append(b, '}', '\n')
	return c.makeMsg(kind, key, b, 1)
}
