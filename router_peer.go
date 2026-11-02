// openbmp v1.7 router / peer 메시지 (openbmpd 출력과 같게, 10-28 표본으로 확인)
//
// router (12 필드) : action seq name hash ip description term_code term_reason init_data term_data timestamp bgp_id
//
//	init : Initiation 메시지 (name = sysName, description = sysDescr)
//	term : Termination 메시지, 또는 TCP 가 그냥 끊김 (code 65535)
//	       name = 그 라우터가 Initiation 에서 알린 sysName (openbmpd 는 DNS 역조회 이름. 10-30 : rDNS 를 쓰지 않음)
//
// peer 의 name 은 빈칸 (openbmpd 는 peer 주소의 DNS 역조회 이름)
//
// peer (31 필드) : action seq hash router_hash name remote_bgp_id router_ip timestamp remote_asn remote_ip peer_rd
//
//	remote_port local_asn local_ip local_port local_bgp_id info_data adv_cap recv_cap remote_holddown adv_holddown
//	bmp_reason bgp_error_code bgp_error_sub_code error_text isL3VPN isPrePolicy isIPv4 isLocRIB isLocRIBFiltered table_name
//
//	up   : Peer Up (adv = 라우터가 보낸 OPEN, recv = peer 가 보낸 OPEN)
//	down : Peer Down (이유, NOTIFICATION 코드)
//
// openbmpd 는 router term 때 peer down 을 따로 내지 않음 -> 소비자가 "그 라우터의 peer 모두 down" 으로 처리
package main

import (
	"encoding/binary"
	"fmt"
	"net/netip"
	"strings"
	"time"
)

// 메시지 종류 (Kafka 토픽 = prefix + "." + 종류)
const (
	kindPrefix = "unicast_prefix"
	kindRouter = "router"
	kindPeer   = "peer"
)

var allKinds = []string{kindPrefix, kindRouter, kindPeer}

// clean : 필드 안의 줄바꿈 / 탭 제거 (openbmpd : 줄바꿈은 글자 "\n" 으로)
func clean(s string) string {
	s = strings.ReplaceAll(s, "\r", "")
	s = strings.ReplaceAll(s, "\n", `\n`)
	return strings.ReplaceAll(s, "\t", " ")
}

// tlvs : BMP 정보 TLV (종류 2 + 길이 2 + 값) -> 종류별 글자 (같은 종류는 공백으로 이음)
func tlvs(b []byte) map[uint16]string {
	m := map[uint16]string{}
	for len(b) >= 4 {
		t, n := be16(b), int(be16(b[2:]))
		if 4+n > len(b) {
			break
		}
		v := string(b[4 : 4+n])
		if m[t] != "" {
			v = m[t] + " " + v
		}
		m[t] = v
		b = b[4+n:]
	}
	return m
}

// routerMsg : router 메시지 하나
func (c *collector) routerMsg(r *router, action, name, descr, code, reason, initData, termData string, now time.Time) outMsg {
	f := []string{action, u32(r.routerSeq), clean(name), r.hash, r.ip, clean(descr), code, clean(reason),
		clean(initData), clean(termData), tsString(0, 0, now), ""}
	r.routerSeq++
	r.stat.routerMsgs++
	return c.lineMsg(kindRouter, r.hash, f)
}

// initiation : TLV 1 = sysDescr, 2 = sysName, 0 = 글자
func (c *collector) initiation(r *router, b []byte, now time.Time) []outMsg {
	r.reset(now) // Initiation = 새 BMP 세션 : peer 상태와 번호를 처음으로
	t := tlvs(b)
	r.sysName, r.sysDescr = clean(t[2]), clean(t[1])
	r.up, r.since, r.lastTerm = true, now, ""
	r.connects++
	return []outMsg{c.routerMsg(r, "init", t[2], t[1], "0", "", t[0], "", now)}
}

// termination : TLV 0 = 글자, 1 = 이유 번호 (2 바이트)
func (c *collector) termination(r *router, b []byte, now time.Time) []outMsg {
	r.terminated = true
	t := tlvs(b)
	code := uint16(0)
	if v := t[1]; len(v) == 2 {
		code = be16([]byte(v))
	}
	r.routerDown(now, fmt.Sprintf("term %d %s", code, termReason(code)))
	return []outMsg{c.routerMsg(r, "term", r.sysName, "", u32(uint32(code)), termReason(code), "", t[0], now)}
}

// routerClosed : TCP 가 Termination 없이 끊김 (tcp 입력만).
// why 가 있으면 collector 가 끊은 것 (reset) : openbmpd 가 스스로 끊을 때처럼 65534 + 이유 글자
func (c *collector) routerClosed(r *router, why string, now time.Time) []outMsg {
	if r.terminated {
		return nil
	}
	r.terminated = true
	if why != "" {
		r.lastReset, r.lastResetWhy = now, why
		r.routerDown(now, why)
		return []outMsg{c.routerMsg(r, "term", r.sysName, "", "65534", why, "", "", now)}
	}
	r.routerDown(now, "connection closed")
	return []outMsg{c.routerMsg(r, "term", r.sysName, "", "65535", "Connection closed", "", "", now)}
}

// termReason : RFC 7854 4.5 (openbmpd 글자는 표본에 없어 RFC 이름)
func termReason(code uint16) string {
	switch code {
	case 0:
		return "Session administratively closed"
	case 1:
		return "Unspecified reason"
	case 2:
		return "Out of resources"
	case 3:
		return "Redundant connection"
	case 4:
		return "Session permanently administratively closed"
	}
	return ""
}

// peerRow : peer 메시지 31 필드 중 공통 부분을 채운 목록
func (c *collector) peerRow(r *router, h *peerHdr, action string, now time.Time) []string {
	p := r.peerGet(h) // peer hash / 주소 글자 (없으면 만듦 : 바로 뒤 peerSession / markDown 도 만듦)
	ip := p.ipStr
	f := make([]string, 31)
	f[0], f[1] = action, u32(r.peerSeq)
	f[2], f[3] = p.key, r.hash
	// f[4] name : 빈칸 (10-30 사용자 : rDNS 이름은 쓸모없고 찾는 데 시간만 듦 -> 찾지 않음)
	f[5], f[6] = netip.AddrFrom4(h.bgpID).String(), r.ip
	f[7], f[8], f[9], f[10] = tsString(h.sec, h.usec, now), u32(h.as), ip, rdString(h.rd)
	f[25] = b01(h.typ == 1)                                   // isL3VPN (RD 인스턴스 peer)
	f[26] = b01(h.flags&flagL == 0)                           // isPrePolicy
	f[27] = b01(h.addr.Is4())                                 // isIPv4
	f[28] = b01(h.typ == peerTypeLocRIB)                      // isLocRIB
	f[29] = b01(h.typ == peerTypeLocRIB && h.flags&0x80 != 0) // isLocRIBFiltered
	r.peerSeq++
	r.stat.peerMsgs++
	return f
}

// peerUpMsg : Peer Up -> peer up 메시지 (local 주소 / 포트, 두 OPEN)
// sent / recv 가 nil 이면 (OPEN 없는 Peer Up) local_asn, local_bgp_id, 능력, holddown 은 빈칸
func (c *collector) peerUpMsg(r *router, h *peerHdr, local netip.Addr, lport, rport uint16, sent, recv *openMsg, now time.Time) outMsg {
	f := c.peerRow(r, h, "up", now)
	f[11], f[13], f[14] = u32(uint32(rport)), local.String(), u32(uint32(lport))
	if sent != nil && recv != nil {
		f[12], f[15] = u32(sent.as), sent.bgpID
		f[17], f[18] = strings.Join(sent.caps, ", "), strings.Join(recv.caps, ", ")
		f[19], f[20] = u32(uint32(recv.hold)), u32(uint32(sent.hold))
	}
	return c.lineMsg(kindPeer, f[2], f)
}

// peerDownMsg : Peer Down -> 이유 1 바이트 + (1, 3 이면 NOTIFICATION 메시지, 2 이면 FSM event 2 바이트)
// 2, 4 (NOTIFICATION 없음) 는 openbmpd 처럼 코드 0 / 0 과 설명 글자 (2 는 10-30 운영에서 확인, 4 는 openbmpd 소스 기준 추정)
func (c *collector) peerDownMsg(r *router, h *peerHdr, b []byte, now time.Time) outMsg {
	f := c.peerRow(r, h, "down", now)
	if len(b) >= 1 {
		f[21] = u32(uint32(b[0]))
		switch b[0] {
		case 1, 3:
			if typ, body, _, err := bgpMsg(b[1:]); err == nil && typ == 3 && len(body) >= 2 {
				f[22], f[23] = u32(uint32(body[0])), u32(uint32(body[1]))
				f[24] = notifyText(body[0], body[1])
			}
		case 2:
			fsm := 0
			if len(b) >= 3 {
				fsm = int(binary.BigEndian.Uint16(b[1:3]))
			}
			f[22], f[23] = "0", "0"
			f[24] = fmt.Sprintf("Local (%s) closed peer (%s) session: fsm_event=%d, No BGP notify message.", r.ip, f[9], fsm)
		case 4:
			f[22], f[23] = "0", "0"
			f[24] = fmt.Sprintf("Remote peer (%s) closed local (%s) session: No BGP notify message.", f[9], r.ip)
		}
	}
	return c.lineMsg(kindPeer, f[2], f)
}

// notifyText : BGP NOTIFICATION 코드 글자 (표본 : 4/0, 6/6. 나머지는 RFC 4271 / 4486 이름)
func notifyText(code, sub uint8) string {
	switch code {
	case 1:
		return "Message header error"
	case 2:
		return "OPEN message error"
	case 3:
		return "UPDATE message error"
	case 4:
		return "Hold timer expired"
	case 5:
		return "Finite state machine error"
	case 6:
		cease := []string{"", "Maximum number of prefixes reached", "Administratively shutdown", "Peer de-configured",
			"Administratively reset", "Connection rejected", "Other configuration change",
			"Connection collision resolution", "Out of resources"}
		if int(sub) < len(cease) && sub > 0 {
			return cease[sub]
		}
		return "Cease"
	}
	return fmt.Sprintf("Unknown %d/%d", code, sub)
}
