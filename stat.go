// 통계 : 1 분마다 기록에 한 줄
package main

import (
	"fmt"
	"strings"
)

type stats struct {
	bmpType    [bmpTypeCount + 1]int // BMP 종류별 건수 (마지막 = 모르는 종류)
	errors     int                   // 해석 오류 메시지 수
	others     int                   // unicast 가 아닌 MP_REACH / UNREACH (VPN 등, 버림)
	add        int                   // add 줄
	del        int                   // del 줄
	msgs       int                   // 보낸 Kafka 메시지
	routerMsgs int                   // 그중 router 메시지
	peerMsgs   int                   // 그중 peer 메시지
	noOpen     int                   // OPEN 이 없는 Peer Up (RFC 7854 위반, 주소만으로 peer up 을 냄)
	dropped    int                   // 규칙 (보내지 않음) 으로 Kafka 에 보내지 않은 줄
}

// String : "route_mon=123 peer_up=1 ... add=10 del=2" (0 인 것은 뺌)
func (s *stats) String() string {
	var p []string
	for i, n := range s.bmpType {
		if n == 0 {
			continue
		}
		name := "unknown"
		if i < bmpTypeCount {
			name = bmpTypeName[i]
		}
		p = append(p, fmt.Sprintf("%s=%d", name, n))
	}
	p = append(p, fmt.Sprintf("errors=%d others=%d add=%d del=%d msgs=%d router_msgs=%d peer_msgs=%d",
		s.errors, s.others, s.add, s.del, s.msgs, s.routerMsgs, s.peerMsgs))
	if s.dropped > 0 {
		p = append(p, fmt.Sprintf("not_sent=%d", s.dropped))
	}
	if s.noOpen > 0 {
		p = append(p, fmt.Sprintf("peer_up_no_open=%d", s.noOpen))
	}
	return strings.Join(p, " ")
}

// addAll : o 를 s 에 더함 (1 분 통계 : 라우터마다의 통계를 합침)
func (s *stats) addAll(o *stats) {
	for i := range s.bmpType {
		s.bmpType[i] += o.bmpType[i]
	}
	s.errors += o.errors
	s.others += o.others
	s.add += o.add
	s.del += o.del
	s.msgs += o.msgs
	s.routerMsgs += o.routerMsgs
	s.peerMsgs += o.peerMsgs
	s.noOpen += o.noOpen
	s.dropped += o.dropped
}
