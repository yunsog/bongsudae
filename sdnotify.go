// sdnotify.go : systemd 알림 (0.2.0). 라이브러리 없이 NOTIFY_SOCKET (unixgram 소켓) 에 "키=값" 한 줄을 씀
//
// watchdog : 단위 파일 WatchdogSec=180 + NotifyAccess=main 이면, 1 분 통계 (logStat : 모든 라우터 잠금을 차례로 잡음) 가
// 끝날 때마다 WATCHDOG=1 을 보냄. 잠금이 꼬여 (deadlock) 살아만 있고 일을 안 하면 신호가 멈춤 -> 180 초 뒤 systemd 가
// 죽이고 다시 띄움 (Restart=always). NOTIFY_SOCKET 이 없으면 (손으로 실행, 시험) 아무것도 안 함
package main

import (
	"net"
	"os"
)

// sdNotify : systemd 에 상태 한 줄 (예 "READY=1", "WATCHDOG=1", "STOPPING=1"). 실패해도 조용히 (systemd 밖일 수 있음)
func sdNotify(state string) {
	path := os.Getenv("NOTIFY_SOCKET")
	if path == "" {
		return
	}
	if path[0] == '@' { // 추상 (abstract) 소켓 : 이름 앞이 0 바이트
		path = "\x00" + path[1:]
	}
	conn, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		return
	}
	defer conn.Close()
	conn.Write([]byte(state))
}
