// TCP 수신 서버 : 라우터마다 goroutine 하나가 읽고 처리까지 함 (해석, 줄 만들기, 경로표, Kafka 로 넘기기).
// 라우터끼리는 동시에 (여러 코어), 한 라우터 안에서는 받은 순서대로. 잠금은 collector.go 머리글
//
// 보호 :
//   - 허용 대역 밖 주소는 바로 끊음
//   - 첫 메시지는 firstWait 안에 와야 하고 BMP v3 Initiation 이어야 함 (스캐너는 여기서 끊김)
//   - 머리의 길이가 bmpMaxLen 보다 크거나 버전이 3 이 아니면 그 연결만 끊음
//   - 읽기 쪽 panic 은 그 연결만, 처리 쪽 panic 은 그 메시지만 (safeProcess)
//   - Kafka 버퍼가 차면 그 라우터의 읽기가 멈춤 -> TCP 가 라우터를 늦춤 (버리지 않음)
//
// reset API (api.go) : connTable 의 연결을 닫음 -> 라우터가 다시 접속해 Peer Up 과 경로 전체를 다시 보냄
package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	firstWait = 30 * time.Second // 연결 뒤 첫 메시지를 기다리는 시간
	keepAlive = 60 * time.Second // TCP keepalive (BMP 는 조용할 수 있어 읽기 시간 제한은 두지 않음)
	maxConns  = 200              // 동시 연결 상한
	resetGap  = 10 * time.Minute // 같은 라우터를 다시 끊을 수 있는 간격 (전체 경로를 다시 받는 부담)
)

// ---- 지금 연결 (reset 용) ----

// routerConn : 라우터 연결 하나. why 는 reset 이 닫기 전에 넣음 (connTable.mu 로 보호)
type routerConn struct {
	conn net.Conn
	why  string
}

// connTable : 라우터 주소 -> 지금 연결, 마지막 reset 시각
type connTable struct {
	mu    sync.Mutex
	m     map[string]*routerConn
	reset map[string]time.Time
}

var (
	errNotConnected = errors.New("연결되어 있지 않음")
	errTooSoon      = errors.New("너무 자주 끊음")
)

func newConnTable() *connTable {
	return &connTable{m: map[string]*routerConn{}, reset: map[string]time.Time{}}
}

func (t *connTable) add(ip string, rc *routerConn) {
	t.mu.Lock()
	t.m[ip] = rc // 같은 주소의 새 연결이 이전 것을 대신함
	t.mu.Unlock()
}

// remove : 연결이 끝남. 돌려주는 것 = collector 가 끊은 이유 (없으면 빈칸)
func (t *connTable) remove(ip string, rc *routerConn) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.m[ip] == rc {
		delete(t.m, ip)
	}
	return rc.why
}

// closeRouter : reset API. 연결을 닫음 (읽기 goroutine 이 오류로 끝나며 closed + why 를 처리 쪽에 보냄)
// 같은 라우터는 resetGap 에 한 번 : 남은 시간을 돌려줌
func (t *connTable) closeRouter(ip, why string) (time.Duration, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	rc := t.m[ip]
	if rc == nil {
		return 0, errNotConnected
	}
	if last, ok := t.reset[ip]; ok && time.Since(last) < resetGap {
		return resetGap - time.Since(last), errTooSoon
	}
	t.reset[ip] = time.Now()
	rc.why = why
	rc.conn.Close()
	return 0, nil
}

// closeOutside : 허용 대역에서 빠진 라우터의 연결을 닫음 (설정 reload). 돌려주는 것 = 닫은 수
func (t *connTable) closeOutside(list []netip.Prefix) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for ip, rc := range t.m {
		if a, err := netip.ParseAddr(ip); err == nil && !allowed(list, a) {
			rc.why = "Closed by collector: not in bmp.allow (config reload)"
			rc.conn.Close()
			n++
		}
	}
	return n
}

// tcpAllow : 지금 허용 대역 (설정 reload 가 바꿈, accept 마다 읽음)
var tcpAllow atomic.Pointer[[]netip.Prefix]

// tcpConns : 지금 연결 표 (tcp 입력일 때만, 설정 reload 가 씀)
var tcpConns atomic.Pointer[connTable]

// parseAllow : "192.0.2.0/24, 10.0.0.1" -> 대역 목록. 주소 하나는 /32 (v6 /128) 로
func parseAllow(s string) ([]netip.Prefix, error) {
	var out []netip.Prefix
	for _, f := range strings.Split(s, ",") {
		f = strings.TrimSpace(f)
		if !strings.Contains(f, "/") {
			a, err := netip.ParseAddr(f)
			if err != nil {
				return nil, fmt.Errorf("허용 대역 %q: %w", f, err)
			}
			out = append(out, netip.PrefixFrom(a.Unmap(), a.Unmap().BitLen()))
			continue
		}
		p, err := netip.ParsePrefix(f)
		if err != nil {
			return nil, fmt.Errorf("허용 대역 %q: %w", f, err)
		}
		out = append(out, p.Masked())
	}
	return out, nil
}

func allowed(list []netip.Prefix, a netip.Addr) bool {
	a = a.Unmap() // ::ffff:192.0.2.1 -> 192.0.2.1
	for _, p := range list {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// runTCP : 수신 + 처리. 돌아오지 않음 (오류면 돌아옴)
func runTCP(listen string, allow []netip.Prefix, c *collector, out *producer) error {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	log.Printf("listen %s allow %v", listen, allow)
	tcpAllow.Store(&allow)
	ct := newConnTable()
	tcpConns.Store(ct)
	c.mu.Lock()
	c.closeRouter = ct.closeRouter // reset API 가 씀 (tcp 입력에서만)
	c.mu.Unlock()
	go func() { // 1 분 통계, Kafka 가 계속 안 되면 끝냄 (systemd 가 다시 띄움)
		for range time.Tick(time.Minute) {
			c.logStat(out)
			if out.dead() {
				log.Printf("kafka 보내기가 %s 넘게 안 됨 : 끝냄 (systemd 가 다시 띄움)", kafkaDeadAfter)
				os.Exit(1)
			}
		}
	}()

	sem := make(chan struct{}, maxConns) // 동시 연결 수 세마포어
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			time.Sleep(time.Second)
			continue
		}
		ap, _ := netip.ParseAddrPort(conn.RemoteAddr().String())
		if !allowed(*tcpAllow.Load(), ap.Addr()) {
			conn.Close()
			continue
		}
		select {
		case sem <- struct{}{}:
			go func() {
				defer func() { <-sem }()
				handleConn(conn, ap.Addr().Unmap().String(), c, out, ct)
			}()
		default:
			log.Printf("too many connections, drop %s", ap)
			conn.Close()
		}
	}
}

// handleConn : 라우터 연결 하나. BMP 메시지를 하나씩 잘라 처리하고 Kafka 로 (이 goroutine 에서)
func handleConn(conn net.Conn, ip string, c *collector, out *producer, ct *connTable) {
	defer conn.Close()
	rc := &routerConn{conn: conn}
	defer func() {
		if e := recover(); e != nil {
			log.Printf("conn %s panic: %v", ip, e)
		}
	}()
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(keepAlive)
	}
	conn.SetReadDeadline(time.Now().Add(firstWait))
	rd := bufio.NewReaderSize(conn, 1<<16) // 메시지마다 read 시스템 호출을 하지 않게
	rHash := md5hex(ip, c.cHash)           // bmp_raw 머리 (collector.routerFor 와 같은 값)
	n := 0
	var gen uint64 // 이 연결의 번호 (connStart)
	defer func() {
		if n > 0 { // Initiation 을 받은 연결만
			why := ct.remove(ip, rc)
			out.send(c.connClosed(ip, why, gen, time.Now()))
		}
	}()
	for {
		msg, err := readBMP(rd)
		if err != nil {
			if n > 0 || err != io.EOF {
				log.Printf("conn %s closed after %d msgs: %v", ip, n, err)
			}
			return
		}
		if n == 0 {
			if msg[5] != bmpInit {
				log.Printf("conn %s: first message type %d (not initiation), drop", ip, msg[5])
				return
			}
			conn.SetReadDeadline(time.Time{}) // 이제부터 시간 제한 없음
			log.Printf("conn %s: initiation", ip)
			ct.add(ip, rc)
			gen = c.connStart(ip, time.Now()) // 새 연결 : 이전 peer 상태는 무효
		}
		n++
		out.sendRaw(c.cHash, rHash, ip, msg) // 받은 그대로 먼저 (해석에 실패해도)
		out.send(c.handle(ip, "", msg, time.Now()))
	}
}

// readBMP : 공통 머리 6 바이트를 읽고 검사한 뒤 나머지를 읽음
func readBMP(r io.Reader) ([]byte, error) {
	var hdr [bmpHdrLen]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n, _, err := bmpHeader(hdr[:])
	if err != nil {
		return nil, err
	}
	msg := make([]byte, n)
	copy(msg, hdr[:])
	if _, err := io.ReadFull(r, msg[bmpHdrLen:]); err != nil {
		return nil, err
	}
	return msg, nil
}
