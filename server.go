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
//
// 끝내기 (0.2.0, SIGTERM) : listener 닫기 -> 모든 연결 닫기 (라우터마다 router term "Closed by collector: shutdown")
// -> 라우터 goroutine 이 끝나기를 shutdownWait 까지 기다림 -> 돌아감 (main 이 Kafka Flush 뒤 끝냄).
// 전에는 바로 Flush 해서 부하 때 (RIB 다시 받기) 라우터 goroutine 이 계속 넣는 바람에 Flush 가 끝나지 못하고 버퍼를 버렸음
//
// 무중단 판 올리기 (0.2.0, handover.go) : 연결마다 읽기 시간 제한을 "지금" 으로 -> 다음 읽기에서 멈춤 (메시지 경계,
// 읽다 만 바이트는 msgReader 에 남음) -> upg.park 에서 기다림. 실패하면 이어 읽고, 성공하면 exec 로 새 판이 이어 받음
//
// 같은 라우터의 새 연결 (0.2.0) : 새 연결이 Initiation 을 보내면 옛 연결을 닫음 (기록 한 줄). 옛 연결이 이미 읽어 둔
// 메시지는 연결 번호 (gen) 가 달라 버림 -> 옛 연결이 새 세션의 경로표 / 출력에 섞이지 않음
package main

import (
	"context"
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

	shutdownWait = 10 * time.Second // 끝낼 때 라우터 goroutine 을 기다리는 시간 (+ Kafka Flush 30 초 < systemd 60 초)
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
	errNotConnected = errors.New("not connected")
	errTooSoon      = errors.New("reset too often")
)

func newConnTable() *connTable {
	return &connTable{m: map[string]*routerConn{}, reset: map[string]time.Time{}}
}

// add : 새 연결 (Initiation 받음). 같은 주소의 옛 연결이 있으면 닫음 (옛 goroutine 은 읽기 오류로 끝남, term 은 안 냄 :
// 이미 새 연결 번호라 connClosed 가 무시). 라우터가 같은 수집기로 BMP 세션을 둘 맺으면 서로 끊으므로 기록을 남김
func (t *connTable) add(ip string, rc *routerConn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if old := t.m[ip]; old != nil && old != rc {
		log.Printf("conn %s: new connection replaces the old one (closed)", ip)
		old.why = "Closed by collector: replaced by new connection"
		old.conn.Close()
	}
	t.m[ip] = rc
}

// closeAll : 끝낼 때 모든 연결을 닫음. 돌려주는 것 = 닫은 수
func (t *connTable) closeAll(why string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, rc := range t.m {
		rc.why = why
		rc.conn.Close()
	}
	return len(t.m)
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
				return nil, fmt.Errorf("allow list %q: %w", f, err)
			}
			out = append(out, netip.PrefixFrom(a.Unmap(), a.Unmap().BitLen()))
			continue
		}
		p, err := netip.ParsePrefix(f)
		if err != nil {
			return nil, fmt.Errorf("allow list %q: %w", f, err)
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

// runTCP : 수신 + 처리. ctx 가 끝나면 (SIGTERM) 연결을 모두 정리하고 nil 로 돌아옴 (위 "끝내기"). 오류면 오류로 돌아옴
// hs 가 있으면 (판 올리기로 시작) listener 와 연결을 옛 판에게서 이어 받음
func runTCP(ctx context.Context, listen string, allow []netip.Prefix, c *collector, out *producer, hs *handoverState) error {
	var ln net.Listener
	var err error
	if hs != nil {
		ln, err = hs.listener()
	} else {
		ln, err = net.Listen("tcp", listen)
	}
	if err != nil {
		return err
	}
	go func() { // 신호 -> listener 를 닫아 아래 Accept 를 깨움
		<-ctx.Done()
		log.Printf("signal : stop accepting, close router connections")
		ln.Close()
	}()
	log.Printf("listen %s allow %v", listen, allow)
	sdNotify("READY=1")
	tcpAllow.Store(&allow)
	ct := newConnTable()
	tcpConns.Store(ct)
	c.mu.Lock()
	c.closeRouter = ct.closeRouter // reset API 가 씀 (tcp 입력에서만)
	c.mu.Unlock()
	upg.setup(ln, ct, c, out)
	go func() { // 1 분 통계, Kafka 가 계속 안 되면 끝냄 (systemd 가 다시 띄움)
		for range time.Tick(time.Minute) {
			c.logStat(out)
			sdNotify("WATCHDOG=1") // 통계가 모든 라우터 잠금을 잡고 끝남 = 멈추지 않음 (sdnotify.go)
			if out.dead() {
				log.Printf("kafka produce failing for more than %s : exiting (systemd restarts)", kafkaDeadAfter)
				os.Exit(1)
			}
		}
	}()

	sem := make(chan struct{}, maxConns) // 동시 연결 수 세마포어
	var wg sync.WaitGroup                // 라우터 goroutine (끝낼 때 기다림)
	start := func(cr *connRun) {         // 연결 goroutine 시작. upg 에 먼저 올림 (판 올리기가 빠뜨리지 않게, accept 와 같은 goroutine 에서)
		upg.enter(cr)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			handleConn(cr, c, out, ct)
		}()
	}
	if hs != nil { // 이어 받은 연결들
		for _, cr := range hs.conns(ct) {
			sem <- struct{}{}
			start(cr)
		}
		hs.resumed()
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) { // 끝내기 (위 goroutine 이 닫음)
				return shutdownConns(ct, &wg)
			}
			if upg.parkAccept(ln, err) { // 판 올리기 중 : 멈췄다가 (실패하면) 이어 받음
				continue
			}
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
			start(newConnRun(conn, ap.Addr().Unmap().String(), nil))
		default:
			log.Printf("too many connections, drop %s", ap)
			conn.Close()
		}
	}
}

// shutdownConns : 모든 연결을 닫고 라우터 goroutine 을 shutdownWait 까지 기다림
// (닫힌 연결마다 goroutine 이 router term 을 Kafka 버퍼에 넣고 끝남)
func shutdownConns(ct *connTable, wg *sync.WaitGroup) error {
	sdNotify("STOPPING=1")
	n := ct.closeAll("Closed by collector: shutdown")
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		log.Printf("shutdown : %d router connections closed", n)
	case <-time.After(shutdownWait):
		log.Printf("shutdown : %d router connections closed, some goroutines still busy after %s", n, shutdownWait)
	}
	return nil
}

// connRun : 라우터 연결 하나의 읽기 상태. 판 올리기가 멈춘 연결을 이것째로 새 판에 넘김 (handover.go)
type connRun struct {
	conn  net.Conn
	ip    string
	rc    *routerConn // connTable 의 것
	rd    *msgReader
	n     int       // 받은 메시지 수 (0 = Initiation 전)
	gen   uint64    // 이 연결의 번호 (connStart, 이어 받은 연결은 옛 판의 번호 그대로)
	until time.Time // Initiation 전 : 첫 메시지를 기다리는 끝 시각
}

func newConnRun(conn net.Conn, ip string, pending []byte) *connRun {
	return &connRun{conn: conn, ip: ip, rc: &routerConn{conn: conn}, rd: newMsgReader(conn, pending)}
}

// handleConn : 라우터 연결 하나. BMP 메시지를 하나씩 잘라 처리하고 Kafka 로 (이 goroutine 에서)
func handleConn(cr *connRun, c *collector, out *producer, ct *connTable) {
	conn, ip := cr.conn, cr.ip
	defer upg.leave(cr) // defer 는 거꾸로 : 끊김 처리 (term) 를 보낸 뒤 판 올리기 목록에서 뺌
	defer conn.Close()
	defer func() {
		if e := recover(); e != nil {
			log.Printf("conn %s panic: %v", ip, e)
		}
	}()
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(keepAlive)
	}
	if cr.n == 0 {
		if cr.until.IsZero() { // 이어 받은 연결은 옛 판의 시각 그대로
			cr.until = time.Now().Add(firstWait)
		}
		conn.SetReadDeadline(cr.until)
	}
	rHash := md5hex(ip, c.cHash) // bmp_raw 머리 (collector.routerFor 와 같은 값)
	raw := newRawHead(c.cHash, rHash, ip)
	cr.rd.pad = raw.room // 메시지 앞에 bmp_raw 머리 자리 (메시지마다 할당 한 번 : 해석과 bmp_raw 가 같은 바이트를 씀)
	defer func() {
		if cr.n > 0 { // Initiation 을 받은 연결만
			why := ct.remove(ip, cr.rc)
			out.send(c.connClosed(ip, why, cr.gen, time.Now()))
		}
	}()
	for {
		msg, err := cr.rd.next()
		if err != nil {
			if upg.park(cr, err) { // 판 올리기로 멈췄다가 판 올리기가 실패함 : 이어 읽음
				continue
			}
			if cr.n > 0 || err != io.EOF {
				log.Printf("conn %s closed after %d msgs: %v", ip, cr.n, err)
			}
			return
		}
		if cr.n == 0 {
			if msg[5] != bmpInit {
				log.Printf("conn %s: first message type %d (not initiation), drop", ip, msg[5])
				return
			}
			conn.SetReadDeadline(time.Time{}) // 이제부터 시간 제한 없음
			log.Printf("conn %s: initiation", ip)
			// 순서 : 연결 번호를 먼저 바꾸고 옛 연결을 닫음 (옛 goroutine 의 connClosed 가 먼저 와도 번호가 달라 term 을 안 냄)
			cr.gen = c.connStart(ip, time.Now()) // 새 연결 : 이전 peer 상태는 무효
			ct.add(ip, cr.rc)
		}
		cr.n++
		msgs, ok := c.handleConn(ip, cr.gen, msg, time.Now())
		if !ok { // 새 연결이 이 라우터를 이어 받음 : 이 연결이 읽어 둔 것은 버림
			log.Printf("conn %s: old connection, %d msgs, stop", ip, cr.n)
			return
		}
		out.sendRaw(raw, cr.rd.full) // 받은 그대로 (해석에 실패해도)
		out.send(msgs)
	}
}

// msgReader : 연결에서 BMP 메시지를 하나씩 잘라 줌 (0.2.0 : bufio 대신).
// 읽기가 시간 제한 (판 올리기의 멈춤) 으로 끝나도 읽다 만 바이트는 buf 에 남음 -> pending() 으로 새 판에 넘김.
// 메시지마다 read 시스템 호출을 하지 않게 한 번에 64 KB 씩 (큰 메시지면 그 크기까지 늘림)
type msgReader struct {
	r          io.Reader
	buf        []byte
	head, tail int    // buf[head:tail] = 아직 내주지 않은 바이트
	pad        int    // 메시지 앞에 비워 둘 바이트 (bmp_raw 머리 자리, 0.2.0)
	full       []byte // 마지막 next 의 할당 전체 = room + 메시지
}

const readBuf = 1 << 16

func newMsgReader(r io.Reader, pending []byte) *msgReader {
	m := &msgReader{r: r, buf: make([]byte, max(readBuf, len(pending)))}
	m.tail = copy(m.buf, pending)
	return m
}

// next : 메시지 하나 (새 slice : 부른 쪽이 가짐, 앞에 pad 바이트가 비어 있음 = m.full). 머리가 틀리면 (길이, 버전) 오류
func (m *msgReader) next() ([]byte, error) {
	for {
		need := bmpHdrLen
		if m.tail-m.head >= bmpHdrLen {
			n, _, err := bmpHeader(m.buf[m.head:m.tail])
			if err != nil {
				return nil, err
			}
			if m.tail-m.head >= n {
				m.full = make([]byte, m.pad+n)
				msg := m.full[m.pad:]
				copy(msg, m.buf[m.head:])
				m.head += n
				return msg, nil
			}
			need = n
		}
		m.room(need)
		k, err := m.r.Read(m.buf[m.tail:])
		m.tail += k
		if err != nil && k == 0 {
			return nil, err
		}
	}
}

// room : 내주지 않은 바이트를 앞으로 당기고, 메시지 하나 (need 바이트) 가 들어가게 늘림
func (m *msgReader) room(need int) {
	if m.head > 0 {
		m.tail = copy(m.buf, m.buf[m.head:m.tail])
		m.head = 0
	}
	if len(m.buf) < need {
		b := make([]byte, need)
		copy(b, m.buf[:m.tail])
		m.buf = b
	}
}

// pending : 읽었지만 아직 메시지로 내주지 않은 바이트 (복사)
func (m *msgReader) pending() []byte {
	return append([]byte(nil), m.buf[m.head:m.tail]...)
}
