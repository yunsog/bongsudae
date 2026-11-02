// 무중단 판 올리기 (0.2.0) : 라우터 연결과 상태를 그대로 새 실행 파일에 넘김 -> 라우터가 다시 붙지 않음
// (전에는 판을 올릴 때마다 라우터가 전체 경로를 다시 보냄 = 약 18 분 + 소비자 지연). 설계 docs/design_handover.md
//
// 명령 : bongsudae -upgrade (= kill -USR2 <pid>, 결과를 /api/stats 로 확인) . 같은 pid 그대로 = systemd 는 모름
//
//	옛 판 (SIGUSR2)                                        새 판 (같은 pid, -resume <상태 파일>)
//	 1. 새 실행 파일 확인 : -handover-check (상태 형식 판), -t (설정)
//	 2. 멈춤 : accept 와 모든 연결의 읽기 시간 제한 = 지금
//	    -> 연결 goroutine 은 메시지 경계에서 park (읽다 만 바이트는 msgReader 에 남음)
//	 3. Kafka 에 남은 것을 다 보냄 (Flush)
//	 4. fd 복사 (dup = CLOEXEC 없음 : exec 뒤에도 열림) : listener, 연결, API listener, 잠금 파일
//	 5. 상태 파일 : 머리 + gob (라우터 / peer / seq / 연결) + 경로표 (state_rib.go)
//	 6. syscall.Exec  ------------------------------------>  7. gob 을 읽어 collector 를 다시 만듦, fd 로 연결을 다시 잡음
//	                                                          8. 연결마다 남은 바이트부터 이어 읽음 (= 라우터 멈춤 끝)
//	                                                          9. 경로표는 뒤에서 읽음 (그동안 온 UPDATE 는 peer 마다 모았다가 적용)
//
// 실패하면 : 1 ~ 6 (exec 실패 포함) 은 멈춘 연결을 그대로 이어 읽음 (옛 판이 계속, 기록 + /api/stats 의 last_error).
// exec 뒤 새 판이 상태를 못 읽으면 끝남 -> 연결이 끊겨 라우터가 다시 붙음 = 지금까지의 재시작과 같음 (더 나빠지지 않음).
// Kafka 출력은 경로표와 상관없으므로 9 를 기다리지 않음 (경로 수 / 찾기 API 만 그동안 덜 나옴 : rib_loading).
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	stateMagic   = "bongsudae-handover\n" // 상태 파일 첫 줄
	stateVersion = 1                      // 상태 파일 형식 판 : 칸을 바꾸면 올림 (옛 판과 새 판이 다르면 판 올리기를 하지 않음)
	pauseWait    = 10 * time.Second       // 연결이 모두 멈추기를 기다리는 시간 (넘으면 그만두고 이어 읽음)
	flushWait    = 20 * time.Second       // Kafka 에 남은 것을 보내는 시간
	checkWait    = 30 * time.Second       // 새 실행 파일 확인 (-handover-check, -t) 시간
)

// exePath : 시작 때의 실행 파일 경로 (판을 올릴 때는 같은 경로의 새 파일을 exec)
var exePath string

// ribLoading : 판 올리기 뒤 경로표를 뒤에서 읽는 중 (/api/stats 에 보임)
var ribLoading atomic.Bool

// ---- 옛 판 : 멈추기 ----

// upgrader : 판 올리기 하나 (전역 upg). 연결 goroutine 목록, 멈춤 상태, 기록
type upgrader struct {
	mu      sync.Mutex
	ln      net.Listener // BMP listener (runTCP 가 넣음)
	httpLn  net.Listener // API listener (startAPI 가 넣음, 없으면 nil)
	ct      *connTable
	c       *collector
	out     *producer
	conns   map[*connRun]bool // 연결 goroutine -> 멈춤 (park) 했는지
	pausing bool
	lnPark  bool          // accept goroutine 이 멈춤
	resume  chan struct{} // 판 올리기를 그만두면 닫음 (멈춘 goroutine 이 깨어남)
	changed chan struct{} // park / leave 때 알림 (waitParked 가 기다림)
	busy    atomic.Bool   // 판 올리기 하는 중 (한 번에 하나)

	info handoverJSON // /api/stats 의 handover (mu 로 보호)
}

var upg = &upgrader{conns: map[*connRun]bool{}, changed: make(chan struct{}, 1)}

// setup : tcp 입력 시작 때 (runTCP)
func (u *upgrader) setup(ln net.Listener, ct *connTable, c *collector, out *producer) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.ln, u.ct, u.c, u.out = ln, ct, c, out
	u.info.Supported = true
}

func (u *upgrader) notify() {
	select {
	case u.changed <- struct{}{}:
	default:
	}
}

// enter : 연결 goroutine 시작 (accept goroutine 에서 부름 : 판 올리기가 막 받은 연결을 빠뜨리지 않게)
func (u *upgrader) enter(cr *connRun) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.conns[cr] = false
	if u.pausing {
		cr.conn.SetReadDeadline(time.Now())
	}
}

// leave : 연결 goroutine 끝 (끊김 처리 뒤)
func (u *upgrader) leave(cr *connRun) {
	u.mu.Lock()
	delete(u.conns, cr)
	u.mu.Unlock()
	u.notify()
}

// park : 연결 goroutine 의 읽기 오류. 판 올리기의 멈춤 (시간 제한) 이면 멈춰 기다렸다가 판 올리기가 실패하면 true (이어 읽음).
// 성공하면 exec 로 프로세스가 바뀌어 돌아오지 않음. 그 밖의 오류는 false (연결 끝)
func (u *upgrader) park(cr *connRun, err error) bool {
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		return false
	}
	u.mu.Lock()
	if !u.pausing {
		u.mu.Unlock()
		// 판 올리기가 막 끝났는데 늦게 온 시간 제한 : Initiation 뒤 연결은 시간 제한이 없으므로 판 올리기의 것,
		// 그 전 연결은 원래 시간 (firstWait) 이 아직 안 지났으면 판 올리기의 것
		if cr.n > 0 || time.Now().Before(cr.until) {
			cr.restoreDeadline()
			return true
		}
		return false
	}
	u.conns[cr] = true
	ch := u.resume
	u.mu.Unlock()
	u.notify()
	<-ch // 판 올리기를 그만둠
	cr.restoreDeadline()
	return true
}

// restoreDeadline : 멈춤 전의 읽기 시간 제한으로 (Initiation 전 = firstWait 시각, 뒤 = 없음)
func (cr *connRun) restoreDeadline() {
	if cr.n == 0 {
		cr.conn.SetReadDeadline(cr.until)
	} else {
		cr.conn.SetReadDeadline(time.Time{})
	}
}

// parkAccept : accept 오류가 판 올리기의 멈춤이면 기다렸다가 true
func (u *upgrader) parkAccept(ln net.Listener, err error) bool {
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		return false
	}
	u.mu.Lock()
	if u.pausing {
		u.lnPark = true
		ch := u.resume
		u.mu.Unlock()
		u.notify()
		<-ch
	} else {
		u.mu.Unlock()
	}
	setDeadline(ln, time.Time{})
	return true
}

func setDeadline(ln net.Listener, t time.Time) {
	if d, ok := ln.(interface{ SetDeadline(time.Time) error }); ok {
		d.SetDeadline(t)
	}
}

// pause : accept 와 모든 연결의 읽기를 멈추게 함 (시간 제한 = 지금)
func (u *upgrader) pause() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.pausing, u.lnPark = true, false
	u.resume = make(chan struct{})
	now := time.Now()
	setDeadline(u.ln, now)
	for cr := range u.conns {
		cr.conn.SetReadDeadline(now)
	}
}

// waitParked : accept 와 모든 연결이 멈추기를 기다림 (끊겨 끝난 연결은 목록에서 빠짐)
func (u *upgrader) waitParked(limit time.Duration) error {
	deadline := time.After(limit)
	for {
		u.mu.Lock()
		busy := 0
		for _, parked := range u.conns {
			if !parked {
				busy++
			}
		}
		done := u.lnPark && busy == 0
		u.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-u.changed:
		case <-deadline:
			return fmt.Errorf("%d connections did not pause within %s", busy, limit)
		}
	}
}

// resumeAll : 판 올리기를 그만두고 이어 읽음
func (u *upgrader) resumeAll() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.pausing = false
	for cr := range u.conns {
		u.conns[cr] = false
	}
	close(u.resume)
}

// parked : 멈춘 연결 목록 (멈춘 동안에만 부름 : 그 goroutine 들은 park 에서 기다리는 중이라 cr 을 건드리지 않음)
func (u *upgrader) parked() []*connRun {
	u.mu.Lock()
	defer u.mu.Unlock()
	list := make([]*connRun, 0, len(u.conns))
	for cr := range u.conns {
		list = append(list, cr)
	}
	return list
}

// ---- 옛 판 : 판 올리기 ----

// upgrade : SIGUSR2. 성공하면 돌아오지 않음 (exec), 실패하면 기록하고 돌아옴 (옛 판이 계속)
func (u *upgrader) upgrade() {
	if !u.busy.CompareAndSwap(false, true) {
		log.Printf("handover: already in progress, ignored")
		return
	}
	defer u.busy.Store(false)
	u.mu.Lock()
	ready := u.ct != nil
	u.mu.Unlock()
	if !ready {
		u.fail(errors.New("not ready (tcp input not started)"))
		return
	}
	if ribLoading.Load() { // 지난 판 올리기의 경로표를 아직 읽는 중 : 지금 넘기면 덜 읽은 경로표를 넘김
		u.fail(errors.New("route tables of the last handover are still loading, try again in a minute"))
		return
	}
	if *optLock == "" {
		u.fail(errors.New("needs a lock file (paths.lock) : the state file goes next to it"))
		return
	}
	log.Printf("handover: start (SIGUSR2), checking %s", exePath)
	if err := checkNewBinary(); err != nil {
		u.fail(err)
		return
	}
	u.fail(u.handover())
}

// fail : 판 올리기를 못 함 (기록 + /api/stats)
func (u *upgrader) fail(err error) {
	log.Printf("handover: aborted, old process keeps running : %v", err)
	u.mu.Lock()
	u.info.LastError, u.info.LastErrorAt = err.Error(), tstr(time.Now())
	u.mu.Unlock()
}

// checkNewBinary : 새 실행 파일이 같은 상태 형식을 알고, 지금 설정을 받아들이는지
func checkNewBinary() error {
	ctx, cancel := context.WithTimeout(context.Background(), checkWait)
	defer cancel()
	out, err := exec.CommandContext(ctx, exePath, "-handover-check").CombinedOutput()
	want := fmt.Sprintf("handover state %d", stateVersion)
	if err != nil || strings.TrimSpace(string(out)) != want {
		return fmt.Errorf("%s -handover-check : %q %v (want %q : the new binary must support the same state format)",
			exePath, strings.TrimSpace(string(out)), err, want)
	}
	out, err = exec.CommandContext(ctx, exePath, append(baseArgs(), "-t")...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s -t : %v : %s", exePath, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// baseArgs : 이 프로세스의 명령줄 옵션에서 -resume 을 뺀 것 (판 올리기를 거듭해도 쌓이지 않게)
func baseArgs() []string {
	var out []string
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "-resume" || a == "--resume" {
			i++ // 값도 뺌
			continue
		}
		if strings.HasPrefix(a, "-resume=") || strings.HasPrefix(a, "--resume=") {
			continue
		}
		out = append(out, a)
	}
	return out
}

// statePath : 상태 파일 = 잠금 파일 옆 (run/bongsudae.lock -> run/bongsudae.state)
func statePath() string {
	return strings.TrimSuffix(*optLock, ".lock") + ".state"
}

// handover : 멈춤 -> Flush -> fd -> 상태 파일 -> exec. 돌아오면 실패 (멈춘 연결은 이어 읽게 해 둠)
func (u *upgrader) handover() (err error) {
	t0 := time.Now()
	u.pause()
	var fds []int
	path := statePath()
	defer func() { // 여기까지 왔으면 실패 : 되돌림
		for _, fd := range fds {
			syscall.Close(fd)
		}
		os.Remove(path)
		u.resumeAll()
	}()
	if err := u.waitParked(pauseWait); err != nil {
		return err
	}
	tPause := time.Since(t0)
	ctx, cancel := context.WithTimeout(context.Background(), flushWait)
	defer cancel()
	if err := u.out.cl.Flush(ctx); err != nil {
		return fmt.Errorf("kafka flush : %w", err)
	}
	tFlush := time.Since(t0)
	u.c.logStat(u.out) // 이 1 분의 통계 (새 판은 0 부터 셈)

	hs := &handoverState{Version: stateVersion, Release: release, Rev: version(), PausedAt: t0, LockFd: -1, ListenFd: -1, HTTPFd: -1,
		Listen: *optListen, HTTP: *optHTTP}
	dup := func(sc syscall.Conn) (int, error) {
		fd, err := dupFd(sc)
		if err == nil {
			fds = append(fds, fd)
		}
		return fd, err
	}
	if hs.LockFd, err = dup(lockFile); err != nil {
		return fmt.Errorf("lock file fd : %w", err)
	}
	if hs.ListenFd, err = dup(u.ln.(syscall.Conn)); err != nil {
		return fmt.Errorf("listener fd : %w", err)
	}
	if u.httpLn != nil {
		if hs.HTTPFd, err = dup(u.httpLn.(syscall.Conn)); err != nil {
			return fmt.Errorf("http listener fd : %w", err)
		}
	}
	for _, cr := range u.parked() {
		sc, ok := cr.conn.(syscall.Conn)
		fd := -1
		if ok {
			fd, err = dup(sc)
		}
		if !ok || err != nil { // 그 사이 닫힌 연결 (reset API 등) : 넘기지 않음 = 라우터가 다시 붙음
			log.Printf("handover: conn %s not passed (%v)", cr.ip, err)
			continue
		}
		hs.Conns = append(hs.Conns, connState{IP: cr.ip, Fd: fd, Gen: cr.gen, N: cr.n, Until: cr.until, Pending: cr.rd.pending()})
	}
	u.ct.mu.Lock()
	hs.Resets = map[string]time.Time{}
	for ip, t := range u.ct.reset {
		hs.Resets[ip] = t
	}
	u.ct.mu.Unlock()
	hist.mu.Lock()
	hs.Started, hs.Minutes = hist.started, append([]minuteStat(nil), hist.list...)
	hist.mu.Unlock()
	u.mu.Lock()
	hs.Count = u.info.Count + 1
	u.mu.Unlock()

	size, nRoutes, err := writeHandover(path, hs, u.c)
	if err != nil {
		return fmt.Errorf("state file %s : %w", path, err)
	}
	for _, fd := range fds { // 넘길 fd 만 exec 뒤에도 열림 (dup 은 CLOEXEC 없음, 나머지 fd 는 Go 가 CLOEXEC 로 엶)
		if err := clearCloexec(fd); err != nil {
			return fmt.Errorf("fd %d : %w", fd, err)
		}
	}
	log.Printf("handover: paused %d connections in %s, kafka flushed in %s, state %s (%d MB, %d routes) written in %s : exec %s",
		len(hs.Conns), tPause.Round(time.Millisecond), (tFlush - tPause).Round(time.Millisecond), path, size>>20, nRoutes,
		(time.Since(t0) - tFlush).Round(time.Millisecond), exePath)
	args := append([]string{os.Args[0]}, baseArgs()...)
	args = append(args, "-resume", path)
	err = syscall.Exec(exePath, args, os.Environ())
	return fmt.Errorf("exec %s : %w", exePath, err) // 여기 오면 exec 실패
}

// dupFd : 소켓 / 파일의 fd 복사 (dup 은 CLOEXEC 를 붙이지 않음). 원래 fd 는 Go 가 그대로 씀
func dupFd(sc syscall.Conn) (int, error) {
	rc, err := sc.SyscallConn()
	if err != nil {
		return -1, err
	}
	nfd, derr := -1, error(nil)
	if err := rc.Control(func(fd uintptr) { nfd, derr = syscall.Dup(int(fd)) }); err != nil {
		return -1, err
	}
	return nfd, derr
}

// clearCloexec : exec 뒤에도 열려 있게 (dup 은 이미 없지만 확실히)
func clearCloexec(fd int) error {
	_, _, e := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_SETFD, 0)
	if e != 0 {
		return e
	}
	return nil
}

// ---- 상태 파일 ----

// handoverState : 상태 파일의 gob 부분 (gob 은 대문자 칸만 씀). 경로표는 뒤에 따로 (state_rib.go, Routers / Peers 순서)
type handoverState struct {
	Version      int
	Release, Rev string    // 옛 판
	PausedAt     time.Time // 멈춘 시각 (새 판이 멈춘 시간을 잼)
	Count        int       // 판 올리기 몇 번째 (재시작하면 0)
	LockFd       int
	ListenFd     int
	HTTPFd       int    // -1 = API 없음
	Listen, HTTP string // 옛 판의 주소 (새 판은 이 소켓을 그대로 씀 : 설정이 바뀌었으면 기록만)
	Seq          uint64 // 줄 번호
	Conns        []connState
	Resets       map[string]time.Time // reset API 마지막 시각 (10 분 제한)
	Started      time.Time            // 처음 시작 시각 (/api/stats)
	Minutes      []minuteStat         // 1 분 통계 (/api/stats)
	Routers      []routerState

	// 새 판에서만 (gob 에 안 들어감)
	f     *os.File
	rd    *bufio.Reader
	peers []*peer   // 경로표 덩어리 순서
	owner []*router // peers[i] 의 라우터
}

// connState : 라우터 연결 하나
type connState struct {
	IP      string
	Fd      int
	Gen     uint64
	N       int       // 받은 메시지 수 (0 = Initiation 전)
	Until   time.Time // Initiation 전이면 첫 메시지를 기다리는 끝 시각
	Pending []byte    // 읽었지만 처리 안 한 바이트 (메시지 조각)
}

// routerState : router 의 칸 (stat 은 판 올리기 직전에 기록하고 0, byID 는 찾을 때 다시 채움)
type routerState struct {
	IP, Hash           string
	RouterSeq, PeerSeq uint32
	Terminated         bool
	ConnGen            uint64
	SysName, SysDescr  string
	Up                 bool
	Since, LastMsg     time.Time
	Connects           int
	TypeCount          []uint64
	Errors, NoOpen     uint64
	LastTerm           string
	LastReset          time.Time
	LastResetWhy       string
	Peers              []peerState
}

// peerState : peer 의 칸 (경로표 rib4 / rib6 은 state_rib.go)
type peerState struct {
	Key                  string
	IP                   netip.Addr
	IPStr, RD            string
	Typ, Flags           uint8
	AS                   uint32
	BgpID                string
	BgpIDRaw             [4]byte
	BmpStats             map[string]uint64
	BmpStatsAt           time.Time
	Up, GotUp, NoOpen    bool
	Since                time.Time
	LocalIP              string
	LocalPort, RmtPort   uint16
	HoldSent, HoldRecv   uint16
	SentCaps, RecvCaps   string
	AddPathV4, AddPathV6 bool
	FamV4, FamV6         bool
	EorV4, EorV6         time.Time
	LastUpdate           time.Time
	Adds, Dels           uint64
	DownReason           string
	Hist                 []eventState
}

type eventState struct {
	T    time.Time
	Text string
}

func saveRouter(r *router) routerState {
	return routerState{IP: r.ip, Hash: r.hash, RouterSeq: r.routerSeq, PeerSeq: r.peerSeq, Terminated: r.terminated, ConnGen: r.connGen,
		SysName: r.sysName, SysDescr: r.sysDescr, Up: r.up, Since: r.since, LastMsg: r.lastMsg, Connects: r.connects,
		TypeCount: append([]uint64(nil), r.typeCount[:]...), Errors: r.errors, NoOpen: r.noOpen, LastTerm: r.lastTerm,
		LastReset: r.lastReset, LastResetWhy: r.lastResetWhy}
}

func (s *routerState) load() *router {
	r := &router{ip: s.IP, hash: s.Hash, peers: map[string]*peer{}, byID: map[peerID]*peer{}, routerSeq: s.RouterSeq, peerSeq: s.PeerSeq,
		terminated: s.Terminated, connGen: s.ConnGen, sysName: s.SysName, sysDescr: s.SysDescr, up: s.Up, since: s.Since,
		lastMsg: s.LastMsg, connects: s.Connects, errors: s.Errors, noOpen: s.NoOpen, lastTerm: s.LastTerm,
		lastReset: s.LastReset, lastResetWhy: s.LastResetWhy}
	copy(r.typeCount[:], s.TypeCount)
	return r
}

func savePeer(p *peer) peerState {
	s := peerState{Key: p.key, IP: p.ip, IPStr: p.ipStr, RD: p.rd, Typ: p.typ, Flags: p.flags, AS: p.as, BgpID: p.bgpID, BgpIDRaw: p.bgpIDRaw,
		BmpStats: p.bmpStats, BmpStatsAt: p.bmpStatsAt, Up: p.up, GotUp: p.gotUp, NoOpen: p.noOpen, Since: p.since,
		LocalIP: p.localIP, LocalPort: p.localPort, RmtPort: p.rmtPort, HoldSent: p.holdSent, HoldRecv: p.holdRecv,
		SentCaps: p.sentCaps, RecvCaps: p.recvCaps, AddPathV4: p.addPathV4, AddPathV6: p.addPathV6, FamV4: p.famV4, FamV6: p.famV6,
		EorV4: p.eorV4, EorV6: p.eorV6, LastUpdate: p.lastUpdate, Adds: p.adds, Dels: p.dels, DownReason: p.downReason}
	for _, e := range p.hist {
		s.Hist = append(s.Hist, eventState{e.t, e.text})
	}
	return s
}

func (s *peerState) load() *peer {
	p := &peer{key: s.Key, ip: s.IP, ipStr: s.IPStr, rd: s.RD, typ: s.Typ, flags: s.Flags, as: s.AS, bgpID: s.BgpID, bgpIDRaw: s.BgpIDRaw,
		bmpStats: s.BmpStats, bmpStatsAt: s.BmpStatsAt, up: s.Up, gotUp: s.GotUp, noOpen: s.NoOpen, since: s.Since,
		localIP: s.LocalIP, localPort: s.LocalPort, rmtPort: s.RmtPort, holdSent: s.HoldSent, holdRecv: s.HoldRecv,
		sentCaps: s.SentCaps, recvCaps: s.RecvCaps, addPathV4: s.AddPathV4, addPathV6: s.AddPathV6, famV4: s.FamV4, famV6: s.FamV6,
		eorV4: s.EorV4, eorV6: s.EorV6, lastUpdate: s.LastUpdate, adds: s.Adds, dels: s.Dels, downReason: s.DownReason}
	for _, e := range s.Hist {
		p.hist = append(p.hist, event{e.T, e.Text})
	}
	return p
}

// writeHandover : 상태 파일 = 머리 줄 + gob 길이 (8 바이트) + gob + 경로표 덩어리들.
// 라우터 잠금 (읽기) 을 잡고 씀 : 연결 goroutine 은 멈춰 있고 API 는 읽기만 하므로 그 사이 바뀌지 않음
func writeHandover(path string, hs *handoverState, c *collector) (size int64, routes int, err error) {
	rs := c.sortedRouters()
	for _, r := range rs {
		r.mu.RLock()
		defer r.mu.RUnlock()
	}
	var ps []*peer
	for _, r := range rs {
		s := saveRouter(r)
		for _, p := range sortedPeers(r) {
			s.Peers = append(s.Peers, savePeer(p))
			ps = append(ps, p)
			routes += len(p.rib4) + len(p.rib6)
		}
		hs.Routers = append(hs.Routers, s)
	}
	hs.Seq = c.seq.Load()
	var g bytes.Buffer
	if err := gob.NewEncoder(&g).Encode(hs); err != nil {
		return 0, 0, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	w.WriteString(stateMagic)
	var n [8]byte
	binary.LittleEndian.PutUint64(n[:], uint64(g.Len()))
	w.Write(n[:])
	w.Write(g.Bytes())
	ribSize, err := writeRIBs(w, ps)
	if err == nil {
		err = w.Flush()
	}
	if err == nil {
		err = f.Close()
	}
	return int64(len(stateMagic)+8+g.Len()) + ribSize, routes, err
}

// ---- 새 판 ----

// handoverCheck : -handover-check (옛 판이 exec 전에 부름) : 아는 상태 형식 판
func handoverCheck() {
	fmt.Printf("handover state %d\n", stateVersion)
}

// readHandover : -resume 의 상태 파일에서 gob 부분까지 읽음 (경로표는 loadRIBs 가 이어서)
func readHandover(path string) (*handoverState, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	rd := bufio.NewReaderSize(f, 1<<20)
	head := make([]byte, len(stateMagic)+8)
	if _, err := io.ReadFull(rd, head); err != nil || string(head[:len(stateMagic)]) != stateMagic {
		f.Close()
		return nil, fmt.Errorf("%s : not a handover state file", path)
	}
	n := binary.LittleEndian.Uint64(head[len(stateMagic):])
	if n > 1<<30 {
		f.Close()
		return nil, fmt.Errorf("%s : state %d bytes", path, n)
	}
	g := make([]byte, n)
	if _, err := io.ReadFull(rd, g); err != nil {
		f.Close()
		return nil, fmt.Errorf("%s : %w", path, err)
	}
	hs := &handoverState{}
	if err := gob.NewDecoder(bytes.NewReader(g)).Decode(hs); err != nil {
		f.Close()
		return nil, fmt.Errorf("%s : %w", path, err)
	}
	if hs.Version != stateVersion {
		f.Close()
		return nil, fmt.Errorf("%s : state version %d, this binary knows %d", path, hs.Version, stateVersion)
	}
	hs.f, hs.rd = f, rd
	return hs, nil
}

// restore : 라우터 / peer / 줄 번호 / 통계를 collector 에. 경로표는 비어 있고 기다림 (loadRIBs)
func (hs *handoverState) restore(c *collector) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq.Store(hs.Seq)
	for i := range hs.Routers {
		s := &hs.Routers[i]
		r := s.load()
		for j := range s.Peers {
			p := s.Peers[j].load()
			p.ribWait = !c.noRIB
			r.peers[p.key] = p
			hs.peers = append(hs.peers, p)
			hs.owner = append(hs.owner, r)
		}
		c.routers[r.ip] = r
	}
	hist.mu.Lock()
	hist.started, hist.list = hs.Started, hs.Minutes
	hist.mu.Unlock()
	ribLoading.Store(!c.noRIB)
}

// fileFd : 이어 받은 fd 하나 -> os.File (net.FileListener / FileConn 이 다시 dup 하므로 쓴 뒤 닫음)
func fileFd(fd int, name string) *os.File {
	return os.NewFile(uintptr(fd), name)
}

// listener : 이어 받은 BMP listener
func (hs *handoverState) listener() (net.Listener, error) {
	if *optListen != hs.Listen {
		log.Printf("handover: bmp.listen %s -> %s ignored, keeping the inherited socket (needs a restart)", hs.Listen, *optListen)
	}
	f := fileFd(hs.ListenFd, "bmp listener")
	defer f.Close()
	return net.FileListener(f)
}

// httpListener : 이어 받은 API listener (없으면 nil = startAPI 가 새로 엶)
func (hs *handoverState) httpListener() net.Listener {
	if hs == nil || hs.HTTPFd < 0 {
		return nil
	}
	f := fileFd(hs.HTTPFd, "http listener")
	defer f.Close()
	if *optHTTP != hs.HTTP {
		log.Printf("handover: http.listen %q -> %q ignored, keeping the inherited socket (needs a restart)", hs.HTTP, *optHTTP)
	}
	ln, err := net.FileListener(f)
	if err != nil {
		log.Printf("handover: http listener : %v (opening a new one)", err)
		return nil
	}
	if *optHTTP == "" { // API 를 끔 : 받은 소켓은 닫음
		ln.Close()
		return nil
	}
	return ln
}

// conns : 이어 받은 연결들 (Initiation 뒤 연결은 연결 표에도 넣음). 잡지 못한 연결은 기록만 (라우터가 다시 붙음)
func (hs *handoverState) conns(ct *connTable) []*connRun {
	ct.mu.Lock()
	for ip, t := range hs.Resets {
		ct.reset[ip] = t
	}
	ct.mu.Unlock()
	var list []*connRun
	for _, s := range hs.Conns {
		f := fileFd(s.Fd, "conn "+s.IP)
		conn, err := net.FileConn(f)
		f.Close()
		if err != nil {
			log.Printf("handover: conn %s : %v", s.IP, err)
			continue
		}
		cr := newConnRun(conn, s.IP, s.Pending)
		cr.n, cr.gen, cr.until = s.N, s.Gen, s.Until
		if cr.n > 0 {
			ct.add(s.IP, cr.rc)
		}
		list = append(list, cr)
	}
	return list
}

// resumed : 연결을 모두 이어 읽기 시작함 = 라우터 멈춤 끝
func (hs *handoverState) resumed() {
	pause := time.Since(hs.PausedAt)
	sdNotify("WATCHDOG=1")
	upg.mu.Lock() // count 는 여기서 올림 : -upgrade 명령이 count 를 보고 끝을 앎 (멈춘 시간까지 함께 보이게)
	upg.info.Count, upg.info.LastAt, upg.info.PauseMs = hs.Count, tstr(time.Now()), pause.Milliseconds()
	upg.info.From = strings.TrimSpace(hs.Release + " " + hs.Rev)
	upg.mu.Unlock()
	log.Printf("handover: resumed from %s %s : %d connections, %d routers, %d peers, routers paused %s",
		hs.Release, hs.Rev, len(hs.Conns), len(hs.Routers), len(hs.peers), pause.Round(time.Millisecond))
}

// loadRIBs : 경로표를 뒤에서 읽음 (go 로 부름). peer 마다 다 읽으면 잠금 안에서 넣고 미뤄 둔 UPDATE 를 적용 (rib.go ribLoaded).
// 다 읽으면 상태 파일을 지움. 읽다 틀리면 아직 기다리는 peer 는 빈 경로표에서 이어 감 (기록 : 다음 전체 전송까지 경로 수가 모자람)
func (hs *handoverState) loadRIBs(c *collector) {
	start := time.Now()
	defer func() {
		hs.f.Close()
		os.Remove(hs.f.Name())
		ribLoading.Store(false)
	}()
	if c.noRIB {
		return
	}
	var routes atomic.Int64
	err := readRIBs(hs.rd, len(hs.peers), func(i int, b []byte) error {
		tmp := &peer{}
		if err := tmp.decodeRIB(b); err != nil {
			return err
		}
		r, p := hs.owner[i], hs.peers[i]
		r.mu.Lock()
		if p.ribLoaded(tmp.rib4, tmp.rib6) {
			routes.Add(int64(len(tmp.rib4) + len(tmp.rib6)))
		}
		r.mu.Unlock()
		return nil
	})
	if err != nil {
		left := 0
		for i, p := range hs.peers {
			r := hs.owner[i]
			r.mu.Lock()
			if p.ribLoaded(nil, nil) {
				left++
			}
			r.mu.Unlock()
		}
		log.Printf("handover: route tables : %v : %d peers continue with empty tables (route counts are low until their next full table)", err, left)
	}
	took := time.Since(start)
	upg.mu.Lock()
	upg.info.RIBLoadMs = took.Milliseconds()
	upg.mu.Unlock()
	log.Printf("handover: route tables loaded : %d routes in %s", routes.Load(), took.Round(time.Millisecond))
}

// ---- /api/stats 와 -upgrade 명령 ----

// handoverJSON : /api/stats 의 handover (tcp 입력만 = 이 판이 판 올리기를 앎)
type handoverJSON struct {
	Supported   bool   `json:"supported"`
	Count       int    `json:"count"`                   // 재시작 뒤 판 올리기 수
	LastAt      string `json:"last_at,omitempty"`       // 마지막 판 올리기 (새 판이 시작한 시각)
	From        string `json:"from,omitempty"`          // 그때 옛 판 (판 번호, git rev)
	PauseMs     int64  `json:"pause_ms,omitempty"`      // 라우터 읽기가 멈춘 시간
	RIBLoading  bool   `json:"rib_loading"`             // 경로표를 뒤에서 읽는 중
	RIBLoadMs   int64  `json:"rib_load_ms,omitempty"`   // 경로표 읽기에 걸린 시간
	LastError   string `json:"last_error,omitempty"`    // 마지막으로 그만둔 이유 (옛 판이 계속)
	LastErrorAt string `json:"last_error_at,omitempty"` // 그 시각
}

// statsInfo : /api/stats 에 넣을 것 (tcp 입력이 아니면 nil)
func (u *upgrader) statsInfo() *handoverJSON {
	u.mu.Lock()
	defer u.mu.Unlock()
	if !u.info.Supported {
		return nil
	}
	j := u.info
	j.RIBLoading = ribLoading.Load()
	return &j
}

// runUpgrade : -upgrade 명령. 지금 판이 판 올리기를 아는지 /api/stats 로 확인 -> SIGUSR2 -> 결과를 기다림
func runUpgrade(api string) error {
	before, err := upgradeStats(api)
	if err != nil {
		return fmt.Errorf("%v : the API (http.listen) is needed to check the running process", err)
	}
	if before.Handover == nil || !before.Handover.Supported {
		return fmt.Errorf("running release %s cannot hand over (0.2.0 or later) : use systemctl restart", before.Release)
	}
	b, err := os.ReadFile(*optLock)
	if err != nil {
		return fmt.Errorf("lock file (paths.lock) : %w", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 1 {
		return fmt.Errorf("lock file %s : pid %q", *optLock, strings.TrimSpace(string(b)))
	}
	fmt.Printf("running %s (%s), pid %d : sending SIGUSR2 (new binary %s)\n", before.Release, before.Version, pid, exePath)
	if err := syscall.Kill(pid, syscall.SIGUSR2); err != nil {
		return err
	}
	for range 240 { // 2 분
		time.Sleep(500 * time.Millisecond)
		st, err := upgradeStats(api)
		if err != nil || st.Handover == nil {
			continue // exec 하는 동안 API 가 잠깐 없음
		}
		h := st.Handover
		if h.Count > before.Handover.Count {
			fmt.Printf("done : now %s (%s), routers paused %d ms, route tables loading in the background (see log)\n", st.Release, st.Version, h.PauseMs)
			return nil
		}
		if h.LastErrorAt != "" && h.LastErrorAt != before.Handover.LastErrorAt {
			return fmt.Errorf("aborted, the old process keeps running : %s", h.LastError)
		}
	}
	return errors.New("no result after 2 minutes : see the log")
}

type upgradeView struct {
	Release  string        `json:"release"`
	Version  string        `json:"version"`
	Handover *handoverJSON `json:"handover"`
}

func upgradeStats(api string) (*upgradeView, error) {
	resp, err := httpDo("GET", strings.TrimSuffix(api, "/")+"/api/stats", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var v upgradeView
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return nil, err
	}
	return &v, nil
}
