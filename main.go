// bongsudae : openbmpd 를 대신하는 BMP 수집기
//
// 입력 (-input) :
//
//	tcp   : 라우터에서 BMP 를 직접 받음 (-listen, -allow) = 운영
//	kafka : openbmp.bmp_raw 를 실시간으로 읽음 (-src, -group) = 그림자 실행 (5 단계)
//	file  : bmp_raw 덤프 파일 (tools/kafka_dump) -> 출력 덤프 파일 (-in, -out) = 시험.
//	        -http 를 주면 다 처리한 뒤 API 를 열어 두고 기다림 (전체 경로표로 API 시험, 규칙 변경은 꺼짐)
//
// 출력 : openbmp v1.7 unicast_prefix / router / peer 메시지 -> Kafka 토픽 <-prefix>.<종류>
//
// 설정 파일 (config.go) : /etc/bongsudae/bongsudae.conf (또는 -c). 옵션과 같은 값을 [구역] 키 = 값 으로. 명령줄이 우선.
// systemctl reload (SIGHUP) = 설정 파일 + 규칙 파일을 다시 읽음 (허용 대역, bmp_raw, API 허용 대역은 바로, 나머지는 재시작)
//
// 예 :
//
//	bongsudae                    (운영 : systemd, 값은 모두 설정 파일에서)
//	bongsudae -input tcp -http :5001 -lock /var/bongsudae/run/bongsudae.lock   (설정 파일 없이 : -prefix 기본 openbmp.parsed)
//	bongsudae -input kafka -prefix openbmp.test -lock /var/bongsudae/run/shadow.lock >> log/shadow.log 2>&1   (그림자)
//	bongsudae -status            (현황 표 : 웹 API 를 부름, status.go)
//	bongsudae -set -topics unicast_prefix1 kafka rules routers 192.0.2.95   (규칙 바꾸기)
//	bongsudae -reload            (규칙 파일을 다시 읽음, kill -HUP 과 같음)
//	bongsudae -post routers 192.0.2.20 reset   (BMP 세션을 끊음 -> 라우터가 다시 접속해 전체를 다시 보냄)
package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// 옵션 (C 의 전역 설정 구조체와 같음)
var (
	optConf      = flag.String("c", "", "config file (-input tcp and API commands only). Empty = "+defConfig+" if it exists. Command line options win")
	optInput     = flag.String("input", "tcp", "input : tcp | kafka | file")
	optListen    = flag.String("listen", ":5000", "tcp : listen address for BMP")
	optAllow     = flag.String("allow", "127.0.0.1,::1", "tcp : allowed routers, addresses / prefixes (comma separated), e.g. the loopback range")
	optKafka     = flag.String("kafka", "127.0.0.1:9092", "Kafka bootstrap servers")
	optPrefix    = flag.String("prefix", "openbmp.parsed", "output topic prefix (<prefix>.unicast_prefix / .router / .peer). Tests : openbmp.test")
	optSrc       = flag.String("src", "openbmp.bmp_raw", "kafka : topic to read (bmp_raw)")
	optGroup     = flag.String("group", "bmp-test-shadow", "kafka : consumer group")
	optStart     = flag.Bool("from-start", false, "kafka : a new group starts at the beginning of the topic (replay tests; default = now)")
	optBucket    = flag.Int("key-buckets", 0, "unicast_prefix Kafka key : 0 = peer hash (one message per UPDATE), N = split rows into N buckets by prefix, key <peer hash>.<bucket> (spreads a big peer over partitions)")
	optParts     = flag.Int("partitions", 8, "partitions of topics the collector creates (existing topics are not changed)")
	optRepl      = flag.Int("replication", 1, "replication factor of topics the collector creates")
	optRetention = flag.Duration("retention", 90*time.Minute, "retention (retention.ms) of topics the collector creates")
	optFormat    = flag.String("format", "openbmp", "Kafka message format of unicast_prefix / router / peer : openbmp (v1.7, tab separated) or json (one JSON object per line). bmp_raw is unchanged")
	optRIB       = flag.Bool("rib", true, "keep per-peer route tables in memory (route counts, prefix lookup API). false = forward only, much less memory")
	optRaw       = flag.Bool("raw", true, "tcp : forward received BMP unchanged to <prefix without .parsed>.bmp_raw (openbmp.parsed -> openbmp.bmp_raw)")
	optCHash     = flag.String("chash", "", "tcp : C_HASH_ID in message headers (32 hex digits). Empty = md5(host name), like openbmpd without admin_id")
	optLock      = flag.String("lock", "", "lock file (if another instance holds it : log one line and exit)")
	optIn        = flag.String("in", "", "file : bmp_raw dump file to read")
	optOut       = flag.String("out", "", "file : output dump file prefix (<out>.unicast_prefix.dump ...), none = discard")
	optTest      = flag.Bool("t", false, "check the configuration and rules, then exit")
	optHTTP      = flag.String("http", "", "HTTP API address : 127.0.0.1:5001 (local) | :5001 (all, use -http-allow / firewall) | empty (off)")
	optHTTPOK    = flag.String("http-allow", "", "HTTP API allowed clients, addresses / prefixes (comma separated), others get 403. Empty = everyone")
	optRules     = flag.String("rules", "conf/kafka_rules.json", "Kafka forwarding rules file (read at start and on reload, written on API changes)")
	optToken     = flag.String("token", "conf/api_token", "API key file for changes (server : checks it, -set / -del / -post / -reload : send it)")

	// 웹 API 를 부르는 명령 (status.go) : 인자 = API 경로 (/api/ 뒤), 이름=값 = 질의
	optStatus = flag.Bool("status", false, "GET    : -status routers 192.0.2.100 peers 198.51.100.33")
	optSet    = flag.Bool("set", false, "PUT    : -set -topics a,b|none kafka rules routers IP [peers PEER]")
	optDel    = flag.Bool("del", false, "DELETE : -del kafka rules routers IP [peers PEER]")
	optPost   = flag.Bool("post", false, "POST   : -post routers IP reset (close the BMP session), -post kafka rules reload")
	optReload = flag.Bool("reload", false, "POST   : reload the rules file (= -post kafka rules reload)")
	optTopics = flag.String("topics", "", "-set : topic names (comma separated), none = do not send")
	optJSON   = flag.Bool("json", false, "print the JSON as received instead of a table")
	optAPI    = flag.String("api", "http://127.0.0.1:5001", "HTTP API base URL (also of a collector on another server)")

	// 무중단 판 올리기 (handover.go)
	optUpgrade = flag.Bool("upgrade", false, "hand the running collector (pid in the lock file) over to the binary now at its path, without dropping router sessions (SIGUSR2)")
	optResume  = flag.String("resume", "", "internal : state file from the old process (set by the handover exec)")
	optHOCheck = flag.Bool("handover-check", false, "internal : print the handover state format and exit")
)

// confApplied : 시작 때 설정 파일에서 넣은 항목 (기록용)
var confApplied []string

// resumeState : 판 올리기로 시작했으면 옛 판의 상태 (handover.go), 아니면 nil
var resumeState *handoverState

func main() {
	flag.Parse()
	if *optHOCheck {
		handoverCheck()
		return
	}
	exePath, _ = os.Executable() // 판 올리기 : 같은 경로의 (새) 파일을 exec
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if *optInput == "tcp" { // 운영 / API 명령. 시험 입력 (kafka, file) 은 설정 파일을 읽지 않음 (운영 값이 섞이지 않게)
		var err error
		if confApplied, err = loadConfig(*optConf); err != nil {
			fmt.Fprintln(os.Stderr, "config file :", err)
			os.Exit(1)
		}
	} else if *optConf != "" {
		log.Fatalf("-c only with -input tcp (now -input %s)", *optInput)
	}
	if *optUpgrade {
		if err := runUpgrade(*optAPI); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *optStatus || *optSet || *optDel || *optPost || *optReload {
		if err := runClient(*optAPI, flag.Args()); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := checkOpts(); err != nil {
		log.Fatal(err)
	}
	ha, _ := parseHTTPAllow(*optHTTPOK)
	httpAllow.Store(&ha)
	if *optTest {
		rl, err := newRules(*optRules)
		if err != nil {
			log.Fatal(err)
		}
		if confPath != "" {
			fmt.Printf("ok (config %s : %s)\n", confPath, strings.Join(confApplied, ", "))
		}
		fmt.Printf("ok (rules %s : default %s, routers %d)\n", *optRules, topicsText(rl.set.Default), len(rl.set.Routers))
		return
	}
	if *optResume != "" { // 판 올리기로 시작 : 잠금은 옛 판의 fd 를 그대로 (같은 프로세스)
		if *optInput != "tcp" {
			log.Fatal("-resume only with -input tcp")
		}
		hs, err := readHandover(*optResume)
		if err != nil {
			log.Fatalf("handover: %v (exiting : routers reconnect)", err)
		}
		resumeState = hs
		lockFile = fileFd(hs.LockFd, *optLock)
	} else if *optLock != "" && !lock(*optLock) {
		// 다른 것이 이미 돌고 있음. systemd 에서는 이 줄이 "되풀이 재시작" 을 알려 줌 (cron 때는 조용히 끝났음)
		pid, _ := os.ReadFile(*optLock)
		log.Printf("lock %s : already running (pid %s), exiting", *optLock, strings.TrimSpace(string(pid)))
		return
	}
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// checkOpts : 옵션 검사. 운영 토픽 (openbmp.parsed.*) 은 tcp 입력일 때만 허용
func checkOpts() error {
	if _, err := parseHTTPAllow(*optHTTPOK); err != nil {
		return fmt.Errorf("-http-allow : %w", err)
	}
	if *optParts < 1 || *optParts > 1000 || *optRepl < 1 || *optRepl > 10 || *optRetention < time.Minute {
		return fmt.Errorf("-partitions 1 to 1000, -replication 1 to 10, -retention at least 1m")
	}
	if *optFormat != "openbmp" && *optFormat != "json" {
		return fmt.Errorf("-format %q : openbmp or json", *optFormat)
	}
	if *optBucket < 0 || *optBucket > maxKeyBuckets {
		return fmt.Errorf("-key-buckets %d : 0 to %d", *optBucket, maxKeyBuckets)
	}
	switch *optInput {
	case "tcp":
		_, err := parseAllow(*optAllow)
		return err
	case "kafka":
		if *optPrefix == "openbmp.parsed" || *optPrefix == "openbmp" {
			return fmt.Errorf("-input kafka cannot write production topics (-prefix %s) : use a test prefix such as -prefix openbmp.test", *optPrefix)
		}
		if !strings.HasPrefix(*optGroup, "bmp-test-") {
			return fmt.Errorf("-group must be bmp-test-* (protects production groups)")
		}
	case "file":
		if *optIn == "" || *optOut == "" {
			return errors.New("-input file needs -in and -out")
		}
	default:
		return fmt.Errorf("-input %q ?", *optInput)
	}
	return nil
}

// lock : flock (C 와 같음). 프로세스가 끝나면 커널이 풀어 줌
// lockFile : 잠금 파일. 전역에 두어야 함 - 지역 변수면 GC 가 파일을 닫아 잠금이 풀림 (10-28 버그 : cron 이 매분 하나씩 더 띄움)
var lockFile *os.File

func lock(path string) bool {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("lock %s: %v", path, err)
	}
	if syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return false
	}
	f.Truncate(0)
	fmt.Fprintf(f, "%d\n", os.Getpid())
	lockFile = f // 프로세스가 끝날 때까지 열어 둠 (닫으면 잠금이 풀림)
	return true
}

// run : 입력 모드별 시작
func run() error {
	if *optInput == "file" {
		c, err := runFile(*optIn, *optOut)
		if err != nil || *optHTTP == "" || c == nil {
			return err
		}
		return serveFile(c)
	}
	out, err := newProducer(*optKafka, *optPrefix)
	if err != nil {
		return err
	}
	defer out.close()
	// kill (SIGTERM) / Ctrl-C : 버퍼에 남은 메시지를 보내고 끝남
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if confPath != "" {
		log.Printf("config %s : %s", confPath, strings.Join(confApplied, ", "))
	}
	how := ""
	if resumeState != nil {
		how = " (handover)"
	}
	log.Printf("start bongsudae %s (%s)%s input=%s prefix=%s http=%q rules=%s", release, version(), how, *optInput, *optPrefix, *optHTTP, *optRules)
	rl, err := newRules(*optRules)
	if err != nil {
		return err
	}
	rl.ensure = func(t string) error { return ensureTopic(out.cl, *optPrefix+"."+t) }
	if err := rl.ensureAll(rl.set.allTopics()); err != nil {
		return err
	}
	out.rules = rl
	log.Printf("rules: default %s, routers %d", topicsText(rl.set.Default), len(rl.set.Routers))
	// kill -HUP (= systemctl reload) : 설정 파일, 규칙 파일을 다시 읽음 (C 데몬처럼). 등록하지 않으면 HUP 에 프로세스가 끝남
	// 둘은 따로 : 설정 파일이 틀려도 규칙 파일은 읽음
	addConfHooks(out)
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			reloadConf("SIGHUP")
			rl.reloadLog("SIGHUP")
		}
	}()
	c := newCollector(collectorHash(*optCHash))
	c.keyBuckets = *optBucket
	c.noRIB = !*optRIB
	c.json = *optFormat == "json"
	c.rules, c.prefix, c.tokenPath = rl, *optPrefix, *optToken
	hs := resumeState
	if hs != nil { // 판 올리기 : 라우터 / peer 를 되살리고 경로표는 뒤에서
		hs.restore(c)
		go hs.loadRIBs(c)
	}
	if ln := hs.httpListener(); *optHTTP != "" || ln != nil {
		if err := startAPI(*optHTTP, c, ln); err != nil {
			return err
		}
	}
	if *optInput == "kafka" {
		return runKafka(ctx, *optKafka, *optSrc, *optGroup, c, out)
	}
	if *optRaw {
		if err := out.enableRaw(*optPrefix); err != nil {
			return err
		}
		log.Printf("bmp_raw -> %s", out.rawTopic)
	}
	// kill -USR2 (= bongsudae -upgrade) : 무중단 판 올리기 (handover.go). 등록하지 않으면 USR2 에 프로세스가 끝남
	usr2 := make(chan os.Signal, 1)
	signal.Notify(usr2, syscall.SIGUSR2)
	go func() {
		for range usr2 {
			upg.upgrade()
		}
	}()
	allow, _ := parseAllow(*optAllow)
	if err := runTCP(ctx, *optListen, allow, c, out, hs); err != nil {
		return err
	}
	log.Printf("shutdown : flush kafka and exit") // 연결은 정리됨. Flush 는 run 의 defer out.close()
	return nil
}

// addConfHooks : reload 로 바로 바뀌는 설정 (config.go). 나머지 항목은 재시작해야 바뀜
func addConfHooks(out *producer) {
	confHooks["allow"] = confHook{ // bmp.allow : 새 연결부터, 빠진 라우터의 지금 연결은 끊음
		check: func(v string) error { _, err := parseAllow(v); return err },
		apply: func(v string) string {
			list, _ := parseAllow(v)
			tcpAllow.Store(&list)
			if ct := tcpConns.Load(); ct != nil {
				if n := ct.closeOutside(list); n > 0 {
					return fmt.Sprintf("closed %d connections of removed routers", n)
				}
			}
			return ""
		},
	}
	confHooks["raw"] = confHook{ // bmp.raw : bmp_raw 내보내기 켜고 끔 (켤 때 토픽이 없으면 만듦)
		check: func(v string) error {
			on, err := strconv.ParseBool(v)
			if err != nil || !on {
				return err
			}
			return ensureTopic(out.cl, rawTopicFor(*optPrefix))
		},
		apply: func(v string) string {
			if on, _ := strconv.ParseBool(v); on {
				out.enableRaw(*optPrefix)
			} else {
				out.rawOn.Store(false)
			}
			return ""
		},
	}
	confHooks["http-allow"] = confHook{ // http.allow : 다음 요청부터
		check: func(v string) error { _, err := parseHTTPAllow(v); return err },
		apply: func(v string) string {
			list, _ := parseHTTPAllow(v)
			httpAllow.Store(&list)
			return ""
		},
	}
}

// serveFile : file 입력을 다 처리한 뒤 그 경로표로 웹 API 를 열고 kill / Ctrl-C 까지 기다림.
// 규칙은 파일에서 읽기만 (key 파일을 비워 변경 API 를 끔 = 시험이 운영 설정을 덮어쓰지 않음)
func serveFile(c *collector) error {
	rl, err := newRules(*optRules)
	if err != nil {
		return err
	}
	c.rules, c.tokenPath = rl, ""
	if err := startAPI(*optHTTP, c, nil); err != nil {
		return err
	}
	log.Printf("file done : API %s stays open (kill / Ctrl-C to stop)", *optHTTP)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	<-ctx.Done()
	return nil
}

// runFile : bmp_raw 덤프를 읽어 처리하고 결과를 종류별 덤프 (<out>.<종류>.dump) 로 씀. 시각 = 원래 레코드 시각.
// 돌려주는 collector = 처리가 끝난 경로표 (-http 시험용)
func runFile(inPath, outPrefix string) (*collector, error) {
	fi, err := os.Open(inPath)
	if err != nil {
		return nil, err
	}
	defer fi.Close()
	r := bufio.NewReaderSize(fi, 1<<20)

	// 종류마다 출력 파일 하나
	outs := map[string]*bufio.Writer{}
	pipe := false // 출력이 FIFO (tools/regress) : 입력 레코드마다 바로 내보냄 (모아 두면 두 bongsudae 를 나란히 비교할 때 서로 기다리다 멈춤)
	offset := map[string]int64{}
	for _, k := range allKinds {
		if outPrefix == "none" { // 출력을 버림 (API 시험만 할 때 : 전체 경로표면 출력이 수십 GB)
			outs[k] = bufio.NewWriter(io.Discard)
			continue
		}
		fo, err := os.Create(outPrefix + "." + k + ".dump")
		if err != nil {
			return nil, err
		}
		defer fo.Close()
		w := bufio.NewWriterSize(fo, 1<<20)
		defer w.Flush() // defer 는 거꾸로 : Flush 다음 Close
		outs[k] = w
		if st, err := fo.Stat(); err == nil && st.Mode()&os.ModeNamedPipe != 0 {
			pipe = true
		}
	}

	var c *collector
	for {
		d, err := readDump(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		raw, err := parseRaw(d.value)
		if err != nil {
			return nil, fmt.Errorf("offset %d: %w", d.offset, err)
		}
		if c == nil {
			c = newCollector(raw.cHash)
			c.keyBuckets = *optBucket
			c.noRIB = !*optRIB
			c.json = *optFormat == "json"
		}
		for _, m := range c.handle(raw.routerIP, raw.routerHash, raw.body, d.ts) {
			writeDump(outs[m.kind], &dumpRecord{offset: offset[m.kind], ts: d.ts, key: []byte(m.key), value: m.value})
			offset[m.kind]++
			if pipe {
				outs[m.kind].Flush()
			}
		}
	}
	if c != nil {
		c.logStat(nil)
	}
	return c, nil
}

// rawMsg : openbmp.bmp_raw 메시지 (머리 V / C_HASH_ID / R_HASH / R_IP / L + 빈 줄 + BMP 바이트)
type rawMsg struct {
	cHash      string
	routerHash string
	routerIP   string
	body       []byte
}

func parseRaw(v []byte) (rawMsg, error) {
	var m rawMsg
	i := bytes.Index(v, []byte("\n\n"))
	if i < 0 {
		return m, errors.New("bmp_raw: no header end")
	}
	length := -1
	for _, line := range bytes.Split(v[:i], []byte("\n")) {
		k, val, _ := bytes.Cut(line, []byte(": "))
		switch string(k) {
		case "C_HASH_ID":
			m.cHash = string(val)
		case "R_HASH":
			m.routerHash = string(val)
		case "R_IP":
			m.routerIP = string(val)
		case "L":
			length, _ = strconv.Atoi(string(val))
		}
	}
	m.body = v[i+2:]
	if length != len(m.body) || !validIP(m.routerIP) {
		return m, fmt.Errorf("bmp_raw: bad header (L=%d body=%d R_IP=%q)", length, len(m.body), m.routerIP)
	}
	return m, nil
}

// collectorHash : -chash 가 비면 md5(호스트 이름) (openbmpd 가 admin_id 를 안 적었을 때와 같은 방식)
func collectorHash(v string) string {
	if v != "" {
		return v
	}
	h, err := os.Hostname()
	if err != nil {
		h = "bongsudae"
	}
	return md5hex(h)
}
