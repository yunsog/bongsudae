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
)

// 옵션 (C 의 전역 설정 구조체와 같음)
var (
	optConf   = flag.String("c", "", "설정 파일 (-input tcp 와 API 명령만). 빈 값 = "+defConfig+" 가 있으면 그것. 명령줄 옵션이 설정 파일보다 우선")
	optInput  = flag.String("input", "tcp", "입력 : tcp | kafka | file")
	optListen = flag.String("listen", ":5000", "tcp : 받을 주소")
	optAllow  = flag.String("allow", "127.0.0.1,::1", "tcp : 허용 주소 / 대역 (쉼표 구분). 라우터 loopback 대역을 적음")
	optKafka  = flag.String("kafka", "127.0.0.1:9092", "Kafka 주소")
	optPrefix = flag.String("prefix", "openbmp.parsed", "출력 토픽 앞부분 (<prefix>.unicast_prefix / .router / .peer). 시험은 openbmp.test")
	optSrc    = flag.String("src", "openbmp.bmp_raw", "kafka : 읽을 토픽")
	optGroup  = flag.String("group", "bmp-test-shadow", "kafka : consumer group")
	optStart  = flag.Bool("from-start", false, "kafka : 새 group 이면 토픽 처음부터 (재생 시험용, 기본은 지금부터)")
	optRaw    = flag.Bool("raw", true, "tcp : 받은 BMP 를 그대로 <prefix 에서 .parsed 뺀 것>.bmp_raw 로 (openbmp.parsed -> openbmp.bmp_raw)")
	optCHash  = flag.String("chash", "", "tcp : 메시지 머리 C_HASH_ID (32 자리 16 진수). 빈칸 = md5(호스트 이름) (openbmpd 기본 admin_id 와 같은 방식)")
	optLock   = flag.String("lock", "", "잠금 파일 (이미 돌고 있으면 기록 한 줄 남기고 끝남 : 둘이 뜨지 않게)")
	optIn     = flag.String("in", "", "file : bmp_raw 덤프 파일")
	optOut    = flag.String("out", "", "file : 출력 덤프 파일 앞부분 (<out>.unicast_prefix.dump 등), none = 버림")
	optTest   = flag.Bool("t", false, "설정만 검사하고 끝남")
	optHTTP   = flag.String("http", "", "웹 API 주소 : 127.0.0.1:5001 (로컬) | :5001 (글로벌, 접근 제어는 방화벽) | 빈칸 (끔)")
	optHTTPOK = flag.String("http-allow", "", "웹 API 허용 주소 / 대역 (쉼표 구분), 밖이면 403. 빈칸 = 모두 (방화벽만)")
	optRules  = flag.String("rules", "conf/kafka_rules.json", "Kafka 전달 규칙 파일 (시작할 때 읽음, API 로 바꿀 때 씀)")
	optToken  = flag.String("token", "conf/api_token", "규칙 변경 key 파일 (서버 : 검사, -set / -del / -reload : 보냄)")

	// 웹 API 를 부르는 명령 (status.go) : 인자 = API 경로 (/api/ 뒤), 이름=값 = 질의
	optStatus = flag.Bool("status", false, "GET    : -status routers 192.0.2.100 peers 198.51.100.33")
	optSet    = flag.Bool("set", false, "PUT    : -set -topics a,b|none kafka rules routers IP [peers PEER]")
	optDel    = flag.Bool("del", false, "DELETE : -del kafka rules routers IP [peers PEER]")
	optPost   = flag.Bool("post", false, "POST   : -post routers IP reset (BMP 세션 끊기), -post kafka rules reload")
	optReload = flag.Bool("reload", false, "POST   : 규칙 파일을 다시 읽게 함 (= -post kafka rules reload)")
	optTopics = flag.String("topics", "", "-set : 토픽 이름 (쉼표 구분), none = 보내지 않음")
	optJSON   = flag.Bool("json", false, "표 대신 받은 JSON 그대로")
	optAPI    = flag.String("api", "http://127.0.0.1:5001", "웹 API 주소 (다른 서버의 collector 도)")
)

// confApplied : 시작 때 설정 파일에서 넣은 항목 (기록용)
var confApplied []string

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if *optInput == "tcp" { // 운영 / API 명령. 시험 입력 (kafka, file) 은 설정 파일을 읽지 않음 (운영 값이 섞이지 않게)
		var err error
		if confApplied, err = loadConfig(*optConf); err != nil {
			fmt.Fprintln(os.Stderr, "설정 파일 :", err)
			os.Exit(1)
		}
	} else if *optConf != "" {
		log.Fatalf("-c 는 -input tcp 에서만 (지금 -input %s)", *optInput)
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
			fmt.Printf("ok (설정 %s : %s)\n", confPath, strings.Join(confApplied, ", "))
		}
		fmt.Printf("ok (rules %s : default %s, routers %d)\n", *optRules, topicsText(rl.set.Default), len(rl.set.Routers))
		return
	}
	if *optLock != "" && !lock(*optLock) {
		// 다른 것이 이미 돌고 있음. systemd 에서는 이 줄이 "되풀이 재시작" 을 알려 줌 (cron 때는 조용히 끝났음)
		pid, _ := os.ReadFile(*optLock)
		log.Printf("lock %s : 이미 돌고 있음 (pid %s), 끝남", *optLock, strings.TrimSpace(string(pid)))
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
	switch *optInput {
	case "tcp":
		_, err := parseAllow(*optAllow)
		return err
	case "kafka":
		if *optPrefix == "openbmp.parsed" || *optPrefix == "openbmp" {
			return fmt.Errorf("-input kafka 는 운영 토픽에 쓸 수 없음 (-prefix %s) : -prefix openbmp.test 처럼 시험 토픽을 적음", *optPrefix)
		}
		if !strings.HasPrefix(*optGroup, "bmp-test-") {
			return fmt.Errorf("-group 은 bmp-test-* 로 (운영 group 보호)")
		}
	case "file":
		if *optIn == "" || *optOut == "" {
			return errors.New("-input file 은 -in, -out 필요")
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
		log.Printf("설정 %s : %s", confPath, strings.Join(confApplied, ", "))
	}
	log.Printf("start bongsudae %s (%s) input=%s prefix=%s http=%q rules=%s", release, version(), *optInput, *optPrefix, *optHTTP, *optRules)
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
	c.rules, c.prefix, c.tokenPath = rl, *optPrefix, *optToken
	if *optHTTP != "" {
		if err := startAPI(*optHTTP, c); err != nil {
			return err
		}
	}
	if *optInput == "kafka" {
		return runKafka(ctx, *optKafka, *optSrc, *optGroup, c, out)
	}
	// signal 은 따로 받음 : ctx 는 run 이 오류로 돌아올 때 (defer stop) 도 끝나므로, ctx 로 기다리면
	// listen 실패 같은 오류가 "signal" 로 기록되고 exit 0 이 됨 (10-30 교체 때 봄)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		s := <-sig
		log.Printf("signal %v: flush and exit", s)
		out.close()
		os.Exit(0)
	}()
	if *optRaw {
		if err := out.enableRaw(*optPrefix); err != nil {
			return err
		}
		log.Printf("bmp_raw -> %s", out.rawTopic)
	}
	allow, _ := parseAllow(*optAllow)
	return runTCP(*optListen, allow, c, out)
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
					return fmt.Sprintf("빠진 라우터 연결 %d 개 끊음", n)
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
	if err := startAPI(*optHTTP, c); err != nil {
		return err
	}
	log.Printf("file done : API %s 열어 둠 (끝내려면 kill / Ctrl-C)", *optHTTP)
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
