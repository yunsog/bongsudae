// config.go : 설정 파일 (기본 /etc/bongsudae/bongsudae.conf, ini 모양)
//
// 모양
//
//	# 주석 (줄 어디서나 # 뒤는 무시)
//	[bmp]                         구역
//	listen = :5000                키 = 값
//	allow  = 203.0.113.0/24,       값이 쉼표로 끝나면 다음 줄에 이어짐 (목록을 여러 줄로)
//	         10.0.0.0/8
//
// 구역.키 를 명령줄 옵션 이름으로 바꿔 flag.Set 으로 넣음 (confKeys 표). 없는 키 = 옵션 기본값.
// 명령줄에 준 옵션은 건드리지 않음 (명령줄이 우선). 모르는 키 / 같은 키 두 번 / 잘못된 값 = 오류 -> 시작하지 않음.
// 읽는 때 : 운영 (-input tcp) 과 API 명령 (-status 등 : key 파일 위치). 시험 입력 (-input kafka / file) 은 읽지 않음.
//
// systemctl reload (SIGHUP) : reloadConf. 순서
//  1. 시작 때 읽은 파일을 같은 규칙으로 다시 읽음 (명령줄이 우선)
//  2. 바뀐 값을 모두 먼저 확인. 하나라도 틀리면 아무것도 안 바꿈 = 지금 값으로 계속
//  3. 재시작해야 바뀌는 항목 (듣는 주소, Kafka, 토픽, 파일 경로 등) 은 지금 값을 유지하고 경고만
//  4. 나머지를 바꿈. 기록에 바뀐 항목만 "키 옛값 -> 새값"
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
)

const defConfig = "/etc/bongsudae/bongsudae.conf"

// confKeys : 설정 파일 "구역.키" -> 옵션 이름
var confKeys = map[string]string{
	"paths.rules": "rules",
	"paths.token": "token",
	"paths.lock":  "lock",

	"bmp.listen": "listen",
	"bmp.allow":  "allow",
	"bmp.raw":    "raw",
	"bmp.chash":  "chash",

	"kafka.brokers": "kafka",
	"kafka.prefix":  "prefix",

	"http.listen": "http",
	"http.allow":  "http-allow",
}

// confEntry : 설정 파일 한 항목 (줄 번호는 오류 표시용)
type confEntry struct {
	Key  string // 구역.키
	Val  string // 목록이면 "a,b,c" (공백 뺌)
	Line int
}

// parseConf : 설정 파일 글자 -> 항목 목록
func parseConf(r io.Reader) ([]confEntry, error) {
	var out []confEntry
	seen := map[string]int{}
	section := ""
	sc := bufio.NewScanner(r)
	n := 0
	var cur *confEntry // 쉼표로 끝나 다음 줄을 기다리는 항목
	for sc.Scan() {
		n++
		line := sc.Text()
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		if cur != nil { // 앞 줄이 쉼표로 끝남 -> 이어 붙임
			if line == "" {
				continue
			}
			if strings.HasPrefix(line, "[") || strings.Contains(line, "=") {
				return nil, fmt.Errorf("%d 줄 : %s 의 목록이 쉼표로 끝났는데 다음 값이 없음", cur.Line, cur.Key)
			}
			cur.Val += line
			if !strings.HasSuffix(line, ",") {
				out = append(out, *cur)
				cur = nil
			}
			continue
		}
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[") {
			if !strings.HasSuffix(line, "]") {
				return nil, fmt.Errorf("%d 줄 : 구역 이름 %q", n, line)
			}
			section = strings.TrimSpace(line[1 : len(line)-1])
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("%d 줄 : \"키 = 값\" 이 아님 : %q", n, line)
		}
		key := section + "." + strings.TrimSpace(k)
		if _, ok := confKeys[key]; !ok {
			return nil, fmt.Errorf("%d 줄 : 모르는 키 [%s] %s", n, section, strings.TrimSpace(k))
		}
		if prev, dup := seen[key]; dup {
			return nil, fmt.Errorf("%d 줄 : %s 가 %d 줄에도 있음", n, key, prev)
		}
		seen[key] = n
		e := confEntry{Key: key, Val: strings.TrimSpace(v), Line: n}
		if strings.HasSuffix(e.Val, ",") {
			cur = &e
			continue
		}
		out = append(out, e)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if cur != nil {
		return nil, fmt.Errorf("%d 줄 : %s 의 목록이 쉼표로 끝났는데 파일이 끝남", cur.Line, cur.Key)
	}
	for i := range out {
		out[i].Val = normList(out[i].Val)
	}
	return out, nil
}

// normList : "a, b ,c" -> "a,b,c" (쉼표 없는 값은 그대로)
func normList(s string) string {
	if !strings.Contains(s, ",") {
		return s
	}
	var parts []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			parts = append(parts, p)
		}
	}
	return strings.Join(parts, ",")
}

// readConf : 파일 하나를 읽어 항목 목록
func readConf(path string) ([]confEntry, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	entries, err := parseConf(fh)
	if err != nil {
		return nil, fmt.Errorf("%s %w", path, err)
	}
	return entries, nil
}

// 시작 때 정한 것 (reload 가 씀)
var (
	confPath  string          // 읽은 설정 파일 ("" = 없음)
	cmdGiven  map[string]bool // 명령줄에 준 옵션 (설정 파일보다 우선)
	confMu    sync.Mutex      // reload 둘이 겹치지 않게
	confHooks = map[string]confHook{}
)

// confHook : reload 로 바로 바뀌는 옵션 하나. check = 바꾸기 전 확인, apply = 바꿈 (기록에 붙일 글자를 돌려줌)
// 등록 안 된 옵션은 재시작해야 바뀜
type confHook struct {
	check func(v string) error
	apply func(v string) string
}

// loadConfig : path 가 비었으면 기본 파일이 있을 때만 읽음. 돌려주는 것 = 넣은 항목 (기록용 "키=값")
func loadConfig(path string) ([]string, error) {
	cmdGiven = map[string]bool{}
	flag.Visit(func(fl *flag.Flag) { cmdGiven[fl.Name] = true })
	if path == "" {
		if _, err := os.Stat(defConfig); errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		path = defConfig
	}
	entries, err := readConf(path)
	if err != nil {
		return nil, err
	}
	var applied []string
	for _, e := range entries {
		name := confKeys[e.Key]
		if cmdGiven[name] {
			applied = append(applied, e.Key+"=(명령줄 -"+name+")")
			continue
		}
		if err := flag.Set(name, e.Val); err != nil {
			return nil, fmt.Errorf("%s %d 줄 : %s = %q : %v", path, e.Line, e.Key, e.Val, err)
		}
		applied = append(applied, e.Key+"="+e.Val)
	}
	confPath = path
	return applied, nil
}

// confChange : reload 때 바뀐 항목 하나
type confChange struct {
	key, name, old, new string
}

// reloadConf : 설정 파일을 다시 읽어 바꿀 수 있는 것만 바꿈. 실패하면 아무것도 안 바꾸고 오류
func reloadConf(who string) error {
	confMu.Lock()
	defer confMu.Unlock()
	if confPath == "" {
		log.Printf("설정 다시 읽기 (%s) : 설정 파일 없이 시작함 (규칙 파일만 다시 읽음)", who)
		return nil
	}
	entries, err := readConf(confPath)
	if err != nil {
		return confFail(err)
	}
	file := map[string]confEntry{}
	for _, e := range entries {
		file[e.Key] = e
	}
	keys := make([]string, 0, len(confKeys))
	for k := range confKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// 1. 새 값 (파일에 없으면 기본값) 과 지금 값 비교
	var live, restart []confChange
	for _, k := range keys {
		name := confKeys[k]
		if cmdGiven[name] {
			continue // 명령줄이 우선
		}
		fl := flag.Lookup(name)
		nv := fl.DefValue
		if e, ok := file[k]; ok {
			nv = e.Val
		}
		if nv == fl.Value.String() {
			continue
		}
		ch := confChange{k, name, fl.Value.String(), nv}
		if _, ok := confHooks[name]; ok {
			live = append(live, ch)
		} else {
			restart = append(restart, ch)
		}
	}
	// 2. 바꾸기 전에 모두 확인
	for _, ch := range live {
		if err := confHooks[ch.name].check(ch.new); err != nil {
			return confFail(fmt.Errorf("%s = %q : %v", ch.key, ch.new, err))
		}
	}
	// 3. 바꿈
	var done []string
	for _, ch := range live {
		flag.Set(ch.name, ch.new)
		s := fmt.Sprintf("%s %q -> %q", ch.key, ch.old, ch.new)
		if note := confHooks[ch.name].apply(ch.new); note != "" {
			s += " (" + note + ")"
		}
		done = append(done, s)
	}
	if len(done) == 0 {
		log.Printf("설정 다시 읽음 (%s, %s) : 바뀐 것 없음", confPath, who)
	} else {
		log.Printf("설정 다시 읽음 (%s, %s) : %s", confPath, who, strings.Join(done, ", "))
	}
	if len(restart) > 0 {
		var s []string
		for _, ch := range restart {
			s = append(s, fmt.Sprintf("%s (지금 %q, 파일 %q)", ch.key, ch.old, ch.new))
		}
		log.Printf("설정 다시 읽음 : 재시작해야 바뀌는 항목 (지금 값 유지) : %s", strings.Join(s, ", "))
	}
	return nil
}

func confFail(err error) error {
	log.Printf("설정 다시 읽기 실패, 지금 값 유지 : %v", err)
	return err
}
