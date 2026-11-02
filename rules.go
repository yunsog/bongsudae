// Kafka 전달 규칙 : unicast_prefix 메시지를 어느 토픽 (0 개 이상) 으로 보낼지
//
//	default                    모든 라우터의 기본 (지울 수 없음)
//	 └ router {ip}             그 라우터의 기본 (없으면 default)
//	    └ router + peer {ip}   그 peer 만 (없으면 router)
//
// 값 = 토픽 이름 목록 (unicast_prefix 로 시작). [] = 보내지 않음. 실제 토픽 = -prefix + "." + 이름.
// router / peer 메시지는 규칙과 상관없이 늘 보냄.
//
// 파일 (-rules, 기본 conf/kafka_rules.json) :
//   - 시작할 때 읽음 (없으면 default = [unicast_prefix])
//   - API 로 바꿀 때마다 바로 씀 (임시 파일 -> rename : 쓰다가 꺼져도 깨지지 않음). 끝날 때는 쓰지 않음
//   - 손으로 고친 뒤 : 재시작 / -reload / kill -HUP 으로 적용. 오류가 있으면 이전 규칙 그대로
//   - 손으로 고친 뒤 reload 전에 API 로 바꾸려 하면 거절 (409) : 손으로 고친 것이 덮어써지지 않게
//
// 잠금 : rules.mu 하나 (collector.mu 안에서 읽기만 함, 반대 순서로 잡는 곳 없음)
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// routerRule : 라우터 하나의 규칙. Topics == nil 이면 라우터 규칙 없음 (default 를 따름), 빈 목록이면 보내지 않음
type routerRule struct {
	Topics *[]string           `json:"topics,omitempty"`
	Peers  map[string][]string `json:"peers,omitempty"` // peer 주소 -> 토픽들
}

// ruleSet : 규칙 전체 (= 파일 모양)
type ruleSet struct {
	Default []string               `json:"default"`
	Routers map[string]*routerRule `json:"routers"`
}

// rules : 지금 규칙 + 파일 상태 + 토픽별 1 분 통계
// 잠금 둘 (0.2.0) : editMu = 편집 (change / reload : 파일 읽기, 검사, 쓰기, fsync) 을 하나씩.
// mu = set / sum / loaded 를 바꿔 끼우는 짧은 순간만 (라우터 goroutine 의 topics() 가 RLock).
// set / sum 은 두 잠금을 다 잡고 바꾸므로 읽기는 둘 중 하나만 잡으면 됨. 순서 editMu -> mu
type rules struct {
	editMu  sync.Mutex
	mu      sync.RWMutex
	path    string
	set     *ruleSet  // 바꿀 때는 복사본을 만들어 통째로 바꿈
	sum     [32]byte  // 마지막으로 읽거나 쓴 파일 내용의 sha256 (없었으면 0)
	loaded  time.Time // 마지막으로 읽거나 쓴 시각
	ensure  func(topic string) error
	ensured map[string]bool

	smu       sync.Mutex
	cur, last map[string]*topicCount // 지금 1 분, 지난 1 분
}

type topicCount struct {
	Msgs int `json:"msgs"` // 메시지 수 (/api/stats 의 topics : 0.2.0 전에는 꼬리표가 없어 "Msgs" / "Rows" 로 나갔음)
	Rows int `json:"rows"` // 줄 수
}

var topicName = regexp.MustCompile(`^unicast_prefix[A-Za-z0-9_.-]{0,64}$`)

func defaultRules() *ruleSet {
	return &ruleSet{Default: []string{kindPrefix}, Routers: map[string]*routerRule{}}
}

// newRules : 파일을 읽어 규칙을 만듦 (파일이 없으면 기본)
func newRules(path string) (*rules, error) {
	rl := &rules{path: path, ensured: map[string]bool{}, cur: map[string]*topicCount{}, last: map[string]*topicCount{}}
	set, sum, err := readRules(path)
	if errors.Is(err, os.ErrNotExist) {
		set, err = defaultRules(), nil
	}
	if err != nil {
		return nil, err
	}
	rl.set, rl.sum, rl.loaded = set, sum, time.Now()
	return rl, nil
}

// readRules : 파일 읽기 + 검사 (주소는 정규 글자로)
func readRules(path string) (*ruleSet, [32]byte, error) {
	var sum [32]byte
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, sum, err
	}
	sum = sha256.Sum256(b)
	set := &ruleSet{}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields() // 글자 틀린 키 (예 "topic") 를 조용히 무시하지 않게
	if err := d.Decode(set); err != nil {
		return nil, sum, fmt.Errorf("%s: %s", path, jsonErr(b, err))
	}
	if err := set.check(); err != nil {
		return nil, sum, fmt.Errorf("%s: %w", path, err)
	}
	return set, sum, nil
}

// jsonErr : JSON 문법 오류에 줄 번호를 붙임
func jsonErr(b []byte, err error) string {
	var se *json.SyntaxError
	var te *json.UnmarshalTypeError
	off := int64(-1)
	if errors.As(err, &se) {
		off = se.Offset
	} else if errors.As(err, &te) {
		off = te.Offset
	}
	if off < 0 || off > int64(len(b)) {
		return err.Error()
	}
	return fmt.Sprintf("line %d: %v", bytes.Count(b[:off], []byte("\n"))+1, err)
}

// check : 검사하고 주소 / 토픽 목록을 정리 (주소 정규화, 토픽 중복 제거)
func (s *ruleSet) check() error {
	if s.Default == nil {
		return errors.New(`"default" missing (use [] to send nothing)`)
	}
	var err error
	if s.Default, err = checkTopics(s.Default); err != nil {
		return fmt.Errorf("default: %w", err)
	}
	routers := map[string]*routerRule{}
	for ip, rr := range s.Routers {
		key, err := normIP(ip)
		if err != nil {
			return fmt.Errorf("router %q: %w", ip, err)
		}
		if rr == nil {
			return fmt.Errorf("router %s: null", ip)
		}
		if routers[key] != nil {
			return fmt.Errorf("router %s: listed twice", key)
		}
		if rr.Topics != nil {
			t, err := checkTopics(*rr.Topics)
			if err != nil {
				return fmt.Errorf("router %s: %w", key, err)
			}
			rr.Topics = &t
		}
		peers := map[string][]string{}
		for pip, t := range rr.Peers {
			pk, err := normIP(pip)
			if err != nil {
				return fmt.Errorf("router %s peer %q: %w", key, pip, err)
			}
			if t == nil {
				return fmt.Errorf("router %s peer %s: null (use [] to send nothing)", key, pk)
			}
			if _, dup := peers[pk]; dup {
				return fmt.Errorf("router %s peer %s: listed twice", key, pk)
			}
			if peers[pk], err = checkTopics(t); err != nil {
				return fmt.Errorf("router %s peer %s: %w", key, pk, err)
			}
		}
		rr.Peers = peers
		if rr.Topics == nil && len(peers) == 0 {
			continue // 빈 항목은 버림
		}
		routers[key] = rr
	}
	s.Routers = routers
	return nil
}

// checkTopics : 이름 검사, 중복 제거 (순서 유지). 결과는 nil 이 아님 ([] 도 값)
func checkTopics(in []string) ([]string, error) {
	out := []string{}
	for _, t := range in {
		if !topicName.MatchString(t) {
			return nil, fmt.Errorf("topic name %q : must start with unicast_prefix, characters A-Z a-z 0-9 _ . -", t)
		}
		if !slices.Contains(out, t) {
			out = append(out, t)
		}
	}
	return out, nil
}

// normIP : 주소 글자를 정규 모양으로 (2001:0db8::0020:63 -> 2001:db8::20:63)
func normIP(s string) (string, error) {
	a, err := netip.ParseAddr(s)
	if err != nil {
		return "", errors.New("not an address")
	}
	return a.Unmap().String(), nil
}

// clone : 깊은 복사 (바꾸기 전에)
func (s *ruleSet) clone() *ruleSet {
	n := &ruleSet{Default: slices.Clone(s.Default), Routers: map[string]*routerRule{}}
	for ip, rr := range s.Routers {
		c := &routerRule{Peers: map[string][]string{}}
		if rr.Topics != nil {
			t := slices.Clone(*rr.Topics)
			c.Topics = &t
		}
		for p, t := range rr.Peers {
			c.Peers[p] = slices.Clone(t)
		}
		n.Routers[ip] = c
	}
	return n
}

// lookup : 적용할 토픽과 그 출처 (peer / router / default)
func (s *ruleSet) lookup(router, peer string) ([]string, string) {
	if rr := s.Routers[router]; rr != nil {
		if t, ok := rr.Peers[peer]; ok && peer != "" {
			return t, "peer"
		}
		if rr.Topics != nil {
			return *rr.Topics, "router"
		}
	}
	return s.Default, "default"
}

// allTopics : 규칙에 나오는 토픽 이름 모두
func (s *ruleSet) allTopics() []string {
	var out []string
	add := func(t []string) {
		for _, x := range t {
			if !slices.Contains(out, x) {
				out = append(out, x)
			}
		}
	}
	add(s.Default)
	for _, rr := range s.Routers {
		if rr.Topics != nil {
			add(*rr.Topics)
		}
		for _, t := range rr.Peers {
			add(t)
		}
	}
	sort.Strings(out)
	return out
}

// flat : "default" / "router IP" / "router IP peer P" -> 토픽 글자 (바뀐 것 비교용)
func (s *ruleSet) flat() map[string]string {
	m := map[string]string{"default": topicsText(s.Default)}
	for ip, rr := range s.Routers {
		if rr.Topics != nil {
			m["router "+ip] = topicsText(*rr.Topics)
		}
		for p, t := range rr.Peers {
			m["router "+ip+" peer "+p] = topicsText(t)
		}
	}
	return m
}

// topicsText : [a b] / none
func topicsText(t []string) string {
	if len(t) == 0 {
		return "none"
	}
	return strings.Join(t, ",")
}

// diff : 두 규칙의 차이 (사람이 읽는 줄들)
func diff(old, cur *ruleSet) []string {
	a, b := old.flat(), cur.flat()
	var out []string
	for k, v := range b {
		if ov, ok := a[k]; !ok {
			out = append(out, fmt.Sprintf("+ %s : %s", k, v))
		} else if ov != v {
			out = append(out, fmt.Sprintf("~ %s : %s -> %s", k, ov, v))
		}
	}
	for k, v := range a {
		if _, ok := b[k]; !ok {
			out = append(out, fmt.Sprintf("- %s : %s", k, v))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i][2:] < out[j][2:] })
	return out
}

// ---- 처리 쪽 (메시지마다) ----

// topics : 이 라우터 / peer 의 unicast_prefix 를 보낼 토픽들. rl 이 nil 이면 (파일 입력) 기본
func (rl *rules) topics(router, peer string) []string {
	if rl == nil {
		return []string{kindPrefix}
	}
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	t, _ := rl.set.lookup(router, peer)
	return t
}

// effective : API 현황용 (토픽, 출처)
func (rl *rules) effective(router, peer string) ([]string, string) {
	if rl == nil {
		return []string{kindPrefix}, "default"
	}
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.set.lookup(router, peer)
}

// count : 토픽별 1 분 통계 (보낼 때)
func (rl *rules) count(topic string, rows int) {
	rl.smu.Lock()
	tc := rl.cur[topic]
	if tc == nil {
		tc = &topicCount{}
		rl.cur[topic] = tc
	}
	tc.Msgs++
	tc.Rows += rows
	rl.smu.Unlock()
}

// rotate : 1 분마다 (logStat). 지난 1 분 통계를 글자로 돌려줌
func (rl *rules) rotate() string {
	rl.smu.Lock()
	defer rl.smu.Unlock()
	rl.last, rl.cur = rl.cur, map[string]*topicCount{}
	var p []string
	for t, c := range rl.last {
		p = append(p, fmt.Sprintf("%s=%d/%d", t, c.Msgs, c.Rows))
	}
	sort.Strings(p)
	return strings.Join(p, " ")
}

func (rl *rules) lastMinute() map[string]topicCount {
	rl.smu.Lock()
	defer rl.smu.Unlock()
	m := map[string]topicCount{}
	for t, c := range rl.last {
		m[t] = *c
	}
	return m
}

// ---- 바꾸기 (API / reload) ----

// ensureAll : 토픽들이 Kafka 에 있게 (처음 쓰는 것만 만듦)
func (rl *rules) ensureAll(topics []string) error {
	if rl.ensure == nil {
		return nil
	}
	for _, t := range topics {
		rl.mu.RLock()
		done := rl.ensured[t]
		rl.mu.RUnlock()
		if done {
			continue
		}
		if err := rl.ensure(t); err != nil {
			return err
		}
		rl.mu.Lock()
		rl.ensured[t] = true
		rl.mu.Unlock()
	}
	return nil
}

var errFileChanged = errors.New("rules file was edited by hand (not applied yet) : -reload first")

// change : 규칙 하나를 바꾸고 파일에 씀. edit 은 복사본을 고침 (오류면 아무것도 안 바뀜)
// 파일 I/O 는 editMu 만 잡고 함 (rl.mu 를 잡으면 그동안 모든 라우터 처리가 topics() 에서 멈춤)
func (rl *rules) change(edit func(s *ruleSet) error) error {
	rl.editMu.Lock()
	defer rl.editMu.Unlock()
	// 손으로 고친 것을 덮어쓰지 않게
	var sum [32]byte
	if b, err := os.ReadFile(rl.path); err == nil {
		sum = sha256.Sum256(b)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if sum != rl.sum {
		return errFileChanged
	}
	n := rl.set.clone()
	if err := edit(n); err != nil {
		return err
	}
	if err := n.check(); err != nil {
		return err
	}
	b, err := json.MarshalIndent(n, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if err := writeAtomic(rl.path, b); err != nil {
		return err
	}
	rl.mu.Lock()
	rl.set, rl.sum, rl.loaded = n, sha256.Sum256(b), time.Now()
	rl.mu.Unlock()
	return nil
}

// writeAtomic : 임시 파일에 쓰고 fsync 뒤 rename (C 의 write + fsync + rename 과 같음)
func writeAtomic(path string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		os.Remove(tmp)
	}
	return err
}

// reload : 파일을 다시 읽어 적용. 오류면 이전 규칙 그대로. 돌려주는 것 = 바뀐 줄들
// editMu 를 잡고 읽기부터 바꿔 끼우기까지 (그 사이에 change 가 끼어들어 옛 내용으로 덮어쓰지 않게)
func (rl *rules) reload() ([]string, error) {
	rl.editMu.Lock()
	defer rl.editMu.Unlock()
	set, sum, err := readRules(rl.path)
	if err != nil {
		return nil, err // 파일이 없는 것도 오류 (실수로 지워서 모두 기본으로 가는 것을 막음)
	}
	if err := rl.ensureAll(set.allTopics()); err != nil {
		return nil, err
	}
	rl.mu.Lock()
	changes := diff(rl.set, set)
	rl.set, rl.sum, rl.loaded = set, sum, time.Now()
	rl.mu.Unlock()
	return changes, nil
}

// reloadLog : reload + 기록 (SIGHUP / API 공통)
func (rl *rules) reloadLog(who string) ([]string, error) {
	changes, err := rl.reload()
	if err != nil {
		log.Printf("rules reload (%s): %v (previous rules kept)", who, err)
		return nil, err
	}
	log.Printf("rules reload (%s): %d changed", who, len(changes))
	for _, c := range changes {
		log.Printf("rules   %s", c)
	}
	return changes, nil
}
