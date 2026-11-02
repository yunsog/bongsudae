// 웹 API : Kafka 전달 규칙 (rules.go). 조회는 누구나, 바꾸기는 key (X-API-Key = -token 파일 내용) 가 맞아야 함
//
//	GET    /api/kafka/rules                                 규칙 전체 + 토픽별 지난 1 분 수
//	GET    /api/kafka/rules/default                         rule / effective / from
//	GET    /api/kafka/rules/routers/{ip}                    (+ 그 라우터의 peer 규칙)
//	GET    /api/kafka/rules/routers/{ip}/peers/{peer}
//	PUT    (위 셋)                                          {"topics": [...]}   ([] = 보내지 않음)
//	DELETE /api/kafka/rules/routers/{ip}[/peers/{peer}]     규칙을 지움 -> 한 단계 위를 따름
//	POST   /api/kafka/rules/reload                          파일을 다시 읽음
//
// 바꾸는 요청의 머리 X-Actor (부른 쪽의 사용자 이름, 선택) 는 기록 (log) 에 actor=<값> 으로 남음
package main

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

func (c *collector) addRuleAPI(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/kafka/rules", c.apiRules)
	mux.HandleFunc("GET /api/kafka/rules/default", c.apiRuleGet)
	mux.HandleFunc("GET /api/kafka/rules/routers/{ip}", c.apiRuleGet)
	mux.HandleFunc("GET /api/kafka/rules/routers/{ip}/peers/{peer}", c.apiRuleGet)
	mux.HandleFunc("PUT /api/kafka/rules/default", c.auth(c.apiRulePut))
	mux.HandleFunc("PUT /api/kafka/rules/routers/{ip}", c.auth(c.apiRulePut))
	mux.HandleFunc("PUT /api/kafka/rules/routers/{ip}/peers/{peer}", c.auth(c.apiRulePut))
	mux.HandleFunc("DELETE /api/kafka/rules/routers/{ip}", c.auth(c.apiRuleDel))
	mux.HandleFunc("DELETE /api/kafka/rules/routers/{ip}/peers/{peer}", c.auth(c.apiRuleDel))
	mux.HandleFunc("POST /api/kafka/rules/reload", c.auth(c.apiRuleReload))
}

// ---- JSON 모양 ----

// ruleJSON : 규칙 하나 (rule = 적힌 값, 없으면 null / effective = 실제 적용 / from = peer | router | default)
type ruleJSON struct {
	Router    string              `json:"router,omitempty"`
	Peer      string              `json:"peer,omitempty"`
	Rule      *[]string           `json:"rule"`
	Effective []string            `json:"effective"`
	From      string              `json:"from"`
	Peers     map[string][]string `json:"peers,omitempty"` // 라우터 조회 때 그 peer 규칙들
}

type topicJSON struct {
	Topic   string `json:"topic"`
	Msgs1m  int    `json:"msgs_1m"`
	Rows1m  int    `json:"rows_1m"`
	InRules bool   `json:"in_rules"`
}

type rulesJSON struct {
	File   string      `json:"file"`
	Loaded string      `json:"loaded"`
	Prefix string      `json:"prefix"` // 실제 토픽 = prefix + "." + 이름
	Rules  *ruleSet    `json:"rules"`
	Topics []topicJSON `json:"topics"`
}

type reloadJSON struct {
	Changes []string `json:"changes"`
}

// ---- key 검사 ----

// auth : X-API-Key 가 -token 파일 내용과 같아야 함 (파일은 요청마다 읽음 : 바꾸면 바로 적용)
func (c *collector) auth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		b, err := os.ReadFile(c.tokenPath)
		tok := strings.TrimSpace(string(b))
		if err != nil || len(tok) < 16 {
			httpErr(w, 403, "changes disabled : key file %s missing or shorter than 16 characters", c.tokenPath)
			return
		}
		if !validActor(req.Header.Get("X-Actor")) {
			httpErr(w, 400, "X-Actor : 1 to 64 printable ASCII characters, no spaces")
			return
		}
		got := strings.TrimSpace(req.Header.Get("X-API-Key"))
		if subtle.ConstantTimeCompare([]byte(got), []byte(tok)) != 1 {
			log.Printf("rules: wrong key %s %s from %s%s", req.Method, req.URL.Path, req.RemoteAddr, actor(req))
			httpErr(w, 403, "wrong key")
			return
		}
		h(w, req)
	}
}

// validActor : X-Actor (부른 쪽의 사용자 이름). 없으면 통과. 있으면 0x21 ~ 0x7e, 64 글자까지
func validActor(s string) bool {
	if len(s) > 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x21 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

// actor : 기록에 붙일 " actor=<값>" (머리가 없으면 빈칸)
func actor(req *http.Request) string {
	if a := req.Header.Get("X-Actor"); a != "" {
		return " actor=" + a
	}
	return ""
}

// ---- 처리기 ----

func (c *collector) apiRules(w http.ResponseWriter, req *http.Request) {
	rl := c.rules
	rl.mu.RLock()
	j := rulesJSON{File: rl.path, Loaded: tstr(rl.loaded), Prefix: c.prefix, Rules: rl.set.clone()}
	used := rl.set.allTopics()
	rl.mu.RUnlock()
	last := rl.lastMinute()
	for _, t := range used {
		n := last[t]
		j.Topics = append(j.Topics, topicJSON{Topic: t, Msgs1m: n.Msgs, Rows1m: n.Rows, InRules: true})
		delete(last, t)
	}
	for t, n := range last { // 규칙에서 빠졌지만 지난 1 분에 보낸 것
		j.Topics = append(j.Topics, topicJSON{Topic: t, Msgs1m: n.Msgs, Rows1m: n.Rows})
	}
	writeJSON(w, j)
}

// ruleTarget : 경로의 {ip} {peer} 를 정규 주소로 ("" 면 그 단계 아님)
func ruleTarget(req *http.Request) (router, peer string, err error) {
	if s := req.PathValue("ip"); s != "" {
		if router, err = normIP(s); err != nil {
			return "", "", errors.New("router " + s + " : not an address")
		}
	}
	if s := req.PathValue("peer"); s != "" {
		if peer, err = normIP(s); err != nil {
			return "", "", errors.New("peer " + s + " : not an address")
		}
	}
	return
}

// ruleView : 규칙 하나 보기 (rl.mu 읽기 잠금 안에서)
func ruleView(s *ruleSet, router, peer string) ruleJSON {
	j := ruleJSON{Router: router, Peer: peer}
	rr := s.Routers[router]
	switch {
	case router == "":
		t := s.Default
		j.Rule = &t
	case peer == "":
		if rr != nil {
			j.Rule = rr.Topics
			j.Peers = rr.Peers
		}
	default:
		if rr != nil {
			if t, ok := rr.Peers[peer]; ok {
				j.Rule = &t
			}
		}
	}
	if router == "" {
		j.Effective, j.From = s.Default, "default"
	} else {
		j.Effective, j.From = s.lookup(router, peer)
	}
	return j
}

func (c *collector) apiRuleGet(w http.ResponseWriter, req *http.Request) {
	router, peer, err := ruleTarget(req)
	if err != nil {
		httpErr(w, 400, "%v", err)
		return
	}
	c.rules.mu.RLock()
	j := ruleView(c.rules.set, router, peer)
	c.rules.mu.RUnlock()
	writeJSON(w, j)
}

func (c *collector) apiRulePut(w http.ResponseWriter, req *http.Request) {
	router, peer, err := ruleTarget(req)
	if err != nil {
		httpErr(w, 400, "%v", err)
		return
	}
	var body struct {
		Topics *[]string `json:"topics"`
	}
	d := json.NewDecoder(io.LimitReader(req.Body, 64<<10))
	d.DisallowUnknownFields()
	if err := d.Decode(&body); err != nil {
		httpErr(w, 400, "body : %v (expected {\"topics\": [...]})", err)
		return
	}
	if body.Topics == nil {
		httpErr(w, 400, `"topics" missing (use "topics": [] to send nothing)`)
		return
	}
	topics, err := checkTopics(*body.Topics)
	if err != nil {
		httpErr(w, 400, "%v", err)
		return
	}
	if err := c.rules.ensureAll(topics); err != nil {
		httpErr(w, 500, "create topic : %v", err)
		return
	}
	err = c.rules.change(func(s *ruleSet) error {
		if router == "" {
			s.Default = topics
			return nil
		}
		rr := s.Routers[router]
		if rr == nil {
			rr = &routerRule{Peers: map[string][]string{}}
			s.Routers[router] = rr
		}
		if peer == "" {
			rr.Topics = &topics
		} else {
			rr.Peers[peer] = topics
		}
		return nil
	})
	c.ruleDone(w, req, router, peer, "PUT "+topicsText(topics), err)
}

func (c *collector) apiRuleDel(w http.ResponseWriter, req *http.Request) {
	router, peer, err := ruleTarget(req)
	if err != nil {
		httpErr(w, 400, "%v", err)
		return
	}
	errNone := errors.New("no such rule (already follows the level above)")
	err = c.rules.change(func(s *ruleSet) error {
		rr := s.Routers[router]
		if peer == "" {
			if rr == nil || rr.Topics == nil {
				return errNone
			}
			rr.Topics = nil // peer 규칙은 남김 (빈 항목은 check 가 지움)
			return nil
		}
		if rr == nil {
			return errNone
		}
		if _, ok := rr.Peers[peer]; !ok {
			return errNone
		}
		delete(rr.Peers, peer)
		return nil
	})
	if err == errNone {
		httpErr(w, 404, "%v", err)
		return
	}
	c.ruleDone(w, req, router, peer, "DELETE", err)
}

// ruleDone : 바꾼 뒤 공통 (오류 응답 또는 기록 + 바뀐 뒤 모습)
func (c *collector) ruleDone(w http.ResponseWriter, req *http.Request, router, peer, what string, err error) {
	if err == errFileChanged {
		httpErr(w, 409, "%v", err)
		return
	}
	if err != nil {
		httpErr(w, 500, "%v", err)
		return
	}
	target := "default"
	if router != "" {
		target = "router " + router
	}
	if peer != "" {
		target += " peer " + peer
	}
	log.Printf("rules: %s %s (from %s%s)", target, what, req.RemoteAddr, actor(req))
	c.rules.mu.RLock()
	j := ruleView(c.rules.set, router, peer)
	c.rules.mu.RUnlock()
	writeJSON(w, j)
}

func (c *collector) apiRuleReload(w http.ResponseWriter, req *http.Request) {
	changes, err := c.rules.reloadLog("api " + req.RemoteAddr + actor(req))
	if err != nil {
		httpErr(w, 400, "%v (previous rules kept)", err)
		return
	}
	if changes == nil {
		changes = []string{}
	}
	writeJSON(w, reloadJSON{Changes: changes})
}
