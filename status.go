// 웹 API 를 부르는 명령 : 인자 = API 경로 (/api/ 뒤), 이름=값 = 질의, 옵션 = HTTP 방법 (1 대 1)
//
//	bongsudae -status  a b c k=v           GET    /api/a/b/c?k=v   (인자 없으면 routers)
//	bongsudae -set -topics x,y  a b c      PUT    /api/a/b/c  {"topics": ["x","y"]}  (none = [])
//	bongsudae -del     a b c               DELETE /api/a/b/c
//	bongsudae -post    a b c               POST   /api/a/b/c
//	bongsudae -reload                      POST   /api/kafka/rules/reload  (= -post kafka rules reload)
//
// 예 :
//
//	-status routers                                     라우터 표
//	-status routers 192.0.2.100                        라우터 하나 + peer 표
//	-status peers state=down                            (router= as= warn=1)
//	-status routers 192.0.2.100 peers 198.51.100.33   peer 상세
//	-status routers 192.0.2.100 prefixes distinct=1    경로 (탭 구분 글자)
//	-status prefix p=1.11.56.0/21 match=more            모든 라우터에서 프리픽스
//	-status prefix p=192.0.2.0/24 match=more not_origin=64500 limit=50   하이재킹 의심 줄
//	-status kafka rules                                 규칙 표
//	-set -topics none kafka rules routers 192.0.2.31   이 라우터는 보내지 않음
//	-post routers 192.0.2.20 reset                     BMP 세션 끊기 (tcp 입력만)
//
// 바꾸는 요청 (-set -del -post -reload) 은 key (-token 파일) 와 X-Actor = cli:<사용자> 를 보냄
//
// 옵션 (-api, -json, -topics, -token) 은 경로 앞에 (Go 의 옵션은 인자 앞에서만 읽음)
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"
)

// runClient : 옵션에 따라 GET / PUT / DELETE / POST 하나
func runClient(api string, args []string) error {
	api = strings.TrimSuffix(api, "/")
	var segs []string
	q := url.Values{}
	for _, a := range args {
		if strings.HasPrefix(a, "-") {
			return fmt.Errorf("옵션 %s 은 경로 앞에 (예 : -status -json routers)", a)
		}
		if k, v, ok := strings.Cut(a, "="); ok {
			q.Add(k, v)
		} else {
			segs = append(segs, a)
		}
	}
	switch {
	case *optReload:
		return callRule("POST", api+"/api/kafka/rules/reload", nil)
	case *optPost:
		return callRule("POST", apiURL(api, segs, q), nil)
	case *optSet:
		topicsSet := false
		flag.Visit(func(f *flag.Flag) { topicsSet = topicsSet || f.Name == "topics" })
		if !topicsSet {
			return errors.New("-set 은 -topics 필요 (보내지 않으려면 -topics none)")
		}
		topics := []string{}
		if *optTopics != "none" {
			for _, t := range strings.Split(*optTopics, ",") {
				if t = strings.TrimSpace(t); t != "" {
					topics = append(topics, t)
				}
			}
		}
		body, _ := json.Marshal(map[string][]string{"topics": topics})
		return callRule("PUT", apiURL(api, segs, q), body)
	case *optDel:
		return callRule("DELETE", apiURL(api, segs, q), nil)
	}
	return get(api, segs, q)
}

// apiURL : 경로 조각과 질의로 주소 (조각은 하나씩 escape)
func apiURL(api string, segs []string, q url.Values) string {
	var b strings.Builder
	b.WriteString(api + "/api")
	for _, s := range segs {
		b.WriteString("/" + url.PathEscape(s))
	}
	if len(q) > 0 {
		b.WriteString("?" + q.Encode())
	}
	return b.String()
}

// is : 경로 모양 비교 ("*" = 아무 값)
func is(segs []string, pat ...string) bool {
	if len(segs) != len(pat) {
		return false
	}
	for i, p := range pat {
		if p != "*" && p != segs[i] {
			return false
		}
	}
	return true
}

// get : GET 하나. 아는 경로는 표로, 모르는 경로 / -json 은 받은 그대로
func get(api string, segs []string, q url.Values) error {
	if len(segs) == 0 {
		segs = []string{"routers"}
	}
	last := segs[len(segs)-1]
	if last == "prefixes" && q.Get("format") == "" && !*optJSON {
		q.Set("format", "text") // 경로 목록은 크므로 탭 구분 글자
	}
	resp, err := httpDo("GET", apiURL(api, segs, q), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if *optJSON || last == "prefixes" {
		_, err = io.Copy(os.Stdout, resp.Body)
		return err
	}
	d := json.NewDecoder(resp.Body)
	switch {
	case is(segs, "routers"):
		var rs []routerJSON
		if err := d.Decode(&rs); err != nil {
			return err
		}
		printRouters(rs)
	case is(segs, "routers", "*"):
		var r routerJSON
		if err := d.Decode(&r); err != nil {
			return err
		}
		printRouters([]routerJSON{r})
		fmt.Println()
		printPeers(r.Peers)
	case is(segs, "peers"):
		var ps []peerJSON
		if err := d.Decode(&ps); err != nil {
			return err
		}
		printPeers(ps)
	case is(segs, "routers", "*", "peers", "*"):
		var p peerJSON
		if err := d.Decode(&p); err != nil {
			return err
		}
		printPeer(&p)
	case is(segs, "prefix"), is(segs, "routers", "*", "prefix"):
		var res findJSON
		if err := d.Decode(&res); err != nil {
			return err
		}
		filter := ""
		if len(res.NotOrigin) > 0 {
			filter = fmt.Sprintf("  not_origin=%v", res.NotOrigin)
		} else if len(res.OriginAS) > 0 {
			filter = fmt.Sprintf("  origin_as=%v", res.OriginAS)
		}
		fmt.Printf("%s  match=%s%s  %d / %d 줄\n\n", res.Query, res.Match, filter, res.Count, res.Total)
		printRoutes(res.Routes)
	case is(segs, "kafka", "rules"):
		var j rulesJSON
		if err := d.Decode(&j); err != nil {
			return err
		}
		printRules(&j)
	case len(segs) >= 3 && segs[0] == "kafka" && segs[1] == "rules":
		var j ruleJSON
		if err := d.Decode(&j); err != nil {
			return err
		}
		printRule(&j)
	default:
		_, err = io.Copy(os.Stdout, d.Buffered())
		if err == nil {
			_, err = io.Copy(os.Stdout, resp.Body)
		}
		return err
	}
	return nil
}

// callRule : 규칙 바꾸기 / 다시 읽기 (key 를 머리에 넣음)
func callRule(method, u string, body []byte) error {
	b, err := os.ReadFile(*optToken)
	if err != nil {
		return fmt.Errorf("key 파일 : %v (-token 으로 지정)", err)
	}
	who := os.Getenv("USER")
	if who == "" {
		who = "unknown"
	}
	resp, err := httpDo(method, u, body, "X-API-Key", strings.TrimSpace(string(b)), "X-Actor", "cli:"+who)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if *optJSON {
		_, err = io.Copy(os.Stdout, resp.Body)
		return err
	}
	if strings.HasSuffix(u, "/reset") {
		var j resetJSON
		if err := json.NewDecoder(resp.Body).Decode(&j); err != nil {
			return err
		}
		fmt.Printf("router %s : 끊음 (%s)\n  %s\n  %s\n", j.Router, short(j.At), j.Why, j.Note)
		return nil
	}
	if strings.HasSuffix(u, "/reload") {
		var j reloadJSON
		if err := json.NewDecoder(resp.Body).Decode(&j); err != nil {
			return err
		}
		fmt.Printf("다시 읽음 : %d 바뀜\n", len(j.Changes))
		for _, c := range j.Changes {
			fmt.Println("  " + c)
		}
		return nil
	}
	var j ruleJSON
	if err := json.NewDecoder(resp.Body).Decode(&j); err != nil {
		return err
	}
	printRule(&j)
	return nil
}

// httpDo : 요청 하나. 200 이 아니면 오류 (본문 = 서버가 준 이유). hdr = 이름, 값, ...
func httpDo(method, u string, body []byte, hdr ...string) (*http.Response, error) {
	req, err := http.NewRequest(method, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%v (bongsudae 가 -http 로 돌고 있는지 확인)", err)
	}
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("%s %s : %s", method, resp.Status, strings.TrimSpace(string(b)))
	}
	return resp, nil
}

// short : RFC3339 시각 -> "10-29 02:10"
func short(s string) string {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return s
	}
	return t.Local().Format("01-02 15:04")
}

func ago(s string) string {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return ""
	}
	return time.Since(t).Round(time.Second).String()
}

func printRouters(rs []routerJSON) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ROUTER\tNAME\tSTATE\tSINCE\tLAST MSG\tPEERS up/down\tSYNCED\tROUTES v4 / v6\tERR\tKAFKA\tWARN")
	up, peersUp, peersDown := 0, 0, 0
	for _, r := range rs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%ds\t%d / %d\t%d\t%d / %d\t%d\t%s\t%s\n", r.IP, r.Name, r.State, short(r.Since),
			r.LastMsgAgo, r.PeersUp, r.PeersDown, r.Synced, r.RoutesV4, r.RoutesV6, r.Errors, kafkaText(r.Kafka, r.KafkaFrom),
			strings.Join(r.Warn, ", "))
		if r.State == "up" {
			up++
		}
		peersUp += r.PeersUp
		peersDown += r.PeersDown
	}
	tw.Flush()
	if len(rs) > 1 {
		fmt.Printf("\n합계 : 라우터 %d / %d up, peer %d up · %d down\n", up, len(rs), peersUp, peersDown)
	}
}

func printPeers(ps []peerJSON) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ROUTER\tPEER\tAS\tBGP ID\tTYPE\tSTATE\tSINCE\tSYNCED\tROUTES v4 / v6\tLAST UPDATE\tADD / DEL\tKAFKA\tWARN")
	for _, p := range ps {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%d / %d\t%s\t%d / %d\t%s\t%s\n", p.Router, p.Peer, p.AS, p.BGPID, p.Type,
			p.State, short(p.Since), yesNo(p.Synced), p.RoutesV4, p.RoutesV6, ago(p.LastUpdate), p.Adds, p.Dels, kafkaText(p.Kafka, p.KafkaFrom),
			strings.Join(p.Warn, ", "))
	}
	tw.Flush()
}

func printPeer(p *peerJSON) {
	fmt.Printf("peer %s  AS %d  BGP ID %s   router %s (%s)\n", p.Peer, p.AS, p.BGPID, p.Router, p.RouterName)
	d := p.Detail
	fmt.Printf("  type      %s, rd %s, pre-policy %v, adj-rib-in %v, 4-byte AS %v\n", p.Type, p.RD, d.PrePolicy, d.AdjRibIn, d.AS4)
	fmt.Printf("  state     %s since %s (Peer Up %v)", p.State, p.Since, p.PeerUp)
	if p.DownReason != "" {
		fmt.Printf("  %s", p.DownReason)
	}
	fmt.Println()
	fmt.Printf("  session   local %s:%d  remote port %d  hold sent %d / recv %d\n", d.LocalIP, d.LocalPort, d.RemotePort, d.HoldSent, d.HoldRecv)
	fmt.Printf("  sent cap  %s\n  recv cap  %s\n", d.SentCaps, d.RecvCaps)
	fmt.Printf("  routes    v4 %d / v6 %d  (synced %v, End-of-RIB v4 %s v6 %s)\n", p.RoutesV4, p.RoutesV6, p.Synced,
		short(p.EoRV4), short(p.EoRV6))
	fmt.Printf("  updates   last %s ago, add %d / del %d (이번 up 이후)\n", ago(p.LastUpdate), p.Adds, p.Dels)
	fmt.Printf("  kafka     %s (from %s)\n", topicsText(p.Kafka), p.KafkaFrom)
	if len(p.Warn) > 0 {
		fmt.Printf("  warn      %s\n", strings.Join(p.Warn, ", "))
	}
	fmt.Println("  history")
	for _, h := range d.History {
		fmt.Println("    " + h)
	}
}

func printRoutes(rs []routeJSON) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ROUTER\tNAME\tPEER\tPEER AS\tPREFIX\tAS PATH\tNEXTHOP\tCOMMUNITIES\tSINCE")
	for _, r := range rs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\n", r.Router, r.RouterName, r.Peer, r.PeerAS, r.Prefix,
			r.ASPath, r.NextHop, r.Communities, short(r.Since))
	}
	tw.Flush()
}

// kafkaText : 표의 KAFKA 칸. default 에서 온 것은 이름만, 아니면 출처를 붙임
func kafkaText(t []string, from string) string {
	if from == "default" || from == "" {
		return topicsText(t)
	}
	return topicsText(t) + " (" + from + ")"
}

// printRules : 규칙 전체 표 + 토픽별 지난 1 분
func printRules(j *rulesJSON) {
	fmt.Printf("file %s (읽음 / 씀 %s), 토픽 = %s.<이름>\n\n", j.File, short(j.Loaded), j.Prefix)
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SCOPE\tROUTER\tPEER\tTOPICS")
	fmt.Fprintf(tw, "default\t\t\t%s\n", topicsText(j.Rules.Default))
	for _, ip := range sortedKeys(j.Rules.Routers) {
		rr := j.Rules.Routers[ip]
		if rr.Topics != nil {
			fmt.Fprintf(tw, "router\t%s\t\t%s\n", ip, topicsText(*rr.Topics))
		} else {
			fmt.Fprintf(tw, "router\t%s\t\t(default 를 따름)\n", ip)
		}
		for _, p := range sortedKeys(rr.Peers) {
			fmt.Fprintf(tw, "peer\t%s\t%s\t%s\n", ip, p, topicsText(rr.Peers[p]))
		}
	}
	tw.Flush()
	fmt.Println()
	tw = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "TOPIC\tMSGS 1m\tROWS 1m\t")
	for _, t := range j.Topics {
		note := ""
		if !t.InRules {
			note = "(규칙에 없음)"
		}
		fmt.Fprintf(tw, "%s.%s\t%d\t%d\t%s\n", j.Prefix, t.Topic, t.Msgs1m, t.Rows1m, note)
	}
	tw.Flush()
}

// printRule : 규칙 하나 (적힌 값 / 실제 적용 / 출처)
func printRule(j *ruleJSON) {
	target := "default"
	if j.Router != "" {
		target = "router " + j.Router
	}
	if j.Peer != "" {
		target += " peer " + j.Peer
	}
	rule := "(없음 : 위 단계를 따름)"
	if j.Rule != nil {
		rule = topicsText(*j.Rule)
	}
	fmt.Printf("%s\n  rule       %s\n  effective  %s (from %s)\n", target, rule, topicsText(j.Effective), j.From)
	for _, p := range sortedKeys(j.Peers) {
		fmt.Printf("  peer %s  %s\n", p, topicsText(j.Peers[p]))
	}
}

// sortedKeys : 주소 글자 열쇠를 주소 순으로
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, cmpAddr)
	return keys
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "-"
}
