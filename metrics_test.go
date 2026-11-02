package main

import (
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

// /metrics : 줄마다 Prometheus 글자 꼴, metric 마다 HELP / TYPE 한 번, 같은 이름의 줄은 붙어 있음
func TestMetrics(t *testing.T) {
	c := newCollector("x")
	r := c.routerFor("192.0.2.100", "")
	r.sysName = `name "with" \ quote`
	c.safeProcess(r, mustHex(t, "03000000440300000000000000000000000000000000000000000000c63364670000fbf0c63364676ae222c300051812000000000000000000000000c0000264e2e800b3"), testNow)
	w := httptest.NewRecorder()
	c.apiMetrics(w, httptest.NewRequest("GET", "/metrics", nil))
	body := w.Body.String()
	sample := regexp.MustCompile(`^[a-z_]+(\{[a-z_]+="([^"\\]|\\.)*"(,[a-z_]+="([^"\\]|\\.)*")*\})? -?[0-9.e+]+$`)
	seen := map[string]bool{}
	cur := ""
	for _, l := range strings.Split(strings.TrimSuffix(body, "\n"), "\n") {
		switch {
		case strings.HasPrefix(l, "# HELP "):
		case strings.HasPrefix(l, "# TYPE "):
			cur = strings.Fields(l)[2]
			if seen[cur] {
				t.Fatalf("TYPE 두 번 : %s", cur)
			}
			seen[cur] = true
		default:
			if !sample.MatchString(l) {
				t.Fatalf("틀린 줄 %q", l)
			}
			if name := strings.FieldsFunc(l, func(r rune) bool { return r == '{' || r == ' ' })[0]; name != cur {
				t.Fatalf("%s 줄이 %s 묶음 밖", name, cur)
			}
		}
	}
	for _, want := range []string{
		`bongsudae_router_up{router="192.0.2.100",name="name \"with\" \\ quote"} 1`,
		`bongsudae_router_bmp_messages_total{router="192.0.2.100",type="peer_up"} 1`,
		`bongsudae_router_peer_up_no_open_total{router="192.0.2.100"} 1`,
		`bongsudae_peers{router="192.0.2.100",state="up"} 1`,
	} {
		if !strings.Contains(body, want+"\n") {
			t.Errorf("없음 : %s", want)
		}
	}
}
