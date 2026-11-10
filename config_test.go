package main

import (
	"flag"
	"log"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"
)

// 설정 파일 읽기 : 구역, 주석, 쉼표로 이어지는 목록, 공백 정리
func TestParseConf(t *testing.T) {
	in := `
# 머리 주석
[bmp]
listen = :5000          # 줄 끝 주석
allow  = 203.0.113.0/24,
         10.0.0.1 ,     # 이어지는 줄에도 주석

         192.0.2.0/24
raw    = false
[http]
allow  =
`
	got, err := parseConf(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"bmp.listen": ":5000",
		"bmp.allow":  "203.0.113.0/24,10.0.0.1,192.0.2.0/24",
		"bmp.raw":    "false",
		"http.allow": "",
	}
	if len(got) != len(want) {
		t.Fatalf("항목 %d 개 : %v", len(got), got)
	}
	for _, e := range got {
		if want[e.Key] != e.Val {
			t.Errorf("%s = %q, 기대 %q", e.Key, e.Val, want[e.Key])
		}
	}
}

// 틀린 파일은 오류 (시작하지 않음)
func TestParseConfErrors(t *testing.T) {
	bad := map[string]string{
		"모르는 키":     "[bmp]\nlisten2 = :5000\n",
		"구역 밖 키":    "listen = :5000\n",
		"같은 키 두 번":  "[bmp]\nraw = true\nraw = false\n",
		"= 없음":      "[bmp]\nlisten :5000\n",
		"구역 괄호":     "[bmp\nraw = true\n",
		"쉼표 뒤 끝":    "[bmp]\nallow = 10.0.0.0/8,\n",
		"쉼표 뒤 다른 키": "[bmp]\nallow = 10.0.0.0/8,\nraw = true\n",
	}
	for name, in := range bad {
		if _, err := parseConf(strings.NewReader(in)); err == nil {
			t.Errorf("%s : 오류가 나야 함", name)
		}
	}
}

// 허용 대역 : 주소 하나는 /32, /128. 틀린 값 / 빈칸은 오류 (bmp.allow), http.allow 의 빈칸 = 모두
func TestParseAllow(t *testing.T) {
	l, err := parseAllow("203.0.113.0/24, 10.0.0.1, 2001:db8::1, 203.0.113.9/24")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"203.0.113.0/24", "10.0.0.1/32", "2001:db8::1/128", "203.0.113.0/24"}
	for i, p := range l {
		if p.String() != want[i] {
			t.Errorf("%d : %s, 기대 %s", i, p, want[i])
		}
	}
	for _, s := range []string{"", "203.0.113.0/33", "abc"} {
		if _, err := parseAllow(s); err == nil {
			t.Errorf("parseAllow(%q) : 오류가 나야 함", s)
		}
	}
	if l, err := parseHTTPAllow(" "); err != nil || l != nil {
		t.Errorf("parseHTTPAllow 빈칸 = 모두 : %v %v", l, err)
	}
	ha, _ := parseHTTPAllow("127.0.0.1, 198.51.100.0/24")
	httpAllow.Store(&ha)
	defer httpAllow.Store(nil)
	for addr, ok := range map[string]bool{"127.0.0.1:4000": true, "198.51.100.9:1": true, "[::ffff:198.51.100.9]:1": true, "203.0.113.1:1": false} {
		if httpAllowed(addr) != ok {
			t.Errorf("httpAllowed(%s) != %v", addr, ok)
		}
	}
}

// 모든 설정 키는 있는 옵션을 가리킴
func TestConfKeys(t *testing.T) {
	for k, name := range confKeys {
		if flag.Lookup(name) == nil {
			t.Errorf("%s -> -%s : 그런 옵션 없음", k, name)
		}
	}
}

// 0.3.0 : 없앤 [export] 구역은 이제 모르는 키 (받고 무시하던 표를 지움)
func TestConfRetiredExport(t *testing.T) {
	if _, err := parseConf(strings.NewReader("[export]\norigins = true\n")); err == nil || !strings.Contains(err.Error(), "unknown key [export] origins") {
		t.Fatalf("%v", err)
	}
}

// reload 비교 : 시간 값은 길이로 ("90m" = "1h30m0s"), 그 밖은 글자로
func TestConfSameValue(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.Duration("d", 90*time.Minute, "")
	fs.String("s", "a", "")
	d, sv := fs.Lookup("d"), fs.Lookup("s")
	for _, c := range []struct {
		fl   *flag.Flag
		v    string
		want bool
	}{{d, "90m", true}, {d, "1h30m0s", true}, {d, "60m", false}, {d, "x", false}, {sv, "a", true}, {sv, "b", false}} {
		if got := sameValue(c.fl, c.v); got != c.want {
			t.Errorf("%s %q : %v", c.fl.Name, c.v, got)
		}
	}
}

// 허용 대역 밖 연결 기록 : 주소마다 1 분에 한 줄, 그 사이 끊은 수는 다음 줄에
func TestDenyLog(t *testing.T) {
	var buf strings.Builder
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)
	d := denyLog{last: map[netip.Addr]time.Time{}, more: map[netip.Addr]int{}}
	a, b := netip.MustParseAddr("192.0.2.9"), netip.MustParseAddr("198.51.100.7")
	t0 := time.Unix(1_000_000, 0)
	for i := range 5 {
		d.note(a, t0.Add(time.Duration(i)*time.Second), "not in bmp.allow") // 1 줄 + 4 개 셈
	}
	d.note(b, t0, "not in bmp.allow")                     // 다른 주소 1 줄
	d.note(a, t0.Add(61*time.Second), "not in bmp.allow") // 1 분 뒤 1 줄 (4 more)
	out := buf.String()
	if strings.Count(out, "refused : not in bmp.allow") != 3 || !strings.Contains(out, "conn 192.0.2.9 refused : not in bmp.allow (4 more since last line)") {
		t.Fatal(out)
	}
}
