package main

import (
	"flag"
	"strings"
	"testing"
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
[export]                # 없앤 구역 : 받고 무시
origins = true
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
