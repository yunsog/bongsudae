package main

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestRules : 단계 (peer -> router -> default), [] 와 지움의 차이, 파일 쓰기 / 손으로 고침 / reload
func TestRules(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conf", "kafka_rules.json")
	rl, err := newRules(path) // 파일 없음 -> 기본
	if err != nil {
		t.Fatal(err)
	}
	want := func(router, peer, from string, topics ...string) {
		t.Helper()
		got, f := rl.effective(router, peer)
		if f != from || !slices.Equal(got, topics) {
			t.Errorf("%s %s : %v (%s), want %v (%s)", router, peer, got, f, topics, from)
		}
	}
	want("192.0.2.95", "2001:db8::20:63", "default", "unicast_prefix")

	set := func(router, peer string, topics ...string) {
		t.Helper()
		ts, _ := checkTopics(topics)
		err := rl.change(func(s *ruleSet) error {
			if router == "" {
				s.Default = ts
				return nil
			}
			rr := s.Routers[router]
			if rr == nil {
				rr = &routerRule{Peers: map[string][]string{}}
				s.Routers[router] = rr
			}
			if peer == "" {
				rr.Topics = &ts
			} else {
				rr.Peers[peer] = ts
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	set("192.0.2.95", "", "unicast_prefix1", "unicast_prefix2")
	set("192.0.2.95", "2001:db8::20:63", "unicast_prefix2")
	set("192.0.2.31", "")
	want("192.0.2.95", "2001:db8::20:63", "peer", "unicast_prefix2")
	want("192.0.2.95", "10.0.0.1", "router", "unicast_prefix1", "unicast_prefix2")
	want("192.0.2.31", "10.0.0.1", "router")
	want("192.0.2.48", "10.0.0.1", "default", "unicast_prefix")

	// peer 를 [] 로 = 보내지 않음, 지움 = 라우터를 따름
	set("192.0.2.95", "2001:db8::20:63")
	want("192.0.2.95", "2001:db8::20:63", "peer")
	rl.change(func(s *ruleSet) error { delete(s.Routers["192.0.2.95"].Peers, "2001:db8::20:63"); return nil })
	want("192.0.2.95", "2001:db8::20:63", "router", "unicast_prefix1", "unicast_prefix2")

	// 라우터 규칙을 지워도 peer 규칙은 남음
	set("192.0.2.95", "10.0.0.1", "unicast_prefix3")
	rl.change(func(s *ruleSet) error { s.Routers["192.0.2.95"].Topics = nil; return nil })
	want("192.0.2.95", "10.0.0.1", "peer", "unicast_prefix3")
	want("192.0.2.95", "10.0.0.2", "default", "unicast_prefix")

	// 파일을 다시 읽으면 같은 규칙 (쓴 것 = 메모리)
	b, _ := os.ReadFile(path)
	t.Logf("file:\n%s", b)
	if ch, err := rl.reload(); err != nil || len(ch) != 0 {
		t.Fatalf("reload same file: %v %v", ch, err)
	}

	// 손으로 고침 -> API 변경은 거절 (409), reload 하면 적용
	hand := `{"default": [], "routers": {"192.0.2.095": {"topics": ["unicast_prefix9"]}}}`
	if err := rl.change(func(s *ruleSet) error { return nil }); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(path, []byte(hand), 0644)
	if err := rl.change(func(s *ruleSet) error { return nil }); err != errFileChanged {
		t.Fatalf("hand edit not detected: %v", err)
	}
	ch, err := rl.reload()
	if err == nil {
		t.Fatalf("bad address 192.0.2.095 accepted: %v", ch)
	}
	want("192.0.2.95", "10.0.0.1", "peer", "unicast_prefix3") // 오류 -> 이전 규칙 그대로
	os.WriteFile(path, []byte(`{"default": [], "routers": {"192.0.2.95": {"topics": ["unicast_prefix9"]}}}`), 0644)
	ch, err = rl.reload()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("changes: %q", ch)
	want("192.0.2.95", "10.0.0.1", "router", "unicast_prefix9")
	want("192.0.2.31", "10.0.0.1", "default")
	if err := rl.change(func(s *ruleSet) error { return nil }); err != nil {
		t.Fatalf("after reload change should work: %v", err)
	}

	// 잘못된 파일들
	for _, bad := range []string{
		`{"routers": {}}`,               // default 없음
		`{"default": ["prefix1"]}`,      // unicast_prefix 로 시작하지 않음
		`{"default": [], "router": {}}`, // 키 글자 틀림
		`{"default": [], "routers": {"1.1.1.1": {"peers": {"2.2.2.2": null}}}}`, // null
		"{\"default\": [],\n \"routers\": {,}}",                                 // 문법 (줄 번호)
	} {
		os.WriteFile(path, []byte(bad), 0644)
		_, err := rl.reload()
		if err == nil {
			t.Errorf("accepted: %s", bad)
		}
		t.Logf("%v", err)
	}
}

// TestEoR : End-of-RIB (IPv4 빈 UPDATE, IPv6 빈 MP_UNREACH) 와 synced (협상된 종류마다 EoR)
func TestEoR(t *testing.T) {
	u, err := parseUpdate([]byte{0, 0, 0, 0}, pathOpts{})
	if err != nil || !u.eorV4 || u.eorV6 {
		t.Fatalf("v4 EoR %+v %v", u, err)
	}
	// 철회 0, 속성 길이 6 : MP_UNREACH (flags 0x80, type 15, len 3) afi 2 safi 1
	u, err = parseUpdate([]byte{0, 0, 0, 6, 0x80, 15, 3, 0, 2, 1}, pathOpts{})
	if err != nil || !u.eorV6 || u.eorV4 {
		t.Fatalf("v6 EoR %+v %v", u, err)
	}
	v4only := openMsg{mp: map[uint32]bool{}}
	both := openMsg{mp: map[uint32]bool{1<<8 | 1: true, 2<<8 | 1: true}}
	if !famOn(v4only, v4only, afiIPv4, safiUnicast) || famOn(v4only, both, afiIPv6, safiUnicast) || !famOn(both, both, afiIPv6, safiUnicast) {
		t.Fatal("famOn")
	}
	p := &peer{up: true, gotUp: true, famV4: true, famV6: true}
	now := time.Now()
	p.gotEoR(viewInPre, &update{eorV4: true}, now)
	if p.synced() {
		t.Fatal("synced before v6 EoR")
	}
	p.gotEoR(viewInPre, &update{eorV6: true}, now)
	if !p.synced() {
		t.Fatal("not synced")
	}
	p.markDown(now, "test")
	if p.synced() || !p.view(-1).eorV4.IsZero() {
		t.Fatal("down keeps sync")
	}
}

// key 나누기의 partition (0.2.0) : 나누지 않은 key 는 Kafka 기본 (franz-go StickyKeyPartitioner = murmur2) 과 같고,
// 한 peer 의 칸 8 개는 partition 8 개에 하나씩
func TestBucketPartition(t *testing.T) {
	std := kgo.StickyKeyPartitioner(nil).ForTopic("t")
	for i := 0; i < 1000; i++ {
		k := []byte(md5hex(fmt.Sprint(i)))
		r := &kgo.Record{Key: k}
		if got, want := bucketPartition(r, 8), std.Partition(r, 8); got != want {
			t.Fatalf("key %s : %d, kafka default %d", k, got, want)
		}
		seen := map[int]bool{}
		for b := 0; b < 8; b++ {
			seen[bucketPartition(&kgo.Record{Key: []byte(fmt.Sprintf("%s.%d", k, b))}, 8)] = true
		}
		if len(seen) != 8 {
			t.Fatalf("peer %s : buckets on %d partitions", k, len(seen))
		}
	}
}
