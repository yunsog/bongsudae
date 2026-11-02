package main

import (
	"bufio"
	"os"
	"testing"
	"time"
)

// BenchmarkProcess : 표본 bmp_raw 덤프 (BONGSUDAE_BENCH_DUMP) 앞 30 만 메시지를 처리하는 데 드는 시간 / 할당 (GC 줄이기, 0.2.0).
// 파일은 저장소 밖이라 없으면 건너뜀. 예
//
//	BONGSUDAE_BENCH_DUMP=data/openbmp.bmp_raw.20261028-0445.dump go test -run '^$' -bench Process -benchmem -memprofile /tmp/mem.out .
func BenchmarkProcess(b *testing.B) {
	path := os.Getenv("BONGSUDAE_BENCH_DUMP")
	if path == "" {
		b.Skip("BONGSUDAE_BENCH_DUMP not set")
	}
	fh, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer fh.Close()
	r := bufio.NewReaderSize(fh, 1<<20)
	var msgs []rawMsg
	var ts []time.Time
	for len(msgs) < 300000 {
		d, err := readDump(r)
		if err != nil {
			break
		}
		m, err := parseRaw(d.value)
		if err != nil {
			b.Fatal(err)
		}
		msgs, ts = append(msgs, m), append(ts, d.ts)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := newCollector(msgs[0].cHash)
		for j, m := range msgs {
			c.handle(m.routerIP, m.routerHash, m.body, ts[j])
		}
	}
}
