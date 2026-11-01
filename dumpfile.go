// 덤프 파일 (tools/kafka_dump 형식) 읽기 / 쓰기
//
// 레코드마다 : "partition \t offset \t 시각ms \t key 길이 \t value 길이 \n" + key + value + "\n"
package main

import (
	"bufio"
	"fmt"
	"io"
	"time"
)

type dumpRecord struct {
	partition int
	offset    int64
	ts        time.Time
	key       []byte
	value     []byte
}

// readDump : 레코드 하나. 파일 끝이면 io.EOF
func readDump(r *bufio.Reader) (*dumpRecord, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		if err == io.EOF && line == "" {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("dump head: %w", err)
	}
	var d dumpRecord
	var ms int64
	var kl, vl int
	if _, err := fmt.Sscanf(line, "%d\t%d\t%d\t%d\t%d\n", &d.partition, &d.offset, &ms, &kl, &vl); err != nil {
		return nil, fmt.Errorf("dump head %q: %w", line, err)
	}
	buf := make([]byte, kl+vl+1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("dump body: %w", err)
	}
	d.ts = time.UnixMilli(ms)
	d.key, d.value = buf[:kl], buf[kl:kl+vl]
	return &d, nil
}

// writeDump : 레코드 하나
func writeDump(w *bufio.Writer, d *dumpRecord) {
	fmt.Fprintf(w, "%d\t%d\t%d\t%d\t%d\n", d.partition, d.offset, d.ts.UnixMilli(), len(d.key), len(d.value))
	w.Write(d.key)
	w.Write(d.value)
	w.WriteByte('\n')
}
