// 무중단 판 올리기 (0.2.0) 의 상태 파일 중 경로표 부분 : peer 경로표를 바이트로 쓰고 / 다시 만듦
//
// peer 마다 한 덩어리 (속성 표 + 경로들). 덩어리끼리는 서로 모르므로 여러 코어가 나눠 만들고 / 읽음.
// 파일에는 peer 순서대로 (읽는 쪽도 같은 순서의 peer 목록을 가짐 : 상태 파일의 peer 부분).
//
//	덩어리 = 내용 길이 (8 바이트) + 내용
//	내용   = 속성 수, 속성마다 13 칸 (글자 = 길이 + 바이트, 수 = uvarint)
//	         형식 1 (0.2) : 표 하나 = 보기 하나 (주 보기)
//	         형식 2 (0.3) : 보기 수, 보기마다 보기 번호 1 바이트 + 표 하나 (속성 표는 보기들이 함께 씀)
//	표     = v4 경로 수, 경로마다 열쇠 5 바이트 (주소 4 + 길이 1) + 속성 번호 uvarint + 시각 4 바이트
//	         v6 경로 수, 경로마다 열쇠 17 바이트 (주소 16 + 길이 1) + 속성 번호 uvarint + 시각 4 바이트
//	         (형식 2 만) Add-Path : v4 프리픽스 수, 프리픽스마다 열쇠 5 바이트 + 경로 수 + 경로마다 (path ID uvarint + 속성 번호 + 시각),
//	         v6 도 같은 꼴 (열쇠 17 바이트). 형식 1 은 Add-Path 경로를 못 담음 (writeHandover 가 막음)
//	         (형식 2 만) 경로 종류 표 수, 표마다 종류 uvarint + 표 (famrib.go encode). 속성에 labels 칸 (형식 2 만)
//
// 수 (개수, 번호) 는 uvarint, 고정 길이는 little endian. 속성 번호는 그 덩어리 안에서만 (peer 마다 속성 표).
package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"unique"
)

// ---- 쓰기 ----

// encodeRIB : peer 하나의 경로표 (보기들) -> 덩어리 내용 (format 1 = 주 보기 하나, 2 = 보기마다)
func (p *peer) encodeRIB(format int) []byte {
	idx := map[unique.Handle[pathAttr]]uint64{} // 속성 -> 이 덩어리의 번호
	var attrs []unique.Handle[pathAttr]
	num := func(h unique.Handle[pathAttr]) uint64 {
		i, ok := idx[h]
		if !ok {
			i = uint64(len(attrs))
			idx[h] = i
			attrs = append(attrs, h)
		}
		return i
	}
	key4b := func(b []byte, k key4) []byte {
		b = binary.LittleEndian.AppendUint32(b, uint32(k>>8))
		return append(b, byte(k))
	}
	key6b := func(b []byte, k key6) []byte {
		b = binary.LittleEndian.AppendUint64(b, k.hi)
		b = binary.LittleEndian.AppendUint64(b, k.lo)
		return append(b, k.n)
	}
	rt := func(b []byte, r route) []byte {
		b = binary.AppendUvarint(b, num(r.a))
		return binary.LittleEndian.AppendUint32(b, r.t)
	}
	table := func(routes []byte, rv *ribView) []byte {
		routes = binary.AppendUvarint(routes, uint64(len(rv.rib4.m)))
		for k, r := range rv.rib4.m {
			routes = rt(key4b(routes, k), r)
		}
		routes = binary.AppendUvarint(routes, uint64(len(rv.rib6.m)))
		for k, r := range rv.rib6.m {
			routes = rt(key6b(routes, k), r)
		}
		if format < 2 {
			return routes
		}
		routes = binary.AppendUvarint(routes, uint64(len(rv.rib4.mp)))
		for k, ps := range rv.rib4.mp {
			routes = binary.AppendUvarint(key4b(routes, k), uint64(len(ps)))
			for _, p := range ps {
				routes = rt(binary.AppendUvarint(routes, uint64(p.id)), p.rt)
			}
		}
		routes = binary.AppendUvarint(routes, uint64(len(rv.rib6.mp)))
		for k, ps := range rv.rib6.mp {
			routes = binary.AppendUvarint(key6b(routes, k), uint64(len(ps)))
			for _, p := range ps {
				routes = rt(binary.AppendUvarint(routes, uint64(p.id)), p.rt)
			}
		}
		fs := make([]family, 0, len(rv.fam)) // 경로 종류 표 (famrib.go)
		for f := range rv.fam {
			fs = append(fs, f)
		}
		slices.Sort(fs)
		routes = binary.AppendUvarint(routes, uint64(len(fs)))
		for _, f := range fs {
			routes = binary.AppendUvarint(routes, uint64(f))
			routes = rv.fam[f].encode(routes, num)
		}
		return routes
	}
	// 경로부터 만들고 (속성 번호가 정해짐) 속성 표를 앞에 붙임
	n := 16
	for _, rv := range p.views {
		if rv != nil {
			n += rv.rib4.paths()*14 + rv.rib6.paths()*26 + 16
		}
	}
	routes := make([]byte, 0, n)
	if format < 2 {
		routes = table(routes, p.view(-1))
	} else {
		routes = binary.AppendUvarint(routes, uint64(p.viewCount()))
		for v, rv := range p.views {
			if rv != nil {
				routes = append(routes, byte(v))
				routes = table(routes, rv)
			}
		}
	}
	b := binary.AppendUvarint(nil, uint64(len(attrs)))
	for _, h := range attrs {
		b = appendAttr(b, h.Value(), format)
	}
	return append(b, routes...)
}

func appendStr(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

// appendAttr : 속성 묶음 하나 (칸 순서 = pathAttr 순서. 칸을 더하면 stateVersion 을 올림). labels 는 형식 2 만
func appendAttr(b []byte, a pathAttr, format int) []byte {
	b = appendStr(b, a.asPath)
	b = binary.AppendUvarint(b, uint64(a.originAS))
	b = appendStr(b, a.originSet)
	b = appendStr(b, a.nextHop)
	b = appendStr(b, a.origin)
	b = binary.AppendUvarint(b, uint64(a.med))
	b = binary.AppendUvarint(b, uint64(a.localPref))
	b = appendStr(b, a.community)
	b = appendStr(b, a.extComm)
	b = appendStr(b, a.largeComm)
	b = appendStr(b, a.clusterList)
	b = appendStr(b, a.originatorID)
	b = appendStr(b, a.aggregator)
	if format >= 2 {
		b = appendStr(b, a.labels)
	}
	return b
}

// writeRIBs : peer 들의 경로표를 차례로 씀. 덩어리는 여러 goroutine 이 미리 만들고, 쓰기는 순서대로 하나씩.
// 돌려주는 것 = 쓴 바이트 수
func writeRIBs(w io.Writer, ps []*peer, format int) (int64, error) {
	outs := make([]chan []byte, len(ps)) // peer i 의 덩어리가 오는 곳
	for i := range outs {
		outs[i] = make(chan []byte, 1)
	}
	var next atomic.Int64 // 다음에 만들 peer 번호 (일꾼들이 차례로 가져감 = 쓰는 순서와 거의 같음)
	var wg sync.WaitGroup
	for range runtime.NumCPU() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= len(ps) {
					return
				}
				outs[i] <- ps[i].encodeRIB(format)
			}
		}()
	}
	var total int64
	var err error
	for i := range ps {
		b := <-outs[i]
		if err != nil {
			continue // 일꾼이 끝나도록 받기만
		}
		var n [8]byte
		binary.LittleEndian.PutUint64(n[:], uint64(len(b)))
		if _, err = w.Write(n[:]); err == nil {
			_, err = w.Write(b)
		}
		total += int64(8 + len(b))
	}
	wg.Wait()
	return total, err
}

// ---- 읽기 ----

var errStateShort = errors.New("state file: truncated")

// dec : 바이트를 앞에서부터 읽음 (C 의 포인터 + 남은 길이). 틀리면 err 에 남기고 0 을 돌려줌
type dec struct {
	b   []byte
	err error
}

func (d *dec) uvarint() uint64 {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *dec) take(n int) []byte {
	if n < 0 || len(d.b) < n {
		d.fail()
		return nil
	}
	v := d.b[:n]
	d.b = d.b[n:]
	return v
}

func (d *dec) fail()       { d.err, d.b = errStateShort, nil }
func (d *dec) str() string { return string(d.take(int(d.uvarint()))) }
func (d *dec) u32() uint32 { return uint32(d.uvarint()) }

// 고정 길이 : 모자라면 take 가 nil (err 기록) -> 0
func (d *dec) u8() uint8 {
	if b := d.take(1); b != nil {
		return b[0]
	}
	return 0
}

func (d *dec) le32() uint32 {
	if b := d.take(4); b != nil {
		return binary.LittleEndian.Uint32(b)
	}
	return 0
}

func (d *dec) le64() uint64 {
	if b := d.take(8); b != nil {
		return binary.LittleEndian.Uint64(b)
	}
	return 0
}

// count : 개수. 남은 바이트 (하나에 적어도 each 바이트) 로 될 수 없는 큰 수면 틀린 파일
// (map 을 미리 크게 잡다가 메모리를 다 쓰지 않게)
func (d *dec) count(each int) int {
	n := d.uvarint()
	if n > uint64(len(d.b)/each) {
		d.fail()
		return 0
	}
	return int(n)
}

func (d *dec) attr(format int) pathAttr {
	a := pathAttr{
		asPath: d.str(), originAS: d.u32(), originSet: d.str(), nextHop: d.str(), origin: d.str(),
		med: d.u32(), localPref: d.u32(), community: d.str(), extComm: d.str(), largeComm: d.str(),
		clusterList: d.str(), originatorID: d.str(), aggregator: d.str(),
	}
	if format >= 2 {
		a.labels = d.str()
	}
	return a
}

// decodeRIB : 덩어리 내용 -> 보기마다 경로표 (nil = 그 보기 없음) 와 경로 수.
// 형식 1 의 표 하나는 보기 defView (그 peer 의 마지막 per-peer 머리 보기)
func decodeRIB(b []byte, format, defView int) ([nViews]*ribView, int, error) {
	var tabs [nViews]*ribView
	d := &dec{b: b}
	attrs := make([]unique.Handle[pathAttr], d.count(13))
	for i := range attrs {
		attrs[i] = unique.Make(d.attr(format))
	}
	handle := func() unique.Handle[pathAttr] {
		i := d.uvarint()
		if i >= uint64(len(attrs)) {
			d.fail()
			return unique.Handle[pathAttr]{}
		}
		return attrs[i]
	}
	routes := 0
	key4d := func() key4 {
		a := d.le32()
		return key4(uint64(a)<<8 | uint64(d.u8()))
	}
	key6d := func() key6 { return key6{hi: d.le64(), lo: d.le64(), n: d.u8()} }
	table := func(rv *ribView) {
		if n := d.count(10); n > 0 {
			rv.rib4 = newRIBTable[key4](n)
			for range n {
				k := key4d()
				rv.rib4.set(k, route{handle(), d.le32()})
				if d.err != nil {
					return
				}
			}
		}
		if n := d.count(22); n > 0 {
			rv.rib6 = newRIBTable[key6](n)
			for range n {
				k := key6d()
				rv.rib6.set(k, route{handle(), d.le32()})
				if d.err != nil {
					return
				}
			}
		}
		if format >= 2 { // Add-Path
			for range d.count(6) {
				k, np := key4d(), d.count(6)
				for range np {
					id := d.u32()
					rv.rib4.setPath(k, id, route{handle(), d.le32()})
				}
				if d.err != nil {
					return
				}
			}
			for range d.count(18) {
				k, np := key6d(), d.count(6)
				for range np {
					id := d.u32()
					rv.rib6.setPath(k, id, route{handle(), d.le32()})
				}
				if d.err != nil {
					return
				}
			}
			for range d.count(2) { // 경로 종류 표
				f := family(d.uvarint())
				t := newFamTable(f)
				if t == nil || d.err != nil {
					d.fail()
					return
				}
				t.decode(d, handle)
				if rv.fam == nil {
					rv.fam = map[family]famTable{}
				}
				rv.fam[f] = t
				routes += t.paths()
			}
		}
		routes += rv.rib4.paths() + rv.rib6.paths()
	}
	if format < 2 {
		tabs[defView] = &ribView{}
		table(tabs[defView])
	} else {
		nv := d.count(1)
		for range nv {
			v := int(d.u8())
			if d.err != nil || v >= nViews || tabs[v] != nil {
				return tabs, 0, fmt.Errorf("state file: bad view %d", v)
			}
			tabs[v] = &ribView{}
			table(tabs[v])
		}
	}
	if d.err != nil {
		return tabs, 0, d.err
	}
	if len(d.b) != 0 {
		return tabs, 0, fmt.Errorf("state file: %d bytes left in a route table", len(d.b))
	}
	return tabs, routes, nil
}

// readRIBs : writeRIBs 가 쓴 덩어리 n 개를 읽음. 읽기는 하나씩, 덩어리마다 fn(번호, 내용) 은 여러 goroutine 이 부름
// (fn 이 경로표를 다시 만들고 넣음 : 간단히는 ps[i].decodeRIB, 판 올리기는 handover.go loadRIBs)
func readRIBs(r io.Reader, n int, fn func(i int, b []byte) error) error {
	type job struct {
		i int
		b []byte
	}
	jobs := make(chan job, runtime.NumCPU())
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	for range runtime.NumCPU() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := fn(j.i, j.b); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("route table %d: %w", j.i, err)
					}
					errMu.Unlock()
				}
			}
		}()
	}
	var err error
	for i := range n {
		var h [8]byte
		if _, err = io.ReadFull(r, h[:]); err != nil {
			break
		}
		size := binary.LittleEndian.Uint64(h[:])
		if size > 1<<34 { // 16 GB : peer 하나로 될 수 없는 크기 = 틀린 파일
			err = fmt.Errorf("state file: route table %d bytes", size)
			break
		}
		b := make([]byte, size)
		if _, err = io.ReadFull(r, b); err != nil {
			break
		}
		jobs <- job{i, b}
	}
	close(jobs)
	wg.Wait()
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		err = errStateShort
	}
	if err != nil {
		return err
	}
	return firstErr
}
