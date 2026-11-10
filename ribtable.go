// ribtable.go : 경로표 한 장 (0.3.0). 경로 종류 / 보기가 늘어도 표를 다루는 코드는 이것 하나 (Go 제네릭 = C++ 템플릿).
//
// 열쇠 (K) 만 경로 종류마다 다름 : key4 (IPv4 프리픽스), key6 (IPv6), 나중에 VPN 은 RD + 프리픽스 등.
// 열쇠를 종류마다 꼭 필요한 크기로 두어 메모리가 늘지 않게 함 (공용 큰 열쇠면 7,700 만 경로에 1.5 ~ 2 GB 더 듦).
//
// Add-Path (RFC 7911) : 같은 프리픽스에 경로가 여럿 (path ID 로 구분). 같은 (프리픽스, path ID) 로 오면 그 경로만 바뀌고,
// 철회도 path ID 를 적어 그 경로만 지움. Add-Path 를 협상한 세션의 표만 m 대신 mp (프리픽스 -> path ID 별 목록) 를 씀
// = 보통 세션은 메모리가 그대로. 찾기는 프리픽스로 바로 (path ID 를 몰라도 그 프리픽스의 경로 모두).
//
//	var t ribTable[key4]
//	t.set(k, route{...})          넣기 / 바꾸기 (Add-Path 아님)
//	t.setPath(k, id, route{...})  넣기 / 바꾸기 (Add-Path : 그 path ID 만)
//	t.del(k) / t.delPath(k, id)   빼기 (없으면 그냥 지나감)
//	t.lookup(k)                   그 프리픽스의 경로들
//	for k, x := range t.all() { x.rt, x.id, x.addPath }   모든 경로 (순서 없음, Add-Path 면 같은 k 가 여러 번)
//	t.size() = 프리픽스 수, t.paths() = 경로 수 (Add-Path 아니면 같음)
//
// 비어 있는 표는 map 이 nil (메모리를 쓰지 않음). 잠금은 부르는 쪽 (r.mu)
package main

import (
	"iter"
	"net/netip"
)

// ribKey : 경로표 열쇠가 할 줄 알아야 하는 것
type ribKey interface {
	comparable
	prefix() netip.Prefix // 출력 / 찾기용 프리픽스
}

// pathRoute : Add-Path 의 경로 하나 (path ID + 경로)
type pathRoute struct {
	id uint32
	rt route
}

// pathOf : 돌거나 찾을 때 내주는 경로 하나 (addPath = Add-Path 세션의 것 : id 가 뜻이 있음)
type pathOf struct {
	rt      route
	id      uint32
	addPath bool
}

// ribTable : 열쇠 K -> 경로 (속성 + 마지막 add 시각)
type ribTable[K ribKey] struct {
	m  map[K]route       // Add-Path 아닌 세션 : 프리픽스 -> 경로 하나
	mp map[K][]pathRoute // Add-Path 세션 : 프리픽스 -> path ID 별 경로 (보통 2 ~ 4 개)
	np int               // mp 의 경로 수
}

// newRIBTable : n 개가 들어갈 자리를 미리 잡은 표 (상태 파일 읽기)
func newRIBTable[K ribKey](n int) ribTable[K] {
	return ribTable[K]{m: make(map[K]route, n)}
}

// size : 프리픽스 수
func (t *ribTable[K]) size() int { return len(t.m) + len(t.mp) }

// paths : 경로 수 (Add-Path 면 프리픽스 수보다 많을 수 있음)
func (t *ribTable[K]) paths() int { return len(t.m) + t.np }

// multi : Add-Path 경로가 있는가
func (t *ribTable[K]) multi() bool { return len(t.mp) > 0 }

// get : 열쇠 k 의 경로 (Add-Path 아닌 표)
func (t *ribTable[K]) get(k K) (route, bool) {
	rt, ok := t.m[k]
	return rt, ok
}

// lookup : 열쇠 k 의 경로 모두 (없으면 nil)
func (t *ribTable[K]) lookup(k K) []pathOf {
	if rt, ok := t.m[k]; ok {
		return []pathOf{{rt: rt}}
	}
	ps := t.mp[k]
	if len(ps) == 0 {
		return nil
	}
	out := make([]pathOf, len(ps))
	for i, p := range ps {
		out[i] = pathOf{rt: p.rt, id: p.id, addPath: true}
	}
	return out
}

// set : 넣거나 바꿈 (처음이면 map 을 만듦)
func (t *ribTable[K]) set(k K, rt route) {
	if t.m == nil {
		t.m = map[K]route{}
	}
	t.m[k] = rt
}

// setPath : Add-Path 경로 하나를 넣거나 바꿈 (같은 path ID 면 그것만 바뀜)
func (t *ribTable[K]) setPath(k K, id uint32, rt route) {
	if t.mp == nil {
		t.mp = map[K][]pathRoute{}
	}
	ps := t.mp[k]
	for i := range ps {
		if ps[i].id == id {
			ps[i].rt = rt
			return
		}
	}
	t.mp[k] = append(ps, pathRoute{id, rt})
	t.np++
}

// del : 뺌 (nil map 에 delete 는 그냥 지나감)
func (t *ribTable[K]) del(k K) { delete(t.m, k) }

// delPath : Add-Path 경로 하나만 뺌. 그 프리픽스의 마지막이면 프리픽스도 뺌
func (t *ribTable[K]) delPath(k K, id uint32) {
	ps := t.mp[k]
	for i := range ps {
		if ps[i].id == id {
			ps = append(ps[:i], ps[i+1:]...)
			t.np--
			if len(ps) == 0 {
				delete(t.mp, k)
			} else {
				t.mp[k] = ps
			}
			return
		}
	}
}

// clear : 모두 버림 (nil 로 : 메모리를 돌려줌)
func (t *ribTable[K]) clear() { *t = ribTable[K]{} }

// all : 모든 경로 (for k, x := range t.all() { ... }). 순서 없음. Add-Path 면 같은 프리픽스가 경로 수만큼
func (t *ribTable[K]) all() iter.Seq2[K, pathOf] {
	return func(yield func(K, pathOf) bool) {
		for k, rt := range t.m {
			if !yield(k, pathOf{rt: rt}) {
				return
			}
		}
		for k, ps := range t.mp {
			for _, p := range ps {
				if !yield(k, pathOf{rt: p.rt, id: p.id, addPath: true}) {
					return
				}
			}
		}
	}
}
