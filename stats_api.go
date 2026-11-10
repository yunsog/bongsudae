// 운영 상태 API (10-30) : GET /api/stats
//
// 1 분 통계 (기록 파일의 "stat ..." 줄과 같은 수) 를 최근 60 분까지 메모리에 두고, 판 / 시작 시각 / 메모리 / Kafka 상태와 함께 줌.
// 감시 화면에서 bongsudae 가 잘 도는지 (errors, kafka_fail, raw, 토픽별 수) 보려고. 재시작하면 지난 통계는 없음.
package main

import (
	"maps"
	"net/http"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

const statKeep = 60 // 남기는 1 분 통계 수

// minuteStat : 1 분 통계 하나
type minuteStat struct {
	Time      string                `json:"time"`             // 이 1 분이 끝난 시각
	Routers   int                   `json:"routers"`          // 아는 라우터 수 (down 포함)
	Counts    map[string]int        `json:"counts"`           // route_mon, peer_up, ..., errors, add, del, msgs, not_sent, peer_up_no_open
	Raw       *int64                `json:"raw,omitempty"`    // bmp_raw 로 보낸 수 (tcp 입력만)
	KafkaFail int64                 `json:"kafka_fail_total"` // 시작 뒤 보내기 실패 모두
	Topics    map[string]topicCount `json:"topics,omitempty"` // 토픽별 메시지 / 줄 (전달 규칙)
	// OtherFamilies : 해석기가 없어 버린 경로 종류마다 MP_REACH / UNREACH 수 (0.3.0, 없으면 빠짐). counts.others 의 나눔
	OtherFamilies map[string]int `json:"other_families,omitempty"`
	// ByRouter : 라우터별 같은 수 (0.2.0, 그 1 분에 메시지나 오류가 있던 라우터만). ?by_router=1 일 때만 내보냄
	ByRouter map[string]map[string]int `json:"by_router,omitempty"`
}

// statHist : 최근 1 분 통계들 (logStat 이 넣고 API 가 읽음)
type statHist struct {
	mu      sync.Mutex
	list    []minuteStat
	started time.Time
	kafka   *producer // 마지막 logStat 의 producer (nil = 파일 입력)
	cum     stats     // 시작 뒤 1 분 통계를 모두 더한 것 (/metrics 의 _total, 0.2.0)
	rawCum  int64     // 시작 뒤 bmp_raw 로 보낸 수
}

var hist = statHist{started: time.Now()}

// add : logStat 이 1 분마다 부름 (total = 이 1 분의 합계)
func (h *statHist) add(m minuteStat, total *stats, p *producer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cum.addAll(total)
	if m.Raw != nil {
		h.rawCum += *m.Raw
	}
	h.list = append(h.list, m)
	if len(h.list) > statKeep {
		h.list = h.list[len(h.list)-statKeep:]
	}
	if p != nil {
		h.kafka = p
	}
}

// totals : 시작 뒤 누적 (/metrics)
func (h *statHist) totals() (stats, int64, *producer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	cum := h.cum
	cum.otherFam = maps.Clone(h.cum.otherFam) // map 은 복사 (잠금 밖에서 읽음)
	return cum, h.rawCum, h.kafka
}

// counts : 통계를 이름 -> 수 로 (0 인 BMP 종류는 뺌, 나머지는 늘 넣음)
func (s *stats) counts() map[string]int {
	m := map[string]int{"errors": s.errors, "others": s.others, "add": s.add, "del": s.del, "msgs": s.msgs,
		"router_msgs": s.routerMsgs, "peer_msgs": s.peerMsgs, "not_sent": s.dropped, "peer_up_no_open": s.noOpen, "family_rows": s.famRows}
	for i, n := range s.bmpType {
		if n == 0 {
			continue
		}
		name := "unknown"
		if i < bmpTypeCount {
			name = bmpTypeName[i]
		}
		m[name] = n
	}
	return m
}

// release : 판 번호 (GitHub 태그 v<release> 와 같음, 세 자리 = Go 모듈 판). 개발 중에는 "-dev" 를 붙이고,
// 올리는 날 떼어 냄 (0.1.0 = 11-01 첫 공개, 0.2.0 = 11-03 공개)
const release = "0.3.0"

// version : 빌드 정보의 git rev (+ 빌드 때 고친 것이 있으면 "-modified")
func version() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	rev, mod := "", ""
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				mod = "-modified"
			}
		}
	}
	if len(rev) > 7 {
		rev = rev[:7]
	}
	return rev + mod
}

func unixStr(ns int64) string {
	if ns == 0 {
		return ""
	}
	return tstr(time.Unix(0, ns))
}

type statsJSON struct {
	Release   string        `json:"release"` // 판 번호 (0.1)
	Version   string        `json:"version"` // git rev
	Started   string        `json:"started"`
	UptimeS   int64         `json:"uptime_s"`
	HeapMB    uint64        `json:"heap_mb"`
	SysMB     uint64        `json:"sys_mb"`
	NumGC     uint32        `json:"gc_cycles"`
	Goroutine int           `json:"goroutines"`
	Kafka     *kafkaJSON    `json:"kafka,omitempty"`
	Handover  *handoverJSON `json:"handover,omitempty"` // 무중단 판 올리기 (0.2.0, tcp 입력만, handover.go)
	Minutes   []minuteStat  `json:"minutes"`            // 오래된 것부터, 최대 60
}

type kafkaJSON struct {
	FailsTotal int64  `json:"fails_total"`
	LastOK     string `json:"last_ok"`
	LastFail   string `json:"last_fail,omitempty"`
}

func (c *collector) apiStats(w http.ResponseWriter, req *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	j := statsJSON{Release: release, Version: version(), Started: tstr(hist.started), UptimeS: int64(time.Since(hist.started).Seconds()),
		HeapMB: ms.HeapAlloc >> 20, SysMB: ms.Sys >> 20, NumGC: ms.NumGC, Goroutine: runtime.NumGoroutine()}
	hist.mu.Lock()
	j.Minutes = append([]minuteStat{}, hist.list...)
	p := hist.kafka
	hist.mu.Unlock()
	if req.URL.Query().Get("by_router") != "1" { // 기본은 합계만 (라우터별은 커서 원할 때만)
		for i := range j.Minutes {
			j.Minutes[i].ByRouter = nil
		}
	}
	j.Handover = upg.statsInfo()
	if p != nil {
		j.Kafka = &kafkaJSON{FailsTotal: p.fails.Load(), LastOK: unixStr(p.lastOK.Load()), LastFail: unixStr(p.lastFail.Load())}
	}
	writeJSON(w, j)
}
