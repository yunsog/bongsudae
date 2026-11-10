// Kafka : 보내기 (unicast_prefix) 와 읽기 (bmp_raw, 그림자 실행용)
//
// Kafka 가 고장 날 때 (10-29 : KRaft 로 바꾸며 토픽을 다시 만듦 -> 예전 토픽 ID 로 계속 읽어 오류가 23 분에 4 GB 기록) :
//   - 오류 기록은 종류마다 1 분에 한 줄 (처음 한 번은 바로) : kafkaErrs
//   - 읽기 오류만 2 분 넘게 계속되거나, 보내기가 2 분 넘게 하나도 성공하지 못하면 스스로 끝냄
//     -> systemd (10 초 뒤, 11-01 전에는 cron 매분) 가 다시 띄우면 새 토픽 정보로 이어짐
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// producer : 묶어 보내기. 버퍼 (maxBuffered 레코드) 가 차면 send 가 기다림 = 읽기가 멈춤 (버리지 않음)
type producer struct {
	cl       *kgo.Client
	prefix   string       // 토픽 = prefix + "." + 종류 (openbmp.test / openbmp.parsed)
	rules    *rules       // 토픽별 1 분 통계
	fails    atomic.Int64 // 보내기 실패 (시작 뒤 모두)
	lastOK   atomic.Int64 // 마지막으로 보내기에 성공한 시각 (unix 나노초)
	lastFail atomic.Int64 // 마지막으로 실패한 시각
	closed   sync.Once

	rawTopic string       // 받은 BMP 를 그대로 내보낼 토픽 (빈칸 = 만든 적 없음). tcp 입력만 (enableRaw)
	rawOn    atomic.Bool  // 지금 내보내는지 (설정 bmp.raw, reload 로 켜고 끔)
	rawN     atomic.Int64 // 지난 1 분에 보낸 bmp_raw 수

	regoneMu sync.Mutex
	regoneAt map[string]time.Time // 토픽을 다시 읽은 시각 (topicGone, 30 초에 한 번)

	famMade sync.Map // 만든 경로 종류 토픽 (0.3.0 : labeled_unicast_prefix, l3vpn, multicast_prefix 는 처음 쓸 때 만듦)
}

const (
	maxBuffered    = 200000
	kafkaDeadAfter = 2 * time.Minute // 이만큼 Kafka 가 계속 안 되면 끝냄 (systemd 가 다시 띄움)
)

// ---- 오류 기록 (폭주 방지) ----

// errLog : 같은 오류 글자를 1 분에 한 줄로. 처음 나온 것은 바로 기록, 나머지는 flush 때 "N 번" 으로
type errLog struct {
	mu    sync.Mutex
	count map[string]int
}

var kafkaErrs = &errLog{count: map[string]int{}}

func (e *errLog) add(msg string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.count[msg] == 0 {
		log.Printf("%s", msg)
	}
	e.count[msg]++
}

// flush : 1 분마다 (logStat). 두 번 넘게 나온 것만 요약
func (e *errLog) flush() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for msg, n := range e.count {
		if n > 1 {
			log.Printf("%s : %d times (last minute)", msg, n)
		}
	}
	e.count = map[string]int{}
}

func newProducer(broker, prefix string) (*producer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(broker),
		kgo.ProducerLinger(20 * time.Millisecond),
		kgo.MaxBufferedRecords(maxBuffered),
		kgo.ProducerBatchCompression(kgo.NoCompression()), // 소비자 (Python) 가 압축 라이브러리 없이 읽도록
		kgo.RecordRetries(10),
		kgo.ProducerBatchMaxBytes(4 << 20), // bmp_raw 하나가 BMP 최대 1 MB + 머리 (기본 1 MB 를 넘을 수 있음)
	}
	if *optBucket > 0 { // key 나누기 : partition 을 직접 고름 (bucketPartition). 0 이면 Kafka 기본 그대로
		opts = append(opts, kgo.RecordPartitioner(kgo.BasicConsistentPartitioner(func(string) func(*kgo.Record, int) int {
			return bucketPartition
		})))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	for _, k := range allKinds {
		if err := ensureTopic(cl, prefix+"."+k); err != nil {
			cl.Close()
			return nil, err
		}
	}
	p := &producer{cl: cl, prefix: prefix}
	p.lastOK.Store(time.Now().UnixNano())
	return p, nil
}

// ---- bmp_raw : 라우터가 보낸 BMP 를 그대로 (openbmpd 의 openbmp.bmp_raw 와 같은 꼴) ----
//
//	key   = router hash
//	value = "V: 1.7\nC_HASH_ID: <c>\nR_HASH: <r>\nR_IP: <ip>\nL: <길이>\n\n" + BMP 바이트 그대로
//
// openbmpd 와 다른 점 : 해석에 실패한 메시지도 보냄 (다른 수집기로 다시 보내기 / 재생 시험에는 빠짐없는 것이 맞음).
// 전달 규칙 (rules.go) 은 적용하지 않음 (모든 라우터).

// rawTopicFor : 토픽 이름 = -prefix 에서 ".parsed" 를 뗀 것 + ".bmp_raw" (openbmp.parsed -> openbmp.bmp_raw)
func rawTopicFor(prefix string) string {
	return strings.TrimSuffix(prefix, ".parsed") + ".bmp_raw"
}

// enableRaw : bmp_raw 토픽을 만들고 켬 (tcp 입력 시작 때, 설정 reload 의 확인 단계)
// rawTopic 은 처음 한 번만 씀 (그 뒤로는 읽기만 : 토픽 이름은 -prefix 로 정해져 바뀌지 않음)
func (p *producer) enableRaw(prefix string) error {
	t := rawTopicFor(prefix)
	if err := ensureTopic(p.cl, t); err != nil {
		return err
	}
	if p.rawTopic == "" {
		p.rawTopic = t
	}
	p.rawOn.Store(true)
	return nil
}

// rawHead : 연결 하나의 bmp_raw 머리 "V: 1.7\nC_HASH_ID: ..\nR_HASH: ..\nR_IP: ..\nL: <길이>\n\n" 중 길이 앞까지 (0.2.0)
type rawHead struct {
	prefix string
	key    []byte // Kafka key = router hash (레코드마다 같은 바이트, 읽기만)
	room   int    // 머리가 들어갈 자리 = prefix + 길이 7 자리 (메시지 최대 1 MB) + "\n\n"
}

func newRawHead(cHash, rHash, ip string) *rawHead {
	pre := "V: 1.7\nC_HASH_ID: " + cHash + "\nR_HASH: " + rHash + "\nR_IP: " + ip + "\nL: "
	return &rawHead{prefix: pre, key: []byte(rHash), room: len(pre) + 7 + 2}
}

// sendRaw : BMP 메시지 하나를 그대로 (켜져 있을 때만). full = 앞 h.room 바이트가 빈 메시지 (msgReader.full) :
// 머리를 메시지 바로 앞에 써 넣어 복사 없이 보냄 (전에는 머리 + 메시지를 새로 할당해 복사)
func (p *producer) sendRaw(h *rawHead, full []byte) {
	if !p.rawOn.Load() {
		return
	}
	p.produceRec(&kgo.Record{Topic: p.rawTopic, Key: h.key, Value: h.value(full)})
	p.rawN.Add(1)
}

// value : 머리를 메시지 바로 앞에 써 넣고 머리 + 메시지 slice 를 돌려줌
func (h *rawHead) value(full []byte) []byte {
	msg := full[h.room:]
	var tmp [192]byte // 머리 (보통 150 바이트 안팎) : 넘으면 append 가 늘림
	head := append(append(append(tmp[:0], h.prefix...), strconv.Itoa(len(msg))...), '\n', '\n')
	start := h.room - len(head)
	if start < 0 { // 길이 자리가 모자람 (메시지 최대 1 MB 라 없어야 함) : 예전처럼 새로 할당해 복사
		return append(append(make([]byte, 0, len(head)+len(msg)), head...), msg...)
	}
	copy(full[start:], head)
	return full[start:]
}

// dead : 보내기가 kafkaDeadAfter 넘게 하나도 성공하지 못했고, 그동안 실패했거나 보낼 것이 쌓여 있음
// (조용해서 보낼 것이 없을 때는 dead 가 아님)
func (p *producer) dead() bool {
	ok := time.Unix(0, p.lastOK.Load())
	if time.Since(ok) < kafkaDeadAfter {
		return false
	}
	return p.lastFail.Load() > p.lastOK.Load() || p.cl.BufferedProduceRecords() > 0
}

// ensureTopic : 토픽이 없으면 만듦. partition / replication / 보관 시간 = 옵션 -partitions / -replication / -retention
// (설정 kafka.partitions / replication / retention, 기본 8 / 1 / 90 분 = 우리 운영 토픽과 같게). 이미 있는 토픽은 건드리지 않음
func ensureTopic(cl *kgo.Client, topic string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	retention := strconv.FormatInt(optRetention.Milliseconds(), 10)
	r, err := kadm.NewClient(cl).CreateTopic(ctx, int32(*optParts), int16(*optRepl), map[string]*string{"retention.ms": &retention}, topic)
	if err == nil {
		err = r.Err
	}
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("create topic %s: %w", topic, err)
	}
	return nil
}

// send : 메시지들을 보냄 (비동기, 결과는 callback). unicast_prefix 는 규칙의 토픽마다 한 번씩 (내용 같음)
func (p *producer) send(msgs []outMsg) {
	for _, m := range msgs {
		if m.kind != kindPrefix {
			if familyKinds[m.kind] {
				p.ensureFamily(m.kind)
			}
			p.produce(m.kind, &m)
			continue
		}
		for _, t := range m.topics {
			p.produce(t, &m)
			if p.rules != nil {
				p.rules.count(t, m.rows)
			}
		}
	}
}

// ensureFamily : 경로 종류 토픽을 처음 쓸 때 만듦 (그 라우터 goroutine 이 한 번 기다림. 실패하면 기록하고 보내기는 해 봄 = 자동 생성에 맡김)
func (p *producer) ensureFamily(kind string) {
	if _, ok := p.famMade.Load(kind); ok {
		return
	}
	t := p.prefix + "." + kind
	if err := ensureTopic(p.cl, t); err != nil {
		kafkaErrs.add(err.Error())
		return
	}
	if _, loaded := p.famMade.LoadOrStore(kind, true); !loaded {
		log.Printf("kafka topic %s : first %s route, topic ready", t, kind)
	}
}

func (p *producer) produce(name string, m *outMsg) {
	p.produceRec(&kgo.Record{Topic: p.prefix + "." + name, Key: []byte(m.key), Value: m.value})
}

// produceRec : 레코드 하나 (비동기). 결과로 성공 / 실패 시각과 오류 기록 (kafkaErrs)
func (p *producer) produceRec(rec *kgo.Record) {
	p.cl.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
		now := time.Now().UnixNano()
		if err == nil {
			p.lastOK.Store(now)
			return
		}
		p.fails.Add(1)
		p.lastFail.Store(now)
		kafkaErrs.add(fmt.Sprintf("kafka produce %s: %v", r.Topic, err))
		if topicGone(err) {
			p.retopic(r.Topic)
		}
	})
}

// topicGone : 토픽이 지워졌거나 다시 만들어져 (새 토픽 ID) 클라이언트가 가진 정보가 틀렸다는 오류
// (10-29 KRaft 전환 때 겪음 : franz-go 는 옛 ID 를 계속 써서 2 분 뒤 끝내고 systemd 재시작으로만 회복했음)
func topicGone(err error) bool {
	return errors.Is(err, kerr.UnknownTopicID) || errors.Is(err, kerr.UnknownTopicOrPartition)
}

// retopic : 그 토픽의 정보를 버리고 (버퍼의 그 토픽 레코드도 실패로) 다시 읽음, 없으면 만듦. 토픽마다 30 초에 한 번 (0.2.0)
// 바로 다음 레코드부터 새 토픽 ID 로 감 = 재시작 (RIB 다시 받기) 없이 회복
func (p *producer) retopic(topic string) {
	p.regoneMu.Lock()
	if p.regoneAt == nil {
		p.regoneAt = map[string]time.Time{}
	}
	if time.Since(p.regoneAt[topic]) < 30*time.Second {
		p.regoneMu.Unlock()
		return
	}
	p.regoneAt[topic] = time.Now()
	p.regoneMu.Unlock()
	go func() { // callback 안에서 막히지 않게 따로
		log.Printf("kafka topic %s : unknown topic (deleted / re-created?), re-reading topic metadata", topic)
		p.cl.PurgeTopicsFromProducing(topic)
		if err := ensureTopic(p.cl, topic); err != nil {
			log.Printf("kafka topic %s : %v", topic, err)
		}
	}()
}

// close : 남은 것을 다 보내고 닫음 (여러 번 불러도 한 번만)
func (p *producer) close() {
	p.closed.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := p.cl.Flush(ctx); err != nil {
			log.Printf("kafka flush: %v", err)
		}
		p.cl.Close()
	})
}

// rawJob : 라우터 goroutine 에게 넘기는 bmp_raw 레코드 하나
type rawJob struct {
	raw rawMsg
	ts  time.Time
}

// rawWorkers : 라우터마다 goroutine 하나 (tcp 입력의 연결 goroutine 과 같은 역할 = 라우터끼리 동시에, 라우터 안은 순서대로)
type rawWorkers struct {
	c   *collector
	out *producer
	ch  map[string]chan rawJob
	wg  sync.WaitGroup
}

// put : 라우터의 goroutine 에게 넘김 (없으면 만듦). 그 goroutine 의 줄이 차면 기다림 (= Kafka 읽기가 늦춰짐)
func (w *rawWorkers) put(j rawJob) {
	ch := w.ch[j.raw.routerIP]
	if ch == nil {
		ch = make(chan rawJob, 1000)
		w.ch[j.raw.routerIP] = ch
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for j := range ch {
				w.out.send(w.c.handle(j.raw.routerIP, j.raw.routerHash, j.raw.body, j.ts))
			}
		}()
	}
	ch <- j
}

// stop : 받은 것을 다 처리할 때까지 기다림
func (w *rawWorkers) stop() {
	for _, ch := range w.ch {
		close(ch)
	}
	w.wg.Wait()
}

// runKafka : bmp_raw 를 실시간으로 읽어 처리 (그림자 실행). 시각 = bmp_raw 레코드 시각
// ctx 가 끝나면 (SIGTERM) : 라우터 goroutine 이 다 처리하고, 보낼 것을 다 보내고, 읽은 위치를 저장하고, group 에서 나감
// -> 다시 시작할 때 같은 메시지를 두 번 처리하지 않음
func runKafka(ctx context.Context, broker, src, group string, c *collector, out *producer) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(src),
		kgo.ConsumeResetOffset(startOffset()), // 처음이면 지금부터 (-from-start 면 토픽 처음부터)
	)
	if err != nil {
		return err
	}
	defer cl.Close() // group 에서 나감
	w := &rawWorkers{c: c, out: out, ch: map[string]chan rawJob{}}
	tick := time.Now()
	var badSince time.Time // 읽기 오류만 오기 시작한 시각 (레코드를 받으면 0 으로)
	var regone time.Time   // 읽는 토픽을 다시 읽은 시각 (토픽이 다시 만들어짐, 30 초에 한 번)
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			w.stop()
			out.close()
			err := cl.CommitUncommittedOffsets(context.Background())
			log.Printf("stop: flushed, commit offsets: %v", err)
			return nil
		}
		nerr := 0
		gone := false
		fetches.EachError(func(t string, p int32, err error) {
			nerr++
			kafkaErrs.add(fmt.Sprintf("fetch %s: %v", t, err)) // partition 은 빼고 : 같은 오류 = 한 종류
			gone = gone || topicGone(err)
		})
		if gone && time.Since(regone) > 30*time.Second { // 토픽이 다시 만들어짐 : 버리고 다시 읽기 (0.2.0)
			regone = time.Now()
			log.Printf("kafka topic %s : unknown topic (deleted / re-created?), re-subscribing", src)
			cl.PurgeTopicsFromConsuming(src)
			cl.AddConsumeTopics(src)
		}
		switch {
		case fetches.NumRecords() > 0:
			badSince = time.Time{}
		case nerr > 0:
			if badSince.IsZero() {
				badSince = time.Now()
			}
			if time.Since(badSince) > kafkaDeadAfter {
				kafkaErrs.flush()
				w.stop()
				out.close()
				return fmt.Errorf("kafka read errors for more than %s : exiting (systemd restarts)", kafkaDeadAfter)
			}
			time.Sleep(time.Second) // 오류만 올 때 빈 돌기 막기
		}
		fetches.EachRecord(func(r *kgo.Record) {
			raw, err := parseRaw(r.Value)
			if err != nil {
				c.statErr()
				log.Printf("p%d/%d: %v", r.Partition, r.Offset, err)
				return
			}
			w.put(rawJob{raw: raw, ts: r.Timestamp})
		})
		if time.Since(tick) >= time.Minute {
			tick = time.Now()
			c.logStat(out)
			sdNotify("WATCHDOG=1")
			if out.dead() {
				w.stop()
				return fmt.Errorf("kafka produce failing for more than %s : exiting (systemd restarts)", kafkaDeadAfter)
			}
		}
	}
}

// startOffset : 새 group 이 읽기 시작할 곳
func startOffset() kgo.Offset {
	if *optStart {
		return kgo.NewOffset().AtStart()
	}
	return kgo.NewOffset().AtEnd()
}

// ---- key 나누기의 partition 고르기 (0.2.0, -key-buckets > 0 일 때만) ----
//
// key "<peer hash>.<칸>" (unicast_prefix) : partition = (murmur2(peer hash) + 칸) % n.
// 한 peer 의 칸들이 이웃 partition 에 하나씩 놓여 (칸 8 개, partition 8 개면 모든 partition 에 정확히 하나)
// 큰 peer 도 고르게 퍼짐. key 를 murmur2 에 그대로 맡기면 칸끼리 같은 partition 에 몰릴 수 있음
// (11-01 RIB 7,894 만 줄 실측 : peer hash 만 = 최대 / 최소 1.73, key 나누기 + Kafka murmur2 = 1.21, 이 방식 = 1.00).
// 같은 router + peer + 프리픽스 = 같은 칸 = 같은 partition (순서 유지). 그 밖의 key (peer, router, bmp_raw) 는
// Kafka 기본 (Java 클라이언트, franz-go 기본) 과 같은 murmur2(key) % n = 지금과 같은 partition
func bucketPartition(r *kgo.Record, n int) int {
	k := r.Key
	if i := bytes.LastIndexByte(k, '.'); i == 32 { // md5 글자 32 자 + "." + 칸
		if b, err := strconv.Atoi(string(k[i+1:])); err == nil && b >= 0 {
			return int((uint64(murmur2(k[:i])&0x7fffffff) + uint64(b)) % uint64(n))
		}
	}
	return int(murmur2(k)&0x7fffffff) % n
}

// murmur2 : Kafka 의 key 해시 (Java 클라이언트 Utils.murmur2 와 같음)
func murmur2(b []byte) uint32 {
	const (
		seed uint32 = 0x9747b28c
		m    uint32 = 0x5bd1e995
		r           = 24
	)
	h := seed ^ uint32(len(b))
	for len(b) >= 4 {
		k := uint32(b[3])<<24 | uint32(b[2])<<16 | uint32(b[1])<<8 | uint32(b[0])
		b = b[4:]
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}
	switch len(b) {
	case 3:
		h ^= uint32(b[2]) << 16
		fallthrough
	case 2:
		h ^= uint32(b[1]) << 8
		fallthrough
	case 1:
		h ^= uint32(b[0])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}
