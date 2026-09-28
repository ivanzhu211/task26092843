package notify

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentModel 是主验收场景：
// 多 goroutine 并发投递（含重复事件编号、失败重试、多收件人），
// 穿插标记已读与到期重试，全程校验未读不变量，
// 最终未读集合必须与按规则独立推算的结果完全一致。
func TestConcurrentModel(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	const (
		totalEvents   = 3000 // 去重后的事件数
		deliverFans   = 8    // 投递 goroutine 数
		extraReaders  = 3    // 其他收件人
		baseTime      = int64(1_700_000_000)
		oldEventLimit = 2000 // 事件序号 < 2000 的绝不可能进入最终 top-500
	)

	// 事件 i：event_time = base + i/4（每组 4 个共享时间，考验同时间排序）。
	makeDelivery := func(i int, recipient string) Delivery {
		return Delivery{
			RecipientID: recipient,
			EventID:     fmt.Sprintf("evt-%06d", i),
			EventTime:   time.Unix(baseTime+int64(i/4), 0),
			Payload:     fmt.Sprintf("doc-%d", i),
		}
	}

	// 构造投递任务：每个事件投 1~3 次，打乱后分给各 goroutine。
	var tasks []Delivery
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < totalEvents; i++ {
		for k := 0; k <= i%3; k++ {
			tasks = append(tasks, makeDelivery(i, "alice"))
		}
	}
	rng.Shuffle(len(tasks), func(a, b int) { tasks[a], tasks[b] = tasks[b], tasks[a] })

	// 事件序号 0..499 的首次投递使用失败 gate，进入重试队列。
	failSet := map[string]bool{}
	for i := 0; i < 500; i++ {
		failSet[fmt.Sprintf("evt-%06d", i)] = true
	}
	var failOnce sync.Map // 每个事件只让它首次投递失败

	var done atomic.Bool
	var invariantErr atomic.Value
	var wg sync.WaitGroup

	// 监控 goroutine：全程校验不变量。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !done.Load() {
			count, list, err := s.UnreadSnapshot(ctx, "alice")
			if err != nil {
				invariantErr.Store(fmt.Sprintf("snapshot: %v", err))
				return
			}
			if count > MaxUnread {
				invariantErr.Store(fmt.Sprintf("unread count %d > %d", count, MaxUnread))
				return
			}
			if count != len(list) {
				invariantErr.Store(fmt.Sprintf("count %d != list len %d", count, len(list)))
				return
			}
		}
	}()

	// 投递 goroutine。
	chunk := (len(tasks) + deliverFans - 1) / deliverFans
	for g := 0; g < deliverFans; g++ {
		lo, hi := g*chunk, (g+1)*chunk
		if hi > len(tasks) {
			hi = len(tasks)
		}
		if lo >= hi {
			continue
		}
		wg.Add(1)
		go func(part []Delivery) {
			defer wg.Done()
			for _, d := range part {
				var g Gate
				if failSet[d.EventID] {
					if _, loaded := failOnce.LoadOrStore(d.EventID, true); !loaded {
						g = alwaysFail
					}
				}
				if err := s.Deliver(ctx, d, g); err != nil {
					invariantErr.Store(fmt.Sprintf("deliver: %v", err))
					return
				}
			}
		}(tasks[lo:hi])
	}

	// 标记已读 goroutine：只针对必定被挤出的旧事件，保证结果可推算。
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(7))
		for i := 0; i < 400; i++ {
			idx := r.Intn(oldEventLimit)
			d := makeDelivery(idx, "alice")
			if err := s.MarkRead(ctx, "alice", d.EventID, baseTime+1_000_000); err != nil {
				invariantErr.Store(fmt.Sprintf("mark read: %v", err))
				return
			}
			if i%50 == 0 {
				// cutoff 远低于 top-500 的事件时间，不影响最终集合。
				if _, err := s.MarkAllReadBefore(ctx, "alice", baseTime+100, baseTime+1_000_000); err != nil {
					invariantErr.Store(fmt.Sprintf("mark all: %v", err))
					return
				}
			}
		}
	}()

	// 重试 goroutine：调用方驱动的虚拟时钟，不真实等待。
	wg.Add(1)
	go func() {
		defer wg.Done()
		now := time.Now().Unix()
		for !done.Load() {
			if _, err := s.ProcessRetries(ctx, now+120, nil); err != nil {
				invariantErr.Store(fmt.Sprintf("process retries: %v", err))
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// 其他收件人并发投递，验证互不影响。
	for r := 0; r < extraReaders; r++ {
		recipient := fmt.Sprintf("user-%d", r)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 600; i++ {
				if err := s.Deliver(ctx, makeDelivery(i, recipient), nil); err != nil {
					invariantErr.Store(fmt.Sprintf("deliver %s: %v", recipient, err))
					return
				}
			}
		}()
	}

	// 等待投递与标记结束，然后让重试全部落地。
	waitCh := make(chan struct{})
	go func() { wg.Wait(); close(waitCh) }()

	// 先等投递类 goroutine（不含监控与重试）。
	// 简单起见：轮询直到所有事件都已落地。
	deadline := time.Now().Add(60 * time.Second)
	for {
		if v := invariantErr.Load(); v != nil {
			done.Store(true)
			<-waitCh
			t.Fatal(v.(string))
		}
		info, err := s.DeliveryStatus(ctx, "alice", makeDelivery(0, "alice").EventID)
		if err == nil && info.State == DeliveryDelivered {
			// 抽样确认重试已推进；再统计 pending 数。
			var pending int
			row := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM deliveries WHERE state = 'pending'`)
			if row.Scan(&pending) == nil && pending == 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			done.Store(true)
			<-waitCh
			t.Fatal("timeout waiting for deliveries to settle")
		}
		time.Sleep(5 * time.Millisecond)
	}
	done.Store(true)
	<-waitCh

	if v := invariantErr.Load(); v != nil {
		t.Fatal(v.(string))
	}

	// 独立推算：全部事件去重后按 (event_time DESC, event_id DESC) 取前 500。
	all := make([]Delivery, totalEvents)
	for i := 0; i < totalEvents; i++ {
		all[i] = makeDelivery(i, "alice")
	}
	sort.Slice(all, func(a, b int) bool {
		if !all[a].EventTime.Equal(all[b].EventTime) {
			return all[a].EventTime.After(all[b].EventTime)
		}
		return all[a].EventID > all[b].EventID
	})
	want := all[:MaxUnread]

	got, err := s.ListUnread(ctx, "alice", MaxUnread+10, 0)
	if err != nil {
		t.Fatalf("ListUnread: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("unread len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].EventID != want[i].EventID {
			t.Fatalf("position %d = %s, want %s", i, got[i].EventID, want[i].EventID)
		}
		if !got[i].EventTime.Equal(want[i].EventTime) {
			t.Fatalf("position %d time = %v, want %v", i, got[i].EventTime, want[i].EventTime)
		}
	}
	mustUnreadCount(t, s, "alice", MaxUnread)

	// 分页遍历结果应与一次性取前 500 一致。
	var paged []Notification
	for off := 0; off < MaxUnread; off += 100 {
		page, err := s.ListUnread(ctx, "alice", 100, off)
		if err != nil {
			t.Fatalf("ListUnread page: %v", err)
		}
		paged = append(paged, page...)
	}
	if len(paged) != len(got) {
		t.Fatalf("paged len = %d, want %d", len(paged), len(got))
	}
	for i := range paged {
		if paged[i].EventID != got[i].EventID {
			t.Fatalf("paged position %d = %s, want %s", i, paged[i].EventID, got[i].EventID)
		}
	}

	// 其他收件人：各自 500 条未读（600 投 500 留）。
	for r := 0; r < extraReaders; r++ {
		mustUnreadCount(t, s, fmt.Sprintf("user-%d", r), MaxUnread)
	}
}
