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

// 验收场景：多个 goroutine 同时向同一收件人投递大量通知（含重复事件编号），
// 同时穿插标记已读与重投。结束后的未读集合必须与按
// “事件时间倒序（同刻按事件编号倒序）、去重、上限 500 挤掉最旧”
// 独立推算的结果完全一致；全程未读数不得超过 500。
func TestConcurrentDeliveryMatchesOracle(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, nil, nil)

	const recipients = 8
	const eventsPerRecipient = MaxUnread + 100 // 600 条，其中 100 条会被挤出

	// 事件 idx：事件时间 = idx/2 秒（刻意制造同刻，走事件编号次序），
	// 编号零填充保证字典序 == 数值序。
	eventOf := func(idx int) Event {
		return Event{
			EventID:   fmt.Sprintf("e%04d", idx),
			EventTime: testBase.Add(time.Duration(idx/2) * time.Second),
			Payload:   fmt.Sprintf("payload-%d", idx),
		}
	}

	var stopChecker atomic.Bool
	var checkerWG sync.WaitGroup
	checkerWG.Add(1)
	go func() {
		defer checkerWG.Done()
		for !stopChecker.Load() {
			for r := 0; r < recipients; r++ {
				recipient := fmt.Sprintf("user-%d", r)
				n, err := s.UnreadCount(ctx, recipient)
				if err != nil {
					t.Errorf("unread count: %v", err)
					return
				}
				if n > MaxUnread {
					t.Errorf("recipient %s: unread count %d exceeds %d", recipient, n, MaxUnread)
					return
				}
				page, err := s.ListUnread(ctx, recipient, 50, nil)
				if err != nil {
					t.Errorf("list unread: %v", err)
					return
				}
				for i := 1; i < len(page.Items); i++ {
					prev, cur := page.Items[i-1], page.Items[i]
					if prev.EventTime.Before(cur.EventTime) ||
						(prev.EventTime.Equal(cur.EventTime) && prev.EventID < cur.EventID) {
						t.Errorf("recipient %s: unread list out of order at %d", recipient, i)
						return
					}
				}
			}
		}
	}()

	// 阶段一：并发投递，每条事件投两遍（重复事件编号）。
	var wg sync.WaitGroup
	for r := 0; r < recipients; r++ {
		r := r
		recipient := fmt.Sprintf("user-%d", r)
		order := rand.New(rand.NewSource(int64(r))).Perm(eventsPerRecipient * 2)
		wg.Add(1)
		go func() {
			defer wg.Done()
			const workers = 4
			var wwg sync.WaitGroup
			for w := 0; w < workers; w++ {
				w := w
				wwg.Add(1)
				go func() {
					defer wwg.Done()
					for i := w; i < len(order); i += workers {
						ev := eventOf(order[i] % eventsPerRecipient)
						ev.Recipient = recipient
						if err := s.Deliver(ctx, ev); err != nil {
							t.Errorf("deliver: %v", err)
							return
						}
					}
				}()
			}
			wwg.Wait()
		}()
	}
	wg.Wait()

	// 阶段二：并发标记已读（逐条 + 批量）与重复重投，彼此可交换，结果确定。
	cutoff := testBase.Add(100 * time.Second) // 事件时间 < 100s（idx < 200）的未读被批量标记
	for r := 0; r < recipients; r++ {
		r := r
		recipient := fmt.Sprintf("user-%d", r)
		wg.Add(1)
		go func() {
			defer wg.Done()
			var wwg sync.WaitGroup
			wwg.Add(3)
			go func() { // 逐条标记 idx%3==0
				defer wwg.Done()
				for idx := 0; idx < eventsPerRecipient; idx += 3 {
					if err := s.MarkRead(ctx, recipient, fmt.Sprintf("e%04d", idx)); err != nil {
						t.Errorf("mark read: %v", err)
						return
					}
				}
			}()
			go func() { // 批量标记
				defer wwg.Done()
				if _, err := s.MarkAllReadBefore(ctx, recipient, cutoff); err != nil {
					t.Errorf("mark all before: %v", err)
				}
			}()
			go func() { // 全部重投一遍，包括已被挤出的
				defer wwg.Done()
				for idx := 0; idx < eventsPerRecipient; idx++ {
					ev := eventOf(idx)
					ev.Recipient = recipient
					if err := s.Deliver(ctx, ev); err != nil {
						t.Errorf("redeliver: %v", err)
						return
					}
				}
			}()
			wwg.Wait()
		}()
	}
	wg.Wait()
	stopChecker.Store(true)
	checkerWG.Wait()

	// 推算 Oracle：去重后按（事件时间倒序、事件编号倒序）取前 500 为未读候选，
	// 其余为已溢出；候选中去掉被标记已读的集合即为最终未读。
	for r := 0; r < recipients; r++ {
		recipient := fmt.Sprintf("user-%d", r)

		idxs := make([]int, eventsPerRecipient)
		for i := range idxs {
			idxs[i] = i
		}
		sort.Slice(idxs, func(a, b int) bool {
			ea, eb := eventOf(idxs[a]), eventOf(idxs[b])
			if !ea.EventTime.Equal(eb.EventTime) {
				return ea.EventTime.After(eb.EventTime)
			}
			return ea.EventID > eb.EventID
		})

		wantUnread := map[string]bool{}
		wantOverflowed := map[string]bool{}
		for rank, idx := range idxs {
			id := fmt.Sprintf("e%04d", idx)
			if rank >= MaxUnread {
				wantOverflowed[id] = true
				continue
			}
			read := idx%3 == 0 || eventOf(idx).EventTime.Before(cutoff)
			if !read {
				wantUnread[id] = true
			}
		}

		// 未读集合与顺序完全一致。
		gotIDs := unreadIDs(t, s, recipient)
		if len(gotIDs) != len(wantUnread) {
			t.Fatalf("recipient %s: unread size = %d, want %d", recipient, len(gotIDs), len(wantUnread))
		}
		for _, id := range gotIDs {
			if !wantUnread[id] {
				t.Fatalf("recipient %s: unexpected unread %s", recipient, id)
			}
		}
		wantOrder := make([]string, 0, len(wantUnread))
		for _, idx := range idxs {
			id := fmt.Sprintf("e%04d", idx)
			if wantUnread[id] {
				wantOrder = append(wantOrder, id)
			}
		}
		if len(gotIDs) != len(wantOrder) {
			t.Fatalf("recipient %s: unread size = %d, want %d", recipient, len(gotIDs), len(wantOrder))
		}
		for i := range wantOrder {
			if gotIDs[i] != wantOrder[i] {
				t.Fatalf("recipient %s: unread[%d] = %s, want %s", recipient, i, gotIDs[i], wantOrder[i])
			}
		}

		// 未读数 == 未读列表实际条数，且 <= 500。
		n, err := s.UnreadCount(ctx, recipient)
		if err != nil {
			t.Fatalf("unread count: %v", err)
		}
		if n != len(gotIDs) {
			t.Fatalf("recipient %s: unread count %d != list length %d", recipient, n, len(gotIDs))
		}
		if n > MaxUnread {
			t.Fatalf("recipient %s: unread count %d exceeds %d", recipient, n, MaxUnread)
		}

		// 被挤出的集合与推算一致，且重投后没有复活。
		for id := range wantOverflowed {
			d, err := s.GetDelivery(ctx, recipient, id)
			if err != nil || !d.Found {
				t.Fatalf("recipient %s: delivery %s: %+v, %v", recipient, id, d, err)
			}
			if d.Status != StatusOverflowed {
				t.Fatalf("recipient %s: %s status = %s, want overflowed", recipient, id, d.Status)
			}
		}

		// 总列表 = 全部 600 条（未读 + 已读 + 已溢出）。
		total := 0
		var cursor *Cursor
		for {
			page, err := s.ListNotifications(ctx, recipient, 100, cursor)
			if err != nil {
				t.Fatalf("list all: %v", err)
			}
			total += len(page.Items)
			if page.NextCursor == nil {
				break
			}
			cursor = page.NextCursor
		}
		if total != eventsPerRecipient {
			t.Fatalf("recipient %s: total list = %d, want %d", recipient, total, eventsPerRecipient)
		}
	}
}
