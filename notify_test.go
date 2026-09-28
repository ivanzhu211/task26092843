package notify

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "notify.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func event(i int) Delivery {
	return Delivery{
		RecipientID: "alice",
		EventID:     fmt.Sprintf("evt-%04d", i),
		EventTime:   time.Unix(1_700_000_000+int64(i), 0),
		Payload:     fmt.Sprintf("payload-%d", i),
	}
}

func mustUnreadCount(t *testing.T, s *Store, recipient string, want int) {
	t.Helper()
	got, err := s.UnreadCount(context.Background(), recipient)
	if err != nil {
		t.Fatalf("UnreadCount: %v", err)
	}
	if got != want {
		t.Fatalf("unread count = %d, want %d", got, want)
	}
}

func TestDeliverDedup(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := s.Deliver(ctx, event(1), nil); err != nil {
			t.Fatalf("Deliver: %v", err)
		}
	}
	mustUnreadCount(t, s, "alice", 1)

	all, err := s.ListAll(ctx, "alice", 100, 0)
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("total notifications = %d, want 1", len(all))
	}
}

func TestOverflowEvictsOldest(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	// 连续投递 MaxUnread+50 条，全程校验不变量。
	for i := 0; i < MaxUnread+50; i++ {
		if err := s.Deliver(ctx, event(i), nil); err != nil {
			t.Fatalf("Deliver %d: %v", i, err)
		}
		count, list, err := s.UnreadSnapshot(ctx, "alice")
		if err != nil {
			t.Fatalf("UnreadSnapshot: %v", err)
		}
		if count > MaxUnread {
			t.Fatalf("after %d deliveries: unread count %d > %d", i+1, count, MaxUnread)
		}
		if count != len(list) {
			t.Fatalf("after %d deliveries: count %d != list len %d", i+1, count, len(list))
		}
	}
	mustUnreadCount(t, s, "alice", MaxUnread)

	// 最旧的 50 条应被挤出，未读列表第一条是最新的。
	list, err := s.ListUnread(ctx, "alice", MaxUnread+10, 0)
	if err != nil {
		t.Fatalf("ListUnread: %v", err)
	}
	if len(list) != MaxUnread {
		t.Fatalf("unread list len = %d, want %d", len(list), MaxUnread)
	}
	if list[0].EventID != event(MaxUnread+49).EventID {
		t.Fatalf("newest unread = %s, want %s", list[0].EventID, event(MaxUnread+49).EventID)
	}
	if list[len(list)-1].EventID != event(50).EventID {
		t.Fatalf("oldest unread = %s, want %s", list[len(list)-1].EventID, event(50).EventID)
	}

	// 被挤掉的最早一条：状态为 overflowed，总列表可查。
	info, err := s.DeliveryStatus(ctx, "alice", event(0).EventID)
	if err != nil {
		t.Fatalf("DeliveryStatus: %v", err)
	}
	if info.State != DeliveryDelivered || info.Inbox == nil || *info.Inbox != InboxOverflowed {
		t.Fatalf("oldest event status = %+v, want delivered/overflowed", info)
	}
	all, err := s.ListAll(ctx, "alice", MaxUnread+100, 0)
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(all) != MaxUnread+50 {
		t.Fatalf("total list len = %d, want %d", len(all), MaxUnread+50)
	}
}

func TestRedeliverAfterOverflowOrRead(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	// 501 条，第 0 条被挤出。
	for i := 0; i <= MaxUnread; i++ {
		if err := s.Deliver(ctx, event(i), nil); err != nil {
			t.Fatalf("Deliver: %v", err)
		}
	}
	// 把最新的一条标记已读。
	if err := s.MarkRead(ctx, "alice", event(MaxUnread).EventID, 1_700_100_000); err != nil {
		t.Fatalf("MarkRead: %v", err)
	}
	mustUnreadCount(t, s, "alice", MaxUnread-1)

	// 重投被挤掉的第 0 条与已读的第 500 条：都不得回到未读。
	for _, i := range []int{0, MaxUnread} {
		if err := s.Deliver(ctx, event(i), nil); err != nil {
			t.Fatalf("redeliver %d: %v", i, err)
		}
	}
	mustUnreadCount(t, s, "alice", MaxUnread-1)

	info0, _ := s.DeliveryStatus(ctx, "alice", event(0).EventID)
	if *info0.Inbox != InboxOverflowed {
		t.Fatalf("event 0 inbox = %v, want overflowed", *info0.Inbox)
	}
	infoN, _ := s.DeliveryStatus(ctx, "alice", event(MaxUnread).EventID)
	if *infoN.Inbox != InboxRead {
		t.Fatalf("event %d inbox = %v, want read", MaxUnread, *infoN.Inbox)
	}
}

func TestMarkRead(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if err := s.Deliver(ctx, event(i), nil); err != nil {
			t.Fatalf("Deliver: %v", err)
		}
	}
	readAt := int64(1_700_200_000)
	if err := s.MarkRead(ctx, "alice", event(3).EventID, readAt); err != nil {
		t.Fatalf("MarkRead: %v", err)
	}
	mustUnreadCount(t, s, "alice", 9)

	// 重复标记：幂等，不报错，已读时间不变。
	if err := s.MarkRead(ctx, "alice", event(3).EventID, readAt+999); err != nil {
		t.Fatalf("re-MarkRead: %v", err)
	}
	all, err := s.ListAll(ctx, "alice", 100, 0)
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	for _, n := range all {
		if n.EventID == event(3).EventID {
			if n.Status != InboxRead || n.ReadAt == nil || n.ReadAt.Unix() != readAt {
				t.Fatalf("event 3 = %+v, want read at %d", n, readAt)
			}
		}
	}

	// 批量标记：事件时间 <= event(5) 的全部已读。
	n, err := s.MarkAllReadBefore(ctx, "alice", event(5).EventTime.Unix(), readAt)
	if err != nil {
		t.Fatalf("MarkAllReadBefore: %v", err)
	}
	if n != 5 { // 0,1,2,4,5（3 已读，不再计入）
		t.Fatalf("MarkAllReadBefore affected = %d, want 5", n)
	}
	mustUnreadCount(t, s, "alice", 4) // 剩 6,7,8,9

	// 对已溢出通知标记已读：不报错，状态不变。
	for i := 10; i <= MaxUnread+10; i++ {
		if err := s.Deliver(ctx, event(i), nil); err != nil {
			t.Fatalf("Deliver: %v", err)
		}
	}
	// 此时 event(6) 已被挤出（最旧）。
	info, _ := s.DeliveryStatus(ctx, "alice", event(6).EventID)
	if *info.Inbox != InboxOverflowed {
		t.Fatalf("event 6 inbox = %v, want overflowed", *info.Inbox)
	}
	if err := s.MarkRead(ctx, "alice", event(6).EventID, readAt); err != nil {
		t.Fatalf("MarkRead overflowed: %v", err)
	}
	info, _ = s.DeliveryStatus(ctx, "alice", event(6).EventID)
	if *info.Inbox != InboxOverflowed {
		t.Fatalf("event 6 inbox after MarkRead = %v, want still overflowed", *info.Inbox)
	}
}

func alwaysFail(context.Context, string) error { return errors.New("account disabled") }

func TestRetryExhaustion(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	t0 := int64(1_700_000_000)
	d := event(0)

	if err := s.deliverAt(ctx, d, alwaysFail, t0); err != nil {
		t.Fatalf("deliverAt: %v", err)
	}
	// 重试中的通知不出现在任何列表。
	mustUnreadCount(t, s, "alice", 0)
	if all, _ := s.ListAll(ctx, "alice", 10, 0); len(all) != 0 {
		t.Fatalf("pending notification visible in ListAll: %+v", all)
	}

	info, err := s.DeliveryStatus(ctx, "alice", d.EventID)
	if err != nil {
		t.Fatalf("DeliveryStatus: %v", err)
	}
	if info.State != DeliveryPending || info.Attempts != 1 {
		t.Fatalf("info = %+v, want pending/attempts=1", info)
	}
	if info.NextRetryAt == nil || info.NextRetryAt.Unix() != t0+2 {
		t.Fatalf("next retry = %v, want %d", info.NextRetryAt, t0+2)
	}

	// 未到期不处理。
	if n, _ := s.ProcessRetries(ctx, t0+1, alwaysFail); n != 0 {
		t.Fatalf("processed %d before due", n)
	}

	// 走满 5 次重试：间隔 2,4,8,16,32。
	now := t0
	for k := 1; k <= MaxRetries; k++ {
		now += int64(1) << uint(k)
		n, err := s.ProcessRetries(ctx, now, alwaysFail)
		if err != nil {
			t.Fatalf("ProcessRetries: %v", err)
		}
		if n != 1 {
			t.Fatalf("retry %d: processed %d, want 1", k, n)
		}
		info, _ := s.DeliveryStatus(ctx, "alice", d.EventID)
		if k < MaxRetries {
			wantNext := now + (int64(1) << uint(k+1))
			if info.State != DeliveryPending || info.Attempts != k+1 ||
				info.NextRetryAt == nil || info.NextRetryAt.Unix() != wantNext {
				t.Fatalf("retry %d: info = %+v, want pending/attempts=%d/next=%d",
					k, info, k+1, wantNext)
			}
		} else {
			if info.State != DeliveryFailed || info.Attempts != MaxRetries+1 || info.NextRetryAt != nil {
				t.Fatalf("final: info = %+v, want failed/attempts=%d", info, MaxRetries+1)
			}
		}
		mustUnreadCount(t, s, "alice", 0)
	}

	// 失败后不再重试，重投也是幂等空操作。
	if n, _ := s.ProcessRetries(ctx, now+10_000, alwaysFail); n != 0 {
		t.Fatalf("processed %d after failed", n)
	}
	if err := s.Deliver(ctx, d, nil); err != nil {
		t.Fatalf("redeliver: %v", err)
	}
	info, _ = s.DeliveryStatus(ctx, "alice", d.EventID)
	if info.State != DeliveryFailed {
		t.Fatalf("after redeliver state = %v, want failed", info.State)
	}
	mustUnreadCount(t, s, "alice", 0)
}

func TestRetrySuccessLandsAtEventTime(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	t0 := int64(1_700_000_000)

	// 三条正常通知，事件时间分别为 t0+100, t0+200, t0+300。
	for i, ts := range []int64{100, 200, 300} {
		d := event(i)
		d.EventTime = time.Unix(t0+ts, 0)
		if err := s.Deliver(ctx, d, nil); err != nil {
			t.Fatalf("Deliver: %v", err)
		}
	}
	// 一条事件时间为 t0+250 的通知投递失败，进入重试。
	d := event(99)
	d.EventTime = time.Unix(t0+250, 0)
	if err := s.deliverAt(ctx, d, alwaysFail, t0); err != nil {
		t.Fatalf("deliverAt: %v", err)
	}
	mustUnreadCount(t, s, "alice", 3)

	// 第一次重试仍失败，第二次成功。
	if n, _ := s.ProcessRetries(ctx, t0+2, alwaysFail); n != 1 {
		t.Fatalf("first retry processed = %d", n)
	}
	mustUnreadCount(t, s, "alice", 3)
	if n, err := s.ProcessRetries(ctx, t0+2+4, nil); err != nil || n != 1 {
		t.Fatalf("second retry = %d, %v", n, err)
	}

	// 以原事件时间落在正确位置：t0+300, t0+250, t0+200, t0+100。
	list, err := s.ListUnread(ctx, "alice", 10, 0)
	if err != nil {
		t.Fatalf("ListUnread: %v", err)
	}
	if len(list) != 4 {
		t.Fatalf("unread len = %d, want 4", len(list))
	}
	wantOrder := []string{event(2).EventID, event(99).EventID, event(1).EventID, event(0).EventID}
	for i, w := range wantOrder {
		if list[i].EventID != w {
			t.Fatalf("position %d = %s, want %s", i, list[i].EventID, w)
		}
	}
	if !list[1].EventTime.Equal(time.Unix(t0+250, 0)) {
		t.Fatalf("retried event time = %v, want %v", list[1].EventTime, t0+250)
	}
}
