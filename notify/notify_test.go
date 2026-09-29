package notify

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var testBase = time.Unix(1_700_000_000, 0)

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func ev(recipient, id string, sec int64) Event {
	return Event{Recipient: recipient, EventID: id, EventTime: testBase.Add(time.Duration(sec) * time.Second), Payload: "payload-" + id}
}

func newStore(t *testing.T, gate Gate, clock *testClock) *Store {
	t.Helper()
	if clock == nil {
		clock = &testClock{t: testBase}
	}
	opts := []Option{WithClock(clock.now)}
	if gate != nil {
		opts = append(opts, WithGate(gate))
	}
	s, err := Open(filepath.Join(t.TempDir(), "notify.db"), opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func unreadIDs(t *testing.T, s *Store, recipient string) []string {
	t.Helper()
	var ids []string
	var cursor *Cursor
	for {
		page, err := s.ListUnread(context.Background(), recipient, 100, cursor)
		if err != nil {
			t.Fatalf("list unread: %v", err)
		}
		for _, n := range page.Items {
			ids = append(ids, n.EventID)
		}
		if page.NextCursor == nil {
			return ids
		}
		cursor = page.NextCursor
	}
}

// 重复投递与重试不会制造第二条通知；已读/已溢出后重投不会复活。
func TestDedupAndNoResurrect(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, nil, nil)

	for i := 0; i < 5; i++ {
		if err := s.Deliver(ctx, ev("alice", "e1", 1)); err != nil {
			t.Fatalf("deliver: %v", err)
		}
	}
	if n, _ := s.UnreadCount(ctx, "alice"); n != 1 {
		t.Fatalf("unread = %d, want 1", n)
	}

	// 已读之后重投：不复活。
	if err := s.MarkRead(ctx, "alice", "e1"); err != nil {
		t.Fatalf("mark read: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := s.Deliver(ctx, ev("alice", "e1", 1)); err != nil {
			t.Fatalf("redeliver: %v", err)
		}
	}
	if n, _ := s.UnreadCount(ctx, "alice"); n != 0 {
		t.Fatalf("unread after read+redeliver = %d, want 0", n)
	}
	d, _ := s.GetDelivery(ctx, "alice", "e1")
	if !d.Found || d.Status != StatusRead {
		t.Fatalf("delivery = %+v, want read", d)
	}

	// 溢出之后重投：不复活。
	for i := 0; i < MaxUnread+1; i++ {
		if err := s.Deliver(ctx, ev("alice", evID(i), int64(100+i))); err != nil {
			t.Fatalf("deliver %d: %v", i, err)
		}
	}
	d, _ = s.GetDelivery(ctx, "alice", evID(0))
	if d.Status != StatusOverflowed {
		t.Fatalf("oldest status = %s, want overflowed", d.Status)
	}
	if err := s.Deliver(ctx, ev("alice", evID(0), 100)); err != nil {
		t.Fatalf("redeliver overflowed: %v", err)
	}
	d, _ = s.GetDelivery(ctx, "alice", evID(0))
	if d.Status != StatusOverflowed {
		t.Fatalf("overflowed resurrected to %s", d.Status)
	}
	if n, _ := s.UnreadCount(ctx, "alice"); n != MaxUnread {
		t.Fatalf("unread = %d, want %d", n, MaxUnread)
	}
}

func evID(i int) string {
	return "ev-" + string(rune('a'+i/26)) + string(rune('a'+i%26)) + "-" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [8]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

// 第 501 条未读挤掉最旧未读；已读不占名额；总列表仍查得到已溢出。
func TestOverflowEviction(t *testing.T) {
	ctx := context.Background()
	s := newStore(t, nil, nil)

	// 先投递 10 条并全部标记已读：不应占用 500 的名额。
	for i := 0; i < 10; i++ {
		if err := s.Deliver(ctx, ev("bob", evID(i), int64(i))); err != nil {
			t.Fatalf("deliver: %v", err)
		}
	}
	if _, err := s.MarkAllReadBefore(ctx, "bob", testBase.Add(100*time.Second)); err != nil {
		t.Fatalf("mark all: %v", err)
	}
	if n, _ := s.UnreadCount(ctx, "bob"); n != 0 {
		t.Fatalf("unread = %d, want 0", n)
	}

	// 再投递 501 条：最早的一条未读被挤出。
	for i := 0; i < MaxUnread+1; i++ {
		if err := s.Deliver(ctx, ev("bob", "n"+itoa(i), int64(1000+i))); err != nil {
			t.Fatalf("deliver: %v", err)
		}
	}
	if n, _ := s.UnreadCount(ctx, "bob"); n != MaxUnread {
		t.Fatalf("unread = %d, want %d", n, MaxUnread)
	}
	d, _ := s.GetDelivery(ctx, "bob", "n0")
	if d.Status != StatusOverflowed {
		t.Fatalf("n0 status = %s, want overflowed", d.Status)
	}

	// 已溢出不出现在未读列表，但总列表查得到。
	ids := unreadIDs(t, s, "bob")
	for _, id := range ids {
		if id == "n0" {
			t.Fatal("overflowed notification appeared in unread list")
		}
	}
	found := false
	var cursor *Cursor
	for {
		page, err := s.ListNotifications(ctx, "bob", 100, cursor)
		if err != nil {
			t.Fatalf("list all: %v", err)
		}
		for _, n := range page.Items {
			if n.EventID == "n0" && n.Status == StatusOverflowed {
				found = true
			}
		}
		if page.NextCursor == nil {
			break
		}
		cursor = page.NextCursor
	}
	if !found {
		t.Fatal("overflowed notification missing from total list")
	}
}

// 逐条与批量标记已读：记录已读时间，重复标记幂等。
func TestMarkRead(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{t: testBase}
	s := newStore(t, nil, clock)

	for i := 0; i < 5; i++ {
		if err := s.Deliver(ctx, ev("carol", evID(i), int64(i*10))); err != nil {
			t.Fatalf("deliver: %v", err)
		}
	}
	clock.add(time.Hour)
	readAt := clock.now()

	if err := s.MarkRead(ctx, "carol", evID(1)); err != nil {
		t.Fatalf("mark read: %v", err)
	}
	// 重复标记同一条、标记不存在的、标记已读过的：都不报错。
	for _, id := range []string{evID(1), "no-such", evID(1)} {
		if err := s.MarkRead(ctx, "carol", id); err != nil {
			t.Fatalf("idempotent mark read %s: %v", id, err)
		}
	}
	if n, _ := s.UnreadCount(ctx, "carol"); n != 4 {
		t.Fatalf("unread = %d, want 4", n)
	}

	// 批量：标记事件时间早于 25 秒的全部已读（ev0、ev2 命中，ev1 已读过不重复计）。
	marked, err := s.MarkAllReadBefore(ctx, "carol", testBase.Add(25*time.Second))
	if err != nil {
		t.Fatalf("mark all before: %v", err)
	}
	if marked != 2 {
		t.Fatalf("marked = %d, want 2", marked)
	}
	if n, _ := s.UnreadCount(ctx, "carol"); n != 2 {
		t.Fatalf("unread = %d, want 2", n)
	}

	// 已读时间被记录。
	page, err := s.ListNotifications(ctx, "carol", 10, nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, n := range page.Items {
		if n.EventID == evID(1) {
			if n.ReadAt == nil || !n.ReadAt.Equal(readAt) {
				t.Fatalf("read at = %v, want %v", n.ReadAt, readAt)
			}
		}
	}
}

// 重试走满 5 次后标记为投递失败；间隔为 2^n 秒；重试中不出现在任何列表。
func TestRetryExhaustion(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{t: testBase}
	gateErr := errors.New("account disabled")
	s := newStore(t, func(context.Context, string) error { return gateErr }, clock)

	if err := s.Deliver(ctx, ev("dave", "e1", 1)); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	d, _ := s.GetDelivery(ctx, "dave", "e1")
	if !d.Found || d.Status != StatusRetrying || d.RetryCount != 0 {
		t.Fatalf("delivery = %+v, want retrying(0)", d)
	}
	if n, _ := s.UnreadCount(ctx, "dave"); n != 0 {
		t.Fatalf("retrying notification visible in unread")
	}
	if ids := unreadIDs(t, s, "dave"); len(ids) != 0 {
		t.Fatalf("retrying notification in unread list: %v", ids)
	}
	page, _ := s.ListNotifications(ctx, "dave", 10, nil)
	if len(page.Items) != 0 {
		t.Fatalf("retrying notification in total list")
	}

	// 未到期不处理。
	if n, err := s.ProcessDueRetries(ctx, clock.now().Add(time.Second)); err != nil || n != 0 {
		t.Fatalf("early process = %d, %v; want 0", n, err)
	}

	// 依次走 5 次重试，间隔 2,4,8,16,32 秒。
	for k := 1; k <= MaxRetries; k++ {
		clock.add(retryDelay(k))
		n, err := s.ProcessDueRetries(ctx, clock.now())
		if err != nil || n != 1 {
			t.Fatalf("retry %d: processed = %d, %v; want 1", k, n, err)
		}
		d, _ := s.GetDelivery(ctx, "dave", "e1")
		if k < MaxRetries {
			if d.Status != StatusRetrying || d.RetryCount != k {
				t.Fatalf("after retry %d: %+v, want retrying(%d)", k, d, k)
			}
		} else if d.Status != StatusFailed || d.RetryCount != MaxRetries {
			t.Fatalf("after final retry: %+v, want failed(%d)", d, MaxRetries)
		}
	}

	// 失败后不再重试，重投也是无操作。
	clock.add(time.Hour)
	if n, _ := s.ProcessDueRetries(ctx, clock.now()); n != 0 {
		t.Fatalf("processed after failure = %d, want 0", n)
	}
	if err := s.Deliver(ctx, ev("dave", "e1", 1)); err != nil {
		t.Fatalf("redeliver failed: %v", err)
	}
	d, _ = s.GetDelivery(ctx, "dave", "e1")
	if d.Status != StatusFailed {
		t.Fatalf("status = %s, want failed", d.Status)
	}
}

// 中途某次重试成功：以原事件时间出现在未读列表的正确位置。
func TestRetrySuccessKeepsEventTimePosition(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{t: testBase}
	var fail atomic.Bool
	s := newStore(t, func(context.Context, string) error {
		if fail.Load() {
			return errors.New("blocked")
		}
		return nil
	}, clock)

	// 三条正常通知，事件时间 100/200/300。
	for _, sec := range []int64{100, 200, 300} {
		if err := s.Deliver(ctx, ev("erin", "t"+itoa(int(sec)), sec)); err != nil {
			t.Fatalf("deliver: %v", err)
		}
	}
	// 一条事件时间为 150 的通知投递失败，进入重试。
	fail.Store(true)
	if err := s.Deliver(ctx, ev("erin", "late", 150)); err != nil {
		t.Fatalf("deliver late: %v", err)
	}
	if n, _ := s.UnreadCount(ctx, "erin"); n != 3 {
		t.Fatalf("unread = %d, want 3", n)
	}

	// 第一次重试仍失败，第二次成功。
	clock.add(retryDelay(1))
	if n, _ := s.ProcessDueRetries(ctx, clock.now()); n != 1 {
		t.Fatalf("retry 1 processed = %d, want 1", n)
	}
	fail.Store(false)
	clock.add(retryDelay(2))
	if n, _ := s.ProcessDueRetries(ctx, clock.now()); n != 1 {
		t.Fatalf("retry 2 processed = %d, want 1", n)
	}

	ids := unreadIDs(t, s, "erin")
	want := []string{"t300", "t200", "late", "t100"} // 按事件时间倒序
	if len(ids) != len(want) {
		t.Fatalf("unread ids = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("unread ids = %v, want %v", ids, want)
		}
	}
}
