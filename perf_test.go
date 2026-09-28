package notify

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestQueryPerfWithHistory 验证：收件人积累 100 万条历史通知后，
// 未读数与未读列表首页仍然即时返回。
func TestQueryPerfWithHistory(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	// 直接批量造 100 万条已读历史 + 500 条未读。
	const history = 1_000_000
	_, err := s.db.Exec(`
		WITH RECURSIVE seq(x) AS (
			SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < ?
		)
		INSERT INTO notifications (recipient_id, event_id, event_time, payload, status, read_at, created_at)
		SELECT 'heavy', 'hist-' || x, 1600000000 + x, '', 'read', 1600000100 + x, 1600000100 + x
		FROM seq`, history)
	if err != nil {
		t.Fatalf("seed history: %v", err)
	}
	_, err = s.db.Exec(`
		WITH RECURSIVE seq(x) AS (
			SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < ?
		)
		INSERT INTO notifications (recipient_id, event_id, event_time, payload, status, created_at)
		SELECT 'heavy', 'unread-' || printf('%04d', x), 1700000000 + x, '', 'unread', 1700000100 + x
		FROM seq`, MaxUnread)
	if err != nil {
		t.Fatalf("seed unread: %v", err)
	}

	const limit = 500 * time.Millisecond

	start := time.Now()
	count, err := s.UnreadCount(ctx, "heavy")
	if err != nil {
		t.Fatalf("UnreadCount: %v", err)
	}
	if d := time.Since(start); d > limit {
		t.Fatalf("UnreadCount took %v with %d history rows", d, history)
	} else {
		t.Logf("UnreadCount: %v", d)
	}
	if count != MaxUnread {
		t.Fatalf("count = %d, want %d", count, MaxUnread)
	}

	start = time.Now()
	page, err := s.ListUnread(ctx, "heavy", 20, 0)
	if err != nil {
		t.Fatalf("ListUnread: %v", err)
	}
	if d := time.Since(start); d > limit {
		t.Fatalf("ListUnread first page took %v with %d history rows", d, history)
	} else {
		t.Logf("ListUnread first page: %v", d)
	}
	if len(page) != 20 {
		t.Fatalf("page len = %d, want 20", len(page))
	}
	// 首页第一条应是最新未读。
	if page[0].EventID != fmt.Sprintf("unread-%04d", MaxUnread) {
		t.Fatalf("first unread = %s", page[0].EventID)
	}
}
