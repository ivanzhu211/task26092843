package notify

import (
	"context"
	"time"
)

// MarkRead 把一条通知标记为已读并记录已读时间。
// 对已读、已溢出、重试中或不存在的通知重复调用，结果不变，也不报错。
func (s *Store) MarkRead(ctx context.Context, recipient, eventID string) error {
	mu := s.lock(recipient)
	mu.Lock()
	defer mu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE notifications
		SET status = 'read', read_at_ns = ?
		WHERE recipient = ? AND event_id = ? AND status = 'unread'`,
		s.now().UnixNano(), recipient, eventID)
	return err
}

// MarkAllReadBefore 把某个收件人事件时间早于 t 的全部未读通知标记为已读，
// 已读时间统一记为当前时间。返回新标记为已读的条数。
func (s *Store) MarkAllReadBefore(ctx context.Context, recipient string, t time.Time) (int64, error) {
	mu := s.lock(recipient)
	mu.Lock()
	defer mu.Unlock()
	res, err := s.db.ExecContext(ctx, `UPDATE notifications
		SET status = 'read', read_at_ns = ?
		WHERE recipient = ? AND status = 'unread' AND event_time_ns < ?`,
		s.now().UnixNano(), recipient, t.UnixNano())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
