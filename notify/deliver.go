package notify

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Deliver 处理一个上游分派事件。
//
// 同一收件人名下同一事件编号只落地一次：重复投递、重试投递都是无操作，
// 已被读取或已被溢出的通知不会因此重新变成未读。
// 若闸门判定本次投递失败，通知以“重试中”状态落库，成功之前不出现在任何列表里。
func (s *Store) Deliver(ctx context.Context, ev Event) error {
	if ev.Recipient == "" || ev.EventID == "" {
		return errors.New("notify: recipient and event id are required")
	}
	mu := s.lock(ev.Recipient)
	mu.Lock()
	defer mu.Unlock()

	status := StatusUnread
	var nextRetry *int64
	if err := s.gate(ctx, ev.Recipient); err != nil {
		status = StatusRetrying
		t := s.now().Add(retryDelay(1)).UnixNano()
		nextRetry = &t
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO notifications
		(recipient, event_id, event_time_ns, payload, status, retry_count, next_retry_at_ns, created_at_ns)
		VALUES (?, ?, ?, ?, ?, 0, ?, ?)`,
		ev.Recipient, ev.EventID, ev.EventTime.UnixNano(), ev.Payload,
		string(status), nextRetry, s.now().UnixNano())
	if err != nil {
		return err
	}
	inserted, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if inserted > 0 && status == StatusUnread {
		if err := evictOverflow(ctx, tx, ev.Recipient); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// retryDelay 返回第 n 次重试前的等待间隔：2^n 秒。
func retryDelay(n int) time.Duration { return time.Duration(1<<uint(n)) * time.Second }

// evictOverflow 把超出 MaxUnread 的最旧未读（按事件时间）标记为已溢出。
// 调用方必须已持有该收件人的锁，并在事务内调用。
func evictOverflow(ctx context.Context, tx *sql.Tx, recipient string) error {
	var unread int
	err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM notifications WHERE recipient = ? AND status = 'unread'`,
		recipient).Scan(&unread)
	if err != nil {
		return err
	}
	excess := unread - MaxUnread
	if excess <= 0 {
		return nil
	}
	_, err = tx.ExecContext(ctx, `UPDATE notifications SET status = 'overflowed'
		WHERE id IN (
			SELECT id FROM notifications
			WHERE recipient = ? AND status = 'unread'
			ORDER BY event_time_ns ASC, event_id ASC
			LIMIT ?
		)`, recipient, excess)
	return err
}
