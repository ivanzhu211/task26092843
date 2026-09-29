package notify

import (
	"context"
	"database/sql"
	"time"
)

// ProcessDueRetries 处理所有在 now 之前到期的重试，由调用方按自己的节奏驱动。
// 返回本次实际尝试的重试条数。每条到期的通知重新走一次闸门：
// 成功则以原事件时间落为未读（可能挤掉别人的最旧未读）；
// 失败则按 2^n 秒安排下一次重试，重试满 MaxRetries 次后标记为投递失败。
func (s *Store) ProcessDueRetries(ctx context.Context, now time.Time) (int, error) {
	type due struct {
		recipient string
		eventID   string
	}
	rows, err := s.db.QueryContext(ctx, `SELECT recipient, event_id FROM notifications
		WHERE status = 'retrying' AND next_retry_at_ns <= ?
		ORDER BY next_retry_at_ns`, now.UnixNano())
	if err != nil {
		return 0, err
	}
	var dues []due
	for rows.Next() {
		var d due
		if err := rows.Scan(&d.recipient, &d.eventID); err != nil {
			rows.Close()
			return 0, err
		}
		dues = append(dues, d)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	attempted := 0
	for _, d := range dues {
		ok, err := s.retryOne(ctx, d.recipient, d.eventID, now)
		if err != nil {
			return attempted, err
		}
		if ok {
			attempted++
		}
	}
	return attempted, nil
}

// retryOne 处理一条到期重试；返回是否真的进行了尝试。
// 行状态可能在扫描后被并发改变，需在锁内重新确认。
func (s *Store) retryOne(ctx context.Context, recipient, eventID string, now time.Time) (bool, error) {
	mu := s.lock(recipient)
	mu.Lock()
	defer mu.Unlock()

	var retryCount int
	var nextRetryAt int64
	err := s.db.QueryRowContext(ctx, `SELECT retry_count, next_retry_at_ns FROM notifications
		WHERE recipient = ? AND event_id = ? AND status = 'retrying'`,
		recipient, eventID).Scan(&retryCount, &nextRetryAt)
	if err == sql.ErrNoRows {
		return false, nil // 已被并发处理
	}
	if err != nil {
		return false, err
	}
	if nextRetryAt > now.UnixNano() {
		return false, nil // 尚未到期（被并发重排过）
	}

	if err := s.gate(ctx, recipient); err != nil {
		retryCount++
		if retryCount >= MaxRetries {
			_, err = s.db.ExecContext(ctx, `UPDATE notifications
				SET status = 'failed', retry_count = ?, next_retry_at_ns = NULL
				WHERE recipient = ? AND event_id = ?`,
				retryCount, recipient, eventID)
			return true, err
		}
		next := now.Add(retryDelay(retryCount + 1)).UnixNano()
		_, err = s.db.ExecContext(ctx, `UPDATE notifications
			SET retry_count = ?, next_retry_at_ns = ?
			WHERE recipient = ? AND event_id = ?`,
			retryCount, next, recipient, eventID)
		return true, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE notifications
		SET status = 'unread', next_retry_at_ns = NULL
		WHERE recipient = ? AND event_id = ?`,
		recipient, eventID); err != nil {
		return false, err
	}
	if err := evictOverflow(ctx, tx, recipient); err != nil {
		return false, err
	}
	return true, tx.Commit()
}
