package notify

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// retryDelay 返回第 n 次重试的间隔：2 的 n 次方秒（n 从 1 开始）。
func retryDelay(attempts int) int64 {
	return int64(1) << uint(attempts)
}

// Deliver 处理一次投递。gate 为 nil 或返回 nil 时直接落地收件箱；
// 否则进入重试队列。同一 (收件人, 事件编号) 只会落地一次，
// 重复投递（包括已读、已溢出、投递失败后的重投）都是幂等空操作。
func (s *Store) Deliver(ctx context.Context, d Delivery, gate Gate) error {
	return s.deliverAt(ctx, d, gate, time.Now().Unix())
}

func (s *Store) deliverAt(ctx context.Context, d Delivery, gate Gate, nowUnix int64) error {
	return withBusyRetry(ctx, func() error {
		return s.deliverOnce(ctx, d, gate, nowUnix)
	})
}

func (s *Store) deliverOnce(ctx context.Context, d Delivery, gate Gate, nowUnix int64) error {
	deliverable := true
	if gate != nil {
		deliverable = gate(ctx, d.RecipientID) == nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	state := string(DeliveryDelivered)
	var nextRetryAt sql.NullInt64
	attempts := 0
	if !deliverable {
		state = string(DeliveryPending)
		attempts = 1
		nextRetryAt = sql.NullInt64{Int64: nowUnix + retryDelay(1), Valid: true}
	}

	res, err := tx.ExecContext(ctx, `
		INSERT INTO deliveries
			(recipient_id, event_id, event_time, payload, state, attempts, next_retry_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (recipient_id, event_id) DO NOTHING`,
		d.RecipientID, d.EventID, d.EventTime.Unix(), d.Payload,
		state, attempts, nextRetryAt, nowUnix, nowUnix)
	if err != nil {
		return fmt.Errorf("insert delivery: %w", err)
	}
	inserted, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("insert delivery rows: %w", err)
	}
	if inserted == 0 {
		// 同一事件已投递过（无论处于何种状态），幂等返回。
		return commitTx(tx, "dedup")
	}

	if deliverable {
		if err := landTx(ctx, tx, d); err != nil {
			return err
		}
	}
	return commitTx(tx, "deliver")
}

// landTx 在事务内把通知写入收件箱并挤出超出上限的最旧未读。
// 调用前必须已确认该 (收件人, 事件编号) 的投递记录由本事务创建。
func landTx(ctx context.Context, tx *sql.Tx, d Delivery) error {
	res, err := tx.ExecContext(ctx, `
		INSERT INTO notifications (recipient_id, event_id, event_time, payload, status, created_at)
		VALUES (?, ?, ?, ?, 'unread', ?)
		ON CONFLICT (recipient_id, event_id) DO NOTHING`,
		d.RecipientID, d.EventID, d.EventTime.Unix(), d.Payload, time.Now().Unix())
	if err != nil {
		return fmt.Errorf("insert notification: %w", err)
	}
	inserted, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("insert notification rows: %w", err)
	}
	if inserted == 0 {
		return nil
	}
	// 未读超过 MaxUnread 时，把排序末尾（最旧）的多余未读标记为已溢出。
	_, err = tx.ExecContext(ctx, `
		UPDATE notifications SET status = 'overflowed'
		WHERE recipient_id = ? AND status = 'unread' AND id IN (
			SELECT id FROM notifications
			WHERE recipient_id = ? AND status = 'unread'
			ORDER BY event_time DESC, event_id DESC
			LIMIT -1 OFFSET ?
		)`, d.RecipientID, d.RecipientID, MaxUnread)
	if err != nil {
		return fmt.Errorf("trim overflow: %w", err)
	}
	return nil
}

// commitTx 提交事务，并给提交阶段的错误附上标签。
func commitTx(tx *sql.Tx, label string) error {
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit %s: %w", label, err)
	}
	return nil
}
