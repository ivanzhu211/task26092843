package notify

import (
	"context"
	"database/sql"
	"time"
)

// MarkRead 将某收件人的一条通知标记为已读，记录已读时间。
// 对已读或已溢出的通知重复调用是幂等空操作，不报错。
func (s *Store) MarkRead(ctx context.Context, recipientID, eventID string, readAtUnix int64) error {
	return withBusyRetry(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `
			UPDATE notifications SET status = 'read', read_at = ?
			WHERE recipient_id = ? AND event_id = ? AND status = 'unread'`,
			readAtUnix, recipientID, eventID)
		return err
	})
}

// MarkAllReadBefore 将某收件人事件时间不晚于 cutoffUnix 的
// 全部未读通知标记为已读，返回标记条数。幂等。
func (s *Store) MarkAllReadBefore(ctx context.Context, recipientID string, cutoffUnix, readAtUnix int64) (int, error) {
	var n int
	err := withBusyRetry(ctx, func() error {
		res, err := s.db.ExecContext(ctx, `
			UPDATE notifications SET status = 'read', read_at = ?
			WHERE recipient_id = ? AND status = 'unread' AND event_time <= ?`,
			readAtUnix, recipientID, cutoffUnix)
		if err != nil {
			return err
		}
		affected, err := res.RowsAffected()
		n = int(affected)
		return err
	})
	return n, err
}

// UnreadCount 返回某收件人当前未读数，保证 <= MaxUnread。
func (s *Store) UnreadCount(ctx context.Context, recipientID string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM notifications
		WHERE recipient_id = ? AND status = 'unread'`, recipientID).Scan(&n)
	return n, err
}

// ListUnread 按事件时间从新到旧（相同则按事件编号倒序）分页返回未读通知。
func (s *Store) ListUnread(ctx context.Context, recipientID string, limit, offset int) ([]Notification, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT recipient_id, event_id, event_time, payload, status, read_at, created_at
		FROM notifications
		WHERE recipient_id = ? AND status = 'unread'
		ORDER BY event_time DESC, event_id DESC
		LIMIT ? OFFSET ?`, recipientID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNotifications(rows)
}

// UnreadSnapshot 在同一事务快照内返回未读数与全部未读列表，
// 两者保证一致（用于对账、测试等场景）。
func (s *Store) UnreadSnapshot(ctx context.Context, recipientID string) (int, []Notification, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback()

	var count int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM notifications
		WHERE recipient_id = ? AND status = 'unread'`, recipientID).Scan(&count); err != nil {
		return 0, nil, err
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT recipient_id, event_id, event_time, payload, status, read_at, created_at
		FROM notifications
		WHERE recipient_id = ? AND status = 'unread'
		ORDER BY event_time DESC, event_id DESC`, recipientID)
	if err != nil {
		return 0, nil, err
	}
	list, err := scanNotifications(rows)
	if err != nil {
		return 0, nil, err
	}
	return count, list, tx.Commit()
}

// DeliveryStatus 查询某个事件的投递状态；事件不存在时返回 ErrNotFound。
func (s *Store) DeliveryStatus(ctx context.Context, recipientID, eventID string) (DeliveryInfo, error) {
	info := DeliveryInfo{RecipientID: recipientID, EventID: eventID}
	var nextRetryAt sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
		SELECT state, attempts, next_retry_at FROM deliveries
		WHERE recipient_id = ? AND event_id = ?`, recipientID, eventID).
		Scan(&info.State, &info.Attempts, &nextRetryAt)
	if err == sql.ErrNoRows {
		return info, ErrNotFound
	}
	if err != nil {
		return info, err
	}
	if nextRetryAt.Valid {
		t := unixToTime(nextRetryAt.Int64)
		info.NextRetryAt = &t
	}
	if info.State == DeliveryDelivered {
		var status string
		err := s.db.QueryRowContext(ctx, `
			SELECT status FROM notifications
			WHERE recipient_id = ? AND event_id = ?`, recipientID, eventID).Scan(&status)
		if err != nil && err != sql.ErrNoRows {
			return info, err
		}
		if err == nil {
			st := InboxStatus(status)
			info.Inbox = &st
		}
	}
	return info, nil
}

func scanNotifications(rows *sql.Rows) ([]Notification, error) {
	defer rows.Close()
	var out []Notification
	for rows.Next() {
		var n Notification
		var eventTime, createdAt int64
		var readAt sql.NullInt64
		var status string
		if err := rows.Scan(&n.RecipientID, &n.EventID, &eventTime, &n.Payload, &status, &readAt, &createdAt); err != nil {
			return nil, err
		}
		n.EventTime = unixToTime(eventTime)
		n.CreatedAt = unixToTime(createdAt)
		n.Status = InboxStatus(status)
		if readAt.Valid {
			t := time.Unix(readAt.Int64, 0).UTC()
			n.ReadAt = &t
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// ListAll 按事件时间从新到旧分页返回某收件人的全部通知
// （含未读、已读、已溢出）。
func (s *Store) ListAll(ctx context.Context, recipientID string, limit, offset int) ([]Notification, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT recipient_id, event_id, event_time, payload, status, read_at, created_at
		FROM notifications
		WHERE recipient_id = ?
		ORDER BY event_time DESC, event_id DESC
		LIMIT ? OFFSET ?`, recipientID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNotifications(rows)
}
