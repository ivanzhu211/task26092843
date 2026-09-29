package notify

import (
	"context"
	"database/sql"
	"time"
)

// UnreadCount 返回某个收件人当前的未读条数，恒等于 ListUnread 全量拉取的条数。
func (s *Store) UnreadCount(ctx context.Context, recipient string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM notifications WHERE recipient = ? AND status = 'unread'`,
		recipient).Scan(&n)
	return n, err
}

// ListUnread 按事件时间从新到旧（事件时间相同按事件编号倒序）返回未读通知。
// cursor 传 nil 取第一页；把返回的 Page.NextCursor 原样传回即可取下一页。
func (s *Store) ListUnread(ctx context.Context, recipient string, limit int, cursor *Cursor) (Page, error) {
	return s.list(ctx, recipient, limit, cursor, `AND status = 'unread'`)
}

// ListNotifications 返回收件箱总列表（未读、已读、已溢出），排序与分页同 ListUnread。
// 重试中与投递失败的通知不出现在任何列表里。
func (s *Store) ListNotifications(ctx context.Context, recipient string, limit int, cursor *Cursor) (Page, error) {
	return s.list(ctx, recipient, limit, cursor, `AND status IN ('unread', 'read', 'overflowed')`)
}

func (s *Store) list(ctx context.Context, recipient string, limit int, cursor *Cursor, filter string) (Page, error) {
	if limit <= 0 {
		limit = 50
	}
	q := `SELECT event_id, event_time_ns, payload, status, read_at_ns
		FROM notifications
		WHERE recipient = ? ` + filter
	args := []any{recipient}
	if cursor != nil {
		q += ` AND (event_time_ns < ? OR (event_time_ns = ? AND event_id < ?))`
		args = append(args, cursor.EventTime.UnixNano(), cursor.EventTime.UnixNano(), cursor.EventID)
	}
	q += ` ORDER BY event_time_ns DESC, event_id DESC LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return Page{}, err
	}
	defer rows.Close()

	page := Page{}
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return Page{}, err
		}
		if len(page.Items) == limit {
			last := page.Items[len(page.Items)-1]
			page.NextCursor = &Cursor{EventTime: last.EventTime, EventID: last.EventID}
			return page, rows.Err()
		}
		page.Items = append(page.Items, n)
	}
	return page, rows.Err()
}

func scanNotification(rows *sql.Rows) (Notification, error) {
	var n Notification
	var eventTimeNs int64
	var status string
	var readAtNs sql.NullInt64
	err := rows.Scan(&n.EventID, &eventTimeNs, &n.Payload, &status, &readAtNs)
	if err != nil {
		return n, err
	}
	n.EventTime = time.Unix(0, eventTimeNs)
	n.Status = Status(status)
	if readAtNs.Valid {
		t := time.Unix(0, readAtNs.Int64)
		n.ReadAt = &t
	}
	return n, nil
}

// GetDelivery 查询某个事件在某个收件人名下的投递状态。
func (s *Store) GetDelivery(ctx context.Context, recipient, eventID string) (Delivery, error) {
	d := Delivery{Recipient: recipient, EventID: eventID}
	var status string
	err := s.db.QueryRowContext(ctx,
		`SELECT status, retry_count FROM notifications WHERE recipient = ? AND event_id = ?`,
		recipient, eventID).Scan(&status, &d.RetryCount)
	if err == sql.ErrNoRows {
		return d, nil
	}
	if err != nil {
		return d, err
	}
	d.Status = Status(status)
	d.Found = true
	return d, nil
}
