package notify

import "context"

// ProcessRetries 由调用方驱动，处理 nowUnix 时刻到期的重试。
// 返回本次尝试的投递条数。重试成功则以原事件时间落入收件箱；
// 失败则按 2^n 秒顺延，重试满 MaxRetries 次后标记为投递失败。
func (s *Store) ProcessRetries(ctx context.Context, nowUnix int64, gate Gate) (int, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT recipient_id, event_id, event_time, payload
		FROM deliveries
		WHERE state = ? AND next_retry_at <= ?`,
		string(DeliveryPending), nowUnix)
	if err != nil {
		return 0, err
	}
	var due []Delivery
	for rows.Next() {
		var d Delivery
		var eventTime int64
		if err := rows.Scan(&d.RecipientID, &d.EventID, &eventTime, &d.Payload); err != nil {
			rows.Close()
			return 0, err
		}
		d.EventTime = unixToTime(eventTime)
		due = append(due, d)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	processed := 0
	for _, d := range due {
		ok, err := s.retryOne(ctx, d, gate, nowUnix)
		if err != nil {
			return processed, err
		}
		if ok {
			processed++
		}
	}
	return processed, nil
}

// retryOne 处理单条到期投递，返回是否确实由本次调用推进了状态。
func (s *Store) retryOne(ctx context.Context, d Delivery, gate Gate, nowUnix int64) (processed bool, err error) {
	err = withBusyRetry(ctx, func() error {
		var e error
		processed, e = s.retryOnce(ctx, d, gate, nowUnix)
		return e
	})
	return processed, err
}

func (s *Store) retryOnce(ctx context.Context, d Delivery, gate Gate, nowUnix int64) (bool, error) {
	deliverable := true
	if gate != nil {
		deliverable = gate(ctx, d.RecipientID) == nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	if deliverable {
		res, err := tx.ExecContext(ctx, `
			UPDATE deliveries
			SET state = ?, attempts = attempts + 1, next_retry_at = NULL, updated_at = ?
			WHERE recipient_id = ? AND event_id = ? AND state = ?`,
			string(DeliveryDelivered), nowUnix, d.RecipientID, d.EventID, string(DeliveryPending))
		if err != nil {
			return false, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return false, err
		}
		if n == 0 {
			return false, tx.Commit() // 已被并发调用处理
		}
		if err := landTx(ctx, tx, d); err != nil {
			return false, err
		}
		return true, tx.Commit()
	}

	// 仍然失败：占用本条记录并累计失败次数。
	res, err := tx.ExecContext(ctx, `
		UPDATE deliveries
		SET attempts = attempts + 1, updated_at = ?
		WHERE recipient_id = ? AND event_id = ? AND state = ?`,
		nowUnix, d.RecipientID, d.EventID, string(DeliveryPending))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, tx.Commit()
	}

	var attempts int
	err = tx.QueryRowContext(ctx, `
		SELECT attempts FROM deliveries WHERE recipient_id = ? AND event_id = ?`,
		d.RecipientID, d.EventID).Scan(&attempts)
	if err != nil {
		return false, err
	}

	if attempts > MaxRetries {
		_, err = tx.ExecContext(ctx, `
			UPDATE deliveries
			SET state = ?, next_retry_at = NULL, updated_at = ?
			WHERE recipient_id = ? AND event_id = ?`,
			string(DeliveryFailed), nowUnix, d.RecipientID, d.EventID)
	} else {
		_, err = tx.ExecContext(ctx, `
			UPDATE deliveries
			SET next_retry_at = ?, updated_at = ?
			WHERE recipient_id = ? AND event_id = ?`,
			nowUnix+retryDelay(attempts), nowUnix, d.RecipientID, d.EventID)
	}
	if err != nil {
		return false, err
	}
	return true, tx.Commit()
}
