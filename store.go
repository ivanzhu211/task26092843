package notify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Store 是通知库的入口，由上层文档流转服务持有。
// 所有方法均可被多个 goroutine 并发调用。
type Store struct {
	db *sql.DB
}

const schema = `
CREATE TABLE IF NOT EXISTS deliveries (
	recipient_id  TEXT    NOT NULL,
	event_id      TEXT    NOT NULL,
	event_time    INTEGER NOT NULL,
	payload       TEXT    NOT NULL DEFAULT '',
	state         TEXT    NOT NULL,          -- pending / delivered / failed
	attempts      INTEGER NOT NULL DEFAULT 0, -- 已失败次数（含首次尝试）
	next_retry_at INTEGER,                    -- 下次重试时间（unix 秒），pending 时有效
	created_at    INTEGER NOT NULL,
	updated_at    INTEGER NOT NULL,
	PRIMARY KEY (recipient_id, event_id)
);
CREATE INDEX IF NOT EXISTS idx_deliveries_retry
	ON deliveries (state, next_retry_at);

CREATE TABLE IF NOT EXISTS notifications (
	id           INTEGER PRIMARY KEY AUTOINCREMENT,
	recipient_id TEXT    NOT NULL,
	event_id     TEXT    NOT NULL,
	event_time   INTEGER NOT NULL,
	payload      TEXT    NOT NULL DEFAULT '',
	status       TEXT    NOT NULL DEFAULT 'unread', -- unread / read / overflowed
	read_at      INTEGER,
	created_at   INTEGER NOT NULL,
	UNIQUE (recipient_id, event_id)
);
CREATE INDEX IF NOT EXISTS idx_notifications_unread
	ON notifications (recipient_id, status, event_time DESC, event_id DESC);
`

// Open 打开（必要时创建）位于 path 的 SQLite 通知库。
// 传 ":memory:" 可得到纯内存库（仅建议测试使用）。
func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// SQLite 同一时刻只允许一个写事务；写事务都很短，
	// 配合 busy_timeout 即可让不同收件人的并发操作互不长等。
	db.SetMaxOpenConns(8)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("notify: init schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Close 关闭底层数据库。
func (s *Store) Close() error {
	return s.db.Close()
}

// isBusy 判断错误是否为 SQLITE_BUSY（数据库被锁）。
func isBusy(err error) bool {
	var se interface{ Code() int }
	if errors.As(err, &se) {
		return se.Code() == 5 // SQLITE_BUSY
	}
	return false
}

// withBusyRetry 在遭遇 SQLITE_BUSY 时以递增退避重试操作。
// 库内所有写操作都是幂等或事务性的，整体重试是安全的。
func withBusyRetry(ctx context.Context, op func() error) error {
	var err error
	for attempt := 0; attempt < 100; attempt++ {
		if err = op(); err == nil || !isBusy(err) {
			return err
		}
		backoff := time.Duration(attempt+1) * 2 * time.Millisecond
		if backoff > 100*time.Millisecond {
			backoff = 100 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return err
}
