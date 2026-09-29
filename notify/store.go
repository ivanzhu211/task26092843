package notify

import (
	"context"
	"database/sql"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Gate 在投递（含重试）落库为未读之前被调用。
// 返回非 nil error 表示本次投递失败（例如账户被停用），将进入重试。
type Gate func(ctx context.Context, recipient string) error

// Option 用于配置 Store。
type Option func(*Store)

// WithGate 设置投递闸门，默认总是成功。
func WithGate(g Gate) Option {
	return func(s *Store) { s.gate = g }
}

// WithClock 替换内部时钟，主要用于测试。
func WithClock(c func() time.Time) Option {
	return func(s *Store) { s.now = c }
}

// Store 是通知库的主入口，对并发调用安全。
type Store struct {
	db    *sql.DB
	gate  Gate
	now   func() time.Time
	locks sync.Map // recipient -> *sync.Mutex，按收件人串行化，互不影响
}

// Open 打开（必要时创建）位于 path 的 SQLite 通知库。
func Open(path string, opts ...Option) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// 单连接串行化写入，避免 SQLITE_BUSY；应用层按收件人加锁，
	// 不同收件人的处理逻辑互不阻塞，连接只是短暂的 IO 资源。
	db.SetMaxOpenConns(1)
	s := &Store{db: db, gate: func(context.Context, string) error { return nil }, now: time.Now}
	for _, o := range opts {
		o(s)
	}
	if err := s.init(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close 关闭底层数据库。
func (s *Store) Close() error { return s.db.Close() }

// init 建表与索引。
func (s *Store) init(ctx context.Context) error {
	stmts := []string{
		`PRAGMA journal_mode = WAL`,
		`PRAGMA synchronous = NORMAL`,
		`CREATE TABLE IF NOT EXISTS notifications (
			id              INTEGER PRIMARY KEY AUTOINCREMENT,
			recipient       TEXT    NOT NULL,
			event_id        TEXT    NOT NULL,
			event_time_ns   INTEGER NOT NULL,
			payload         TEXT    NOT NULL,
			status          TEXT    NOT NULL,
			retry_count     INTEGER NOT NULL DEFAULT 0,
			next_retry_at_ns INTEGER,
			read_at_ns      INTEGER,
			created_at_ns   INTEGER NOT NULL,
			UNIQUE (recipient, event_id)
		)`,
		// 未读数 / 未读列表：按收件人+状态过滤，按事件时间倒序。
		`CREATE INDEX IF NOT EXISTS idx_inbox
			ON notifications (recipient, status, event_time_ns DESC, event_id DESC)`,
		// 到期重试扫描。
		`CREATE INDEX IF NOT EXISTS idx_retry
			ON notifications (next_retry_at_ns) WHERE status = 'retrying'`,
	}
	for _, q := range stmts {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// lock 返回某个收件人的互斥锁。
func (s *Store) lock(recipient string) *sync.Mutex {
	v, _ := s.locks.LoadOrStore(recipient, &sync.Mutex{})
	return v.(*sync.Mutex)
}
