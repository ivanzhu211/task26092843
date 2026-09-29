# notify — 站内通知投递与收件箱库

纯库（无接口层），由文档流转服务直接以 Go 调用，数据落在本地 SQLite 文件。
对并发调用安全；不同收件人的处理互不阻塞（按收件人加锁）。

## 用法

```go
s, _ := notify.Open("/var/lib/docflow/notify.db",
    notify.WithGate(func(ctx context.Context, recipient string) error {
        return accountDisabledErrOrNil(recipient) // 返回非 nil 则本次投递进入重试
    }),
)
defer s.Close()

// 投递（幂等：同一收件人 + 同一事件编号只落地一次）
s.Deliver(ctx, notify.Event{Recipient: "u1", EventID: "assign-123", EventTime: ts, Payload: "..."})

// 收件箱
n, _      := s.UnreadCount(ctx, "u1")
page, _   := s.ListUnread(ctx, "u1", 50, nil)        // page.NextCursor 传回翻页
all, _    := s.ListNotifications(ctx, "u1", 50, nil) // 总列表（未读/已读/已溢出）
d, _      := s.GetDelivery(ctx, "u1", "assign-123")  // 某事件的投递状态

// 标记已读（幂等，记录已读时间）
s.MarkRead(ctx, "u1", "assign-123")
s.MarkAllReadBefore(ctx, "u1", time.Now())

// 重试由调用方驱动：定时调用，传入当前时间
s.ProcessDueRetries(ctx, time.Now())
```

## 行为规则

- 去重：`UNIQUE(recipient, event_id)`，重复投递/重试投递一律无操作；
  已读或已溢出的通知不会因重投复活为未读。
- 未读上限 500：第 501 条未读到达时，按事件时间最旧的未读变为“已溢出”，
  总列表可查，未读列表不再出现；已读不占名额。
- 重试：投递失败进入重试，第 n 次重试间隔 2^n 秒，满 5 次标记“投递失败”。
  重试中的通知不出现在任何列表；成功后以**原事件时间**进入未读的正确位置。
- 排序：未读列表按事件时间倒序，同刻按事件编号倒序；游标分页。

## 实现要点

- 表 `notifications`：`UNIQUE(recipient, event_id)`；
  索引 `(recipient, status, event_time DESC, event_id DESC)` 支撑未读数/未读列表首屏，
  部分索引 `(next_retry_at) WHERE status='retrying'` 支撑到期扫描。
- 未读数用 `COUNT(*)`：未读恒 ≤500，走索引，与历史总量无关。
- 每个收件人一把互斥锁（`sync.Map`），投递/已读/重试在锁内 + 事务内完成，
  溢出挤压与新通知落库在同一事务，任何时刻未读数 ≤500 且与未读列表一致。
- 单 SQLite 连接（WAL），写入在库内串行，避免 `SQLITE_BUSY`。

## 测试

```sh
go test ./...        # 单元 + 并发验收
go test -race ./...  # 竞态检测
```

并发验收：8 个收件人 × 600 事件 × 2 遍重复投递，多 goroutine 穿插逐条/批量
标记已读与全量重投，结束后未读集合与独立推算的 Oracle 逐条比对一致。
