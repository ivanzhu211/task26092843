# notify — 站内通知投递库

面向文档流转服务的 Go 库：文档分派事件到达时投递站内通知，
数据持久化在本地 SQLite 文件中。无接口层，直接以库形式被上层调用。

## 功能

- **幂等投递**：同一收件人名下同一事件编号只落地一次，重复投递 / 重试不产生重复通知。
- **未读上限**：每个收件人最多 500 条未读；超出时最旧的未读自动转为「已溢出」，
  不计未读、不进未读列表，但总列表可查。已读不占名额。
- **状态不可逆**：已溢出 / 已读的通知不会因重投而回到未读。
- **已读标记**：支持逐条标记与按事件时间批量标记，记录已读时间，重复标记幂等。
- **失败重试**：投递失败进入重试队列，第 n 次重试间隔 2^n 秒，最多 5 次，
  之后标记为「投递失败」。重试中的通知不出现在任何列表。
  重试计时由调用方驱动（传入当前时间），库本身不启动任何后台计时器。
- **排序**：未读列表按上游事件时间从新到旧排列，事件时间相同按事件编号倒序。

## 用法

```go
s, err := notify.Open("/var/lib/myapp/notify.db")
if err != nil { ... }
defer s.Close()

// 投递（gate 判断收件人是否可投递，如账户停用则返回 error）
err = s.Deliver(ctx, notify.Delivery{
    RecipientID: "user-123",
    EventID:     "assign-evt-987",
    EventTime:   evtTime,
    Payload:     `{"doc":"INV-2026-001"}`,
}, gate)

// 由调用方定期驱动到期重试（虚拟时钟，无需真实等待）
n, _ := s.ProcessRetries(ctx, time.Now().Unix(), gate)

// 收件箱
count, _ := s.UnreadCount(ctx, "user-123")
page, _ := s.ListUnread(ctx, "user-123", 20, 0)
s.MarkRead(ctx, "user-123", "assign-evt-987", time.Now().Unix())
s.MarkAllReadBefore(ctx, "user-123", cutoffUnix, time.Now().Unix())
info, _ := s.DeliveryStatus(ctx, "user-123", "assign-evt-987")
```

所有方法可并发调用；不同收件人的操作互不长等
（WAL 模式 + 短事务 + busy 重试）。

## 测试

```sh
go test ./...        # 全部测试
go test -race ./...  # 含并发模型对账测试
```

测试覆盖：并发投递 + 去重 + 溢出 + 标记已读的模型对账、
溢出后重投不复活、重试 5 次耗尽与中途成功落位、
全程未读数 ≤ 500 且与列表一致、百万历史下的查询性能。
