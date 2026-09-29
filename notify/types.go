// Package notify 提供基于 SQLite 的站内通知投递与收件箱管理。
package notify

import "time"

// Status 表示一条通知在其生命周期中的状态。
type Status string

const (
	// StatusUnread 未读：投递成功，等待用户阅读。
	StatusUnread Status = "unread"
	// StatusRead 已读。
	StatusRead Status = "read"
	// StatusOverflowed 已溢出：未读超过上限时被挤掉的最旧未读。
	StatusOverflowed Status = "overflowed"
	// StatusRetrying 投递重试中：成功之前不出现在任何收件人列表里。
	StatusRetrying Status = "retrying"
	// StatusFailed 投递失败：重试次数耗尽，不再重试。
	StatusFailed Status = "failed"
)

// MaxUnread 每个收件人未读通知的最大条数。
const MaxUnread = 500

// MaxRetries 投递失败后的最大重试次数。
const MaxRetries = 5

// Event 是上游文档流转服务投递过来的分派事件。
type Event struct {
	Recipient string    // 收件人标识
	EventID   string    // 上游分配的事件编号（同一收件人名下唯一）
	EventTime time.Time // 上游给出的事件发生时间
	Payload   string    // 通知内容
}

// Notification 是收件箱中一条已落地的通知。
type Notification struct {
	Recipient string
	EventID   string
	EventTime time.Time
	Payload   string
	Status    Status
	ReadAt    *time.Time // 已读时间，未读时为 nil
}

// Delivery 描述某个事件的投递状态。
type Delivery struct {
	Recipient  string
	EventID    string
	Status     Status
	RetryCount int  // 已进行的重试次数
	Found      bool // 该事件是否已落地
}

// Cursor 是未读列表的分页游标，由 ListUnread 返回、原样传回。
type Cursor struct {
	EventTime time.Time
	EventID   string
}

// Page 是一页未读通知。
type Page struct {
	Items      []Notification
	NextCursor *Cursor // 为 nil 表示没有更多
}
