package notify

import (
	"context"
	"errors"
	"time"
)

// InboxStatus 是通知在收件箱中的状态。
type InboxStatus string

const (
	InboxUnread     InboxStatus = "unread"
	InboxRead       InboxStatus = "read"
	InboxOverflowed InboxStatus = "overflowed"
)

// DeliveryState 是一条投递记录的状态。
type DeliveryState string

const (
	DeliveryPending   DeliveryState = "pending"   // 等待重试
	DeliveryDelivered DeliveryState = "delivered" // 已落地收件箱
	DeliveryFailed    DeliveryState = "failed"    // 重试耗尽，投递失败
)

const (
	// MaxUnread 是每个收件人未读通知的上限。
	MaxUnread = 500
	// MaxRetries 是投递失败后的最大重试次数。
	MaxRetries = 5
)

// ErrNotFound 表示查询的事件不存在。
var ErrNotFound = errors.New("notify: not found")

// Delivery 是上游发起一次投递的输入。
type Delivery struct {
	RecipientID string
	EventID     string
	EventTime   time.Time
	Payload     string
}

// Gate 由上层提供，用于判断某收件人当前是否可投递
// （例如账户被停用时返回错误）。返回 nil 表示可投递。
type Gate func(ctx context.Context, recipientID string) error

// Notification 是收件箱中的一条通知。
type Notification struct {
	RecipientID string
	EventID     string
	EventTime   time.Time
	Payload     string
	Status      InboxStatus
	ReadAt      *time.Time
	CreatedAt   time.Time
}

// DeliveryInfo 描述某个事件的投递状态。
type DeliveryInfo struct {
	RecipientID string
	EventID     string
	State       DeliveryState
	Attempts    int // 已执行的失败尝试次数（含首次）
	NextRetryAt *time.Time
	Inbox       *InboxStatus // 已落地时为收件箱状态，否则为 nil
}

func unixToTime(sec int64) time.Time {
	return time.Unix(sec, 0).UTC()
}
