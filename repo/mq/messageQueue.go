package mq

import (
	"context"
)

// PushByAliasMessageConsumerHandler alias 推送消息消费的实现
type PushByAliasMessageConsumerHandler func(ctx context.Context, message *PushMessage) error

// PushAllMessageConsumerHandler  广播推送消息消费的实现
type PushAllMessageConsumerHandler func(ctx context.Context, message *PushMessage) error

// PushByTopicMessageConsumerHandler topic推送消息消费的实现
type PushByTopicMessageConsumerHandler func(ctx context.Context, message *PushMessage) error

// 推送消息格式
type PushMessage struct {
	AppName string `json:"app_name"`
	Topic   string `json:"topic"`
	UserID  string `json:"user_id"`
	Message []byte `json:"message"`
}

// MessageQueue 异步队列
type MessageQueue interface {
	// 订阅alias 推送消息主题并开始消费
	StartPushByAliasMessageConsumerSync(ctx context.Context, handler PushByAliasMessageConsumerHandler)
	// 订阅广播推送消息主题并开始消费
	StartPushAllMessageConsumerSync(ctx context.Context, handler PushAllMessageConsumerHandler)
	// 订阅topic推送消息主题并开始消费
	StartPushByTopicMessageConsumerSync(ctx context.Context, handler PushByTopicMessageConsumerHandler)

	// 单播生产消息消费的实现
	PushByAliasMessageProducer(ctx context.Context, key string, message *PushMessage) error
	// 组播生产消息消费的实现
	PushByTopicMessageProducer(ctx context.Context, key string, message *PushMessage) error
	// 广播生产消息消费的实现
	PushAllMessageProducer(ctx context.Context, key string, message *PushMessage) error
}
