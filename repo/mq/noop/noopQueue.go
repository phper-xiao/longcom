package noop

import (
    "context"

    "github.com/tylerxiao/longcom/repo/mq"
)

// NoopMessageQueue 空实现
type NoopMessageQueue struct {
}

// NewNoopMessageQueue create an NoopMessageQueue
func NewNoopMessageQueue() (*NoopMessageQueue, error) {
	return &NoopMessageQueue{}, nil
}

// StartPushByAliasMessageConsumerSync implements MessageQueue  interface.
func (q *NoopMessageQueue) StartPushByAliasMessageConsumerSync(
	ctx context.Context,
	handler mq.PushByAliasMessageConsumerHandler,
) {
	select {}
}

// StartPushAllMessageConsumerSync implements MessageQueue  interface.
func (q *NoopMessageQueue) StartPushAllMessageConsumerSync(
	ctx context.Context,
	handler mq.PushAllMessageConsumerHandler,
) {
}

// StartPushByTopicMessageConsumerSync implements MessageQueue interface.
func (q *NoopMessageQueue) StartPushByTopicMessageConsumerSync(ctx context.Context,
	handler mq.PushByTopicMessageConsumerHandler) {
}
