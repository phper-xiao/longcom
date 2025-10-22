package kafka

import (
	"context"
	"encoding/json"
	"fmt"

    "github.com/tylerxiao/longcom/repo/mq"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/pquerna/ffjson/ffjson"
	"github.com/trpc-group/trpc-go/log"
)

// SalmonPushByAliasTopicName push by alias topic name
var SalmonPushByAliasTopicName = "push_alias_topic"

// SalmonPushAllTopicName push all topic name
var SalmonPushAllTopicName = "push_all_topic"

// SalmonPushByTopicTopicName push by topic topic name
var SalmonPushByTopicTopicName = "push_topic_topic"

// KafkaMessageQueueConfig kafka message queue config.
type KafkaMessageQueueConfig struct {
	PushByAliasTopicConsumerConfig KafkaConsumerConfig
	PushAllTopicConsumerConfig     KafkaConsumerConfig
	PushByTopicTopicConsumerConfig KafkaConsumerConfig

	PushByAliasTopicProducerConfig KafkaProducerConfig
	PushAllTopicProducerConfig     KafkaProducerConfig
	PushByTopicTopicProducerConfig KafkaProducerConfig
}

// KafkaMessageQueueConfig queue of the kafka.
type KafkaMessageQueue struct {
	pushByAliasTopicConsumer []*KafkaConsumer
	pushAllTopicConsumer     []*KafkaConsumer
	pushByTopicTopicConsumer []*KafkaConsumer

	pushByAliasTopicProducer *KafkaProducer
	pushAllTopicProducer     *KafkaProducer
	pushByTopicTopicProducer *KafkaProducer
}

// NewKafkaMessageQueue create KafkaMessageQueue with config.
func NewKafkaMessageQueue(
	ctx context.Context,
	cfg *KafkaMessageQueueConfig,
	nodeID string,
) (*KafkaMessageQueue, error) {
	// 对于 Consumer, GroupID 使用节点ID
	// 这样子每个 broker 节点都可以收到topic中的所有消息
	cfg.PushByAliasTopicConsumerConfig.ClientID = "alias_" + nodeID
	cfg.PushByAliasTopicConsumerConfig.GroupID = "alias_" + nodeID
	cfg.PushByAliasTopicConsumerConfig.Clients = 3
	if len(cfg.PushByAliasTopicConsumerConfig.Topic) > 0 {
		SalmonPushByAliasTopicName = cfg.PushByAliasTopicConsumerConfig.Topic
	}
	pushByAliasTopicConsumer, err := newKafkaConsumers(&cfg.PushByAliasTopicConsumerConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	cfg.PushAllTopicConsumerConfig.ClientID = "all_" + nodeID
	cfg.PushAllTopicConsumerConfig.GroupID = "all_" + nodeID
	cfg.PushAllTopicConsumerConfig.Clients = 1
	if len(cfg.PushAllTopicConsumerConfig.Topic) > 0 {
		SalmonPushAllTopicName = cfg.PushAllTopicConsumerConfig.Topic
	}
	pushAllConsumer, err := newKafkaConsumers(&cfg.PushAllTopicConsumerConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	cfg.PushByTopicTopicConsumerConfig.ClientID = "topic_" + nodeID
	cfg.PushByTopicTopicConsumerConfig.GroupID = "topic_" + nodeID
	cfg.PushByTopicTopicConsumerConfig.Clients = 1
	if len(cfg.PushByTopicTopicConsumerConfig.Topic) > 0 {
		SalmonPushByTopicTopicName = cfg.PushByTopicTopicConsumerConfig.Topic
	}
	pushByTopicTopicConsumer, err := newKafkaConsumers(&cfg.PushByTopicTopicConsumerConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// new producer
	cfg.PushByAliasTopicProducerConfig.ClientID = nodeID
	cfg.PushByAliasTopicProducerConfig.Topic = SalmonPushByAliasTopicName
	pushByAliasTopicProducer, err := NewKafkaProducer(&cfg.PushByAliasTopicProducerConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	cfg.PushByTopicTopicProducerConfig.ClientID = nodeID
	cfg.PushByTopicTopicProducerConfig.Topic = SalmonPushByTopicTopicName
	pushByTopicTopicProducer, err := NewKafkaProducer(&cfg.PushByTopicTopicProducerConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	cfg.PushAllTopicProducerConfig.ClientID = nodeID
	cfg.PushAllTopicProducerConfig.Topic = SalmonPushAllTopicName
	pushAllTopicProducer, err := NewKafkaProducer(&cfg.PushAllTopicProducerConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &KafkaMessageQueue{
		pushByAliasTopicConsumer: pushByAliasTopicConsumer,
		pushAllTopicConsumer:     pushAllConsumer,
		pushByTopicTopicConsumer: pushByTopicTopicConsumer,

		pushByAliasTopicProducer: pushByAliasTopicProducer,
		pushByTopicTopicProducer: pushByTopicTopicProducer,
		pushAllTopicProducer:     pushAllTopicProducer,
	}, nil
}

func newKafkaConsumers(cfg *KafkaConsumerConfig) ([]*KafkaConsumer, error) {
	var consumers []*KafkaConsumer
	for i := 0; i < cfg.Clients; i++ {
		cfg.ClientID = fmt.Sprintf("%s_%d", cfg.ClientID, i)
		consumer, err := NewKafkaConsumer(cfg)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		consumers = append(consumers, consumer)
	}

	return consumers, nil
}

// StartPushByAliasMessageConsumerSync implements MessageQueue interface.
func (q *KafkaMessageQueue) StartPushByAliasMessageConsumerSync(
	ctx context.Context,
	handler mq.PushByAliasMessageConsumerHandler,
) {
	log.InfoContextf(
		ctx,
		"StartPushByAliasMessageConsumerSync|topic=%s|",
		SalmonPushByAliasTopicName,
	)
	for _, consumer := range q.pushByAliasTopicConsumer {
		go consumer.StartConsume(
			ctx,
			[]string{SalmonPushByAliasTopicName},
			func(ctx context.Context, message *sarama.ConsumerMessage) error {
				log.DebugContextf(
					ctx,
					"KafkaMessageComsumerHandler|topic=%s|key=%s|partition=%d|offset=%d|",
					message.Topic,
					string(message.Key),
					message.Partition,
					message.Offset,
				)
				var pushAliasMessage mq.PushMessage
				err := json.Unmarshal(message.Value, &pushAliasMessage)
				if err != nil {
					log.WarnContextf(
						ctx,
						"StartPushByAliasMessageConsumerSync|Unmarshal err=%+v|",
						err,
					)
					return nil
				}

				err = handler(ctx, &pushAliasMessage)
				if err != nil {
					// 这里忽略处理异常，否则会卡kafka队列中消息
					log.WarnContextf(
						ctx,
						"StartPushByAliasMessageConsumerSync|err=%+v|",
						err,
					)
				}

				return nil
			},
		)
	}
}

// StartPushAllMessageConsumerSync implements MessageQueue interface.
func (q *KafkaMessageQueue) StartPushAllMessageConsumerSync(
	ctx context.Context,
	handler mq.PushAllMessageConsumerHandler,
) {
	for _, consumer := range q.pushAllTopicConsumer {
		go consumer.StartConsume(
			ctx,
			[]string{SalmonPushAllTopicName},
			func(ctx context.Context, message *sarama.ConsumerMessage) error {
				log.DebugContextf(
					ctx,
					"KafkaMessageComsumerHandler|topic=%s|key=%s|partition=%d|offset=%d|",
					message.Topic,
					string(message.Key),
					message.Partition,
					message.Offset,
				)
				var pushAliasMessage mq.PushMessage
				err := json.Unmarshal(message.Value, &pushAliasMessage)
				if err != nil {
					return errors.WithStack(err)
				}

				err = handler(ctx, &pushAliasMessage)
				if err != nil {
					// 这里忽略处理异常，否则会卡kafka队列中消息
					log.WarnContextf(
						ctx,
						"StartPushAllMessageConsumerSync|err=%+v|",
						err,
					)
				}

				return nil
			},
		)
	}
}

// StartPushByTopicMessageConsumerSync implements MessageQueue interface.
func (q *KafkaMessageQueue) StartPushByTopicMessageConsumerSync(ctx context.Context,
	handler mq.PushByTopicMessageConsumerHandler) {
	for _, consumer := range q.pushByTopicTopicConsumer {
		go consumer.StartConsume(
			ctx,
			[]string{SalmonPushByTopicTopicName},
			func(ctx context.Context, message *sarama.ConsumerMessage) error {
				log.DebugContextf(ctx, "StartPushByTopicMessageConsumerSync|topic=%s|key=%s|partition=%d|offset=%d|val=%s",
					message.Topic, string(message.Key), message.Partition, message.Offset, string(message.Value))
				pushAliasMessage := new(mq.PushMessage)
				err := json.Unmarshal(message.Value, pushAliasMessage)
				if err != nil {
					log.ErrorContextf(ctx, "StartPushByTopicMessageConsumerSync|err: %v", err)
					return errors.WithStack(err)
				}

				err = handler(ctx, pushAliasMessage)
				if err != nil {
					log.WarnContextf(ctx, "StartPushByTopicMessageConsumerSync|err: %v|msg: %v", err, pushAliasMessage)
				}
				return nil
			},
		)
	}
}

// PushByAliasMessageProducer implements MessageQueue interface.
func (q *KafkaMessageQueue) PushByAliasMessageProducer(ctx context.Context, key string, message *mq.PushMessage) error {
	messageByte, err := ffjson.Marshal(message)
	if err != nil {
		log.ErrorContextf(ctx, "PushByAliasMessageProducer|kafka Produce(%s, %s, %s) error", message.AppName, message.Topic, err)
		return err
	}

	err = q.pushByAliasTopicProducer.SendBytesMsg(ctx, SalmonPushByAliasTopicName, key, messageByte)
	if err != nil {
		log.ErrorContextf(ctx, "PushByAliasMessageProducer|kafka Produce(%s, %s, %s, %d) error", message.AppName, message.Topic, err, len(messageByte))
		return err
	}
	return nil
}

// PushByTopicMessageProducer implements MessageQueue interface.
func (q *KafkaMessageQueue) PushByTopicMessageProducer(ctx context.Context, key string, message *mq.PushMessage) error {
	messageByte, err := ffjson.Marshal(message)
	if err != nil {
		log.ErrorContextf(ctx, "PushByTopicMessageProducer|kafka Produce(%s, %s, %s) error", message.AppName, message.Topic, err)
		return err
	}

	err = q.pushByAliasTopicProducer.SendBytesMsg(ctx, SalmonPushByTopicTopicName, key, messageByte)
	if err != nil {
		log.ErrorContextf(ctx, "PushByTopicMessageProducer|kafka Produce(%s, %s, %s) error", message.AppName, message.Topic, err)
		return err
	}
	return nil
}

// PushAllMessageProducer implements MessageQueue interface.
func (q *KafkaMessageQueue) PushAllMessageProducer(ctx context.Context, key string, message *mq.PushMessage) error {
	messageByte, err := ffjson.Marshal(message)
	if err != nil {
		log.ErrorContextf(ctx, "PushAllMessageProducer|kafka Produce(%s, %s, %s) error", message.AppName, message.Topic, err)
		return err
	}

	err = q.pushByAliasTopicProducer.SendBytesMsg(ctx, SalmonPushAllTopicName, key, messageByte)
	if err != nil {
		log.ErrorContextf(ctx, "PushAllMessageProducer|kafka Produce(%s, %s, %s) error", message.AppName, message.Topic, err)
		return err
	}
	return nil
}
