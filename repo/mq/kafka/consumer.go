package kafka

import (
	"context"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/trpc-group/trpc-go/log"
)

// NOTE:
// Do not move the code below to a goroutine.
// The `ConsumeClaim` itself is called within a goroutine, see:
// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
type ConsumerMessageHandler func(ctx context.Context, message *sarama.ConsumerMessage) error

// KafkaConsumerConfig consumer config of kafka
type KafkaConsumerConfig struct {
	User       string
	Password   string
	ClientID   string
	GroupID    string
	BrokerAddr string
	Assignor   string

	SASLEnable bool

	Topic   string
	Clients int
}

// KafkaConsumer consumer group
type KafkaConsumer struct {
	client sarama.ConsumerGroup
}

// NewKafkaConsumer create a KafkaConsumer
func NewKafkaConsumer(
	cfg *KafkaConsumerConfig,
) (*KafkaConsumer, error) {
	conf := sarama.NewConfig()
	conf.Version = sarama.V1_1_1_0
	conf.ClientID = cfg.ClientID
	conf.Metadata.Full = true

	if cfg.SASLEnable {
		conf.Net.SASL.Enable = true
		conf.Net.SASL.User = cfg.User
		conf.Net.SASL.Password = cfg.Password
		conf.Net.SASL.Handshake = true
		conf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
		}
		conf.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
	}

	conf.Consumer.Offsets.Initial = sarama.OffsetNewest

	switch cfg.Assignor {
	case "Sticky":
		conf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "RoundRobin":
		conf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "Range":
		fallthrough
	default:
		conf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	}

	err := conf.Validate()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	client, err := sarama.NewConsumerGroup(
		[]string{cfg.BrokerAddr},
		cfg.GroupID,
		conf,
	)

	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &KafkaConsumer{
		client: client,
	}, nil
}

// StartConsume
func (c *KafkaConsumer) StartConsume(
	ctx context.Context,
	topics []string,
	handler ConsumerMessageHandler,
) error {
	log.Infof("KafkaConsumer.StartConsume|Consume Loop|topics=%#v", topics)

	consumer := Consumer{
		ready:   make(chan bool),
		handler: handler,
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := c.client.Consume(ctx, topics, &consumer); err != nil {
				// 只打印错误，不退出循环
				// TODO: 增加上报
				log.Errorf(
					"kafka StartConsume|err=%+v|",
					err,
				)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready
	select {
	case <-ctx.Done():
		log.Info("terminating: context cancelled")
	}

	wg.Wait()
	if err := ctx.Err(); err != nil {
		log.Errorf("kafka StartConsume Error Context|err=%v|", err)
		return errors.WithStack(err)
	}

	if err := c.client.Close(); err != nil {
		log.Errorf("kafka StartConsume Error closing client|err=%v|", err)
		return errors.WithStack(err)
	}
	return nil
}

// 实现 ConsumerGroupHandler 接口
type Consumer struct {
	ready   chan bool
	handler ConsumerMessageHandler
}

// Setup 实现 Setup 接口
func (c *Consumer) Setup(s sarama.ConsumerGroupSession) error {
	log.Infof(
		"kafka.consumer Setup|MemberID=%s|GenerationID=%d|",
		s.MemberID(),
		s.GenerationID(),
	)
	close(c.ready)
	return nil
}

// Cleanup 实现 Cleanup 接口
func (c *Consumer) Cleanup(s sarama.ConsumerGroupSession) error {
	log.Infof(
		"kafka.consumer Cleanup|MemberID=%s|GenerationID=%d|",
		s.MemberID(),
		s.GenerationID(),
	)
	return nil
}

// ConsumeClaim 实现 ConsumeClaim 接口
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		log.Debugf(
			"kafka.consumer ConsumeClaim|GenerationID=%d|Topic=%s|Partition=%d|InitialOffset=%d|HighWaterMarkOffset=%d|",
			session.GenerationID(),
			claim.Topic(),
			claim.Partition(),
			claim.InitialOffset(),
			claim.HighWaterMarkOffset(),
		)
		err := c.handler(session.Context(), message)
		if err != nil {
			// 处理失败，打日志，但不会 MarkMessage
			log.Errorf(
				"kafka.consumer ConsumeClaim|GenerationID=%d|Topic=%s|Partition=%d|InitialOffset=%d|HighWaterMarkOffset=%d|err=%#v|",
				session.GenerationID(),
				claim.Topic(),
				claim.Partition(),
				claim.InitialOffset(),
				claim.HighWaterMarkOffset(),
				err,
			)
		} else {
			// 处理成功，标志为已消费
			session.MarkMessage(message, "")
		}
	}

	return nil
}
