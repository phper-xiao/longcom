package kafka

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"trpc.group/trpc-go/trpc-go/log"
)

// KafkaProducerConfig kafka 生产者配置
type KafkaProducerConfig struct {
	User        string
	Password    string
	ClientID    string
	BrokerAddr  string
	Partitioner string
	MaxRetry    int
	KeepAlive   int

	SASLEnable bool

	Topic string
}

// KafkaProducer kafka 生产者
type KafkaProducer struct {
	sarama.SyncProducer
}

// NewKafkaProducer creates a new KafkaProducer using the given broker addresses and configuration
func NewKafkaProducer(
	cfg *KafkaProducerConfig,
) (*KafkaProducer, error) {
	conf := sarama.NewConfig()
	conf.Producer.Retry.Max = cfg.MaxRetry
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Successes = true
	conf.Version = sarama.V1_1_1_0
	conf.ClientID = cfg.ClientID
	conf.Metadata.Full = true
	conf.Producer.MaxMessageBytes = 10000000

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

	conf.Net.KeepAlive = time.Duration(cfg.KeepAlive) * time.Millisecond // in case that connection broke pipeline

	// 根据配置，使用不同的分区策略
	switch cfg.Partitioner {
	case "Hash":
		// 选择使用基于key hash的分区器
		conf.Producer.Partitioner = sarama.NewHashPartitioner
	case "RoundRobin":
		// 轮
		conf.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	case "Random":
		fallthrough
	default:
		// 默认使用随机的
		conf.Producer.Partitioner = sarama.NewRandomPartitioner
	}

	err := conf.Validate()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// 初始化 kafka producer
	syncProducer, err := sarama.NewSyncProducer([]string{cfg.BrokerAddr}, conf)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &KafkaProducer{
		syncProducer,
	}, nil
}

// SendMsg 生产 string 类型消息
func (p *KafkaProducer) SendMsg(
	ctx context.Context,
	topic,
	key string,
	msg string,
) (panicerr error) {
	defer func() {
		if r := recover(); r != nil {
			panicerr = errors.Errorf("panic: %v", r)
		}
	}()

	partition, offset, err := p.SendMessage(
		&sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.StringEncoder(msg),
		},
	)
	if err != nil {
		partition, offset, err = p.SendMessage(
			&sarama.ProducerMessage{
				Topic: topic,
				Key:   sarama.StringEncoder(key),
				Value: sarama.StringEncoder(msg),
			},
		)
		if err != nil {
			log.ErrorContextf(ctx, "KafkaProducer.SendMsg|err=%+v", err)
			return err
		}
	}
	log.DebugContextf(
		ctx,
		"KafkaProducer.SendMsg|partition=%d|offset=%d|",
		partition,
		offset,
	)
	return nil
}

// SendBytesMsg 生产 Bytes 类型消息
func (p *KafkaProducer) SendBytesMsg(
	ctx context.Context,
	topic string,
	key string,
	msg []byte,
) (panicerr error) {
	defer func() {
		if r := recover(); r != nil {
			panicerr = errors.Errorf("panic: %v", r)
		}
	}()

	partition, offset, err := p.SendMessage(
		&sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.ByteEncoder(msg),
		},
	)

	if err != nil {
		partition, offset, err = p.SendMessage(
			&sarama.ProducerMessage{
				Topic: topic,
				Key:   sarama.StringEncoder(key),
				Value: sarama.ByteEncoder(msg),
			},
		)
		if err != nil {
			log.ErrorContextf(ctx, "KafkaProducer.SendBytesMsg|err=%+v", err)
			return err
		}
	}
	log.DebugContextf(
		ctx,
		"KafkaProducer.SendBytesMsg|partition=%d|offset=%d|",
		partition,
		offset,
	)
	return nil
}
