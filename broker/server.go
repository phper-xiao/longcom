package broker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

    "github.com/tylerxiao/longcom/config"
    "github.com/tylerxiao/longcom/repo/merrors"
    "github.com/tylerxiao/longcom/repo/metrics"
    "github.com/tylerxiao/longcom/repo/mq"
    "github.com/tylerxiao/longcom/repo/mq/kafka"
    "github.com/tylerxiao/longcom/repo/wrapper"
	longcom "git.code.oa.com/up-common/rpcprotocol/svip_longcom"
	"github.com/google/uuid"
	"github.com/panjf2000/gnet"
	"github.com/pkg/errors"
	"github.com/trpc-group/trpc-go/log"
	"github.com/trpc-group/trpc-go/server"
)

// Server that backs the broker.
type Server struct {
	nextLUID  uint64
	connCount int64

	nodeID string

	cfg atomic.Value

	ctx context.Context

	startTime time.Time
	exitChan  chan int

	tcpAddress string

	trpcServer *server.Server

	connectionIDToConnMap sync.Map

	waitGroup wrapper.WaitGroupWrapper

	polarisHeartbeatInterval time.Duration

	// antsPool *ants.Pool
	messageQueue mq.MessageQueue

	// business group manager, 按业务区分的 group 管理实例
	bgm *businessGroupManager

	// gnet Conn -> Broker Conn 映射
	gnetConnToConn sync.Map

	// 在 gent 模型下， setDeadline 已经无法使用了，这里用基于最小堆的 HeartbeatTimer 做连接超时
	heartbeatTimer *HeartbeatTimer

	metrics metrics.Metrics
}

func (s *Server) fetchNextLUID() uint64 {
	return atomic.AddUint64(&s.nextLUID, 1)
}

func (s *Server) incrConnCount() {
	atomic.AddInt64(&s.connCount, 1)
}

func (s *Server) decrConnCount() {
	atomic.AddInt64(&s.connCount, -1)
}

// ConnCount return Server connection count
func (s *Server) ConnCount() int64 {
	return atomic.LoadInt64(&s.connCount)
}

func (s *Server) getCfg() *config.Config {
	return s.cfg.Load().(*config.Config)
}

func (s *Server) swapCfg(config *config.Config) {
	s.cfg.Store(config)
}

// NewServer creates a new server.
func NewServer(trpcServer *server.Server, cfg *config.Config) (*Server, error) {
	uid, _ := uuid.NewRandom()
	ctx := context.Background()

	nodeID := uid.String()

	heartbeatTimer := NewHeartbeatTimer(30*time.Second, 65535)

	s := &Server{
		nodeID:         nodeID,
		ctx:            ctx,
		heartbeatTimer: heartbeatTimer,
		startTime:      time.Now(),
		exitChan:       make(chan int),
	}

	s.swapCfg(cfg)

	messageQueue, err := kafka.NewKafkaMessageQueue(
		ctx,
		&cfg.KafkaMessageQueueConfig,
		nodeID,
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	s.messageQueue = messageQueue

	s.trpcServer = trpcServer

	s.tcpAddress = cfg.TCPAddress

	s.bgm = newBusinessGroupManager(cfg)

	s.metrics = metrics.NewZhiyanMetrics()

	return s, nil
}

// Start the server.
func (s *Server) Start() error {
	exitCh := make(chan error)
	var once sync.Once

	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				log.Fatalf("exit broker error|err=%+v", err)
			}
			exitCh <- err
		})
	}

	s.waitGroup.Wrap(func() {
		exitFunc(func() error {
			longcom.RegisterLongComService(s.trpcServer, &Server{
				messageQueue: s.messageQueue,
				tcpAddress:   s.tcpAddress,
			})
			err := s.trpcServer.Serve()
			if err != nil {
				return errors.WithStack(err)
			}

			return nil
		}())
	})

	s.waitGroup.Wrap(func() {
		exitFunc(func() error {
			err := gnet.Serve(
				s,
				s.tcpAddress,
				gnet.WithLoadBalancing(gnet.RoundRobin),
				gnet.WithMulticore(true),
				gnet.WithCodec(&WebsocketFrameCodec{}),
			)
			return errors.WithStack(err)
		}())
	})

	s.waitGroup.Wrap(func() {
		exitFunc(func() error {
			s.heartbeatTimer.Run()
			return nil
		}())
	})

	s.messageQueue.StartPushByAliasMessageConsumerSync(
		s.ctx,
		s.PushByAliasMessageHandler,
	)

	s.messageQueue.StartPushAllMessageConsumerSync(
		s.ctx,
		s.PushAllMessageHandler,
	)

	s.messageQueue.StartPushByTopicMessageConsumerSync(
		s.ctx,
		s.PushByTopicMessageHandler,
	)

	err := <-exitCh
	return err
}

// NodeID return Server node ID.
func (s *Server) NodeID() string {
	return s.nodeID
}

// AddAuthedConnection add connection into the Server
func (s *Server) AddAuthedConnection(
	ctx context.Context,
	session *ConnectionSession,
	conn *Conn,
	topic []string,
) error {
	// 获取对应业务的 group manager
	gm, err := s.bgm.getGroupManager(ctx, session.Business)
	if err != nil {
		log.ErrorContextf(ctx, "AddAuthedConnection|getGroupManager failed, err: %v", err)
		return errors.WithStack(err)
	}
	// 连接 alias -> conn
	gm.addAliasConn(ctx, session.Business, s.NodeID(), session.Alias, conn, topic)
	gm.addConn(ctx, session.ConnectionID, conn)

	// 建连 callback 业务
	connectCallbackHandler(ctx, session)
	return nil
}

// RemoveAuthedConnection remove connection from the Server
func (s *Server) RemoveAuthedConnection(
	ctx context.Context,
	session *ConnectionSession,
	conn *Conn,
) error {
	// s.connectionIDToConnMap.Delete(session.ConnectionID)
	// 获取对应业务的 group manager
	gm, err := s.bgm.getGroupManager(ctx, session.Business)
	if err != nil {
		log.ErrorContextf(ctx, "RemoveAuthedConnection|getGroupManager failed, err: %v", err)
		return errors.WithStack(err)
	}
	// 移除 alias -> conn
	gm.removeAliasConn(ctx, session.Business, s.NodeID(), session.Alias, conn)
	gm.removeConn(ctx, session.ConnectionID)

	// 断连 callback 业务
	disconnectCallbackHandler(ctx, session)
	return nil
}

// PushByAliasMessageHandler push by alias handler
func (s *Server) PushByAliasMessageHandler(
	ctx context.Context,
	message *mq.PushMessage,
) (err error) {
	timeBeg := time.Now()
	defer func() {
		cost := time.Since(timeBeg)
		if err != nil {
			log.ErrorContextf(ctx, "PushByAliasMessageHandler|cost: %v|err: %v|msg: %v", cost, err,
				message)
		} else {
			log.DebugContextf(ctx, "PushByAliasMessageHandler|cost: %v", cost)
		}
	}()

	gm, err := s.bgm.getGroupManager(ctx, message.AppName)
	if err != nil {
		log.ErrorContextf(ctx, "PushByAliasMessageHandler|getGroupManager failed, err: %v", err)
		return
	}
	err = gm.BroadcastByAlias(
		ctx,
		message.UserID,
		message.Message,
	)

	return errors.WithStack(err)
}

// PushAllMessageHandler push all handler
func (s *Server) PushAllMessageHandler(
	ctx context.Context,
	message *mq.PushMessage,
) error {
	if message.AppName == "" {
		return errors.WithStack(merrors.ErrInvalidPushByAliasMessageParams)
	}

	gm, err := s.bgm.getGroupManager(ctx, message.AppName)
	if err != nil {
		log.ErrorContextf(ctx, "PushAllMessageHandler|getGroupManager failed, err: %v", err)
		return errors.WithStack(err)
	}

	err = gm.Broadcast(
		ctx,
		message.Message,
	)

	return errors.WithStack(err)
}

// PushByTopicMessageHandler push topic handler
func (s *Server) PushByTopicMessageHandler(ctx context.Context, message *mq.PushMessage) (err error) {
	timeBeg := time.Now()
	defer func() {
		cost := time.Since(timeBeg)
		if err != nil {
			log.ErrorContextf(ctx, "PushByTopicMessageHandler|cost: %v|err: %v|msg: %v", cost, err,
				message)
		} else {
			log.DebugContextf(ctx, "PushByTopicMessageHandler|cost: %v|msg: %v", cost, message)
		}
	}()
	if len(message.AppName) == 0 || len(message.Topic) == 0 {
		log.ErrorContextf(ctx,
			"PushByTopicMessageHandler|business: %v or topic: %v is empty",
			message.AppName, message.Topic,
		)
		return errors.WithStack(merrors.ErrInvalidPushByAliasMessageParams)
	}

	gm, err := s.bgm.getGroupManager(ctx, message.AppName)
	if err != nil {
		log.ErrorContextf(ctx, "PushByTopicMessageHandler|getGroupManager failed, err: %v", err)
		return errors.WithStack(err)
	}
	err = gm.BroadcastByTopic(ctx, message.Topic, message.Message)
	if err != nil {
		log.ErrorContextf(ctx, "PushByTopicMessageHandler|BroadcastByTopic failed, err: %v", err)
		return errors.WithStack(err)
	}

	return
}

// Exit terminates the server.
func (s *Server) Exit() {

	cancelctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()

	if s.tcpAddress != "" {
		gnet.Stop(cancelctx, s.tcpAddress)
	}

	if s.heartbeatTimer != nil {
		s.heartbeatTimer.Close()
	}

	close(s.exitChan)
	s.waitGroup.Wait()
}
