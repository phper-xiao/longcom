package broker

import (
	"context"
	"net/url"
	"runtime"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/trpc-group/trpc-go/log"
	"github.com/tylerxiao/longcom/logic"
	"github.com/tylerxiao/longcom/repo/merrors"
)

var MaxTime time.Time = time.Unix(1<<63-1, 0)
var ZeroTime time.Time

func (c *Conn) onFrame(ctx context.Context, frame []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.ErrorContextf(
				ctx,
				"onFrame panic|r=%+v|",
				r,
			)
		}
	}()

	// 更新收到 frame 的时间
	c.lastReceiveFrameTime.Store(time.Now().Unix())

	count := c.incInflightMessageCount()
	if count >= defaultMaxInflightMessageCount {
		c.decInflightMessageCount()
		return
	}

	// 还未到达 Max Inflight Message
	go func() {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 16*1024*1024)
				buf = buf[:runtime.Stack(buf, false)]
				log.ErrorContextf(
					ctx,
					"onFrame panic|r=%+v|stack: %s",
					r, buf,
				)
			}

			c.decInflightMessageCount()
		}()

		c.onInboundRequestDispatch(ctx, frame)
	}()
}

func (c *Conn) onInboundRequestDispatch(ctx context.Context, frame []byte) {
	// report metrics
	c.server.metrics.ReportReceivePackage(ctx, len(frame))

	c.onRequest(ctx, frame)
}

func (c *Conn) onOutboundResponseDispatch(ctx context.Context, frame []byte) {
	log.Infof(
		"Connection onOutboundResponseDispatch unimplemented|header=%s|",
		frame,
	)
}

// getIntervals 获取间隔
func getIntervals(heartbeatInterval int32, heartbeatDeadlineInterval int32) (int32, int32) {
	if heartbeatInterval <= 0 { // 如果未指定心跳间隔, 则给定默认心跳间隔
		heartbeatInterval = int32(defaultHeartbeatInterval / time.Second)
	}
	if heartbeatDeadlineInterval < 2*heartbeatInterval { // 如果超时检测间隔小于心跳间隔2倍, 则设置为心跳间隔2倍
		heartbeatDeadlineInterval = 2 * heartbeatInterval
	}
	return heartbeatInterval, heartbeatDeadlineInterval
}

// TODO: 设计 protocolError 异常返回码
func (c *Conn) onAuthRequest(ctx context.Context, frame []byte) {
	uri := string(frame)
	unescapeURI, _ := url.QueryUnescape(uri)
	u, err := url.Parse(unescapeURI)
	if err != nil {
		c.protocolError(ctx, err, 1, -1, "token not set")
		return
	}

	claims, err := logic.VerifyToken(ctx, u.Query().Get("token"))
	log.DebugContextf(
		ctx,
		"onAuthRequest|authRequest=%s|claims=%+v",
		frame,
		claims,
	)
	if err != nil {
		c.protocolError(ctx, err, 1, -1, "token verify err")
		return
	}

	connectionSession, err := newConnectionSession(
		ctx,
		c.connectionID,
		claims.AppName,
		[]byte(""),
		claims.UserID,
		claims.CallbackURL,
		claims.Topic,
	)
	if err != nil {
		c.protocolError(ctx, errors.WithStack(err), 1, -1, "broker err")
		return
	}

	c.setAuthed(connectionSession, claims.Topic)
	if claims.MessageType != frameUnAssign {
		c.frameType = claims.MessageType
	}

	return
}

func (c *Conn) onRequest(ctx context.Context, packet []byte) {
	// 能发送消息前必须先鉴权
	if !c.isAuthed() {
		// 否则直接关闭连接
		log.ErrorContextf(
			ctx,
			"onRequest|connection receive request before auth|connectionID=%s|",
			c.connectionID,
		)
		c.protocolError(ctx, merrors.ErrNotConnectd, 1, -1, "receive request before auth")
		return
	}

	c.mu.RLock()
	session := c.session
	c.mu.RUnlock()

	//心跳包 直接写pong
	if string(packet) == "heartbeat" {
		c.SendRequestBytes(ctx, []byte("heartbeat success"))
		return
	}

	if err := messageCallbackHandler(ctx, session, packet); err != nil {
		log.ErrorContextf(
			ctx,
			"onRequest|callbackHandler|UserID=%s｜err=%+v|",
			session.Alias,
			err,
		)
	}

	return
}

func (c *Conn) onReply(ctx context.Context, packet []byte) error {

	log.DebugContextf(
		ctx,
		"[Broker] onReply|connectionID=%s|packet=%s|",
		c.connectionID,
		packet,
	)

	return nil
}

func (c *Conn) onDisconnectReply(ctx context.Context) error {
	return nil
}

// needUpdateStatus 判断是否需要更新状态
func (c *Conn) needUpdateStatus(ctx context.Context) bool {
	curTs := time.Now().Unix()
	return (curTs >= c.updateStatusLastTime.Load()+int64(c.updateStatusInterval.Load().Seconds()))
}

func (c *Conn) onPing(ctx context.Context, frame []byte) {
	defer func() {
		if r := recover(); r != nil {
			log.ErrorContextf(
				ctx,
				"onFrame panic|r=%+v|",
				r,
			)
		}
	}()

	// 更新收到 frame 的时间
	c.lastReceiveFrameTime.Store(time.Now().Unix())
	c.Pong(frame)
	return
}

// 来自客户端发起的 Disconnect
func (c *Conn) onDisconnectRequest(
	ctx context.Context,
) {

	err := c.Close(
		ctx,
		CallCloseFromClient,
		-1,
		"client disconnect",
	)

	if err != nil {
		log.WarnContextf(
			ctx,
			"onDisconnectRequest|err=%+v|",
			err,
		)
	}
}

// 根据 ContentType ，获取对应的响应类型和空接口
func (c *Conn) getEmptyResponseByContentType(
	ctx context.Context,
) (proto.Message, error) {
	return nil, nil
}
