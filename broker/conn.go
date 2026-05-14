package broker

import (
	"bytes"
	"context"
	"net"
	"sync"
	"time"

	"github.com/gobwas/ws"
	"github.com/google/uuid"
	"github.com/panjf2000/gnet"
	"github.com/pkg/errors"
	"trpc.group/trpc-go/trpc-go/log"
	"github.com/tylerxiao/longcom/repo/merrors"
	"github.com/tylerxiao/longcom/repo/message"
	"go.uber.org/atomic"
)

type separateReadWriteBuffer struct {
	reader *bytes.Reader
	writer *bytes.Buffer
}

func newSeparateReadWriteBuffer(in []byte) *separateReadWriteBuffer {
	reader := bytes.NewReader(in)
	writer := bytes.NewBuffer(nil)
	return &separateReadWriteBuffer{
		reader: reader,
		writer: writer,
	}
}

// Read implements the io.Reader interface.
func (b *separateReadWriteBuffer) Read(p []byte) (n int, err error) {
	return b.reader.Read(p)
}

// UnreadLen returns the number of bytes of the unread portion of the slice
func (b *separateReadWriteBuffer) UnreadLen() int {
	return b.reader.Len()
}

// Write appends the contents of p to the buffer, growing the buffer as
// needed. The return value n is the length of p;
func (b *separateReadWriteBuffer) Write(p []byte) (n int, err error) {
	return b.writer.Write(p)
}

// Bytes returns a slice of length b.Len() holding the unread portion of the buffer.
func (b *separateReadWriteBuffer) Bytes() []byte {
	return b.writer.Bytes()
}

const (
	connStateInit    = iota + 1
	connStateAuthed  // 连接已经通过校验
	connStateClosing // 连接已经
	connStateClosed
)

const defaultReaderBufferSize = 1024

// 初始化Conn时，Auth前的默认心跳间隔
const defaultHeartbeatInterval = 30 * time.Second

// 每条连接最多可同时有 1024 个来自客户端的请求在处理中
const defaultMaxInflightMessageCount = 1024

// defaultDeadConnDeadlineInterval 死连接默认超时时间
const defaultDeadConnDeadlineInterval = 5 * time.Second

// defaultDeadConnHeartbeatDeadline 死连接默认超时时间
const defaultDeadConnHeartbeatDeadline int64 = -1

// defaultUpdateStatusInterval 更新状态默认时间间隔
const defaultUpdateStatusInterval = 10 * time.Second

const (
	frameUnAssign = iota
	frameText
	frameBinary
)

// Conn is the broker connection.
type Conn struct {
	// 连接的 ctx 和 cancel
	// 用于在 close 时，关闭监听该 ctx 的 select
	ctx    context.Context
	cancel context.CancelFunc

	csMgr *connStateManager

	ExitChan          chan int
	closeExitChanOnce sync.Once

	connectionID string // The client id is generate by server during newConn
	luid         uint64 // local unique id of this connection

	mu      sync.RWMutex
	session *ConnectionSession
	// FlushInterval    time.Duration

	HeartbeatDeadlineInterval *atomic.Duration

	// 从客户端进来的，正在处理的消息数
	inflightInboundMessageCount *atomic.Int64

	server *Server

	setErrOnce sync.Once
	// 设置连接异常信息
	connErr error

	outboundMessageIDFuture *message.MessageIDFuture

	// 当前连接是否已经完成 websocket upgrade
	websocketUpgraded bool

	salmonFrameConn *salmonFrameConn

	// 记录最近一次接收到数据的时间 (unix epoch in second)
	lastReceiveFrameTime *atomic.Int64

	// connectTime 记录连接生成时间
	connectTime int64

	// isDeadConn 是否死连接, 用于处理失败时候, 过期更新的逻辑
	isDeadConn bool

	// onceSetDead 设置死连接只会设置一次
	onceSetDead sync.Once

	// deadConnDeadlineInterval 死连接超时时间间隔
	deadConnDeadlineInterval *atomic.Duration

	// updateStatusInterval 更新状态时间间隔
	updateStatusInterval *atomic.Duration

	// updateStatusLastTime 最近一次更新状态成功的时间
	updateStatusLastTime *atomic.Int64

	// frameType 数据帧类型 text/bin
	frameType int32
}

func newConn(s *Server, gnetConn gnet.Conn) (*Conn, error) {
	uid, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	timeNow := time.Now()
	conn := &Conn{
		csMgr: &connStateManager{
			// 初始化状态
			state: connStateInit,
		},

		HeartbeatDeadlineInterval:   atomic.NewDuration(defaultHeartbeatInterval),
		inflightInboundMessageCount: atomic.NewInt64(0),

		ExitChan: make(chan int),

		// 生成连接ID 和 本地ID
		connectionID: uid.String(),
		luid:         s.fetchNextLUID(),

		server:                  s,
		outboundMessageIDFuture: message.NewMessageIDFuture(),

		// 初始化时，把当前时间认为是数据接收时间
		lastReceiveFrameTime: atomic.NewInt64(timeNow.Unix()),

		connectTime: timeNow.Unix(),

		deadConnDeadlineInterval: atomic.NewDuration(defaultDeadConnDeadlineInterval),

		updateStatusInterval: atomic.NewDuration(defaultUpdateStatusInterval),

		updateStatusLastTime: atomic.NewInt64(timeNow.Unix()),
	}

	conn.ctx, conn.cancel = context.WithCancel(context.Background())

	salmonFrameConn := newSalmonFrameConn(
		gnetConn,
		func(frame []byte) {
			conn.onFrame(conn.ctx, frame)
		},
		func(frame []byte) {
			conn.onAuthRequest(conn.ctx, frame)
		},
		func(frame []byte) {
			conn.onPing(conn.ctx, frame)
		},
	)

	conn.salmonFrameConn = salmonFrameConn
	conn.frameType = frameText

	return conn, nil
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.salmonFrameConn.RemoteAddr()
}

// GetHeartbeatDeadline return validity of connection heartbeat
func (c *Conn) GetHeartbeatDeadline() int64 {
	if c.isDeadConn {
		return defaultDeadConnHeartbeatDeadline
	}
	t := time.Unix(c.lastReceiveFrameTime.Load(), 0)
	heartbeatInterval := c.HeartbeatDeadlineInterval.Load()
	return t.Add(heartbeatInterval).Unix()
}

// Ping 发送ping包
func (c *Conn) Ping() error {
	wsTextFrame := ws.NewPingFrame(nil)
	pingFrame, err := ws.CompileFrame(wsTextFrame)
	if err != nil {
		return errors.WithStack(err)
	}

	return c.salmonFrameConn.AsyncWrite(pingFrame)
}

// Pong 发送pong包
func (c *Conn) Pong(p []byte) error {
	wsTextFrame := ws.NewPingFrame(p)
	pongFrame, err := ws.CompileFrame(wsTextFrame)
	if err != nil {
		return errors.WithStack(err)
	}

	return c.salmonFrameConn.AsyncWrite(pongFrame)
}

// getDeadConnCheckTs 获取死连接超时检查时间戳
func (c *Conn) getDeadConnCheckTs() int64 {
	return time.Now().Add(c.deadConnDeadlineInterval.Load()).Unix()
}

// setDead 设置为死连接, 会在指定时间(默认为当前时间+deadConnDeadlineInterval)后被断掉
func (c *Conn) setDead(ctx context.Context) {
	if c.isDeadConn {
		return
	}
	c.onceSetDead.Do(func() {
		c.isDeadConn = true
		c.server.heartbeatTimer.UpdateConn(ctx, c, c.getDeadConnCheckTs())
	})
}

// makeAuthFailedRspFrame 生成认证失败请求
func makeAuthFailedRspFrame(errAuthFailed error) []byte {
	err := errors.Unwrap(errAuthFailed)
	if err == nil {
		err = errAuthFailed
	}
	return nil
}

// sendAuthFailedAndSetDead 发送认证失败请求并设置为死连接
func (c *Conn) sendAuthFailedAndSetDead(ctx context.Context, errAuthFailed error) error {
	c.setDead(ctx)
	err := c.SendRequestBytes(ctx, makeAuthFailedRspFrame(errAuthFailed))
	if err != nil {
		log.ErrorContextf(ctx, "sendAuthFailedAndSetDead|c.SendFrame failed, err: %v, errAuthFailed: %v", err,
			errAuthFailed)
		return err
	}
	return nil
}

// OnReceiveData 必然是 evio server 单个goroutine循环中调用的
// 不要有任何阻塞 loop 的行为
func (c *Conn) OnReceiveData(in []byte) (out []byte, action gnet.Action) {
	return c.salmonFrameConn.OnReact(in)
}

func (c *Conn) setAuthed(session *ConnectionSession, topic string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 这里不能直接更新连接状态
	// 假如连接在 setAuthed 的过程时，已经被关闭了
	// 那会导致对已经进入 Closed 状态的连接又改回 authed 状态了
	// 然而已经调用过 Closed 的连接，一般不会被二次 Close
	// 那就会出现泄露了 hub goroutine 泄露了
	// 一定要先检查
	if c.csMgr.getState() == connStateInit {
		c.csMgr.updateState(connStateAuthed)

		c.session = session

		err := c.server.AddAuthedConnection(
			context.Background(),
			session,
			c,
			[]string{topic},
		)
		if err != nil {
			log.Warnf("setAuthed|connectionID=%s|err=%+v", session.ConnectionID, err)
		}
	}
}

func (c *Conn) isAuthed() bool {
	state := c.csMgr.getState()
	return state == connStateAuthed
}

// SendRequestBytes send data to peer
func (c *Conn) SendRequestBytes(ctx context.Context, requestBytes []byte) error {
	var wsFrame ws.Frame
	switch c.frameType {
	case frameText:
		// 文本消息
		wsFrame = ws.NewTextFrame(requestBytes)
	case frameBinary:
		// 二进制消息
		wsFrame = ws.NewBinaryFrame(requestBytes)
	default:
		wsFrame = ws.NewTextFrame(requestBytes)
	}

	dat, err := ws.CompileFrame(wsFrame)
	if err != nil {
		return errors.WithStack(err)
	}

	err = c.Send(ctx, dat)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// Send data to the peer.
func (c *Conn) Send(ctx context.Context, b []byte) error {
	// report metrics
	c.server.metrics.ReportSendPackage(ctx, len(b))

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.ExitChan:
		return merrors.ErrConnClosed
	default:
		return c.salmonFrameConn.AsyncWrite(b)
	}
}

// Exit the connection closed
func (c *Conn) Exit() {
	c.closeExitChanOnce.Do(func() {
		close(c.ExitChan)
	})
}

func (c *Conn) setConnError(err error) {
	c.setErrOnce.Do(func() {
		c.connErr = err
	})
}

// Close the connection.
func (c *Conn) Close(
	ctx context.Context,
	callCloseFrom CallCloseFromType,
	reasonCode int32,
	reason string,
) error {
	if reasonCode != 0 {
		log.WarnContextf(
			ctx,
			"Close|connectionID=%s|callCloseFrom=%d|reasonCode=%d|reason=%s|err=%+v|",
			c.connectionID,
			callCloseFrom,
			reasonCode,
			reason,
			c.connErr,
		)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.csMgr.getState()
	switch state {
	case connStateInit:
		defer c.cancel()
		c.Exit()
		c.closeNetwork()
		c.csMgr.updateState(connStateClosed)
		return nil
	case connStateAuthed:
		defer c.cancel()
		c.csMgr.updateState(connStateClosing)

		c.Exit()

		if c.session != nil {
			err := c.server.RemoveAuthedConnection(
				ctx,
				c.session,
				c,
			)
			if err != nil {
				log.Warnf("Close|connectionID=%s|err=%+v", c.session.ConnectionID, err)
			}
		}

		c.closeNetwork()
		c.csMgr.updateState(connStateClosed)
		return nil
	case connStateClosing, connStateClosed:
		return merrors.ErrConnClosed
	default:
		log.ErrorContextf(ctx, "Close|Unknown Conn State %d", state)
		return merrors.ErrUnknownConnStatus
	}
}

func (c *Conn) closeNetwork() {
	c.salmonFrameConn.Close()
}

// ID return local unique id of this connection
func (c *Conn) ID() uint64 {
	return c.luid
}

func (c *Conn) protocolError(ctx context.Context, err error, sequenceID uint32, errCode int32, errMsg string) error {
	c.setErrOnce.Do(func() {
		c.connErr = err
	})

	closeErr := c.Close(ctx, CallCloseFromLocal, errCode, errMsg)

	if closeErr != nil {
		return errors.WithStack(closeErr)
	}

	return nil
}

func (c *Conn) incInflightMessageCount() int64 {
	return c.inflightInboundMessageCount.Inc()
}

func (c *Conn) decInflightMessageCount() int64 {
	return c.inflightInboundMessageCount.Dec()
}

type connStateManager struct {
	mu    sync.RWMutex
	state int32
}

func (csm *connStateManager) updateState(state int32) {
	csm.mu.Lock()
	defer csm.mu.Unlock()
	if csm.state == connStateClosed {
		return
	}
	if csm.state == state {
		return
	}
	csm.state = state
}

func (csm *connStateManager) getState() int32 {
	csm.mu.RLock()
	defer csm.mu.RUnlock()
	return csm.state
}
