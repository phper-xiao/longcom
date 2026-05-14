package broker

import (
    "context"
    "time"

    "github.com/panjf2000/gnet"
    "trpc.group/trpc-go/trpc-go/log"
)

// OnInitComplete gnet 库 OnInitComplete 接口的实现
func (s *Server) OnInitComplete(server gnet.Server) (action gnet.Action) {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.exitChan:
				return
			case <-ticker.C:
				s.metrics.ReportMemoryUsage(context.Background())
				s.metrics.ReportConnectionCount(context.Background(), int32(s.ConnCount()))
			}
		}
	}()
	return
}

// OnShutdown gnet 库 OnShutdown 接口的空实现
func (s *Server) OnShutdown(server gnet.Server) {

}

// PreWrite gnet 库 PreWrite 接口的空实现
func (s *Server) PreWrite(c gnet.Conn) {

}

// PreWrite gnet 库 AfterWrite 接口的空实现
func (s *Server) AfterWrite(c gnet.Conn, b []byte) {

}

// OnOpened gnet 库 OnOpened 接口的实现
func (s *Server) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	s.incrConnCount()

	log.InfoContextf(
		s.ctx,
		"onGnetOpened|RemoteAddr=%s|brokerConnCount=%d|LocalAddr=%s|Context=%v",
		c.RemoteAddr(),
		s.ConnCount(),
		c.LocalAddr(),
		c.Context(),
	)

	conn, err := newConn(s, c)

	if err != nil {
		log.WarnContextf(
			s.ctx,
			"onGnetOpened|RemoteAddr=%s|",
			c.RemoteAddr(),
		)
		action = gnet.Close
		return
	}

	s.gnetConnToConn.Store(c, conn)
	s.heartbeatTimer.AddConn(context.Background(), conn)

	return
}

// OnClosed gnet 库 OnClosed 接口的实现
func (s *Server) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	s.decrConnCount()

	log.InfoContextf(
		s.ctx,
		"onGnetClosed|RemoteAddr=%s|brokerConnCount=%d|",
		c.RemoteAddr(),
		s.ConnCount(),
	)

	connIF, exist := s.gnetConnToConn.LoadAndDelete(c)
	if exist {
		conn := connIF.(*Conn)
		s.heartbeatTimer.RemoveConn(context.Background(), conn)
		// 关闭连接涉及到了 IO 操作，可能会堵塞当前 OnClosed
		conn.Close(s.ctx, 0, 0, "")
	}

	if err != nil {
		log.WarnContextf(
			s.ctx,
			"onGnetClosed|RemoteAddr=%s|err=%+v|",
			c.RemoteAddr(),
			err,
		)
	}
	return
}

// Tick gnet 库 Tick 接口的空实现
func (s *Server) Tick() (delay time.Duration, action gnet.Action) {
	return
}

// React gnet 库 React 接口的实现
func (s *Server) React(in []byte, c gnet.Conn) (out []byte, action gnet.Action) {
	// 查找出 Broker Conn
	connIF, ok := s.gnetConnToConn.Load(c)
	if !ok {
		// 查找不到对应连接，关闭该连接
		return nil, gnet.Close
	}

	conn := connIF.(*Conn)

	// 调用对应连接的数据接收处理机制
	return conn.OnReceiveData(in)
}
