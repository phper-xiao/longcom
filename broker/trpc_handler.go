package broker

import (
	"context"

	"github.com/tylerxiao/longcom/logic"
	pb "github.com/tylerxiao/longcom/protocols"
	"github.com/tylerxiao/longcom/repo/merrors"
	mq "github.com/tylerxiao/longcom/repo/mq"
	"go.uber.org/zap"
	"trpc.group/trpc-go/trpc-go/log"
)

// GetToken 获取长连接token
func (s *Server) GetToken(ctx context.Context, req *pb.GetTokenRequest) (*pb.GetTokenResponse, error) {
	token, err := logic.GetToken(ctx, req.AppName, req.Topic, req.UserId, req.CallbackUrl, frameUnAssign)
	if err != nil {
		return nil, err
	}

	return &pb.GetTokenResponse{Token: token}, nil
}

// Connect 建立长链接
func (s *Server) Connect(ctx context.Context, req *pb.ConnectRequest) (*pb.ConnectResponse, error) {
	// 校验token
	return &pb.ConnectResponse{}, nil
}

// GetUserState 查询用户连接状态
func (s *Server) GetUserState(ctx context.Context, req *pb.GetUserStateRequest) (*pb.GetUserStateResponse, error) {
	state, err := logic.GetUserState(req.AppName, req.Topic, req.UserId)
	if err != nil {
		log.WarnContextf(ctx, "GetUserState fail to GetUserOnlineState", zap.Any("req", req), zap.Error(err))
		return nil, err
	}

	return &pb.GetUserStateResponse{
		Node:           state.Node,
		Closed:         state.ConnectTime == 0 || state.DisConnectTime > state.ConnectTime,
		ConnectTime:    state.ConnectTime,
		DisconnectTime: state.DisConnectTime,
	}, nil
}

// GetOnlineUsers 获取在线用户列表
func (s *Server) GetOnlineUsers(ctx context.Context, req *pb.GetOnlineUsersRequest) (*pb.GetOnlineUsersResponse, error) {
	return &pb.GetOnlineUsersResponse{}, nil
}

// GetTopics 发送长连接消息，已废弃
func (s *Server) GetTopics(ctx context.Context, req *pb.GetTopicsRequest,
	rsp *pb.GetTopicsResponse) error {
	return merrors.ErrUserConnectionGetFail
}

// SendMessage 给指定连接发送消息
func (s *Server) SendMessage(ctx context.Context, req *pb.SendMessageRequest) (*pb.SendMessageResponse, error) {
	if req.ConnId == "" {
		return nil, merrors.ErrParametersConnectionEmpty
	}

	connIF, ok := s.connectionIDToConnMap.Load(req.ConnId)
	if !ok {
		return nil, merrors.ErrParametersConnectionNotFound
	}

	conn := connIF.(*Conn)
	if err := conn.SendRequestBytes(ctx, []byte(req.Message)); err != nil {
		return nil, err
	}

	return &pb.SendMessageResponse{}, nil
}

// SendMessageByUser 给用户的所有连接发送消息
func (s *Server) SendMessageByUser(ctx context.Context, req *pb.SendMessageByUserRequest) (*pb.SendMessageByUserResponse, error) {
	log.InfoContextf(
		ctx,
		"SendMessageByUser|app=%s|topic=%s|userid=%s|msg=%d",
		req.AppName, req.Topic,
		req.UserId, len(req.Message),
	)

	pushMsg := &mq.PushMessage{
		AppName: req.AppName,
		Topic:   req.Topic,
		UserID:  req.UserId,
		Message: []byte(req.Message),
	}

	if s.messageQueue == nil {
		log.InfoContextf(ctx, "messageQueue is nil")
	}
	log.InfoContextf(ctx, "s.tcpAddress=%s", s.tcpAddress)
	if err := s.messageQueue.PushByAliasMessageProducer(ctx, req.UserId, pushMsg); err != nil {
		log.ErrorContextf(ctx, "SendMessageByUser|kafka Produce(%s, %s, %s) error(%+v)\n", req.AppName, req.Topic, req.UserId, err)
		return nil, err
	}

	return &pb.SendMessageByUserResponse{}, nil
}

// SendMessageByTopic 按topic给用户发送消息
func (s *Server) SendMessageByTopic(ctx context.Context, req *pb.SendMessageByTopicRequest) (*pb.SendMessageByTopicResponse, error) {
	log.InfoContextf(ctx, "SendMessageByTopic| app=%s, topic=%s, msg=%s", req.AppName, req.Topic, string(req.Message))

	pushMsg := &mq.PushMessage{
		AppName: req.AppName,
		Topic:   req.Topic,
		Message: req.Message,
	}
	if err := s.messageQueue.PushByTopicMessageProducer(ctx, req.Topic, pushMsg); err != nil {
		log.ErrorContextf(ctx, "PushByAlias kafka Produce(%s, %s) error(%+v)\n", req.AppName, req.Topic, err)
		return nil, err
	}

	return &pb.SendMessageByTopicResponse{}, nil
}
