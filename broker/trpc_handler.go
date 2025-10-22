package broker

import (
	"context"

    "github.com/tylerxiao/longcom/logic"
    "github.com/tylerxiao/longcom/repo/merrors"
    mq "github.com/tylerxiao/longcom/repo/mq"
	pb "git.code.oa.com/up-common/rpcprotocol/svip_longcom"
	"github.com/trpc-group/trpc-go/log"
	"go.uber.org/zap"
)

// GetToken 获取长连接token
func (s *Server) GetToken(ctx context.Context, req *pb.GetTokenRequest,
	rsp *pb.GetTokenResponse) (err error) {

	token, err := logic.GetToken(ctx, req.AppName, req.Topic, req.UserId, req.CallbackUrl, req.MessageType)
	if err != nil {
		return err
	}

	rsp.Token = token
	return
}

// Connect 建立长链接
func (s *Server) Connect(ctx context.Context, req *pb.ConnectRequest, rsp *pb.ConnectResponse) error {
	// 校验token
	return nil
}

// GetUserState 查询用户连接状态
func (s *Server) GetUserState(ctx context.Context, req *pb.GetUserStateRequest,
	rsp *pb.GetUserStateResponse) (err error) {

	state, err := logic.GetUserState(req.AppName, req.Topic, req.UserId)
	if err != nil {
		log.WarnContextf(ctx, "GetUserState fail to GetUserOnlineState", zap.Any("req", req), zap.Error(err))
		return
	}

	rsp.Online = state.ConnectTime > 0 && state.DisConnectTime <= state.ConnectTime
	rsp.ConnectTime = state.ConnectTime
	return
}

// GetOnlineUsers 获取在线用户列表
func (s *Server) GetOnlineUsers(ctx context.Context, req *pb.GetOnlineUsersRequest,
	rsp *pb.GetOnlineUsersResponse) (err error) {
	return
}

// GetTopics 发送长连接消息，已废弃
func (s *Server) GetTopics(ctx context.Context, req *pb.GetTopicsRequest,
	rsp *pb.GetTopicsResponse) error {
	return merrors.ErrUserConnectionGetFail
}

// SendMessageByUser 给用户的所有连接发送消息
func (s *Server) SendMessageByUser(ctx context.Context, req *pb.SendMessageByUserRequest,
	rsp *pb.SendMessageByUserResponse) error {
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
		Message: req.Message,
	}

	if s.messageQueue == nil {
		log.InfoContextf(ctx, "messageQueue is nil")
	}
	log.InfoContextf(ctx, "s.tcpAddress=%s", s.tcpAddress)
	if err := s.messageQueue.PushByAliasMessageProducer(ctx, req.UserId, pushMsg); err != nil {
		log.ErrorContextf(ctx, "SendMessageByUser|kafka Produce(%s, %s, %s) error(%+v)\n", req.AppName, req.Topic, err)
		return err
	}

	return nil
}

// SendMessageByTopic 按topic给用户发送消息
func (s *Server) SendMessageByTopic(ctx context.Context, req *pb.SendMessageByTopicRequest,
	rsp *pb.SendMessageByTopicResponse) error {
	log.InfoContextf(ctx, "SendMessageByTopic| app=%s, topic=%s, msg=%s", req.AppName, req.Topic, string(req.Message))

	pushMsg := &mq.PushMessage{
		AppName: req.AppName,
		Topic:   req.Topic,
		Message: req.Message,
	}
	if err := s.messageQueue.PushByTopicMessageProducer(ctx, req.Topic, pushMsg); err != nil {
		log.ErrorContextf(ctx, "PushByAlias kafka Produce(%s, %s, %s) error(%+v)\n", req.AppName, req.Topic, err)
		return err
	}

	return nil
}
