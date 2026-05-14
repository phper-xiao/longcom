package broker

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/pquerna/ffjson/ffjson"
	pb "github.com/tylerxiao/longcom/protocols"
	"github.com/tylerxiao/longcom/repo/request"
	"trpc.group/trpc-go/trpc-go/log"
)

// messageCallbackHandler 读消息callback处理
func messageCallbackHandler(ctx context.Context, c *ConnectionSession, msg []byte) error {
	data := &pb.CallbackReceiveMessage{
		Message: msg,
	}
	return callbackHandler(ctx, c, pb.CallbackDataType_Receive, data)
}

// connectCallbackHandler 建立连接callback处理
func connectCallbackHandler(ctx context.Context, c *ConnectionSession) error {
	data := &pb.CallbackConnect{
		ConnectTime: c.ConnectTime,
	}
	return callbackHandler(ctx, c, pb.CallbackDataType_Connect, data)
}

// disconnectCallbackHandler 关闭连接callback处理
func disconnectCallbackHandler(ctx context.Context, c *ConnectionSession) error {
	data := &pb.CallbackDisconnect{
		ConnectTime:    c.ConnectTime,
		DisconnectTime: time.Now().Unix(),
	}
	return callbackHandler(ctx, c, pb.CallbackDataType_Disconnect, data)
}

// callbackHandler 读消息处理
func callbackHandler(ctx context.Context, c *ConnectionSession, dataType pb.CallbackDataType, data interface{}) error {
	// 不需要回调，直接返回
	if c.Callback == "" {
		return nil
	}

	req := &pb.CallbackRequest{
		AppName:  c.Business,
		Topic:    c.Topic,
		UserId:   c.Alias,
		DataType: dataType,
		Data:     data,
	}

	return callbackDo(ctx, c.Callback, req)
}

// callbackDo callback接口调用
func callbackDo(ctx context.Context, callbackURL string, req *pb.CallbackRequest) error {
	headers := map[string]string{"Content-Type": "application/json"}
	params, _ := ffjson.Marshal(req)
	proxy := request.NewHTTP(callbackURL, request.MethodPost, params, headers, 3)
	if _, err := proxy.Request(); err != nil {
		log.WarnContextf(ctx, "callbackDo|callback do request fail|url=%s|err=%v", callbackURL, err)
		return err
	}

	return nil
}

// callbackParseURL 解析回调地址
func callbackParseURL(fullURL string) (target, uri string, err error) {
	u, err := url.Parse(fullURL)
	if err != nil {
		return "", "", err
	}

	scheme := u.Scheme
	if u.Scheme == "http" || u.Scheme == "https" {
		host := net.ParseIP(u.Hostname())
		if host != nil {
			scheme = "ip"
		} else {
			scheme = "dns"
		}
	}

	target = fmt.Sprintf("%s://%s", scheme, u.Host)
	uri = u.RequestURI()

	return target, uri, nil
}
