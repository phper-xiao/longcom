// Package merrors contains all project-wide errors.
package merrors

import (
	"github.com/trpc-group/trpc-go/errs"
)

var (
	ErrBrokerServerInternalError       = errs.New(20000, "Broker Server Internal Error")
	ErrNotConnectd                     = errs.New(20001, "Not Connected")
	ErrConnClosed                      = errs.New(20002, "Connection closed")
	ErrNoMessageIDAvailable            = errs.New(20003, "No message id available")
	ErrParametersBusinessEmpty         = errs.New(20004, "Parameters Business Empty")
	ErrParametersConnectionEmpty       = errs.New(20005, "Parameters Connection ID Empty")
	ErrParametersConnectionNotFound    = errs.New(20006, "Parameters Connection Not Found")
	ErrUnsupportQOS                    = errs.New(20007, "Unsupport QOS")
	ErrConnAlreadyAuth                 = errs.New(20008, "Connection already auth")
	ErrConnNotAuth                     = errs.New(20009, "Connection not auth")
	ErrConnUnknownState                = errs.New(20010, "Connection unknown state")
	ErrSendBufferFull                  = errs.New(20011, "Send Buffer Full")
	ErrTooManyInflightInboundMessage   = errs.New(20012, "Too Many Inflight Inbound Message")
	ErrAliasHubNotExist                = errs.New(20013, "Alias Hub Not Exist")
	ErrHubHasBeenClosed                = errs.New(20014, "Hub Has Been Closed")
	ErrInvalidPushByAliasMessageParams = errs.New(20015, "Invalid Push By Alias Message Params")
	ErrParametersAliasEmpty            = errs.New(20016, "Parameters Alias Empty")
	ErrParametersTopicListEmpty        = errs.New(20017, "Parameters TopicList Empty")
	ErrTrpcRspIsNil                    = errs.New(20018, "Trpc rsp is nil")
	ErrNewGroupManager                 = errs.New(20019, "New Group Manager failed")
	ErrAliasInvalid                    = errs.New(20020, "Alias invalid")
	ErrAPIDisconnectParams             = errs.New(20021, "APIDisconnect params invalid")
	ErrUnknownConnStatus               = errs.New(20022, "Conn status unknown")
	ErrPayloadTypeNotSupport           = errs.New(20023, "Payload type not support")

	// ErrCallbackHandlerNotFound define
	ErrCallbackHandlerNotFound = errs.New(37001, "callback handler not found")
	// ErrLoginSessionInvalid define
	ErrLoginSessionInvalid = errs.New(37002, "login session invalid")
	// ErrAnchorNotFound define
	ErrAnchorNotFound = errs.New(37003, "anchor not found")
	// ErrWebSocketTokenInvalid define
	ErrWebSocketTokenInvalid = errs.New(37004, "websocket token invalid")
	// ErrSendMessageFail define
	ErrSendMessageFail = errs.New(37005, "websocket send message failed")
	// ErrUserConnectionGetFail define
	ErrUserConnectionGetFail = errs.New(37006, "websocket connection get failed")
	// ErrMsgSendOccurConnectionIsClosed define
	ErrMsgSendOccurConnectionIsClosed = errs.New(37007, "msg send occur connection is closed")
	// ErrConnExceedsLimit
	ErrConnExceedsLimit = errs.New(37008, "connections exceeds the maximum limit")

	// 29000 开始时框架内部错误
	ErrGnetWebsocketCodecNeedMoreData = errs.New(29001, "Gnet Websocket Codec Need More Data")
)
