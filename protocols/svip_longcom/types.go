package svip_longcom

// 仅用于编译通过的最小类型定义，非真实的 protoc 生成代码

// RegisterLongComService 占位，避免编译报错
func RegisterLongComService(_ interface{}, _ interface{}) {}

type GetTokenRequest struct {
	AppName     string
	Topic       string
	UserId      string
	CallbackUrl string
	MessageType int32
}

type GetTokenResponse struct {
	Token string
}

type ConnectRequest struct {
	Token string
}

type ConnectResponse struct {
	ConnId string
}

type GetUserStateRequest struct {
	AppName string
	Topic   string
	UserId  string
}

type GetUserStateResponse struct {
	Online      bool
	ConnectTime int64
}

type GetOnlineUsersRequest struct{}

type GetOnlineUsersResponse struct{}

type SendMessageByUserRequest struct {
	AppName string
	Topic   string
	UserId  string
	Message []byte
}

type SendMessageByUserResponse struct{}

type SendMessageByTopicRequest struct {
	AppName string
	Topic   string
	Message []byte
}

type SendMessageByTopicResponse struct{}

// callback 相关
type CallbackReceiveMessage struct {
	Message []byte
}

type CallbackConnect struct {
	ConnectTime int64
}

type CallbackDisconnect struct {
	ConnectTime    int64
	DisconnectTime int64
}

type CallbackDataType int32

const (
	CallbackDataType_Receive    CallbackDataType = 1
	CallbackDataType_Connect    CallbackDataType = 2
	CallbackDataType_Disconnect CallbackDataType = 3
)

type CallbackRequest struct {
	AppName  string
	Topic    string
	UserId   string
	DataType CallbackDataType
	Data     interface{}
}
