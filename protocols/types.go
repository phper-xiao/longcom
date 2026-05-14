package protocols

// Deprecated compatibility types retained for broker callbacks and older callers.

type CallbackReceiveMessage struct {
	Message []byte `json:"message,omitempty"`
}

type CallbackConnect struct {
	ConnectTime int64 `json:"connect_time,omitempty"`
}

type CallbackDisconnect struct {
	ConnectTime    int64 `json:"connect_time,omitempty"`
	DisconnectTime int64 `json:"disconnect_time,omitempty"`
}

type CallbackDataType int32

const (
	CallbackDataType_Receive    CallbackDataType = 1
	CallbackDataType_Connect    CallbackDataType = 2
	CallbackDataType_Disconnect CallbackDataType = 3
)

type CallbackRequest struct {
	AppName  string           `json:"app_name,omitempty"`
	Topic    string           `json:"topic,omitempty"`
	UserId   string           `json:"user_id,omitempty"`
	DataType CallbackDataType `json:"data_type,omitempty"`
	Data     interface{}      `json:"data,omitempty"`
}

type GetTopicsRequest struct{}

type GetTopicsResponse struct{}
