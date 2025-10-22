package broker

const (
	NormalDisconnectReasonCode = iota
	HeartbeatTimeoutDisconnectReasonCode
)

const (
	NormalDisconnectReason           = ""
	HeartbeatTimeoutDisconnectReason = "Heartbeat Timeout"
)

// payload 类型
const (
	PayloadTypeJSON = "json" // json类型
	PayloadTypePB   = "pb"   // pb类型
)

type CallCloseFromType int

const (
	CallCloseFromLocal   = iota + 1 // 从 broker 内部关闭连接
	CallCloseFromBackend            // 后端服务通过调用 RPC 接口关闭连接
	CallCloseFromClient             // 客户端关闭连接
)
