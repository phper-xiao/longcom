package ws

import "context"

// CtxKey context key
type CtxKey string

// WSCancel 用于标识ctx中关闭ws的value
const WSCancel = CtxKey("ws_cancel")

// Canceler 获取ctx
func Canceler(ctx context.Context) context.Context {
	cctx, ok := ctx.Value(WSCancel).(context.Context)
	if !ok {
		return cctx
	}
	return ctx
}
