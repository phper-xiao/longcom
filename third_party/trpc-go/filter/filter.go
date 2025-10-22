package filter

import "context"

type ServerHandleFunc func(ctx context.Context, req interface{}) (rsp interface{}, err error)
type ServerFilter func(ctx context.Context, req interface{}, next ServerHandleFunc) (rsp interface{}, err error)

func Register(_ string, _ ...ServerFilter) {}
