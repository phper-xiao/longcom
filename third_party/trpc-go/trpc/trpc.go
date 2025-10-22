package trpc

import "github.com/tylerxiao/longcom/third_party/trpc-go/trpc/server"

// NewServer proxies to server.NewServer for compatibility with imports like:
//
//	trpc "github.com/trpc-group/trpc-go"
func NewServer(opts ...server.Option) *server.Server {
	return server.NewServer(opts...)
}
