package server

import "context"

type Option func(*Server)

type Server struct {
	onShutdown func()
}

func NewServer(_ ...Option) *Server { return &Server{} }

type ServerHandleFunc func(ctx context.Context, req interface{}) (rsp interface{}, err error)

type ServerFilter func(ctx context.Context, req interface{}, next ServerHandleFunc) (rsp interface{}, err error)

func WithFilter(_ ServerFilter) Option { return func(*Server) {} }

func (s *Server) RegisterOnShutdown(f func()) { s.onShutdown = f }

func (s *Server) Serve() error { return nil }
