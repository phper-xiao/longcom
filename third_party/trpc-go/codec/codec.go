package codec

import "context"

type message struct{ ctx context.Context }

func Message(ctx context.Context) *message { return &message{ctx: ctx} }

func (m *message) ServerRPCName() string                    { return "/" }
func (m *message) ClientRPCName() string                    { return "/" }
func (m *message) ServerMetaData() map[string][]byte        { return map[string][]byte{} }
func (m *message) ClientMetaData() map[string][]byte        { return map[string][]byte{} }
func (m *message) RemoteAddr() interface{ String() string } { return nil }
func (m *message) LocalAddr() interface{ String() string }  { return nil }
