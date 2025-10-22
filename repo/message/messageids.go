package message

import (
	"sync"
	"sync/atomic"
)

// MessageIDFuture Future 和 sequenceID 映射关系
type MessageIDFuture struct {
	currentID uint32
	index     sync.Map
}

// NewMessageIDFuture 初始化
func NewMessageIDFuture() *MessageIDFuture {
	return &MessageIDFuture{
		currentID: 0,
	}
}

// GenerateID 生成一个新的 sequenceID， 原子操作
func (r *MessageIDFuture) GenerateID() uint32 {
	sequenceID := atomic.AddUint32(&r.currentID, 1)
	return sequenceID
}

// SetFuture
// 如果一个 Future 被设置，但是一直没有 Free, 就会导致内存泄露
// 所以如果存在 SetFuture 调用，那么在任何情况下都必须调用 GetFutureAndFreeID 或 FreeFuture
func (r *MessageIDFuture) SetFuture(sequenceID uint32, p *Future) {
	r.index.Store(sequenceID, p)
}

// GetFutureAndFreeID 获取 Future, 并从 MessageIDFuture 释放掉
func (r *MessageIDFuture) GetFutureAndFreeID(i uint32) (*Future, bool) {
	future, ok := r.index.LoadAndDelete(i)
	if !ok {
		return nil, ok
	}
	return future.(*Future), ok
}

// FreeFuture 释放 Future
func (r *MessageIDFuture) FreeFuture(mid uint32) {
	r.index.Delete(mid)
}
