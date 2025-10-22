package message

import (
	"context"
	"errors"
	"go.uber.org/atomic"
)

type FutureType int32

const (
	FutureTypeLocal FutureType = iota + 1
)

//定义 Future
type Future struct {
	terminated atomic.Bool
	errC       chan error
	valueC     chan interface{}
}

// NewFuture 创建 Future
func NewFuture() *Future {
	return &Future{
		errC:   make(chan error, 1),
		valueC: make(chan interface{}, 1),
	}
}

// Type 获取 Future 类型
func (p *Future) Type() FutureType {
	return FutureTypeLocal
}

// Get 从 Future 中获取结果，并支持监听 context.Done 实现超时，流程控制
func (p *Future) Get(ctx context.Context) (interface{}, error) {
	select {
	case <-ctx.Done():
		return nil, errors.New("context done")
	case err := <-p.errC:
		// 返回原err, 不要 WithStack 包裹
		return nil, err
	case b := <-p.valueC:
		return b, nil
	}
}

// 如果 future 被创建出来后，没有被 Invoke, 那么会导致 Channel 未被正确关闭，导致内存泄漏
func (p *Future) Terminate() {
	if p.terminated.CAS(false, true) {
		p.errC <- errors.New("Future Terminated")
		close(p.errC)
		close(p.valueC)
	}
}

// 触发 Future，关闭 Channel
func (p *Future) Invoke(ctx context.Context, value interface{}, err error) error {
	// cas 检测 是否曾经 terminated
	if !p.terminated.CAS(false, true) {
		return errors.New("Future Invoke Multi Times")
	}

	if err != nil {
		p.errC <- err
	} else {
		p.valueC <- value
	}
	close(p.errC)
	close(p.valueC)
	return nil
}
