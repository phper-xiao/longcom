package wrapper

import (
	"sync"
)

// WaitGroupWrapper
type WaitGroupWrapper struct {
	sync.WaitGroup
}

// Wrap with cb func
func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}
