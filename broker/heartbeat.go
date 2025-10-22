package broker

import (
    "container/heap"
    "context"
    "sync"
    "time"

    "github.com/trpc-group/trpc-go/log"
    "github.com/pkg/errors"
)

// 修改于 https://golang.org/pkg/container/heap/ 的 priority queue

// 最小堆优先队列
type PQItem struct {
	Value    *Conn // The value of the item; arbitrary.
	Priority int64 // 越小越优先
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A PriorityQueueImpl implements heap.Interface and holds Items.
type PriorityQueueImpl []*PQItem

// 最小堆 Len 接口的实现
func (pq PriorityQueueImpl) Len() int { return len(pq) }

// 最小堆 Less 接口的实现
func (pq PriorityQueueImpl) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}

// 最小堆 Swap 接口的实现
func (pq PriorityQueueImpl) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// 最小堆 Push 接口的实现
func (pq *PriorityQueueImpl) Push(x interface{}) {
	n := len(*pq)
	item := x.(*PQItem)
	item.index = n
	*pq = append(*pq, item)
}

// 最小堆 Pop 接口的实现
func (pq *PriorityQueueImpl) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueueImpl) update(item *PQItem, value *Conn, priority int64) {
	item.Value = value
	item.Priority = priority
	heap.Fix(pq, item.index)
}

// PriorityQueue queue of the *PriorityQueueImpl*
type PriorityQueue struct {
	pqimpl *PriorityQueueImpl
}

// NewPriorityQueue return a queue of the *PriorityQueueImpl*
func NewPriorityQueue() *PriorityQueue {
	pqi := &PriorityQueueImpl{}
	heap.Init(pqi)
	return &PriorityQueue{
		pqimpl: pqi,
	}
}

// Len return len of the *PriorityQueueImpl* queue
func (q *PriorityQueue) Len() int {
	return q.pqimpl.Len()
}

// Push element into the queue and up it
func (q *PriorityQueue) Push(el *PQItem) {
	heap.Push(q.pqimpl, el)
}

// Pop out the queue top element and swap other element
func (q *PriorityQueue) Pop() *PQItem {
	if q.Len() <= 0 {
		return nil
	}

	el := heap.Pop(q.pqimpl)
	return el.(*PQItem)
}

// func (q *PriorityQueue) Peek() *PQItem {
// 	if q.Len() <= 0 {
// 		return nil
// 	}

// 	return (*q.pqimpl)[0]
// }

// Remove removes and returns the element at index i from the heap.
func (q *PriorityQueue) Remove(el *PQItem) {
	heap.Remove(q.pqimpl, el.index)
}

//Update modifies the priority and value of an Item in the queue.
func (q *PriorityQueue) Update(el *PQItem, priority int64) {
	q.pqimpl.update(el, el.Value, priority)
}

type heartbeatTimerOPType int

const (
	heartbeatTimerOPTypeAddConn heartbeatTimerOPType = iota + 1
	heartbeatTimerOPTypeRemoveConn
	heartbeatTimerOPTypeUpdateConn
)

type heartbeatTimerOP struct {
	op             heartbeatTimerOPType
	conn           *Conn
	updatePriority int64
}

// HeartbeatTimer 心跳检查
type HeartbeatTimer struct {
	opC chan *heartbeatTimerOP
	pq  *PriorityQueue
	// 记录 连接 -> PQItem 关系
	// TODO: map value 是指针的话，可能会影响GC性能，这里后续 flightweight 优化下
	connToPQItem map[*Conn]*PQItem

	exitOnce sync.Once
	exit     chan int

	checkIntervalTicker *time.Ticker
	// 每次检查超时最多检查多少个连接
	maxCheckTimeoutConn int
}

// NewHeartbeatTimer create HeartbeatTimer
func NewHeartbeatTimer(checkInterval time.Duration, maxCheckTimeoutConn int) *HeartbeatTimer {
	pq := NewPriorityQueue()
	ticker := time.NewTicker(checkInterval)

	return &HeartbeatTimer{
		// 默认缓存 65535 个 op
		opC:          make(chan *heartbeatTimerOP, 65535),
		pq:           pq,
		connToPQItem: make(map[*Conn]*PQItem),

		exitOnce: sync.Once{},
		exit:     make(chan int),

		checkIntervalTicker: ticker,
		maxCheckTimeoutConn: maxCheckTimeoutConn,
	}
}

// Run 开始心跳检查
func (hb *HeartbeatTimer) Run() {
	for {
		select {
		case <-hb.exit:
			return
		case op := <-hb.opC:
			switch op.op {
			case heartbeatTimerOPTypeAddConn:
				hb.addConn(op.conn)
			case heartbeatTimerOPTypeRemoveConn:
				hb.removeConn(op.conn)
			case heartbeatTimerOPTypeUpdateConn:
				hb.updateConn(op.conn, op.updatePriority)
			}
		case <-hb.checkIntervalTicker.C:
			hb.checkTimeoutConn()
		}
	}

	hb.checkIntervalTicker.Stop()
}

// Close 关闭心跳检查
func (hb *HeartbeatTimer) Close() {
	hb.exitOnce.Do(func() {
		close(hb.exit)
	})
}

// AddConn 新连接建立，加入到心跳检查记录中
func (hb *HeartbeatTimer) AddConn(ctx context.Context, conn *Conn) error {
	select {
	case <-ctx.Done():
		return errors.New("Context Done")
	case <-hb.exit:
		return errors.New("Heartbeat Timer Exit")
	case hb.opC <- &heartbeatTimerOP{
		op:   heartbeatTimerOPTypeAddConn,
		conn: conn,
	}:
	}

	return nil
}

// RemoveConn 连接通道关闭，主动从心跳检查记录移除
func (hb *HeartbeatTimer) RemoveConn(ctx context.Context, conn *Conn) error {
	select {
	case <-ctx.Done():
		return errors.New("Context Done")
	case <-hb.exit:
		return errors.New("Heartbeat Timer Exit")
	case hb.opC <- &heartbeatTimerOP{
		op:   heartbeatTimerOPTypeRemoveConn,
		conn: conn,
	}:
	}

	return nil
}

// UpdateConn 更新连接
func (hb *HeartbeatTimer) UpdateConn(ctx context.Context, conn *Conn, updatePriority int64) error {
	select {
	case <-ctx.Done():
		return errors.New("Context Done")
	case <-hb.exit:
		return errors.New("Heartbeat Timer Exit")
	case hb.opC <- &heartbeatTimerOP{
		op:             heartbeatTimerOPTypeUpdateConn,
		conn:           conn,
		updatePriority: updatePriority,
	}:
	}
	return nil
}

// 仅限 heartbeat timer 内部调用, 因为这里没加锁，都收敛到 Run() Loop 里面序列化操作，避免过多的锁锁竞争
// 当连接创建时，把它增加到 心跳 timer 记录中
func (hb *HeartbeatTimer) addConn(conn *Conn) {
	pqItem := &PQItem{
		Value:    conn,
		Priority: conn.GetHeartbeatDeadline(),
	}

	hb.pq.Push(pqItem)
	hb.connToPQItem[conn] = pqItem
}

func (hb *HeartbeatTimer) removeConn(conn *Conn) {
	pqItem, ok := hb.connToPQItem[conn]
	if !ok {
		return
	}

	delete(hb.connToPQItem, conn)
	hb.pq.Remove(pqItem)

	return
}

// updateConn 更新连接
func (hb *HeartbeatTimer) updateConn(conn *Conn, updatePriority int64) {
	pqItem, ok := hb.connToPQItem[conn]
	if !ok {
		return
	}

	hb.pq.Update(pqItem, updatePriority)
}

func (hb *HeartbeatTimer) removeConnToPGItem(conn *Conn) {
	delete(hb.connToPQItem, conn)
	return
}

// 遍历 最小堆中 所有timer, 关闭超时的连接
func (hb *HeartbeatTimer) checkTimeoutConn() {
	now := time.Now().Unix()
	count := 0
	log.Infof(
		"checkTimeoutConn|len=%d|", hb.pq.Len(),
	)
	for {
		if count > hb.maxCheckTimeoutConn {
			break
		}
		count++

		//  防止 堆为空 时，内存错误导致 panic
		if hb.pq.Len() <= 0 {
			break
		}

		el := hb.pq.Pop()

		if el.Priority >= now {
			// 由于 优先队列的特性，当前队列头部的优先级(超时时间)已经大于 now，那么往后的元素都会大于 now
			// 也就是说，堆后面的元素都还没到超时检查时间
			// 放回去
			hb.pq.Push(el)
			break
		}

		conn := el.Value

		// 优先级表示到什么时候需要检查连接
		// 此时获取连接当前的 deadline，检查连接是否心跳超时了
		deadline := conn.GetHeartbeatDeadline()

		if deadline <= now {
			// ping 连接保活
			err := conn.Ping()
			if err == nil {
				// 更新收到 frame 的时间
				conn.lastReceiveFrameTime.Store(time.Now().Unix())
				// 重新放回队列里
				el.Priority = conn.GetHeartbeatDeadline()
				hb.pq.Push(el)
				continue
			}

			// 1 如果当前连接超时，那么关闭它
			log.Infof(
				"HeartbeatTimer|checkTimeoutConn|connectionID=%s|remoteAddr=%s|Ping err=%v",
				conn.connectionID,
				conn.RemoteAddr(),
				err,
			)
			err = conn.Close(
				context.Background(),
				CallCloseFromLocal,
				HeartbeatTimeoutDisconnectReasonCode,
				HeartbeatTimeoutDisconnectReason,
			)

			if err != nil {
				log.Warnf(
					"HeartbeatTimer|checkTimeoutConn|connectionID=%s|remoteAddr=%s|err=%+v|",
					conn.connectionID,
					conn.RemoteAddr(),
					err,
				)
			}
			// 移除连接记录
			hb.removeConnToPGItem(conn)
		} else {
			// 否则更新该连接的检查时间，重新放回队列里
			el.Priority = deadline
			hb.pq.Push(el)
		}
	}
}
