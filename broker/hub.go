package broker

import (
	"context"
	"sync"

	"github.com/trpc-group/trpc-go/log"
	"go.uber.org/atomic"
)

type hubType int

const (
	// hub 类型
	// alias 类型的 hub, alias 意味着hub中conns数量较少
	hubTypeAlias = iota + 1
	// group 类型的 hub, 意味着 conns 数量较多
	hubTypeGroup
)

type topicRequest struct {
	request     []byte
	subscribeTs int64
	topicName   string
}

type hub struct {
	sync.RWMutex
	exitOnce sync.Once
	name     string
	hubType  hubType
	// 存储 Conn 实例 的映射关系
	conns     map[*Conn]bool
	broadcast chan []byte
	exit      chan int

	running *atomic.Bool

	topicSet      map[string]struct{}
	topicSetMutex sync.RWMutex
	topicPush     chan *topicRequest
}

func newHub(name string, hubType hubType) *hub {
	return &hub{
		name:     name,
		hubType:  hubType,
		exitOnce: sync.Once{},
		// conn 本身作为 Key
		conns: make(map[*Conn]bool),
		// broadcast channel 增加 buffer
		broadcast: make(chan []byte, 16),
		exit:      make(chan int),

		running: atomic.NewBool(false),

		topicSet:  make(map[string]struct{}),
		topicPush: make(chan *topicRequest, 16),
	}
}

func (h *hub) run() {
	ctx := context.Background()
	for {
		select {
		case request := <-h.broadcast:
			h.RLock()
			for conn := range h.conns {
				// 发送 frame
				err := conn.SendRequestBytes(ctx, request)
				if err != nil {
					log.WarnContextf(
						ctx,
						"hub|Boardcast|err: %v|connectionID: %s|alias: %v|request: %v", err, conn.connectionID,
						h.name, request,
					)
				}
			}
			h.RUnlock()
		case request := <-h.topicPush:
			h.RLock()
			requestBytes := request.request
			for conn := range h.conns {
				if conn.connectTime > request.subscribeTs {
					log.InfoContextf(ctx, "hub|topic: %v|alias: %v|connectionID: %s|connectTime: %v|subts: %v",
						request.topicName, h.name, conn.connectionID, conn.connectTime, request.subscribeTs)
					continue
				}
				err := conn.SendRequestBytes(ctx, requestBytes)
				log.DebugContextf(ctx, "hub|topic: %v|err: %v|connectionID: %s|alias: %v|request: %v",
					request.topicName, err, conn.connectionID, h.name, requestBytes)
				if err != nil {
					log.WarnContextf(ctx, "hub|topic: %v|err: %v|connectionID: %s|alias: %v|request: %v",
						request.topicName, err, conn.connectionID, h.name, requestBytes)
				}
			}
			h.RUnlock()
		case <-h.exit:
			return
		}
	}
}

func (h *hub) register(conn *Conn) {
	h.Lock()
	h.conns[conn] = true
	h.Unlock()
}

func (h *hub) unregister(conn *Conn) {
	h.Lock()
	delete(h.conns, conn)
	h.Unlock()
}

func (h *hub) close() {
	h.exitOnce.Do(func() {
		close(h.exit)
	})
}

func (h *hub) closed() bool {
	select {
	case <-h.exit:
		return true
	default:
		return false
	}
}

func (h *hub) len() int {
	h.RLock()
	defer h.RUnlock()

	return len(h.conns)
}

func (h *hub) started() bool {
	return h.running.Load()
}

func (h *hub) start() {
	// 这里使用 cas 没有问题，不会有 sync.Once 的 incorrect implementation 导致的问题
	// 因为hub其实已经实例化了
	if h.running.CAS(false, true) {
		go h.run()
	}
}

func (h *hub) subscribe(topics []string) {
	if h.closed() {
		return
	}
	h.topicSetMutex.Lock()
	for i := 0; i < len(topics); i++ {
		h.topicSet[topics[i]] = struct{}{}
	}
	h.topicSetMutex.Unlock()
}

func (h *hub) unsubscribe(topics []string) {
	if h.closed() {
		return
	}
	h.topicSetMutex.Lock()
	for i := 0; i < len(topics); i++ {
		delete(h.topicSet, topics[i])
	}
	h.topicSetMutex.Unlock()
}
