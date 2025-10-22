package broker

import (
	"context"
	"strconv"
	"sync"
	"time"

    "github.com/tylerxiao/longcom/config"
    "github.com/tylerxiao/longcom/logic"
    "github.com/tylerxiao/longcom/repo/merrors"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"github.com/trpc-group/trpc-go/log"
)

// 按 business 区分 group
// businessGroupManager 只新增 business，不会清理 business
type businessGroupManager struct {
	buisnessGroup sync.Map

	PushByTopicGoroutinePoolSize int
}

func newBusinessGroupManager(cfg *config.Config) *businessGroupManager {
	return &businessGroupManager{PushByTopicGoroutinePoolSize: cfg.PushByTopicGoroutinePoolSize}
}

func (b *businessGroupManager) getGroupManager(ctx context.Context, business string) (*groupManager, error) {
	m, exist := b.buisnessGroup.Load(business)
	if exist {
		return m.(*groupManager), nil
	}

	// 不存在，再尝试 LoadOrStore
	// 重试过程中有可能存在别的并发创建，所以要使用 LoadOrStore
	m, err := newGroupManager(business, b.PushByTopicGoroutinePoolSize)
	if err != nil {
		log.ErrorContextf(ctx, "getGroupManager|newGroupManager failed, err: %v", err)
		return nil, merrors.ErrNewGroupManager
	}
	actualGroupManager, _ := b.buisnessGroup.LoadOrStore(business, m)
	gm := actualGroupManager.(*groupManager)
	gm.StartReport()
	return gm, nil
}

type aliasInfo struct {
	subscribeTs int64
}

// 管理 alias/group -> hub 的映射关系
type groupManager struct {
	businessName string // 业务名称标识

	allConns         sync.Map
	boardcastAllPool *ants.Pool

	aliasHubMutex      sync.RWMutex
	aliasHub           map[string]*hub
	boardcastAliasPool *ants.Pool // alias -> ants.Pool

	topicAliasInfoMap sync.Map   // topic -> alias -> aliasInfo
	topicPool         *ants.Pool // topic -> ants.Pool

	startReportOnce sync.Once
}

func newGroupManager(businessName string, pushByTopicGoroutinePoolSize int) (*groupManager, error) {
	// 魔数
	// ants pool 默认再 submit 时时 blocking 的，可以利用这个进行限流
	boardcastAllPool, err := ants.NewPool(51200)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	topicPool, err := ants.NewPool(pushByTopicGoroutinePoolSize)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	aliasPool, err := ants.NewPool(51200)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &groupManager{
		businessName:       businessName,
		boardcastAllPool:   boardcastAllPool,
		aliasHub:           make(map[string]*hub),
		topicPool:          topicPool,
		boardcastAliasPool: aliasPool,
	}, nil
}

// StartReport create ticker for report group manager base info
func (m *groupManager) StartReport() {
	m.startReportOnce.Do(func() {
		ticker := time.NewTicker(3 * time.Second)
		go func() {
			for {
				<-ticker.C
				log.Infof("groupManager|businessName=%s|aliasConnCount=%d", m.businessName, m.GetAliasConnCount())
			}
		}()
	})
}

// GetAliasConnCount return len of alias hub
func (m *groupManager) GetAliasConnCount() int {
	var count int
	m.aliasHubMutex.RLock()
	count = len(m.aliasHub)
	m.aliasHubMutex.RUnlock()
	return count
}

func (m *groupManager) addConn(ctx context.Context, connectionID string, conn *Conn) error {
	m.allConns.Store(connectionID, conn)
	return nil
}

func (m *groupManager) removeConn(ctx context.Context, connectionID string) error {
	m.allConns.Delete(connectionID)
	return nil
}

func (m *groupManager) addAliasConn(
	ctx context.Context,
	business string,
	nodeID string,
	alias string,
	conn *Conn,
	topics []string,
) error {
	log.DebugContextf(
		ctx,
		"addAliasConn|businessName=%s|alias=%s|connectionID=%s|",
		m.businessName,
		alias,
		conn.connectionID,
	)
	m.aliasHubMutex.Lock()
	defer m.aliasHubMutex.Unlock()
	// 查找 alias 对应的 hub 是否已经存在
	h, exist := m.aliasHub[alias]

	if !exist {
		// hub 不存在，以 alias 为 hub 名称，实例化一个hub
		h = newHub(alias, hubTypeAlias)
		// // h.run() 可以在 Broadcast 调用时再去运行，这样子没有使用推送的连接可以节省掉一个goroutine
		// go h.run()
		m.aliasHub[alias] = h
	}

	h.register(conn)
	if len(topics) > 0 {
		m.subscribeImpl(ctx, alias, topics, true, false)
	}

	if err := logic.SetUserState(
		business,
		"",
		alias,
		logic.UserStateField(nodeID, "connect"),
		strconv.FormatInt(time.Now().Unix(), 10),
	); err != nil {
		log.WarnContextf(
			ctx,
			"addAliasConn|businessName=%s|alias=%s|connectionID=%s|ExpireUserState|err=%+v",
			m.businessName,
			alias,
			conn.connectionID,
			err,
		)
	}

	return nil
}

func (m *groupManager) removeAliasConn(
	ctx context.Context,
	business string,
	nodeID string,
	alias string,
	conn *Conn,
) error {
	log.DebugContextf(
		ctx,
		"removeAliasConn|businessName=%s|alias=%s|connectionID=%s|",
		m.businessName,
		alias,
		conn.connectionID,
	)
	m.aliasHubMutex.Lock()
	defer m.aliasHubMutex.Unlock()
	// 查找 alias 对应的 hub 是否已经存在
	h, exist := m.aliasHub[alias]

	if !exist {
		// hub 不存在，无需处理
		return nil
	}

	// 要注意 unregister() 和 len() 之前，会不会有其他逻辑插入，导致 hub 发生变化
	h.unregister(conn)
	// 检查 hub 元素数量
	if h.len() == 0 {
		// 关闭hub
		h.close()
		// 清理 aliasHub
		delete(m.aliasHub, alias)
		for topic := range h.topicSet {
			aliasMapIF, ok := m.topicAliasInfoMap.Load(topic)
			if ok {
				aliasMap := aliasMapIF.(*sync.Map)
				aliasMap.Delete(alias)
			}
		}
		h.topicSet = nil
		if err := logic.ExpireUserState(
			business,
			"",
			alias,
			nodeID,
		); err != nil {
			log.WarnContextf(
				ctx,
				"removeAliasConn|businessName=%s|alias=%s|connectionID=%s|ExpireUserState|err=%+v",
				m.businessName,
				alias,
				conn.connectionID,
				err,
			)
		}

	}

	return nil
}

// BroadcastByAlias push by alias
func (m *groupManager) BroadcastByAlias(ctx context.Context, alias string, requestBytes []byte) error {
	err := m.boardcastAliasPool.Submit(func() {
		// 尽快解锁
		m.aliasHubMutex.RLock()
		h, exist := m.aliasHub[alias]
		m.aliasHubMutex.RUnlock()
		// hub 指针从 aliasHub 中取出来了，已经脱离了 aliasHubMutex, 注意 hub 相关逻辑
		if !exist {
			return
		}
		if h.closed() {
			return
		}

		if !h.started() {
			h.start()
		}

		select {
		case <-ctx.Done():
			log.WarnContextf(ctx, "BroadcastByAlias|Context Done|business=%s|alias=%s",
				m.businessName, alias)
			return
		// 监听 exit 事件
		case <-h.exit:
			log.WarnContextf(ctx, "BroadcastByAlias|hub exit|business=%s|alias=%s",
				m.businessName, alias)
			return
		// 消息写入hub
		case h.broadcast <- requestBytes:
		}
	})

	if err != nil {
		log.WarnContextf(
			ctx,
			"BroadcastByAlias|businessName=%s|alias=%s|err=%v",
			m.businessName,
			alias,
			err,
		)
	}

	return nil
}

// Broadcast push all alias
func (m *groupManager) Broadcast(ctx context.Context, requestBytes []byte) error {
	m.allConns.Range(func(key, value interface{}) bool {
		_ = m.boardcastAllPool.Submit(func() {
			conn := value.(*Conn)
			// m := copiedRequest.(*salmon.Request)
			err := conn.SendRequestBytes(
				ctx,
				requestBytes,
			)
			if err != nil {
				log.WarnContextf(
					ctx,
					"Broadcast|businessName=%s|connectionID=%s|remoteAddr=%s|err=%v",
					m.businessName,
					conn.connectionID,
					conn.RemoteAddr(),
					err,
				)
			}
		})
		return true
	})

	return nil
}

// BroadcastByTopic 组播
func (m *groupManager) BroadcastByTopic(ctx context.Context, topic string, request []byte) (err error) {
	aliasMapIF, ok := m.topicAliasInfoMap.Load(topic)
	if !ok {
		// log.DebugContextf(ctx, "BroadcastByTopic|topic: %v alias info not found", topic)
		return
	}
	aliasMap := aliasMapIF.(*sync.Map)
	aliasMap.Range(func(key, value interface{}) bool {
		errTmp := m.topicPool.Submit(func() {
			alias := key.(string)
			aliasInfo := value.(*aliasInfo)
			m.aliasHubMutex.RLock()
			h, exist := m.aliasHub[alias]
			m.aliasHubMutex.RUnlock()
			if !exist {
				return
			}
			if h.closed() {
				return
			}
			if !h.started() {
				h.start()
			}

			topicReq := &topicRequest{
				request:     request,
				subscribeTs: aliasInfo.subscribeTs,
				topicName:   topic,
			}
			select {
			case <-ctx.Done():
				log.WarnContextf(ctx, "BroadcastByTopic|BroadcastByTopic ctx Done|business=%s|alias=%s",
					m.businessName, h.name)
				return
			case <-h.exit:
				log.WarnContextf(ctx, "BroadcastByTopic|BroadcastByTopic ctx Done|business=%s|alias=%s",
					m.businessName, h.name)
				return
			case h.topicPush <- topicReq:
			}
		})
		if errTmp != nil {
			log.ErrorContextf(ctx, "BroadcastByTopic|range map failed, err: %v", err)
			return true
		}
		return true
	})
	return
}

func (m *groupManager) doWithTopicAliasInfo(ctx context.Context, alias string, topicList []string, isSubscribe bool,
	needLock bool) (
	h *hub, exist bool) {
	timeNow := time.Now()

	if needLock {
		m.aliasHubMutex.Lock()
		defer m.aliasHubMutex.Unlock()
	}

	h, exist = m.aliasHub[alias]
	if !exist {
		// log.DebugContextf(ctx, "doWithTopicAliasInfo|alias: %v hub not exist", alias)
		return
	}
	if h.closed() {
		// log.DebugContextf(ctx, "doWithTopicAliasInfo|alias: %v hub closed", alias)
		return
	}

	if !isSubscribe {
		for i := 0; i < len(topicList); i++ {
			aliasMapIF, ok := m.topicAliasInfoMap.Load(topicList[i])
			if ok {
				aliasMap := aliasMapIF.(*sync.Map)
				aliasMap.Delete(alias)
			}
		}
		return
	}
	for i := 0; i < len(topicList); i++ {
		aliasMapIF, _ := m.topicAliasInfoMap.LoadOrStore(topicList[i], &sync.Map{})
		aliasMap := aliasMapIF.(*sync.Map)
		aliasMap.Store(alias, &aliasInfo{subscribeTs: timeNow.Unix()})
	}
	return
}

func (m *groupManager) subscribeImpl(ctx context.Context, alias string, topicList []string, isSubscribe bool,
	needLock bool) {
	h, exist := m.doWithTopicAliasInfo(ctx, alias, topicList, isSubscribe, needLock)
	if !exist {
		return
	}
	if h.closed() {
		return
	}
	if isSubscribe {
		h.subscribe(topicList)
	} else {
		h.unsubscribe(topicList)
	}
}

// Subscribe 订阅
func (m *groupManager) Subscribe(ctx context.Context, alias string, topicList []string) {
	// log.DebugContextf(ctx, "Subscribe|alias: %v|business: %v|topicList: %v", alias, m.businessName, topicList)
	m.subscribeImpl(ctx, alias, topicList, true, true)
}

// UnSubscribe 取消订阅
func (m *groupManager) UnSubscribe(ctx context.Context, alias string, topicList []string) {
	// log.DebugContextf(ctx, "UnSubscribe|alias: %v|business: %v|topicList: %v", alias, m.businessName, topicList)
	m.subscribeImpl(ctx, alias, topicList, false, true)
}

// GetAliasConnectInfo return connection info by alias
func (m *groupManager) GetAliasConnectInfo(ctx context.Context, alias string) (conns []string, length int) {
	m.aliasHubMutex.RLock()
	h, exist := m.aliasHub[alias]
	m.aliasHubMutex.RUnlock()

	if !exist {
		// log.DebugContextf(ctx, "GetAliasConnectInfo|alias: %v hub not exist", alias)
		return
	}
	if h.closed() {
		// log.DebugContextf(ctx, "GetAliasConnectInfo|alias: %v hub closed", alias)
		return
	}

	h.RLock()
	length = len(h.conns)
	conns = make([]string, 0, length)
	for conn := range h.conns {
		conns = append(conns, conn.connectionID)
	}
	h.RUnlock()

	return
}
