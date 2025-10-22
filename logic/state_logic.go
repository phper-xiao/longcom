package logic

import (
	"bytes"
	"fmt"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
)

// UserState 用户状态
type UserState struct {
	Node           string
	ConnectTime    int64
	DisConnectTime int64
}

const (
	cacheKeyState    = "{longcom}:ws:state:%s:%s:%s" // appname:topic:userid  状态(hash)
	cacheExpireState = 3600
)

// GetUserState  获取用户状态
func GetUserState(appName, topic, userID string) (UserState, error) {
	key := fmt.Sprintf(cacheKeyState, appName, topic, userID)
	// 拿到redis链接
	rd, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		return ret, err
	}
	defer rd.Close()

	var ret UserState
	reply, err := rd.Do("hGetAll", key)
	if err != nil {
		return ret, err
	}

	// 解析成map结构
	h := valueMap(reply)
	if len(h) == 0 {
		return ret, nil
	}
	//  最早一次在线中的连接或最后一次已关闭的连接
	for _, s := range h {
		if ret.ConnectTime == 0 {
			ret = *s
			continue
		}

		if s.DisConnectTime == 0 {
			// 在线
			if s.ConnectTime < ret.ConnectTime {
				ret = *s
			}
		} else if s.DisConnectTime > 0 && ret.DisConnectTime > 0 {
			// 不在线
			if ret.DisConnectTime < s.DisConnectTime {
				ret = *s
			}
		}
	}

	return ret, nil
}

// valueMap 解析成map结构
func valueMap(reply interface{}) map[string]*UserState {
	values, _ := reply.([]interface{})
	if len(values) == 0 || len(values)%2 != 0 {
		return nil
	}
	h := make(map[string]*UserState, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, okKey := values[i].([]byte)
		value, okValue := values[i+1].([]byte)
		if !okKey || !okValue {
			continue
		}

		idx := bytes.IndexByte(key, '_')
		if idx <= 0 || idx+1 > len(key) {
			continue
		}
		val, _ := strconv.ParseInt(string(value), 10, 0)
		if val <= 0 {
			continue
		}

		node := string(key[:idx])
		s, ok := h[node]
		if !ok {
			s = &UserState{Node: node}
			h[node] = s
		}

		if string(key[idx+1:]) == "connect" {
			s.ConnectTime = val
		} else if string(key[idx+1:]) == "disconnect" {
			s.DisConnectTime = val
		}
	}
	return h
}

const (
	expireStateScript = `
redis.call('hset', KEYS[1], KEYS[3], ARGV[1])
if (redis.call('hlen', KEYS[1])>2)
then
  redis.call('hdel', KEYS[1], KEYS[2], KEYS[3])
  return 0
else
  redis.call('expire', KEYS[1], ARGV[2])
  return 1
end
`
)

// UserStateField 状态字段
func UserStateField(node, f string) string {
	// {longcom} hash tag保持一致
	// redis 多个shard会hash到一个分片里面
	return fmt.Sprintf("{longcom}%s_%s", node, f)
}

// SetUserState 设置用户状态
func SetUserState(appName, topic, userID string, field, value string) error {
	key := fmt.Sprintf(cacheKeyState, appName, topic, userID)

	rd, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		return err
	}
	defer rd.Close()

	_, err = rd.Do("hSet", key, field, value)
	return err
}

// ExpireUserState 设置过期
func ExpireUserState(appName, topic, userID string, node string) error {
	key := fmt.Sprintf(cacheKeyState, appName, topic, userID)

	rd, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		return err
	}
	defer rd.Close()

	field1 := UserStateField(node, "connect")
	field2 := UserStateField(node, "disconnect")
	now := time.Now().Unix()

	_, err = rd.Do("eval", expireStateScript, 3, key, field1, field2, now, cacheExpireState)
	return err
}
