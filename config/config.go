// Package config define
package config

import (
    "os"

    "github.com/tylerxiao/longcom/repo/mq/kafka"
    "github.com/BurntSushi/toml"
)

// Config define
type Config struct {
	Env                          string                        `toml:"env"`
	MaxConn                      int32                         `toml:"max_conn"`
	JWTSecret                    string                        `toml:"jwt_secret"`
	TCPAddress                   string                        `toml:"tcp_address"`
	PushByTopicGoroutinePoolSize int                           `toml:"topic_push_pool"`
	KafkaMessageQueueConfig      kafka.KafkaMessageQueueConfig `toml:"mq_config"`
}

// Env string
var Env string

// MaxConn 最大连接数
var MaxConn int32

var JWTSecret []byte

var ServerConfig *Config

// init config
func init() {
	confFile := "./config/config.toml"
	// 读取配置文件, 解决跑测试的时候找不到配置文件的问题，最多往上找10层目录
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(confFile); err == nil {
			break
		} else {
			confFile = "../" + confFile
		}
	}

	var conf Config
	if _, err := toml.DecodeFile(confFile, &conf); err != nil {
		panic(err)
	}

	ServerConfig = &conf
	Env = conf.Env
	JWTSecret = []byte(conf.JWTSecret)
	MaxConn = conf.MaxConn
	if MaxConn <= 0 {
		MaxConn = 10000
	}
}
