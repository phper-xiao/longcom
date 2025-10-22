// Package accesslog 在trpc框架外使用时，初始化日志
package accesslog

import (
	"fmt"
	"io/ioutil"

	"github.com/trpc-group/trpc-go/log"
	"gopkg.in/yaml.v2"
)

/*
配置文件示例如下
access_log:

	file_path: /data1/logs/ #控制台标准输出 默认
	file_name: rpc.log #标准输出日志的级别
*/
type config struct {
	Conf Config `yaml:"accesslog"`
}

// RegisterOutsideTrpc trpc框架外使用时，初始化日志
func RegisterOutsideTrpc(path string) error {
	yamlFile, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read file %s error:%v", path, err)
	}

	cfg := config{}
	err = yaml.Unmarshal(yamlFile, &cfg)
	if err != nil {
		return err
	}
	if cfg.Conf.LogPath == "" || cfg.Conf.LogName == "" {
		return fmt.Errorf("invalid config, please check 'accesslog' at %s", path)
	}
	log.Init(cfg.Conf.LogPath, cfg.Conf.LogName)
	return nil
}
