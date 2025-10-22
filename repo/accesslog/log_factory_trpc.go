// Package accesslog 在trpc框架内使用时，初始化日志并插入框架
package accesslog

import (
    "fmt"

    "github.com/trpc-group/trpc-go/log"
    "github.com/trpc-group/trpc-go/filter"
    "github.com/trpc-group/trpc-go/plugin"
)

const (
	//PluginName 插件名字
	PluginName = "accesslog"
	pluginType = "log"
)

func init() {
	plugin.Register(PluginName, &Plugin{})
}

// Plugin access_log  trpc 插件实现
type Plugin struct {
}

// Type access_log trpc插件类型
func (p *Plugin) Type() string {
	return pluginType
}

// Config access_log插件配置
type Config struct {
	LogPath string `yaml:"file_path"`
	LogName string `yaml:"file_name"`
}

// Setup access_log实例初始化
func (p *Plugin) Setup(name string, configDec plugin.Decoder) error {

	var conf Config
	err := configDec.Decode(&conf)
	if err != nil {
		return err
	}
	//初始化日志
	if len(conf.LogPath) == 0 || len(conf.LogName) == 0 {
		return fmt.Errorf("please config file_path and file_name under plugin in yaml file")
	}
    log.Init(conf.LogPath, conf.LogName)

	filter.Register(PluginName, ServerFilter(), ClientFilter())
	return nil
}
