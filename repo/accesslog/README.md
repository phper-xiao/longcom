# access_log 
因https://git.code.oa.com/trpc-go/trpc-filter/tree/master/debuglog 不支持trace_id，且我们习惯使用zap log，参考实现。

1.自动打印access日志,包括服务收到的请求，以及调用第三方服务。将ctx在整个业务逻辑中传递，可用trace_id串起来某个请求的所有日志
2.打印业务日志

## 使用说明:

 - 1.在main.go增加import
```go
import (
   _ "git.code.oa.com/svip/common/trpc/filter/access_log"
)
func main(){
    trpc.NewServer() //初始化日志，建议在main函数最开始调用，否则可能丢失初始化的日志
    ...初始化
    s.Serve()
}

```
 - 2.修改TRPC框架配置文件
```trpc_go.yaml
server:
 ...
 filter:
  - access_log   #打印服务端access日志

client:
 ...
 filter:
  - access_log   #打印调用第三方服务的rpc access日志

plugins:                                          #插件配置
  log:                                            #日志配置
    access_log:                                      #access_log日志的配置，注意和default（trpc框架默认日志）区分开
        file_path: /data1/logs/vdesk_ticket_logic/    #文件路径
        file_name: zticket_svr.log                    #文件名
```
 - 3.打印其他日志

 ```go
import (
    "git.code.oa.com/svip/common/utils/logger"  //已经在插件中初始化
)
//推荐使用T系列，可通过trace_id把某个请求的日志都串起来.ctx向后传递
logger.InfoT(ctx,...) 
logger.WarnT(ctx,...)
```
