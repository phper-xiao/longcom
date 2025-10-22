global: #全局配置
  namespace: {{ data.namespace }} #环境类型，分正式production和非正式development两种类型
  env_name: test #环境名称，非正式环境下多环境的名称
  container_name: ${container_name} #容器名称, 占位符由运营平台替换成实际容器名
  local_ip: ${local_ip} #本地ip，容器内为容器ip，物理机或虚拟机为本机ip

server: #服务端配置
  app: svip #业务的应用名
  server: LongCom #进程服务名
  bin_path: /usr/local/trpc/bin/ #二进制可执行文件和框架配置文件所在路径
  conf_path: /usr/local/trpc/conf/ #业务配置文件所在路径
  data_path: /usr/local/trpc/data/ #业务数据文件所在路径
  admin:  #管理端口,对接天机阁上报cpu等指标
    nic: eth1
    port: 8081
  filter:
    - validation
    - recovery
    - skywalking
    - accesslog
    - zhiyan
  service: #业务服务提供的service，可以有多个
    - name: longcom
      nic: eth1
      port: 19039
      network: tcp #网络监听类型  tcp udp
      protocol: http #应用层协议 trpc http
      timeout: 10000 #请求最长处理时间 单位 毫秒
      idletime: 300000 #连接空闲时间 单位 毫秒
client:
  filter:
    - skywalking
    - accesslog
    - zhiyan
plugins: #插件配置
  log: #日志配置
    default: #默认日志的配置，可支持多输出
      - writer: console #控制台标准输出 默认
        level: debug #标准输出日志的级别
      - writer: file #本地文件日志
        level: info #本地文件滚动日志的级别
        formatter: json #标准输出日志的格式
        writer_config:
          filename: /data1/logs/SvipLongCom/trpc.log #本地文件滚动日志存放的路径
          max_size: 10 #本地文件滚动日志的大小 单位 MB
          max_backups: 10 #最大日志文件数
          max_age: 7 #最大日志保留天数
          compress: false #日志文件是否压缩
    accesslog:
      file_path: /data1/logs/SvipLongCom/
      file_name: run.log
  metrics: #监控配置
    zhiyan: #智研插件名字
      frameCode: trpc  #框架版本 trpc grpc等 [可选，默认为trpc]
      aModAppMark: {{ data.zhiyan.appmark }}
      aModMetricGroup: srv_active #自定义的主调上报指标组[可选，默认为空，即上报到default指标组]
      pModAppMark: {{ data.zhiyan.appmark }}
      pModMetricGroup: srv_passive #自定义的被调上报指标组[可选，默认为空，即上报到default指标组]
      env: "{{ data.zhiyan.env }}" #数据上报的智研监控环境。[可选，未配置为空，会上报值默认的生产环境]
      #智研上报sdk的配置[可选，如果不配置这个选项的话，那么默认使用下面的默认值]，详细情况请查看https://git.code.oa.com/zhiyan-monitor/sdk/go-sdk/tree/master
      agentConfig:
        agent:
          agent_socket_path: /data/zhiyan/agent/bin/agent_socket
          agent_string_socket_path: /data/zhiyan/agent/plugins/plugin_socket
  tracing:
   skywalking:
     server: svip_longcom # 服务名
     service: svip_longcom # 服务实例名 建议与服务名一致
     address: trace-collect-cluster.zhiyan.tencent-cloud.net:11800
     check_interval: 10s                           # 健康检查时间 默认20s
     max_send_queue_size:  100                     # 最大发送队列 默认30000
     component_id: 23                              # 组件Id 主要是图标 23 代表grpc服务
     auth: {{ data.skywalking_auth }}              # 认证用的token，如果没有可以不填
     sampler : 1                                   # 采样率，默认全部采样，sampler[0-1]