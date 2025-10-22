FROM csighub.tencentyun.com/tkex/tlinux3.2-bridge-tcloud-underlay-mini:latest
LABEL MAINTAINER="ekopei@tencent.com"

COPY bin /data/services/SvipLongCom/bin/
COPY config /data/services/SvipLongCom/config/

ENV TIME_ZONE=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TIME_ZONE /etc/localtime && echo $TIME_ZONE > /etc/timezone

RUN mkdir -p /data1/logs/SvipLongCom && \
    ln -snf /data1/logs/SvipLongCom /data/services/SvipLongCom/logs

WORKDIR /data/services/SvipLongCom

CMD [ "/bin/sh", "-c", "./bin/longcom -conf ./config/trpc_go.yaml", ">>", "/data1/logs/SvipLongCom/panic.log" ]