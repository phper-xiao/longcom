#!/bin/bash
# 启用了 validate.proto，需要安装protoc-gen-secv插件
# go get git.code.oa.com/devsec/protoc-gen-secv

appName=longcom

trpc create --alias -k -f --protofile=${appName}.proto --rpconly --mock=false -o ./${appName}
sed -i 's/,omitempty//g' ./${appName}/*.pb.go

# 用mockgen生成mock, 原trpc cmd的路经有问题
mockgen -source ./${appName}/${appName}.trpc.go \
	-destination ./${appName}/${appName}.mock.go \
	-package svip_${appName}

git clone https://git.code.oa.com/up-common/rpcprotocol.git
mkdir -p rpcprotocol/svip_${appName}/
cp ${appName}/*.go rpcprotocol/svip_${appName}/
cp *.proto rpcprotocol/svip_${appName}/

cd rpcprotocol/svip_${appName}
go mod init git.code.oa.com/up-common/rpcprotocol/svip_${appName}
# go get -u
go mod tidy
git add .
git commit -m "update svip_${appName} proto"
git push

cd ../..
rm -rf rpcprotocol/ && rm -rf ${appName}/
