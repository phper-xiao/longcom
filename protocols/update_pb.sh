#!/bin/bash
# 启用了 validate.proto，需要安装protoc-gen-secv插件
# go get git.code.oa.com/devsec/protoc-gen-secv

appName=longcom

trpc create --alias -k -f --protofile=${appName}.proto --rpconly --mock=false -o ./
sed -i '' 's/,omitempty//g' ./*.pb.go
