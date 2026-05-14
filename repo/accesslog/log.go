package accesslog

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"trpc.group/trpc-go/trpc-go/codec"
	"trpc.group/trpc-go/trpc-go/filter"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	filterTypeServer = 1
	filterTypeClient = 2
)

type filterType int

var logger, _ = zap.NewProduction()

type traceIDKey struct{}

func init() {
	filter.Register("accesslog", ServerFilter(), ClientFilter())
}

// ServerFilter 打印服务端日志
func ServerFilter() filter.ServerFilter {
	return func(ctx context.Context, req interface{}, handler filter.ServerHandleFunc) (rsp interface{}, err error) {
		startTime := time.Now()

		// 生成trace_id
		var traceID string
		ctx, traceID = getTraceID(ctx)
		rsp, err = handler(ctx, req)
		var (
			method, remoteAddr, localAddr string
		)
		msg := codec.Message(ctx)
		method = msg.ServerRPCName()

		metaData := msg.ServerMetaData()
		metaDataWithStringValue := map[string]string{}
		for mkey, value := range metaData {
			metaDataWithStringValue[mkey] = string(value)
		}

		if msg.RemoteAddr() != nil {
			remoteAddr = msg.RemoteAddr().String()
		}

		if msg.LocalAddr() != nil {
			localAddr = msg.LocalAddr().String()
		}

		reqData, _ := json.Marshal(req)
		rspData, _ := json.Marshal(rsp)

		cost := time.Since(startTime).Milliseconds()

		// 探测请求不打日志
		if method == "/" {
			return
		}

		zapFields := []zapcore.Field{
			zap.String("remote_ip", remoteAddr),
			zap.String("local_ip", localAddr),
			zap.String("method", method),
			zap.Int64("cost_ms", cost),
			zap.ByteString("rsp", rspData),
			zap.Any("meta", metaDataWithStringValue),
			zap.String("tid", traceID),
		}

		if len(reqData) < 1000 {
			zapFields = append(zapFields, zap.ByteString("req", reqData))
		}

		if err != nil {
			zapFields = append(zapFields, zap.Error(err))
			logger.Error("access_log", zapFields...)
			return
		}
		logger.Info("access_log", zapFields...)
		return
	}
}

// ClientFilter 打印客户端端日志
func ClientFilter() filter.ClientFilter {
	return func(ctx context.Context, req, rsp interface{}, handler filter.ClientHandleFunc) (err error) {
		startTime := time.Now()

		// 生成trace_id
		var traceID string
		ctx, traceID = getTraceID(ctx)
		err = handler(ctx, req, rsp)
		var (
			method, remoteAddr, localAddr string
		)
		msg := codec.Message(ctx)

		method = msg.ClientRPCName()
		metaData := msg.ClientMetaData()

		metaDataWithStringValue := map[string]string{}
		for mkey, value := range metaData {
			metaDataWithStringValue[mkey] = string(value)
		}

		if msg.RemoteAddr() != nil {
			remoteAddr = msg.RemoteAddr().String()
		}

		if msg.LocalAddr() != nil {
			localAddr = msg.LocalAddr().String()
		}

		reqData, _ := json.Marshal(req)
		rspData, _ := json.Marshal(rsp)

		cost := time.Since(startTime).Milliseconds()

		zapFields := []zapcore.Field{
			zap.String("remote_ip", remoteAddr),
			zap.String("local_ip", localAddr),
			zap.String("method", method),
			zap.Int64("cost_ms", cost),
			zap.ByteString("req", reqData),
			zap.ByteString("rsp", rspData),
			zap.Any("meta", metaDataWithStringValue),
			zap.String("tid", traceID),
		}
		if err != nil {
			zapFields = append(zapFields, zap.Error(err))
			logger.Error("rpc_log", zapFields...)
			return
		}
		logger.Info("rpc_log", zapFields...)
		return
	}
}

func getTraceID(ctx context.Context) (context.Context, string) {
	var traceID string
	if traceID, ok := ctx.Value(traceIDKey{}).(string); ok {
		return ctx, traceID
	}

	traceID = uuid.New().String()
	ctx = context.WithValue(ctx, traceIDKey{}, traceID)
	return ctx, traceID
}
