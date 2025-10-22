package metrics

import "context"

// Metrics 指标上报
type Metrics interface {
	ReportConnectionCount(ctx context.Context, v int32)
	ReportReceivePackage(ctx context.Context, s int)
	ReportSendPackage(ctx context.Context, s int)
	ReportMemoryUsage(ctx context.Context)
}

// ZhiyanMetrics 为兼容保留的空实现，避免内网依赖
type ZhiyanMetrics struct{}

// NewZhiyanMetrics 返回空实现
func NewZhiyanMetrics() *ZhiyanMetrics {
	return &ZhiyanMetrics{}
}

func (m *ZhiyanMetrics) ReportConnectionCount(ctx context.Context, v int32) {}
func (m *ZhiyanMetrics) ReportReceivePackage(ctx context.Context, s int)    {}
func (m *ZhiyanMetrics) ReportSendPackage(ctx context.Context, s int)       {}
func (m *ZhiyanMetrics) ReportMemoryUsage(ctx context.Context)              {}
