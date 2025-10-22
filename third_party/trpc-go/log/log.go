package log

import (
	"context"
	stdlog "log"
)

// Context keys
type ctxKey string

// KeyTraceID is the context key used in accesslog
var KeyTraceID ctxKey = "trace_id"

// TID is the field name used in logs
const TID = "trace_id"

func Info(args ...interface{})                 { stdlog.Print(args...) }
func Infof(format string, args ...interface{}) { stdlog.Printf(format, args...) }
func InfoContextf(_ context.Context, format string, args ...interface{}) {
	stdlog.Printf(format, args...)
}

func Warnf(format string, args ...interface{}) { stdlog.Printf(format, args...) }
func WarnContextf(_ context.Context, format string, args ...interface{}) {
	stdlog.Printf(format, args...)
}

func Error(args ...interface{})                 { stdlog.Print(args...) }
func Errorf(format string, args ...interface{}) { stdlog.Printf(format, args...) }
func ErrorContextf(_ context.Context, format string, args ...interface{}) {
	stdlog.Printf(format, args...)
}

func Fatalf(format string, args ...interface{}) { stdlog.Fatalf(format, args...) }
