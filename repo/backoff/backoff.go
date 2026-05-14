// Package backoff TODO
package backoff

import (
	"context"
	"time"

	"trpc.group/trpc-go/trpc-go/log"
	"github.com/cenkalti/backoff/v4"
)

// Retry 报错时重试，使用指数退避算法的重试
func Retry(ctx context.Context, maxRetry int, fn func() error) error {
	b := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(maxRetry))
	b = backoff.WithContext(b, ctx)
	return backoff.RetryNotify(fn, b, func(err error, duration time.Duration) {
		log.WarnContextf(ctx, "Backoff retry|duration=%d|err=%v", duration, err)
	})
}
