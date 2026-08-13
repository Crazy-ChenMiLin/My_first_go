package resilience

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// RetryConfig 控制退避重试行为。
type RetryConfig struct {
	MaxAttempts int           // 总尝试次数（含首次）
	BaseDelay   time.Duration // 首次退避基准
	MaxDelay    time.Duration // 退避上限
	Jitter      float64       // 抖动比例 0~1
}

func DefaultRetry() RetryConfig {
	return RetryConfig{MaxAttempts: 3, BaseDelay: 120 * time.Millisecond, MaxDelay: 2 * time.Second, Jitter: 0.3}
}

// Retryable 标记「这个错误值得重试」。下游 5xx、连接重置属于此类；
// 400 参数错误、内容审核拒绝重试多少次都一样，不该重试。
type Retryable struct{ Err error }

func (r Retryable) Error() string { return r.Err.Error() }
func (r Retryable) Unwrap() error { return r.Err }

// MarkRetryable 包装一个可重试错误。
func MarkRetryable(err error) error {
	if err == nil {
		return nil
	}
	return Retryable{Err: err}
}

// Do 执行带指数退避的重试。
//
// 三条铁律：
//  1. context 结束立刻停止，绝不在已取消的链路上继续重试；
//  2. 只重试被显式标记为 Retryable 的错误；
//  3. 退避要加抖动，否则大量客户端会在同一毫秒同时重试，把刚缓过来的下游再打死（惊群）。
func Do(ctx context.Context, cfg RetryConfig, fn func(ctx context.Context, attempt int) error) error {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	var lastErr error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return lastErr
			}
			return err
		}
		err := fn(ctx, attempt)
		if err == nil {
			return nil
		}
		lastErr = err

		var r Retryable
		if !errors.As(err, &r) {
			return err // 不可重试，直接上抛
		}
		if attempt == cfg.MaxAttempts {
			break
		}

		delay := backoff(cfg, attempt)
		// 退避前先看 context 还剩多少预算：如果睡完就超时了，不如现在就失败
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) < delay {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
	}
	return lastErr
}

func backoff(cfg RetryConfig, attempt int) time.Duration {
	d := cfg.BaseDelay
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= cfg.MaxDelay {
			d = cfg.MaxDelay
			break
		}
	}
	if cfg.Jitter > 0 {
		j := (rand.Float64()*2 - 1) * cfg.Jitter // [-jitter, +jitter]
		d = time.Duration(float64(d) * (1 + j))
	}
	if d < 0 {
		d = cfg.BaseDelay
	}
	return d
}
