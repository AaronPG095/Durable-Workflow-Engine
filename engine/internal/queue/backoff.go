package queue

import (
	"math/rand"
	"time"
)

// BackoffConfig controls exponential retry backoff with full jitter.
type BackoffConfig struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

// DefaultBackoffConfig returns sensible retry backoff defaults (250ms base, 30s cap).
func DefaultBackoffConfig() BackoffConfig {
	return BackoffConfig{
		BaseDelay: 250 * time.Millisecond,
		MaxDelay:  30 * time.Second,
	}
}

func (c BackoffConfig) normalized() BackoffConfig {
	base := c.BaseDelay
	if base <= 0 {
		base = 250 * time.Millisecond
	}
	maxD := c.MaxDelay
	if maxD <= 0 {
		maxD = 30 * time.Second
	}
	if maxD < base {
		maxD = base
	}
	return BackoffConfig{BaseDelay: base, MaxDelay: maxD}
}

// FullJitter computes a randomized delay in [0, min(MaxDelay, BaseDelay * 2^(attempt-1))].
// Overflow-safe for large attempt counts.
func FullJitter(attempt int, cfg BackoffConfig, rng *rand.Rand) time.Duration {
	c := cfg.normalized()
	if attempt < 1 {
		attempt = 1
	}

	capDelay := c.BaseDelay
	for i := 1; i < attempt; i++ {
		if capDelay >= c.MaxDelay/2 {
			capDelay = c.MaxDelay
			break
		}
		capDelay *= 2
	}
	if capDelay > c.MaxDelay {
		capDelay = c.MaxDelay
	}
	if capDelay <= 0 {
		return 0
	}

	if rng != nil {
		return time.Duration(rng.Int63n(int64(capDelay) + 1))
	}
	return time.Duration(rand.Int63n(int64(capDelay) + 1))
}
