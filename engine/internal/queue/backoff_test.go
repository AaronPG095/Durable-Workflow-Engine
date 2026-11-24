package queue

import (
	"math/rand"
	"testing"
	"time"
)

func TestFullJitterBounds(t *testing.T) {
	cfg := BackoffConfig{
		BaseDelay: 100 * time.Millisecond,
		MaxDelay:  1600 * time.Millisecond,
	}
	rng := rand.New(rand.NewSource(42))

	cases := []struct {
		attempt int
		maxCap  time.Duration
	}{
		{attempt: 0, maxCap: 100 * time.Millisecond},
		{attempt: 1, maxCap: 100 * time.Millisecond},
		{attempt: 2, maxCap: 200 * time.Millisecond},
		{attempt: 3, maxCap: 400 * time.Millisecond},
		{attempt: 4, maxCap: 800 * time.Millisecond},
		{attempt: 5, maxCap: 1600 * time.Millisecond},
		{attempt: 10, maxCap: 1600 * time.Millisecond},
		{attempt: 1000, maxCap: 1600 * time.Millisecond}, // overflow check
	}

	for _, tc := range cases {
		for i := 0; i < 50; i++ {
			d := FullJitter(tc.attempt, cfg, rng)
			if d < 0 || d > tc.maxCap {
				t.Fatalf("attempt %d: delay %v outside [0, %v]", tc.attempt, d, tc.maxCap)
			}
		}
	}
}

func TestFullJitterDefaults(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	d := FullJitter(1, BackoffConfig{}, rng)
	if d < 0 || d > 250*time.Millisecond {
		t.Fatalf("unexpected default delay %v", d)
	}
}
