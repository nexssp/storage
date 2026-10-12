package memory

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

// Tiny helpers used by the rate-limit backend. Kept in a separate
// file so the main memory.go stays focused on Backend definitions.

func parseInt(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid int %q: %w", s, err)
	}
	return n, nil
}

func parseRPS(s string) (float64, error) {
	if s == "" {
		return 1, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid rps %q: %w", s, err)
	}
	if f <= 0 {
		return 0, fmt.Errorf("rps must be positive, got %v", f)
	}
	return f, nil
}

// tokenBucket is a minimal token-bucket implementation. The Kernel
// already ships one (golang.org/x/time/rate); we duplicate the
// minimum here to keep the memory backend dependency-free for the
// prototype. A real backend (Redis) will use its own server-side
// algorithm and just return the Allow decision.
type tokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	last       time.Time
	refillPerS float64
	burst      int
}

func newTokenBucket(rps float64, burst int) *tokenBucket {
	return &tokenBucket{
		tokens:     float64(burst),
		last:       time.Now(),
		refillPerS: rps,
		burst:      burst,
	}
}

func (b *tokenBucket) allow(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	elapsed := now.Sub(b.last).Seconds()
	b.tokens += elapsed * b.refillPerS
	if b.tokens > float64(b.burst) {
		b.tokens = float64(b.burst)
	}
	b.last = now

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}
