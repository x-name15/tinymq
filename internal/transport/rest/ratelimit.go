package rest

import (
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type tokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64
	lastRefill time.Time
}

type ipRateLimiter struct {
	mu      sync.RWMutex
	buckets map[string]*tokenBucket
	rate    float64
}

func newTokenBucket(rate float64) *tokenBucket {
	return &tokenBucket{
		tokens:     rate, // start full
		maxTokens:  rate,
		refillRate: rate,
		lastRefill: time.Now(),
	}
}

func (tb *tokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens += elapsed * tb.refillRate
	if tb.tokens > tb.maxTokens {
		tb.tokens = tb.maxTokens
	}
	tb.lastRefill = now

	if tb.tokens >= 1.0 {
		tb.tokens--
		return true
	}
	return false
}

func newIPRateLimiter(rate float64) *ipRateLimiter {
	rl := &ipRateLimiter{
		buckets: make(map[string]*tokenBucket),
		rate:    rate,
	}
	go rl.cleanupLoop()
	return rl
}

func (rl *ipRateLimiter) getBucket(ip string) *tokenBucket {
	rl.mu.RLock()
	if b, ok := rl.buckets[ip]; ok {
		rl.mu.RUnlock()
		return b
	}
	rl.mu.RUnlock()

	rl.mu.Lock()
	defer rl.mu.Unlock()

	if b, ok := rl.buckets[ip]; ok {
		return b
	}
	b := newTokenBucket(rl.rate)
	rl.buckets[ip] = b
	return b
}

func (rl *ipRateLimiter) Allow(ip string) bool {
	return rl.getBucket(ip).Allow()
}

func (rl *ipRateLimiter) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		rl.mu.Lock()
		now := time.Now()
		for ip, b := range rl.buckets {
			b.mu.Lock()
			idle := now.Sub(b.lastRefill) > 1*time.Minute
			b.mu.Unlock()
			if idle {
				delete(rl.buckets, ip)
			}
		}
		rl.mu.Unlock()
	}
}

func getRateLimitFromEnv() float64 {
	val := os.Getenv("TINYMQ_RATE_LIMIT")
	if val == "" {
		return 0
	}
	rate, err := strconv.ParseFloat(val, 64)
	if err != nil || rate <= 0 {
		log.Printf("[RateLimit] Invalid TINYMQ_RATE_LIMIT value '%s' — rate limiting disabled\n", val)
		return 0
	}
	return rate
}

func extractIP(r *http.Request) string {
	if os.Getenv("TINYMQ_TRUST_PROXY_HEADERS") == "true" {
		if realIP := r.Header.Get("X-Real-IP"); realIP != "" {
			return realIP
		}
	}
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}
