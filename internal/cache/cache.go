package cache

import "time"

type Interface interface {
	MGet(keys []string) (map[string][]byte, error)
	Set(key string, val []byte, ttl time.Duration) error
	Del(keys ...string) error
}
