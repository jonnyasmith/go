package cache_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cache "github.com/jonnyasmith/go/cache"
)

func BenchmarkStoreReadContention(b *testing.B) {
	benchmarkStoreContention(b, 100)
}

func BenchmarkStoreReadHeavyContention(b *testing.B) {
	benchmarkStoreContention(b, 95)
}

func BenchmarkStoreMixedContention(b *testing.B) {
	benchmarkStoreContention(b, 50)
}

var contentionCases = []struct {
	name       string
	allocating bool
	skewed     bool
}{
	{name: "GetAllocating/uniform-shards", allocating: true},
	{name: "GetAllocating/skewed-hot-shard", allocating: true, skewed: true},
	{name: "GetIntoReusable/uniform-shards"},
	{name: "GetIntoReusable/skewed-hot-shard", skewed: true},
}

func benchmarkStoreContention(b *testing.B, readPercent uint64) {
	for _, benchmark := range contentionCases {
		for _, goroutines := range []int{1, 2, 4, 8, 16} {
			b.Run(fmt.Sprintf("%s/goroutines=%d", benchmark.name, goroutines), func(b *testing.B) {
				store, err := cache.Open(context.Background(), b.TempDir(),
					cache.WithFlushInterval(time.Hour),
					cache.WithSnapshotThreshold(1<<62),
				)
				if err != nil {
					b.Fatalf("open: %v", err)
				}
				keys := make([]string, 1024)
				for index := range keys {
					keys[index] = fmt.Sprintf("key-%04d", index)
					if err := store.Set(keys[index], []byte("value")); err != nil {
						b.Fatalf("prefill %q: %v", keys[index], err)
					}
				}
				b.Cleanup(func() {
					if err := store.Close(); err != nil {
						b.Errorf("close: %v", err)
					}
				})

				b.ReportAllocs()
				b.ResetTimer()
				var next atomic.Uint64
				var workers sync.WaitGroup
				for range goroutines {
					workers.Add(1)
					go func() {
						defer workers.Done()
						dst := make([]byte, 0, len("value"))
						for {
							operation := next.Add(1) - 1
							if operation >= uint64(b.N) {
								return
							}
							keyIndex := operation & uint64(len(keys)-1)
							if benchmark.skewed && operation%10 != 0 {
								keyIndex = 0
							}
							key := keys[keyIndex]
							if operation%100 < readPercent {
								if benchmark.allocating {
									store.Get(key)
								} else {
									dst, _ = store.GetInto(key, dst)
								}
							} else if err := store.Set(key, []byte("value")); err != nil {
								b.Errorf("set %q: %v", key, err)
								return
							}
						}
					}()
				}
				workers.Wait()
			})
		}
	}
}
