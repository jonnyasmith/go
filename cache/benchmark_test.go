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

func BenchmarkStoreReadHeavy(b *testing.B) {
	benchmarkStoreContention(b, 95)
}

func BenchmarkStoreMixed(b *testing.B) {
	benchmarkStoreContention(b, 50)
}

func benchmarkStoreContention(b *testing.B, readPercent uint64) {
	for _, goroutines := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("goroutines=%d", goroutines), func(b *testing.B) {
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

			b.ReportAllocs()
			b.ResetTimer()
			var next atomic.Uint64
			var workers sync.WaitGroup
			for range goroutines {
				workers.Add(1)
				go func() {
					defer workers.Done()
					for {
						operation := next.Add(1) - 1
						if operation >= uint64(b.N) {
							return
						}
						key := keys[operation&uint64(len(keys)-1)]
						if operation%100 < readPercent {
							store.GetInto(key, nil)
						} else if err := store.Set(key, []byte("value")); err != nil {
							b.Errorf("set %q: %v", key, err)
							return
						}
					}
				}()
			}
			workers.Wait()
			b.StopTimer()
			if err := store.Close(); err != nil {
				b.Fatalf("close: %v", err)
			}
		})
	}
}
