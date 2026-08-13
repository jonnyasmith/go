package cache_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	cache "github.com/jonnyasmith/go/cache"
)

func FuzzOpenArbitraryPersistentFile(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("CWAL\x01\x00\x00\x00"))
	f.Add([]byte("CSNP\x01\x00\x00\x00\x01\x00\x00\x00"))
	f.Fuzz(func(t *testing.T, image []byte) {
		for _, name := range []string{"00000001.seg", "00000001.snap"} {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, name), image, 0o600); err != nil {
				t.Fatalf("write %s: %v", name, err)
			}
			store, err := cache.Open(context.Background(), dir)
			if err == nil {
				if closeErr := store.Close(); closeErr != nil {
					t.Fatalf("close after opening %s: %v", name, closeErr)
				}
			}
		}
	})
}
