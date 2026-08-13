package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	cache "github.com/jonnyasmith/go/cache"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "load" {
		fmt.Fprintln(os.Stderr, "usage: cached load -dir DIRECTORY")
		os.Exit(2)
	}
	flags := flag.NewFlagSet("load", flag.ExitOnError)
	dir := flags.String("dir", "", "store directory")
	_ = flags.Parse(os.Args[2:])
	if *dir == "" {
		fmt.Fprintln(os.Stderr, "cached load: -dir is required")
		os.Exit(2)
	}
	if err := runLoad(*dir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runLoad(dir string) error {
	store, err := cache.Open(context.Background(), dir)
	if err != nil {
		return fmt.Errorf("cached load: %w", err)
	}
	defer store.Close()
	for sequence := uint64(1); ; sequence++ {
		key := fmt.Sprintf("key-%020d", sequence)
		if err := store.Set(key, []byte(key)); err != nil {
			return fmt.Errorf("cached load: set %q: %w", key, err)
		}
		if _, err := fmt.Fprintln(os.Stdout, key); err != nil {
			return fmt.Errorf("cached load: acknowledge %q: %w", key, err)
		}
	}
}
