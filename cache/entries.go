package cache

import "time"

const (
	entryOverhead      = uint64(64)
	sweepSampleSize    = 20
	sweepRepeatDivisor = 4
	sweepShardBudget   = time.Millisecond
)

type removalKind uint8

const (
	removalDelete removalKind = iota
	removalExpiry
	removalEviction
)

func (store *Store) getInto(key string, dst []byte) ([]byte, bool) {
	shard := store.shardFor(key)
	shard.mu.Lock()
	found := shard.entries[key]
	if found == nil {
		shard.mu.Unlock()
		store.stats.misses.Add(1)
		return dst[:0], false
	}
	if deadlinePassed(found.deadline, time.Now().UnixNano()) {
		store.removeEntryLocked(shard, found, removalExpiry)
		shard.mu.Unlock()
		store.stats.misses.Add(1)
		return dst[:0], false
	}
	store.markRecentLocked(shard, found)
	dst = append(dst[:0], found.value...)
	shard.mu.Unlock()
	store.stats.hits.Add(1)
	return dst, true
}

func (store *Store) applySet(key string, value []byte, deadline int64, sequence uint64) {
	shard := store.shardFor(key)
	shard.mu.Lock()
	current := store.setEntryLocked(shard, key, value, deadline, sequence)
	now := time.Now().UnixNano()
	if deadlinePassed(current.deadline, now) {
		store.removeEntryLocked(shard, current, removalExpiry)
	} else if shard.bytes > shard.capacity {
		store.evictToCapacityLocked(shard, now)
	}
	store.logSequence.Store(sequence)
	shard.mu.Unlock()
}

func (store *Store) applyRecoveredSet(key string, value []byte, deadline int64, sequence uint64) {
	shard := store.shardFor(key)
	shard.mu.Lock()
	_ = store.setEntryLocked(shard, key, value, deadline, sequence)
	shard.mu.Unlock()
}

func (store *Store) setEntryLocked(shard *shard, key string, value []byte, deadline int64, sequence uint64) *entry {
	if current := shard.entries[key]; current != nil {
		oldSize := entrySize(current)
		current.value = value
		current.deadline = deadline
		current.sequence = sequence
		newSize := entrySize(current)
		shard.bytes = shard.bytes - oldSize + newSize
		store.bytes.Add(int64(newSize) - int64(oldSize))
		store.markRecentLocked(shard, current)
		return current
	}

	current := &entry{key: key, value: value, deadline: deadline, sequence: sequence}
	shard.entries[key] = current
	store.markRecentLocked(shard, current)
	size := entrySize(current)
	shard.bytes += size
	store.entries.Add(1)
	store.bytes.Add(int64(size))
	return current
}

func (store *Store) applyDelete(key string, sequence uint64) {
	shard := store.shardFor(key)
	shard.mu.Lock()
	if current := shard.entries[key]; current != nil {
		store.removeEntryLocked(shard, current, removalDelete)
	}
	store.logSequence.Store(sequence)
	shard.mu.Unlock()
}

func (store *Store) enforceRecoveredState(now int64) {
	for index := range store.shards {
		shard := &store.shards[index]
		shard.mu.Lock()
		store.reclaimExpiredLocked(shard, now)
		store.evictToCapacityLocked(shard, now)
		shard.mu.Unlock()
	}
}

func (store *Store) reclaimExpiredLocked(shard *shard, now int64) {
	for _, current := range shard.entries {
		if deadlinePassed(current.deadline, now) {
			store.removeEntryLocked(shard, current, removalExpiry)
		}
	}
}

func (store *Store) evictToCapacityLocked(shard *shard, now int64) {
	for shard.bytes > shard.capacity {
		kind := removalEviction
		if deadlinePassed(shard.oldest.deadline, now) {
			kind = removalExpiry
		}
		store.removeEntryLocked(shard, shard.oldest, kind)
	}
}

func (store *Store) removeEntryLocked(shard *shard, current *entry, kind removalKind) {
	if current.previous != nil {
		current.previous.next = current.next
	} else {
		shard.recent = current.next
	}
	if current.next != nil {
		current.next.previous = current.previous
	} else {
		shard.oldest = current.previous
	}
	delete(shard.entries, current.key)
	size := entrySize(current)
	shard.bytes -= size
	store.entries.Add(^uint64(0))
	store.bytes.Add(-int64(size))

	switch kind {
	case removalExpiry:
		store.stats.expiries.Add(1)
	case removalEviction:
		store.evictionInterlock.Lock()
		store.evictionGeneration++
		store.evictionInterlock.Unlock()
		store.stats.evictions.Add(1)
	}
}

func (store *Store) markRecentLocked(shard *shard, current *entry) {
	if shard.recent == current {
		return
	}
	if current.previous != nil {
		current.previous.next = current.next
	}
	if current.next != nil {
		current.next.previous = current.previous
	} else if shard.oldest == current {
		shard.oldest = current.previous
	}
	current.previous = nil
	current.next = shard.recent
	if shard.recent != nil {
		shard.recent.previous = current
	} else {
		shard.oldest = current
	}
	shard.recent = current
}

func (store *Store) runSweep(interval time.Duration) {
	defer close(store.sweepDone)
	stagger := interval / time.Duration(len(store.shards))
	if stagger <= 0 {
		stagger = time.Nanosecond
	}
	ticker := time.NewTicker(stagger)
	defer ticker.Stop()

	shardIndex := 0
	for {
		select {
		case <-store.sweepStop:
			return
		case <-ticker.C:
			store.sweepShard(&store.shards[shardIndex])
			shardIndex = (shardIndex + 1) & int(store.shardMask)
		}
	}
}

func (store *Store) sweepShard(shard *shard) {
	budgetEnd := time.Now().Add(sweepShardBudget)
	for {
		now := time.Now().UnixNano()
		sampled := 0
		reclaimed := 0
		shard.mu.Lock()
		for _, current := range shard.entries {
			if sampled == sweepSampleSize || time.Now().After(budgetEnd) {
				break
			}
			sampled++
			if deadlinePassed(current.deadline, now) {
				store.removeEntryLocked(shard, current, removalExpiry)
				reclaimed++
			}
		}
		shard.mu.Unlock()
		if sampled == 0 || reclaimed*sweepRepeatDivisor < sampled || time.Now().After(budgetEnd) {
			return
		}
	}
}

func entrySize(current *entry) uint64 {
	return entryOverhead + uint64(len(current.key)) + uint64(len(current.value))
}

func deadlinePassed(deadline, now int64) bool {
	return deadline != 0 && now >= deadline
}
