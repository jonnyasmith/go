package cache

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSnapshotRejectedWhenEvictionPrecedesUncapturedShard(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(context.Background(), dir,
		WithShards(2),
		WithCapacity(1<<20),
		WithSegmentSize(40),
		WithSnapshotThreshold(1<<20),
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	oldKey := testKeyForShard(store, 1, "old")
	newKey := testKeyForShard(store, 1, "new")
	if err := store.Set(oldKey, []byte("old-value")); err != nil {
		t.Fatalf("set old entry: %v", err)
	}
	for index := range 4 {
		key := testKeyForShard(store, 0, string(rune('a'+index)))
		if err := store.Set(key, []byte("roll")); err != nil {
			t.Fatalf("set rollover entry %d: %v", index, err)
		}
	}
	segmentsBefore, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil {
		t.Fatalf("list segments before snapshot: %v", err)
	}
	if len(segmentsBefore) < 2 {
		t.Fatalf("segment count before snapshot = %d; want at least 2", len(segmentsBefore))
	}

	store.shards[1].mu.Lock()
	store.shards[1].capacity = entrySize(store.shards[1].entries[oldKey])
	store.shards[1].mu.Unlock()

	store.shards[0].mu.Lock()
	snapshotDone := make(chan error, 1)
	go func() { snapshotDone <- store.writeSnapshot() }()
	deadline := time.Now().Add(time.Second)
	for {
		temporary, globErr := filepath.Glob(filepath.Join(dir, ".snapshot-*"))
		if globErr != nil {
			store.shards[0].mu.Unlock()
			t.Fatalf("list temporary snapshots: %v", globErr)
		}
		if len(temporary) != 0 {
			break
		}
		if time.Now().After(deadline) {
			store.shards[0].mu.Unlock()
			t.Fatal("snapshot did not begin")
		}
		time.Sleep(time.Millisecond)
	}

	if err := store.Set(newKey, []byte("new-value")); err != nil {
		store.shards[0].mu.Unlock()
		t.Fatalf("set entry that evicts: %v", err)
	}
	store.shards[0].mu.Unlock()
	if err := <-snapshotDone; err != nil {
		t.Fatalf("complete rejected snapshot attempt: %v", err)
	}
	if store.Stats().Evictions != 1 {
		t.Fatalf("evictions = %d; want 1", store.Stats().Evictions)
	}
	if store.Stats().Snapshots != 0 {
		t.Fatalf("successful snapshots = %d; want 0", store.Stats().Snapshots)
	}
	snapshots, err := filepath.Glob(filepath.Join(dir, "*.snap"))
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}
	if len(snapshots) != 0 {
		t.Fatalf("installed snapshots = %d; want 0", len(snapshots))
	}
	segmentsAfter, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil {
		t.Fatalf("list segments after snapshot: %v", err)
	}
	if len(segmentsAfter) < len(segmentsBefore) {
		t.Fatalf("segments after rejected snapshot = %d; want at least %d", len(segmentsAfter), len(segmentsBefore))
	}
	for _, retained := range segmentsBefore {
		if _, err := os.Stat(retained); err != nil {
			t.Fatalf("stat segment retained across rejected snapshot %q: %v", retained, err)
		}
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	recovered, err := Open(context.Background(), dir, WithShards(2), WithCapacity(1<<20))
	if err != nil {
		t.Fatalf("reopen with greater capacity: %v", err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	value, ok := recovered.Get(oldKey)
	if !ok || string(value) != "old-value" {
		t.Fatalf("recovered evicted entry = %q, %v; want old-value, true", value, ok)
	}
}

func TestRecoveryRejectsMissingLeadingHistoryWithoutSnapshot(t *testing.T) {
	dir := t.TempDir()
	writeTestSegment(t, dir, 2, encodeRecord(2, &writeRequest{kind: requestSet, key: "key", value: []byte("value")}))
	_, err := Open(context.Background(), dir)
	if err == nil || !strings.Contains(err.Error(), "sequence gap") {
		t.Fatalf("open error = %v; want sequence gap", err)
	}
}

func TestRecoveryRejectsMissingHistoryAfterSkewedSnapshot(t *testing.T) {
	dir := t.TempDir()
	writeTestSnapshotHeader(t, dir, []uint64{1, 2})
	_, err := Open(context.Background(), dir, WithShards(2))
	if err == nil || !strings.Contains(err.Error(), "sequence gap") {
		t.Fatalf("open error = %v; want sequence gap", err)
	}
}

func TestRecoveryRejectsRetainedHistoryEndingBeforeSkewedSnapshot(t *testing.T) {
	dir := t.TempDir()
	writeTestSnapshotHeader(t, dir, []uint64{1, 3})
	writeTestSegment(t, dir, 2, encodeRecord(2, &writeRequest{kind: requestSet, key: "key", value: []byte("value")}))
	_, err := Open(context.Background(), dir, WithShards(2))
	if err == nil || !strings.Contains(err.Error(), "sequence gap") {
		t.Fatalf("open error = %v; want sequence gap", err)
	}
}

func TestRecoveryAcceptsSnapshotOnlyWhenNoReplayIsRequired(t *testing.T) {
	dir := t.TempDir()
	writeTestSnapshotHeader(t, dir, []uint64{5, 5})
	store, err := Open(context.Background(), dir, WithShards(2))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := os.Stat(filepath.Join(dir, segmentName(6))); err != nil {
		t.Fatalf("stat next segment: %v", err)
	}
}

func TestChangedShardCountUsesSnapshotBaseWithoutReplay(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(context.Background(), dir, WithShards(2))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.Set("base", []byte("image")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	segments, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	for _, segment := range segments {
		if err := os.Remove(segment); err != nil {
			t.Fatalf("remove replay segment %q: %v", segment, err)
		}
	}

	recovered, err := Open(context.Background(), dir, WithShards(4))
	if err != nil {
		t.Fatalf("reopen with changed shard count: %v", err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	value, ok := recovered.Get("base")
	if !ok || string(value) != "image" {
		t.Fatalf("snapshot base value = %q, %v; want image, true", value, ok)
	}
}

func TestChangedShardCountReplaysHistoryAfterSnapshotBase(t *testing.T) {
	dir := t.TempDir()
	baseKey := testKeyForMask(1, 0, "base")
	replayedKey := testKeyForMask(1, 0, "replayed")
	highShardKey := testKeyForMask(1, 1, "high")
	writeTestSnapshot(t, dir, []uint64{1, 3},
		&entry{key: baseKey, value: []byte("image"), sequence: 1},
		&entry{key: highShardKey, value: []byte("latest"), sequence: 3},
	)
	writeTestSegment(t, dir, 2,
		encodeRecord(2, &writeRequest{kind: requestSet, key: replayedKey, value: []byte("history")}),
		encodeRecord(3, &writeRequest{kind: requestSet, key: highShardKey, value: []byte("latest")}),
	)

	store, err := Open(context.Background(), dir, WithShards(4))
	if err != nil {
		t.Fatalf("open with changed shard count: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if value, ok := store.Get(baseKey); !ok || string(value) != "image" {
		t.Fatalf("snapshot base value = %q, %v; want image, true", value, ok)
	}
	if value, ok := store.Get(replayedKey); !ok || string(value) != "history" {
		t.Fatalf("replayed value = %q, %v; want history, true", value, ok)
	}
}

func TestSegmentInstallationSyncFailureLatchesStore(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(context.Background(), dir, WithSegmentSize(9), WithSnapshotThreshold(1<<20))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.Set("first", []byte("value")); err != nil {
		t.Fatalf("first set: %v", err)
	}
	installErr := errors.New("injected directory sync failure")
	store.directorySync = func(string) error { return installErr }
	writeErr := store.Set("second", []byte("value"))
	if !errors.Is(writeErr, installErr) {
		t.Fatalf("rollover error = %v; want %v", writeErr, installErr)
	}
	if syncErr := store.Sync(); syncErr != writeErr {
		t.Fatalf("sync error = %v; want latched error %v", syncErr, writeErr)
	}
	store.directorySync = syncDirectory
	if err := store.Close(); err == nil {
		t.Fatal("close after segment installation failure unexpectedly succeeded")
	}
}

func TestSequenceFilenamesCoverUint64AndLegacyNamesRemainReadable(t *testing.T) {
	tests := []struct {
		sequence uint64
		segment  string
		snapshot string
	}{
		{99_999_999, "00000000000099999999.seg", "00000000000099999999.snap"},
		{100_000_000, "00000000000100000000.seg", "00000000000100000000.snap"},
		{maxSequence, "18446744073709551615.seg", "18446744073709551615.snap"},
	}
	for _, test := range tests {
		if got := segmentName(test.sequence); got != test.segment {
			t.Errorf("segmentName(%d) = %q; want %q", test.sequence, got, test.segment)
		}
		if got := snapshotName(test.sequence); got != test.snapshot {
			t.Errorf("snapshotName(%d) = %q; want %q", test.sequence, got, test.snapshot)
		}
	}

	dir := t.TempDir()
	legacyPath := filepath.Join(dir, "00000001.seg")
	writeTestSegmentAt(t, legacyPath, encodeRecord(1, &writeRequest{kind: requestSet, key: "legacy", value: []byte("value")}))
	store, err := Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("open legacy segment: %v", err)
	}
	value, ok := store.Get("legacy")
	if !ok || string(value) != "value" {
		t.Fatalf("legacy value = %q, %v; want value, true", value, ok)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close legacy store: %v", err)
	}

	snapshotDir := t.TempDir()
	snapshotKey := testKeyForMask(0, 0, "legacy-snapshot")
	writeTestSnapshotAt(t, filepath.Join(snapshotDir, "00000001.snap"), []uint64{1},
		&entry{key: snapshotKey, value: []byte("image"), sequence: 1},
	)
	snapshotStore, err := Open(context.Background(), snapshotDir, WithShards(1))
	if err != nil {
		t.Fatalf("open legacy snapshot: %v", err)
	}
	value, ok = snapshotStore.Get(snapshotKey)
	if !ok || string(value) != "image" {
		t.Fatalf("legacy snapshot value = %q, %v; want image, true", value, ok)
	}
	if err := snapshotStore.Close(); err != nil {
		t.Fatalf("close legacy snapshot store: %v", err)
	}
}

func TestMixedFilenameEncodingsRejectDuplicateSequence(t *testing.T) {
	dir := t.TempDir()
	writeTestSegmentAt(t, filepath.Join(dir, "00000001.seg"))
	writeTestSegmentAt(t, filepath.Join(dir, segmentName(1)))
	_, err := Open(context.Background(), dir)
	if err == nil || !strings.Contains(err.Error(), "duplicate segment sequence") {
		t.Fatalf("open error = %v; want duplicate segment sequence", err)
	}

	snapshotDir := t.TempDir()
	writeTestSnapshotAt(t, filepath.Join(snapshotDir, "00000001.snap"), []uint64{1})
	writeTestSnapshotAt(t, filepath.Join(snapshotDir, snapshotName(1)), []uint64{1})
	_, err = Open(context.Background(), snapshotDir)
	if err == nil || !strings.Contains(err.Error(), "duplicate snapshot sequence") {
		t.Fatalf("open error = %v; want duplicate snapshot sequence", err)
	}
}

func testKeyForShard(store *Store, shardIndex uint64, prefix string) string {
	for suffix := uint64(0); ; suffix++ {
		key := prefix + string(rune('a'+suffix%26)) + string(rune('a'+suffix/26))
		if hashKey(key)&store.shardMask == shardIndex {
			return key
		}
	}
}

func testKeyForMask(mask, shardIndex uint64, prefix string) string {
	for suffix := uint64(0); ; suffix++ {
		key := prefix + string(rune('a'+suffix%26)) + string(rune('a'+suffix/26))
		if hashKey(key)&mask == shardIndex {
			return key
		}
	}
}

func writeTestSnapshotHeader(t *testing.T, dir string, sequences []uint64) {
	t.Helper()
	writeTestSnapshot(t, dir, sequences)
}

func writeTestSnapshot(t *testing.T, dir string, sequences []uint64, entries ...*entry) {
	t.Helper()
	lowest := sequences[0]
	for _, sequence := range sequences[1:] {
		if sequence < lowest {
			lowest = sequence
		}
	}
	writeTestSnapshotAt(t, filepath.Join(dir, snapshotName(lowest)), sequences, entries...)
}

func writeTestSnapshotAt(t *testing.T, path string, sequences []uint64, entries ...*entry) {
	t.Helper()
	image := make([]byte, snapshotFixedHeaderSize+int64(len(sequences))*8)
	copy(image[:4], snapshotMagic[:])
	binary.LittleEndian.PutUint16(image[4:6], formatVersion)
	binary.LittleEndian.PutUint32(image[8:12], uint32(len(sequences)))
	for index, sequence := range sequences {
		binary.LittleEndian.PutUint64(image[snapshotFixedHeaderSize+int64(index)*8:], sequence)
	}
	for _, current := range entries {
		image = append(image, encodeSnapshotEntry(current)...)
	}
	if err := os.WriteFile(path, image, 0o600); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
}

func writeTestSegment(t *testing.T, dir string, firstSequence uint64, records ...[]byte) {
	t.Helper()
	writeTestSegmentAt(t, filepath.Join(dir, segmentName(firstSequence)), records...)
}

func writeTestSegmentAt(t *testing.T, path string, records ...[]byte) {
	t.Helper()
	image := make([]byte, segmentHeaderSize)
	copy(image[:4], segmentMagic[:])
	binary.LittleEndian.PutUint16(image[4:6], formatVersion)
	for _, record := range records {
		image = append(image, record...)
	}
	if err := os.WriteFile(path, image, 0o600); err != nil {
		t.Fatalf("write segment: %v", err)
	}
}
