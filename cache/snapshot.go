package cache

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	snapshotFixedHeaderSize = int64(12)
	snapshotLengthSize      = 4
	snapshotCRCOffset       = 4
	snapshotKeyLengthOffset = 8
	snapshotDeadlineOffset  = 10
	snapshotSequenceOffset  = 18
	snapshotKeyOffset       = 26
	snapshotRecordFixedSize = snapshotKeyOffset
)

var snapshotMagic = [4]byte{'C', 'S', 'N', 'P'}

type snapshotFile struct {
	path           string
	lowestSequence uint64
}

func (store *Store) startSnapshot() bool {
	if store.stats.evictions.Load() != 0 {
		return false
	}
	if !store.snapshotRunning.CompareAndSwap(false, true) {
		return false
	}
	store.snapshotWG.Add(1)
	go func() {
		defer store.snapshotWG.Done()
		defer store.snapshotRunning.Store(false)
		if err := store.writeSnapshot(); err != nil {
			store.stats.lastSnapshotError.Store(&errorState{err: err})
			if store.logger != nil {
				store.logger.Error("write automatic snapshot", "error", err)
			}
		}
	}()
	return true
}

func (store *Store) writeSnapshot() error {

	file, err := os.CreateTemp(store.dir, ".snapshot-*")
	if err != nil {
		return fmt.Errorf("cache: create snapshot temporary file: %w", err)
	}
	temporaryPath := file.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	fail := func(err error) error {
		_ = file.Close()
		return err
	}
	if err := file.Chmod(0o600); err != nil {
		return fail(fmt.Errorf("cache: set snapshot permissions %q: %w", temporaryPath, err))
	}

	headerSize := snapshotFixedHeaderSize + int64(len(store.shards))*8
	header := make([]byte, headerSize)
	copy(header[:4], snapshotMagic[:])
	binary.LittleEndian.PutUint16(header[4:6], formatVersion)
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(store.shards)))
	if err := writeFull(file, header); err != nil {
		return fail(fmt.Errorf("cache: write snapshot header %q: %w", temporaryPath, err))
	}

	sequences := make([]uint64, len(store.shards))
	for index := range store.shards {
		shard := &store.shards[index]
		shard.mu.RLock()
		sequences[index] = store.logSequence.Load()
		for _, current := range shard.entries {
			if err := writeFull(file, encodeSnapshotEntry(current)); err != nil {
				shard.mu.RUnlock()
				return fail(fmt.Errorf("cache: write snapshot %q: %w", temporaryPath, err))
			}
		}
		shard.mu.RUnlock()
	}
	lowest := sequences[0]
	for index, sequence := range sequences {
		binary.LittleEndian.PutUint64(header[snapshotFixedHeaderSize+int64(index)*8:], sequence)
		if sequence < lowest {
			lowest = sequence
		}
	}
	if _, err := file.WriteAt(header, 0); err != nil {
		return fail(fmt.Errorf("cache: finalize snapshot header %q: %w", temporaryPath, err))
	}
	if err := file.Sync(); err != nil {
		return fail(fmt.Errorf("cache: sync snapshot %q: %w", temporaryPath, err))
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("cache: close snapshot %q: %w", temporaryPath, err)
	}

	target := filepath.Join(store.dir, snapshotName(lowest))
	if err := os.Rename(temporaryPath, target); err != nil {
		return fmt.Errorf("cache: install snapshot %q: %w", target, err)
	}
	removeTemporary = false
	if err := syncDirectory(store.dir); err != nil {
		return err
	}
	if err := removeOtherSnapshots(store.dir, target); err != nil {
		return err
	}
	if err := deleteSegmentsBefore(store.dir, lowest); err != nil {
		return err
	}
	store.stats.snapshots.Add(1)
	return nil
}

func encodeSnapshotEntry(current *entry) []byte {
	length := snapshotRecordFixedSize + len(current.key) + len(current.value)
	record := make([]byte, length)
	binary.LittleEndian.PutUint32(record[:snapshotCRCOffset], uint32(length-snapshotLengthSize))
	binary.LittleEndian.PutUint16(record[snapshotKeyLengthOffset:snapshotDeadlineOffset], uint16(len(current.key)))
	binary.LittleEndian.PutUint64(record[snapshotDeadlineOffset:snapshotSequenceOffset], uint64(current.deadline))
	binary.LittleEndian.PutUint64(record[snapshotSequenceOffset:snapshotKeyOffset], current.sequence)
	copy(record[snapshotKeyOffset:], current.key)
	copy(record[snapshotKeyOffset+len(current.key):], current.value)
	binary.LittleEndian.PutUint32(record[snapshotCRCOffset:snapshotKeyLengthOffset], crc32.Checksum(record[snapshotKeyLengthOffset:], crcTable))
	return record
}

func loadSnapshot(ctx context.Context, store *Store) ([]uint64, error) {
	snapshots, err := listSnapshots(store.dir)
	if err != nil || len(snapshots) == 0 {
		return nil, err
	}
	snapshot := snapshots[len(snapshots)-1]
	file, err := os.Open(snapshot.path)
	if err != nil {
		return nil, fmt.Errorf("cache: open snapshot %q: %w", snapshot.path, err)
	}
	defer file.Close()
	fail := func(offset int64, cause error) ([]uint64, error) {
		return nil, fmt.Errorf("cache: recover %q at offset %d: %w", snapshot.path, offset, cause)
	}
	info, err := file.Stat()
	if err != nil {
		return fail(0, err)
	}
	if info.Size() < snapshotFixedHeaderSize {
		return fail(0, io.ErrUnexpectedEOF)
	}
	fixed := make([]byte, snapshotFixedHeaderSize)
	if _, err := file.ReadAt(fixed, 0); err != nil {
		return fail(0, err)
	}
	if string(fixed[:4]) != string(snapshotMagic[:]) {
		return fail(0, fmt.Errorf("unknown snapshot magic %q", fixed[:4]))
	}
	version := binary.LittleEndian.Uint16(fixed[4:6])
	if version != formatVersion {
		return fail(4, fmt.Errorf("unsupported snapshot version %d", version))
	}
	if binary.LittleEndian.Uint16(fixed[6:8]) != 0 {
		return fail(6, fmt.Errorf("unsupported snapshot flags"))
	}
	shardCount := binary.LittleEndian.Uint32(fixed[8:12])
	if shardCount == 0 || shardCount&(shardCount-1) != 0 {
		return fail(8, fmt.Errorf("invalid snapshot shard count %d", shardCount))
	}
	mismatchedShards := shardCount != uint32(len(store.shards))

	headerSize := snapshotFixedHeaderSize + int64(shardCount)*8
	if headerSize > info.Size() {
		return fail(snapshotFixedHeaderSize, io.ErrUnexpectedEOF)
	}
	sequenceBytes := make([]byte, int64(shardCount)*8)
	if _, err := file.ReadAt(sequenceBytes, snapshotFixedHeaderSize); err != nil {
		return fail(snapshotFixedHeaderSize, err)
	}
	sequences := make([]uint64, shardCount)
	for index := range sequences {
		sequences[index] = binary.LittleEndian.Uint64(sequenceBytes[index*8:])
	}

	for offset := headerSize; offset < info.Size(); {
		if err := ctx.Err(); err != nil {
			return fail(offset, err)
		}
		if info.Size()-offset < snapshotLengthSize {
			return fail(offset, io.ErrUnexpectedEOF)
		}
		lengthBytes := make([]byte, snapshotLengthSize)
		if _, err := file.ReadAt(lengthBytes, offset); err != nil {
			return fail(offset, err)
		}
		length := int64(binary.LittleEndian.Uint32(lengthBytes))
		end := offset + snapshotLengthSize + length
		if length < snapshotRecordFixedSize-snapshotLengthSize || end > info.Size() {
			return fail(offset, io.ErrUnexpectedEOF)
		}
		payload := make([]byte, length)
		if _, err := file.ReadAt(payload, offset+snapshotLengthSize); err != nil {
			return fail(offset, err)
		}
		expectedCRC := binary.LittleEndian.Uint32(payload[snapshotCRCOffset-snapshotLengthSize : snapshotKeyLengthOffset-snapshotLengthSize])
		if crc32.Checksum(payload[snapshotKeyLengthOffset-snapshotLengthSize:], crcTable) != expectedCRC {
			return fail(offset, fmt.Errorf("CRC32C mismatch"))
		}
		keyLength := int(binary.LittleEndian.Uint16(payload[snapshotKeyLengthOffset-snapshotLengthSize : snapshotDeadlineOffset-snapshotLengthSize]))
		keyOffset := snapshotKeyOffset - snapshotLengthSize
		if keyLength > len(payload)-keyOffset {
			return fail(offset, fmt.Errorf("key length %d exceeds snapshot entry", keyLength))
		}
		deadline := int64(binary.LittleEndian.Uint64(payload[snapshotDeadlineOffset-snapshotLengthSize : snapshotSequenceOffset-snapshotLengthSize]))
		sequence := binary.LittleEndian.Uint64(payload[snapshotSequenceOffset-snapshotLengthSize : snapshotKeyOffset-snapshotLengthSize])
		key := string(payload[keyOffset : keyOffset+keyLength])
		snapshotShard := uint64(hashKey(key)) & uint64(shardCount-1)
		if sequence > sequences[snapshotShard] {
			return fail(offset, fmt.Errorf("entry sequence %d exceeds shard sequence %d", sequence, sequences[snapshotShard]))
		}
		store.applyRecoveredSet(key, append([]byte(nil), payload[keyOffset+keyLength:]...), deadline, sequence)
		offset = end
	}
	if mismatchedShards {
		return nil, nil
	}
	return sequences, nil
}

func listSnapshots(dir string) ([]snapshotFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("cache: read directory %q: %w", dir, err)
	}
	snapshots := make([]snapshotFile, 0, 1)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".snap") {
			continue
		}
		if len(name) != len("00000001.snap") {
			return nil, fmt.Errorf("cache: invalid snapshot name %q", filepath.Join(dir, name))
		}
		sequence, err := strconv.ParseUint(name[:8], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cache: invalid snapshot name %q", filepath.Join(dir, name))
		}
		snapshots = append(snapshots, snapshotFile{path: filepath.Join(dir, name), lowestSequence: sequence})
	}
	sort.Slice(snapshots, func(left, right int) bool {
		return snapshots[left].lowestSequence < snapshots[right].lowestSequence
	})
	return snapshots, nil
}

func removeOtherSnapshots(dir, retained string) error {
	snapshots, err := listSnapshots(dir)
	if err != nil {
		return err
	}
	for _, snapshot := range snapshots {
		if snapshot.path != retained {
			if err := os.Remove(snapshot.path); err != nil {
				return fmt.Errorf("cache: remove superseded snapshot %q: %w", snapshot.path, err)
			}
		}
	}
	return nil
}

func deleteSegmentsBefore(dir string, lowest uint64) error {
	segments, err := listSegments(dir)
	if err != nil {
		return err
	}
	for index := 0; index+1 < len(segments); index++ {
		if segments[index+1].firstSequence > lowest {
			break
		}
		if err := os.Remove(segments[index].path); err != nil {
			return fmt.Errorf("cache: remove superseded segment %q: %w", segments[index].path, err)
		}
	}
	return nil
}

func writeFull(file *os.File, value []byte) error {
	for len(value) > 0 {
		written, err := file.Write(value)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		value = value[written:]
	}
	return nil
}

func syncDirectory(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("cache: open directory %q for sync: %w", dir, err)
	}
	err = file.Sync()
	closeErr := file.Close()
	if err != nil {
		return fmt.Errorf("cache: sync directory %q: %w", dir, err)
	}
	if closeErr != nil {
		return fmt.Errorf("cache: close directory %q after sync: %w", dir, closeErr)
	}
	return nil
}

func snapshotName(lowestSequence uint64) string {
	return fmt.Sprintf("%08d.snap", lowestSequence)
}
