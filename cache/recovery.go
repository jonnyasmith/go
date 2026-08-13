package cache

import (
	"bufio"
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

type segmentFile struct {
	path          string
	firstSequence uint64
}

type snapshotState struct {
	loaded          bool
	replaySequences []uint64
	lowest          uint64
	highest         uint64
}

func newSnapshotState(sequences []uint64, perShardReplay bool) snapshotState {
	state := snapshotState{
		loaded:  true,
		lowest:  sequences[0],
		highest: sequences[0],
	}
	for _, sequence := range sequences[1:] {
		if sequence < state.lowest {
			state.lowest = sequence
		}
		if sequence > state.highest {
			state.highest = sequence
		}
	}
	if perShardReplay {
		state.replaySequences = sequences
	}
	return state
}

func recoverLog(ctx context.Context, store *Store) (*logState, error) {
	snapshot, err := loadSnapshot(ctx, store)
	if err != nil {
		return nil, err
	}
	segments, err := listSegments(store.dir)
	if err != nil {
		return nil, err
	}

	if len(segments) == 0 {
		nextSequence := uint64(1)
		if snapshot.loaded {
			if snapshot.lowest != snapshot.highest {
				return nil, fmt.Errorf("cache: sequence gap after snapshot sequence %d: no retained WAL covers through sequence %d", snapshot.lowest, snapshot.highest)
			}
			if snapshot.highest == maxSequence {
				return nil, fmt.Errorf("cache: sequence space exhausted at %d", snapshot.highest)
			}
			nextSequence = snapshot.highest + 1
		}
		file, err := store.createSegment(nextSequence)
		if err != nil {
			return nil, err
		}
		store.logSequence.Store(nextSequence - 1)
		return &logState{file: file, offset: segmentHeaderSize, seq: nextSequence - 1}, nil
	}

	start := 0
	previousSequence := uint64(0)
	if snapshot.loaded {
		firstRequired := snapshot.lowest
		if firstRequired != maxSequence {
			firstRequired++
		}
		if segments[0].firstSequence > firstRequired {
			return nil, fmt.Errorf("cache: recover %q at offset %d: sequence gap after snapshot sequence %d", segments[0].path, segmentHeaderSize, snapshot.lowest)
		}
		for index, segment := range segments {
			if segment.firstSequence <= firstRequired {
				start = index
			}
		}
		previousSequence = segments[start].firstSequence - 1
	} else if segments[0].firstSequence > 1 {
		return nil, fmt.Errorf("cache: recover %q at offset %d: sequence gap: oldest retained sequence is %d, want 1", segments[0].path, segmentHeaderSize, segments[0].firstSequence)
	}

	var finalFile *os.File
	var finalOffset int64
	lastSequence := previousSequence
	for index := start; index < len(segments); index++ {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("cache: recover %q: %w", segments[index].path, err)
		}
		final := index == len(segments)-1
		file, offset, sequence, err := recoverSegment(ctx, store, segments[index], final, lastSequence, snapshot, maxSequence)
		if err != nil {
			return nil, err
		}
		lastSequence = sequence
		if final {
			finalFile = file
			finalOffset = offset
		} else if err := file.Close(); err != nil {
			return nil, fmt.Errorf("cache: close recovered segment %q: %w", segments[index].path, err)
		}
	}
	if snapshot.loaded && lastSequence < snapshot.highest {
		_ = finalFile.Close()
		return nil, fmt.Errorf("cache: sequence gap after retained WAL sequence %d: snapshot requires history through sequence %d", lastSequence, snapshot.highest)
	}
	store.logSequence.Store(lastSequence)
	return &logState{file: finalFile, offset: finalOffset, seq: lastSequence}, nil
}

func recoverDurableImage(ctx context.Context, store *Store, through uint64) error {
	snapshot, err := loadSnapshot(ctx, store)
	if err != nil {
		return err
	}
	if snapshot.loaded && snapshot.highest > through {
		return fmt.Errorf("cache: snapshot sequence %d is newer than compaction boundary %d", snapshot.highest, through)
	}
	segments, err := listSegments(store.dir)
	if err != nil {
		return err
	}

	start := 0
	previousSequence := uint64(0)
	if snapshot.loaded {
		firstRequired := snapshot.lowest
		if firstRequired != maxSequence {
			firstRequired++
		}
		if snapshot.highest == through && (len(segments) == 0 || segments[0].firstSequence > through) {
			store.logSequence.Store(through)
			return nil
		}
		if len(segments) == 0 || segments[0].firstSequence > firstRequired {
			return fmt.Errorf("cache: sequence gap after snapshot sequence %d while compacting through %d", snapshot.lowest, through)
		}
		for index, segment := range segments {
			if segment.firstSequence <= firstRequired {
				start = index
			}
		}
		previousSequence = segments[start].firstSequence - 1
	} else {
		if len(segments) == 0 || segments[0].firstSequence > 1 {
			return fmt.Errorf("cache: sequence gap while compacting through %d", through)
		}
	}

	lastSequence := previousSequence
	for index := start; index < len(segments) && segments[index].firstSequence <= through; index++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("cache: compact %q: %w", segments[index].path, err)
		}
		file, _, sequence, replayErr := recoverSegment(ctx, store, segments[index], false, lastSequence, snapshot, through)
		if replayErr != nil {
			return replayErr
		}
		lastSequence = sequence
		if closeErr := file.Close(); closeErr != nil {
			return fmt.Errorf("cache: close compacted segment %q: %w", segments[index].path, closeErr)
		}
	}
	if lastSequence != through {
		return fmt.Errorf("cache: sequence gap while compacting: reached %d, want %d", lastSequence, through)
	}
	store.logSequence.Store(through)
	return nil
}

func listSegments(dir string) ([]segmentFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("cache: read directory %q: %w", dir, err)
	}
	segments := make([]segmentFile, 0)
	seen := make(map[uint64]string)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".seg") {
			continue
		}
		sequence, err := parseSegmentFilename(dir, name)
		if err != nil {
			return nil, err
		}
		if previous, exists := seen[sequence]; exists {
			return nil, fmt.Errorf("cache: duplicate segment sequence %d in %q and %q", sequence, previous, filepath.Join(dir, name))
		}
		path := filepath.Join(dir, name)
		seen[sequence] = path
		segments = append(segments, segmentFile{path: path, firstSequence: sequence})
	}
	sort.Slice(segments, func(left, right int) bool {
		return segments[left].firstSequence < segments[right].firstSequence
	})
	return segments, nil
}

func parseSegmentFilename(dir, name string) (uint64, error) {
	return parseSequenceFilename(dir, name, ".seg", "segment", false)
}

func parseSnapshotFilename(dir, name string) (uint64, error) {
	return parseSequenceFilename(dir, name, ".snap", "snapshot", true)
}

func parseSequenceFilename(dir, name, suffix, kind string, allowZero bool) (uint64, error) {
	stem := strings.TrimSuffix(name, suffix)
	if len(stem) != 8 && len(stem) != 20 {
		return 0, fmt.Errorf("cache: invalid %s name %q", kind, filepath.Join(dir, name))
	}
	sequence, err := strconv.ParseUint(stem, 10, 64)
	if err != nil || (!allowZero && sequence == 0) {
		return 0, fmt.Errorf("cache: invalid %s name %q", kind, filepath.Join(dir, name))
	}
	return sequence, nil
}

const recoveryReadBufferSize = 64 << 10

func recoverSegment(ctx context.Context, store *Store, segment segmentFile, final bool, previousSequence uint64, snapshot snapshotState, through uint64) (*os.File, int64, uint64, error) {
	flags := os.O_RDONLY
	if final {
		flags = os.O_RDWR
	}
	file, err := os.OpenFile(segment.path, flags, 0o600)
	if err != nil {
		return nil, 0, previousSequence, fmt.Errorf("cache: open segment %q: %w", segment.path, err)
	}
	fail := func(offset int64, cause error) (*os.File, int64, uint64, error) {
		_ = file.Close()
		return nil, 0, previousSequence, fmt.Errorf("cache: recover %q at offset %d: %w", segment.path, offset, cause)
	}

	info, err := file.Stat()
	if err != nil {
		return fail(0, err)
	}
	if info.Size() < segmentHeaderSize {
		return fail(0, io.ErrUnexpectedEOF)
	}
	reader := bufio.NewReaderSize(file, recoveryReadBufferSize)
	var header [segmentHeaderSize]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return fail(0, err)
	}
	if string(header[:4]) != string(segmentMagic[:]) {
		return fail(0, fmt.Errorf("unknown segment magic %q", header[:4]))
	}
	version := binary.LittleEndian.Uint16(header[4:6])
	if version != formatVersion {
		return fail(4, fmt.Errorf("unsupported segment version %d", version))
	}
	if binary.LittleEndian.Uint16(header[6:8]) != 0 {
		return fail(6, fmt.Errorf("unsupported segment flags"))
	}

	offset := segmentHeaderSize
	sequence := previousSequence
	firstRecord := true
	var lengthBytes [segmentLengthSize]byte
	var payload []byte
	for offset < info.Size() {
		if err := ctx.Err(); err != nil {
			return fail(offset, err)
		}
		if info.Size()-offset < segmentLengthSize {
			if final {
				if err := file.Truncate(offset); err != nil {
					return fail(offset, err)
				}
				store.warnTornTail(segment.path, offset)
				break
			}
			return fail(offset, io.ErrUnexpectedEOF)
		}

		if _, err := io.ReadFull(reader, lengthBytes[:]); err != nil {
			return fail(offset, err)
		}
		length := int64(binary.LittleEndian.Uint32(lengthBytes[:]))
		end := offset + segmentLengthSize + length
		if length < recordFixedSize-segmentLengthSize || end > info.Size() {
			if final {
				if err := file.Truncate(offset); err != nil {
					return fail(offset, err)
				}
				store.warnTornTail(segment.path, offset)
				break
			}
			return fail(offset, io.ErrUnexpectedEOF)
		}
		if length > int64(^uint(0)>>1) {
			return fail(offset, fmt.Errorf("record length %d exceeds platform limit", length))
		}

		payloadLength := int(length)
		if cap(payload) < payloadLength {
			payload = make([]byte, payloadLength)
		} else {
			payload = payload[:payloadLength]
		}
		if _, err := io.ReadFull(reader, payload); err != nil {
			return fail(offset, err)
		}
		expectedCRC := binary.LittleEndian.Uint32(payload[recordCRCOffset-segmentLengthSize : recordOpOffset-segmentLengthSize])
		if crc32.Checksum(payload[recordOpOffset-segmentLengthSize:], crcTable) != expectedCRC {
			if final && end == info.Size() {
				if err := file.Truncate(offset); err != nil {
					return fail(offset, err)
				}
				store.warnTornTail(segment.path, offset)
				break
			}
			return fail(offset, fmt.Errorf("CRC32C mismatch"))
		}

		op := payload[recordOpOffset-segmentLengthSize]
		keyLength := int(binary.LittleEndian.Uint16(payload[recordKeyLengthOffset-segmentLengthSize : recordDeadlineOffset-segmentLengthSize]))
		deadline := int64(binary.LittleEndian.Uint64(payload[recordDeadlineOffset-segmentLengthSize : recordSequenceOffset-segmentLengthSize]))
		recordSequence := binary.LittleEndian.Uint64(payload[recordSequenceOffset-segmentLengthSize : recordKeyOffset-segmentLengthSize])
		if recordSequence > through {
			break
		}
		keyOffset := recordKeyOffset - segmentLengthSize
		if keyLength > len(payload)-keyOffset {
			return fail(offset, fmt.Errorf("key length %d exceeds record", keyLength))
		}
		if sequence == maxSequence || recordSequence != sequence+1 {
			return fail(offset, fmt.Errorf("sequence gap: got %d after %d", recordSequence, sequence))
		}
		if firstRecord && recordSequence != segment.firstSequence {
			return fail(offset, fmt.Errorf("segment starts at sequence %d, record is %d", segment.firstSequence, recordSequence))
		}
		key := string(payload[keyOffset : keyOffset+keyLength])
		value := payload[keyOffset+keyLength:]
		shardIndex := hashKey(key) & store.shardMask
		apply := store.recoveryShard == nil || int(shardIndex) == *store.recoveryShard
		if snapshot.loaded {
			if snapshot.replaySequences != nil {
				apply = apply && recordSequence > snapshot.replaySequences[shardIndex]
			} else {
				apply = apply && recordSequence > snapshot.lowest
			}
		}
		switch op {
		case opSet:
			if apply {
				store.applyRecoveredSet(key, append([]byte(nil), value...), deadline, recordSequence)
			}
		case opDelete:
			if deadline != 0 {
				return fail(offset, fmt.Errorf("delete record has a deadline"))
			}
			if len(value) != 0 {
				return fail(offset, fmt.Errorf("delete record has a value"))
			}
			if apply {
				store.applyDelete(key, recordSequence)
			}
		default:
			return fail(offset, fmt.Errorf("unknown operation %d", op))
		}
		sequence = recordSequence
		firstRecord = false
		offset = end
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return fail(offset, err)
	}
	return file, offset, sequence, nil
}
