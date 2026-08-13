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

type segmentFile struct {
	path          string
	firstSequence uint64
}

func recoverLog(ctx context.Context, store *Store) (*logState, error) {
	segments, err := listSegments(store.dir)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		file, err := createSegment(store.dir, 1)
		if err != nil {
			return nil, err
		}
		return &logState{file: file, offset: segmentHeaderSize}, nil
	}

	var lastSequence uint64
	for _, segment := range segments[:len(segments)-1] {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("cache: recover %q: %w", segment.path, err)
		}
		file, _, sequence, err := recoverSegment(ctx, store, segment, false, lastSequence)
		if err != nil {
			return nil, err
		}
		lastSequence = sequence
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("cache: close recovered segment %q: %w", segment.path, err)
		}
	}

	final := segments[len(segments)-1]
	file, offset, lastSequence, err := recoverSegment(ctx, store, final, true, lastSequence)
	if err != nil {
		return nil, err
	}
	return &logState{file: file, offset: offset, seq: lastSequence}, nil
}

func listSegments(dir string) ([]segmentFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("cache: read directory %q: %w", dir, err)
	}
	segments := make([]segmentFile, 0)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".seg") {
			continue
		}
		if len(name) != len("00000001.seg") {
			return nil, fmt.Errorf("cache: invalid segment name %q", filepath.Join(dir, name))
		}
		sequence, err := strconv.ParseUint(name[:8], 10, 64)
		if err != nil || sequence == 0 {
			return nil, fmt.Errorf("cache: invalid segment name %q", filepath.Join(dir, name))
		}
		segments = append(segments, segmentFile{path: filepath.Join(dir, name), firstSequence: sequence})
	}
	sort.Slice(segments, func(left, right int) bool {
		return segments[left].firstSequence < segments[right].firstSequence
	})
	return segments, nil
}

func recoverSegment(ctx context.Context, store *Store, segment segmentFile, final bool, previousSequence uint64) (*os.File, int64, uint64, error) {
	file, err := os.OpenFile(segment.path, os.O_RDWR, 0o600)
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
	header := make([]byte, segmentHeaderSize)
	if _, err := file.ReadAt(header, 0); err != nil {
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
	for offset < info.Size() {
		if err := ctx.Err(); err != nil {
			return fail(offset, err)
		}
		if info.Size()-offset < 4 {
			if final {
				if err := file.Truncate(offset); err != nil {
					return fail(offset, err)
				}
				store.warnTornTail(segment.path, offset)
				break
			}
			return fail(offset, io.ErrUnexpectedEOF)
		}

		lengthBytes := make([]byte, 4)
		if _, err := file.ReadAt(lengthBytes, offset); err != nil {
			return fail(offset, err)
		}
		length := int64(binary.LittleEndian.Uint32(lengthBytes))
		end := offset + 4 + length
		if length < recordFixedSize-4 || end > info.Size() {
			if final {
				if err := file.Truncate(offset); err != nil {
					return fail(offset, err)
				}
				store.warnTornTail(segment.path, offset)
				break
			}
			return fail(offset, io.ErrUnexpectedEOF)
		}

		payload := make([]byte, length)
		if _, err := file.ReadAt(payload, offset+4); err != nil {
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
		recordSequence := binary.LittleEndian.Uint64(payload[recordSequenceOffset-segmentLengthSize : recordKeyOffset-segmentLengthSize])
		keyOffset := recordKeyOffset - segmentLengthSize
		if keyLength > len(payload)-keyOffset {
			return fail(offset, fmt.Errorf("key length %d exceeds record", keyLength))
		}
		if recordSequence != sequence+1 {
			return fail(offset, fmt.Errorf("sequence gap: got %d after %d", recordSequence, sequence))
		}
		if firstRecord && recordSequence != segment.firstSequence {
			return fail(offset, fmt.Errorf("segment starts at sequence %d, record is %d", segment.firstSequence, recordSequence))
		}
		key := string(payload[keyOffset : keyOffset+keyLength])
		value := payload[keyOffset+keyLength:]
		switch op {
		case opSet:
			store.applySet(key, append([]byte(nil), value...))
		case opDelete:
			if len(value) != 0 {
				return fail(offset, fmt.Errorf("delete record has a value"))
			}
			store.applyDelete(key)
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
