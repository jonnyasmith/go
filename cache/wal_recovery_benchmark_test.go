package cache

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

var benchmarkEncodedRecords []byte

func BenchmarkWALRecordBatchAllocations(b *testing.B) {
	requests := makeBenchmarkRequests(64)
	totalBytes := 0
	for _, request := range requests {
		totalBytes += recordSizeFor(request)
	}
	for _, benchmark := range []struct {
		name   string
		encode func([]*writeRequest, int) []byte
	}{
		{name: "PerRecordAllocationBaseline", encode: encodeRecordsSeparately},
		{name: "CallerOwnedBatchBuffer", encode: encodeRecordsIntoBatch},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			allocations := testing.AllocsPerRun(100, func() {
				benchmarkEncodedRecords = benchmark.encode(requests, totalBytes)
			})
			b.ReportAllocs()
			b.ReportMetric(allocations/float64(len(requests)), "allocs/record")
			b.ReportMetric(float64(len(requests)), "records/op")
			b.SetBytes(int64(totalBytes))
			b.ResetTimer()
			for range b.N {
				benchmarkEncodedRecords = benchmark.encode(requests, totalBytes)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(requests)), "ns/record")
		})
	}
}

func BenchmarkRecoveryLargeLog(b *testing.B) {
	const recordCount = 10_000
	dir := b.TempDir()
	path := filepath.Join(dir, segmentName(1))
	segment := make([]byte, segmentHeaderSize)
	copy(segment[:4], segmentMagic[:])
	binary.LittleEndian.PutUint16(segment[4:6], formatVersion)
	for sequence := 1; sequence <= recordCount; sequence++ {
		request := &writeRequest{
			kind:  requestSet,
			key:   fmt.Sprintf("key-%08d", sequence),
			value: []byte("representative-recovery-value"),
		}
		segment = appendRecord(segment, uint64(sequence), request)
	}
	if err := os.WriteFile(path, segment, 0o600); err != nil {
		b.Fatalf("write benchmark segment: %v", err)
	}

	b.ReportAllocs()
	b.ReportMetric(recordCount, "records/op")
	b.SetBytes(int64(len(segment)))
	b.ResetTimer()
	for range b.N {
		store := newStoreState(dir, nil, 16, 1<<62, nil)
		file, _, sequence, err := recoverSegment(context.Background(), store, segmentFile{path: path, firstSequence: 1}, false, 0, snapshotState{}, maxSequence)
		if err != nil {
			b.Fatalf("recover: %v", err)
		}
		if sequence != recordCount {
			b.Fatalf("recovered sequence = %d; want %d", sequence, recordCount)
		}
		if err := file.Close(); err != nil {
			b.Fatalf("close segment: %v", err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*recordCount), "ns/record")
}

func makeBenchmarkRequests(count int) []*writeRequest {
	requests := make([]*writeRequest, count)
	for index := range requests {
		requests[index] = &writeRequest{
			kind:  requestSet,
			key:   fmt.Sprintf("key-%04d", index),
			value: make([]byte, 16+(index%8)*31),
		}
	}
	return requests
}

func encodeRecordsSeparately(requests []*writeRequest, totalBytes int) []byte {
	payload := make([]byte, 0, totalBytes)
	for sequence, request := range requests {
		payload = append(payload, encodeRecord(uint64(sequence+1), request)...)
	}
	return payload
}

func encodeRecordsIntoBatch(requests []*writeRequest, totalBytes int) []byte {
	payload := make([]byte, 0, totalBytes)
	for sequence, request := range requests {
		payload = appendRecord(payload, uint64(sequence+1), request)
	}
	return payload
}
