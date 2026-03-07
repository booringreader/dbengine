package main

import (
	"fmt"
	"os"
	"time"

	"github.com/booringreader/kvdb/storage"
)

const OPS = 100000

func writeBench(seg *storage.Segment) []int64 {

	offsets := make([]int64, OPS)

	start := time.Now()

	for i := 0; i < OPS; i++ {

		key := []byte(fmt.Sprintf("key_%d", i))
		val := []byte("hello_world")

		rec := storage.NewRecord(storage.Put, key, val)

		offset, err := seg.Append(rec)
		if err != nil {
			panic(err)
		}

		offsets[i] = offset
	}

	duration := time.Since(start)

	fmt.Println("WRITE")
	fmt.Println("Time:", duration)
	fmt.Printf("Throughput: %.0f ops/sec\n\n", float64(OPS)/duration.Seconds())

	return offsets
}

func readBench(seg *storage.Segment, offsets []int64) {

	start := time.Now()

	for i := 0; i < OPS; i++ {

		_, err := seg.Read(offsets[i])
		if err != nil {
			panic(err)
		}
	}

	duration := time.Since(start)

	fmt.Println("READ")
	fmt.Println("Time:", duration)
	fmt.Printf("Throughput: %.0f ops/sec\n\n", float64(OPS)/duration.Seconds())
}

func main() {

	os.Remove("data/bench.log")

	seg, err := storage.OpenSegment("data/bench.log")
	if err != nil {
		panic(err)
	}

	offsets := writeBench(seg)

	readBench(seg, offsets)
}