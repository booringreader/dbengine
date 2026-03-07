package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/booringreader/kvdb/storage"
)

const OPS = 100000

func main() {

	os.Remove("data/bench.log")

	seg, err := storage.OpenSegment("data/bench.log")
	if err != nil {
		panic(err)
	}

	offsets := make([]int64, OPS)

	// populate DB
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

	start := time.Now()

	for i := 0; i < OPS; i++ {

		idx := rand.Intn(OPS)

		_, err := seg.Read(offsets[idx])
		if err != nil {
			panic(err)
		}
	}

	duration := time.Since(start)

	fmt.Println("Reads:", OPS)
	fmt.Println("Time:", duration)
	fmt.Printf("Read throughput: %.0f ops/sec\n", float64(OPS)/duration.Seconds())
}