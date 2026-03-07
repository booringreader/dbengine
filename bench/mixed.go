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

	offsets := make([]int64, 0, OPS)

	start := time.Now()

	for i := 0; i < OPS; i++ {

		if rand.Float64() < 0.7 && len(offsets) > 0 {

			idx := rand.Intn(len(offsets))

			_, err := seg.Read(offsets[idx])
			if err != nil {
				panic(err)
			}

		} else {

			key := []byte(fmt.Sprintf("key_%d", i))
			val := []byte("hello_world")

			rec := storage.NewRecord(storage.Put, key, val)

			offset, err := seg.Append(rec)
			if err != nil {
				panic(err)
			}

			offsets = append(offsets, offset)
		}
	}

	duration := time.Since(start)

	fmt.Println("Mixed operations:", OPS)
	fmt.Println("Time:", duration)
	fmt.Printf("Throughput: %.0f ops/sec\n", float64(OPS)/duration.Seconds())
}