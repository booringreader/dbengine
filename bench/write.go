package main

import (
	"fmt"
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

	start := time.Now()

	for i := 0; i < OPS; i++ {

		key := []byte(fmt.Sprintf("key_%d", i))
		val := []byte("hello_world")

		rec := storage.NewRecord(storage.Put, key, val)

		_, err := seg.Append(rec)
		if err != nil {
			panic(err)
		}
	}

	duration := time.Since(start)

	fmt.Println("Operations:", OPS)
	fmt.Println("Total time:", duration)
	fmt.Printf("Throughput: %.0f ops/sec\n", float64(OPS)/duration.Seconds())
}