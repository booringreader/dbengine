package main

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/booringreader/kvdb/storage"
)

const (
	OPS        = 100000
	GOROUTINES = 8
)

func worker(seg *storage.Segment, counter *int64, wg *sync.WaitGroup) {
	defer wg.Done()

	for {

		i := atomic.AddInt64(counter, 1)

		if i > OPS {
			return
		}

		key := []byte(fmt.Sprintf("key_%d", i))
		val := []byte("hello_world")

		rec := storage.NewRecord(storage.Put, key, val)

		_, err := seg.Append(rec)
		if err != nil {
			panic(err)
		}
	}
}

func main() {

	os.Remove("data/bench.log")

	seg, err := storage.OpenSegment("data/bench.log")
	if err != nil {
		panic(err)
	}

	var counter int64
	var wg sync.WaitGroup

	start := time.Now()

	for i := 0; i < GOROUTINES; i++ {
		wg.Add(1)
		go worker(seg, &counter, &wg)
	}

	wg.Wait()

	duration := time.Since(start)

	fmt.Println("Concurrent Write Benchmark")
	fmt.Println("Operations:", OPS)
	fmt.Println("Goroutines:", GOROUTINES)
	fmt.Println("Time:", duration)
	fmt.Printf("Throughput: %.0f ops/sec\n", float64(OPS)/duration.Seconds())
}