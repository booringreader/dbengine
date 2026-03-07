package main

import (
	"fmt"
	"os"
	"log"
	"github.com/booringreader/kvdb/storage"
)

const MaxSegmentSize = 1 << 20

type DB struct {
	dir      string
	index    *storage.Index
	segments []*storage.Segment
	active   int
}

func OpenDB(dir string) *DB {

	os.MkdirAll(dir, 0755)

	index := storage.NewIndex()
	var segments []*storage.Segment

	storage.Recover(dir, index, &segments)

	if len(segments) == 0 {
		seg, _ := storage.OpenSegment(dir + "/0.log")
		segments = append(segments, seg)
	}

	return &DB{
		dir:      dir,
		index:    index,
		segments: segments,
		active:   len(segments) - 1,
	}
}

func (db *DB) rotate() {

	id := len(db.segments)

	path := fmt.Sprintf("%s/%d.log", db.dir, id)

	seg, _ := storage.OpenSegment(path)

	db.segments = append(db.segments, seg)
	db.active = id
}

func (db *DB) Put(key string, value []byte) error {

	rec := storage.NewRecord(storage.Put, []byte(key), value)

	seg := db.segments[db.active]

	offset, err := seg.Append(rec)
	if err != nil {
		return err
	}

	db.index.Set(key, db.active, offset)

	if seg.Size() > MaxSegmentSize {
		db.rotate()
	}

	return nil
}

func (db *DB) Get(key string) ([]byte, error) {

	entry, ok := db.index.Get(key)
	if !ok {
		return nil, fmt.Errorf("not found")
	}

	seg := db.segments[entry.SegmentID]

	rec, err := seg.Read(entry.Offset)
	if err != nil {
		return nil, err
	}

	return rec.Value, nil
}

func main() {

	db := OpenDB("data")

	db.Put("name", []byte("shubham"))

	val, err := db.Get("name")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(val))
}