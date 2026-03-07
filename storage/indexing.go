package storage

import "sync"

type Entry struct {
	SegmentID int
	Offset    int64
}

type Index struct {
	data map[string]Entry
	mu   sync.RWMutex
}

func NewIndex() *Index {
	return &Index{
		data: make(map[string]Entry),
	}
}

func (i *Index) Set(key string, segID int, offset int64) {

	i.mu.Lock()
	defer i.mu.Unlock()

	i.data[key] = Entry{
		SegmentID: segID,
		Offset:    offset,
	}
}

func (i *Index) Get(key string) (Entry, bool) {

	i.mu.RLock()
	defer i.mu.RUnlock()

	e, ok := i.data[key]
	return e, ok
}

func (i *Index) Delete(key string) {
	i.mu.Lock()
	defer i.mu.Unlock()

	delete(i.data, key)
}

func (i *Index) Data() map[string]Entry {
	return i.data
}