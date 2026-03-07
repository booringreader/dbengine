package storage

import (
	"os"
	"sync"
)

type Segment struct {
	file *os.File
	size int64
	path string
	mu   sync.Mutex
}

func OpenSegment(path string) (*Segment, error) {

	f, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)

	if err != nil {
		return nil, err
	}

	info, _ := f.Stat()

	return &Segment{
		file: f,
		size: info.Size(),
		path: path,
	}, nil
}

func (s *Segment) Append(rec *Record) (int64, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := rec.Encode()
	if err != nil {
		return 0, err
	}

	offset := s.size

	n, err := s.file.Write(data)
	if err != nil {
		return 0, err
	}

	s.size += int64(n)

	return offset, nil
}

func (s *Segment) Read(offset int64) (*Record, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	return DecodeRecord(s.file)
}

func (s *Segment) Size() int64 {
	return s.size
}