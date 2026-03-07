package storage

import (
	"path/filepath"
	"sort"
)

func Recover(dir string, index *Index, segments *[]*Segment) error {

	files, err := filepath.Glob(dir + "/*.log")
	if err != nil {
		return err
	}

	sort.Strings(files)

	for id, file := range files {

		seg, err := OpenSegment(file)
		if err != nil {
			return err
		}

		*segments = append(*segments, seg)

		offset := int64(0)

		for {

			rec, err := seg.Read(offset)
			if err != nil {
				break
			}

			if rec.Op == Put {
				index.Set(string(rec.Key), id, offset)
			} else {
				index.Delete(string(rec.Key))
			}

			size := int64(4 + 1 + 4 + 4 + rec.KeyLen + rec.ValueLen)
			offset += size
		}
	}

	return nil
}