package storage

import (
	"fmt"
	"os"
)

func Compact(dir string, index *Index, segments *[]*Segment) error {

	newSegPath := fmt.Sprintf("%s/compact.log", dir)

	newSeg, err := OpenSegment(newSegPath)
	if err != nil {
		return err
	}

	for key, entry := range index.data {

		oldSeg := (*segments)[entry.SegmentID]

		rec, err := oldSeg.Read(entry.Offset)
		if err != nil {
			continue
		}

		offset, err := newSeg.Append(rec)
		if err != nil {
			return err
		}

		index.Set(key, 0, offset)
	}

	for _, seg := range *segments {
		os.Remove(seg.path)
	}

	*segments = []*Segment{newSeg}

	return nil
}