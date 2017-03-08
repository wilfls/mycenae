package storage

import (
	"fmt"

	"github.com/uol/mycenae/lib/plot"
)

const (
	bucketSize = 128
	hour       = 3600000 * 2
	//hour = 7200000
)

type Bucket struct {
	Index  int
	Points [bucketSize]plot.Pnt
}

func (b *Bucket) add(date int64, value float64) (bool, error) {
	// there isn't free slot at this bucket
	if b.Index >= bucketSize {
		return false, nil
	}

	if b.Index == 0 {
		b.Points[0] = plot.Pnt{Date: date, Value: value}
		b.Index++
		return true, nil
	}

	// bucket must not have points with more than
	// a hour range
	delta := date - b.Points[0].Date
	if delta >= hour {
		return false, nil
	}

	// date is older than the first time in bucket
	// so it's out of order, it must go to cassandra
	if delta < 0 {
		return false, fmt.Errorf("point out of order can't be appended in bucket")
	}

	b.Points[b.Index] = plot.Pnt{Date: date, Value: value}
	b.Index++

	return true, nil
}
