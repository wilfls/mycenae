package storage

import (
	"fmt"

	"github.com/uol/mycenae/lib/plot"
)

const (
	bucketSize = 64
)

type bucket struct {
	index  int
	points [bucketSize]plot.Pnt
}

func (b *bucket) add(date int64, value float64) bool {

	fmt.Println(b.index)

	// there isn't free slot at this bucket
	if b.index >= bucketSize {
		return false
	}

	if b.index == 0 {
		b.points[0] = plot.Pnt{Date: date, Value: value}
		b.index++
		return true
	}

	// bucket must not have points with more than
	// a hour range
	delta := date - b.points[0].Date
	if delta > 36000000 {
		return false

	}

	b.points[b.index] = plot.Pnt{Date: date, Value: value}
	b.index++

	return true
}
