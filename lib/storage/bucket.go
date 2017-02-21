package storage

import "github.com/uol/mycenae/lib/plot"

type bucket struct {
	index  int
	points [bucketSize]plot.Pnt
}

func (b *bucket) add(date int64, value float64) {
	b.points[b.index] = plot.Pnt{Date: date, Value: value}
	b.index++
}
