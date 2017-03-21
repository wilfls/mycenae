package storage

import (
	"fmt"
	"sync"

	"github.com/uol/mycenae/lib/plot"
)

const (
	bucketSize = 7200
)

// Bucket is exported to satisfy gob
type bucket struct {
	points  [bucketSize]float32
	deltas  []int
	created int64
	timeout int
	start   int64
	end     int64
	mtx     sync.RWMutex
	count   int
}

func newBucket(tc TC) *bucket {
	return &bucket{
		created: tc.Now(),
		timeout: bucketSize,
	}
}

/*
add returns
(true, 0, nil) if everthyng is fine
(false, delta, error) if delta is negative
(false, delta, error) if point is in future, it might happen if the date passed by
user is bigger than two hours (in seconds) and the bucket didn't time out.
*/
func (b *bucket) add(date int64, value float64) (int, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	delta := int(date - b.created)

	if delta < 0 {
		return delta, fmt.Errorf("point out of order can't be added to the bucket")
	}

	if delta >= bucketSize {
		return delta, fmt.Errorf("point in future can't be added to the bucket")
	}

	b.points[delta] = float32(value)
	b.count++
	b.deltas = append(b.deltas, delta)

	if date > b.end {
		b.end = date
	}

	if date < b.start || b.start == 0 {
		b.start = date
	}

	return delta, nil
}

func (b *bucket) rangePoints(start, end int64, ptsCh chan []plot.Pnt) {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	pts := make([]plot.Pnt, b.count)
	index := 0
	if b.start >= start || b.end <= end {
		for _, i := range b.deltas {
			date := b.created + int64(i)
			if date >= start && date <= end {
				pts[index] = plot.Pnt{Date: date, Value: float64(b.points[i])}
				index++
			}
		}
	}

	ptsCh <- pts[:index]
}

func (b *bucket) dumpPoints() []*plot.Pnt {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	pts := make([]*plot.Pnt, b.count)
	index := 0
	for _, i := range b.deltas {
		pts[index] = &plot.Pnt{Date: b.created + int64(i), Value: float64(b.points[i])}
		index++
	}

	return pts

}
