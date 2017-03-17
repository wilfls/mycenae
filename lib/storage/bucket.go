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
type Bucket struct {
	Points    [bucketSize]float32
	Created   int64
	Timeout   int
	FirstTime int64
	LastTime  int64
	mtx       sync.RWMutex
	count     int
	deltas    []int
}

func newBucket(tc TC) *Bucket {
	return &Bucket{
		Created: tc.Now(),
		Timeout: bucketSize,
	}
}

/*
add returns
(true, 0, nil) if everthyng is fine
(false, delta, error) if delta is negative
(false, delta, error) if point is in future, it might happen if the date passed by
user is bigger than two hours (in seconds) and the bucket didn't time out.
*/
func (b *Bucket) add(date int64, value float64) (int, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	delta := int(date - b.Created)

	if delta < 0 {
		return delta, fmt.Errorf("point out of order can't be added to the bucket")
	}

	if delta >= bucketSize {
		return delta, fmt.Errorf("point in future can't be added to the bucket")
	}

	b.Points[delta] = float32(value)
	b.count++
	b.deltas = append(b.deltas, delta)

	if date > b.LastTime {
		b.LastTime = date
	}

	if date < b.FirstTime || b.FirstTime == 0 {
		b.FirstTime = date
	}

	return delta, nil
}

func (b *Bucket) rangePoints(start, end int64, ptsCh chan []plot.Pnt) {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	pts := make([]plot.Pnt, b.count)
	index := 0
	if b.FirstTime >= start || b.LastTime <= end {
		for _, i := range b.deltas {
			date := b.Created + int64(i)
			if date >= start && date <= end {
				pts[index] = plot.Pnt{Date: date, Value: float64(b.Points[i])}
				index++
			}
		}
	}
	//fmt.Println("bucket", index)
	ptsCh <- pts[:index]
}

func (b *Bucket) dumpPoints() []*plot.Pnt {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	pts := make([]*plot.Pnt, b.count)
	index := 0
	for _, i := range b.deltas {
		pts[index] = &plot.Pnt{Date: b.Created + int64(i), Value: float64(b.Points[i])}
		index++
	}

	return pts

}
