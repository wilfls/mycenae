package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/uol/mycenae/lib/plot"
)

const (
	bucketSize = 7200
)

// Bucket is exported to satisfy gob
type Bucket struct {
	Points    [bucketSize + 1]*float64
	Created   int64
	Timeout   int64
	FirstTime int64
	LastTime  int64
	mtx       sync.RWMutex
	count     int
}

func newBucket() *Bucket {
	//fmt.Println("New bucket")
	return &Bucket{
		Created: time.Now().Unix(),
		Timeout: bucketSize,
	}
}

/*
add returns
(true, 0, nil) if everthyng is fine
(false, 0, nil) if bucket timed out, usually two hours after it has been created
(false, delta, error) if delta is negative
(false, delta, error) if point is in future, it might happen if the date passed by
user is bigger than two hours (in seconds) but the bucket hasn't timed out.
*/
func (b *Bucket) add(date int64, value float64) (bool, int64, error) {

	b.mtx.Lock()
	defer b.mtx.Unlock()

	timeout := time.Now().Unix() - b.Created
	if timeout >= b.Timeout {
		return false, 0, nil
	}

	//h, _, _ := time.Unix(date, 0).Clock()

	delta := date - b.Created

	if delta < 0 {
		return false, delta, fmt.Errorf("point out of order can't be added to the bucket")
	}

	if delta > bucketSize {
		return false, delta, fmt.Errorf("point in future can't be added to the bucket")
	}

	b.Points[delta] = &value
	b.count++

	if date > b.LastTime {
		b.LastTime = date
	}

	if date < b.FirstTime && b.FirstTime == 0 {
		b.FirstTime = date
	}

	return true, 0, nil
}

func (b *Bucket) rangePoints(start, end int64, ptsCh chan []plot.Pnt) {
	b.mtx.RLock()
	defer b.mtx.RUnlock()
	pts := make([]plot.Pnt, b.count)
	index := 0
	if b.FirstTime >= start || b.LastTime <= end {
		for i := 0; i < bucketSize; i++ {
			if b.Points[i] != nil {
				date := b.Created + int64(i)
				if date >= start && date <= end {
					pts[index] = plot.Pnt{Date: date, Value: *b.Points[i]}
					index++
				}
			}
		}
	}

	ptsCh <- pts[:index]

}

func (b *Bucket) dumpPoints() []*plot.Pnt {

	pts := make([]*plot.Pnt, b.count)
	index := 0
	for i := 0; i < bucketSize; i++ {
		if b.Points[i] != nil {
			pts[index] = &plot.Pnt{Date: b.Created + int64(i), Value: *b.Points[i]}
			index++
		}
	}

	return pts

}
