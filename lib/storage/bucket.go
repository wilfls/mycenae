package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/uol/mycenae/lib/plot"
)

const (
	bucketSize = 128
)

// Bucket is exported to satisfy gob
type Bucket struct {
	Index     int
	Points    [bucketSize]plot.Pnt
	Created   int64
	Timeout   time.Duration
	FirstTime int64
	LastTime  int64
	mtx       sync.RWMutex
}

func newBucket(timeout time.Duration) *Bucket {

	return &Bucket{
		Created: time.Now().Unix(),
		Timeout: timeout,
	}
}

func (b *Bucket) refresh(timeout time.Duration) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.Created = time.Now().Unix()
	b.Timeout = timeout
	b.Index = 0
	b.FirstTime = 0
	b.LastTime = 0
}

func (b *Bucket) add(date int64, value float64) (bool, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	// there isn't free slot at this bucket
	if b.Index >= bucketSize {
		return false, nil
	}

	if b.Index == 0 {

		b.Points[0] = plot.Pnt{Date: date, Value: value}
		b.FirstTime = date
		b.LastTime = date
		b.Index++
		//fmt.Println("wrote", date, value)
		return true, nil
	}

	// bucket must not have points with more than
	// a Timeout range
	delta := date - b.FirstTime
	//fmt.Println(delta, int64(b.Timeout))
	if delta >= int64(b.Timeout) {
		return false, nil
	}

	// date is older than the first time in bucket
	// so it's out of order, it must go to cassandra
	if delta < 0 {
		return false, fmt.Errorf("point out of order can't be appended in bucket")
	}

	if date < b.FirstTime {
		b.FirstTime = date
	}

	if date > b.LastTime {
		b.LastTime = date
	}

	b.Points[b.Index] = plot.Pnt{Date: date, Value: value}
	b.Index++

	//fmt.Println("wrote", date, value)
	return true, nil
}

func (b *Bucket) rangePoints(start, end int64, ptsCh chan []plot.Pnt) {
	pts := make([]plot.Pnt, len(b.Points))
	index := 0
	if b.FirstTime >= start || b.LastTime <= end {
		for _, pt := range b.Points {
			if pt.Date >= start && pt.Date <= end {
				pts[index] = pt
				index++
			}
		}
	}

	ptsCh <- pts[:index]

}
