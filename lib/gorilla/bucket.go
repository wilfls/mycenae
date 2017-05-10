package gorilla

import (
	"github.com/pkg/errors"
	"sync"
)

const (
	bucketSize = 7200
)

// Bucket is exported to satisfy gob
type bucket struct {
	points  [bucketSize]*bucketPoint
	created int64
	timeout int64
	start   int64
	end     int64
	mtx     sync.RWMutex
	count   int
}

type bucketPoint struct {
	t int64
	v float32
}

func newBucket(tc TC) *bucket {
	return &bucket{
		created: bucketKey(tc.Now()),
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
func (b *bucket) add(date int64, value float32) (int64, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	delta := date - b.created

	if delta < 0 {
		return delta, errors.New("point out of order can't be added to the bucket")
	}

	if delta >= bucketSize {
		return delta, errors.New("point in future can't be added to the bucket")
	}

	b.points[delta] = &bucketPoint{date, value}

	b.count++

	if date > b.end {
		b.end = date
	}

	if date < b.start || b.start == 0 {
		b.start = date
	}

	return delta, nil
}

func (b *bucket) rangePoints(id int, start, end int64, queryCh chan query) {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	pts := make(Pnts, b.count)
	index := 0
	if b.start >= start || b.end <= end {
		for i := 0; i <= bucketSize-1; i++ {
			if b.points[i] != nil {
				if b.points[i].t >= start && b.points[i].t <= end {
					pts[index] = Pnt{Date: b.points[i].t, Value: b.points[i].v}
					index++
				}
			}
		}
	}

	gblog.Sugar().Infof("%v points read from bucket %v", index, id)
	queryCh <- query{
		id:  id,
		pts: pts[:index],
	}

}

func (b *bucket) dumpPoints() []*Pnt {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	pts := make([]*Pnt, b.count)
	index := 0
	for i := 0; i < bucketSize; i++ {
		if b.points[i] != nil {
			pts[index] = &Pnt{Date: b.points[i].t, Value: b.points[i].v}
			index++
		}
	}

	gblog.Sugar().Infof("%v points dumped from bucket", index)
	return pts

}
