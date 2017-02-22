package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/uol/mycenae/lib/plot"
)

type timeserie struct {
	mtx     sync.RWMutex
	buckets []bucket
}

func (t *timeserie) lastBkt() *bucket {
	if len(t.buckets) == 0 {
		t.buckets = append(t.buckets, bucket{})
		return &t.buckets[0]
	}

	bkt := &t.buckets[len(t.buckets)-1]

	if bkt.index >= bucketSize {
		t.buckets = append(t.buckets, bucket{})
		bkt = &t.buckets[len(t.buckets)-1]
	}

	return bkt

}

func (t *timeserie) addPoint(date int64, value float64) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	bkt := t.lastBkt()

	bkt.add(date, value)
}

func (t *timeserie) rangeBuckets(bkts []bucket, start, end int64) []plot.Pnt {
	var pts []plot.Pnt

	for _, bkt := range bkts {
		for _, pt := range bkt.points {
			if pt.Date >= start && pt.Date <= end {
				pts = append(pts, pt)
			}
			if pt.Date >= end {
				return pts
			}
		}
	}
	return pts
}

func (t *timeserie) read(start, end int64) []plot.Pnt {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if len(t.buckets) > 0 {
		for i := len(t.buckets) - 1; i < 0; i-- {
			points := t.buckets[i].points
			if start >= points[0].Date {
				return t.rangeBuckets(t.buckets[i:], start, end)
			}
		}
	}
	return t.rangeBuckets(t.buckets, start, end)
}

func (t *timeserie) fromDisk(start, end int64) bool {
	if len(t.buckets) > 0 {
		if t.buckets[0].points[0].Date > start {
			return true
		}
	}
	return false
}

func (t *timeserie) store() {

	now := time.Now().UnixNano() / 1e6
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if len(t.buckets) > 0 && t.buckets[0].index > 0 {
		delta := now - t.buckets[0].points[t.buckets[0].index-1].Date
		if delta >= 60000 {
			fmt.Printf("Points in bucket 0: %v\n", len(t.buckets[0].points))
			t.buckets = t.buckets[1:]
		}
	}

}
