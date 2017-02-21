package storage

import (
	"fmt"
	"sync"

	"github.com/uol/mycenae/lib/plot"
)

type timeserie struct {
	mtx     sync.RWMutex
	buckets []bucket
}

func (t *timeserie) lastBkt() *bucket {
	if len(t.buckets) == 0 {
		t.buckets = append(t.buckets, bucket{})
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

func (t *timeserie) read(start, end int64) []plot.Pnt {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	var pts []plot.Pnt
	for _, b := range t.buckets {
		if b.index > 0 {
			if b.points[0].Date >= start && b.points[b.index-1].Date <= end {
				for i := 0; i <= b.index-1; i++ {
					fmt.Println(b.index)
					pts = append(pts, b.points[i])
				}
			}
		}
	}

	return pts

}

func (t *timeserie) fromDisk(start, end int64) bool {
	if len(t.buckets) > 0 {
		if t.buckets[0].points[0].Date > start {
			return true
		}
	}
	return false
}
