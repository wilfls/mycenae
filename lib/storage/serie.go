package storage

import (
	"bytes"
	"encoding/gob"
	"sync"
	"time"

	"github.com/uol/mycenae/lib/plot"
)

type serie struct {
	mtx     sync.RWMutex
	buckets []*Bucket
}

func (t *serie) lastBkt() *Bucket {
	if len(t.buckets) == 0 {
		t.addBkt()
		return t.buckets[0]
	}

	return t.buckets[len(t.buckets)-1]
}

func (t *serie) addBkt() {
	t.buckets = append(t.buckets, newBucket(time.Hour*2))
}

func (t *serie) addPoint(p persistence, ksid, tsid string, date int64, value float64) (bool, error) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	// it's only false  if we need a new bucket
	bkt := t.lastBkt()
	ok, err := bkt.add(date, value)
	if err != nil {
		return false, err
	}
	if !ok {

		go t.store(p, ksid, tsid, bkt)

		t.addBkt()
		return t.lastBkt().add(date, value)
	}

	return true, nil

}

func (t *serie) rangeBuckets(bkts []*Bucket, start, end int64) []plot.Pnt {
	var pts []plot.Pnt

	for _, bkt := range bkts {
		for _, pt := range bkt.Points {
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

func (t *serie) read(start, end int64) []plot.Pnt {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	n := len(t.buckets)

	if n > 0 {
		for i := n - 1; i < 0; i-- {
			points := t.buckets[i].Points
			if start >= points[0].Date {
				return t.rangeBuckets(t.buckets[i:], start, end)
			}
		}
	}
	return t.rangeBuckets(t.buckets, start, end)
}

func (t *serie) fromDisk(start, end int64) bool {
	if len(t.buckets) > 0 {
		if t.buckets[0].Points[0].Date > start {
			return true
		}
	}
	return false
}

func (t *serie) store(p persistence, ksid, tsid string, bkt *Bucket) {

	// compress and store it in cassandra
	t.mtx.RLock()
	timestamp := bkt.Points[0].Date
	pts := bkt.Points[:bkt.Index-1]
	t.mtx.RUnlock()

	buff := new(bytes.Buffer)
	encoder := gob.NewEncoder(buff)

	err := encoder.Encode(pts)
	if err != nil {
		// log ERR
		return
	}

	p.InsertBucket(ksid, tsid, timestamp, buff.Bytes())

	if len(t.buckets) > 12 {
		/*
			delta := now - t.buckets[0].Points[t.buckets[0].Index-1].Date
			if delta >= 60000 {
				fmt.Printf("Points in bucket 0: %v\n", len(t.buckets[0].Points))
				t.buckets = t.buckets[1:]
			}
		*/
	}

}
