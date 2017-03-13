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
	ksid    string
	tsid    string
	buckets []*Bucket
	index   int
	timeout time.Duration
}

func newSerie(ksid, tsid string, buckets int) *serie {

	s := &serie{
		ksid:    ksid,
		tsid:    tsid,
		timeout: 7200,
	}

	for i := 0; i < buckets; i++ {
		s.buckets = append(s.buckets, newBucket(s.timeout))
		s.buckets[i].refresh(s.timeout)
	}

	return s
}

func (t *serie) lastBkt() *Bucket {
	return t.buckets[t.index]
}

func (t *serie) addBkt() {
	t.buckets = append(t.buckets, newBucket(t.timeout))
}

func (t *serie) nextBkt() *Bucket {
	if t.index >= len(t.buckets)-1 {
		t.index = 0
	} else {
		t.index++
	}

	t.buckets[t.index].refresh(t.timeout)
	return t.buckets[t.index]
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

		return t.nextBkt().add(date, value)
	}

	return true, nil

}

func (t *serie) read(start, end int64) []plot.Pnt {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	ptsCh := make(chan []plot.Pnt)
	defer close(ptsCh)

	for _, bkt := range t.buckets {
		go bkt.rangePoints(start, end, ptsCh)
	}

	var pts []plot.Pnt
	for i := 0; i <= len(t.buckets)-1; i++ {
		p := <-ptsCh
		//fmt.Println("read", p)
		if len(p) > 0 {
			pts = append(pts, p...)
		}

	}

	return pts

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

	if p.cassandra == nil {
		return
	}

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
