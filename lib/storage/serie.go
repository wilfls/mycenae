package storage

import (
	"fmt"
	"sync"
	"time"

	tsz "github.com/uol/go-tsz"
	"github.com/uol/mycenae/lib/plot"
)

type serie struct {
	mtx     sync.RWMutex
	ksid    string
	tsid    string
	bucket  *Bucket
	blocks  [12]block
	index   int
	timeout time.Duration
}

func newSerie(ksid, tsid string, buckets int) *serie {

	s := &serie{
		ksid:    ksid,
		tsid:    tsid,
		timeout: 7200,
		blocks:  [12]block{},
		bucket:  newBucket(),
	}

	return s
}

func (t *serie) addPoint(p persistence, ksid, tsid string, date int64, value float64) (bool, error) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	ok, delta, err := t.bucket.add(date, value)
	if err != nil {
		fmt.Println(delta, err)
		// Point must go to cassandra
		return false, err
	}
	if !ok {
		fmt.Println("Storing...")
		go t.store(p, ksid, tsid, t.bucket)
		t.bucket = newBucket()
		ok, _, err := t.bucket.add(date, value)
		return ok, err
	}

	return true, nil

}

func (t *serie) read(start, end int64) []plot.Pnt {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	now := time.Now().Unix()
	dStart := now - start
	dEnd := now - end
	pts := []plot.Pnt{}

	if dStart >= 86400 && dEnd >= 86400 {
		// read only from cassandra
		return pts
	}

	ptsCh := make(chan []plot.Pnt)
	defer close(ptsCh)

	blkCount := len(t.blocks)
	ranges := 0

	if dStart >= 7200 {
		for i := 0; i < blkCount; i++ {
			fmt.Println("Reading from blocks")
			go t.blocks[i].rangePoints(start, end, ptsCh)
			ranges++
		}
	}

	if dEnd <= 7200 {
		fmt.Println("Reading from bucket", dStart, dEnd)
		go t.bucket.rangePoints(start, end, ptsCh)
		ranges++
	}
	for i := 0; i < ranges; i++ {
		p := <-ptsCh
		if len(p) > 0 {
			pts = append(pts, p...)
		}
	}

	return pts
}

/*
func (t *serie) fromDisk(start, end int64) bool {
	if len(t.buckets) > 0 {
		if t.buckets[0].Points[0].Date > start {
			return true
		}
	}
	return false
}
*/

func (t *serie) store(p persistence, ksid, tsid string, bkt *Bucket) {

	enc := tsz.NewEncoder(bkt.FirstTime)

	for _, pt := range bkt.dumpPoints() {
		enc.Encode(pt.Date, float32(pt.Value))
	}

	pts, err := enc.Close()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Index: %v\tPoints Size: %v\tPoints Count:%v\n", t.index, len(pts), bkt.count)
	t.setBlk(t.index, bkt.count, bkt.FirstTime, bkt.LastTime, pts)
	t.nextBlk()

	if p.cassandra != nil {
		p.InsertBucket(ksid, tsid, bkt.FirstTime, pts)
	}

}

func (t *serie) setBlk(index, count int, start, end int64, pts []byte) {

	t.blocks[index].start = start

	t.blocks[index].end = end

	t.blocks[index].count = count

	t.blocks[index].points = pts
}

func (t *serie) nextBlk() {
	if t.index >= len(t.blocks)-1 {
		t.index = 0
		return
	}
	t.index++
}
