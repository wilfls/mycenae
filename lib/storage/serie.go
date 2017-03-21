package storage

import (
	"fmt"
	"sync"

	tsz "github.com/uol/go-tsz"
	"github.com/uol/mycenae/lib/plot"
)

type serie struct {
	mtx     sync.RWMutex
	ksid    string
	tsid    string
	bucket  *bucket
	blocks  [12]block
	index   int
	timeout int64
	tc      TC
}

func newSerie(ksid, tsid string, tc TC) *serie {

	s := &serie{
		ksid:    ksid,
		tsid:    tsid,
		timeout: 7200,
		tc:      tc,
		blocks:  [12]block{},
		bucket:  newBucket(tc),
	}

	return s
}

func (t *serie) addPoint(p persistence, ksid, tsid string, date int64, value float64) error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	delta, err := t.bucket.add(date, value)

	if delta >= t.bucket.timeout {
		t.store(p, ksid, tsid, t.bucket)
		t.bucket = newBucket(t.tc)
		_, err = t.bucket.add(date, value)
		return err
	}

	return err

}

func (t *serie) read(start, end int64) []plot.Pnt {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	now := t.tc.Now()
	dStart := now - start
	dEnd := now - end

	if dStart >= 86400 && dEnd >= 86400 {
		// read only from cassandra
		fmt.Println("from cassandra...")
		return []plot.Pnt{}
	}

	ptsCh := make(chan []plot.Pnt)
	defer close(ptsCh)

	blks := 0

	go t.bucket.rangePoints(start, end, ptsCh)
	blks++

	blkCount := len(t.blocks) - 1

	for x := 0; x <= blkCount; x++ {
		go t.blocks[x].rangePoints(start, end, ptsCh)
		blks++
	}

	result := make([][]plot.Pnt, blks)

	size := 0
	resultCount := 0

	for i := 0; i < blks; i++ {
		result[i] = <-ptsCh
		size = len(result[i])
		if size > 0 {
			resultCount += size
		}
	}

	points := make([]plot.Pnt, resultCount)

	size = 0
	for i := 0; i < blks; i++ {
		size += len(result[i])
		copy(points[:size], result[i])
	}

	return points
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

func (t *serie) store(p persistence, ksid, tsid string, bkt *bucket) {

	enc := tsz.NewEncoder(bkt.start)

	for _, pt := range bkt.dumpPoints() {
		if pt != nil {
			enc.Encode(pt.Date, float32(pt.Value))
		}
	}

	pts, err := enc.Close()
	if err != nil {
		panic(err)
	}

	//fmt.Printf("Index: %v\tPoints Size: %v\tPoints Count:%v\n", t.index, len(pts), bkt.count)
	t.setBlk(t.index, bkt.count, bkt.start, bkt.end, pts)
	t.nextBlk()

	if p.cassandra != nil {
		p.InsertBucket(ksid, tsid, bkt.start, pts)
	}

}

func (t *serie) setBlk(index, count int, start, end int64, pts []byte) {

	//fmt.Printf("index: %v\tstart: %v\tend: %v\tcount: %v\tblk size: %v\n", index, start, end, count, len(pts))
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
