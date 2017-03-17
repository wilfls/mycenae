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
	bucket  *Bucket
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

	if delta >= t.bucket.Timeout {
		//fmt.Println(delta)
		//d := t.tc.Now() - t.bucket.Created
		//if d >= t.bucket.Timeout {

		t.store(p, ksid, tsid, t.bucket)
		t.bucket = newBucket(t.tc)
		_, err = t.bucket.add(date, value)
		return err
		//}
	}

	return err

}

func (t *serie) read(start, end int64) []plot.Pnt {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	now := t.tc.Now()
	dStart := now - start
	dEnd := now - end
	pts := []plot.Pnt{}

	if dStart >= 86400 && dEnd >= 86400 {
		// read only from cassandra
		fmt.Println("from cassandra...")
		return pts
	}

	ptsCh := make(chan []plot.Pnt)
	defer close(ptsCh)

	blkCount := len(t.blocks) - 1

	y := t.index
	blks := 0
	for x := 0; x <= blkCount; x++ {
		if t.blocks[y].start >= start {
			blks++
			go t.blocks[y].rangePoints(start, end, ptsCh)
		}
		y--
		if y < 0 {
			y = blkCount
		}
		//fmt.Println("index", y)
	}

	//if dStart >= 7200 {
	//for i := 0; i <= blkCount; i++ {
	//	go t.blocks[i].rangePoints(start, end, ptsCh)
	//}
	//}

	//if dEnd <= 7200 {
	if t.bucket.FirstTime >= start {
		go t.bucket.rangePoints(start, end, ptsCh)
		blks++
	}

	//}

	//fmt.Println(blks + 1)
	for i := 0; i < blks; i++ {
		p := <-ptsCh
		//fmt.Println("read", len(p))
		if len(p) > 0 {
			pts = append(pts, p...)
		}
	}

	//fmt.Println(len(pts))
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
		if pt != nil {
			enc.Encode(pt.Date, float32(pt.Value))
		}
	}

	pts, err := enc.Close()
	if err != nil {
		panic(err)
	}

	//fmt.Printf("Index: %v\tPoints Size: %v\tPoints Count:%v\n", t.index, len(pts), bkt.count)
	t.setBlk(t.index, bkt.count, bkt.FirstTime, bkt.LastTime, pts)
	t.nextBlk()

	if p.cassandra != nil {
		p.InsertBucket(ksid, tsid, bkt.FirstTime, pts)
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
