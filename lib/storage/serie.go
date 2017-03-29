package storage

import (
	"fmt"
	"sync"
	"time"

	tsz "github.com/uol/go-tsz"
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

type query struct {
	id  int
	pts []Pnt
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

func (t *serie) addPoint(cass Cassandra, ksid, tsid string, date int64, value float32) error {
	if date > t.tc.Now() {
		return fmt.Errorf("point in future is not supported")
	}
	t.mtx.Lock()
	defer t.mtx.Unlock()

	delta, err := t.bucket.add(date, value)
	if err != nil {
		if delta >= t.bucket.timeout {
			t.store(cass, ksid, tsid, t.bucket)
			t.bucket = newBucket(t.tc)
			_, err = t.bucket.add(date, value)
			return err
		}

		// Point must be saved in cassandra
		if delta <= -86400 {
			// At this point we don't care to lose a single point
			go t.singleStore(cass, ksid, tsid, date, value)
			return nil
		}
	}
	return err
}

func (t *serie) read(cass Cassandra, start, end int64) Pnts {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	now := t.tc.Now()
	dStart := now - start
	dEnd := now - end

	if dStart >= 86400 && dEnd >= 86400 {
		// read only from cassandra
		fmt.Println("from cassandra...")
		return Pnts{}
	}

	ptsCh := make(chan query)
	defer close(ptsCh)

	blks := 0

	go t.bucket.rangePoints(blks, start, end, ptsCh)
	blks++

	blkCount := len(t.blocks) - 1

	for x := 0; x <= blkCount; x++ {
		go t.blocks[x].rangePoints(blks, start, end, ptsCh)
		blks++
	}

	result := make([]Pnts, blks)

	size := 0
	resultCount := 0

	for i := 0; i < blks; i++ {
		q := <-ptsCh
		result[q.id] = q.pts
		size = len(result[q.id])
		if size > 0 {
			resultCount += size
		}
	}

	points := make(Pnts, resultCount)

	size = 0

	index := t.index + 1

	// index must be from oldest point to the newest
	for i := 1; i <= blks-1; i++ {
		if len(result[index]) == 0 {
			if index == blks-1 {
				index = 1
				continue
			}
			index++
			continue
		}

		copy(points[size:], result[index])
		size += len(result[index])

		if index == blks-1 {
			index = 1
		}
		index++
	}

	if len(result[0]) > 0 {
		copy(points[size:], result[0])
	}

	return points
}

func (t *serie) store(cass Cassandra, ksid, tsid string, bkt *bucket) {

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
	t.setBlk(bkt.count, bkt.start, bkt.end, pts)

	if cass.session != nil {
		cass.InsertBucket(ksid, tsid, bkt.start, pts)
	}

}

func (t *serie) singleStore(cass Cassandra, ksid, tsid string, date int64, value float32) {
	enc := tsz.NewEncoder(date)

	enc.Encode(date, value)

	pts, err := enc.Close()
	if err != nil {
		panic(err)
	}

	if cass.session != nil {
		cass.InsertBucket(ksid, tsid, date, pts)
	}
}

func (t *serie) setBlk(count int, start, end int64, pts []byte) {

	t.index = getIndex(start)

	//fmt.Printf("index: %v\tstart: %v\tend: %v\tcount: %v\tblk size: %v\n", index, start, end, count, len(pts))
	t.blocks[t.index].start = start

	t.blocks[t.index].end = end

	t.blocks[t.index].count = count

	t.blocks[t.index].points = pts
}

func getIndex(timestamp int64) int {
	ts := time.Unix(timestamp, 0)

	switch ts.Hour() {
	case 0, 1:
		return 0
	case 2, 3:
		return 1
	case 4, 5:
		return 2
	case 6, 7:
		return 3
	case 8, 9:
		return 4
	case 10, 11:
		return 5
	case 12, 13:
		return 6
	case 14, 15:
		return 7
	case 16, 17:
		return 8
	case 18, 19:
		return 9
	case 20, 21:
		return 10
	case 22, 23:
		return 11
	}

	return 0
}
