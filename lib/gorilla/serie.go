package gorilla

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
	blocks  [maxBlocks]block
	index   int
	timeout int64
	persist Persistence
}

type query struct {
	id  int
	pts []Pnt
}

func newSerie(persist Persistence, ksid, tsid string) *serie {

	// Must fetch this block from cassandra
	s := &serie{
		ksid:    ksid,
		tsid:    tsid,
		timeout: 2 * hour,
		persist: persist,
		blocks:  [12]block{},
		bucket:  newBucket(BlockID(time.Now().Unix())),
	}

	go s.init()

	return s
}

func (t *serie) init() {

	gblog.Infof("initializing serie %v - %v", t.ksid, t.tsid)

	now := time.Now().Unix()
	bktid := BlockID(now)

	bktPoints, err := t.persist.Read(t.ksid, t.tsid, bktid)
	if err != nil {
		for {
			bktPoints, err = t.persist.Read(t.ksid, t.tsid, bktid)
			if err == nil {
				break
			}
			time.Sleep(time.Second)
		}
	}

	if len(bktPoints) > headerSize {
		dec := tsz.NewDecoder(bktPoints)

		var date int64
		var value float32
		for dec.Scan(&date, &value) {
			t.bucket.add(date, value)
		}

		if err := dec.Close(); err != nil {
			gblog.Error("serie %v-%v - unable to read block", t.ksid, t.tsid, err)
		}
	}

	t.mtx.Lock()
	defer t.mtx.Unlock()

	blkTime := now - int64(bucketSize)
	for x := 0; x < maxBlocks; x++ {

		bktid = BlockID(blkTime)
		i := getIndex(bktid)

		gblog.Infof("serie %v-%v - initializing %v for index %d", t.ksid, t.tsid, bktid, i)
		bktPoints, err := t.persist.Read(t.ksid, t.tsid, bktid)
		if err != nil {
			gblog.Error(err)
			continue
		}

		if len(bktPoints) > headerSize {
			gblog.Infof("serie %v-%v - block %v initialized at index %v - size %v", t.ksid, t.tsid, bktid, i, len(bktPoints))
			t.blocks[i].id = bktid
			t.blocks[i].start = bktid
			t.blocks[i].end = bktid + int64(bucketSize-1)
			t.blocks[i].points = bktPoints
			t.blocks[i].count = bucketSize
		}

		blkTime = blkTime - int64(bucketSize)
	}

	gblog.Infof("serie %v-%v initialized", t.ksid, t.tsid)
}

func (t *serie) addPoint(date int64, value float32) error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	delta, err := t.bucket.add(date, value)
	if err != nil {

		if delta >= t.timeout {
			gblog.Infof("serie %v-%v generating new bucket", t.ksid, t.tsid)
			go t.store(t.bucket)
			t.bucket = newBucket(BlockID(date))
			_, err = t.bucket.add(date, value)
			return err
		}

		return t.update(date, value)

	}

	return nil
}

func (t *serie) update(date int64, value float32) error {

	blkID := BlockID(date)

	index := getIndex(blkID)

	if t.blocks[index].id == blkID {
		return t.blocks[index].update(date, value)
	}

	pts, err := t.persist.Read(t.ksid, t.tsid, blkID)
	if err != nil {
		return err
	}

	bkt := newBucket(blkID)

	if len(pts) < headerSize {
		_, err := bkt.add(date, value)
		if err != nil {
			return err
		}

		pts, err := t.encode(bkt)
		if err != nil {
			return err
		}

		if len(pts) > headerSize {
			go t.persist.Write(t.ksid, t.tsid, blkID, pts)
		}
		return nil
	}

	points := [bucketSize]*Pnt{}

	dec := tsz.NewDecoder(pts)
	var d int64
	var v float32

	for dec.Scan(&d, &v) {

		delta := d - blkID

		if delta > bucketSize || delta < 0 {
			return fmt.Errorf("aborting block update %v, delta %v not in range: %v", blkID, delta, bucketSize)
		}

		points[delta] = &Pnt{Date: d, Value: v}
	}
	err = dec.Close()
	if err != nil {
		return fmt.Errorf("aborting block update, error decoding block %v: %v", blkID, err)
	}

	delta := date - blkID
	points[delta] = &Pnt{Date: date, Value: value}

	var t0 int64
	for _, p := range points {
		if p != nil {
			t0 = p.Date
			break
		}
	}

	enc := tsz.NewEncoder(t0)
	for _, p := range points {
		if p != nil {
			enc.Encode(p.Date, p.Value)
		}
	}

	np, err := enc.Close()
	if err != nil {
		return fmt.Errorf("aborting block update, error encoding block %v: %v", blkID, err)
	}

	go t.persist.Write(t.ksid, t.tsid, blkID, np)

	return nil

}

func (t *serie) read(start, end int64) Pnts {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	ptsCh := make(chan query)
	defer close(ptsCh)

	for x := 0; x < maxBlocks; x++ {
		go t.blocks[x].rangePoints(x, start, end, ptsCh)
	}

	result := make([]Pnts, maxBlocks)

	size := 0
	resultCount := 0

	for i := 0; i < maxBlocks; i++ {
		q := <-ptsCh
		result[q.id] = q.pts
		size = len(result[q.id])
		if size > 0 {
			resultCount += size
		}
	}

	go t.bucket.rangePoints(0, start, end, ptsCh)
	q := <-ptsCh

	resultCount += len(q.pts)
	points := make(Pnts, resultCount)

	size = 0
	indexTime := time.Now().Unix() - int64(2*hour)
	index := getIndex(indexTime) + 1
	if index >= maxBlocks {
		index = 0
	}
	// index must be from oldest point to the newest
	for i := 0; i < maxBlocks; i++ {
		if len(result[index]) > 0 {
			copy(points[size:], result[index])
			size += len(result[index])
		}
		index++
		if index >= maxBlocks {
			index = 0
		}
	}

	if len(q.pts) > 0 {
		copy(points[size:], q.pts)
	}

	gblog.Infof("serie %v %v - points read: %v", t.ksid, t.tsid, len(points))

	return points
}

func (t *serie) encode(bkt *bucket) ([]byte, error) {
	enc := tsz.NewEncoder(bkt.start)

	for _, pt := range bkt.dumpPoints() {
		if pt != nil {
			enc.Encode(pt.Date, pt.Value)
		}
	}

	return enc.Close()

}

func (t *serie) store(bkt *bucket) {

	pts, err := t.encode(bkt)
	if err != nil {
		gblog.Errorf("serie %v %v - key %v: %v", t.ksid, t.tsid, bkt.created, err)
		return
	}

	t.index = getIndex(bkt.created)
	t.blocks[t.index].id = bkt.created
	t.blocks[t.index].start = bkt.start
	t.blocks[t.index].end = bkt.end
	t.blocks[t.index].count = bkt.count
	t.blocks[t.index].points = pts

	if len(pts) > headerSize {
		go t.persist.Write(t.ksid, t.tsid, bkt.created, pts)
	}

}
