package gorilla

import (
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	tsz "github.com/uol/go-tsz"
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/depot"
)

type serie struct {
	mtx     sync.RWMutex
	ksid    string
	tsid    string
	bucket  *bucket
	blocks  [maxBlocks]block
	index   int
	timeout int64
	persist depot.Persistence
}

type query struct {
	id  int
	pts []Pnt
}

func newSerie(persist depot.Persistence, ksid, tsid string) *serie {

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

	gblog.Debugf("initializing serie %v - %v", t.ksid, t.tsid)

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

		gblog.Debugf("serie %v-%v - initializing %v for index %d", t.ksid, t.tsid, bktid, i)
		bktPoints, err := t.persist.Read(t.ksid, t.tsid, bktid)
		if err != nil {
			gblog.Error(err)
			continue
		}

		if len(bktPoints) > headerSize {
			gblog.Debugf("serie %v-%v - block %v initialized at index %v - size %v", t.ksid, t.tsid, bktid, i, len(bktPoints))
			t.blocks[i].id = bktid
			t.blocks[i].start = bktid
			t.blocks[i].end = bktid + int64(bucketSize-1)
			t.blocks[i].points = bktPoints
			t.blocks[i].count = bucketSize
		}

		blkTime = blkTime - int64(bucketSize)
	}

	gblog.Debugf("serie %v-%v initialized", t.ksid, t.tsid)
}

func (t *serie) addPoint(date int64, value float32) gobol.Error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	delta, err := t.bucket.add(date, value)
	if err != nil {
		if delta >= t.timeout {
			gblog.Debugf("serie %v-%v generating new bucket", t.ksid, t.tsid)
			go t.store(t.bucket)
			t.bucket = newBucket(BlockID(date))
			_, err = t.bucket.add(date, value)
			return err
		}

		return t.update(date, value)
	}

	return nil
}

func (t *serie) update(date int64, value float32) gobol.Error {

	blkID := BlockID(date)

	index := getIndex(blkID)

	if t.blocks[index].id == blkID {
		return t.blocks[index].update(date, value)
	}

	pts, gerr := t.persist.Read(t.ksid, t.tsid, blkID)
	if gerr != nil {
		return gerr
	}

	bkt := newBucket(blkID)

	if len(pts) < headerSize {
		_, gerr := bkt.add(date, value)
		if gerr != nil {
			return gerr
		}

		pts, err := t.encode(bkt)
		if err != nil {
			return errTszEnc(t.ksid, t.tsid, blkID, err)
		}

		if len(pts) > headerSize {
			return t.persist.Write(t.ksid, t.tsid, blkID, pts)
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
			return errUpdateDelta(t.ksid, t.tsid, blkID, delta)
		}

		points[delta] = &Pnt{Date: d, Value: v}
	}
	err := dec.Close()
	if err != nil {
		return errTszDec(t.ksid, t.tsid, blkID, err)
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
		return errTszEnc(t.ksid, t.tsid, blkID, err)
	}

	return t.persist.Write(t.ksid, t.tsid, blkID, np)
}

func (t *serie) read(start, end int64) (Pnts, gobol.Error) {
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
	oldest := t.blocks[index].start
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

	if start < oldest {
		p, err := t.readPersistence(start, oldest)
		if err != nil {
			return nil, err
		}
		if len(p) > 0 {
			pts := make(Pnts, len(p)+len(points))
			copy(pts, p)
			copy(pts[len(p):], points)
			points = pts
		}
	}

	return points, nil
}

func (t *serie) readPersistence(start, end int64) (Pnts, gobol.Error) {

	oldBlocksID := []int64{}

	for x := start; x <= end; x = x + (2 * hour) {
		oldBlocksID = append(oldBlocksID, BlockID(x))
	}

	var pts Pnts
	for _, blkid := range oldBlocksID {
		pByte, err := t.persist.Read(t.ksid, t.tsid, blkid)
		if err != nil {
			return nil, err
		}

		if len(pByte) > headerSize {

			p, err := t.decode(pByte)
			if err != nil {
				return nil, errTszDec(t.ksid, t.tsid, blkid, err)
			}

			pts = append(pts, p...)
		}

	}

	return pts, nil

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

func (t *serie) decode(points []byte) (Pnts, error) {
	dec := tsz.NewDecoder(points)

	var pts []Pnt
	var d int64
	var v float32

	for dec.Scan(&d, &v) {
		pts = append(pts, Pnt{Date: d, Value: v})
	}

	if err := dec.Close(); err != nil {
		return nil, err
	}

	return pts, nil

}

func (t *serie) store(bkt *bucket) {

	pts, err := t.encode(bkt)
	if err != nil {
		gblog.WithFields(logrus.Fields{
			"package": "storage",
			"func":    "store",
		}).Debugf("ksid=%v tsid=%v blkid=%v: %v", t.ksid, t.tsid, bkt.created, err)
		return
	}

	t.index = getIndex(bkt.created)
	t.blocks[t.index].id = bkt.created
	t.blocks[t.index].start = bkt.start
	t.blocks[t.index].end = bkt.end
	t.blocks[t.index].count = bkt.count
	t.blocks[t.index].points = pts

	if len(pts) > headerSize {
		err = t.persist.Write(t.ksid, t.tsid, bkt.created, pts)
		if err != nil {
			gblog.WithFields(logrus.Fields{
				"package": "storage",
				"func":    "store",
			}).Errorf("ksid=%v tsid=%v blkid=%v: %v", t.ksid, t.tsid, bkt.created, err)
			return
		}
	}
}
