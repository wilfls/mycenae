package storage

import (
	"fmt"
	"sync"
	"time"

	tsz "github.com/uol/go-tsz"
)

const (
	secHour = 3600
	secDay  = 24 * secHour
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

func newSerie(cass Cassandra, ksid, tsid string, tc TC) *serie {

	// Must fetch this block from cassandra
	s := &serie{
		ksid:    ksid,
		tsid:    tsid,
		timeout: 2 * secHour,
		tc:      tc,
		blocks:  [12]block{},
		bucket:  newBucket(tc),
	}

	go s.init(cass)

	return s
}

func (t *serie) init(cass Cassandra) {

	now := t.tc.Now()
	bktid := bucketKey(now)

	bktPoints, err := cass.ReadBlock(t.ksid, t.tsid, bktid)
	if err != nil {
		for {
			bktPoints, err = cass.ReadBlock(t.ksid, t.tsid, bktid)
			if err == nil {
				break
			}
			time.Sleep(time.Second)
		}
	}

	if len(bktPoints) > 0 {
		dec := tsz.NewDecoder(bktPoints)

		var date int64
		var value float32
		for dec.Scan(&date, &value) {
			t.bucket.add(date, value)
		}

		if err := dec.Close(); err != nil {
			gblog.Error(err)
		}
	}

	yesterday := now - secDay
	twoHours := int64(2 * secHour)

	ct := yesterday
	for {
		bktid = bucketKey(ct)
		idx := getIndex(bktid)

		bktPoints, err := cass.ReadBlock(t.ksid, t.tsid, bktid)
		if err != nil {
			gblog.Error(err)
		}

		t.blocks[idx].start = bktid
		t.blocks[idx].end = bktid + twoHours
		t.blocks[idx].points = bktPoints
		t.blocks[idx].count = int(twoHours)

		if ct >= now {
			break
		}
		ct = ct + twoHours
	}

	gblog.Infof("serie %v%v initialized", t.ksid, t.tsid)
}

func (t *serie) addPoint(cass Cassandra, ksid, tsid string, date int64, value float32) error {
	if date-1 > t.tc.Now() {
		gblog.Errorf("point in future %v is not supported: %v", date, t.tc.Now())
		return fmt.Errorf("point in future is not supported")
	}
	t.mtx.Lock()
	defer t.mtx.Unlock()

	gblog.Infof("saving point at %v => %v", ksid, tsid)
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
			// so we must read from cassandra, open the block,
			// insert the point and save it again at cassandra
			//go t.singleStore(cass, ksid, tsid, date, value)
			return nil
		}
	}

	gblog.Infof("point %v - %v saved", date, value)

	return err
}

func (t *serie) read(cass Cassandra, start, end int64) Pnts {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	index := t.index + 1

	var startMemory int64
	if index >= len(t.blocks) {
		startMemory = t.blocks[0].start
	} else {
		startMemory = t.blocks[index].start
	}

	if start < startMemory {
		// read from cassandra
		//cass.ReadBucket()

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

	gblog.Infof("Interval start: %v\tend: %v", start, end)
	gblog.Infof("serie %v %v - points read: %v", t.ksid, t.tsid, len(points))

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

	t.setBlk(bkt.count, bkt.start, bkt.end, pts)

	if cass.session != nil {
		cass.InsertBlock(ksid, tsid, bkt.created, pts)
	}

}

func (t *serie) setBlk(count int, start, end int64, pts []byte) {

	t.index = getIndex(start)

	t.blocks[t.index].start = start

	t.blocks[t.index].end = end

	t.blocks[t.index].count = count

	t.blocks[t.index].points = pts
}

func bucketKey(timestamp int64) int64 {
	now := time.Unix(timestamp, 0)
	_, m, s := now.Clock()
	now = now.Add(-(time.Duration(m) * time.Minute) - (time.Duration(s) * time.Second))

	if now.Hour()%2 == 0 {
		return now.Unix()
	}

	return now.Unix() - secHour
}

func getIndex(timestamp int64) int {

	return time.Unix(timestamp, 0).Hour() / 2

}
