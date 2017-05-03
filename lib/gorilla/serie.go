package gorilla

import (
	"sync"
	"time"

	tsz "github.com/uol/go-tsz"
)

const (
	secHour = 3600
	secDay  = 24 * secHour
	maxBlks = 12
	header  = 12
)

type serie struct {
	mtx     sync.RWMutex
	ksid    string
	tsid    string
	bucket  *bucket
	blocks  [maxBlks]block
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
		timeout: 2 * secHour,
		persist: persist,
		blocks:  [12]block{},
		bucket:  newBucket(bucketKey(time.Now().Unix())),
	}

	go s.init()

	return s
}

func (t *serie) init() {

	gblog.Infof("initializing serie %v - %v", t.ksid, t.tsid)

	now := time.Now().Unix()
	bktid := bucketKey(now)

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

	if len(bktPoints) > header {
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

	yesterday := now - secDay
	twoHours := int64(2 * secHour)

	ct := yesterday
	for {
		if ct >= now {
			break
		}
		bktid = bucketKey(ct)
		i := getIndex(bktid)

		gblog.Infof("serie %v-%v - initializing %v for index %d", t.ksid, t.tsid, bktid, i)
		bktPoints, err := t.persist.Read(t.ksid, t.tsid, bktid)
		if err != nil {
			gblog.Error(err)
		}

		if len(bktPoints) > header {
			gblog.Infof("serie %v-%v - block %v initialized at index %v - size %v", t.ksid, t.tsid, bktid, i, len(bktPoints))
			t.blocks[i].start = bktid
			t.blocks[i].end = bktid + twoHours - 1
			t.blocks[i].points = bktPoints
			t.blocks[i].count = int(twoHours)
		}

		ct = ct + twoHours
	}

	gblog.Infof("serie %v-%v initialized", t.ksid, t.tsid)
}

func (t *serie) addPoint(ksid, tsid string, date int64, value float32) error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	delta, err := t.bucket.add(date, value)
	if err != nil {

		if delta >= t.timeout {
			gblog.Infof("serie %v-%v generating new bucket", t.ksid, t.tsid)
			t.store(ksid, tsid, t.bucket)
			t.bucket = newBucket(t.bucket.created + t.timeout)
			_, err = t.bucket.add(date, value)
			return err
		}

		// Point must be saved in cassandra
		if delta <= -int64(secDay) {
			// At this point we don't care to lose a single point
			// so we must read from cassandra, open the block,
			// insert the point and save it again at cassandra
			//go t.singleStore(cass, ksid, tsid, date, value)
			return nil
		}
	}

	return err
}

func (t *serie) read(start, end int64) Pnts {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	ptsCh := make(chan query)
	defer close(ptsCh)

	for x := 0; x < maxBlks; x++ {
		go t.blocks[x].rangePoints(x, start, end, ptsCh)
	}

	result := make([]Pnts, maxBlks)

	size := 0
	resultCount := 0

	for i := 0; i < maxBlks; i++ {
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
	index := t.index + 1
	if index >= maxBlks {
		index = 0
	}
	// index must be from oldest point to the newest
	for i := 0; i < maxBlks; i++ {
		if len(result[index]) > 0 {
			copy(points[size:], result[index])
			size += len(result[index])
		}
		index++
		if index >= maxBlks {
			index = 0
		}
	}

	if len(q.pts) > 0 {
		copy(points[size:], q.pts)
	}

	gblog.Infof("serie %v %v - points read: %v", t.ksid, t.tsid, len(points))

	return points
}

func (t *serie) store(ksid, tsid string, bkt *bucket) {

	enc := tsz.NewEncoder(bkt.start)

	for _, pt := range bkt.dumpPoints() {
		if pt != nil {
			enc.Encode(pt.Date, pt.Value)
		}
	}

	pts, err := enc.Close()
	if err != nil {
		gblog.Errorf("serie %v %v - key %v: %v", t.ksid, t.tsid, bkt.created, err)
		return
	}

	t.index = getIndex(bkt.created)
	t.blocks[t.index].start = bkt.start
	t.blocks[t.index].end = bkt.end
	t.blocks[t.index].count = bkt.count
	t.blocks[t.index].points = pts

	if len(pts) > header {
		go t.persist.Write(ksid, tsid, bkt.created, pts)
	}

}

func bucketKey(timestamp int64) int64 {
	now := time.Unix(timestamp, 0).UTC()

	_, m, s := now.Clock()
	now = now.Add(-(time.Duration(m) * time.Minute) - (time.Duration(s) * time.Second))

	if now.Hour()%2 == 0 {
		return now.Unix()
	}

	return now.Unix() - secHour
}

func getIndex(timestamp int64) int {

	return time.Unix(timestamp, 0).UTC().Hour() / 2

}
