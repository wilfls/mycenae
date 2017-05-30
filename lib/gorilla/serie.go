package gorilla

import (
	"sync"
	"time"

	tsz "github.com/uol/go-tsz"
	pb "github.com/uol/mycenae/lib/proto"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/depot"
	"go.uber.org/zap"
)

type serie struct {
	mtx        sync.RWMutex
	ksid       string
	tsid       string
	bucket     *bucket
	blocks     [maxBlocks]block
	index      int
	timeout    int64
	persist    depot.Persistence
	persistMtx sync.RWMutex
}

type query struct {
	id  int
	pts []*pb.Point
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

	t.mtx.Lock()
	defer t.mtx.Unlock()

	gblog.Debug(
		"initializing serie",
		zap.String("package", "gorilla"),
		zap.String("func", "serie/init"),
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
	)

	now := time.Now().Unix()
	bktid := BlockID(now)

	bktPoints, err := t.persist.Read(t.ksid, t.tsid, bktid)
	if err != nil {
		gblog.Error(
			"error to initialize bucket",
			zap.String("ksid", t.ksid),
			zap.String("tsid", t.tsid),
			zap.Int64("blkid", bktid),
			zap.Error(err),
			zap.String("package", "gorilla"),
			zap.String("func", "serie/init"),
		)
	}

	if len(bktPoints) >= headerSize {
		pts, err := t.decode(bktPoints)
		if err != nil {
			gblog.Error(
				"error to initialize bucket",
				zap.String("ksid", t.ksid),
				zap.String("tsid", t.tsid),
				zap.Int64("blkid", bktid),
				zap.Error(err),
				zap.String("package", "gorilla"),
				zap.String("func", "serie/init"),
			)
		}

		for _, p := range pts {
			if p != nil {
				t.bucket.add(p.Date, p.Value)
			}
		}

	}

	blkTime := now - int64(2*hour)
	for x := 0; x < maxBlocks; x++ {

		bktid = BlockID(blkTime)
		i := getIndex(bktid)

		t.blocks[i].id = bktid
		t.blocks[i].start = bktid
		t.blocks[i].end = bktid + int64(bucketSize-1)
		t.blocks[i].count = bucketSize

		bktPoints, err := t.persist.Read(t.ksid, t.tsid, bktid)
		if err != nil {
			gblog.Error(
				"error to initialize block",
				zap.String("ksid", t.ksid),
				zap.String("tsid", t.tsid),
				zap.Int64("blkid", bktid),
				zap.Error(err),
				zap.String("package", "gorilla"),
				zap.String("func", "serie/init"),
			)
			continue
		}

		if len(bktPoints) >= headerSize {

			gblog.Debug(
				"",
				zap.String("ksid", t.ksid),
				zap.String("tsid", t.tsid),
				zap.Int64("blkid", bktid),
				zap.Int("index", i),
				zap.Int("size", len(bktPoints)),
				zap.String("package", "gorilla"),
				zap.String("func", "serie/init"),
			)

			t.blocks[i].points = bktPoints

		}

		blkTime = blkTime - int64(bucketSize)
	}
}

func (t *serie) addPoint(date int64, value float32) gobol.Error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	delta, err := t.bucket.add(date, value)
	if err != nil {
		if delta >= t.timeout {

			gblog.Debug(
				"",
				zap.String("ksid", t.ksid),
				zap.String("tsid", t.tsid),
				zap.String("package", "storage/serie"),
				zap.String("func", "addPoint"),
			)

			go t.store(t.bucket)
			t.bucket = newBucket(BlockID(date))
			_, err = t.bucket.add(date, value)

			return err
		}

		gblog.Debug(
			"point out of order, updating serie",
			zap.String("ksid", t.ksid),
			zap.String("tsid", t.tsid),
			zap.String("package", "gorilla"),
			zap.String("func", "serie/addPoint"),
		)
		return t.update(date, value)
	}

	gblog.Debug(
		"point written successfully",
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.String("package", "gorilla"),
		zap.String("func", "serie/addPoint"),
	)
	return nil
}

func (t *serie) update(date int64, value float32) gobol.Error {
	t.persistMtx.Lock()
	defer t.persistMtx.Unlock()

	f := "serie/update"

	blkID := BlockID(date)

	index := getIndex(blkID)

	if t.blocks[index].id == blkID {

		gblog.Debug(
			"updating block in memory",
			zap.String("ksid", t.ksid),
			zap.String("tsid", t.tsid),
			zap.Int64("blkid", blkID),
			zap.String("package", "gorilla"),
			zap.String("func", f),
		)

		gerr := t.blocks[index].update(date, value)
		if gerr != nil {
			gblog.Error(
				gerr.Error(),
				zap.String("ksid", t.ksid),
				zap.String("tsid", t.tsid),
				zap.Int64("blkid", blkID),
				zap.String("package", "gorilla"),
				zap.String("func", f),
				zap.Error(gerr),
			)
			return gerr
		}

		gerr = t.persist.Write(t.ksid, t.tsid, blkID, t.blocks[index].points)
		if gerr != nil {
			gblog.Error(
				gerr.Error(),
				zap.String("ksid", t.ksid),
				zap.String("tsid", t.tsid),
				zap.String("package", "gorilla"),
				zap.String("func", "serie/update"),
				zap.Error(gerr),
			)

			return gerr
		}

		return nil
	}

	pByte, gerr := t.persist.Read(t.ksid, t.tsid, blkID)
	if gerr != nil {
		return gerr
	}

	blk := &block{id: blkID}

	if len(pByte) >= headerSize {
		blk.points = pByte
	}

	gerr = blk.update(date, value)
	if gerr != nil {
		return gerr
	}

	return t.persist.Write(t.ksid, t.tsid, blkID, blk.points)
}

func (t *serie) read(start, end int64) ([]*pb.Point, gobol.Error) {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	start--
	end++

	ptsCh := make(chan query)
	defer close(ptsCh)

	for x := 0; x < maxBlocks; x++ {
		go t.blocks[x].rangePoints(x, start, end, ptsCh)
	}

	result := make([][]*pb.Point, maxBlocks)

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
	points := make([]*pb.Point, resultCount)

	size = 0

	index := getIndex(time.Now().Unix()) + 1
	if index >= maxBlocks {
		index = 0
	}
	oldest := t.blocks[index].start

	if oldest == 0 {
		oldest = time.Now().Unix() - int64(26*hour)
	}
	if end < oldest {
		oldest = end
	}
	idx := index
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

	gblog.Debug(
		"",
		zap.String("package", "storage/serie"),
		zap.String("func", "read"),
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.Int64("start", start),
		zap.Int64("end", end),
		zap.Int("memoryCount", len(points)),
		zap.Int64("oldest", oldest),
		zap.Int("oldestIndex", idx),
	)

	if start < oldest {
		p, err := t.readPersistence(start, oldest)
		if err != nil {
			return nil, err
		}
		if len(p) > 0 {
			pts := make([]*pb.Point, len(p)+len(points))
			copy(pts, p)
			copy(pts[len(p):], points)
			points = pts
		}
		gblog.Debug(
			"",
			zap.String("package", "storage/serie"),
			zap.String("func", "read"),
			zap.String("ksid", t.ksid),
			zap.String("tsid", t.tsid),
			zap.Int("persistenceCount", len(p)),
		)
	}

	return points, nil
}

func (t *serie) readPersistence(start, end int64) ([]*pb.Point, gobol.Error) {
	t.persistMtx.Lock()
	defer t.persistMtx.Unlock()

	oldBlocksID := []int64{}

	x := start

	for {

		oldBlocksID = append(oldBlocksID, BlockID(x))

		x += 2 * hour
		if x >= end {
			break
		}
	}

	var pts []*pb.Point
	for _, blkid := range oldBlocksID {

		gblog.Debug(
			"reading from persistence",
			zap.String("package", "storage"),
			zap.String("func", "serie/readPersistence"),
			zap.String("ksid", t.ksid),
			zap.String("tsid", t.tsid),
			zap.Int64("blockID", blkid),
		)

		pByte, err := t.persist.Read(t.ksid, t.tsid, blkid)
		if err != nil {
			return nil, err
		}

		if len(pByte) >= headerSize {

			p, err := t.decode(pByte)
			if err != nil {
				return nil, errTsz("serie/readPersistence", t.ksid, t.tsid, blkid, err)
			}

			for _, np := range p {
				if np.Date >= start && np.Date <= end {
					pts = append(pts, np)
				}
			}
		}
	}

	return pts, nil

}

func (t *serie) encode(bkt *bucket) ([]byte, error) {

	var enc *tsz.Encoder
	for _, pt := range bkt.dumpPoints() {
		if enc == nil {
			enc = tsz.NewEncoder(pt.Date)
		}
		if pt != nil {
			enc.Encode(pt.Date, pt.Value)
		}
	}

	return enc.Close()

}

func (t *serie) decode(points []byte) ([]*pb.Point, error) {
	dec := tsz.NewDecoder(points)

	var pts [bucketSize]*pb.Point
	var d int64
	var v float32
	var i int

	for dec.Scan(&d, &v) {
		pts[i] = &pb.Point{Date: d, Value: v}
		i++
	}

	if err := dec.Close(); err != nil {
		return nil, err
	}

	gblog.Debug(
		"decoded points from tsz",
		zap.String("package", "gorilla"),
		zap.String("func", "serie/decode"),
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.Int("count", i),
		zap.Int("size", len(points)),
	)
	return pts[:i], nil

}

func (t *serie) store(bkt *bucket) {

	pts, err := t.encode(bkt)
	if err != nil {
		gblog.Error(
			"",
			zap.String("package", "gorilla"),
			zap.String("func", "serie/store"),
			zap.String("ksid", t.ksid),
			zap.String("tsid", t.tsid),
			zap.Int64("blkid", bkt.id),
			zap.Error(err),
		)
		return
	}

	t.index = getIndex(bkt.id)
	t.blocks[t.index].id = bkt.id
	t.blocks[t.index].count = bkt.count
	t.blocks[t.index].points = pts

	if len(pts) >= headerSize {
		err = t.persist.Write(t.ksid, t.tsid, bkt.id, pts)
		if err != nil {
			gblog.Error(
				"",
				zap.String("package", "gorilla"),
				zap.String("func", "serie/store"),
				zap.String("ksid", t.ksid),
				zap.String("tsid", t.tsid),
				zap.Int64("blkid", bkt.id),
				zap.Error(err),
			)
			return
		}
	}
}
