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

	s.init()

	return s
}

func (t *serie) init() {

	log := gblog.With(
		zap.String("package", "gorilla"),
		zap.String("func", "serie/init"),
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
	)

	t.mtx.Lock()
	defer t.mtx.Unlock()

	log.Debug("initializing serie")

	now := time.Now().Unix()
	bktid := BlockID(now)

	bktPoints, err := t.persist.Read(t.ksid, t.tsid, bktid)
	if err != nil {
		log.Error(
			"error to initialize bucket",
			zap.Int64("blkid", bktid),
			zap.Error(err),
		)
	}

	if len(bktPoints) >= headerSize {
		pts, _, err := t.decode(bktPoints, bktid)
		if err != nil {
			log.Error(
				"error to initialize bucket",
				zap.Int64("blkid", bktid),
				zap.Error(err),
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
		t.blocks[i].SetCount(bucketSize)

		bktPoints, err := t.persist.Read(t.ksid, t.tsid, bktid)
		if err != nil {
			log.Error(
				"error to initialize block",
				zap.Int64("blkid", bktid),
				zap.Error(err),
			)
			continue
		}

		t.blocks[i].SetPoints(bktPoints)

		blkTime = blkTime - int64(bucketSize)
	}
}

func (t *serie) addPoint(date int64, value float32) gobol.Error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	log := gblog.With(
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.String("package", "storage"),
		zap.String("func", "serie/addPoint"),
	)

	delta, err := t.bucket.add(date, value)
	if err != nil {
		if delta >= bucketSize {

			go t.store(t.bucket)
			t.bucket = newBucket(BlockID(date))
			_, err = t.bucket.add(date, value)

			return err
		}

		log.Debug("point out of order, updating serie")
		return t.update(date, value)
	}

	log.Debug("point written successfully")
	return nil
}

func (t *serie) update(date int64, value float32) gobol.Error {
	t.persistMtx.Lock()
	defer t.persistMtx.Unlock()

	blkID := BlockID(date)

	log := gblog.With(
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.Int64("blkid", blkID),
		zap.Int64("pointDate", date),
		zap.Float32("pointValue", value),
		zap.String("package", "gorilla"),
		zap.String("func", "serie/update"),
	)

	index := getIndex(blkID)

	pByte, gerr := t.persist.Read(t.ksid, t.tsid, blkID)
	if gerr != nil {
		log.Error(
			gerr.Error(),
			zap.Error(gerr),
		)
		return gerr
	}

	var pts [bucketSize]*pb.Point
	var count int
	if len(pByte) >= headerSize {
		points, c, gerr := t.decode(pByte, blkID)
		if gerr != nil {
			log.Error(
				gerr.Error(),
				zap.Error(gerr),
			)
			return gerr
		}
		pts = points
		count = c
	}

	delta := date - blkID

	log.Debug(
		"point delta",
		zap.Int64("delta", delta),
	)

	if pts[delta] == nil {
		count++
	}
	pts[delta] = &pb.Point{Date: date, Value: value}

	ptsByte, gerr := t.encode(pts[:], blkID)
	if gerr != nil {
		log.Error(
			gerr.Error(),
			zap.Int64("delta", delta),
			zap.Int("count", count),
			zap.Int("blockSize", len(ptsByte)),
			zap.Error(gerr),
		)
		return gerr
	}

	gerr = t.persist.Write(t.ksid, t.tsid, blkID, ptsByte)
	if gerr != nil {
		log.Error(
			gerr.Error(),
			zap.Int64("delta", delta),
			zap.Int("count", count),
			zap.Int("blockSize", len(ptsByte)),
			zap.Error(gerr),
		)
		return gerr
	}

	if t.blocks[index].id == blkID {
		log.Debug(
			"updating block in memory",
			zap.Int64("delta", delta),
			zap.Int("count", count),
			zap.Int("blockSize", len(ptsByte)),
		)

		t.blocks[index].SetPoints(ptsByte)
		t.blocks[index].SetCount(count)
	}

	log.Debug(
		"block updated",
		zap.Int64("delta", delta),
		zap.Int("blockSize", len(ptsByte)),
		zap.Int("count", count),
	)

	return nil

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
	if end < oldest || end > time.Now().Unix() {
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

	blkidEnd := BlockID(end)
	for {
		blkidStart := BlockID(x)
		oldBlocksID = append(oldBlocksID, blkidStart)

		x += 2 * hour
		if blkidStart >= blkidEnd {
			break
		}
	}

	log := gblog.With(
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.Int64("start", start),
		zap.Int64("end", end),
		zap.String("package", "gorilla"),
		zap.String("func", "serie/readPersistence"),
	)

	var pts []*pb.Point
	for _, blkid := range oldBlocksID {

		log.Debug(
			"reading from persistence",
			zap.Int64("blockID", blkid),
		)

		pByte, err := t.persist.Read(t.ksid, t.tsid, blkid)
		if err != nil {
			return nil, err
		}

		if len(pByte) >= headerSize {

			p, _, err := t.decode(pByte, blkid)
			if err != nil {
				return nil, err
			}

			for i, np := range p {
				if np != nil {
					if np.Date >= start && np.Date <= end {
						pts = append(pts, np)
					}
					log.Debug(
						"point from persistence",
						zap.Int64("blockID", blkid),
						zap.Int64("pointDate", np.Date),
						zap.Float32("pointValue", np.Value),
						zap.Int("rangeIdx", i),
					)
				}
			}
		}
	}

	return pts, nil

}

func (t *serie) encode(points []*pb.Point, id int64) ([]byte, gobol.Error) {

	log := gblog.With(
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.Int64("blkid", id),
	)

	enc := tsz.NewEncoder(id)

	for _, pt := range points {
		if pt != nil {
			enc.Encode(pt.Date, pt.Value)
			log.Debug(
				"encoding point",
				zap.Int64("date", pt.Date),
				zap.Float32("value", pt.Value),
			)
		}
	}

	pts, err := enc.Close()
	if err != nil {
		log.Error(
			err.Error(),
			zap.Error(err),
		)
		return nil, errTsz("serie/encode", t.ksid, t.tsid, 0, err)
	}

	return pts, nil

}

func (t *serie) decode(points []byte, id int64) ([bucketSize]*pb.Point, int, gobol.Error) {
	dec := tsz.NewDecoder(points)

	var pts [bucketSize]*pb.Point
	var d int64
	var v float32

	var count int

	for dec.Scan(&d, &v) {
		delta := d - id
		gblog.Debug(
			"decoding block",
			zap.Int64("delta", delta),
			zap.Int64("blkID", id),
			zap.Int64("pointDate", d),
			zap.Float32("pointValue", v),
		)
		if delta >= 0 && delta < bucketSize {
			pts[delta] = &pb.Point{Date: d, Value: v}
			count++
		}
	}

	if err := dec.Close(); err != nil {
		return [bucketSize]*pb.Point{}, 0, errTsz("serie/decode", t.ksid, t.tsid, 0, err)
	}

	gblog.Debug(
		"decoded points from tsz",
		zap.String("package", "gorilla"),
		zap.String("func", "serie/decode"),
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.Int("count", count),
		zap.Int("size", len(points)),
	)
	return pts, count, nil

}

func (t *serie) store(bkt *bucket) {

	pts, err := t.encode(bkt.dumpPoints(), bkt.id)
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
	t.blocks[t.index].SetCount(bkt.count)
	t.blocks[t.index].SetPoints(pts)

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
