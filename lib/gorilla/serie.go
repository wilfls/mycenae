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
	blocks     [maxBlocks]*block
	index      int
	lastWrite  int64
	lastAccess int64
	persist    depot.Persistence
}

type query struct {
	id  int
	pts []*pb.Point
}

func newSerie(persist depot.Persistence, ksid, tsid string) *serie {

	s := &serie{
		ksid:       ksid,
		tsid:       tsid,
		lastWrite:  time.Now().Unix(),
		lastAccess: time.Now().Unix(),
		persist:    persist,
		blocks:     [12]*block{},
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

	t.index = getIndex(now)

	blkTime := now
	//	for x := 0; x < maxBlocks; x++ {

	blkid := BlockID(blkTime)
	i := getIndex(blkid)

	t.blocks[i] = &block{id: blkid}

	blkPoints, err := t.persist.Read(t.ksid, t.tsid, blkid)
	if err != nil {
		log.Error(
			"error to initialize block",
			zap.Int64("blkid", blkid),
			zap.Error(err),
		)
		return
	}

	t.blocks[i].SetPoints(blkPoints)

	//blkTime = blkTime - int64(bucketSize)
	//}
}

func (t *serie) addPoint(date int64, value float32) gobol.Error {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	now := time.Now().Unix()

	log := gblog.With(
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.String("package", "gorilla"),
		zap.String("func", "serie/addPoint"),
	)

	delta := int(date - t.blocks[t.index].id)

	if delta >= bucketSize {
		t.lastWrite = now
		pts := t.blocks[t.index].close()
		go t.store(t.blocks[t.index].id, pts)

		t.index = getIndex(date)

		blkid := BlockID(date)
		if t.blocks[t.index] == nil {
			log.Debug(
				"new block",
				zap.Int64("blkid", blkid),
			)
			t.blocks[t.index] = &block{id: blkid}
			t.blocks[t.index].add(date, value)
			return nil
		}

		if t.blocks[t.index].id != blkid {
			log.Debug(
				"resetting block",
				zap.Int64("blkid", blkid),
			)
			t.blocks[t.index].reset(blkid)
			t.blocks[t.index].add(date, value)
			return nil
		}

		log.Debug(
			"updating block",
			zap.Int64("blkid", blkid),
		)
		return t.update(date, value)

	}

	if delta < 0 {
		t.lastWrite = now
		return t.update(date, value)
	}

	t.lastWrite = now
	t.blocks[t.index].add(date, value)

	//log.Debug("point written successfully")
	return nil
}

func (t *serie) toDepot() bool {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	log := gblog.With(
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.String("package", "gorilla"),
		zap.String("func", "serie/toDepot"),
	)

	now := time.Now().Unix()
	delta := now - t.lastWrite

	log.Debug(
		"analyzing serie",
		zap.Int64("lastWrite", t.lastWrite),
		zap.Int64("lastAccess", t.lastAccess),
		zap.Int64("delta", delta),
	)

	if delta >= hour {
		log.Info(
			"sending serie to depot",
			zap.Int64("lastWrite", t.lastWrite),
			zap.Int64("lastAccess", t.lastAccess),
			zap.Int64("delta", delta),
		)
		pts := t.blocks[t.index].close()
		go t.store(t.blocks[t.index].id, pts)
	}

	if now-t.lastAccess >= 600 {
		log.Info(
			"clenaup serie",
		)
		for i := 0; i < maxBlocks; i++ {
			if t.index == i {
				continue
			}
			t.blocks[i] = nil
		}
	}

	if now-t.lastAccess >= hour && now-t.lastWrite >= hour {
		log.Info(
			"serie must leave memory",
			zap.Int64("lastWrite", t.lastWrite),
			zap.Int64("lastAccess", t.lastAccess),
		)
		return true
	}

	return false
}

func (t *serie) stop() gobol.Error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	pts := t.blocks[t.index].close()
	return t.store(t.blocks[t.index].id, pts)

}

func (t *serie) update(date int64, value float32) gobol.Error {

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

	if t.blocks[index] != nil && t.blocks[index].id == blkID {
		log.Debug(
			"updating block in memory",
			zap.Int64("delta", delta),
			zap.Int("count", count),
			zap.Int("blockSize", len(ptsByte)),
		)

		t.blocks[index].SetPoints(ptsByte)
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
	now := time.Now().Unix()
	t.lastAccess = now

	// Oldest Index
	oi := t.index + 1
	if oi >= maxBlocks {
		oi = 0
	}

	var ot int64
	if t.blocks[oi] != nil {
		ot = t.blocks[oi].id
	} else {
		ot = t.blocks[t.index].id - (22 * hour)
		// calc old time
	}

	log := gblog.With(
		zap.String("package", "storage/serie"),
		zap.String("func", "read"),
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.Int64("start", start),
		zap.Int64("end", end),
		zap.Int("index", t.index),
		zap.Int("oldestIndex", oi),
		zap.Int64("oldestTime", ot),
	)

	memStart := start
	var oldPts []*pb.Point
	if start < ot {
		memStart = ot
		pEnd := ot
		if end < ot || end > now {
			pEnd = end
		}
		p, err := t.readPersistence(start, pEnd)
		if err != nil {
			return nil, err
		}
		if len(p) > 0 {
			oldPts = p
		}
		log.Debug(
			"points read from depot",
			zap.Int("persistenceCount", len(oldPts)),
		)
	}

	totalCount := len(oldPts)

	blksID := []int64{}

	x := memStart
	blkidEnd := BlockID(end)
	for {
		blkidStart := BlockID(x)
		blksID = append(blksID, blkidStart)

		x += 2 * hour
		if blkidStart >= blkidEnd {
			break
		}
	}

	var memPts []*pb.Point
	if end > ot {
		ptsCh := make(chan query)
		defer close(ptsCh)

		for x, b := range blksID {
			i := getIndex(b)
			if t.blocks[i] != nil {
				go t.blocks[i].rangePoints(x, start, end, ptsCh)
			} else {
				pByte, gerr := t.persist.Read(t.ksid, t.tsid, b)
				if gerr != nil {
					log.Error(
						gerr.Error(),
						zap.Error(gerr),
					)
					return nil, gerr
				}
				t.blocks[i] = &block{id: b, points: pByte}
				go t.blocks[i].rangePoints(x, start, end, ptsCh)
			}

			/*
				log.Debug(
					"reading N blocks...",
					zap.Int("loop_count", x),
					zap.Int("loop_index", i),
					zap.Int("blks_to_read", len(blksID)),
				)
			*/

		}

		result := make([][]*pb.Point, len(blksID))
		var resultCount int
		for _ = range blksID {
			q := <-ptsCh
			result[q.id] = q.pts
			size := len(result[q.id])
			if size > 0 {
				resultCount += size
			}
		}

		points := make([]*pb.Point, resultCount)
		totalCount += resultCount

		var size int
		// index must be from oldest point to the newest
		//index := oi
		var i int
		for _ = range blksID {
			//idx := getIndex(b)

			if len(result[i]) > 0 {
				copy(points[size:], result[i])
				size += len(result[i])
			}
			i++
			//index++
			//if index >= maxBlocks {
			//	index = 0
			//}
		}

		memPts = points
	}

	pts := make([]*pb.Point, totalCount)
	copy(pts, oldPts)
	copy(pts[len(oldPts):], memPts)

	/*
		gblog.Debug(
			"points read",
			zap.Int("memoryCount", len(memPts)),
			zap.Int("persistenceCount", len(oldPts)),
			zap.Int("totalCount", len(pts)),
		)
	*/

	return pts, nil
}

func (t *serie) readPersistence(start, end int64) ([]*pb.Point, gobol.Error) {

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
		zap.Int("blocksCount", len(oldBlocksID)),
	)

	log.Debug("reading...")

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
		zap.String("package", "storage/serie"),
		zap.String("func", "encode"),
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.Int64("blkid", id),
	)

	enc := tsz.NewEncoder(id)
	var count int

	for _, pt := range points {
		if pt != nil {
			enc.Encode(pt.Date, pt.Value)
			count++
		}
	}

	pts, err := enc.Close()
	if err != nil {
		log.Error(
			err.Error(),
			zap.Error(err),
			zap.Int("blockSize", len(pts)),
			zap.Int("count", count),
		)
		return nil, errTsz("serie/encode", t.ksid, t.tsid, 0, err)
	}

	log.Debug(
		"finished tsz encoding",
		zap.Int("blockSize", len(pts)),
		zap.Int("count", count),
	)

	return pts, nil
}

func (t *serie) decode(points []byte, id int64) ([bucketSize]*pb.Point, int, gobol.Error) {
	dec := tsz.NewDecoder(points)

	var pts [bucketSize]*pb.Point
	var d int64
	var v float32

	var count int

	log := gblog.With(
		zap.String("package", "storage"),
		zap.String("func", "serie/decode"),
		zap.String("ksid", t.ksid),
		zap.String("tsid", t.tsid),
		zap.Int64("blkid", id),
		zap.Int("blockSize", len(points)),
	)

	for dec.Scan(&d, &v) {
		delta := d - id
		if delta >= 0 && delta < bucketSize {
			pts[delta] = &pb.Point{Date: d, Value: v}
			count++
		}
	}

	if err := dec.Close(); err != nil {
		log.Error(
			err.Error(),
			zap.Error(err),
			zap.Int("count", count),
		)
		return [bucketSize]*pb.Point{}, 0, errTsz("serie/decode", t.ksid, t.tsid, 0, err)
	}

	log.Debug(
		"finished tsz decoding",
		zap.Int("count", count),
	)

	return pts, count, nil

}

func (t *serie) store(bktid int64, pts []byte) gobol.Error {

	if len(pts) >= headerSize {
		err := t.persist.Write(t.ksid, t.tsid, bktid, pts)
		if err != nil {
			gblog.Error(
				"",
				zap.String("package", "gorilla"),
				zap.String("func", "serie/store"),
				zap.String("ksid", t.ksid),
				zap.String("tsid", t.tsid),
				zap.Int64("blkid", bktid),
				zap.Error(err),
			)
			return err
		}
	}

	return nil
}
