package gorilla

import (
	"fmt"
	"io"
	"sync"

	"go.uber.org/zap"

	tsz "github.com/uol/go-tsz"
	pb "github.com/uol/mycenae/lib/proto"

	"github.com/uol/gobol"
)

// block contains compressed points
type block struct {
	mtx            sync.Mutex
	points         []byte
	count          int
	id, start, end int64
}

func (b *block) update(date int64, value float32) gobol.Error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	f := "block/update"

	pts := [bucketSize]*Pnt{}

	if len(b.points) >= headerSize {

		gblog.Debug(
			"updating block",
			zap.Int64("blkid", b.id),
			zap.String("package", "gorilla"),
			zap.String("func", "block/update"),
			zap.Int("count", b.count),
			zap.Int("size", len(b.points)),
		)

		dec := tsz.NewDecoder(b.points)
		defer dec.Close()
		var d int64
		var v float32
		var count int
		for dec.Scan(&d, &v) {

			delta := d - b.id
			if delta > bucketSize || delta < 0 {

				/*
					if time.Unix(d, 0).After(time.Now()) {
						b.reset(date, value)
					}
				*/

				gblog.Debug(
					"delta out of range or byte array corrupted",
					zap.Int64("blkid", b.id),
					zap.String("package", "gorilla"),
					zap.String("func", "block/update"),
					zap.Int64("date", d),
					zap.Int64("delta", delta),
					zap.Int("count", b.count),
				)
				/*
					return errMemoryUpdatef(
						f,
						"delta out of range",
						map[string]interface{}{
							"blockid": b.id,
							"delta":   delta,
						},
					)
				*/
				continue
			}
			pts[delta] = &Pnt{Date: d, Value: v}
			count++
		}

		delta := date - b.id
		pts[delta] = &Pnt{Date: date, Value: value}
		count++

		var t0 int64
		for _, p := range pts {
			if p != nil {
				t0 = p.Date
				break
			}
		}

		gblog.Debug(
			"point added to block",
			zap.Int64("blkid", b.id),
			zap.String("package", "gorilla"),
			zap.String("func", "block/update"),
			zap.Int64("date", date),
			zap.Int64("delta", delta),
			zap.Int64("t0", t0),
			zap.Int("count", count),
		)

		s := b.start
		e := b.end
		enc := tsz.NewEncoder(t0)
		for _, p := range pts {
			if p != nil {
				enc.Encode(p.Date, p.Value)

				if p.Date < s {
					s = p.Date
				}
				if p.Date > e {
					e = p.Date
				}
			}
		}

		np, err := enc.Close()
		if err != nil {
			return errMemoryUpdate(
				f,
				fmt.Sprintf("blockid=%v - %v", b.id, err),
			)
		}

		b.points = np
		b.start = s
		b.end = e
		b.count = count

		gblog.Debug(
			"block updated",
			zap.Int64("blkid", b.id),
			zap.String("package", "gorilla"),
			zap.String("func", "block/update"),
			zap.Int64("date", date),
			zap.Int64("delta", delta),
			zap.Int64("t0", t0),
			zap.Int("count", count),
			zap.Int("size", len(np)),
		)

		return nil
	}

	return b.reset(date, value)

}

func (b *block) getPoints() []byte {
	return b.points
}

func (b *block) reset(date int64, value float32) gobol.Error {
	enc := tsz.NewEncoder(date)
	enc.Encode(date, value)
	np, err := enc.Close()
	if err != nil {
		return errMemoryUpdate(
			"block/reset",
			fmt.Sprintf("blockid=%v - %v", b.id, err),
		)
	}
	b.points = np
	b.start = BlockID(date)
	b.end = BlockID(date) + bucketSize - 1
	b.count = 1

	gblog.Debug(
		"block in memory reseted",
		zap.Int64("blkid", b.id),
		zap.String("package", "gorilla"),
		zap.String("func", "block/reset"),
		zap.Int("count", b.count),
		zap.Int("size", len(b.points)),
	)

	return nil
}

func (b *block) rangePoints(id int, start, end int64, queryCh chan query) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if len(b.points) >= headerSize {
		pts := make([]*pb.Point, b.count)
		index := 0

		dec := tsz.NewDecoder(b.points)

		var c int
		var d int64
		var v float32
		for dec.Scan(&d, &v) {
			if d >= start && d <= end {

				pts[index] = &pb.Point{
					Date:  d,
					Value: v,
				}
				index++
			}
			c++
		}
		b.count = c

		err := dec.Close()
		if err != io.EOF && err != nil {
			gblog.Error("", zap.Error(err))
		}

		gblog.Sugar().Infof("read %v points in block %v", c, id)

		queryCh <- query{
			id:  id,
			pts: pts[:index],
		}

	} else {
		queryCh <- query{
			id:  id,
			pts: []*pb.Point{},
		}
	}
}
