package gorilla

import (
	"io"

	"go.uber.org/zap"

	tsz "github.com/uol/go-tsz"
	pb "github.com/uol/mycenae/lib/proto"
)

// block contains compressed points
type block struct {
	points []byte
	id     int64
}

func (b *block) SetPoints(pts []byte) {
	if len(pts) >= headerSize {
		b.points = pts
	}
}

func (b *block) GetPoints() []byte {
	return b.points
}

func (b *block) SetID(id int64) {
	b.id = id
}

func (b *block) ID() int64 {
	return b.id
}

func (b *block) rangePoints(id int, start, end int64, queryCh chan query) {
	if len(b.points) >= headerSize {
		if start >= b.id || end >= b.id {
			pts := make([]*pb.Point, bucketSize)

			dec := tsz.NewDecoder(b.points)

			var d int64
			var v float32
			var i int
			for dec.Scan(&d, &v) {
				if d >= start && d <= end {
					pts[i] = &pb.Point{
						Date:  d,
						Value: v,
					}
					i++
				}
			}

			err := dec.Close()
			if err != io.EOF && err != nil {
				gblog.Error("", zap.Error(err))
			}

			queryCh <- query{
				id:  id,
				pts: pts[:i],
			}
		}
	} else {
		queryCh <- query{
			id:  id,
			pts: []*pb.Point{},
		}
	}
}
