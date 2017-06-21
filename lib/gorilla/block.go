package gorilla

import (
	"io"
	"sync"

	"go.uber.org/zap"

	tsz "github.com/uol/go-tsz"
	pb "github.com/uol/mycenae/lib/proto"
)

// block contains compressed points
type block struct {
	mtx            sync.Mutex
	points         []byte
	count          int
	id, start, end int64
}

func (b *block) SetPoints(pts []byte) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if len(pts) >= headerSize {
		b.points = pts
	}
}

func (b *block) GetPoints() []byte {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	return b.points
}

func (b *block) SetCount(c int) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	b.count = c
}

func (b *block) rangePoints(id int, start, end int64, queryCh chan query) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if len(b.points) >= headerSize {
		if end >= b.id || start >= b.id {
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

			queryCh <- query{
				id:  id,
				pts: pts[:index],
			}
		}
	} else {
		queryCh <- query{
			id:  id,
			pts: []*pb.Point{},
		}
	}
}
