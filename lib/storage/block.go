package storage

import (
	"fmt"
	"io"
	"sync"

	tsz "github.com/uol/go-tsz"
)

// block contains compressed points
type block struct {
	mtx        sync.Mutex
	points     []byte
	count      int
	start, end int64
}

func (b *block) rangePoints(id int, start, end int64, queryCh chan query) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if b.start >= start || b.end <= end {
		pts := make([]Pnt, b.count)
		index := 0

		dec := tsz.NewDecoder(b.points, b.start)

		var d int64
		var v float32
		for dec.Scan(&d, &v) {
			if d >= start && d <= end {

				pts[index] = Pnt{
					Date:  d,
					Value: float64(v),
				}
				index++
			}
		}

		err := dec.Close()
		if err != io.EOF && err != nil {
			fmt.Println(err)
		}
		queryCh <- query{
			id:  id,
			pts: pts[:index],
		}
	}
}
