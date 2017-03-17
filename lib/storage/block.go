package storage

import (
	"fmt"
	"io"
	"sync"

	tsz "github.com/uol/go-tsz"
	"github.com/uol/mycenae/lib/plot"
)

// block contains compressed points
type block struct {
	mtx        sync.Mutex
	points     []byte
	count      int
	start, end int64
}

func (b *block) rangePoints(start, end int64, ptsCh chan []plot.Pnt) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	pts := make([]plot.Pnt, b.count)
	index := 0

	if b.points != nil {
		if b.start >= start || b.end <= end {

			ptsBytes := b.points
			//fmt.Println("block size", len(ptsBytes), "start:", b.start)

			dec := tsz.NewDecoder(ptsBytes, b.start)

			var d int64
			var v float32
			for dec.Scan(&d, &v) {
				if d >= start && d <= end {
					pts[index] = plot.Pnt{Date: d, Value: float64(v)}
					index++
					//ptsCh <- plot.Pnt{Date: d, Value: float64(v)}
				}
			}

			err := dec.Close()
			if err != io.EOF && err != nil {
				fmt.Println(err)
			}

		}
	}

	//fmt.Println("block", index)
	ptsCh <- pts[:index]

}
