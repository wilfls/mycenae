package storage

import (
	tsz "github.com/uol/go-tsz"
	"github.com/uol/mycenae/lib/plot"
)

// block contains compressed points
type block struct {
	points     []byte
	count      int
	start, end int64
}

func (b *block) rangePoints(start, end int64, ptsCh chan []plot.Pnt) {
	pts := make([]plot.Pnt, b.count)
	index := 0
	if b.start >= start || b.end <= end {

		dec := tsz.NewDecoder(b.points, b.start)

		var d int64
		var v float32
		for dec.Scan(&d, &v) {
			if d >= start && d <= end {
				pts[index] = plot.Pnt{Date: d, Value: float64(v)}
				index++
			}
		}

		if err := dec.Close(); err.Error() != "EOF" {
			panic(err)
		}
	}

	ptsCh <- pts[:index]

}
