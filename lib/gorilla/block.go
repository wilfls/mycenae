package gorilla

import (
	"fmt"
	"io"
	"sync"

	tsz "github.com/uol/go-tsz"
)

// block contains compressed points
type block struct {
	mtx            sync.Mutex
	points         []byte
	count          int
	id, start, end int64
}

func (b *block) update(date int64, value float32) error {

	var pts [bucketSize]*Pnt

	dec := tsz.NewDecoder(b.points)
	var d int64
	var v float32

	for dec.Scan(&d, &v) {
		delta := b.id - d
		pts[delta] = &Pnt{Date: d, Value: v}
	}
	err := dec.Close()
	if err != nil {
		return fmt.Errorf("aborting block update, error decoding block %v: %v", b.id, err)
	}

	delta := b.id - date
	pts[delta] = &Pnt{Date: date, Value: value}

	var t0 int64
	for _, p := range pts {
		if p != nil {
			t0 = p.Date
			break
		}
	}

	var c int
	s := b.start
	e := b.end
	enc := tsz.NewEncoder(t0)
	for _, p := range pts {
		if p != nil {
			enc.Encode(p.Date, p.Value)
			c++
			if p.Date > s {
				s = p.Date
			}
			if p.Date < e {
				e = p.Date
			}
		}
	}

	np, err := enc.Close()
	if err != nil {
		return fmt.Errorf("aborting block update, error encoding block %v: %v", b.id, err)
	}

	b.points = np
	b.start = s
	b.end = e
	b.count = c

	return nil

}

func (b *block) rangePoints(id int, start, end int64, queryCh chan query) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if len(b.points) > 0 && (b.start >= start || b.end <= end) {
		pts := make([]Pnt, b.count)
		index := 0

		dec := tsz.NewDecoder(b.points)

		var c int
		var d int64
		var v float32
		for dec.Scan(&d, &v) {
			if d >= start && d <= end {

				pts[index] = Pnt{
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
			gblog.Error(err)
		}

		queryCh <- query{
			id:  id,
			pts: pts[:index],
		}
	} else {
		queryCh <- query{
			id:  id,
			pts: Pnts{},
		}
	}
}