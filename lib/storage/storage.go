package storage

const (
	bucketSize = 64
)

// Storage keeps all timeseries in memory
// each serie has its own first and last time
// after a while the serie will be saved at cassandra
// if the time range is not in memory it must query cassandra
type Storage struct {
	addCh  chan idPoint
	readCh chan idPoint
	stop   chan struct{}
	series map[string]timeseries
}

type timeseries struct {
	buckets []bucket
}

type bucket struct {
	firstPoint    int64
	firstPointSet bool
	lastPoint     int64
	position      int
	points        [bucketSize]Point
}

type Point struct {
	Timestamp int64
	Value     float64
}

type idPoint struct {
	id     string
	bktsCh chan []bucket
	Point
}

// New returns Storage
func New() *Storage {
	s := make(map[string]timeseries)
	strg := &Storage{
		addCh:  make(chan idPoint),
		readCh: make(chan idPoint),
		stop:   make(chan struct{}),
		series: s,
	}
	strg.start()
	return strg
}

func (s *Storage) Add(id string, t int64, v float64) error {
	s.addCh <- idPoint{
		id:    id,
		Point: Point{t, v},
	}
	return nil
}

func (s *Storage) add(p idPoint) {
	if _, exist := s.series[p.id]; !exist {
		ts := timeseries{}
		ts.buckets = append(ts.buckets, bucket{
			firstPoint:    p.Point.Timestamp,
			lastPoint:     p.Point.Timestamp,
			firstPointSet: true,
		})
		s.series[p.id] = ts
	}
	ts := s.series[p.id]

	b := &ts.buckets[len(ts.buckets)-1]

	if b.position >= bucketSize {
		ts.buckets = append(ts.buckets, bucket{
			firstPoint:    p.Point.Timestamp,
			lastPoint:     p.Point.Timestamp,
			firstPointSet: true,
		})
		b = &ts.buckets[len(ts.buckets)-1]
		s.series[p.id] = ts
	}
	//fmt.Printf("Position: %v\tbkts: %v\n", b.position, len(ts.buckets))
	b.points[b.position] = p.Point
	b.lastPoint = p.Point.Timestamp
	b.position++

}

func (s *Storage) Read(id string, start, end int64) <-chan Point {

	pts := make(chan Point)
	idPt := idPoint{
		id:     id,
		bktsCh: make(chan []bucket),
	}

	go func() {
		defer close(pts)
		s.readCh <- idPt
		bkt := <-idPt.bktsCh

		for _, b := range bkt {
			if b.firstPoint >= start || b.lastPoint <= end {
				for _, pt := range b.points {
					if pt.Timestamp >= start && pt.Timestamp <= end {
						pts <- pt
					}
				}
			}
		}

	}()

	return pts
}

func (s *Storage) start() {

	go func() {
		for {
			select {
			case p := <-s.addCh:
				//fmt.Printf("TS: %v\tV: %v\n", p.Point.Timestamp, p.Point.Value)
				s.add(p)

			case r := <-s.readCh:
				bkts := make([]bucket, len(s.series[r.id].buckets))
				copy(bkts, s.series[r.id].buckets)
				r.bktsCh <- bkts

			case <-s.stop:
				// TODO: cleanup the addCh before return
				return

			}
		}
	}()

}
