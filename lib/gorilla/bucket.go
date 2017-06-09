package gorilla

import pb "github.com/uol/mycenae/lib/proto"

const (
	bucketSize = 7200
)

// Bucket is exported to satisfy gob
type bucket struct {
	points [bucketSize]*pb.Point
	id     int64
	count  int
}

func newBucket(key int64) *bucket {
	return &bucket{id: key}
}

/*
add returns
(true, 0, nil) if everthyng is fine
(false, delta, error) if delta is negative
(false, delta, error) if point is in future, it might happen if the date passed by
user is bigger than two hours (in seconds) and the bucket didn't time out.
*/
func (b *bucket) add(date int64, value float32, delta int64) {

	b.points[delta] = &pb.Point{Date: date, Value: value}
	b.count++

}

func (b *bucket) rangePoints(id int, start, end int64, queryCh chan query) {
	pts := make([]*pb.Point, b.count)

	var index int
	if start >= b.id || end >= b.id {
		for _, p := range b.points {
			if p != nil {
				if p.Date >= start && p.Date <= end {
					pts[index] = p
					index++
				}
			}
		}
	}

	queryCh <- query{
		id:  id,
		pts: pts[:index],
	}
}

func (b *bucket) dumpPoints() []*pb.Point {
	pts := make([]*pb.Point, b.count)
	index := 0
	for _, p := range b.points {
		if p != nil {
			pts[index] = p
			index++
		}
	}
	return pts[:index]
}
