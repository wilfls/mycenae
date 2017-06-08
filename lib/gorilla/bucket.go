package gorilla

import (
	"github.com/uol/gobol"
	pb "github.com/uol/mycenae/lib/proto"
)

const (
	bucketSize = 7200
)

// Bucket is exported to satisfy gob
type bucket struct {
	points [bucketSize]*bucketPoint
	id     int64
	count  int
}

type bucketPoint struct {
	t int64
	v float32
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
func (b *bucket) add(date int64, value float32) (int64, gobol.Error) {
	delta := date - b.id

	if delta < 0 {
		return delta, errAddPoint(
			"points out of order cannot be added to the bucket",
			map[string]interface{}{
				"date":  date,
				"value": value,
			},
		)
	}

	if delta >= bucketSize {
		return delta, errAddPoint(
			"points in the future cannot be added to the bucket",
			map[string]interface{}{
				"date":  date,
				"value": value,
			},
		)
	}

	b.points[delta] = &bucketPoint{date, value}
	b.count++

	return delta, nil
}

func (b *bucket) rangePoints(id int, start, end int64, queryCh chan query) {
	pts := make([]*pb.Point, b.count)

	var index int
	if start >= b.id || end >= b.id {
		for _, p := range b.points {
			if p != nil {
				if p.t >= start && p.t <= end {
					pts[index] = &pb.Point{Date: p.t, Value: p.v}
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
			pts[index] = &pb.Point{Date: p.t, Value: p.v}
			index++
		}
	}
	return pts[:index]
}
