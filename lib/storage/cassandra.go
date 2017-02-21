package storage

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/plot"
)

func (s *Storage) getTStamp(keyspace, key string, start, end int64, ms bool) ([]plot.Pnt, int, gobol.Error) {
	track := time.Now()
	start--
	end++

	var date int64
	var value float64
	var err error

	for _, cons := range s.consistencies {
		iter := s.cassandra.Query(
			fmt.Sprintf(
				`SELECT date, value FROM %v.ts_number_stamp WHERE id= ? AND date > ? AND date < ? ALLOW FILTERING`,
				keyspace,
			),
			key,
			start,
			end,
		).Consistency(cons).RoutingKey([]byte(key)).Iter()

		points := []plot.Pnt{}
		var count int

		for iter.Scan(&date, &value) {
			count++
			if !ms {
				date = (date / 1000) * 1000
			}
			point := plot.Pnt{
				Date:  date,
				Value: value,
			}
			points = append(points, point)
		}

		if err = iter.Close(); err != nil {

			gblog.WithFields(logrus.Fields{
				"package": "storage/cassandra",
				"func":    "getTStamp",
			}).Error(err)

			if err == gocql.ErrNotFound {
				statsSelect(keyspace, "ts_number_stamp", time.Since(track))
				return []plot.Pnt{}, 0, errNoContent("getTStamp")
			}

			statsSelectQerror(keyspace, "ts_number_stamp")
			continue
		}
		statsSelect(keyspace, "ts_number_stamp", time.Since(track))
		return points, count, nil
	}
	statsSelectFerror(keyspace, "ts_number_stamp")
	return []plot.Pnt{}, 0, errPersist("getTStamp", err)
}
