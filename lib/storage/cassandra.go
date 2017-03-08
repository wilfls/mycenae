package storage

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/plot"
)

type persistence struct {
	cassandra     *gocql.Session
	consistencies []gocql.Consistency
}

func (persist *persistence) SetConsistencies(consistencies []gocql.Consistency) {
	persist.consistencies = consistencies
}

func (persist *persistence) InsertBucket(ksid, tsid string, timestamp int64, value []byte) gobol.Error {
	start := time.Now()
	var err error
	for _, cons := range persist.consistencies {
		if err = persist.cassandra.Query(
			fmt.Sprintf(`INSERT INTO %v.ts_number_stamp (id, date , value) VALUES (?, ?, ?)`, ksid),
			tsid,
			timestamp,
			value,
		).Consistency(cons).RoutingKey([]byte(tsid)).Exec(); err != nil {
			statsInsertFBerror(ksid, "ts_number_stamp")
			gblog.WithFields(
				logrus.Fields{
					"package": "collector/persistence",
					"func":    "insertPoint",
				},
			).Error(err)
			continue
		}
		statsInsert(ksid, "ts_number_stamp", time.Since(start))
		return nil
	}
	statsInsertQerror(ksid, "ts_number_stamp")
	return errPersist("InsertPoint", err)
}

func (persist *persistence) ReadBucket(ksid, tsid string, start, end int64, ms bool) ([]plot.Pnt, int, gobol.Error) {
	track := time.Now()
	start = start - 3600000
	end++

	var date int64
	var value float64
	var err error

	for _, cons := range persist.consistencies {
		iter := persist.cassandra.Query(
			fmt.Sprintf(
				`SELECT date, value FROM %v.ts_number_stamp WHERE id= ? AND date > ? AND date < ? ALLOW FILTERING`,
				ksid,
			),
			tsid,
			start,
			end,
		).Consistency(cons).RoutingKey([]byte(tsid)).Iter()

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
				statsSelect(ksid, "ts_number_stamp", time.Since(track))
				return []plot.Pnt{}, 0, errNoContent("getTStamp")
			}

			statsSelectQerror(ksid, "ts_number_stamp")
			continue
		}
		statsSelect(ksid, "ts_number_stamp", time.Since(track))
		return points, count, nil
	}
	statsSelectFerror(ksid, "ts_number_stamp")
	return []plot.Pnt{}, 0, errPersist("getTStamp", err)
}
