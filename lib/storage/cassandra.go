package storage

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	tsz "github.com/uol/go-tsz"
	"github.com/uol/gobol"
)

type Cassandra struct {
	session            *gocql.Session
	writeConsistencies []gocql.Consistency
	readConsistencies  []gocql.Consistency
	tc                 TC
}

type cassPoints struct {
	date int64
	blob []byte
}

func (cass *Cassandra) SetWriteConsistencies(consistencies []gocql.Consistency) {
	cass.writeConsistencies = consistencies
}

func (cass *Cassandra) SetReadConsistencies(consistencies []gocql.Consistency) {
	cass.readConsistencies = consistencies
}

func (cass *Cassandra) ReadBlock(ksid, tsid string, blkid int64) ([]byte, gobol.Error) {
	track := time.Now()

	year, week := time.Unix(blkid, 0).ISOWeek()
	bktid := fmt.Sprintf("%v%v%v", year, week, tsid)

	var err error
	var value []byte

	date := blkid * 1000

	for _, cons := range cass.readConsistencies {

		err = cass.session.Query(
			fmt.Sprintf(
				`SELECT value FROM %v.timeserie WHERE id= ? AND date = ?`,
				ksid,
			),
			bktid,
			date,
		).Consistency(cons).RoutingKey([]byte(bktid)).Scan(&value)

		if err != nil {

			gblog.WithFields(logrus.Fields{
				"package": "storage/cassandra",
				"func":    "ReadBlock",
			}).Error(err)

			if err == gocql.ErrNotFound {
				statsSelect(ksid, "timeserie", time.Since(track))
				return value, nil
			}

			statsSelectQerror(ksid, "timeserie")
			continue
		}

		statsSelect(ksid, "timeserie", time.Since(track))
		return value, nil
	}
	statsSelectFerror(ksid, "timeserie")

	return value, errPersist("ReadBlock", err)

}

func (cass *Cassandra) InsertBlock(ksid, tsid string, blkid int64, value []byte) gobol.Error {
	start := time.Now()

	year, week := time.Unix(blkid, 0).ISOWeek()
	bktid := fmt.Sprintf("%v%v%v", year, week, tsid)

	var err error
	for _, cons := range cass.writeConsistencies {
		if err = cass.session.Query(
			fmt.Sprintf(`INSERT INTO %v.timeserie (id, date , value) VALUES (?, ?, ?)`, ksid),
			bktid,
			blkid*1000,
			value,
		).Consistency(cons).RoutingKey([]byte(tsid)).Exec(); err != nil {
			statsInsertFBerror(ksid, "timeserie")
			gblog.WithFields(
				logrus.Fields{
					"package": "storage/cassandra",
					"func":    "insertBlock",
				},
			).Error(err)
			continue
		}
		statsInsert(ksid, "timeserie", time.Since(start))
		return nil
	}
	statsInsertQerror(ksid, "timeserie")
	return errPersist("InsertBlock", err)
}

func (cass *Cassandra) ReadBucket(ksid, tsid string, start, end int64) (Pnts, int, gobol.Error) {
	start = start - 7200
	end++

	buckets := []string{}

	w := start

	for {

		year, week := time.Unix(w, 0).ISOWeek()

		buckets = append(buckets, fmt.Sprintf("%v%v%v", year, week, tsid))

		if w > end {
			break
		}
		w += 604800
	}

	//year, week := time.Unix(start, 0).ISOWeek()
	//tsid = fmt.Sprintf("%v%v%v", year, week, tsid)

	points := []cassPoints{}

	for _, bktID := range buckets {
		pts, _, err := cass.read(ksid, bktID, start, end)
		if err != nil {
			continue
		}
		points = append(points, pts...)
	}

	p := Pnts{}
	for _, ptsBlk := range points {
		dec := tsz.NewDecoder(ptsBlk.blob)
		var date int64
		var value float32
		for dec.Scan(&date, &value) {
			if date >= start && date <= end {
				p = append(p, Pnt{Date: date, Value: value})
			}
		}
		dec.Close()
	}

	statsSelectFerror(ksid, "ts_number_stamp")
	return p, 0, errPersist("ReadBuckets", nil)

}

func (cass *Cassandra) read(ksid, bktID string, start, end int64) ([]cassPoints, int, gobol.Error) {
	track := time.Now()

	var date int64
	var value []byte
	var err error

	for _, cons := range cass.readConsistencies {
		iter := cass.session.Query(
			fmt.Sprintf(
				`SELECT date, value FROM %v.timeserie WHERE id= ? AND date > ? AND date < ? ALLOW FILTERING`,
				ksid,
			),
			bktID,
			start,
			end,
		).Consistency(cons).RoutingKey([]byte(bktID)).Iter()

		points := []cassPoints{}
		var count int

		for iter.Scan(&date, &value) {
			count++
			point := cassPoints{
				date: date,
				blob: value,
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
				return []cassPoints{}, 0, errNoContent("getTStamp")
			}

			statsSelectQerror(ksid, "ts_number_stamp")
			continue
		}
		statsSelect(ksid, "ts_number_stamp", time.Since(track))
		return points, count, nil
	}
	statsSelectFerror(ksid, "ts_number_stamp")
	return []cassPoints{}, 0, errPersist("ReadBuckets", nil)
}

func (cass *Cassandra) InsertText(ksid, tsid string, timestamp int64, text string) gobol.Error {
	start := time.Now()
	var err error
	for _, cons := range cass.writeConsistencies {
		if err = cass.session.Query(
			fmt.Sprintf(`INSERT INTO %v.ts_text_stamp (id, date , value) VALUES (?, ?, ?)`, ksid),
			tsid,
			timestamp,
			text,
		).Consistency(cons).RoutingKey([]byte(tsid)).Exec(); err != nil {
			statsInsertQerror(ksid, "ts_text_stamp")
			gblog.WithFields(
				logrus.Fields{
					"package": "collector/persistence",
					"func":    "InsertText",
				},
			).Error(err)
			continue
		}
		statsInsert(ksid, "ts_text_stamp", time.Since(start))
		return nil
	}
	statsInsertFBerror(ksid, "ts_text_stamp")
	return errPersist("InsertText", err)
}

func (cass *Cassandra) InsertError(id, msg, errMsg string, date time.Time) gobol.Error {
	start := time.Now()
	var err error
	for _, cons := range cass.writeConsistencies {
		if err = cass.session.Query(
			`INSERT INTO ts_error (code, tsid, error, message, date) VALUES (?, ?, ?, ?, ?)`,
			0,
			id,
			errMsg,
			msg,
			date,
		).Consistency(cons).RoutingKey([]byte(id)).Exec(); err != nil {
			statsInsertQerror("default", "ts_error")
			gblog.WithFields(
				logrus.Fields{
					"package": "collector/persistence",
					"func":    "InsertError",
				},
			).Error(err)
			continue
		}
		statsInsert("default", "ts_error", time.Since(start))
		return nil
	}
	statsInsertFBerror("default", "ts_error")
	return errPersist("InsertError", err)
}
