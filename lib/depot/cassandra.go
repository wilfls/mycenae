package depot

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/gobol/cassandra"
	"github.com/uol/mycenae/lib/tsstats"

	"go.uber.org/zap"
)

var (
	gblog *zap.Logger
	stats *tsstats.StatsTS
)

func NewCassandra(
	s cassandra.Settings,
	readConsistency []gocql.Consistency,
	writeConsistency []gocql.Consistency,
	log *zap.Logger,
	sts *tsstats.StatsTS,
) (*Cassandra, error) {

	cass, err := cassandra.New(s)
	if err != nil {
		return nil, err
	}

	gblog = log
	stats = sts

	return &Cassandra{
		Session:            cass,
		readConsistencies:  readConsistency,
		writeConsistencies: writeConsistency,
	}, nil

}

type Cassandra struct {
	Session            *gocql.Session
	writeConsistencies []gocql.Consistency
	readConsistencies  []gocql.Consistency
}

/*
Read(ksid, tsid string, blkid int64) ([]byte, error)
Write(ksid, tsid string, blkid int64, points []byte) error
*/

func (c *Cassandra) Close() {
	c.Session.Close()
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

func (cass *Cassandra) Read(ksid, tsid string, blkid int64) ([]byte, error) {
	track := time.Now()

	year, week := time.Unix(blkid, 0).ISOWeek()
	bktid := fmt.Sprintf("%v%v%v", year, week, tsid)

	var err error
	var value []byte

	date := blkid * 1000

	for _, cons := range cass.readConsistencies {

		err = cass.Session.Query(
			fmt.Sprintf(
				`SELECT value FROM %v.timeserie WHERE id= ? AND date = ?`,
				ksid,
			),
			bktid,
			date,
		).Consistency(cons).RoutingKey([]byte(bktid)).Scan(&value)

		if err != nil {

			gblog.Error("",
				zap.String("package", "depot"),
				zap.String("func", "Read"),
				zap.Error(err),
			)

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

func (cass *Cassandra) Write(ksid, tsid string, blkid int64, points []byte) error {
	start := time.Now()

	year, week := time.Unix(blkid, 0).ISOWeek()
	bktid := fmt.Sprintf("%v%v%v", year, week, tsid)

	var err error
	for _, cons := range cass.writeConsistencies {
		if err = cass.Session.Query(
			fmt.Sprintf(`INSERT INTO %v.timeserie (id, date , value) VALUES (?, ?, ?)`, ksid),
			bktid,
			blkid*1000,
			points,
		).Consistency(cons).RoutingKey([]byte(tsid)).Exec(); err != nil {
			statsInsertFBerror(ksid, "timeserie")
			gblog.Error(
				"",
				zap.String("package", "depot"),
				zap.String("func", "Write"),
				zap.Error(err),
			)
			continue
		}
		statsInsert(ksid, "timeserie", time.Since(start))
		return nil
	}
	statsInsertQerror(ksid, "timeserie")
	return errPersist("InsertBlock", err)
}

func (cass *Cassandra) InsertText(ksid, tsid string, timestamp int64, text string) gobol.Error {
	start := time.Now()
	var err error
	for _, cons := range cass.writeConsistencies {
		if err = cass.Session.Query(
			fmt.Sprintf(`INSERT INTO %v.ts_text_stamp (id, date , value) VALUES (?, ?, ?)`, ksid),
			tsid,
			timestamp,
			text,
		).Consistency(cons).RoutingKey([]byte(tsid)).Exec(); err != nil {
			statsInsertQerror(ksid, "ts_text_stamp")
			gblog.Error("",
				zap.String("package", "depot"),
				zap.String("func", "InsertText"),

				zap.Error(err),
			)
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
		if err = cass.Session.Query(
			`INSERT INTO ts_error (code, tsid, error, message, date) VALUES (?, ?, ?, ?, ?)`,
			0,
			id,
			errMsg,
			msg,
			date,
		).Consistency(cons).RoutingKey([]byte(id)).Exec(); err != nil {
			statsInsertQerror("default", "ts_error")
			gblog.Error("",
				zap.String("package", "depot"),
				zap.String("func", "InsertError"),
				zap.Error(err),
			)
			continue
		}
		statsInsert("default", "ts_error", time.Since(start))
		return nil
	}
	statsInsertFBerror("default", "ts_error")
	return errPersist("InsertError", err)
}
