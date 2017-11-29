package collector

import (
	"fmt"
	"io"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"
)

type persistence struct {
	cassandra     *gocql.Session
	esearch       *rubber.Elastic
	consistencies []gocql.Consistency
}

func (persist *persistence) SetConsistencies(consistencies []gocql.Consistency) {
	persist.consistencies = consistencies
}

func (persist *persistence) InsertPoint(ksid, tsid string, timestamp int64, value float64) gobol.Error {
	start := time.Now()
	var err error
	for _, cons := range persist.consistencies {
		if err = persist.cassandra.Query(
			fmt.Sprintf(`INSERT INTO %v.ts_number_stamp (id, date , value) VALUES (?, ?, ?)`, ksid),
			tsid,
			timestamp,
			value,
		).Consistency(cons).RoutingKey([]byte(tsid)).Exec(); err != nil {
			statsInsertQerror(ksid, "ts_number_stamp")
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
	statsInsertFBerror(ksid, "ts_number_stamp")
	return errPersist("InsertPoint", err)
}

func (persist *persistence) InsertText(ksid, tsid string, timestamp int64, text string) gobol.Error {
	start := time.Now()
	var err error
	for _, cons := range persist.consistencies {
		if err = persist.cassandra.Query(
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

func (persist *persistence) InsertError(id, msg, errMsg string, date time.Time) gobol.Error {
	start := time.Now()
	var err error
	for _, cons := range persist.consistencies {
		if err = persist.cassandra.Query(
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

func (persist *persistence) HeadMetaFromES(index, eType, id string) (int, gobol.Error) {
	start := time.Now()
	respCode, err := persist.esearch.GetHead(index, eType, id)
	if err != nil {
		statsIndexError(index, eType, "head")
		return 0, errPersist("HeadMetaFromES", err)
	}
	statsIndex(index, eType, "head", time.Since(start))
	return respCode, nil
}

func (persist *persistence) SendErrorToES(index, eType, id string, doc StructV2Error) gobol.Error {
	start := time.Now()
	_, err := persist.esearch.Put(index, eType, id, doc)
	if err != nil {
		statsIndexError(index, eType, "put")
		return errPersist("SendErrorToES", err)
	}
	statsIndex(index, eType, "PUT", time.Since(start))
	return nil
}

func (persist *persistence) SaveBulkES(body io.Reader) gobol.Error {
	start := time.Now()
	_, err := persist.esearch.PostBulk(body)
	if err != nil {
		statsIndexError("", "", "bulk")
		return errPersist("SaveBulkES", err)
	}
	statsIndex("", "", "bulk", time.Since(start))
	return nil
}
