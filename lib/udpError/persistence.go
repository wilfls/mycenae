package udpError

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"
)

type persistence struct {
	cassandra     *gocql.Session
	esearch       *rubber.Elastic
}

func (persist *persistence) GetErrorInfo(key string) ([]ErrorInfo, gobol.Error) {
	start := time.Now()
	var id, errorMsg, payload string
	date := time.Time{}
	errorsInfo := []ErrorInfo{}
	iter := persist.cassandra.Query(
		`SELECT tsid, error, message, date FROM ts_error WHERE tsid = ? ALLOW FILTERING`,
		key,
	).RoutingKey([]byte(key)).Iter()

	for iter.Scan(&id, &errorMsg, &payload, &date) {
		ei := ErrorInfo{
			ID:      id,
			Error:   errorMsg,
			Message: payload,
			Date:    date,
		}
		errorsInfo = append(errorsInfo, ei)
	}

	if err := iter.Close(); err != nil {

		gblog.WithFields(logrus.Fields{
			"package": "udpError/persistence",
			"func":    "GetErrorInfo",
		}).Error(err)

		if err == gocql.ErrNotFound {
			statsSelct("default", "ts_error", time.Since(start))
			return []ErrorInfo{}, errNotFound("GetErrorInfo")
		}
		statsSelectQerror("default", "ts_error")
		return []ErrorInfo{}, errPersist("GetErrorInfo", err)
	}
	statsSelct("default", "ts_error", time.Since(start))
	return errorsInfo, nil
}

func (persist *persistence) ListESErrorTags(
	esIndex,
	esType string,
	esQuery interface{},
	response *EsResponseTag,
) gobol.Error {
	start := time.Now()
	_, err := persist.esearch.Query(esIndex, esType, esQuery, response)
	if err != nil {
		statsIndexError(esIndex, esType, "POST")
		return errPersist("ListESErrorTags", err)
	}
	statsIndex(esIndex, esType, "POST", time.Since(start))
	return nil
}
