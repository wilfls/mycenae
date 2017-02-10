package plot

import (
	"time"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"
)

type persistence struct {
	cassandra     *gocql.Session
	esTs          *rubber.Elastic
	consistencies []gocql.Consistency
}

func (persist *persistence) SetConsistencies(consistencies []gocql.Consistency) {
	persist.consistencies = consistencies
}

func (persist *persistence) ListESTags(
	esIndex,
	esType string,
	esQuery interface{},
	response *EsResponseTag,
) gobol.Error {
	start := time.Now()
	_, err := persist.esTs.Query(esIndex, esType, esQuery, response)
	if err != nil {
		statsIndexError(esIndex, esType, "post")
		return errPersist("ListESTags", err)
	}
	statsIndex(esIndex, esType, "post", time.Since(start))
	return nil
}

func (persist *persistence) ListESMetrics(
	esIndex,
	esType string,
	esQuery interface{},
	response *EsResponseMetric,
) gobol.Error {
	start := time.Now()
	_, err := persist.esTs.Query(esIndex, esType, esQuery, response)
	if err != nil {
		statsIndexError(esIndex, esType, "post")
		return errPersist("ListESMetrics", err)
	}
	statsIndex(esIndex, esType, "post", time.Since(start))
	return nil
}

func (persist *persistence) ListESTagKey(
	esIndex,
	esType string,
	esQuery interface{},
	response *EsResponseTagKey,
) gobol.Error {
	start := time.Now()
	_, err := persist.esTs.Query(esIndex, esType, esQuery, response)
	if err != nil {
		statsIndexError(esIndex, esType, "post")
		return errPersist("ListESTagKey", err)
	}
	statsIndex(esIndex, esType, "post", time.Since(start))
	return nil
}

func (persist *persistence) ListESTagValue(
	esIndex,
	esType string,
	esQuery interface{},
	response *EsResponseTagValue,
) gobol.Error {
	start := time.Now()
	_, err := persist.esTs.Query(esIndex, esType, esQuery, response)
	if err != nil {
		statsIndexError(esIndex, esType, "post")
		return errPersist("ListESTagValue", err)
	}
	statsIndex(esIndex, esType, "post", time.Since(start))
	return nil
}

func (persist *persistence) ListESMeta(
	esIndex,
	esType string,
	esQuery interface{},
	response *EsResponseMeta,
) gobol.Error {
	start := time.Now()

	_, err := persist.esTs.Query(esIndex, esType, esQuery, response)
	if err != nil {
		statsIndexError(esIndex, esType, "post")
		return errPersist("ListESMeta", err)
	}
	statsIndex(esIndex, esType, "post", time.Since(start))
	return nil
}
