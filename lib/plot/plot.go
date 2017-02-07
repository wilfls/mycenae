package plot

import (
	"github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"

	"github.com/uol/mycenae/lib/bcache"
	"github.com/uol/mycenae/lib/tsstats"
)

var (
	gblog *logrus.Logger
	stats *tsstats.StatsTS
)

func New(
	gbl *logrus.Logger,
	sts *tsstats.StatsTS,
	cass *gocql.Session,
	es *rubber.Elastic,
	bc *bcache.Bcache,
	esIndex string,
	maxTimeseries int,
	maxConcurrentTimeseries int,
	maxConcurrentReads int,
	logQueryTSthreshold int,
	consist []gocql.Consistency,
) (*Plot, gobol.Error) {

	gblog = gbl
	stats = sts

	if maxTimeseries < 1 {
		return nil, errInit("MaxTimeseries needs to be bigger than zero")
	}

	if maxConcurrentReads < 1 {
		return nil, errInit("MaxConcurrentReads needs to be bigger than zero")
	}

	if logQueryTSthreshold < 1 {
		return nil, errInit("LogQueryTSthreshold needs to be bigger than zero")
	}

	if maxConcurrentTimeseries > maxConcurrentReads {
		return nil, errInit("maxConcurrentTimeseries cannot be bigger than maxConcurrentReads")
	}

	return &Plot{
		esIndex:           esIndex,
		MaxTimeseries:     maxTimeseries,
		LogQueryThreshold: logQueryTSthreshold,
		boltc:             bc,
		persist:           persistence{cassandra: cass, esTs: es, consistencies: consist},
		concTimeseries:    make(chan struct{}, maxConcurrentTimeseries),
		concReads:         make(chan struct{}, maxConcurrentReads),
	}, nil
}

func (plot *Plot) SetConsistencies(consistencies []gocql.Consistency) {
	plot.persist.SetConsistencies(consistencies)
}

type Plot struct {
	esIndex           string
	MaxTimeseries     int
	LogQueryThreshold int
	boltc             *bcache.Bcache
	persist           persistence
	concTimeseries    chan struct{}
	concReads         chan struct{}
}
