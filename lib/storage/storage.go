package storage

import (
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/plot"
	"github.com/uol/mycenae/lib/tsstats"
)

var (
	gblog *logrus.Logger
	stats *tsstats.StatsTS
)

// Storage keeps all timeseries in memory
// after a while the serie will be saved at cassandra
// if the time range is not in memory it must query cassandra
type Storage struct {
	cass   persistence
	addCh  chan query
	readCh chan query
	stop   chan struct{}
	series map[string]*serie
	wal    *WAL
	mtx    sync.RWMutex
}

type query struct {
	keyspace string
	key      string
	bktsCh   chan []Bucket
	Point
}

// New returns Storage
func New(
	cass *gocql.Session,
	consist []gocql.Consistency,
	wal *WAL,
) *Storage {

	p := persistence{
		cassandra:     cass,
		consistencies: consist,
	}

	return &Storage{
		addCh:  make(chan query),
		readCh: make(chan query),
		stop:   make(chan struct{}),
		series: make(map[string]*serie),
		cass:   p,
		wal:    wal,
	}
}

// Start dispatch a goroutine to save buckets
// in cassandra. All buckets with more then a hour
// must be compressed and saved in cassandra.
func (s *Storage) Start() {

	go func() {

		for {
			select {

			case <-s.stop:
				// TODO: cleanup the addCh before return
				return

			}
		}
	}()
}

// Add insert new point in a timeserie
func (s *Storage) Add(ksid, tsid string, t int64, v float64) {

	id := s.id(ksid, tsid)

	_, err := s.getSerie(id).addPoint(s.cass, ksid, tsid, t, v)
	if err != nil {
		//LOG ERROR
	}

	if s.wal != nil {
		s.wal.Add(ksid, tsid, t, v)
	}

}

func (s *Storage) Read(keyspace, key string, start, end int64, ms bool) ([]plot.Pnt, int, gobol.Error) {

	id := s.id(keyspace, key)
	pts := s.getSerie(id).read(start, end)

	if ms {
		for i, pt := range pts {
			pt.Date = (pt.Date / 1000) * 1000
			pts[i] = pt
		}
	}

	return pts, len(pts), nil
}

func (s *Storage) getSerie(id string) *serie {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if _, exist := s.series[id]; !exist {
		s.series[id] = &serie{}
	}
	return s.series[id]
}

func (s *Storage) id(keyspace, key string) string {
	id := make([]byte, len(keyspace)+len(key))
	copy(id, keyspace)
	copy(id[len(keyspace):], key)
	return string(id)
}
