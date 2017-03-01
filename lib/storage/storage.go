package storage

import (
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/plot"
	"github.com/uol/mycenae/lib/tsstats"
)

const (
	bucketSize = 64
)

var (
	gblog *logrus.Logger
	stats *tsstats.StatsTS
)

// Storage keeps all timeseries in memory
// after a while the serie will be saved at cassandra
// if the time range is not in memory it must query cassandra
type Storage struct {
	cassandra     *gocql.Session
	consistencies []gocql.Consistency
	addCh         chan query
	readCh        chan query
	stop          chan struct{}
	series        map[string]*timeserie
	wal           *wal
	mtx           sync.RWMutex
}

type query struct {
	keyspace string
	key      string
	bktsCh   chan []bucket
	Point
}

// New returns Storage
func New(
	path string,
	cass *gocql.Session,
	consist []gocql.Consistency,
) *Storage {

	strg := &Storage{
		addCh:         make(chan query),
		readCh:        make(chan query),
		stop:          make(chan struct{}),
		series:        make(map[string]*timeserie),
		cassandra:     cass,
		consistencies: consist,
	}

	l, err := strg.neWal(path)
	if err != nil {
		panic(err)
	}
	strg.wal = l

	//strg.start()
	err = strg.wal.load(strg)
	if err != nil {
		panic(err)
	}
	return strg
}

func (s *Storage) start() {

	go func() {
		ticker := time.NewTicker(time.Second * 2)

		for {
			select {
			case <-ticker.C:

				s.mtx.Lock()
				m := s.series
				s.mtx.Unlock()

				for _, ts := range m {
					ts.store()
				}

			case <-s.stop:
				// TODO: cleanup the addCh before return
				return

			}
		}
	}()
}

// Add insert new point in a timeserie
func (s *Storage) Add(keyspace, key string, t int64, v float64) {

	id := s.id(keyspace, key)

	s.getTimeserie(id).addPoint(t, v)

	go func() {
		s.wal.WriteCh <- Point{
			ID: id,
			T:  t,
			V:  v,
		}
	}()

}

func (s *Storage) Read(keyspace, key string, start, end int64, ms bool) ([]plot.Pnt, int, gobol.Error) {

	id := s.id(keyspace, key)
	pts := s.getTimeserie(id).read(start, end)

	if ms {
		for i, pt := range pts {
			pt.Date = (pt.Date / 1000) * 1000
			pts[i] = pt
		}
	}

	return pts, len(pts), nil
}

func (s *Storage) getTimeserie(id string) *timeserie {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if _, exist := s.series[id]; !exist {
		s.series[id] = &timeserie{}
	}
	return s.series[id]
}

func (s *Storage) id(keyspace, key string) string {
	id := make([]byte, len(keyspace)+len(key))
	copy(id, keyspace)
	copy(id[len(keyspace):], key)
	return string(id)
}
