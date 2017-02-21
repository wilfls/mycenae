package storage

import (
	"sync"

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
	mtx           sync.RWMutex
}

type point struct {
	timestamp int64
	value     float64
}

type query struct {
	keyspace string
	key      string
	bktsCh   chan []bucket
	point
}

// New returns Storage
func New(
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

	return strg
}

func (s *Storage) start() {
	/*
		go func() {
			for {
				select {
				case p := <-s.addCh:
					//fmt.Printf("TS: %v\tV: %v\n", p.Point.Timestamp, p.Point.Value)
					tsMap := s.keyspaces[p.keyspace]
					go s.add(tsMap, p)

				case r := <-s.readCh:
					bkts := make([]bucket, len(s.series[r.id].buckets))
					copy(bkts, s.series[r.id].buckets)
					r.bktsCh <- bkts

				case <-s.stop:
					// TODO: cleanup the addCh before return
					return

				}
			}
		}()
	*/
}

// Add insert new point in a timeserie
func (s *Storage) Add(keyspace, key string, t int64, v float64) {

	id := s.id(keyspace, key)

	ts := s.getTimeserie(id)

	ts.addPoint(t, v)

}

func (s *Storage) Read(keyspace, key string, start, end int64, ms bool) ([]plot.Pnt, int, gobol.Error) {

	id := s.id(keyspace, key)

	ts := s.getTimeserie(id)

	pts := ts.read(start, end)

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
