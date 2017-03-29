package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/uol/gobol"
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
	Cassandra   Cassandra
	stop        chan struct{}
	saveSerieCh chan timeToSaveSerie
	saveSeries  []timeToSaveSerie
	tsmap       map[string]*serie
	wal         *WAL
	mtx         sync.RWMutex
	tc          TC
}

// TC interface to controle Now().Unix()
type TC interface {
	Now() int64
	Hour() int64
}

type timeToSaveSerie struct {
	ksid     string
	tsid     string
	lastSave int64
}

// New returns Storage
func New(
	session *gocql.Session,
	consist []gocql.Consistency,
	wal *WAL,
	tc TC,
) *Storage {

	c := Cassandra{
		session:            session,
		writeConsistencies: consist,
		readConsistencies:  consist,
	}

	return &Storage{
		stop:        make(chan struct{}),
		tsmap:       make(map[string]*serie),
		saveSerieCh: make(chan timeToSaveSerie, 1000),
		saveSeries:  []timeToSaveSerie{},
		Cassandra:   c,
		wal:         wal,
		tc:          tc,
	}
}

// Start dispatch a goroutine to save buckets
// in cassandra. All buckets with more then a hour
// must be compressed and saved in cassandra.
func (s *Storage) Start() {

	go func() {
		ticker := time.NewTicker(time.Second * 10)

		for {
			select {
			case <-ticker.C:

				now := time.Now().Unix()
				for _, ts := range s.saveSeries {
					delta := now - ts.lastSave

					if delta > 7200 {
						s.getSerie(ts.ksid, ts.tsid)
					}

				}

			case serie := <-s.saveSerieCh:
				s.saveSeries = append(s.saveSeries, serie)

			case <-s.stop:
				// TODO: cleanup the addCh before return
				return

			}
		}
	}()
}

// Add insert new point in a timeseries
func (s *Storage) Add(ksid, tsid string, t int64, v float32) {

	err := s.getSerie(ksid, tsid).addPoint(s.Cassandra, ksid, tsid, t, v)
	if err != nil {
		fmt.Println(err)
	}

	if s.wal != nil {
		s.wal.Add(ksid, tsid, t, v)
	}

}

func (s *Storage) Read(ksid, tsid string, start, end int64) (Pnts, int, gobol.Error) {

	pts := s.getSerie(ksid, tsid).read(start, end)

	return pts, len(pts), nil
}

func (s *Storage) getSerie(ksid, tsid string) *serie {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	id := s.id(ksid, tsid)
	serie := s.tsmap[id]
	if serie == nil {
		serie = newSerie(ksid, tsid, s.tc)
		s.tsmap[id] = serie
	}

	return serie
}

func (s *Storage) id(keyspace, key string) string {
	id := make([]byte, len(keyspace)+len(key))
	copy(id, keyspace)
	copy(id[len(keyspace):], key)
	return string(id)
}
