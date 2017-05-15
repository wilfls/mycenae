package gorilla

import (
	"sync"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/depot"
	"github.com/uol/mycenae/lib/tsstats"

	"go.uber.org/zap"
)

var (
	gblog *zap.Logger
	stats *tsstats.StatsTS
)

// Storage keeps all timeseries in memory
// after a while the serie will be saved at cassandra
// if the time range is not in memory it must query cassandra
type Storage struct {
	persist     depot.Persistence
	stop        chan struct{}
	saveSerieCh chan timeToSaveSerie
	saveSeries  []timeToSaveSerie
	tsmap       map[string]*serie
	wal         *WAL
	mtx         sync.RWMutex
}

type timeToSaveSerie struct {
	ksid     string
	tsid     string
	lastSave int64
}

// New returns Storage
func New(
	lgr *zap.Logger,
	sts *tsstats.StatsTS,
	persist depot.Persistence,
	wal *WAL,
) *Storage {

	stats = sts
	gblog = lgr

	s := &Storage{
		stop:        make(chan struct{}),
		tsmap:       make(map[string]*serie),
		saveSerieCh: make(chan timeToSaveSerie, 1000),
		saveSeries:  []timeToSaveSerie{},
		persist:     persist,
		wal:         wal,
	}

	go func() {
		time.Sleep(time.Minute)
		ptsChan := wal.load()
		for pts := range ptsChan {
			for _, p := range pts {
				err := s.getSerie(p.KSID, p.TSID).addPoint(p.T, p.V)
				if err != nil {
					gblog.Error("", zap.Error(err))
				}
			}
		}
	}()

	return s

}

// Load dispatch a goroutine to save buckets
// in cassandra. All buckets with more then a hour
// must be compressed and saved in cassandra.
/*
func (s *Storage) Load() {
		go func() {
			ticker := time.NewTicker(time.Second * 10)

			for {
				select {
				case <-ticker.C:

					now := s.tc.Now()
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
*/

//Add new point in a timeseries
func (s *Storage) Add(ksid, tsid string, t int64, v float32) error {

	err := s.getSerie(ksid, tsid).addPoint(t, v)
	if err != nil {
		return err
	}

	s.wal.Add(ksid, tsid, t, v)

	return nil
}

//Read points from a timeseries, if range start bigger than 24hours
// it will read points from persistence
func (s *Storage) Read(ksid, tsid string, start, end int64) (Pnts, gobol.Error) {

	return s.getSerie(ksid, tsid).read(start, end)

}

func (s *Storage) getSerie(ksid, tsid string) *serie {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	id := s.id(ksid, tsid)
	serie := s.tsmap[id]
	if serie == nil {
		serie = newSerie(s.persist, ksid, tsid)
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
