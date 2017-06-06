package gorilla

import (
	"sync"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/depot"
	pb "github.com/uol/mycenae/lib/proto"
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
	persist depot.Persistence
	stop    chan struct{}
	tsmap   map[string]*serie
	localTS localTSmap
	dump    chan struct{}
	wal     *WAL
	mtx     sync.RWMutex
}

type localTSmap struct {
	mtx   sync.RWMutex
	tsmap map[string]Meta
}

type Meta struct {
	KSID      string
	TSID      string
	lastCheck int64
}

// New returns Storage
func New(
	lgr *zap.Logger,
	sts *tsstats.StatsTS,
	persist depot.Persistence,
	w *WAL,
) *Storage {

	stats = sts
	gblog = lgr

	return &Storage{
		stop:    make(chan struct{}),
		tsmap:   make(map[string]*serie),
		localTS: localTSmap{tsmap: make(map[string]Meta)},
		dump:    make(chan struct{}),
		wal:     w,
		persist: persist,
	}
}

// Load dispatch a goroutine to save buckets
// in cassandra. All buckets with more then a hour
// must be compressed and saved in cassandra.
func (s *Storage) Load() {
	go func(s *Storage) {
		ticker := time.NewTicker(time.Minute)

		for {
			select {
			case <-ticker.C:
				now := time.Now().Unix()

				for _, serie := range s.ListSeries() {
					delta := now - serie.lastCheck
					if delta > hour {
						// we need a way to persist ts older than 2h
						// after 26h the serie must be out of memory
						gblog.Debug(
							"checking if serie must be persisted",
							zap.String("ksid", serie.KSID),
							zap.String("tsid", serie.TSID),
							zap.String("func", "Load"),
							zap.String("package", "gorilla"),
						)
						s.updateLastCheck(&serie)
						if s.getSerie(serie.KSID, serie.TSID).toDepot() {
							s.deleteSerie(serie.KSID, serie.TSID)
						}

					}
				}
			case <-s.stop:
				s.mtx.Lock()

				c := make(chan struct{}, 100)
				u := make(map[string]Meta)
				for _, m := range s.ListSeries() {
					go func() {
						c <- struct{}{}
						err := s.getSerie(m.KSID, m.TSID).stop()
						if err != nil {
							gblog.Error(
								"unable to save serie",
								zap.String("ksid", m.KSID),
								zap.String("tsid", m.TSID),
								zap.Error(err),
							)

							i := s.id(m.KSID, m.TSID)
							u[i] = m
						}
						<-c
					}()
				}

				// write file
				s.wal.flush(u)

				return

			}
		}
	}(s)
}

func (s *Storage) ListSeries() []Meta {
	s.localTS.mtx.RLock()
	defer s.localTS.mtx.RUnlock()

	m := []Meta{}
	for _, meta := range s.localTS.tsmap {
		m = append(m, meta)
	}
	return m
}

func (s *Storage) updateLastCheck(serie *Meta) {

	id := s.id(serie.KSID, serie.TSID)
	s.localTS.mtx.Lock()
	defer s.localTS.mtx.Unlock()

	serie.lastCheck = time.Now().Unix()
	s.localTS.tsmap[id] = *serie
}

func (s *Storage) Delete(m Meta) <-chan []*pb.Point {

	ptsC := make(chan []*pb.Point)

	now := time.Now().Unix()

	start := BlockID(now)

	go func() {
		defer close(ptsC)

		pts, err := s.getSerie(m.KSID, m.TSID).read(start, now)
		if err != nil {
			gblog.Error(
				"",
				zap.String("package", "gorilla"),
				zap.String("func", "storage/Delete"),
				zap.Error(err),
			)
			return
		}

		ptsC <- pts
		s.deleteSerie(m.KSID, m.TSID)

	}()

	return ptsC
}

//Add new point in a timeseries
func (s *Storage) Write(ksid, tsid string, t int64, v float32) gobol.Error {
	s.wal.Add(ksid, tsid, t, v)

	return s.getSerie(ksid, tsid).addPoint(t, v)
}

//Read points from a timeseries, if range start bigger than 24hours
// it will read points from persistence
func (s *Storage) Read(ksid, tsid string, start, end int64) ([]*pb.Point, gobol.Error) {
	return s.getSerie(ksid, tsid).read(start, end)
}

func (s *Storage) getSerie(ksid, tsid string) *serie {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	id := s.id(ksid, tsid)
	serie := s.tsmap[id]
	if serie == nil {
		serie = newSerie(s.persist, ksid, tsid)
		s.mtx.Lock()
		s.tsmap[id] = serie
		s.mtx.Unlock()

		s.localTS.mtx.Lock()
		s.localTS.tsmap[id] = Meta{
			KSID:      ksid,
			TSID:      tsid,
			lastCheck: time.Now().Unix(),
		}
		s.localTS.mtx.Unlock()

	}

	return serie
}

func (s *Storage) deleteSerie(ksid, tsid string) {

	id := s.id(ksid, tsid)
	gblog.Info(
		"removing serie from memory",
		zap.String("ksid", ksid),
		zap.String("tsid", tsid),
		zap.String("package", "gorilla"),
		zap.String("func", "deleteSerie"),
	)

	s.mtx.Lock()
	delete(s.tsmap, id)
	s.mtx.Unlock()

	s.localTS.mtx.Lock()
	delete(s.localTS.tsmap, id)
	s.localTS.mtx.Unlock()

}

func (s *Storage) id(ksid, tsid string) string {
	id := make([]byte, len(ksid)+len(tsid))
	copy(id, ksid)
	copy(id[len(ksid):], tsid)
	return string(id)
}
