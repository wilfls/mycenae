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
	persist  depot.Persistence
	stop     chan struct{}
	tsmap    map[string]*serie
	localTS  localTSmap
	localTSC chan Meta
	dump     chan struct{}
	wal      *WAL
	mtx      sync.RWMutex
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
	wal *WAL,
) *Storage {

	stats = sts
	gblog = lgr

	s := &Storage{
		stop:     make(chan struct{}),
		tsmap:    make(map[string]*serie),
		localTS:  localTSmap{tsmap: make(map[string]Meta)},
		localTSC: make(chan Meta, 1000),
		dump:     make(chan struct{}),
		persist:  persist,
		wal:      wal,
	}

	go func() {
		time.Sleep(time.Minute)
		ptsChan := wal.load()
		for pts := range ptsChan {
			for _, p := range pts {
				err := s.getSerie(p.KSID, p.TSID).addPoint(p.T, p.V)
				if err != nil {
					gblog.Error(
						"",
						zap.String("package", "gorilla"),
						zap.String("func", "storage/New"),
						zap.Error(err),
					)
				}
			}
		}
	}()

	return s

}

// Load dispatch a goroutine to save buckets
// in cassandra. All buckets with more then a hour
// must be compressed and saved in cassandra.

func (s *Storage) Load() {
	go func(s *Storage) {
		ticker := time.NewTicker(time.Second * 10)

		for {
			select {
			case <-ticker.C:
				now := time.Now().Unix()

				for _, serie := range s.ListSeries() {
					delta := now - serie.lastCheck
					if delta > 7200 {
						// we need a way to persist ts older than 2h
						// after 26h the serie must be out of memory
						s.getSerie(serie.KSID, serie.TSID)
					}
				}

			case meta := <-s.localTSC:
				s.localTS.mtx.Lock()
				defer s.localTS.mtx.Unlock()
				s.localTS.tsmap[s.id(meta.KSID, meta.TSID)] = meta

			case <-s.stop:
				// TODO: cleanup the addCh before return
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
	s.mtx.Lock()
	defer s.mtx.Unlock()
	id := s.id(ksid, tsid)
	serie := s.tsmap[id]
	if serie == nil {
		serie = newSerie(s.persist, ksid, tsid)
		s.tsmap[id] = serie
		s.localTS.tsmap[id] = Meta{KSID: ksid, TSID: tsid}
	}

	return serie
}

func (s *Storage) deleteSerie(ksid, tsid string) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	id := s.id(ksid, tsid)
	delete(s.tsmap, id)
	delete(s.localTS.tsmap, id)
}

func (s *Storage) id(ksid, tsid string) string {
	id := make([]byte, len(ksid)+len(tsid))
	copy(id, ksid)
	copy(id[len(ksid):], tsid)
	return string(id)
}
