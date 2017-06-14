package collector

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"net"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"

	"github.com/uol/mycenae/lib/bcache"
	"github.com/uol/mycenae/lib/cluster"
	"github.com/uol/mycenae/lib/depot"
	"github.com/uol/mycenae/lib/gorilla"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/tsstats"

	"go.uber.org/zap"
)

var (
	gblog *zap.Logger
	stats *tsstats.StatsTS
)

func New(
	log *zap.Logger,
	sts *tsstats.StatsTS,
	cluster *cluster.Cluster,
	cass *depot.Cassandra,
	es *rubber.Elastic,
	bc *bcache.Bcache,
	set *structs.Settings,
) (*Collector, error) {

	d, err := time.ParseDuration(set.MetaSaveInterval)
	if err != nil {
		return nil, err
	}

	gblog = log
	stats = sts

	collect := &Collector{
		boltc: bc,
		persist: persistence{
			cluster: cluster,
			esearch: es,
			cass:    cass,
		},
		validKey:    regexp.MustCompile(`^[0-9A-Za-z-._%&#;/]+$`),
		settings:    set,
		concPoints:  make(chan struct{}, set.MaxConcurrentPoints),
		concBulk:    make(chan struct{}, set.MaxConcurrentBulks),
		metaChan:    make(chan gorilla.Point, set.MetaBufferSize),
		metaPayload: &bytes.Buffer{},
	}

	go collect.metaCoordinator(d)

	return collect, nil
}

type Collector struct {
	boltc    *bcache.Bcache
	persist  persistence
	validKey *regexp.Regexp
	settings *structs.Settings

	concPoints  chan struct{}
	concBulk    chan struct{}
	metaChan    chan gorilla.Point
	metaPayload *bytes.Buffer

	receivedSinceLastProbe int64
	errorsSinceLastProbe   int64
	saving                 int64
	shutdown               bool
}

func (collect *Collector) CheckUDPbind() bool {

	ctxt := gblog.With(
		zap.String("struct", "CollectorV2"),
		zap.String("func", "CheckUDPbind"),
	)

	port := ":" + collect.settings.UDPserverV2.Port

	addr, err := net.ResolveUDPAddr("udp", port)
	if err != nil {
		ctxt.Error("addr:", zap.Error(err))
	}

	_, err = net.ListenUDP("udp", addr)
	if err != nil {
		ctxt.Debug("", zap.Error(err))
		return true
	}

	return false
}

func (collect *Collector) ReceivedErrorRatio() float64 {

	ctxt := gblog.With(
		zap.String("struct", "CollectorV2"),
		zap.String("func", "ReceivedErrorRatio"),
	)

	y := atomic.LoadInt64(&collect.receivedSinceLastProbe)
	var ratio float64
	if y != 0 {
		ratio = float64(atomic.LoadInt64(&collect.errorsSinceLastProbe) / y)
	}

	ctxt.Debug("", zap.Float64("ratio", ratio))

	atomic.StoreInt64(&collect.receivedSinceLastProbe, 0)
	atomic.StoreInt64(&collect.errorsSinceLastProbe, 0)

	return ratio
}

func (collect *Collector) Stop() {
	collect.shutdown = true
	for {
		if collect.saving <= 0 {
			return
		}
	}
}

func (collect *Collector) HandlePacket(rcvMsg gorilla.TSDBpoint, number bool) gobol.Error {

	start := time.Now()

	atomic.AddInt64(&collect.receivedSinceLastProbe, 1)

	packet := gorilla.Point{}

	gerr := collect.makePacket(&packet, rcvMsg, number)
	if gerr != nil {
		gblog.Error("makePacket", zap.Error(gerr))
		return gerr
	}

	if number {
		gerr = collect.saveValue(&packet)
	} else {
		gerr = collect.saveText(packet)
	}

	if gerr != nil {

		atomic.AddInt64(&collect.errorsSinceLastProbe, 1)

		gblog.Error("save", zap.Error(gerr))
		return gerr
	}

	if len(collect.metaChan) < collect.settings.MetaBufferSize {
		go collect.saveMeta(packet)
	} else {
		gblog.Warn(
			fmt.Sprintf("discarding point: %v", rcvMsg),
			zap.String("func", "collector/HandlePacket"),
		)
		statsLostMeta()
	}

	statsProcTime(packet.KsID, time.Since(start))
	return nil
}

func GenerateID(rcvMsg gorilla.TSDBpoint) string {

	h := crc32.NewIEEE()

	if rcvMsg.Metric != "" {
		h.Write([]byte(rcvMsg.Metric))
	}

	mk := []string{}

	for k := range rcvMsg.Tags {
		if k != "ksid" && k != "ttl" {
			mk = append(mk, k)
		}
	}

	sort.Strings(mk)

	for _, k := range mk {

		h.Write([]byte(k))
		h.Write([]byte(rcvMsg.Tags[k]))

	}

	return fmt.Sprint(h.Sum32())
}

func (collect *Collector) CheckTSID(esType, id string) (bool, gobol.Error) {

	info := strings.Split(id, "|")

	respCode, gerr := collect.persist.HeadMetaFromES(info[0], esType, info[1])
	if gerr != nil {
		return false, gerr
	}
	if respCode != 200 {
		return false, nil
	}

	return true, nil
}
