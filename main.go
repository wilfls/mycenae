package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/gocql/gocql"
	"github.com/uol/gobol/cassandra"
	"github.com/uol/gobol/loader"
	"github.com/uol/gobol/rubber"
	"github.com/uol/gobol/saw"
	"github.com/uol/gobol/snitch"

	"github.com/uol/mycenae/lib/bcache"
	"github.com/uol/mycenae/lib/cluster"
	"github.com/uol/mycenae/lib/collector"
	"github.com/uol/mycenae/lib/keyspace"
	"github.com/uol/mycenae/lib/plot"
	"github.com/uol/mycenae/lib/rest"
	"github.com/uol/mycenae/lib/storage"
	"github.com/uol/mycenae/lib/storage/timecontrol"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/tsstats"
	"github.com/uol/mycenae/lib/udp"
	"github.com/uol/mycenae/lib/udpError"
)

func main() {

	fmt.Println("Starting Mycenae")

	//Parse of command line arguments.
	var confPath string

	flag.StringVar(&confPath, "config", "config.toml", "path to configuration file")
	flag.Parse()

	//Load conf file.
	settings := new(structs.Settings)

	err := loader.ConfToml(confPath, &settings)
	if err != nil {
		log.Fatalln("ERROR - Loading Config file: ", err)
	} else {
		fmt.Println("Config file loaded.")
	}

	tsLogger := new(structs.TsLog)
	tsLogger.General, err = saw.New(settings.Logs.General)
	if err != nil {
		log.Fatalln("ERROR - Starting logger: ", err)
	}

	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6666", nil))
	}()

	tsLogger.Stats, err = saw.New(settings.Logs.Stats)
	if err != nil {
		log.Fatalln("ERROR - Starting logger: ", err)
	}

	sts, err := snitch.New(tsLogger.Stats, settings.Stats)
	if err != nil {
		log.Fatalln("ERROR - Starting stats: ", err)
	}

	tssts, err := tsstats.New(tsLogger.General, sts, settings.Stats.Interval)
	if err != nil {
		tsLogger.General.Error(err)
		os.Exit(1)
	}

	rcs, err := parseConsistencies(settings.ReadConsistency)
	if err != nil {
		tsLogger.General.Error(err)
		os.Exit(1)
	}

	wcs, err := parseConsistencies(settings.WriteConsisteny)
	if err != nil {
		tsLogger.General.Error(err)
		os.Exit(1)
	}

	cass, err := cassandra.New(settings.Cassandra)
	if err != nil {
		log.Fatalln("ERROR - Connecting to cassandra: ", err)
	}
	defer cass.Close()

	es := rubber.New(tsLogger.General, settings.ElasticSearch.Cluster)

	ks := keyspace.New(
		tssts,
		cass,
		es,
		settings.Cassandra.Username,
		settings.Cassandra.Keyspace,
		settings.CompactionStrategy,
		settings.TTL.Max,
	)

	bc, err := bcache.New(tssts, ks, settings.BoltPath)
	if err != nil {
		tsLogger.General.Error(err)
		os.Exit(1)
	}

	wal, err := storage.NewWAL(settings.WALPath)
	if err != nil {
		log.Println("WAL:", err)
		tsLogger.General.Error(err)
		os.Exit(1)
	}

	tc := timecontrol.New()
	strg := storage.New(cass, wcs, wal, tc)

	log.Println("storage initialized successfully")

	cluster, err := cluster.New(tsLogger.General, strg, tc, settings.Cluster)
	if err != nil {
		tsLogger.General.Error(err)
		os.Exit(1)
	}
	log.Println("cluster initialized successfully")

	coll, err := collector.New(tsLogger, tssts, cluster, es, bc, settings)
	if err != nil {
		log.Println(err)
		return
	}

	uV2server := udp.New(tsLogger.General, settings.UDPserverV2, coll)

	uV2server.Start()

	collectorV1 := collector.UDPv1{}

	uV1server := udp.New(tsLogger.General, settings.UDPserver, collectorV1)

	uV1server.Start()

	p, err := plot.New(
		tsLogger.General,
		tssts,
		cluster,
		es,
		bc,
		settings.ElasticSearch.Index,
		settings.MaxTimeseries,
		settings.MaxConcurrentTimeseries,
		settings.MaxConcurrentReads,
		settings.LogQueryTSthreshold,
	)

	if err != nil {
		tsLogger.General.Error(err)
		os.Exit(1)
	}

	uError := udpError.New(
		tsLogger.General,
		tssts,
		cass,
		bc,
		es,
		settings.ElasticSearch.Index,
		rcs,
	)

	tsRest := rest.New(
		tsLogger,
		sts,
		p,
		uError,
		ks,
		bc,
		coll,
		settings.HTTPserver,
		settings.Probe.Threshold,
	)

	tsRest.Start()

	signalChannel := make(chan os.Signal, 1)

	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {

		for {
			sig := <-signalChannel
			switch sig {
			case os.Interrupt, syscall.SIGTERM:
				stop(tsLogger, tsRest, coll)
				wg.Done()
				return
			case syscall.SIGHUP:
				//THIS IS A HACK DO NOT EXTEND IT. THE FEATURE IS NICE BUT NEEDS TO BE DONE CORRECTLY!!!!!
				settings := new(structs.Settings)
				var err error

				if strings.HasSuffix(confPath, ".json") {
					err = loader.ConfJson(confPath, &settings)
				} else if strings.HasSuffix(confPath, ".toml") {
					err = loader.ConfToml(confPath, &settings)
				}
				if err != nil {
					tsLogger.General.Error("ERROR - Loading Config file: ", err)
					continue
				} else {
					tsLogger.General.Info("Config file loaded.")
				}

				rcs, err := parseConsistencies(settings.ReadConsistency)
				if err != nil {
					tsLogger.General.Errorln(err)
					continue
				}

				wcs, err := parseConsistencies(settings.WriteConsisteny)
				if err != nil {
					tsLogger.General.Errorln(err)
					continue
				}

				strg.Cassandra.SetWriteConsistencies(wcs)

				strg.Cassandra.SetReadConsistencies(rcs)

				tsLogger.General.Info("New consistency set")

			}
		}

	}()

	fmt.Println("Mycenae started successfully")

	wg.Wait()

	os.Exit(0)
}

func parseConsistencies(names []string) ([]gocql.Consistency, error) {
	if len(names) == 0 {
		return nil, errors.New("consistency array cannot be empty")
	}

	if len(names) > 3 {
		return nil, errors.New("consistency array too big")
	}

	tmp := make([]gocql.Consistency, len(names))
	for i, cons := range names {
		cons = strings.ToLower(cons)

		switch cons {
		case "one":
			tmp[i] = gocql.One
		case "quorum":
			tmp[i] = gocql.Quorum
		case "all":
			tmp[i] = gocql.All
		default:
			return nil, fmt.Errorf("error: unknown consistency: %s", cons)
		}
	}
	return tmp, nil
}

func stop(logger *structs.TsLog, rest *rest.REST, collector *collector.Collector) {

	fmt.Println("Stopping REST")
	logger.General.Info("Stopping REST")
	rest.Stop()
	fmt.Println("REST stopped")

	fmt.Println("Stopping UDPv2")
	logger.General.Info("Stopping UDPv2")
	collector.Stop()
	fmt.Println("UDPv2 stopped")

}
