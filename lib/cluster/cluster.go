package cluster

import (
	"fmt"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/serf/serf"
	"github.com/uol/gobol/snitch"
)

// Cluster defines the mycenae cluster main structure
type Cluster struct {
	*serf.Serf
	logger       *logrus.Logger
	stats        *snitch.Stats
	loopDuration time.Duration

	events chan serf.Event
	stop   chan bool
}

// New creates a cluster structure
func New(settings Settings, logger *logrus.Logger, stats *snitch.Stats) (*Cluster, error) {
	config := makeConfig(settings)

	events := make(chan serf.Event, 10)
	config.EventCh = events

	serf, err := serf.Create(config)
	if err != nil {
		return nil, err
	}

	cluster := &Cluster{
		Serf:   serf,
		logger: logger,
		stats:  stats,
		events: events,
		stop:   make(chan bool),
	}

	cluster.loopDuration, err = settings.getFrenquency()
	if err != nil {
		return nil, err
	}

	count, err := serf.Join(settings.Seed, true)
	logger.WithFields(logrus.Fields{
		"func":   "New",
		"struct": "Cluster",
	}).WithError(err).Infof("Contacted %d / %d nodes", count, len(settings.Seed))
	go cluster.changeLoop()
	return cluster, nil
}

func (cluster *Cluster) activeMembersCount() int {
	var count int
	for _, member := range cluster.Members() {
		if member.Status == serf.StatusAlive {
			count++
		}
	}
	return count
}

func (cluster *Cluster) changeLoop() {
	ticker := time.NewTicker(cluster.loopDuration)
	cluster.logger.WithFields(logrus.Fields{
		"func":     "changeLoop",
		"struct":   "Cluster",
		"duration": cluster.loopDuration,
	}).Debugf("Cluster loop started")
	for {
		select {
		case <-ticker.C:
			clusterSize := len(cluster.Members())
			activeCount := cluster.activeMembersCount()

			cluster.logger.WithFields(logrus.Fields{
				"func":   "changeLoop",
				"struct": "Cluster",
			}).Infof("Cluster size: %d", clusterSize)
			cluster.logger.WithFields(logrus.Fields{
				"func":   "changeLoop",
				"struct": "Cluster",
			}).Infof("Cluster count: %d", activeCount)

			if cluster.stats != nil {
				cluster.stats.SetValue("cluster.size",
					nil, "1m", true, false,
					float64(clusterSize),
				)
				cluster.stats.SetValue("cluster.active",
					nil, "1m", true, false,
					float64(activeCount),
				)
			}
		case event := <-cluster.events:
			// Always clean the event queue
			fmt.Fprintf(os.Stderr, "%s: %s\n", event.EventType(), event.String())
		case <-cluster.stop:
			ticker.Stop()
			cluster.stop <- true
			return
		}
	}
}

// Stop ends the changeLoop and stops the cluster
func (cluster *Cluster) Stop() error {
	cluster.stop <- true
	<-cluster.stop

	return cluster.Serf.Shutdown()
}
