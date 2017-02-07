package tsstats

import (
	"github.com/Sirupsen/logrus"
	"gopkg.in/robfig/cron.v2"
	"github.com/uol/gobol/snitch"
)

func New(gbl *logrus.Logger, gbs *snitch.Stats, intvl string) (*StatsTS, error) {
	if _, err := cron.Parse(intvl); err != nil {
		return nil, err
	}
	return &StatsTS{
		log:      gbl,
		stats:    gbs,
		interval: intvl,
	}, nil
}

type StatsTS struct {
	stats    *snitch.Stats
	log      *logrus.Logger
	interval string
}

func (sts *StatsTS) Increment(callerID string, metric string, tags map[string]string) {
	err := sts.stats.Increment(metric, tags, sts.interval, false, true)
	if err != nil {
		sts.log.WithFields(logrus.Fields{
			"package": callerID,
			"func":    "statsIncrement",
			"metric":  metric,
		}).Error(err)
	}
}

func (sts *StatsTS) ValueAdd(callerID string, metric string, tags map[string]string, v float64) {
	err := sts.stats.ValueAdd(metric, tags, "avg", sts.interval, false, false, v)
	if err != nil {
		sts.log.WithFields(logrus.Fields{
			"package": callerID,
			"func":    "statsValueAdd",
			"metric":  metric,
		}).Error(err)
	}
}
