package timecontrol

import (
	"sync/atomic"
	"time"
)

type Timecontrol struct {
	timestamp int64
}

func New() *Timecontrol {

	h := &Timecontrol{
		timestamp: time.Now().Unix(),
	}

	//h.start()

	return h

}

func (h *Timecontrol) start() {
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:

				atomic.StoreInt64(&h.timestamp, time.Now().Unix())

			}
		}
	}()
}

func (h *Timecontrol) Now() int64 {
	return h.timestamp
}

func (h *Timecontrol) Hour() int64 {
	now := time.Now()
	_, m, s := now.Clock()
	return now.Add(-(time.Duration(m) * time.Minute) - (time.Duration(s) * time.Second)).Unix()
}
