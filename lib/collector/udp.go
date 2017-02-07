package collector

import (
	"encoding/json"
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/uol/gobol"
)

func (collector *Collector) HandleUDPpacket(buf []byte, addr string) {
	go func() {
		collector.saveMutex.Lock()
		collector.saving++
		collector.saveMutex.Unlock()
	}()

	rcvMsg := TSDBpoint{}

	var gerr gobol.Error

	err := json.Unmarshal(buf, &rcvMsg)
	if err != nil {
		gerr = errUnmarshal("HandleUDPpacket", err)

		if gr := collector.saveError(
			map[string]string{},
			"",
			collector.settings.Cassandra.Keyspace,
			collector.settings.ElasticSearch.Index,
			"noKey",
			string(buf),
			gerr.Error(),
		); gr != nil {
			gerr = gr
		}

		collector.fail(gerr, addr)
		return
	}

	msgKs := ""

	if val, ok := rcvMsg.Tags["ksid"]; ok {
		msgKs = val
	}

	isNumber := true

	gerr = collector.HandlePacket(rcvMsg, isNumber)
	if gerr != nil {

		collector.fail(gerr, addr)

		keyspace := collector.settings.Cassandra.Keyspace
		esIndex := collector.settings.ElasticSearch.Index

		if msgKs != "" {
			_, found, gerr := collector.boltc.GetKeyspace(msgKs)
			if found {
				keyspace = msgKs
				esIndex = msgKs
			}
			if gerr == nil {
				collector.fail(gerr, addr)
			}
		}

		id := GenerateID(rcvMsg)
		if !isNumber {
			id = fmt.Sprintf("T%v", id)
		}

		gerr = collector.saveError(
			rcvMsg.Tags,
			rcvMsg.Metric,
			keyspace,
			esIndex,
			id,
			string(buf),
			gerr.Error(),
		)
		if gerr == nil {
			collector.fail(gerr, addr)
		}

	} else {
		statsUDP(msgKs, "number")
	}

	go func() {
		collector.saveMutex.Lock()
		collector.saving--
		collector.saveMutex.Unlock()
	}()
}

func (collector *Collector) fail(gerr gobol.Error, addr string) {
	defer func() {
		if r := recover(); r != nil {
			gblog.WithFields(
				logrus.Fields{
					"func":    "fail",
					"pacakge": "Collector",
				},
			).Error("Panic: %v", r)
		}
	}()

	fields := gerr.LogFields()
	fields["addr"] = addr

	gblog.WithFields(fields).Error(gerr)
}
