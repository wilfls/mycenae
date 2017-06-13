package collector

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/gorilla"

	"go.uber.org/zap"
)

func (collector *Collector) HandleUDPpacket(buf []byte, addr string) {

	atomic.AddInt64(&collector.saving, 1)

	rcvMsg := gorilla.TSDBpoint{}

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
			if gerr != nil {
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
		if gerr != nil {
			collector.fail(gerr, addr)
		}

	} else {
		statsUDP(msgKs, "number")
	}

	
	atomic.AddInt64(&collector.saving, -1)
	
	
}

func (collector *Collector) fail(gerr gobol.Error, addr string) {
	defer func() {
		if r := recover(); r != nil {
			gblog.Error(
				fmt.Sprintf("Panic: %v", r),
				zap.String("func", "fail"),
				zap.String("package", "Collector"),
			)
		}
	}()

	fields := gerr.LogFields()
	fields["addr"] = addr

	gblog.Error(gerr.Error(), zap.Any("fields", fields))
}
