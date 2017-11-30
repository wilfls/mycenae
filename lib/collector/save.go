package collector

import (
	"fmt"

	"github.com/uol/gobol"
)

func (collector *Collector) saveValue(packet Point) gobol.Error {
	return collector.persist.InsertPoint(
		packet.KsID,
		fmt.Sprintf("%v%v", packet.Bucket, packet.ID),
		packet.Timestamp,
		*packet.Message.Value,
	)
}

func (collector *Collector) saveTUUIDvalue(packet Point) gobol.Error {
	return collector.persist.InsertTUUIDpoint(
		packet.KsID,
		fmt.Sprintf("%v%v", packet.Bucket, packet.ID),
		packet.TimeUUID,
		*packet.Message.Value,
	)
}

func (collector *Collector) saveText(packet Point) gobol.Error {
	return collector.persist.InsertText(
		packet.KsID,
		fmt.Sprintf("%v%v", packet.Bucket, packet.ID),
		packet.Timestamp,
		packet.Message.Text,
	)
}

func (collector *Collector) saveTUUIDtext(packet Point) gobol.Error {
	return collector.persist.InsertTUUIDtext(
		packet.KsID,
		fmt.Sprintf("%v%v", packet.Bucket, packet.ID),
		packet.TimeUUID,
		packet.Message.Text,
	)
}
