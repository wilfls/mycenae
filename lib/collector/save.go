package collector

import (
	"time"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/gorilla"
)

func (collector *Collector) saveText(packet gorilla.Point) gobol.Error {
	return collector.persist.InsertText(
		packet.KsID,
		packet.ID,
		packet.Timestamp,
		packet.Message.Text,
	)
}

func (collector *Collector) saveError(
	recvTags map[string]string,
	metric,
	keyspace,
	esIndex,
	id,
	msg,
	errMsg string,
) gobol.Error {

	now := time.Now()
	ks := keyspace
	if keyspace == collector.settings.Cassandra.Keyspace {
		ks = "default"
	}
	statsUDPerror(ks, "number")

	x := make([]byte, len(id)+len(keyspace))
	copy(x, id)
	copy(x[len(id):], keyspace)

	//idks := fmt.Sprintf("%s%s", id, keyspace)

	idks := string(id)

	gerr := collector.persist.InsertError(idks, msg, errMsg, now)
	if gerr != nil {
		return gerr
	}

	var tags []Tag

	for k, v := range recvTags {
		tag := Tag{
			Key:   k,
			Value: v,
		}
		tags = append(tags, tag)
	}

	doc := StructV2Error{
		Key:    id,
		Metric: metric,
		Tags:   tags,
	}

	collector.persist.SendErrorToES(esIndex, "errortag", id, doc)

	return nil
}
