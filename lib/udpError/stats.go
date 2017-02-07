package udpError

import (
	"time"
)

func statsSelectQerror(ks, cf string) {
	go statsIncrement(
		"cassandra.query.error",
		map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"},
	)
}

func statsSelectFerror(ks, cf string) {
	go statsIncrement(
		"cassandra.fallback.error",
		map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"},
	)
}

func statsSelct(ks, cf string, d time.Duration) {
	go statsIncrement("cassandra.query", map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"})
	go statsValueAdd(
		"cassandra.query.duration",
		map[string]string{"keyspace": ks, "column_family": cf, "operation": "select"},
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func statsIndexError(i, t, m string) {
	tags := map[string]string{"index": i, "method": m}
	if t != "" {
		tags["type"] = t
	}
	go statsIncrement("elastic.request.error", tags)
}

func statsIndex(i, t, m string, d time.Duration) {
	tags := map[string]string{"index": i, "method": m}
	if t != "" {
		tags["type"] = t
	}
	go statsIncrement("elastic.request", tags)
	go statsValueAdd(
		"elastic.request.duration",
		tags,
		float64(d.Nanoseconds())/float64(time.Millisecond),
	)
}

func statsIncrement(metric string, tags map[string]string) {
	stats.Increment("udperror/persistence", metric, tags)
}

func statsValueAdd(metric string, tags map[string]string, v float64) {
	stats.ValueAdd("udperror/persistence", metric, tags, v)
}
