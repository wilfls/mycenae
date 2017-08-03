package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type PayloadTsdbQueryMemMeta struct {
	info TsMetaInfoTsdbQueryMem
}

type basicResponseTsdbQueryMem struct {
	TotalRecord int                      `json:"totalRecords"`
	Payload     []TsMetaInfoTsdbQueryMem `json:"payload"`
}

type TsMetaInfoTsdbQueryMem struct {
	TsID   string            `json:"id"`
	Metric string            `json:"metric,omitempty"`
	Tags   map[string]string `json:"tags,omitempty"`
}

type PointsTsdbQueryMem struct {
	Data DataTsdbQueryMem `json:"points"`
}

type DataTsdbQueryMem struct {
	Count int         `json:"count"`
	Total int         `json:"total"`
	Ts    [][]float32 `json:"ts"`
}

type PayloadPointsTsdbQueryMem struct {
	ID map[string]PointsTsdbQueryMem `json:"payload"`
}

type PointTsdbQueryMem struct {
	Value     float32           `json:"value"`
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags"`
	Timestamp int64             `json:"timestamp"`
}

type PayloadTsdbQueryMem struct {
	Metric  string             `json:"metric"`
	Tags    map[string]string  `json:"tags"`
	AggTags []string           `json:"aggregateTags"`
	Tsuuids []string           `json:"tsuids"`
	Dps     map[string]float32 `json:"dps"`
}

type PayloadTsdbNullQueryMem struct {
	Metric  string                 `json:"metric"`
	Tags    map[string]string      `json:"tags"`
	AggTags []string               `json:"aggregateTags"`
	Tsuuids []string               `json:"tsuids"`
	Dps     map[string]interface{} `json:"dps"`
}

type GrafanaPointsErrorMem struct {
	Error     string `json:"error,omitempty"`
	Message   string `json:"message,omitempty"`
	RequestID string `json:"requestID,omitempty"`
}

var ts10IDTsdbQueryMem, ts10_1IDTsdbQueryMem, ts12IDTsdbQueryMem, ts13IDTsdbQueryMem,
	ts13IDTsdbQueryMem2, ts13IDTsdbQueryMem3, ts13IDTsdbQueryMem4, ts13IDTsdbQueryMem5,
	ts13IDTsdbQueryMem6, ts13IDTsdbQueryMem7, ts13IDTsdbQueryMem8 string

var ts12TsdbQueryMemStartTime, ts13TsdbQueryMemStartTime int64

var hashMapMem = map[string]string{}
var hashMapStartTime = map[string]int64{}

func tsInsertMem(keyspace string) {

	cases := map[string]struct {
		metric     string
		tagValue   string
		startTime  int64
		iStartTime int64
		value      float32
		iValue     float32
		numTotal   int
	}{
		// serie: 0,1,2,3,4...
		"ts1TsdbQueryMem":    {"ts01tsdbmem", "test", time.Now().Unix() - (60 * 100), 60, 0.0, 1.0, 100},
		"ts1_1TsdbQueryMem":  {"ts01_1tsdbmem", "test", time.Now().Unix() - (60 * 100), 60, 0.0, 1.0, 100},
		"ts1_1TsdbQueryMem2": {"ts01_1tsdbmem", "test2", time.Now().Unix() - (60 * 100), 60, 0.0, 1.0, 100},
		"ts1_2TsdbQueryMem":  {"ts01_2tsdbmem", "test1", (time.Now().Truncate(time.Minute).Unix() - 6000) + 30, 60, 0.0, 1.0, 100},
		"ts1_2TsdbQueryMem2": {"ts01_2tsdbmem", "test2", time.Now().Truncate(time.Minute).Unix() - 6000, 60, 0.0, 1.0, 100},
		"ts1_3TsdbQueryMem":  {"ts01_3tsdbmem", "test.1", (time.Now().Truncate(time.Minute).Unix() - 100000000) + 30, 60, 0.0, 1.0, 100},
		"ts1_3TsdbQueryMem2": {"ts01_3tsdbmem", "test.2", time.Now().Truncate(time.Minute).Unix() - 100000000, 60, 0.0, 1.0, 100},
		// serie: 0,1,2,1,2,3,2,3,4...
		"ts2TsdbQueryMem":  {"ts02tsdbmem", "test", time.Now().Truncate(time.Minute).Unix() - (60 * 100), 180, 0.0, 1.0, 30},
		"ts2TsdbQueryMem2": {"ts02tsdbmem", "test", time.Now().Truncate(time.Minute).Unix() - (60 * 100) + 60, 180, 1.0, 1.0, 30},
		"ts2TsdbQueryMem3": {"ts02tsdbmem", "test", time.Now().Truncate(time.Minute).Unix() - (60 * 100) + 120, 180, 2.0, 1.0, 30},
		// serie: 0,5,10,15,20...
		"ts3TsdbQueryMem": {"ts03tsdbmem", "test", (time.Now().Truncate(time.Hour).UnixNano() / 1e+9) - (8640 * 100), 1800, 0.0, 5, 480},
		// serie: 0.5,1.0,1.5,2.0...
		"ts4TsdbQueryMem": {"ts04tsdbmem", "test", time.Now().Truncate(time.Minute).Unix() - (1257984 * 100), 604800, 0.0, 0.5, 208},
		// serie: 0, 2, 4, 6, 8...
		"ts5TsdbQueryMem":  {"ts05tsdbmem", "test1", time.Now().Truncate(time.Minute).Unix() - (120 * 45), 120, 0.0, 2.0, 45},
		"ts5TsdbQueryMem2": {"ts05tsdbmem", "test2", time.Now().Truncate(time.Minute).Unix() - (120 * 45) + 60, 120, 1.0, 2.0, 45},
		"ts5TsdbQueryMem3": {"ts05tsdbmem", "test3", time.Now().Truncate(time.Minute).Unix() - (120 * 45), 60, 0.0, 1.0, 90},
		// serie: 0, 1, 2, 3, 4...
		"ts7TsdbQueryMem":    {"ts07tsdbmem", "test1", time.Now().Truncate(time.Minute).Unix() - (60 * 100), 60, 0.0, 1.0, 90},
		"ts7TsdbQueryMem2":   {"ts07tsdbmem", "test2", time.Now().Truncate(time.Minute).Unix() - (60 * 100), 60, 0.0, 5.0, 90},
		"ts7_1TsdbQueryMem":  {"ts07_1tsdbmem", "test1", time.Now().Truncate(time.Minute).Unix() - (60 * 100), 60, 0.0, 1.0, 90},
		"ts7_1TsdbQueryMem2": {"ts07_1tsdbmem", "test2", time.Now().Truncate(time.Minute).Unix() - (60 * 100), 60, 0.0, 1.0, 15},
		"ts7_1TsdbQueryMem3": {"ts07_1tsdbmem", "test2", time.Now().Truncate(time.Minute).Unix() - (60 * 100) + 4500, 60, 75, 1.0, 15},
		"ts7_2TsdbQueryMem":  {"ts07_2tsdbmem", "test1", time.Now().Truncate(time.Minute).Unix() - (60 * 100), 60, 0.0, 1.0, 90},
		"ts7_2TsdbQueryMem2": {"ts07_2tsdbmem", "test2", time.Now().Truncate(time.Minute).Unix() - (60 * 100) + 1, 60, 0.0, 5.0, 90},
		// serie: 0, 1, 2, 3,...,14,15,75,76,77,...,88,89.
		"ts9TsdbQueryMem":    {"ts09tsdbmem", "test", time.Now().Truncate(time.Minute).Unix() - (60 * 100), 60, 0.0, 1.0, 15},
		"ts9TsdbQueryMem2":   {"ts09tsdbmem", "test", time.Now().Truncate(time.Minute).Unix() - (60 * 100) + 4500, 60, 75, 1.0, 15},
		"ts9_1TsdbQueryMem":  {"ts09_1tsdbmem", "test", time.Now().Truncate(time.Minute).Unix() - (60 * 100), 60, 0.0, 1.0, 15},
		"ts9_1TsdbQueryMem2": {"ts09_1tsdbmem", "test", time.Now().Truncate(time.Minute).Unix() - (60 * 100) + 4500, 60, 75, 1.0, 15},
		"ts9_1TsdbQueryMem3": {"ts09_1tsdbmem", "test2", time.Now().Truncate(time.Minute).Unix() - (60 * 100), 60, 0.0, 1.0, 15},
		"ts9_1TsdbQueryMem4": {"ts09_1tsdbmem", "test2", time.Now().Truncate(time.Minute).Unix() - (60 * 100) + 4500, 60, 75, 1.0, 15},
		// serie: 0, 1, 2, 3, 4...
		"ts14TsdbQueryMem":  {"ts14tsdbmem", "test1", (time.Now().Truncate(time.Minute).UnixNano() / 1e+6) - (60 * 100), 200, 0.0, 1.0, 90},
		"ts14TsdbQueryMem2": {"ts14tsdbmem", "test2", 1448452800051, 200, 0.0, 1.0, 90},
	}

	for test, data := range cases {

		Points := make([]PointTsdbQueryMem, data.numTotal)

		hashMapStartTime[test] = data.startTime

		for i := 0; i < data.numTotal; i++ {
			Points[i].Value = data.value
			Points[i].Metric = data.metric
			Points[i].Tags = map[string]string{
				"ksid": keyspace,
				"host": data.tagValue,
			}
			Points[i].Timestamp = data.startTime
			data.value += data.iValue
			data.startTime += data.iStartTime
		}

		sendPointsGrafana(test, Points)

		hashMapMem[test] = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(data.metric, map[string]string{"host": data.tagValue})
	}
}

func ts10TsdbQueryMem(keyspace string) bool {

	metric := "ts10tsdbmem"
	tagKey := "host"
	tagKey2 := "app"
	tagValue := "test2"
	tagValue2 := "test1"
	tagValue3 := "app1"
	startTime := hashMapStartTime["ts1TsdbQueryMem"]
	var value, value2 float32
	const numTotal int = 75
	Points := [numTotal]PointTsdbQueryMem{}

	for i := 0; i < numTotal; i++ {
		Points[i].Value = value
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid": keyspace,
			tagKey: tagValue,
		}
		Points[i].Timestamp = int64(startTime)
		i++

		Points[i].Value = value2
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid": keyspace,

			tagKey:  tagValue2,
			tagKey2: tagValue3,
		}
		Points[i].Timestamp = int64(startTime)
		i++
		value2++

		Points[i].Value = value2
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid": keyspace,

			tagKey:  tagValue2,
			tagKey2: tagValue3,
		}
		Points[i].Timestamp = int64(startTime)

		startTime += 60
		value++
		value2++
	}

	jsonPoints, err := json.Marshal(Points)
	if err != nil {
		fmt.Println("ts10tsdb: ", err)
		return false
	}

	code, _, _ := mycenaeTools.HTTP.POST("api/put", jsonPoints)
	//time.Sleep(time.Second * 5)
	//code, _, _ = mycenaeTools.HTTP.POST("api/put", jsonPoints)

	if code == 204 {
		ts10_1IDTsdbQueryMem = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue2, tagKey2: tagValue3})
		ts10IDTsdbQueryMem = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue})
	} else {
		return false
	}

	return true
}

/*
TS12
serie: 1, 10, 100, 1000, 1000, 1,..,10000, 3000.
interval: 1 min
total: 12
*/
func ts12TsdbQueryMem(keyspace string) bool {

	metric := "ts12-_/.%&#;tsdb"
	tagKey := "hos-_/.%&#;t"
	tagValue := "test-_/.%&#;1"
	ts12TsdbQueryMemStartTime = time.Now().Truncate(time.Minute).Unix() - (60 * 100)
	startTime := ts12TsdbQueryMemStartTime
	var value float32 = 1.0
	const numTotal int = 12
	Points := [numTotal]PointTsdbQueryMem{}

	for i := 0; i < numTotal; i++ {

		if int64(startTime) < (ts12TsdbQueryMemStartTime + 300) {

			Points[i].Value = value
			Points[i].Metric = metric
			Points[i].Tags = map[string]string{
				"ksid": keyspace,
				tagKey: tagValue,
			}
			Points[i].Timestamp = int64(startTime)
			startTime += 60
			value = value * 10.0

		} else if int64(startTime) <= (ts12TsdbQueryMemStartTime + 300) {
			Points[i].Value = 1000.0
			Points[i].Metric = metric
			Points[i].Tags = map[string]string{
				"ksid": keyspace,
				tagKey: tagValue,
			}
			Points[i].Timestamp = int64(startTime)
			startTime += 60
			value = 1.0

		} else if int64(startTime) < (ts12TsdbQueryMemStartTime + 660) {
			Points[i].Value = value
			Points[i].Metric = metric
			Points[i].Tags = map[string]string{
				"ksid": keyspace,
				tagKey: tagValue,
			}
			Points[i].Timestamp = int64(startTime)
			startTime += 60
			value = value * 10.0

		} else {
			Points[i].Value = 3000.0
			Points[i].Metric = metric
			Points[i].Tags = map[string]string{
				"ksid": keyspace,
				tagKey: tagValue,
			}
			Points[i].Timestamp = int64(startTime)
			startTime += 60
			value = 1.0
		}
	}

	jsonPoints, err := json.Marshal(Points)
	if err != nil {
		fmt.Println("ts12tsdb: ", err)
		return false
	}

	code, _, _ := mycenaeTools.HTTP.POST("api/put", jsonPoints)
	//time.Sleep(time.Second * 5)
	//code, _, _ = mycenaeTools.HTTP.POST("api/put", jsonPoints)

	if code != 204 {
		return false
	}

	ts12IDTsdbQueryMem = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue})

	return true
}

func ts13TsdbQueryMem(keyspace string) bool {

	metric := "ts13tsdbmem"
	tagKey := "host"
	tagKey2 := "app"
	tagKey3 := "type"
	tagValue := "host1"
	tagValue2 := "app1"
	tagValue3 := "type1"
	tagValue4 := "app2"
	ts13TsdbQueryMemStartTime = time.Now().Unix() - (60 * 100)
	startTime := ts13TsdbQueryMemStartTime
	var value float32 = 1.0

	const numTotal int = 40
	Points := [numTotal]PointTsdbQueryMem{}

	for i := 0; i < numTotal; i++ {
		Points[i].Value = value
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid": keyspace,

			tagKey:  tagValue,
			tagKey2: tagValue2,
			tagKey3: tagValue3,
		}
		Points[i].Timestamp = int64(startTime)
		i++

		Points[i].Value = value * 2
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid": keyspace,

			tagKey:  tagValue,
			tagKey2: tagValue4,
		}
		Points[i].Timestamp = int64(startTime)
		startTime += 60
		value++

	}

	jsonPoints, err := json.Marshal(Points)

	if err != nil {
		fmt.Println("ts13tsdb: ", err)
		return false
	}

	code, _, _ := mycenaeTools.HTTP.POST("api/put", jsonPoints)
	if code == 204 {
		ts13IDTsdbQueryMem = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, tagKey2: tagValue2, tagKey3: tagValue3})
		ts13IDTsdbQueryMem2 = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, tagKey2: tagValue4})
	} else {
		return false
	}

	//time.Sleep(2 * time.Second)
	tagValue = "host2"
	tagValue3 = "type2"
	startTime = ts13TsdbQueryMemStartTime
	value = 1.0

	Points = [numTotal]PointTsdbQueryMem{}

	for i := 0; i < numTotal; i++ {
		Points[i].Value = value * 3
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid": keyspace,

			tagKey:  tagValue,
			tagKey2: tagValue2,
			tagKey3: tagValue3,
		}
		Points[i].Timestamp = int64(startTime)
		i++

		Points[i].Value = value * 4
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid": keyspace,

			tagKey:  tagValue,
			tagKey2: tagValue4,
		}
		Points[i].Timestamp = int64(startTime)
		startTime += 60
		value++
	}

	jsonPoints, err = json.Marshal(Points)
	if err != nil {
		fmt.Println("ts13tsdb: ", err)
		return false
	}

	code, _, _ = mycenaeTools.HTTP.POST("api/put", jsonPoints)
	if code == 204 {
		ts13IDTsdbQueryMem3 = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, tagKey2: tagValue2, tagKey3: tagValue3})
		ts13IDTsdbQueryMem4 = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, tagKey2: tagValue4})
	} else {
		return false
	}

	//time.Sleep(2 * time.Second)
	tagValue = "host3"
	tagValue3 = "type3"
	startTime = ts13TsdbQueryMemStartTime
	value = 1.0

	Points = [numTotal]PointTsdbQueryMem{}

	for i := 0; i < numTotal; i++ {
		Points[i].Value = value * 5
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid": keyspace,

			tagKey:  tagValue,
			tagKey2: tagValue2,
			tagKey3: tagValue3,
		}
		Points[i].Timestamp = int64(startTime)
		i++

		Points[i].Value = value * 6
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid": keyspace,

			tagKey:  tagValue,
			tagKey2: tagValue4,
		}
		Points[i].Timestamp = int64(startTime)
		startTime += 60
		value++

	}

	jsonPoints, err = json.Marshal(Points)
	if err != nil {
		fmt.Println("ts13tsdb: ", err)
		return false
	}

	code, _, _ = mycenaeTools.HTTP.POST("api/put", jsonPoints)
	if code == 204 {
		ts13IDTsdbQueryMem5 = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, tagKey2: tagValue2, tagKey3: tagValue3})
		ts13IDTsdbQueryMem6 = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, tagKey2: tagValue4})
	} else {
		return false
	}

	startTime = ts13TsdbQueryMemStartTime
	value = 1.0

	//time.Sleep(2 * time.Second)
	tagValue3 = "type4"
	tagValue5 := "type5"
	Points = [numTotal]PointTsdbQueryMem{}

	for i := 0; i < numTotal; i++ {
		Points[i].Value = value * 7
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid": keyspace,

			tagKey:  tagValue,
			tagKey3: tagValue3,
		}
		Points[i].Timestamp = int64(startTime)
		i++

		Points[i].Value = value * 8
		Points[i].Metric = metric
		Points[i].Tags = map[string]string{
			"ksid": keyspace,

			tagKey:  tagValue,
			tagKey3: tagValue5,
		}
		Points[i].Timestamp = int64(startTime)
		startTime += 60
		value++

	}

	jsonPoints, err = json.Marshal(Points)
	if err != nil {
		fmt.Println("ts13tsdb: ", err)
		return false
	}

	code, _, _ = mycenaeTools.HTTP.POST("api/put", jsonPoints)
	if code == 204 {
		ts13IDTsdbQueryMem7 = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, tagKey3: tagValue3})
		ts13IDTsdbQueryMem8 = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, map[string]string{tagKey: tagValue, tagKey3: tagValue5})
	} else {
		return false
	}
	return true

}

func ts16TsdbQueryMem(keyspace string) {

	startTime := time.Now().Unix()
	hashMapStartTime["ts16TsdbQueryMem"] = startTime
	var start, end sync.WaitGroup

	start.Add(1)
	end.Add(9)
	for i := 0; i < 9; i++ {

		go func(i int, startTime int64) {

			start.Wait()
			sendPointsGrafana("ts16TsdbQueryMem", []PointTsdbQueryMem{
				{
					Value:  float32(i),
					Metric: "ts16tsdbmem",
					Tags: map[string]string{
						"ksid": keyspace,
						"host": "test",
					},
					Timestamp: startTime,
				},
				{
					Value:  float32(i + 9),
					Metric: "ts16tsdbmem",
					Tags: map[string]string{
						"ksid": keyspace,
						"host": "test",
					},
					Timestamp: startTime + 540,
				},
				{
					Value:  float32(i + 18),
					Metric: "ts16tsdbmem",
					Tags: map[string]string{
						"ksid": keyspace,
						"host": "test",
					},
					Timestamp: startTime + 1080,
				},
			})
			end.Done()

		}(i, startTime)

		startTime += 60
	}

	start.Done()
	end.Wait()

	hashMapMem["ts16TsdbQueryMem"] = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags("ts16tsdbmem", map[string]string{"host": "test"})
}

func ts17TsdbQueryMem(keyspace string) {

	startTime := time.Now().Truncate(time.Hour).Unix()
	hashMapStartTime["ts17TsdbQueryMem"] = startTime

	sendPointsGrafana("ts17TsdbQueryMem", []PointTsdbQueryMem{
		{
			Value:  float32(0),
			Metric: "ts17tsdbmem",
			Tags: map[string]string{
				"ksid": keyspace,
				"host": "test",
			},
			Timestamp: startTime,
		},
		{
			Value:  float32(1),
			Metric: "ts17tsdbmem",
			Tags: map[string]string{
				"ksid": keyspace,
				"host": "test",
			},
			Timestamp: startTime + 60,
		},
		{
			Value:  float32(2),
			Metric: "ts17tsdbmem",
			Tags: map[string]string{
				"ksid": keyspace,
				"host": "test",
			},
			Timestamp: startTime + 120,
		},
	})

	hashMapMem["ts17TsdbQueryMem"] = mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags("ts17tsdbmem", map[string]string{"host": "test"})
}

func ts17bTsdbQueryMem(keyspace string) {

	startTime := hashMapStartTime["ts17TsdbQueryMem"] + 7200

	sendPointsGrafana("ts17bTsdbQueryMem", []PointTsdbQueryMem{
		{
			Value:  float32(3),
			Metric: "ts17tsdbmem",
			Tags: map[string]string{
				"ksid": keyspace,
				"host": "test",
			},
			Timestamp: startTime,
		},
		{
			Value:  float32(4),
			Metric: "ts17tsdbmem",
			Tags: map[string]string{
				"ksid": keyspace,
				"host": "test",
			},
			Timestamp: startTime + 60,
		},
		{
			Value:  float32(5),
			Metric: "ts17tsdbmem",
			Tags: map[string]string{
				"ksid": keyspace,
				"host": "test",
			},
			Timestamp: startTime + 120,
		},
	})
}

func sendPointsPointsGrafanaMem(keyspace string) {

	fmt.Println("Setting up pointsGrafanaMem_test.go tests...")

	tsInsertMem(keyspace)
	//ts6TsdbQueryMem()
	//ts8TsdbQueryMem()
	ts10TsdbQueryMem(keyspace)
	//ts11TsdbQueryMem()
	ts12TsdbQueryMem(keyspace)
	ts13TsdbQueryMem(keyspace)
	//ts15TsdbQueryMem()
	ts16TsdbQueryMem(keyspace)
	ts17TsdbQueryMem(keyspace)
}

func postAPIQueryAndCheckMem(t *testing.T, payload string, metric string, p, dps, tags, aggtags, tsuuidSize int, tsuuids ...string) ([]string, []PayloadTsdbQueryMem) {

	path := fmt.Sprintf("keyspaces/%s/api/query", ksMycenae)
	code, response, err := mycenaeTools.HTTP.POST(path, []byte(payload))
	if err != nil {
		t.Error(err, string(response))
		t.SkipNow()
	}

	payloadPoints := []PayloadTsdbQueryMem{}
	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err, string(response))
		t.SkipNow()
	}

	if len(payloadPoints) == 0 {
		t.Error("No points were found")
		t.SkipNow()
	}

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, metric, payloadPoints[0].Metric)
	assert.Equal(t, 200, code)
	assert.Equal(t, p, len(payloadPoints))
	assert.Equal(t, dps, len(payloadPoints[0].Dps))
	assert.Equal(t, tags, len(payloadPoints[0].Tags))
	assert.Equal(t, aggtags, len(payloadPoints[0].AggTags))
	assert.Equal(t, tsuuidSize, len(payloadPoints[0].Tsuuids))

	for _, name := range tsuuids {
		if _, ok := hashMapMem[name]; ok {
			assert.Contains(t, payloadPoints[0].Tsuuids, hashMapMem[name], "Tsuuid not found")
		} else {
			assert.Contains(t, payloadPoints[0].Tsuuids, name, "Tsuuid not found")
		}
	}

	return keys, payloadPoints
}

func TestTsdbQueryMemFilter(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "host",
        		"filter": "test",
        		"groupBy": false
	      	}]
	    }]
	}`, hashMapStartTime["ts1TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01tsdbmem", 1, 100, 1, 0, 1, "ts1TsdbQueryMem")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60
		i++
	}

}

func TestTsdbQueryMemFilterNoTsuids(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": false,
		"queries": [{
    		"metric": "ts01tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "host",
        		"filter": "test",
        		"groupBy": false
	      	}]
	    }]
	}`, hashMapStartTime["ts1TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01tsdbmem", 1, 100, 1, 0, 0)
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60
		i++
	}

}

func TestTsdbQueryMemFilterMsResolution(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"msResolution": true,
		"queries": [{
    		"metric": "ts01tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "host",
        		"filter": "test",
        		"groupBy": false
	      	}]
	    }]
	}`, hashMapStartTime["ts1TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01tsdbmem", 1, 100, 1, 0, 1, "ts1TsdbQueryMem")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1TsdbQueryMem"] * 1000
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60000
		i++
	}

}

//func TestTsdbQueryMemFilterPntsSameSecondSecResolution(t *testing.T) {
//
//	payload := fmt.Sprintf(`{
//		"start": %v,
//		"showTSUIDs": true,
//		"msResolution": false,
//		"queries": [
//    {
//    		"metric": "ts14tsdbmem",
//    		"aggregator": "sum",
//    		"filters": [
//        {
//        		"type": "regexp",
//        		"tagk": "host",
//        		"filter": ".*",
//        		"groupBy": false
//        }
//      ]
//    }
//  ]
//	}`, hashMapStartTime["ts14TsdbQueryMem"])
//
//	fmt.Println(payload)
//	fmt.Println(ksMycenae)
//
//	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
//
//	if err != nil {
//		t.Error(err)
//		t.SkipNow()
//	}
//
//	payloadPoints := []PayloadTsdbQueryMem{}
//
//	err = json.Unmarshal(response, &payloadPoints)
//
//	if err != nil {
//		t.Error(err)
//		t.SkipNow()
//	}
//
//	keys := []string{}
//
//	for key := range payloadPoints[0].Dps {
//		keys = append(keys, key)
//	}
//
//	sort.Strings(keys)
//
//	assert.Equal(t, 200, code, "they should be equal")
//	assert.Equal(t, 1, len(payloadPoints), "they should be equal")
//	assert.Equal(t, 18, len(payloadPoints[0].Dps), "they should be equal")
//	assert.Equal(t, 0, len(payloadPoints[0].Tags), "they should be equal")
//	assert.Equal(t, 1, len(payloadPoints[0].AggTags), "they should be equal")
//	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")
//	assert.Equal(t, 2, len(payloadPoints[0].Tsuuids), "they should be equal")
//	assert.Equal(t, "ts14tsdbmem", payloadPoints[0].Metric, "they should be equal")
//
//	if hashMapMem["ts14TsdbQueryMem"] == payloadPoints[0].Tsuuids[0] {
//		assert.Equal(t, hashMapMem["ts14TsdbQueryMem2"], payloadPoints[0].Tsuuids[1], "they should be equal")
//	} else {
//		assert.Equal(t, hashMapMem["ts14TsdbQueryMem"], payloadPoints[0].Tsuuids[1], "they should be equal")
//	}
//
//	i := 0.0
//	dateStart := hashMapStartTime["ts14TsdbQueryMem"]
//
//	for _, key := range keys {
//
//		assert.Exactly(t, ((i * 2) + ((i + 1) * 2) + ((i + 2) * 2) + ((i + 3) * 2) + ((i + 4) * 2)), payloadPoints[0].Dps[key], "they should be equal")
//		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
//
//		dateStart += 1
//		i += 5
//
//	}
//}

func TestTsdbQueryMemFilterDownsampleAvg(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts02tsdbmem",
    		"aggregator": "avg",
    		"downsample": "3m-avg",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "host",
        		"filter": "test",
        		"groupBy": false
	      	}]
	    }]
	}`, hashMapStartTime["ts2TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts02tsdbmem", 1, 30, 1, 0, 1, hashMapMem["ts2TsdbQueryMem3"])
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	var media float32

	dateStart := hashMapStartTime["ts2TsdbQueryMem"]
	for _, key := range keys {

		media = (i + i + 1 + i + 2) / 3
		assert.Exactly(t, media, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 180
		i++
	}

}

func TestTsdbQueryMemFilterDownsampleSec(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts02tsdbmem",
    		"aggregator": "min",
    		"downsample": "180s-min",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "host",
        		"filter": "test",
        		"groupBy": false
	      	}]
	    }]
	}`, hashMapStartTime["ts2TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts02tsdbmem", 1, 30, 1, 0, 1, hashMapMem["ts2TsdbQueryMem3"])
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts2TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 180
		i++
	}
}

func TestTsdbQueryMemFilterDownsampleMin(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts02tsdbmem",
    		"aggregator": "min",
    		"downsample": "3m-min",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "host",
        		"filter": "test",
        		"groupBy": false
	      	}]
	    }]
	}`, hashMapStartTime["ts2TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts02tsdbmem", 1, 30, 1, 0, 1, hashMapMem["ts2TsdbQueryMem3"])
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts2TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 180
		i++
	}
}

func TestTsdbQueryMemFilterDownsampleCount(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts02tsdbmem",
    		"aggregator": "min",
    		"downsample": "3m-count",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "host",
        		"filter": "test",
        		"groupBy": false
	      	}]
	    }]
	}`, hashMapStartTime["ts2TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts02tsdbmem", 1, 30, 1, 0, 1, hashMapMem["ts2TsdbQueryMem3"])
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32 = 3.0
	dateStart := hashMapStartTime["ts2TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 180

	}
}

func TestTsdbQueryMemFilterDownsampleMax(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts02tsdbmem",
    		"aggregator": "max",
    		"downsample": "3m-max",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "host",
        		"filter": "test",
        		"groupBy": false
	      	}]
	    }]
	}`, hashMapStartTime["ts2TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts02tsdbmem", 1, 30, 1, 0, 1, hashMapMem["ts2TsdbQueryMem3"])
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32 = 2.0
	dateStart := hashMapStartTime["ts2TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 180
		i++
	}
}

func TestTsdbQueryMemFilterDownsampleSum(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts02tsdbmem",
    		"aggregator": "sum",
    		"downsample": "3m-sum",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "host",
        		"filter": "test",
        		"groupBy": false
	      	}]
	    }]
	}`, hashMapStartTime["ts2TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts02tsdbmem", 1, 30, 1, 0, 1, hashMapMem["ts2TsdbQueryMem3"])
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	var sum float32

	dateStart := hashMapStartTime["ts2TsdbQueryMem"]
	for _, key := range keys {

		sum = i + i + 1 + i + 2
		assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 180
		i++
	}
}

func TestTsdbQueryMemFilterDownsampleCountSec(t *testing.T) {
	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts02tsdbmem",
    		"aggregator": "sum",
    		"downsample": "30s-count-null",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "host",
        		"filter": "test",
        		"groupBy": false
	      	}]
	    }]
	}`, hashMapStartTime["ts2TsdbQueryMem"], hashMapStartTime["ts2TsdbQueryMem"]+5400)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbNullQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	if len(payloadPoints) == 0 {
		t.Error("No points were found")
		t.SkipNow()
	}

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 1, len(payloadPoints), "they should be equal")
	assert.Equal(t, 180, len(payloadPoints[0].Dps), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[0].Tags), "they should be equal")
	assert.Equal(t, 0, len(payloadPoints[0].AggTags), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[0].Tsuuids), "they should be equal")
	assert.Equal(t, "ts02tsdbmem", payloadPoints[0].Metric, "they should be equal")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")
	assert.Equal(t, hashMapMem["ts2TsdbQueryMem3"], payloadPoints[0].Tsuuids[0], "they should be equal")

	dateStart := hashMapStartTime["ts2TsdbQueryMem"]
	var count float32
	for _, key := range keys {

		if count == 0 {
			assert.Exactly(t, 1.0, payloadPoints[0].Dps[key], "they should be equal")
			count = 1
		} else {
			assert.Exactly(t, nil, payloadPoints[0].Dps[key], "they should be equal")
			count = 0
		}

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 30
	}

}

func TestTsdbQueryMemFilterDownsampleMaxHour(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts03tsdbmem",
    		"aggregator": "max",
    		"downsample": "2h-max",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "host",
        		"filter": "test",
        		"groupBy": false
	      	}]
	    }]
	}`, hashMapStartTime["ts3TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts03tsdbmem", 1, 120, 1, 0, 1, hashMapMem["ts3TsdbQueryMem"])
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32 = 15.0
	dateStart := hashMapStartTime["ts3TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		i += 20.0

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 7200
	}

}

//func TestTsdbQueryMemFilterDownsampleMaxDay(t *testing.T) {
//
//	payload := fmt.Sprintf(`{
//		"start": %v,
//		"showTSUIDs": true,
//		"queries": [
//    {
//    		"metric": "ts03tsdbmem",
//    		"aggregator": "max",
//    		"downsample": "2d-max",
//    		"filters": [
//        {
//        		"type": "regexp",
//        		"tagk": "host",
//        		"filter": "test",
//        		"groupBy": false
//        }
//      ]
//    }
//  ]
//	}`, hashMapStartTime["ts3TsdbQueryMem"])
//
//	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
//
//	if err != nil {
//		t.Error(err)
//		t.SkipNow()
//	}
//
//	fmt.Println(ksMycenae)
//	fmt.Println(payload)
//
//	payloadPoints := []PayloadTsdbQueryMem{}
//
//	err = json.Unmarshal(response, &payloadPoints)
//
//	if err != nil {
//		t.Error(err)
//		t.SkipNow()
//	}
//
//	keys := []string{}
//
//	for key := range payloadPoints[0].Dps {
//		keys = append(keys, key)
//	}
//
//	sort.Strings(keys)
//
//	assert.Equal(t, 200, code, "they should be equal")
//	assert.Equal(t, 1, len(payloadPoints), "they should be equal")
//	assert.Equal(t, 6, len(payloadPoints[0].Dps), "they should be equal")
//	assert.Equal(t, 1, len(payloadPoints[0].Tags), "they should be equal")
//	assert.Equal(t, 0, len(payloadPoints[0].AggTags), "they should be equal")
//	assert.Equal(t, 1, len(payloadPoints[0].Tsuuids), "they should be equal")
//	assert.Equal(t, "ts03tsdbmem", payloadPoints[0].Metric, "they should be equal")
//	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")
//	assert.Equal(t, hashMapMem["ts3TsdbQueryMem"], payloadPoints[0].Tsuuids[0], "they should be equal")
//
//	i := 355.0
//	dateStart := hashMapStartTime["ts3TsdbQueryMem"]
//
//	for _, key := range keys {
//
//		date, _ := strconv.Atoi(key)
//
//		if int64(date) < (hashMapStartTime["ts3TsdbQueryMem"] - 691200) {
//
//			assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
//			i += 480.0
//
//		} else {
//
//			assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
//			i += 120.0
//		}
//
//		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
//		dateStart = dateStart + 172800
//	}
//}
//func TestTsdbQueryMemFilterDownsampleMaxWeek(t *testing.T) {
//
//	payload := fmt.Sprintf(`{
//		"start": %v,
//		"showTSUIDs": true,
//		"queries": [
//    {
//    		"metric": "ts04tsdbmem",
//    		"aggregator": "max",
//    		"downsample": "4w-max",
//    		"filters": [
//        {
//        		"type": "regexp",
//        		"tagk": "host",
//        		"filter": "test",
//        		"groupBy": false
//        }
//      ]
//    }
//  ]
//	}`, hashMapStartTime["ts4TsdbQueryMem"])
//	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
//
//	if err != nil {
//		t.Error(err)
//		t.SkipNow()
//	}
//
//	payloadPoints := []PayloadTsdbQueryMem{}
//
//	err = json.Unmarshal(response, &payloadPoints)
//
//	if err != nil {
//		t.Error(err)
//		t.SkipNow()
//	}
//
//	keys := []string{}
//
//	for key := range payloadPoints[0].Dps {
//		keys = append(keys, key)
//	}
//
//	sort.Strings(keys)
//
//	assert.Equal(t, 200, code, "they should be equal")
//	assert.Equal(t, 1, len(payloadPoints), "they should be equal")
//	assert.Equal(t, 52, len(payloadPoints[0].Dps), "they should be equal")
//	assert.Equal(t, 1, len(payloadPoints[0].Tags), "they should be equal")
//	assert.Equal(t, 0, len(payloadPoints[0].AggTags), "they should be equal")
//	assert.Equal(t, 1, len(payloadPoints[0].Tsuuids), "they should be equal")
//	assert.Equal(t, "ts04tsdbmem", payloadPoints[0].Metric, "they should be equal")
//	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")
//	assert.Equal(t, hashMapMem["ts4TsdbQueryMem"], payloadPoints[0].Tsuuids[0], "they should be equal")
//
//	i := 1.5
//	dateStart := time.Date(2015, 11, 23, 0, 0, 0, 0, time.UTC)
//
//	for _, key := range keys {
//
//		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
//		i += 2.0
//
//		date := int(dateStart.Unix())
//
//		assert.Exactly(t, strconv.Itoa(date), key, "they should be equal")
//		dateStart = dateStart.AddDate(0, 0, 28)
//	}
//}
//
//func TestTsdbQueryMemFilterDownsampleMaxMonth(t *testing.T) {
//
//	payload := `{
//		"start": 1448452740000,
//		"end": 1566734400000,
//		"showTSUIDs": true,
//		"queries": [
//    {
//    		"metric": "ts04tsdbmem",
//    		"aggregator": "max",
//    		"downsample": "3n-max",
//    		"filters": [
//        {
//        		"type": "regexp",
//        		"tagk": "host",
//        		"filter": "test",
//        		"groupBy": false
//        }
//      ]
//    }
//  ]
//	}`
//	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
//
//	if err != nil {
//		t.Error(err)
//		t.SkipNow()
//	}
//
//	payloadPoints := []PayloadTsdbQueryMem{}
//
//	err = json.Unmarshal(response, &payloadPoints)
//
//	if err != nil {
//		t.Error(err)
//		t.SkipNow()
//	}
//
//	keys := []string{}
//
//	for key := range payloadPoints[0].Dps {
//		keys = append(keys, key)
//	}
//
//	sort.Strings(keys)
//
//	assert.Equal(t, 200, code, "they should be equal")
//	assert.Equal(t, 1, len(payloadPoints), "they should be equal")
//	assert.Equal(t, 16, len(payloadPoints[0].Dps), "they should be equal")
//	assert.Equal(t, 1, len(payloadPoints[0].Tags), "they should be equal")
//	assert.Equal(t, 0, len(payloadPoints[0].AggTags), "they should be equal")
//	assert.Equal(t, 1, len(payloadPoints[0].Tsuuids), "they should be equal")
//	assert.Equal(t, "ts04tsdbmem", payloadPoints[0].Metric, "they should be equal")
//	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")
//	assert.Equal(t, hashMapMem["ts4TsdbQueryMem"], payloadPoints[0].Tsuuids[0], "they should be equal")
//
//	i := 4.5
//	dateStart := time.Date(2015, 11, 1, 0, 0, 0, 0, time.UTC)
//
//	for _, key := range keys {
//
//		date, _ := strconv.Atoi(key)
//
//		if date <= 1501545600 {
//
//			assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
//			i += 6.5
//
//		} else if date == 1509494400 {
//
//			i = 57.0
//			assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
//
//		} else if date == 1517443200 {
//
//			i = 63.0
//			assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
//
//		} else if date == 1525132800 {
//
//			i += 6.5
//			assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
//
//		} else if date == 1533081600 {
//
//			i += 7.0
//			assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
//
//		} else if date <= 1541030400 {
//
//			i += 6.5
//			assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
//
//		} else if date == 1548979200 {
//
//			i += 6.0
//			assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
//
//		} else if date == 1556668800 {
//
//			i += 7.0
//			assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
//
//		} else {
//			i += 1.5
//			assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
//		}
//
//		dateInt := int(dateStart.Unix())
//
//		assert.Exactly(t, strconv.Itoa(dateInt), key, "they should be equal")
//		dateStart = dateStart.AddDate(0, 3, 0)
//	}
//}
//
//func TestTsdbQueryMemFilterDownsampleMaxYear(t *testing.T) {
//
//	payload := `{
//		"start": 1448452740000,
//		"end": 1545825600000,
//		"showTSUIDs": true,
//		"queries": [
//    {
//    		"metric": "ts04tsdbmem",
//    		"aggregator": "max",
//    		"downsample": "1y-max",
//    		"filters": [
//        {
//        		"type": "regexp",
//        		"tagk": "host",
//        		"filter": "test",
//        		"groupBy": false
//        }
//      ]
//    }
//  ]
//	}`
//	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
//
//	if err != nil {
//		t.Error(err)
//		t.SkipNow()
//	}
//
//	payloadPoints := []PayloadTsdbQueryMem{}
//
//	err = json.Unmarshal(response, &payloadPoints)
//
//	if err != nil {
//		t.Error(err)
//		t.SkipNow()
//	}
//
//	keys := []string{}
//
//	for key := range payloadPoints[0].Dps {
//		keys = append(keys, key)
//	}
//
//	sort.Strings(keys)
//
//	assert.Equal(t, 200, code, "they should be equal")
//	assert.Equal(t, 1, len(payloadPoints), "they should be equal")
//	assert.Equal(t, 4, len(payloadPoints[0].Dps), "they should be equal")
//	assert.Equal(t, 1, len(payloadPoints[0].Tags), "they should be equal")
//	assert.Equal(t, 0, len(payloadPoints[0].AggTags), "they should be equal")
//	assert.Equal(t, 1, len(payloadPoints[0].Tsuuids), "they should be equal")
//	assert.Equal(t, "ts04tsdbmem", payloadPoints[0].Metric, "they should be equal")
//	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")
//	assert.Equal(t, hashMapMem["ts4TsdbQueryMem"], payloadPoints[0].Tsuuids[0], "they should be equal")
//
//	i := 2.5
//	dateStart := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
//
//	for _, key := range keys {
//
//		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
//		i += 26.0
//
//		date := int(dateStart.Unix())
//
//		assert.Exactly(t, strconv.Itoa(date), key, "they should be equal")
//		dateStart = dateStart.AddDate(1, 0, 0)
//	}
//}

func TestTsdbQueryMemFilterMoreThanOneTS(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "host",
        		"filter": "test",
        		"groupBy": false
        	}]
    	},{
    		"metric": "ts10tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "host",
        		"filter": "test2",
        		"groupBy": false
	      	}]
	    }]
	}`, hashMapStartTime["ts1TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01tsdbmem", 2, 100, 1, 0, 1, "ts1TsdbQueryMem")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60
		i++
	}

	keys = []string{}

	for key := range payloadPoints[1].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, 25, len(payloadPoints[1].Dps), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[1].Tags), "they should be equal")
	assert.Equal(t, 0, len(payloadPoints[1].AggTags), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[1].Tsuuids), "they should be equal")
	assert.Equal(t, "ts10tsdbmem", payloadPoints[1].Metric, "they should be equal")
	assert.Equal(t, "test2", payloadPoints[1].Tags["host"], "they should be equal")
	assert.Equal(t, ts10IDTsdbQueryMem, payloadPoints[1].Tsuuids[0], "they should be equal")

	i = 0.0
	dateStart = hashMapStartTime["ts1TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[1].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60
		i++
	}
}

func TestTsdbQueryMemFilterRegexpValidChars(t *testing.T) {

	payload := `{
		"start": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime, 10) + `,
		"end": ` + strconv.FormatInt((ts12TsdbQueryMemStartTime+240), 10) + `,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "hos-_/.%&#;t",
        		"filter": "test-_/\\.%\\&\\#;1",
        		"groupBy": false
	      	}]
	    }]
	}`

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts12-_/.%&#;tsdb", 1, 5, 1, 0, 1, ts12IDTsdbQueryMem)
	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"], "they should be equal")

	var i float32 = 1.0
	dateStart := ts12TsdbQueryMemStartTime
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60

		i = i * 10.0
	}
}

func TestTsdbQueryMemFilterWildcardValidChars(t *testing.T) {

	payload := `{
		"start": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime, 10) + `,
		"end": ` + strconv.FormatInt((ts12TsdbQueryMemStartTime+240), 10) + `,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "hos-_/.%&#;t",
        		"filter": "test-_/.%&#;1",
        		"groupBy": false
	      	}]
	    }]
	}`
	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts12-_/.%&#;tsdb", 1, 5, 1, 0, 1, ts12IDTsdbQueryMem)
	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"], "they should be equal")

	var i float32 = 1.0
	dateStart := ts12TsdbQueryMemStartTime
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60

		i = i * 10.0
	}
}

func TestTsdbQueryMemFilterLiteralOrValidChars(t *testing.T) {

	payload := `{
		"start": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime, 10) + `,
		"end": ` + strconv.FormatInt((ts12TsdbQueryMemStartTime+240), 10) + `,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "literal_or",
        		"tagk": "hos-_/.%&#;t",
        		"filter": "test-_/.%&#;1",
        		"groupBy": false
	      	}]
	    }]
	}`
	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts12-_/.%&#;tsdb", 1, 5, 1, 0, 1, ts12IDTsdbQueryMem)
	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"], "they should be equal")

	var i float32 = 1.0
	dateStart := ts12TsdbQueryMemStartTime
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60

		i = i * 10.0
	}
}

func TestTsdbQueryMemFilterNotLiteralOrValidChars(t *testing.T) {

	payload := `{
		"start": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime, 10) + `,
		"end": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime+240, 10) + `,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "not_literal_or",
        		"tagk": "hos-_/.%&#;t",
        		"filter": "test-_/.%&#;",
        		"groupBy": false
	      	}]
	    }]
	}`
	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts12-_/.%&#;tsdb", 1, 5, 1, 0, 1, ts12IDTsdbQueryMem)
	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"], "they should be equal")

	var i float32 = 1.0
	dateStart := ts12TsdbQueryMemStartTime
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60

		i = i * 10.0
	}
}

func TestTsdbQueryMemFilterRateTrueRateOptionsFalse(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01tsdbmem",
    		"rate": true,
    		"aggregator": "sum",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "host",
        		"filter": "test",
        		"groupBy": false
	      	}]
	    }]
	}`, hashMapStartTime["ts1TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01tsdbmem", 1, 99, 1, 0, 1, "ts1TsdbQueryMem")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1TsdbQueryMem"]
	for _, key := range keys {

		calc := (((i + 1.0) - i) / (float32((dateStart + 60) - dateStart)))
		assert.Exactly(t, calc, payloadPoints[0].Dps[key])

		dateStart += 60
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
		i++
	}

}

func TestTsdbQueryMemFilterRateTrueRateOptionsTrueCounter(t *testing.T) {

	payload := `{
		"start": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime, 10) + `,
		"end": ` + strconv.FormatInt((ts12TsdbQueryMemStartTime+240), 10) + `,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"rate": true,
    		"rateOptions": {
      			"counter": true
			},
    		"aggregator": "sum",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "hos-_/.%&#;t",
        		"filter": "test-_/\\.%\\&\\#;1",
        		"groupBy": false
	      	}]
	    }]
	}`
	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts12-_/.%&#;tsdb", 1, 4, 1, 0, 1, ts12IDTsdbQueryMem)
	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"], "they should be equal")

	var i float32 = 1.0
	dateStart := ts12TsdbQueryMemStartTime
	for _, key := range keys {

		calc := (((i * 10.0) - i) / (float32((dateStart + 60) - dateStart)))
		assert.Exactly(t, calc, payloadPoints[0].Dps[key])

		dateStart = dateStart + 60
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")

		i = i * 10.0
	}
}

func TestTsdbQueryMemFilterRateTrueRateOptionsTrueCounterMax(t *testing.T) {

	payload := `{
		"start": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime, 10) + `,
		"end": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime+300, 10) + `,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"rate": true,
    		"rateOptions": {
      			"counter": true,
      			"counterMax": 15000
      		},
    		"aggregator": "sum",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "hos-_/.%&#;t",
        		"filter": "test-_/\\.%\\&\\#;1",
        		"groupBy": false
	      	}]
	    }]
	}`
	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts12-_/.%&#;tsdb", 1, 5, 1, 0, 1, ts12IDTsdbQueryMem)
	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"], "they should be equal")

	var i float32 = 1.0
	dateStart := ts12TsdbQueryMemStartTime
	var countermax float32 = 15000.0

	for _, key := range keys {

		if dateStart < (ts12TsdbQueryMemStartTime + 240) {

			calc := (((i * 10.0) - i) / (float32((dateStart + 60) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart += 60
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)

			i = i * 10.0
		} else {
			calc := ((countermax - i + 1000) / (float32((dateStart + 60) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart += 60
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
		}
	}
}

func TestTsdbQueryMemFilterRateTrueRateOptionsTrueResetValue(t *testing.T) {

	payload := `{
		"start": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime, 10) + `,
		"end": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime+300, 10) + `,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"rate": true,
    		"rateOptions": {
      			"counter": true,
      			"resetValue": 10
      		},
    		"aggregator": "sum",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "hos-_/.%&#;t",
        		"filter": "test-_/\\.%\\&\\#;1",
        		"groupBy": false
	      	}]
	    }]
	}`
	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts12-_/.%&#;tsdb", 1, 5, 1, 0, 1, ts12IDTsdbQueryMem)
	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"], "they should be equal")

	var i float32 = 1.0
	dateStart := ts12TsdbQueryMemStartTime
	for _, key := range keys {

		if dateStart < (ts12TsdbQueryMemStartTime + 240) {

			calc := (((i * 10.0) - i) / (float32((dateStart + 60) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart = dateStart + 60
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")

			i = i * 10.0
		} else {
			var calc float32
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart = dateStart + 60
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		}
	}
}

func TestTsdbQueryMemFilterRateTrueRateOptionsTrueCounterMaxAndResetValue(t *testing.T) {

	payload := `{
		"start": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime, 10) + `,
		"end": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime+720, 10) + `,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"rate": true,
    		"rateOptions": {
      			"counter": true,
      			"counterMax": 15000,
				"resetValue": 233
      		},
    		"aggregator": "sum",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "hos-_/.%&#;t",
        		"filter": "test-_/\\.%\\&\\#;1",
        		"groupBy": false
	      	}]
	    }]
	}`
	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts12-_/.%&#;tsdb", 1, 11, 1, 0, 1, ts12IDTsdbQueryMem)
	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"], "they should be equal")

	var i float32 = 1.0
	dateStart := ts12TsdbQueryMemStartTime
	var countermax float32 = 15000.0
	for _, key := range keys {

		if dateStart < (ts12TsdbQueryMemStartTime + 240) {

			calc := (((i * 10.0) - i) / (float32((dateStart + 60) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart = dateStart + 60
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")

			i = i * 10.0
		} else if dateStart < (ts12TsdbQueryMemStartTime + 300) {
			calc := ((countermax - i + 1000) / (float32((dateStart + 60) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart = dateStart + 60
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")

		} else if dateStart < (ts12TsdbQueryMemStartTime + 360) {
			var calc float32
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart = dateStart + 60
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			i = 1.0

		} else if dateStart < (ts12TsdbQueryMemStartTime + 600) {
			calc := (((i * 10.0) - i) / (float32((dateStart + 60) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart = dateStart + 60
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")

			i = i * 10.0
		} else {
			calc := ((countermax - i + 3000.0) / (float32((dateStart + 60) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart = dateStart + 60
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		}
	}
}

func TestTsdbQueryMemFilterRateTrueNoPoints(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01tsdbmem",
    		"rate": true,
    		"aggregator": "sum",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "host",
        		"filter": "test1",
        		"groupBy": false
	      	}]
	    }]
	}`, hashMapStartTime["ts1TsdbQueryMem"])

	code, response, _ := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, "[]", string(response), "they should be equal")

}

// Tags

func TestTsdbQueryMem(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01tsdbmem",
    		"aggregator": "sum",
    		"tags": {
      			"host": "test"
			}
		}]
	}`, hashMapStartTime["ts1TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01tsdbmem", 1, 100, 1, 0, 1, "ts1TsdbQueryMem")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60
		i++
	}

}

func TestTsdbQueryMemAproxMediaExactBeginAndEnd(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts02tsdbmem",
    		"aggregator": "avg",
    		"downsample": "3m-avg",
    		"tags": {
      			"host": "test"
			}
		}]
	}`, hashMapStartTime["ts2TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts02tsdbmem", 1, 30, 1, 0, 1, hashMapMem["ts2TsdbQueryMem3"])
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	var media float32
	dateStart := hashMapStartTime["ts2TsdbQueryMem"]
	for _, key := range keys {

		media = (i + i + 1 + i + 2) / 3
		assert.Exactly(t, media, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 180
		i++
	}

}

func TestTsdbQueryMemAproxMin(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts02tsdbmem",
    		"aggregator": "min",
    		"downsample": "3m-min",
    		"tags": {
      			"host": "test"
			}
		}]
	}`, hashMapStartTime["ts2TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts02tsdbmem", 1, 30, 1, 0, 1, hashMapMem["ts2TsdbQueryMem3"])
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts2TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 180
		i++
	}
}

func TestTsdbQueryMemAproxMax(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts02tsdbmem",
    		"aggregator": "max",
    		"downsample": "3m-max",
    		"tags": {
      			"host": "test"
			}
		}]
	}`, hashMapStartTime["ts2TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts02tsdbmem", 1, 30, 1, 0, 1, hashMapMem["ts2TsdbQueryMem3"])
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32 = 2.0
	dateStart := hashMapStartTime["ts2TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 180
		i++
	}
}

func TestTsdbQueryMemAproxSum(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts02tsdbmem",
    		"aggregator": "sum",
    		"downsample": "3m-sum",
    		"tags": {
      			"host": "test"
			}
		}]
	}`, hashMapStartTime["ts2TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts02tsdbmem", 1, 30, 1, 0, 1, hashMapMem["ts2TsdbQueryMem3"])
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	var sum float32
	dateStart := hashMapStartTime["ts2TsdbQueryMem"]
	for _, key := range keys {

		sum = i + i + 1 + i + 2
		assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 180
		i++
	}
}

func TestTsdbQueryMemAproxCount(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts02tsdbmem",
    		"aggregator": "sum",
    		"downsample": "3m-count",
    		"tags": {
      			"host": "test"
			}
		}]
	}`, hashMapStartTime["ts2TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts02tsdbmem", 1, 30, 1, 0, 1, hashMapMem["ts2TsdbQueryMem3"])
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32 = 3.0
	dateStart := hashMapStartTime["ts2TsdbQueryMem"]
	for _, key := range keys {
		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 180
	}
}

func TestTsdbQueryMemAproxMaxHour(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts03tsdbmem",
    		"aggregator": "max",
    		"downsample": "2h-max",
    		"tags": {
      			"host": "test"
			}
		}]
	}`, hashMapStartTime["ts3TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts03tsdbmem", 1, 120, 1, 0, 1, hashMapMem["ts3TsdbQueryMem"])
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32 = 15.0
	dateStart := hashMapStartTime["ts3TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		i += 20.0

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 7200
	}

}

func TestTsdbQueryMemMergeDateLimit(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01_1tsdbmem",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts1_1TsdbQueryMem2"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01_1tsdbmem", 1, 100, 0, 1, 2, hashMapMem["ts1_1TsdbQueryMem"], hashMapMem["ts1_1TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1_1TsdbQueryMem2"]

	for _, key := range keys {

		assert.Exactly(t, i+i, payloadPoints[0].Dps[key], "they should be equal")
		i++

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 60
	}
}

func TestTsdbQueryMemMergePtsSameDate(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts05tsdbmem",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts5TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts05tsdbmem", 1, 90, 0, 1, 3)
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	if hashMapMem["ts5TsdbQueryMem"] == payloadPoints[0].Tsuuids[0] {
		assert.Equal(t, hashMapMem["ts5TsdbQueryMem"], payloadPoints[0].Tsuuids[0], "they should be equal")

		if hashMapMem["ts5TsdbQueryMem2"] == payloadPoints[0].Tsuuids[1] {
			assert.Equal(t, hashMapMem["ts5TsdbQueryMem2"], payloadPoints[0].Tsuuids[1], "they should be equal")
			assert.Equal(t, hashMapMem["ts5TsdbQueryMem3"], payloadPoints[0].Tsuuids[2], "they should be equal")
		} else {
			assert.Equal(t, hashMapMem["ts5TsdbQueryMem2"], payloadPoints[0].Tsuuids[2], "they should be equal")
			assert.Equal(t, hashMapMem["ts5TsdbQueryMem3"], payloadPoints[0].Tsuuids[1], "they should be equal")
		}

	} else if hashMapMem["ts5TsdbQueryMem"] == payloadPoints[0].Tsuuids[1] {
		assert.Equal(t, hashMapMem["ts5TsdbQueryMem"], payloadPoints[0].Tsuuids[1], "they should be equal")

		if hashMapMem["ts5TsdbQueryMem2"] == payloadPoints[0].Tsuuids[0] {
			assert.Equal(t, hashMapMem["ts5TsdbQueryMem2"], payloadPoints[0].Tsuuids[0], "they should be equal")
			assert.Equal(t, hashMapMem["ts5TsdbQueryMem3"], payloadPoints[0].Tsuuids[2], "they should be equal")
		} else {
			assert.Equal(t, hashMapMem["ts5TsdbQueryMem2"], payloadPoints[0].Tsuuids[2], "they should be equal")
			assert.Equal(t, hashMapMem["ts5TsdbQueryMem3"], payloadPoints[0].Tsuuids[0], "they should be equal")
		}

	} else {
		assert.Equal(t, hashMapMem["ts5TsdbQueryMem"], payloadPoints[0].Tsuuids[2], "they should be equal")

		if hashMapMem["ts5TsdbQueryMem2"] == payloadPoints[0].Tsuuids[0] {
			assert.Equal(t, hashMapMem["ts5TsdbQueryMem2"], payloadPoints[0].Tsuuids[0], "they should be equal")
			assert.Equal(t, hashMapMem["ts5TsdbQueryMem3"], payloadPoints[0].Tsuuids[1], "they should be equal")
		} else {
			assert.Equal(t, hashMapMem["ts5TsdbQueryMem2"], payloadPoints[0].Tsuuids[1], "they should be equal")
			assert.Equal(t, hashMapMem["ts5TsdbQueryMem3"], payloadPoints[0].Tsuuids[0], "they should be equal")
		}
	}

	var i float32
	dateStart := hashMapStartTime["ts5TsdbQueryMem"]

	for _, key := range keys {

		assert.Exactly(t, i*2, payloadPoints[0].Dps[key], "they should be equal")
		i++

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 60
	}
}

func TestTsdbQueryMemMergeAvg(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07tsdbmem",
    		"aggregator": "avg"
    	}]
	}`, hashMapStartTime["ts7TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07tsdbmem", 1, 90, 0, 1, 2, hashMapMem["ts7TsdbQueryMem"], hashMapMem["ts7TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts7TsdbQueryMem"]

	for _, key := range keys {

		avg := (i + j) / 2
		assert.Exactly(t, avg, payloadPoints[0].Dps[key], "they should be equal")
		i++
		j += 5

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 60
	}
}

func TestTsdbQueryMemMergeMin(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07tsdbmem",
    		"aggregator": "min"
    	}]
	}`, hashMapStartTime["ts7TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07tsdbmem", 1, 90, 0, 1, 2, hashMapMem["ts7TsdbQueryMem"], hashMapMem["ts7TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts7TsdbQueryMem"]

	for _, key := range keys {
		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		i++

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 60
	}
}

func TestTsdbQueryMemMergeMax(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07tsdbmem",
    		"aggregator": "max"
    	}]
	}`, hashMapStartTime["ts7TsdbQueryMem"])

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	if len(payloadPoints) == 0 {
		t.Error("No points were found")
		t.SkipNow()
	}

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 1, len(payloadPoints), "they should be equal")
	assert.Equal(t, 90, len(payloadPoints[0].Dps), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[0].AggTags), "they should be equal")
	assert.Equal(t, 2, len(payloadPoints[0].Tsuuids), "they should be equal")
	assert.Equal(t, "ts07tsdbmem", payloadPoints[0].Metric, "they should be equal")
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	if hashMapMem["ts7TsdbQueryMem"] == payloadPoints[0].Tsuuids[0] {
		assert.Equal(t, hashMapMem["ts7TsdbQueryMem"], payloadPoints[0].Tsuuids[0], "they should be equal")
		assert.Equal(t, hashMapMem["ts7TsdbQueryMem2"], payloadPoints[0].Tsuuids[1], "they should be equal")
	} else {
		assert.Equal(t, hashMapMem["ts7TsdbQueryMem"], payloadPoints[0].Tsuuids[1], "they should be equal")
		assert.Equal(t, hashMapMem["ts7TsdbQueryMem2"], payloadPoints[0].Tsuuids[0], "they should be equal")
	}

	var i float32
	dateStart := hashMapStartTime["ts7TsdbQueryMem"]

	for _, key := range keys {
		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		i += 5.0

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 60
	}
}

func TestTsdbQueryMemMergeSum(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07tsdbmem",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts7TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07tsdbmem", 1, 90, 0, 1, 2, hashMapMem["ts7TsdbQueryMem"], hashMapMem["ts7TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	var sum float32
	dateStart := hashMapStartTime["ts7TsdbQueryMem"]

	for _, key := range keys {

		sum = i + j
		assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")
		i++
		j += 5

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 60
	}
}

func TestTsdbQueryMemMergeCount(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07tsdbmem",
    		"aggregator": "count"
    	}]
	}`, hashMapStartTime["ts7TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07tsdbmem", 1, 90, 0, 1, 2, hashMapMem["ts7TsdbQueryMem"], hashMapMem["ts7TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i float32 = 2.0
	dateStart := hashMapStartTime["ts7TsdbQueryMem"]

	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 60
	}
}

func TestTsdbQueryMemMergeSumDownAvgMinute(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07tsdbmem",
    		"downsample": "3m-avg",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts7TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07tsdbmem", 1, 30, 0, 1, 2, hashMapMem["ts7TsdbQueryMem"], hashMapMem["ts7TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts7TsdbQueryMem"]

	for _, key := range keys {

		sum := ((i+i+1+i+2)/3 + (j+j+5+j+10)/3)
		assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")
		i += 3
		j += 15

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 180
	}
}

func TestTsdbQueryMemMergeSumDownSumMinute(t *testing.T) {
	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07tsdbmem",
    		"downsample": "3m-sum",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts7TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07tsdbmem", 1, 30, 0, 1, 2, hashMapMem["ts7TsdbQueryMem"], hashMapMem["ts7TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts7TsdbQueryMem"]

	for _, key := range keys {

		sum := ((i + i + 1 + i + 2) + (j + j + 5 + j + 10))
		assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")
		i += 3
		j += 15

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 180
	}
}

func TestTsdbQueryMemMergeTimeDiff(t *testing.T) {
	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01_2tsdbmem",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts1_2TsdbQueryMem"]-30, hashMapStartTime["ts1_2TsdbQueryMem"]+6000-30)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01_2tsdbmem", 1, 200, 0, 1, 2, hashMapMem["ts1_2TsdbQueryMem"], hashMapMem["ts1_2TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1_2TsdbQueryMem"] - 30
	count := 0.0
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		count++

		if count == 2 {
			i++
			count = 0.0
		}

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 30
	}

}

func TestTsdbQueryMemMergeTimeDiffDownsampleExactBeginAndEnd(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01_2tsdbmem",
    		"downsample": "3m-min",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts1_2TsdbQueryMem"]-30, hashMapStartTime["ts1_2TsdbQueryMem"]+6000)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01_2tsdbmem", 1, 34, 0, 1, 2, hashMapMem["ts1_2TsdbQueryMem"], hashMapMem["ts1_2TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1_2TsdbQueryMem"] - 30
	for _, key := range keys {

		assert.Exactly(t, i+i, payloadPoints[0].Dps[key], "they should be equal")
		i += 3

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 180
	}
}

func TestTsdbQueryMemNullValuesDownsample(t *testing.T) {
	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts09tsdbmem",
    		"downsample": "3m-sum",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts9TsdbQueryMem"], hashMapStartTime["ts9TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts09tsdbmem", 1, 10, 1, 0, 1)
	assert.Equal(t, hashMapMem["ts9TsdbQueryMem"], payloadPoints[0].Tsuuids[0], "they should be equal")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts9TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i > 15 {
			sum := (i) + (i + 1) + (i + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key])

			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
			dateStart += 180

		} else {
			i = 75
			sum := (i) + (i + 1) + (i + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key])

			dateStart += 180 * 20
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
			dateStart += 180
		}
		i += 3
	}
}

func TestTsdbQueryMemNullValuesDownsampleNone(t *testing.T) {
	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts09tsdbmem",
    		"downsample": "3m-sum-none",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts9TsdbQueryMem"], hashMapStartTime["ts9TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts09tsdbmem", 1, 10, 1, 0, 1)
	assert.Equal(t, hashMapMem["ts9TsdbQueryMem"], payloadPoints[0].Tsuuids[0], "they should be equal")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts9TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i > 15 {
			sum := (i) + (i + 1) + (i + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key])

			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
			dateStart += 180

		} else {
			i = 75
			sum := (i) + (i + 1) + (i + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key])

			dateStart += 180 * 20
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
			dateStart += 180
		}
		i += 3
	}
}

func TestTsdbQueryMemNullValuesDownsampleNull(t *testing.T) {
	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts09tsdbmem",
    		"downsample": "3m-sum-null",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts9TsdbQueryMem"], hashMapStartTime["ts9TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts09tsdbmem", 1, 30, 1, 0, 1)
	assert.Equal(t, hashMapMem["ts9TsdbQueryMem"], payloadPoints[0].Tsuuids[0], "they should be equal")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts9TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := (i) + (i + 1) + (i + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			dateStart += 180

		} else {
			assert.Empty(t, payloadPoints[0].Dps[key], "they should be nil")
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			dateStart += 180
		}

		i += 3
	}
}

func TestTsdbQueryMemNullValuesDownsampleNan(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts09tsdbmem",
    		"downsample": "3m-sum-nan",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts9TsdbQueryMem"], hashMapStartTime["ts9TsdbQueryMem"]+5350)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbNullQueryMem{}
	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	if len(payloadPoints) == 0 {
		t.Error("No points were found")
		t.SkipNow()
	}

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 1, len(payloadPoints), "they should be equal")
	assert.Equal(t, 30, len(payloadPoints[0].Dps), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[0].Tags), "they should be equal")
	assert.Equal(t, 0, len(payloadPoints[0].AggTags), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[0].Tsuuids), "they should be equal")
	assert.Equal(t, "ts09tsdbmem", payloadPoints[0].Metric, "they should be equal")
	assert.Equal(t, hashMapMem["ts9TsdbQueryMem"], payloadPoints[0].Tsuuids[0], "they should be equal")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	i := 0.0
	dateStart := hashMapStartTime["ts9TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := (i) + (i + 1) + (i + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			dateStart += 180

		} else {
			assert.Exactly(t, "NaN", payloadPoints[0].Dps[key], "they should be nan")
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			dateStart += 180
		}

		i += 3
	}
}

func TestTsdbQueryMemNullValuesDownsampleZero(t *testing.T) {
	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts09tsdbmem",
    		"downsample": "3m-sum-zero",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts9TsdbQueryMem"], hashMapStartTime["ts9TsdbQueryMem"]+5350)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbNullQueryMem{}
	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	if len(payloadPoints) == 0 {
		t.Error("No points were found")
		t.SkipNow()
	}

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 1, len(payloadPoints), "they should be equal")
	assert.Equal(t, 30, len(payloadPoints[0].Dps), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[0].Tags), "they should be equal")
	assert.Equal(t, 0, len(payloadPoints[0].AggTags), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[0].Tsuuids), "they should be equal")
	assert.Equal(t, "ts09tsdbmem", payloadPoints[0].Metric, "they should be equal")
	assert.Equal(t, hashMapMem["ts9TsdbQueryMem"], payloadPoints[0].Tsuuids[0], "they should be equal")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	i := 0.0
	dateStart := hashMapStartTime["ts9TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := (i) + (i + 1) + (i + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			dateStart += 180

		} else {
			assert.Exactly(t, 0.0, payloadPoints[0].Dps[key], "they should be nil")
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			dateStart += 180
		}

		i += 3
	}
}

func TestTsdbQueryMemNullValueMerge(t *testing.T) {
	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_1tsdbmem",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts7_1TsdbQueryMem"], hashMapStartTime["ts7_1TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07_1tsdbmem", 1, 90, 0, 1, 2, hashMapMem["ts7_1TsdbQueryMem"], hashMapMem["ts7_1TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts7_1TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := i + j
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

		} else {
			assert.Exactly(t, j, payloadPoints[0].Dps[key], "they should be equal")

		}
		i++
		j++

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 60
	}
}

func TestTsdbQueryMemBothValuesNullMerge(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts09_1tsdbmem",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts9_1TsdbQueryMem"], hashMapStartTime["ts9_1TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts09_1tsdbmem", 1, 30, 0, 1, 2, hashMapMem["ts9_1TsdbQueryMem2"], hashMapMem["ts9_1TsdbQueryMem4"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts9_1TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i > 15 {
			sum := i + j
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			dateStart += 60

		} else {
			i = 75
			j = 75
			sum := i + j
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

			dateStart += 180 * 20
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			dateStart += 60
		}

		i++
		j++
	}
}

func TestTsdbQueryMemBothValuesNullMergeAndDownsample(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts09_1tsdbmem",
    		"downsample": "3m-max",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts9_1TsdbQueryMem"], hashMapStartTime["ts9_1TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts09_1tsdbmem", 1, 10, 0, 1, 2, hashMapMem["ts9_1TsdbQueryMem2"], hashMapMem["ts9_1TsdbQueryMem4"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts9_1TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i > 15 {
			sum := i + 2 + j + 2
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			dateStart += 180

		} else {
			i = 75
			j = 75
			sum := i + 2 + j + 2
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

			dateStart += 180 * 20
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			dateStart += 180
		}

		i += 3
		j += 3
	}
}

func TestTsdbQueryMemBothValuesNullMergeAndDownsampleNone(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts09_1tsdbmem",
    		"downsample": "3m-max-none",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts9_1TsdbQueryMem"], hashMapStartTime["ts9_1TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts09_1tsdbmem", 1, 10, 0, 1, 2, hashMapMem["ts9_1TsdbQueryMem2"], hashMapMem["ts9_1TsdbQueryMem4"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts9_1TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i > 15 {
			sum := i + 2 + j + 2
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			dateStart += 180

		} else {
			i = 75
			j = 75
			sum := i + 2 + j + 2
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

			dateStart += 180 * 20
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			dateStart += 180
		}

		i += 3
		j += 3
	}
}

func TestTsdbQueryMemBothValuesNullMergeAndDownsampleNull(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts09_1tsdbmem",
    		"downsample": "3m-max-null",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts9_1TsdbQueryMem"], hashMapStartTime["ts9_1TsdbQueryMem"]+5350)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbNullQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	if len(payloadPoints) == 0 {
		t.Error("No points were found")
		t.SkipNow()
	}

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 1, len(payloadPoints), "they should be equal")
	assert.Equal(t, 30, len(payloadPoints[0].Dps), "they should be equal")
	assert.Equal(t, 0, len(payloadPoints[0].Tags), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[0].AggTags), "they should be equal")
	assert.Equal(t, 2, len(payloadPoints[0].Tsuuids), "they should be equal")
	assert.Equal(t, "ts09_1tsdbmem", payloadPoints[0].Metric, "they should be equal")
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	if hashMapMem["ts9_1TsdbQueryMem2"] == payloadPoints[0].Tsuuids[0] {
		assert.Equal(t, hashMapMem["ts9_1TsdbQueryMem2"], payloadPoints[0].Tsuuids[0], "they should be equal")
		assert.Equal(t, hashMapMem["ts9_1TsdbQueryMem4"], payloadPoints[0].Tsuuids[1], "they should be equal")
	} else {
		assert.Equal(t, hashMapMem["ts9_1TsdbQueryMem2"], payloadPoints[0].Tsuuids[1], "they should be equal")
		assert.Equal(t, hashMapMem["ts9_1TsdbQueryMem4"], payloadPoints[0].Tsuuids[0], "they should be equal")
	}

	i, j := 0.0, 0.0
	dateStart := hashMapStartTime["ts9_1TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := i + 2 + j + 2
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			dateStart += 180

		} else {
			assert.Exactly(t, nil, payloadPoints[0].Dps[key], "they should be nan")
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			dateStart += 180
		}
		i += 3
		j += 3
	}
}

func TestTsdbQueryMemBothValuesNullMergeAndDownsampleNan(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts09_1tsdbmem",
    		"downsample": "3m-max-nan",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts9_1TsdbQueryMem"], hashMapStartTime["ts9_1TsdbQueryMem"]+5350)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbNullQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	if len(payloadPoints) == 0 {
		t.Error("No points were found")
		t.SkipNow()
	}

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 1, len(payloadPoints), "they should be equal")
	assert.Equal(t, 30, len(payloadPoints[0].Dps), "they should be equal")
	assert.Equal(t, 0, len(payloadPoints[0].Tags), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[0].AggTags), "they should be equal")
	assert.Equal(t, 2, len(payloadPoints[0].Tsuuids), "they should be equal")
	assert.Equal(t, "ts09_1tsdbmem", payloadPoints[0].Metric, "they should be equal")
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	if hashMapMem["ts9_1TsdbQueryMem2"] == payloadPoints[0].Tsuuids[0] {
		assert.Equal(t, hashMapMem["ts9_1TsdbQueryMem2"], payloadPoints[0].Tsuuids[0], "they should be equal")
		assert.Equal(t, hashMapMem["ts9_1TsdbQueryMem4"], payloadPoints[0].Tsuuids[1], "they should be equal")
	} else {
		assert.Equal(t, hashMapMem["ts9_1TsdbQueryMem2"], payloadPoints[0].Tsuuids[1], "they should be equal")
		assert.Equal(t, hashMapMem["ts9_1TsdbQueryMem4"], payloadPoints[0].Tsuuids[0], "they should be equal")
	}

	i, j := 0.0, 0.0
	dateStart := hashMapStartTime["ts9_1TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := i + 2 + j + 2
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			dateStart += 180

		} else {
			assert.Exactly(t, "NaN", payloadPoints[0].Dps[key], "they should be nan")
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			dateStart += 180
		}
		i += 3
		j += 3
	}
}

func TestTsdbQueryMemBothValuesNullMergeAndDownsampleZero(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts09_1tsdbmem",
    		"downsample": "3m-max-zero",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts9_1TsdbQueryMem"], hashMapStartTime["ts9_1TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts09_1tsdbmem", 1, 30, 0, 1, 2, hashMapMem["ts9_1TsdbQueryMem2"], hashMapMem["ts9_1TsdbQueryMem4"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts9_1TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := i + 2 + j + 2
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			dateStart += 180

		} else {
			assert.Exactly(t, float32(0.0), payloadPoints[0].Dps[key], "they should be nil")
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
			dateStart += 180
		}
		i += 3
		j += 3
	}
}

func TestTsdbQueryMemNullValueMergeAndDownsample(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_1tsdbmem",
    		"downsample": "3m-sum",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts7_1TsdbQueryMem"], hashMapStartTime["ts7_1TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07_1tsdbmem", 1, 30, 0, 1, 2, hashMapMem["ts7_1TsdbQueryMem"], hashMapMem["ts7_1TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts7_1TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := (i + i + 1 + i + 2) + (j + j + 1 + j + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

		} else {
			sum := (i + i + 1 + i + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

		}
		i += 3
		j += 3

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 180
	}
}

func TestTsdbQueryMemNullValueMergeAndDownsampleNone(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_1tsdbmem",
    		"downsample": "3m-sum-none",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts7_1TsdbQueryMem"], hashMapStartTime["ts7_1TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07_1tsdbmem", 1, 30, 0, 1, 2, hashMapMem["ts7_1TsdbQueryMem"], hashMapMem["ts7_1TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts7_1TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := (i + i + 1 + i + 2) + (j + j + 1 + j + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

		} else {
			sum := (i + i + 1 + i + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

		}
		i += 3
		j += 3

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 180
	}
}

func TestTsdbQueryMemNullValueMergeAndDownsampleNull(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_1tsdbmem",
    		"downsample": "3m-sum-null",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts7_1TsdbQueryMem"], hashMapStartTime["ts7_1TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07_1tsdbmem", 1, 30, 0, 1, 2, hashMapMem["ts7_1TsdbQueryMem"], hashMapMem["ts7_1TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts7_1TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := (i + i + 1 + i + 2) + (j + j + 1 + j + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

		} else {
			sum := (i + i + 1 + i + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

		}
		i += 3
		j += 3

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 180
	}
}

func TestTsdbQueryMemNullValueMergeAndDownsampleNan(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_1tsdbmem",
    		"downsample": "3m-sum-nan",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts7_1TsdbQueryMem"], hashMapStartTime["ts7_1TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07_1tsdbmem", 1, 30, 0, 1, 2, hashMapMem["ts7_1TsdbQueryMem"], hashMapMem["ts7_1TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts7_1TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := (i + i + 1 + i + 2) + (j + j + 1 + j + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

		} else {
			sum := (i + i + 1 + i + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

		}
		i += 3
		j += 3

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 180
	}
}

func TestTsdbQueryMemNullValueMergeAndDownsampleZero(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_1tsdbmem",
    		"downsample": "3m-sum-zero",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts7_1TsdbQueryMem"], hashMapStartTime["ts7_1TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07_1tsdbmem", 1, 30, 0, 1, 2, hashMapMem["ts7_1TsdbQueryMem"], hashMapMem["ts7_1TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts7_1TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := (i + i + 1 + i + 2) + (j + j + 1 + j + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

		} else {
			sum := (i + i + 1 + i + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

		}
		i += 3
		j += 3

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 180
	}
}

func TestTsdbQueryMemTwoTSMerge(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_1tsdbmem",
    		"downsample": "3m-sum",
    		"aggregator": "sum"
     	},{
    		"metric": "ts07_1tsdbmem",
    		"downsample": "3m-sum",
    		"aggregator": "avg"
    	}]
	}`, hashMapStartTime["ts7_1TsdbQueryMem"], hashMapStartTime["ts7_1TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07_1tsdbmem", 2, 30, 0, 1, 2, hashMapMem["ts7_1TsdbQueryMem"], hashMapMem["ts7_1TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts7_1TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := (i + i + 1 + i + 2) + (j + j + 1 + j + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

		} else {
			sum := (i + i + 1 + i + 2)
			assert.Exactly(t, sum, payloadPoints[0].Dps[key], "they should be equal")

		}
		i += 3
		j += 3

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 180
	}

	assert.Equal(t, 0, len(payloadPoints[1].Tags), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[1].AggTags), "they should be equal")
	assert.Equal(t, 30, len(payloadPoints[1].Dps), "they should be equal")
	assert.Equal(t, 2, len(payloadPoints[1].Tsuuids), "they should be equal")
	assert.Equal(t, "ts07_1tsdbmem", payloadPoints[1].Metric, "they should be equal")
	assert.Equal(t, "host", payloadPoints[1].AggTags[0], "they should be equal")

	if hashMapMem["ts7_1TsdbQueryMem"] == payloadPoints[1].Tsuuids[0] {
		assert.Equal(t, hashMapMem["ts7_1TsdbQueryMem"], payloadPoints[1].Tsuuids[0], "they should be equal")
		assert.Equal(t, hashMapMem["ts7_1TsdbQueryMem2"], payloadPoints[1].Tsuuids[1], "they should be equal")
	} else {
		assert.Equal(t, hashMapMem["ts7_1TsdbQueryMem"], payloadPoints[1].Tsuuids[1], "they should be equal")
		assert.Equal(t, hashMapMem["ts7_1TsdbQueryMem2"], payloadPoints[1].Tsuuids[0], "they should be equal")
	}

	i = 0.0
	j = 0.0
	dateStart = hashMapStartTime["ts7_1TsdbQueryMem"]
	for _, key := range keys {

		if i < 15 || i >= 75 {
			sum := ((i + i + 1 + i + 2) + (j + j + 1 + j + 2)) / 2
			assert.Exactly(t, sum, payloadPoints[1].Dps[key], "they should be equal")

		} else {
			sum := (i + i + 1 + i + 2)
			assert.Exactly(t, sum, payloadPoints[1].Dps[key], "they should be equal")

		}
		i += 3
		j += 3

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 180
	}
}

func TestTsdbQueryMemMoreThanOneTS(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01tsdbmem",
    		"aggregator": "sum",
    		"tags": {
				"host": "test"
      		}
     	},{
    		"metric": "ts10tsdbmem",
    		"aggregator": "sum",
    		"tags": {
      			"host": "test2"
			}
		}]
	}`, hashMapStartTime["ts1TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01tsdbmem", 2, 100, 1, 0, 1, "ts1TsdbQueryMem")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60
		i++
	}

	keys = []string{}

	for key := range payloadPoints[1].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, 25, len(payloadPoints[1].Dps), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[1].Tags), "they should be equal")
	assert.Equal(t, 0, len(payloadPoints[1].AggTags), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[1].Tsuuids), "they should be equal")
	assert.Equal(t, "ts10tsdbmem", payloadPoints[1].Metric, "they should be equal")
	assert.Equal(t, "test2", payloadPoints[1].Tags["host"], "they should be equal")
	assert.Equal(t, ts10IDTsdbQueryMem, payloadPoints[1].Tsuuids[0], "they should be equal")

	i = 0.0
	dateStart = hashMapStartTime["ts1TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[1].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60
		i++
	}
}

func TestTsdbQueryMemTagsMoreThanOneTSOnlyOneExists(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01tsdbmem",
    		"aggregator": "sum",
    		"tags": {
				"host": "test"
      		}
     	},{
    		"metric": "nopoints",
    		"aggregator": "sum",
    		"tags": {
      			"host": "test2"
			}
		}]
	}`, hashMapStartTime["ts1TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01tsdbmem", 1, 100, 1, 0, 1, "ts1TsdbQueryMem")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60
		i++
	}
}

func TestTsdbQueryMemTagsMoreThanOneTSOnlyOneExists2(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "nopoints",
    		"aggregator": "sum",
    		"tags": {
				"host": "test2"
      		}
     	},{
    		"metric": "ts01tsdbmem",
    		"aggregator": "sum",
    		"tags": {
      			"host": "test"
			}
		}]
	}`, hashMapStartTime["ts1TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01tsdbmem", 1, 100, 1, 0, 1, "ts1TsdbQueryMem")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60
		i++
	}
}

func TestTsdbQueryMemFilterMoreThanOneTSOnlyOneExists(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "test",
        		"groupBy": false
      		}]
     	},{
    		"metric": "nopoints",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "test2",
        		"groupBy": false
        	}]
        }]
	}`, hashMapStartTime["ts1TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01tsdbmem", 1, 100, 1, 0, 1, "ts1TsdbQueryMem")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60
		i++
	}
}

func TestTsdbQueryMemFilterMoreThanOneTSOnlyOneExists2(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "nopoints",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "test",
        		"groupBy": false
      		}]
     	},{
    		"metric": "ts01tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "test",
        		"groupBy": false
        	}]
        }]
	}`, hashMapStartTime["ts1TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01tsdbmem", 1, 100, 1, 0, 1, "ts1TsdbQueryMem")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60
		i++
	}
}

func TestTsdbQueryMemFilterMoreThanOneTSOnlyOneHasPoints(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "test",
        		"groupBy": false
      		}]
     	},{
    		"metric": "ts01_3tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "test.2",
        		"groupBy": false
        	}]
        }]
	}`, hashMapStartTime["ts1TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01tsdbmem", 1, 100, 1, 0, 1, "ts1TsdbQueryMem")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60
		i++
	}
}

func TestTsdbQueryMemFilterMoreThanOneTSOnlyOneHasPoints2(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01_3tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "test.2",
        		"groupBy": false
      		}]
     	},{
    		"metric": "ts01tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "test",
        		"groupBy": false
        	}]
        }]
	}`, hashMapStartTime["ts1TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01tsdbmem", 1, 100, 1, 0, 1, "ts1TsdbQueryMem")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1TsdbQueryMem"]
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60
		i++
	}
}

func TestTsdbQueryMemFilterValuesGreaterThan(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01_2tsdbmem",
    		"filterValue": "> 49",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts1_2TsdbQueryMem"]-30)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01_2tsdbmem", 1, 100, 0, 1, 2, hashMapMem["ts1_2TsdbQueryMem"], hashMapMem["ts1_2TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i float32 = 50.0
	dateStart := hashMapStartTime["ts1_2TsdbQueryMem"] + 3000 - 30
	count := 0.0
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		count++

		if count == 2 {
			i++
			count = 0.0
		}

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 30
	}

}

func TestTsdbQueryMemFilterValuesGreaterThanEqualTo(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01_2tsdbmem",
    		"filterValue": "> = 49",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts1_2TsdbQueryMem"]-30)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01_2tsdbmem", 1, 102, 0, 1, 2, hashMapMem["ts1_2TsdbQueryMem"], hashMapMem["ts1_2TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i float32 = 49.0
	dateStart := hashMapStartTime["ts1_2TsdbQueryMem"] + 2940 - 30
	count := 0.0
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		count++

		if count == 2 {
			i++
			count = 0.0
		}

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 30
	}

}

func TestTsdbQueryMemFilterValuesLessThan(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01_2tsdbmem",
    		"filterValue": "<50",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts1_2TsdbQueryMem"]-30)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01_2tsdbmem", 1, 100, 0, 1, 2, hashMapMem["ts1_2TsdbQueryMem"], hashMapMem["ts1_2TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1_2TsdbQueryMem"] - 30
	count := 0
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key])
		count++

		if count == 2 {
			i++
			count = 0
		}

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
		dateStart += 30
	}

}

func TestTsdbQueryMemFilterValuesLessThanEqualTo(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01_2tsdbmem",
    		"filterValue": "<=50",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts1_2TsdbQueryMem"]-30)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01_2tsdbmem", 1, 102, 0, 1, 2, hashMapMem["ts1_2TsdbQueryMem"], hashMapMem["ts1_2TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1_2TsdbQueryMem"] - 30
	count := 0
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		count++

		if count == 2 {
			i++
			count = 0.0
		}

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 30
	}

}

func TestTsdbQueryMemFilterValuesEqualTo(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01_2tsdbmem",
    		"filterValue": "==50",
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts1_2TsdbQueryMem"]-30)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01_2tsdbmem", 1, 2, 0, 1, 2, hashMapMem["ts1_2TsdbQueryMem"], hashMapMem["ts1_2TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i float32 = 50.0
	dateStart := hashMapStartTime["ts1_2TsdbQueryMem"] + 3000 - 30
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 30
	}

}

func TestTsdbQueryMemRateTrueRateOptionsFalse(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01tsdbmem",
    		"rate": true,
    		"aggregator": "sum",
    		"tags": {
      			"host": "test"
			}
		}]
	}`, hashMapStartTime["ts1TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01tsdbmem", 1, 99, 1, 0, 1, "ts1TsdbQueryMem")
	assert.Equal(t, "test", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1TsdbQueryMem"]
	for _, key := range keys {

		calc := (((i + 1.0) - i) / (float32((dateStart + 60) - dateStart)))
		assert.Exactly(t, calc, payloadPoints[0].Dps[key])

		dateStart += 60
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
		i++
	}

}

func TestTsdbQueryMemRateTrueAndMergeRateOptionsFalse(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07tsdbmem",
    		"rate": true,
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts7TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07tsdbmem", 1, 89, 0, 1, 2, hashMapMem["ts7TsdbQueryMem"], hashMapMem["ts7TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts7TsdbQueryMem"]

	for _, key := range keys {

		// Rate: (v2 - v1) / (t2 - t1)
		calc := ((((i + 1) + (j + 5)) - (i + j)) / (float32((dateStart + 60) - dateStart)))
		assert.Exactly(t, calc, payloadPoints[0].Dps[key])
		i++
		j += 5

		dateStart += 60
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)

	}
}

func TestTsdbQueryMemRateTrueDownsampleAndMergeRateOptionsFalse(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts7TsdbQueryMem"])

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07tsdbmem", 1, 29, 0, 1, 2, hashMapMem["ts7TsdbQueryMem"], hashMapMem["ts7TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts7TsdbQueryMem"]

	for _, key := range keys {

		// Rate: (v2 - v1) / (t2 - t1)
		calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 +
			((j+15)+(j+15)+5+(j+15)+10)/3) -
			((i+i+1+i+2)/3 +
				(j+j+5+j+10)/3)) /
			(float32((dateStart + 180) - dateStart))
		assert.Exactly(t, calc, payloadPoints[0].Dps[key])
		i += 3
		j += 15

		dateStart += 180
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)

	}
}

func TestTsdbQueryMemRateFillNone(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts09_1tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg-none",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts9_1TsdbQueryMem"], hashMapStartTime["ts9_1TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts09_1tsdbmem", 1, 9, 0, 1, 2, hashMapMem["ts9_1TsdbQueryMem2"], hashMapMem["ts9_1TsdbQueryMem4"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts9_1TsdbQueryMem"] + (60 * 3)
	for _, key := range keys {

		if i < 12 || i > 15 {
			//sum := i + 2 + j + 2
			//assert.Exactly(t, sum, payloadPoints[0].Dps[key])
			calc := (((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+3)+(j+3)+1+(j+3)+2)/3) - ((i+i+1+i+2)/3 + (j+j+1+j+2)/3)) / (float32((dateStart + 180) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
			dateStart += 180

		} else {
			i = 75
			j = 75
			//sum := i + 2 + j + 2
			//assert.Exactly(t, sum, payloadPoints[0].Dps[key])
			calc := (((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+3)+(j+3)+1+(j+3)+2)/3) - ((i+i+1+i+2)/3 + (j+j+1+j+2)/3)) / (float32((dateStart + 180) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart = hashMapStartTime["ts9_1TsdbQueryMem"] + (60 * 75)
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
			dateStart += 180
		}

		i += 3
		j += 3
	}
}

func TestTsdbQueryMemRateFillZero(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts09_1tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg-zero",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts9_1TsdbQueryMem"], hashMapStartTime["ts9_1TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts09_1tsdbmem", 1, 29, 0, 1, 2, hashMapMem["ts9_1TsdbQueryMem2"], hashMapMem["ts9_1TsdbQueryMem4"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts9_1TsdbQueryMem"]

	// TS1 and 2: 0,1,2,3... | interval (1m)
	// Rate: (v2 - v1) / (t2 - t1)
	// Downsample: 3min
	// DefaultOrder:  Merge - Downsample - Rate
	for _, key := range keys {

		if i < 12 || i >= 75 {
			calc := (((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+3)+(j+3)+1+(j+3)+2)/3) - ((i+i+1+i+2)/3 + (j+j+1+j+2)/3)) / (float32((dateStart + 180) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

		} else if i == 12 {
			calc := ((0 - ((i+i+1+i+2)/3 + (j+j+1+j+2)/3)) / (float32((dateStart + 180) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

		} else if i == 72 {
			calc := (((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+3)+(j+3)+1+(j+3)+2)/3) - 0) / (float32((dateStart + 180) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

		} else {
			assert.Exactly(t, float32(0.0), payloadPoints[0].Dps[key])
		}
		dateStart += 180
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)

		i += 3
		j += 3
	}
}

func TestTsdbQueryMemRateFillNull(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts09_1tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg-null",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts9_1TsdbQueryMem"], hashMapStartTime["ts9_1TsdbQueryMem"]+5350)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbNullQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	if len(payloadPoints) == 0 {
		t.Error("No points were found")
		t.SkipNow()
	}

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 1, len(payloadPoints), "they should be equal")
	assert.Equal(t, 29, len(payloadPoints[0].Dps), "they should be equal")
	assert.Equal(t, 0, len(payloadPoints[0].Tags), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[0].AggTags), "they should be equal")
	assert.Equal(t, 2, len(payloadPoints[0].Tsuuids), "they should be equal")
	assert.Equal(t, "ts09_1tsdbmem", payloadPoints[0].Metric, "they should be equal")
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	if hashMapMem["ts9_1TsdbQueryMem2"] == payloadPoints[0].Tsuuids[0] {
		assert.Equal(t, hashMapMem["ts9_1TsdbQueryMem2"], payloadPoints[0].Tsuuids[0], "they should be equal")
		assert.Equal(t, hashMapMem["ts9_1TsdbQueryMem4"], payloadPoints[0].Tsuuids[1], "they should be equal")
	} else {
		assert.Equal(t, hashMapMem["ts9_1TsdbQueryMem2"], payloadPoints[0].Tsuuids[1], "they should be equal")
		assert.Equal(t, hashMapMem["ts9_1TsdbQueryMem4"], payloadPoints[0].Tsuuids[0], "they should be equal")
	}

	var i, j float32
	dateStart := hashMapStartTime["ts9_1TsdbQueryMem"]

	// TS1 and 2: 0,1,2,3... | interval (1m)
	// Rate: (v2 - v1) / (t2 - t1)
	// Downsample: 3min
	// DefaultOrder:  Merge - Downsample - Rate
	for _, key := range keys {

		if i < 12 || i >= 75 {
			calc := (((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+3)+(j+3)+1+(j+3)+2)/3) - ((i+i+1+i+2)/3 + (j+j+1+j+2)/3)) / (float32((dateStart + 180) - dateStart)))
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

		} else {
			assert.Exactly(t, nil, payloadPoints[0].Dps[key])
		}
		dateStart += 180
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)

		i += 3
		j += 3
	}
}

func TestTsdbQueryMemRateFillNan(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts09_1tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg-nan",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts9_1TsdbQueryMem"], hashMapStartTime["ts9_1TsdbQueryMem"]+5350)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbNullQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	if len(payloadPoints) == 0 {
		t.Error("No points were found")
		t.SkipNow()
	}

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 1, len(payloadPoints), "they should be equal")
	assert.Equal(t, 29, len(payloadPoints[0].Dps), "they should be equal")
	assert.Equal(t, 0, len(payloadPoints[0].Tags), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[0].AggTags), "they should be equal")
	assert.Equal(t, 2, len(payloadPoints[0].Tsuuids), "they should be equal")
	assert.Equal(t, "ts09_1tsdbmem", payloadPoints[0].Metric, "they should be equal")
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	if hashMapMem["ts9_1TsdbQueryMem2"] == payloadPoints[0].Tsuuids[0] {
		assert.Equal(t, hashMapMem["ts9_1TsdbQueryMem2"], payloadPoints[0].Tsuuids[0], "they should be equal")
		assert.Equal(t, hashMapMem["ts9_1TsdbQueryMem4"], payloadPoints[0].Tsuuids[1], "they should be equal")
	} else {
		assert.Equal(t, hashMapMem["ts9_1TsdbQueryMem2"], payloadPoints[0].Tsuuids[1], "they should be equal")
		assert.Equal(t, hashMapMem["ts9_1TsdbQueryMem4"], payloadPoints[0].Tsuuids[0], "they should be equal")
	}

	var i, j float32
	dateStart := hashMapStartTime["ts9_1TsdbQueryMem"]

	// TS1 and 2: 0,1,2,3... | interval (1m): 1448452800, 1448452860, 1448452920...
	// Rate: (v2 - v1) / (t2 - t1)
	// Downsample: 3min
	// DefaultOrder:  Merge - Downsample - Rate
	for _, key := range keys {

		if i < 12 || i >= 75 {
			calc := (((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+3)+(j+3)+1+(j+3)+2)/3) - ((i+i+1+i+2)/3 + (j+j+1+j+2)/3)) / (float32((dateStart + 180) - dateStart)))
			assert.Exactly(t, calc, float32(payloadPoints[0].Dps[key].(float64)))

		} else {
			assert.Exactly(t, "NaN", payloadPoints[0].Dps[key])
		}
		dateStart += 180
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)

		i += 3
		j += 3
	}
}

func TestTsdbQueryMemOrderDownsampleMergeRateAndFilterValues(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_2tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg",
    		"filterValue": ">5",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum",
    		"order":["filterValue","downsample","aggregation","rate"]
    	}]
	}`, hashMapStartTime["ts7_2TsdbQueryMem"], hashMapStartTime["ts7_2TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07_2tsdbmem", 1, 29, 0, 1, 2, hashMapMem["ts7_2TsdbQueryMem"], hashMapMem["ts7_2TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	var point float32 = 1
	dateStart := hashMapStartTime["ts7_2TsdbQueryMem"]

	for _, key := range keys {

		// TS1: 0,1,2,3... | interval (1m)
		// TS1: 0,5,10,15... | interval (1m)
		// Rate: (v2 - v1) / (t2 - t1)
		// Downsample: 3min
		// DefaultOrder:	Merge - Downsample - Rate

		//first point
		if point == 1 {
			calc := (10 / (float32((dateStart + 180) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])
			point++
			//second point
		} else if point == 2 {
			calc := (((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+15)+(j+15)+5+(j+15)+10)/3) - ((j + j + 5 + j + 10) / 3)) / (float32((dateStart + 180) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])
			point++
		} else {
			calc := (((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+15)+(j+15)+5+(j+15)+10)/3) - ((i+i+1+i+2)/3 + (j+j+5+j+10)/3)) / (float32((dateStart + 180) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

		}
		i += 3
		j += 15

		dateStart += 180
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)

	}
}

func TestTsdbQueryMemOrderDownsampleRateAndMerge(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_2tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum",
    		"order":["downsample","rate","aggregation"]
    	}]
	}`, hashMapStartTime["ts7_2TsdbQueryMem"], hashMapStartTime["ts7_2TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07_2tsdbmem", 1, 29, 0, 1, 2, hashMapMem["ts7_2TsdbQueryMem"], hashMapMem["ts7_2TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts7_2TsdbQueryMem"]

	for _, key := range keys {

		// TS1: 0,1,2,3... | interval (1m)
		// TS1: 0,5,10,15... | interval (1m)
		// Rate: (v2 - v1) / (t2 - t1)
		// Downsample: 3min
		// DefaultOrder:	Downsample - Rate - Merge
		calc := (((((i + 3) + (i + 3) + 1 + (i + 3) + 2) / 3) - (i+i+1+i+2)/3) / (float32((dateStart + 180) - dateStart))) +
			(((((j + 15) + (j + 15) + 5 + (j + 15) + 10) / 3) - (j+j+5+j+10)/3) / (float32((dateStart + 180) - dateStart)))
		assert.Exactly(t, calc, payloadPoints[0].Dps[key])
		i += 3
		j += 15

		dateStart += 180
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)

	}
}

func TestTsdbQueryMemOrderMergeDownsampleAndRate(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_2tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum",
    		"order":["aggregation","downsample","rate"]
    	}]
	}`, hashMapStartTime["ts7_2TsdbQueryMem"], hashMapStartTime["ts7_2TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07_2tsdbmem", 1, 29, 0, 1, 2, hashMapMem["ts7_2TsdbQueryMem"], hashMapMem["ts7_2TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts7_2TsdbQueryMem"]

	for _, key := range keys {

		// TS1: 0,1,2,3... | interval (1m)
		// TS1: 0,5,10,15... | interval (1m)
		// Rate: (v2 - v1) / (t2 - t1)
		// Downsample: 3min
		// DefaultOrder: Merge - Downsample - Rate
		calc := ((((i+3+j+15)+(i+3+1+j+15+5)+(i+3+2+j+15+10))/6 - ((i+j)+(i+1+j+5)+(i+2+j+10))/6) / (float32((dateStart + 180) - dateStart)))
		assert.Exactly(t, calc, payloadPoints[0].Dps[key])
		i += 3
		j += 15

		dateStart += 180
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)

	}
}

func TestTsdbQueryMemOrderMergeRateAndDownsample(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_2tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum",
    		"order":["aggregation","rate","downsample"]
    	}]
	}`, hashMapStartTime["ts7_2TsdbQueryMem"], hashMapStartTime["ts7_2TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07_2tsdbmem", 1, 30, 0, 1, 2, hashMapMem["ts7_2TsdbQueryMem"], hashMapMem["ts7_2TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	dateStart := hashMapStartTime["ts7_2TsdbQueryMem"]
	count := 0
	var calc, i, j float32

	for _, key := range keys {
		// TS1: 0,1,2,3... | interval (1m)
		// TS1: 0,5,10,15... | interval (1m)
		// Rate: (v2 - v1) / (t2 - t1)
		// Downsample: 3min
		// DefaultOrder: Merge - Rate - Downsample

		if count == 0 {
			calc = (((j - i) / 1) +
				(((i + 1) - j) / 59) +
				(((j + 5) - (i + 1)) / 1) +
				(((i + 2) - (j + 5)) / 59) +
				(((j + 10) - (i + 2)) / 1)) / 5

			assert.Exactly(t, calc, payloadPoints[0].Dps[key])
			i += 2
			j += 10

			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
			dateStart += 180
			count = 1

		} else {
			calc = ((((i + 1) - j) / 59) +
				(((j + 5) - (i + 1)) / 1) +
				(((i + 2) - (j + 5)) / 59) +
				(((j + 10) - (i + 2)) / 1) +
				(((i + 3) - (j + 10)) / 59) +
				((j + 15) - (i+3)/1)) / 6

			assert.Exactly(t, calc, payloadPoints[0].Dps[key])
			i += 3
			j += 15

			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
			dateStart += 180
		}
	}
}

//
//func TestTsdbQueryMemOrderRateMergeAndDownsample(t *testing.T) {
//
//	payload := `{
//		"start": 1448452800000,
//		"end": 1448458150000,
//		"showTSUIDs": true,
//		"queries": [
//    {
//    		"metric": "ts07_2tsdbmem",
//    		"rate": true,
//    		"downsample": "3m-avg",
//    		"aggregator": "sum",
//    		"order":["rate","aggregation","downsample"]
//      }
//  ]
//	}`
//	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
//
//	if err != nil {
//		t.Error(err)
//		t.SkipNow()
//	}
//	payloadPoints := []PayloadTsdbQueryMem{}
//
//	err = json.Unmarshal(response, &payloadPoints)
//
//	if err != nil {
//		t.Error(err)
//		t.SkipNow()
//	}
//
//	keys := []string{}
//
//	for key := range payloadPoints[0].Dps {
//		keys = append(keys, key)
//	}
//
//	sort.Strings(keys)
//
//	assert.Equal(t, 200, code, "they should be equal")
//	assert.Equal(t, 1, len(payloadPoints), "they should be equal")
//	assert.Equal(t, 30, len(payloadPoints[0].Dps), "they should be equal")
//	assert.Equal(t, 0, len(payloadPoints[0].Tags), "they should be equal")
//	assert.Equal(t, 1, len(payloadPoints[0].AggTags), "they should be equal")
//	assert.Equal(t, 2, len(payloadPoints[0].Tsuuids), "they should be equal")
//	assert.Equal(t, "ts07_2tsdbmem", payloadPoints[0].Metric, "they should be equal")
//	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")
//
//	if hashMapMem["ts7_2TsdbQueryMem"] == payloadPoints[0].Tsuuids[0] {
//		assert.Equal(t, hashMapMem["ts7_2TsdbQueryMem"], payloadPoints[0].Tsuuids[0], "they should be equal")
//		assert.Equal(t, hashMapMem["ts7_2TsdbQueryMem2"], payloadPoints[0].Tsuuids[1], "they should be equal")
//	} else {
//		assert.Equal(t, hashMapMem["ts7_2TsdbQueryMem"], payloadPoints[0].Tsuuids[1], "they should be equal")
//		assert.Equal(t, hashMapMem["ts7_2TsdbQueryMem2"], payloadPoints[0].Tsuuids[0], "they should be equal")
//	}
//
//	i := 0.0
//	j := 0.0
//	dateStart := 1448452800
//
//	for _, key := range keys {
//		// TS1: 0,1,2,3... | interval (1m): 1448452800, 1448452860, 1448452920...
//		// TS1: 0,5,10,15... | interval (1m): 1448452801, 1448452861, 1448452921...
//		// Rate: (v2 - v1) / (t2 - t1)
//		// Downsample: 3min
//		// DefaultOrder: Rate - Merge - Downsample
//
//		calc := ((((i + 1) - (i)) / (float64(dateStart+60) - float64(dateStart))) + (((j + 5) - (j)) / (float64(dateStart+60) - float64(dateStart))) +
//			(((i + 2) - (i + 1)) / (float64(dateStart+120) - float64(dateStart+60))) + (((j + 10) - (j + 5)) / (float64(dateStart+120) - float64(dateStart+60))) +
//			(((i + 3) - (i + 2)) / (float64(dateStart+180) - float64(dateStart+120))) + (((j + 15) - (j + 10)) / (float64(dateStart+180) - float64(dateStart+120)))) / 6
//
//		assert.Exactly(t, calc, payloadPoints[0].Dps[key], "they should be equal")
//		i += 1
//		j += 15
//
//		assert.Exactly(t, strconv.Itoa(dateStart), key, "they should be equal")
//		dateStart += 180
//	}
//}
//
//func TestTsdbQueryMemOrderRateDownsampleAndMerge(t *testing.T) {
//
//	payload := `{
//		"start": 1448452800000,
//		"end": 1448458150000,
//		"showTSUIDs": true,
//		"queries": [
//    {
//    		"metric": "ts07_2tsdbmem",
//    		"rate": true,
//    		"downsample": "3m-avg",
//    		"aggregator": "sum",
//    		"order":["rate","downsample","aggregation"]
//      }
//  ]
//	}`
//	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
//
//	if err != nil {
//		t.Error(err)
//		t.SkipNow()
//	}
//	payloadPoints := []PayloadTsdbQueryMem{}
//
//	err = json.Unmarshal(response, &payloadPoints)
//
//	if err != nil {
//		t.Error(err)
//		t.SkipNow()
//	}
//
//	keys := []string{}
//
//	for key := range payloadPoints[0].Dps {
//		keys = append(keys, key)
//	}
//
//	sort.Strings(keys)
//
//	assert.Equal(t, 200, code, "they should be equal")
//	assert.Equal(t, 1, len(payloadPoints), "they should be equal")
//	assert.Equal(t, 30, len(payloadPoints[0].Dps), "they should be equal")
//	assert.Equal(t, 0, len(payloadPoints[0].Tags), "they should be equal")
//	assert.Equal(t, 1, len(payloadPoints[0].AggTags), "they should be equal")
//	assert.Equal(t, 2, len(payloadPoints[0].Tsuuids), "they should be equal")
//	assert.Equal(t, "ts07_2tsdbmem", payloadPoints[0].Metric, "they should be equal")
//	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")
//
//	if hashMapMem["ts7_2TsdbQueryMem"] == payloadPoints[0].Tsuuids[0] {
//		assert.Equal(t, hashMapMem["ts7_2TsdbQueryMem"], payloadPoints[0].Tsuuids[0], "they should be equal")
//		assert.Equal(t, hashMapMem["ts7_2TsdbQueryMem2"], payloadPoints[0].Tsuuids[1], "they should be equal")
//	} else {
//		assert.Equal(t, hashMapMem["ts7_2TsdbQueryMem"], payloadPoints[0].Tsuuids[1], "they should be equal")
//		assert.Equal(t, hashMapMem["ts7_2TsdbQueryMem2"], payloadPoints[0].Tsuuids[0], "they should be equal")
//	}
//
//	i := 0.0
//	j := 0.0
//	dateStart := 1448452800
//
//	for _, key := range keys {
//
//		// TS1: 0,1,2,3... | interval (1m): 1448452800, 1448452860, 1448452920...
//		// TS1: 0,5,10,15... | interval (1m): 1448452801, 1448452861, 1448452921...
//		// Rate: (v2 - v1) / (t2 - t1)
//		// Downsample: 3min
//		// DefaultOrder: Rate - Downsample - Merge
//		calc := (((((i+1)-(i))/(float64(dateStart+60)-float64(dateStart)))+
//			(((i+2)-(i+1))/(float64(dateStart+120)-float64(dateStart+60)))+
//			(((i+3)-(i+2))/(float64(dateStart+180)-float64(dateStart+120))))/3 +
//			((((j+5)-(j))/(float64(dateStart+60)-float64(dateStart)))+
//				(((j+10)-(j+5))/(float64(dateStart+120)-float64(dateStart+60)))+
//				(((j+15)-(j+10))/(float64(dateStart+180)-float64(dateStart+120))))/3)
//
//		assert.Exactly(t, calc, payloadPoints[0].Dps[key], "they should be equal")
//		i += 1
//		j += 15
//
//		assert.Exactly(t, strconv.Itoa(dateStart), key, "they should be equal")
//		dateStart += 180
//	}
//}
//

func TestTsdbQueryMemDefaultOrder(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_2tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg",
    		"filterValue": ">5",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum"
    	}]
	}`, hashMapStartTime["ts7_2TsdbQueryMem"], hashMapStartTime["ts7_2TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07_2tsdbmem", 1, 29, 0, 1, 2, hashMapMem["ts7_2TsdbQueryMem"], hashMapMem["ts7_2TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	var point float32 = 1
	dateStart := hashMapStartTime["ts7_2TsdbQueryMem"]

	for _, key := range keys {

		// TS1: 0,1,2,3... | interval (1m)
		// TS1: 0,5,10,15... | interval (1m)
		// Rate: (v2 - v1) / (t2 - t1)
		// Downsample: 3min
		// DefaultOrder:  Merge - Downsample - Rate

		//first point
		if point == 1 {
			calc := (10 / (float32((dateStart + 180) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])
			point++
			//second point
		} else if point == 2 {
			calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+15)+(j+15)+5+(j+15)+10)/3) - ((j + j + 5 + j + 10) / 3)) / (float32((dateStart + 180) - dateStart))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])
			point++
		} else {
			calc := ((((i+3)+(i+3)+1+(i+3)+2)/3 + ((j+15)+(j+15)+5+(j+15)+10)/3) - ((i+i+1+i+2)/3 + (j+j+5+j+10)/3)) / (float32((dateStart + 180) - dateStart))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

		}
		i += 3
		j += 15

		dateStart += 180
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)

	}
}

func TestTsdbQueryMemOrderFunctionNotUsed(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_2tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum",
    		"order":["aggregation","rate","filterValue","downsample"]
    	}]
	}`, hashMapStartTime["ts7_2TsdbQueryMem"], hashMapStartTime["ts7_2TsdbQueryMem"]+5350)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts07_2tsdbmem", 1, 30, 0, 1, 2, hashMapMem["ts7_2TsdbQueryMem"], hashMapMem["ts7_2TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i, j float32
	dateStart := hashMapStartTime["ts7_2TsdbQueryMem"]
	count := 0

	for _, key := range keys {
		// TS1: 0,1,2,3... | interval (1m)
		// TS1: 0,5,10,15... | interval (1m)
		// Rate: (v2 - v1) / (t2 - t1)
		// Downsample: 3min
		// DefaultOrder: Merge - Rate - Downsample

		if count == 0 {
			calc := (((j - i) / 1) +
				(((i + 1) - j) / 59) +
				(((j + 5) - (i + 1)) / 1) +
				(((i + 2) - (j + 5)) / 59) +
				(((j + 10) - (i + 2)) / 1)) / 5

			assert.Exactly(t, calc, payloadPoints[0].Dps[key])
			i += 2
			j += 10

			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
			dateStart += 180
			count = 1

		} else {
			calc := ((((i + 1) - j) / 59) +
				(((j + 5) - (i + 1)) / 1) +
				(((i + 2) - (j + 5)) / 59) +
				(((j + 10) - (i + 2)) / 1) +
				(((i + 3) - (j + 10)) / 59) +
				((j + 15) - (i+3)/1)) / 6

			assert.Exactly(t, calc, payloadPoints[0].Dps[key])
			i += 3
			j += 15

			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
			dateStart += 180
		}
	}
}

func TestTsdbQueryMemConcurrentPoints(t *testing.T) {

	dateStart := hashMapStartTime["ts16TsdbQueryMem"]

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts16tsdbmem",
    		"aggregator": "sum"
    	}]
	}`, dateStart, dateStart+1560)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts16tsdbmem", 1, 27, 1, 0, 1, hashMapMem["ts16TsdbQueryMem"])

	for i, key := range keys {

		assert.Exactly(t, float32(i), payloadPoints[0].Dps[key])
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
		dateStart += 60
	}
}

func TestTsdbQueryMemGetBeforePosting(t *testing.T) {

	dateStart := hashMapStartTime["ts17TsdbQueryMem"]

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts17tsdbmem",
    		"aggregator": "sum"
    	}]
	}`, dateStart, dateStart+7320)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts17tsdbmem", 1, 3, 1, 0, 1, hashMapMem["ts17TsdbQueryMem"])

	for i, key := range keys {

		assert.Exactly(t, float32(i), payloadPoints[0].Dps[key])
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
		dateStart += 60
	}

	ts17bTsdbQueryMem(ksMycenae)

	keys, payloadPoints = postAPIQueryAndCheckMem(t, payload, "ts17tsdbmem", 1, 6, 1, 0, 1, hashMapMem["ts17TsdbQueryMem"])

	dateStart = hashMapStartTime["ts17TsdbQueryMem"] + 7200

	for i := 3; i < len(keys); i++ {

		assert.Exactly(t, float32(i), payloadPoints[0].Dps[keys[i]])
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), keys[i])
		dateStart += 60

	}
}

func TestTsdbQueryMemRateTrueRateOptionsTrueCounter(t *testing.T) {

	payload := `{
		"start": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime, 10) + `,
		"end": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime+240, 10) + `,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"rate": true,
    		"rateOptions": {
      			"counter": true
			},
    		"aggregator": "sum",
    		"tags": {
      			"hos-_/.%&#;t": "test-_/.%&#;1"
			}
		}]
	}`
	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts12-_/.%&#;tsdb", 1, 4, 1, 0, 1, ts12IDTsdbQueryMem)
	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"], "they should be equal")

	var i float32 = 1.0
	dateStart := ts12TsdbQueryMemStartTime
	for _, key := range keys {

		calc := ((i * 10.0) - i) / (float32(dateStart + 60 - dateStart))
		assert.Exactly(t, calc, payloadPoints[0].Dps[key])

		dateStart += 60
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)

		i = i * 10.0
	}
}

func TestTsdbQueryMemRateTrueRateOptionsTrueCounterMax(t *testing.T) {

	payload := `{
		"start": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime, 10) + `,
		"end": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime+300, 10) + `,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"rate": true,
    		"rateOptions": {
      			"counter": true,
      			"counterMax": 15000
      		},
    		"aggregator": "sum",
    		"tags": {
      			"hos-_/.%&#;t": "test-_/.%&#;1"
			}
		}]
	}`
	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts12-_/.%&#;tsdb", 1, 5, 1, 0, 1, ts12IDTsdbQueryMem)
	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"], "they should be equal")

	var i float32 = 1.0
	dateStart := ts12TsdbQueryMemStartTime
	var countermax float32 = 15000.0
	for _, key := range keys {

		if dateStart < ts12TsdbQueryMemStartTime+(60*4) {

			calc := ((i * 10.0) - i) / (float32(dateStart + 60 - dateStart))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart += 60
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)

			i = i * 10.0
		} else {
			calc := (countermax - i + 1000) / (float32(dateStart + 60 - dateStart))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart += 60
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
		}
	}
}

func TestTsdbQueryMemRateTrueRateOptionsTrueResetValue(t *testing.T) {

	payload := `{
		"start": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime, 10) + `,
		"end": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime+300, 10) + `,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"rate": true,
    		"rateOptions": {
      			"counter": true,
      			"resetValue": 10
      		},
    		"aggregator": "sum",
    		"tags": {
      			"hos-_/.%&#;t": "test-_/.%&#;1"
			}
		}]
	}`
	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts12-_/.%&#;tsdb", 1, 5, 1, 0, 1, ts12IDTsdbQueryMem)
	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"], "they should be equal")

	var i float32 = 1.0
	dateStart := ts12TsdbQueryMemStartTime
	for _, key := range keys {

		if dateStart < ts12TsdbQueryMemStartTime+(60*4) {

			calc := (((i * 10.0) - i) / (float32((dateStart + 60) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart += 60
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)

			i = i * 10.0
		} else {
			var calc float32
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart += 60
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
		}
	}
}

func TestTsdbQueryMemRateTrueRateOptionsTrueCounterMaxAndResetValue(t *testing.T) {

	payload := `{
		"start": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime, 10) + `,
		"end": ` + strconv.FormatInt(ts12TsdbQueryMemStartTime+660, 10) + `,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"rate": true,
    		"rateOptions": {
      			"counter": true,
      			"counterMax": 15000,
				"resetValue": 233
      		},
    		"aggregator": "sum",
    		"tags": {
      			"hos-_/.%&#;t": "test-_/.%&#;1"
			}
		}]
	}`
	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts12-_/.%&#;tsdb", 1, 11, 1, 0, 1, ts12IDTsdbQueryMem)
	assert.Equal(t, "test-_/.%&#;1", payloadPoints[0].Tags["hos-_/.%&#;t"], "they should be equal")

	var i float32 = 1.0
	dateStart := ts12TsdbQueryMemStartTime
	var countermax float32 = 15000.0
	for _, key := range keys {

		if dateStart < ts12TsdbQueryMemStartTime+(60*4) {

			calc := (((i * 10.0) - i) / (float32((dateStart + 60) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart += 60
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)

			i = i * 10.0
		} else if dateStart < ts12TsdbQueryMemStartTime+(60*5) {
			calc := ((countermax - i + 1000) / (float32((dateStart + 60) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart += 60
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)

		} else if dateStart < ts12TsdbQueryMemStartTime+(60*6) {
			var calc float32
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart += 60
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
			i = 1.0

		} else if dateStart < ts12TsdbQueryMemStartTime+(60*10) {
			calc := (((i * 10.0) - i) / (float32((dateStart + 60) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart += 60
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)

			i = i * 10.0
		} else {
			calc := ((countermax - i + 3000.0) / (float32((dateStart + 60) - dateStart)))
			assert.Exactly(t, calc, payloadPoints[0].Dps[key])

			dateStart += 60
			assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
		}
	}
}

func TestTsdbQueryMemRateTrueNoPoints(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01tsdbmem",
    		"rate": true,
    		"aggregator": "sum",
    		"tags": {
      			"host": "test"
			}
		}]
	}`, hashMapStartTime["ts1TsdbQueryMem"]-2, hashMapStartTime["ts1TsdbQueryMem"]-1)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Exactly(t, "[]", string(response), "they should be equal")

}

func TestTsdbQueryMemFilterGroupByWildcard(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "*",
        		"groupBy": true
	      	}]
	    }]
	}`, ts13TsdbQueryMemStartTime, ts13TsdbQueryMemStartTime+1140)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 3, len(payloadPoints), "they should be equal")

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQueryMem, ts13IDTsdbQueryMem2:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQueryMem == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQueryMem2, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQueryMem, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQueryMem2, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 3.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 3.0

			}
		case ts13IDTsdbQueryMem3, ts13IDTsdbQueryMem4:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQueryMem3 == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQueryMem4, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQueryMem3, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQueryMem4, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 7.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 7.0

			}

		case ts13IDTsdbQueryMem5, ts13IDTsdbQueryMem6, ts13IDTsdbQueryMem7, ts13IDTsdbQueryMem8:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 1, len(payloadPoints[i].Tags))
			assert.Equal(t, 2, len(payloadPoints[i].AggTags))
			assert.Equal(t, 4, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "type", payloadPoints[i].AggTags[1])
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])

			//if ts13IDTsdbQuery5 == payloadPoints[i].Tsuuids[0] {
			//	assert.Equal(t, ts13IDTsdbQuery6, payloadPoints[i].Tsuuids[1])
			//
			//} else {
			//	assert.Equal(t, ts13IDTsdbQuery5, payloadPoints[i].Tsuuids[1])
			//	assert.Equal(t, ts13IDTsdbQuery6, payloadPoints[i].Tsuuids[0])
			//}

			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem5, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem6, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem7, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem8, "Tsuuid not found")

			var value float32 = 26.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 26.0

			}

		default:
			t.Error("not found")
		}

	}

}

func TestTsdbQueryMemFilterGroupByWildcardPartialName(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "hos*",
        		"groupBy": true
	      	}]
	    }]
	}`, ts13TsdbQueryMemStartTime, ts13TsdbQueryMemStartTime+1140)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 3, len(payloadPoints), "they should be equal")

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQueryMem, ts13IDTsdbQueryMem2:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQueryMem == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQueryMem2, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQueryMem, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQueryMem2, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 3.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 3.0

			}
		case ts13IDTsdbQueryMem3, ts13IDTsdbQueryMem4:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQueryMem3 == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQueryMem4, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQueryMem3, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQueryMem4, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 7.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 7.0

			}

		case ts13IDTsdbQueryMem5, ts13IDTsdbQueryMem6, ts13IDTsdbQueryMem7, ts13IDTsdbQueryMem8:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 1, len(payloadPoints[i].Tags))
			assert.Equal(t, 2, len(payloadPoints[i].AggTags))
			assert.Equal(t, 4, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "type", payloadPoints[i].AggTags[1])
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])

			//if ts13IDTsdbQuery5 == payloadPoints[i].Tsuuids[0] {
			//	assert.Equal(t, ts13IDTsdbQuery6, payloadPoints[i].Tsuuids[1])
			//
			//} else {
			//	assert.Equal(t, ts13IDTsdbQuery5, payloadPoints[i].Tsuuids[1])
			//	assert.Equal(t, ts13IDTsdbQuery6, payloadPoints[i].Tsuuids[0])
			//}

			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem5, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem6, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem7, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem8, "Tsuuid not found")

			var value float32 = 26.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 26.0

			}

		default:
			t.Error("not found")
		}

	}

}

func TestTsdbQueryMemFilterGroupByWildcardTagWithDot(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01_3tsdbmem",
    		"aggregator": "sum",
    		"downsample": "3m-min",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "test.*",
        		"groupBy": false
	      	}]
	    }]
	}`, hashMapStartTime["ts1_3TsdbQueryMem"]-30)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01_3tsdbmem", 1, 34, 0, 1, 2, hashMapMem["ts1_3TsdbQueryMem"], hashMapMem["ts1_3TsdbQueryMem2"])
	assert.Equal(t, "host", payloadPoints[0].AggTags[0], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1_3TsdbQueryMem"] - 30 - 20
	for _, key := range keys {

		assert.Exactly(t, i+i, payloadPoints[0].Dps[key], "they should be equal")
		i += 3

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 180
	}

}

func TestTsdbQueryMemFilterGroupByWildcardTagWithDotPartialName(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01_3tsdbmem",
    		"aggregator": "sum",
    		"downsample": "3m-min",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "test.2*",
        		"groupBy": false
	      	}]
	    }]
	}`, hashMapStartTime["ts1_3TsdbQueryMem"]-30)

	keys, payloadPoints := postAPIQueryAndCheckMem(t, payload, "ts01_3tsdbmem", 1, 34, 1, 0, 1, hashMapMem["ts1_3TsdbQueryMem2"])
	assert.Equal(t, "test.2", payloadPoints[0].Tags["host"], "they should be equal")

	var i float32
	dateStart := hashMapStartTime["ts1_3TsdbQueryMem"] - 30 - 20
	for _, key := range keys {

		assert.Exactly(t, i, payloadPoints[0].Dps[key], "they should be equal")
		i += 3

		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart += 180
	}

}

func TestTsdbQueryMemFilterGroupByWildcardTagWithoutDot(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts01_2tsdbmem",
    		"aggregator": "sum",
    		"downsample": "3m-min",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "test.*",
        		"groupBy": false
	      	}]
	    }]
	}`, hashMapStartTime["ts1_2TsdbQueryMem"])

	code, response, _ := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, "[]", string(response), "they should be equal")

}

func TestTsdbQueryMemFilterGroupByLiteralOr(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "literal_or",
        		"tagk": "host",
        		"filter": "host1|host2",
        		"groupBy": true
	      	}]
	    }]
	}`, ts13TsdbQueryMemStartTime, ts13TsdbQueryMemStartTime+1140)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 2, len(payloadPoints), "they should be equal")

	for i := 0; i < 2; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQueryMem, ts13IDTsdbQueryMem2:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQueryMem == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQueryMem2, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQueryMem, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQueryMem2, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 3.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 3.0

			}
		case ts13IDTsdbQueryMem3, ts13IDTsdbQueryMem4:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQueryMem3 == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQueryMem4, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQueryMem3, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQueryMem4, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 7.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 7.0

			}

		default:
			t.Error("not found")
		}

	}

}

func TestTsdbQueryMemFilterGroupByNotLiteralOr(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "not_literal_or",
        		"tagk": "host",
        		"filter": "host1|host2",
        		"groupBy": true
	      	}]
	    }]
	}`, ts13TsdbQueryMemStartTime, ts13TsdbQueryMemStartTime+1140)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 1, len(payloadPoints), "they should be equal")

	if len(payloadPoints) == 0 {
		t.Error("No points were found")
		t.SkipNow()
	}

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	assert.Equal(t, 20, len(payloadPoints[0].Dps), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[0].Tags), "they should be equal")
	assert.Equal(t, 2, len(payloadPoints[0].AggTags), "they should be equal")
	assert.Equal(t, 4, len(payloadPoints[0].Tsuuids), "they should be equal")
	assert.Equal(t, "ts13tsdbmem", payloadPoints[0].Metric, "they should be equal")
	assert.Equal(t, "app", payloadPoints[0].AggTags[0], "they should be equal")
	assert.Equal(t, "type", payloadPoints[0].AggTags[1], "they should be equal")
	assert.Equal(t, "host3", payloadPoints[0].Tags["host"], "they should be equal")

	assert.Contains(t, payloadPoints[0].Tsuuids, ts13IDTsdbQueryMem5, "Tsuuid not found")
	assert.Contains(t, payloadPoints[0].Tsuuids, ts13IDTsdbQueryMem6, "Tsuuid not found")
	assert.Contains(t, payloadPoints[0].Tsuuids, ts13IDTsdbQueryMem7, "Tsuuid not found")
	assert.Contains(t, payloadPoints[0].Tsuuids, ts13IDTsdbQueryMem8, "Tsuuid not found")

	var value float32 = 26.0
	dateStart := ts13TsdbQueryMemStartTime
	for _, key := range keys {

		assert.Exactly(t, value, payloadPoints[0].Dps[key], "they should be equal")
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key, "they should be equal")
		dateStart = dateStart + 60

		value += 26.0
	}
}

func TestTsdbQueryMemFilterGroupByRegexp(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "host",
        		"filter": ".*",
        		"groupBy": true
	      	}]
	    }]
	}`, ts13TsdbQueryMemStartTime, ts13TsdbQueryMemStartTime+1140)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 3, len(payloadPoints), "they should be equal")

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQueryMem, ts13IDTsdbQueryMem2:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQueryMem == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQueryMem2, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQueryMem, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQueryMem2, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 3.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 3.0

			}
		case ts13IDTsdbQueryMem3, ts13IDTsdbQueryMem4:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQueryMem3 == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQueryMem4, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQueryMem3, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQueryMem4, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 7.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 7.0

			}

		case ts13IDTsdbQueryMem5, ts13IDTsdbQueryMem6, ts13IDTsdbQueryMem7, ts13IDTsdbQueryMem8:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 1, len(payloadPoints[i].Tags))
			assert.Equal(t, 2, len(payloadPoints[i].AggTags))
			assert.Equal(t, 4, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "type", payloadPoints[i].AggTags[1])
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])

			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem5, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem6, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem7, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem8, "Tsuuid not found")

			var value float32 = 26.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 26.0

			}

		default:
			t.Error("not found")
		}

	}

}

func TestTsdbQueryMemFilterGroupByRegexpNumbers(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "regexp",
        		"tagk": "host",
        		"filter": "host[0-9]{1}",
        		"groupBy": true
	      	}]
	    }]
	}`, ts13TsdbQueryMemStartTime, ts13TsdbQueryMemStartTime+1140)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 3, len(payloadPoints), "they should be equal")

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQueryMem, ts13IDTsdbQueryMem2:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQueryMem == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQueryMem2, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQueryMem, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQueryMem2, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 3.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 3.0

			}
		case ts13IDTsdbQueryMem3, ts13IDTsdbQueryMem4:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQueryMem3 == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQueryMem4, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQueryMem3, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQueryMem4, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 7.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 7.0

			}

		case ts13IDTsdbQueryMem5, ts13IDTsdbQueryMem6, ts13IDTsdbQueryMem7, ts13IDTsdbQueryMem8:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 1, len(payloadPoints[i].Tags))
			assert.Equal(t, 2, len(payloadPoints[i].AggTags))
			assert.Equal(t, 4, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "type", payloadPoints[i].AggTags[1])
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])

			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem5, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem6, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem7, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem8, "Tsuuid not found")

			var value float32 = 26.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 26.0

			}

		default:
			t.Error("not found")
		}

	}

}

func TestTsdbQueryMemFilterGroupByILiteralOr(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "iliteral_or",
        		"tagk": "host",
        		"filter": "host1|host2|host3",
        		"groupBy": true
	      	}]
	    }]
	}`, ts13TsdbQueryMemStartTime, ts13TsdbQueryMemStartTime+1140)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 3, len(payloadPoints), "they should be equal")

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQueryMem, ts13IDTsdbQueryMem2:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQueryMem == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQueryMem2, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQueryMem, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQueryMem2, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 3.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 3.0

			}
		case ts13IDTsdbQueryMem3, ts13IDTsdbQueryMem4:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].AggTags))
			assert.Equal(t, 2, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])

			if ts13IDTsdbQueryMem3 == payloadPoints[i].Tsuuids[0] {
				assert.Equal(t, ts13IDTsdbQueryMem4, payloadPoints[i].Tsuuids[1])

			} else {
				assert.Equal(t, ts13IDTsdbQueryMem3, payloadPoints[i].Tsuuids[1])
				assert.Equal(t, ts13IDTsdbQueryMem4, payloadPoints[i].Tsuuids[0])
			}

			var value float32 = 7.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 7.0

			}

		case ts13IDTsdbQueryMem5, ts13IDTsdbQueryMem6, ts13IDTsdbQueryMem7, ts13IDTsdbQueryMem8:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 1, len(payloadPoints[i].Tags))
			assert.Equal(t, 2, len(payloadPoints[i].AggTags))
			assert.Equal(t, 4, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app", payloadPoints[i].AggTags[0])
			assert.Equal(t, "type", payloadPoints[i].AggTags[1])
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])

			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem5, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem6, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem7, "Tsuuid not found")
			assert.Contains(t, payloadPoints[i].Tsuuids, ts13IDTsdbQueryMem8, "Tsuuid not found")

			var value float32 = 26.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 26.0

			}

		default:
			t.Error("not found")
		}

	}

}

func TestTsdbQueryMemFilterGroupByAllEspecificTag(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "*",
        		"groupBy": true
        	},{
        		"type": "wildcard",
        		"tagk": "app",
        		"filter": "app1",
        		"groupBy": false
	      	}]
	    }]
	}`, ts13TsdbQueryMemStartTime, ts13TsdbQueryMemStartTime+1140)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 3, len(payloadPoints), "they should be equal")

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQueryMem, ts13IDTsdbQueryMem2:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])
			assert.Equal(t, ts13IDTsdbQueryMem, payloadPoints[i].Tsuuids[0])

			var value float32 = 1.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 1.0

			}
		case ts13IDTsdbQueryMem3, ts13IDTsdbQueryMem4:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])
			assert.Equal(t, ts13IDTsdbQueryMem3, payloadPoints[i].Tsuuids[0])

			var value float32 = 3.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 3.0

			}

		case ts13IDTsdbQueryMem5, ts13IDTsdbQueryMem6:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type3", payloadPoints[i].Tags["type"])
			assert.Equal(t, ts13IDTsdbQueryMem5, payloadPoints[i].Tsuuids[0])

			var value float32 = 5.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 5.0

			}

		default:
			t.Errorf("tsid %v not found ", payloadPoints[i].Tsuuids[0])

		}

	}

}

func TestTsdbQueryMemFilterGroupByIsntTheFirstFilter(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "app",
        		"filter": "app1",
        		"groupBy": false
        	},{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "*",
        		"groupBy": true
	      	}]
	    }]
	}`, ts13TsdbQueryMemStartTime, ts13TsdbQueryMemStartTime+1140)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 3, len(payloadPoints), "they should be equal")

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQueryMem, ts13IDTsdbQueryMem2:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])
			assert.Equal(t, ts13IDTsdbQueryMem, payloadPoints[i].Tsuuids[0])

			var value float32 = 1.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 1.0

			}
		case ts13IDTsdbQueryMem3, ts13IDTsdbQueryMem4:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])
			assert.Equal(t, ts13IDTsdbQueryMem3, payloadPoints[i].Tsuuids[0])

			var value float32 = 3.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 3.0

			}

		case ts13IDTsdbQueryMem5, ts13IDTsdbQueryMem6:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type3", payloadPoints[i].Tags["type"])
			assert.Equal(t, ts13IDTsdbQueryMem5, payloadPoints[i].Tsuuids[0])

			var value float32 = 5.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 5.0

			}

		default:
			t.Errorf("tsid %v not found ", payloadPoints[i].Tsuuids[0])

		}

	}

}

func TestTsdbQueryMemFilterGroupByLiteralOrEspecificTag(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "literal_or",
        		"tagk": "host",
        		"filter": "host1|host2",
        		"groupBy": true
        	},{
        		"type": "wildcard",
        		"tagk": "app",
        		"filter": "app1",
        		"groupBy": false
	      	}]
	    }]
	}`, ts13TsdbQueryMemStartTime, ts13TsdbQueryMemStartTime+1140)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 2, len(payloadPoints), "they should be equal")

	for i := 0; i < 2; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQueryMem, ts13IDTsdbQueryMem2:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])
			assert.Equal(t, ts13IDTsdbQueryMem, payloadPoints[i].Tsuuids[0])

			var value float32 = 1.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 1.0

			}
		case ts13IDTsdbQueryMem3, ts13IDTsdbQueryMem4:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])
			assert.Equal(t, ts13IDTsdbQueryMem3, payloadPoints[i].Tsuuids[0])

			var value float32 = 3.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 3.0

			}

		default:
			t.Errorf("tsid %v not found ", payloadPoints[i].Tsuuids[0])

		}

	}

}

func TestTsdbQueryMemFilterGroupByWildcardTwoTags(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "*",
        		"groupBy": true
        	},{
        		"type": "wildcard",
        		"tagk": "type",
        		"filter": "*",
        		"groupBy": true
	      	}]
	    }]
	}`, ts13TsdbQueryMemStartTime, ts13TsdbQueryMemStartTime+1140)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 5, len(payloadPoints), "they should be equal")

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQueryMem:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQueryMem, payloadPoints[i].Tsuuids[0])

			var value float32 = 1.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 1.0

			}

		case ts13IDTsdbQueryMem3:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQueryMem3, payloadPoints[i].Tsuuids[0])

			var value float32 = 3.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 3.0

			}

		case ts13IDTsdbQueryMem5:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type3", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQueryMem5, payloadPoints[i].Tsuuids[0])

			var value float32 = 5.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 5.0

			}

		case ts13IDTsdbQueryMem7:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type4", payloadPoints[i].Tags["type"])

			assert.Equal(t, ts13IDTsdbQueryMem7, payloadPoints[i].Tsuuids[0])

			var value float32 = 7.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 7.0

			}

		case ts13IDTsdbQueryMem8:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 2, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type5", payloadPoints[i].Tags["type"])

			assert.Equal(t, ts13IDTsdbQueryMem8, payloadPoints[i].Tsuuids[0])

			var value float32 = 8.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 8.0

			}

		default:
			t.Error("not found")
		}

	}

}

func TestTsdbQueryMemFilterGroupByWildcardTwoTagsFiltersOutOfOrder1(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "*",
        		"groupBy": true
        	},{
        		"type": "wildcard",
        		"tagk": "type",
        		"filter": "*",
        		"groupBy": true
        	},{
        		"type": "wildcard",
        		"tagk": "app",
        		"filter": "app1",
        		"groupBy": false
	      	}]
	    }]
	}`, ts13TsdbQueryMemStartTime, ts13TsdbQueryMemStartTime+1140)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 3, len(payloadPoints), "they should be equal")

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQueryMem:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQueryMem, payloadPoints[i].Tsuuids[0])

			var value float32 = 1.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 1.0

			}

		case ts13IDTsdbQueryMem3:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQueryMem3, payloadPoints[i].Tsuuids[0])

			var value float32 = 3.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 3.0

			}

		case ts13IDTsdbQueryMem5:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type3", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQueryMem5, payloadPoints[i].Tsuuids[0])

			var value float32 = 5.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 5.0

			}
		default:
			t.Error("not found")
		}
	}
}

func TestTsdbQueryMemFilterGroupByWildcardTwoTagsFiltersOutOfOrder2(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "*",
        		"groupBy": true
        	},{
        		"type": "wildcard",
        		"tagk": "app",
        		"filter": "app1",
        		"groupBy": false
        	},{
        		"type": "wildcard",
        		"tagk": "type",
        		"filter": "*",
        		"groupBy": true
	      	}]
	    }]
	}`, ts13TsdbQueryMemStartTime, ts13TsdbQueryMemStartTime+1140)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 3, len(payloadPoints), "they should be equal")

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQueryMem:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQueryMem, payloadPoints[i].Tsuuids[0])

			var value float32 = 1.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 1.0

			}

		case ts13IDTsdbQueryMem3:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQueryMem3, payloadPoints[i].Tsuuids[0])

			var value float32 = 3.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 3.0

			}

		case ts13IDTsdbQueryMem5:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type3", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQueryMem5, payloadPoints[i].Tsuuids[0])

			var value float32 = 5.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 5.0

			}

		default:
			t.Error("not found")
		}
	}
}

func TestTsdbQueryMemFilterGroupByWildcardTwoTagsFiltersOutOfOrder3(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "app",
        		"filter": "app1",
        		"groupBy": false
        	},{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "*",
        		"groupBy": true
        	},{
        		"type": "wildcard",
        		"tagk": "type",
        		"filter": "*",
        		"groupBy": true
	      	}]
	    }]
	}`, ts13TsdbQueryMemStartTime, ts13TsdbQueryMemStartTime+1140)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 3, len(payloadPoints), "they should be equal")

	for i := 0; i < 3; i++ {

		switch payloadPoints[i].Tsuuids[0] {

		case ts13IDTsdbQueryMem:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 0, len(payloadPoints[i].AggTags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "host1", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type1", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQueryMem, payloadPoints[i].Tsuuids[0])

			var value float32 = 1.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 1.0

			}

		case ts13IDTsdbQueryMem3:
			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "host2", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type2", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQueryMem3, payloadPoints[i].Tsuuids[0])

			var value float32 = 3.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 3.0

			}

		case ts13IDTsdbQueryMem5:

			keys := []string{}

			for key := range payloadPoints[i].Dps {
				keys = append(keys, key)
			}

			sort.Strings(keys)

			assert.Equal(t, 20, len(payloadPoints[i].Dps))
			assert.Equal(t, 3, len(payloadPoints[i].Tags))
			assert.Equal(t, 1, len(payloadPoints[i].Tsuuids))
			assert.Equal(t, "ts13tsdbmem", payloadPoints[i].Metric)
			assert.Equal(t, "host3", payloadPoints[i].Tags["host"])
			assert.Equal(t, "type3", payloadPoints[i].Tags["type"])
			assert.Equal(t, "app1", payloadPoints[i].Tags["app"])

			assert.Equal(t, ts13IDTsdbQueryMem5, payloadPoints[i].Tsuuids[0])

			var value float32 = 5.0
			dateStart := ts13TsdbQueryMemStartTime
			for _, key := range keys {

				assert.Exactly(t, value, payloadPoints[i].Dps[key])

				assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
				dateStart += 60

				value += 5.0

			}

		default:
			t.Error("not found")
		}
	}
}

func TestTsdbQueryMemFilterGroupBySameTagk(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "*",
        		"groupBy": true
        	},{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "host3",
        		"groupBy": true
	      	}]
	    }]
	}`, ts13TsdbQueryMemStartTime, ts13TsdbQueryMemStartTime+1140)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 1, len(payloadPoints), "they should be equal")

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	sort.Strings(payloadPoints[0].AggTags)

	assert.Equal(t, 20, len(payloadPoints[0].Dps), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[0].Tags), "they should be equal")
	assert.Equal(t, 2, len(payloadPoints[0].AggTags), "they should be equal")
	assert.Equal(t, "app", payloadPoints[0].AggTags[0], "they should be equal")
	assert.Equal(t, "type", payloadPoints[0].AggTags[1], "they should be equal")
	assert.Equal(t, 4, len(payloadPoints[0].Tsuuids), "they should be equal")
	assert.Equal(t, "ts13tsdbmem", payloadPoints[0].Metric, "they should be equal")
	assert.Equal(t, "host3", payloadPoints[0].Tags["host"], "they should be equal")

	tsuuidExpected := []string{ts13IDTsdbQueryMem5, ts13IDTsdbQueryMem6, ts13IDTsdbQueryMem7, ts13IDTsdbQueryMem8}
	tsuuidActual := []string{payloadPoints[0].Tsuuids[0], payloadPoints[0].Tsuuids[1],
		payloadPoints[0].Tsuuids[2], payloadPoints[0].Tsuuids[3]}

	sort.Strings(tsuuidExpected)
	sort.Strings(tsuuidActual)

	assert.Equal(t, tsuuidExpected, tsuuidActual, "they should be equal")

	var value float32 = 26.0
	dateStart := ts13TsdbQueryMemStartTime
	for _, key := range keys {

		assert.Exactly(t, value, payloadPoints[0].Dps[key])
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
		dateStart += 60

		value += 26.0

	}
}

func TestTsdbQueryMemFilterSameTagkOnGroupByAndTags(t *testing.T) {

	payload := fmt.Sprintf(`{
		"start": %v,
		"end": %v,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "*",
        		"groupBy": true
        	},{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "host3",
        		"groupBy": false
	      	}]
	    }]
	}`, ts13TsdbQueryMemStartTime, ts13TsdbQueryMemStartTime+1140)

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, 1, len(payloadPoints), "they should be equal")

	keys := []string{}
	for key := range payloadPoints[0].Dps {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	sort.Strings(payloadPoints[0].AggTags)

	assert.Equal(t, 20, len(payloadPoints[0].Dps), "they should be equal")
	assert.Equal(t, 1, len(payloadPoints[0].Tags), "they should be equal")
	assert.Equal(t, 2, len(payloadPoints[0].AggTags), "they should be equal")
	assert.Equal(t, "app", payloadPoints[0].AggTags[0], "they should be equal")
	assert.Equal(t, "type", payloadPoints[0].AggTags[1], "they should be equal")
	assert.Equal(t, 4, len(payloadPoints[0].Tsuuids), "they should be equal")
	assert.Equal(t, "ts13tsdbmem", payloadPoints[0].Metric, "they should be equal")
	assert.Equal(t, "host3", payloadPoints[0].Tags["host"], "they should be equal")

	tsuuidExpected := []string{ts13IDTsdbQueryMem5, ts13IDTsdbQueryMem6, ts13IDTsdbQueryMem7, ts13IDTsdbQueryMem8}
	tsuuidActual := []string{payloadPoints[0].Tsuuids[0], payloadPoints[0].Tsuuids[1],
		payloadPoints[0].Tsuuids[2], payloadPoints[0].Tsuuids[3]}

	sort.Strings(tsuuidExpected)
	sort.Strings(tsuuidActual)

	assert.Equal(t, tsuuidExpected, tsuuidActual, "they should be equal")

	var value float32 = 26.0
	dateStart := ts13TsdbQueryMemStartTime
	for _, key := range keys {

		assert.Exactly(t, value, payloadPoints[0].Dps[key])
		assert.Exactly(t, strconv.FormatInt(dateStart, 10), key)
		dateStart += 60

		value += 26.0

	}
}

func TestTsdbQueryMemFilterGroupByNoPoints(t *testing.T) {

	payload := `{
		"start": 1348452740000,
		"end": 1348453940000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "*",
        		"groupBy": true
	      	}]
	    }]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Exactly(t, "[]", string(response), "they should be equal")
}

func TestTsdbQueryMemFilterGroupByTagNotFound(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "x",
        		"filter": "*",
        		"groupBy": true
	      	}]
	    }]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadPoints := []PayloadTsdbQueryMem{}

	err = json.Unmarshal(response, &payloadPoints)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 200, code, "they should be equal")
	assert.Equal(t, "[]", string(response), "they should be equal")

}

func TestTsdbQueryMemTagsInvalidTagKey(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"rate": "false",
    		"rateOptions": {
      			"counter": true,
      			"counterMax": 15000
      		},
    		"aggregator": "sum",
    		"tags": {
      			"": "test-_/.%&#;1"
			}
		}]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "json: cannot unmarshal string into Go struct field TSDBquery.rate of type bool", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemTagsInvalidTagValue(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"rate": "false",
    		"rateOptions": {
      			"counter": true,
      			"counterMax": 15000
      		},
    		"aggregator": "sum",
    		"tags": {
      			"hos-_/.%&#;t": ""
			}
		}]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "json: cannot unmarshal string into Go struct field TSDBquery.rate of type bool", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemTagsInvalidMetric(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "",
    		"rate": "false",
    		"rateOptions": {
      			"counter": true,
      			"counterMax": 15000
      		},
    		"aggregator": "sum",
    		"tags": {
      			"host": "test"
			}
		}]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "json: cannot unmarshal string into Go struct field TSDBquery.rate of type bool", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemInvalidOrderOperation(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_2tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum",
    		"order":["downsample","aggregation","rate","test"]
    	}]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "invalid operations in order array [test]", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemInvalidOrderDownsampleMissing(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_2tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum",
    		"order":["aggregation","rate"]
    	}]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "downsample configured but no downsample found in order array", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemInvalidOrderMergeMissing(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_2tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum",
    		"order":["downsample","rate"]
    	}]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "aggregation configured but no aggregation found in order array", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemInvalidOrderRateMissing(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_2tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum",
    		"order":["downsample","aggregation"]
    	}]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "rate configured but no rate found in order array", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemInvalidOrderFilterValueMissing(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_2tsdbmem",
    		"filterValue":"==1",
    		"downsample": "3m-avg",
    		"aggregator": "sum",
    		"order":["downsample","aggregation"]
    	}]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "filterValue configured but no filterValue found in order array", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemInvalidOrderDuplicatedDownsample(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_2tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum",
    		"order":["aggregation","downsample","downsample","rate"]
    	}]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "more than one downsample found in order array", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemInvalidOrderDuplicatedFilterValue(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_2tsdbmem",
    		"filterValue":"==1",
    		"rate": true,
    		"downsample": "3m-avg",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum",
    		"order":["aggregation","downsample","filterValue","filterValue","rate"]
    	}]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "more than one filterValue found in order array", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemInvalidOrderDuplicatedMerge(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_2tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum",
    		"order":["aggregation","aggregation","downsample","rate"]
    	}]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "more than one aggregation found in order array", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemInvalidOrderDuplicatedRate(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_2tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum",
    		"order":["downsample","aggregation","rate","rate"]
    	}]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "more than one rate found in order array", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemEmptyOrderOption(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts07_2tsdbmem",
    		"rate": true,
    		"downsample": "3m-avg",
    		"rateOptions": {
      			"counter": false
			},
    		"aggregator": "sum",
    		"order":[""]
    	}]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "aggregation configured but no aggregation found in order array", payloadError.Error, "they should be equal")
}

func TestTsdbQueryMemInvalidRate(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"rate": "a",
    		"rateOptions": {
      			"counter": true,
      			"counterMax": 15000
      		},
    		"aggregator": "sum",
    		"tags": {
      			"hos-_/.%&#;t": "test-_/.%&#;1"
			}
		}]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "json: cannot unmarshal string into Go struct field TSDBquery.rate of type bool", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemInvalidRateOptionCounter(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"rate": true,
    		"rateOptions": {
      			"counter": "a",
      			"counterMax": 15000
      		},
    		"aggregator": "sum",
    		"tags": {
      			"hos-_/.%&#;t": "test-_/.%&#;1"
			}
		}]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "json: cannot unmarshal string into Go struct field TSDBrateOptions.counter of type bool", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemInvalidRateOptionCounterMax(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"rate": true,
    		"rateOptions": {
      			"counter": true,
      			"counterMax": "a"
      		},
    		"aggregator": "sum",
    		"tags": {
      			"hos-_/.%&#;t": "test-_/.%&#;1"
			}
		}]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "json: cannot unmarshal string into Go struct field TSDBrateOptions.counterMax of type int64", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemInvalidRateOptionCounterMaxNegative(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"rate": true,
    		"rateOptions": {
      			"counter": true,
      			"counterMax": -1
      		},
    		"aggregator": "sum",
    		"tags": {
      			"hos-_/.%&#;t": "test-_/.%&#;1"
			}
		}]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "counter max needs to be a positive integer", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemInvalidRateOptionResetValue(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448458150000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts12-_/.%&#;tsdb",
    		"rate": true,
    		"rateOptions": {
      			"counter": true,
      			"resetValue": "a"
      		},
    		"aggregator": "sum",
    		"tags": {
      			"hos-_/.%&#;t": "test-_/.%&#;1"
			}
		}]
	}`
	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "json: cannot unmarshal string into Go struct field TSDBrateOptions.resetValue of type int64", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemInvalidFilterWildcard2(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "wildcard",
        		"tagk": "host",
        		"filter": "host1|host2",
        		"groupBy": false
	      	}]
	    }]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "Invalid characters in field filter: host1|host2", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemInvalidFilterLiteralOr1(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "literal_or",
        		"tagk": "host",
        		"filter": "*",
        		"groupBy": false
	      	}]
	    }]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "Invalid characters in field filter: *", payloadError.Error, "they should be equal")

}

func TestTsdbQueryMemInvalidFilterNotLiteralOr1(t *testing.T) {

	payload := `{
		"start": 1448452740000,
		"end": 1448453940000,
		"showTSUIDs": true,
		"queries": [{
    		"metric": "ts13tsdbmem",
    		"aggregator": "sum",
    		"filters": [{
        		"type": "not_literal_or",
        		"tagk": "host",
        		"filter": "*",
        		"groupBy": false
	      	}]
	    }]
	}`

	code, response, err := mycenaeTools.HTTP.POST("keyspaces/"+ksMycenae+"/api/query", []byte(payload))

	payloadError := GrafanaPointsErrorMem{}

	err = json.Unmarshal(response, &payloadError)

	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, "they should be equal")
	assert.Equal(t, "Invalid characters in field filter: *", payloadError.Error, "they should be equal")

}
