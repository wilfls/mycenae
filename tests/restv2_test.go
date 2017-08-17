package main

import (
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/tests/tools"
)

func assertElastic(t *testing.T, keyspace string, metric string, tags map[string]string, hashID string) {

	lenTags := len(tags)

	if _, ok := tags["ksid"]; ok {
		lenTags--
	}

	esMetric := mycenaeTools.ElasticSearch.Timeseries.GetMetricPost(keyspace, metric)
	assert.Equal(t, 1, esMetric.Hits.Total, "metric sent in the payload does not match the one in elasticsearch")

	meta := mycenaeTools.ElasticSearch.Timeseries.GetMeta(keyspace, hashID)
	assert.Equal(t, hashID, meta.Source.ID, "meta id corresponding to the payload does not match the one in elasticsearch")
	assert.Equal(t, metric, meta.Source.Metric, "metric sent in the payload does not match the one in elasticsearch")
	assert.Equal(t, lenTags, len(meta.Source.Tags))

	for _, tagKV := range meta.Source.Tags {

		tagV, ok := tags[tagKV.TagKey]
		assert.True(t, ok, "tag key in elasticsearch was not sent in payload")
		assert.Contains(t, meta.Source.Tags, tools.TagMeta{TagKey: tagKV.TagKey, TagValue: tagV}, "the pair tagKey-tagValue in elasticsearch does not match the one sent in payload")

		esTagK := mycenaeTools.ElasticSearch.Timeseries.GetTagKeyPost(keyspace, tagKV.TagKey)
		assert.Equal(t, 1, esTagK.Hits.Total, "tag key sent in the payload does not match the one in elasticsearch")

		esTagV := mycenaeTools.ElasticSearch.Timeseries.GetTagValuePost(keyspace, tagV)
		assert.Equal(t, 1, esTagV.Hits.Total, "tag value sent in the payload does not match the one in elasticsearch")
	}
}

func assertElasticEmpty(t *testing.T, keyspace string, metric string, tags map[string]string, hashID string) {

	esMetric := mycenaeTools.ElasticSearch.Timeseries.GetMetricPost(keyspace, metric)
	assert.Equal(t, 0, esMetric.Hits.Total, "metric sent in the payload does not match the one in elasticsearch")

	for tagKey, tagValue := range tags {

		esTagK := mycenaeTools.ElasticSearch.Timeseries.GetTagKeyPost(keyspace, tagKey)
		assert.Equal(t, 0, esTagK.Hits.Total)

		esTagV := mycenaeTools.ElasticSearch.Timeseries.GetTagValuePost(keyspace, tagValue)
		assert.Equal(t, 0, esTagV.Hits.Total)
	}

	meta := mycenaeTools.ElasticSearch.Timeseries.GetMeta(keyspace, hashID)
	assert.False(t, meta.Found, "document has been found when it should not")
	assert.Equal(t, "", meta.Source.ID)
	assert.Equal(t, "", meta.Source.Metric)
	assert.Equal(t, 0, len(meta.Source.Tags))
}

func assertMycenae(t *testing.T, keyspace string, start int64, end int64, value float32, hashID string) {

	status, response := mycenaeTools.Mycenae.GetPoints(keyspace, start, end, hashID)

	if assert.Equal(t, 200, status) {
		if assert.Equal(t, 1, len(response.Payload[hashID].Points.Ts)) {
			assert.True(t, start*1000 <= int64(response.Payload[hashID].Points.Ts[0][0].(float64)) && int64(response.Payload[hashID].Points.Ts[0][0].(float64)) <= end*1000)
			assert.Equal(t, value, float32(response.Payload[hashID].Points.Ts[0][1].(float64)))
		}
	}
}

func assertMycenaeEmpty(t *testing.T, keyspace string, start int64, end int64, hashID string) {

	status, response := mycenaeTools.Mycenae.GetPoints(keyspace, start, end, hashID)

	assert.Equal(t, 204, status)
	assert.Equal(t, tools.MycenaePoints{}, response)
}

// Asserts 1 error
func assertRESTError(t *testing.T, err tools.RestErrors, payload *tools.Payload, keyspace, errMessage string, lenFailed, lenSuccess int) {

	if assert.Equal(t, 1, len(err.Errors)) {

		if payload.Metric == "" {
			assert.Empty(t, err.Errors[0].Datapoint.Metric)
		} else {
			assert.Equal(t, payload.Metric, err.Errors[0].Datapoint.Metric)
		}

		if payload.Value != nil {
			assert.Equal(t, *payload.Value, err.Errors[0].Datapoint.Value)
		}

		assert.Equal(t, len(payload.Tags), len(err.Errors[0].Datapoint.Tags))

		for tagK, tagV := range err.Errors[0].Datapoint.Tags {

			payTagV, ok := payload.Tags[tagK]
			if assert.True(t, ok, "tag key was not sent in payload") {
				assert.Equal(t, tagV, payTagV)
			}
		}

		assert.Equal(t, keyspace, err.Errors[0].Datapoint.Tags["ksid"])
		assert.Contains(t, err.Errors[0].Error, errMessage)
	}
	assert.Equal(t, lenFailed, err.Failed)
	assert.Equal(t, lenSuccess, err.Success)
}

func TestRESTv2PayloadWithAllFields(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	sendRESTPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2PayloadWithNoTimestamp(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Timestamp = nil

	dateBefore := time.Now().Unix()

	sendRESTPayloadAndAssertPoint(t, p, dateBefore, time.Now().Add(tools.Sleep3).Unix())
}

func TestRESTv2PayloadWithMoreThanOneTag(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	p.TagKey2 = fmt.Sprint("testTagKey2-", p.Random)
	p.TagValue2 = fmt.Sprint("testTagValue2-", p.Random)
	p.Tags[p.TagKey2] = p.TagValue2

	sendRESTPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2MultiplePointsSameIDAndTimestampsGreaterThanDay(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	timestamps := [5]int64{
		*p.Timestamp,
		time.Unix(*p.Timestamp, 0).AddDate(0, 0, -1).Unix(),
		time.Unix(*p.Timestamp, 0).AddDate(0, 0, -2).Unix(),
		time.Unix(*p.Timestamp, 0).AddDate(0, 0, -3).Unix(),
		time.Unix(*p.Timestamp, 0).AddDate(0, 0, -4).Unix(),
	}

	values := [5]float32{
		10.0,
		2.2,
		3.3333,
		44444.444,
		0.555555,
	}

	for i := 0; i < 5; i++ {

		*p.Timestamp = timestamps[i]
		*p.Value = values[i]

		ps := tools.PayloadSlice{PS: []tools.Payload{*p}}

		statusCode, _, _ := mycenaeTools.HTTP.POST("api/put", ps.Marshal())
		assert.Equal(t, 204, statusCode)
	}

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(p.Metric, p.Tags)

	time.Sleep(tools.Sleep3)

	for i := 0; i < 5; i++ {
		assertMycenae(t, ksMycenae, timestamps[i], timestamps[i], values[i], hashID)
	}

	assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)
}

func TestRESTv2MultiplePointsSameIDAndNoTimestamp(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Timestamp = nil
	dateBefore := time.Now()

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(p.Metric, p.Tags)

	for i := 0; i < 3; i++ {

		*p.Value = float32(i)

		ps := tools.PayloadSlice{PS: []tools.Payload{*p}}

		statusCode, _, _ := mycenaeTools.HTTP.POST("api/put", ps.Marshal())
		assert.Equal(t, 204, statusCode)

		time.Sleep(tools.Sleep3)

		dateAfter := time.Now()
		assertMycenae(t, ksMycenae, dateBefore.Unix(), dateAfter.Unix(), *p.Value, hashID)
		dateBefore = dateAfter
	}

	assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)
}

func TestRESTv2CheckLocalElasticCache(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(p.Metric, p.Tags)

	for i := 0; i < 2; i++ {

		*p.Value = float32(i)
		*p.Timestamp = time.Now().Unix()

		ps := tools.PayloadSlice{PS: []tools.Payload{*p}}

		statusCode, _, _ := mycenaeTools.HTTP.POST("api/put", ps.Marshal())
		assert.Equal(t, 204, statusCode)
		time.Sleep(tools.Sleep3)

		assertMycenae(t, ksMycenae, *p.Timestamp, *p.Timestamp, *p.Value, hashID)
	}

	assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)

	mycenaeTools.ElasticSearch.Timeseries.DeleteKey(ksMycenae, hashID)
	mycenaeTools.ElasticSearch.Timeseries.DeleteMetric(ksMycenae, p.Metric)
	mycenaeTools.ElasticSearch.Timeseries.DeleteTagKey(ksMycenae, p.TagKey)
	mycenaeTools.ElasticSearch.Timeseries.DeleteTagValue(ksMycenae, p.TagValue)

	*p.Value = 2
	*p.Timestamp = time.Now().Unix()
	ps := tools.PayloadSlice{PS: []tools.Payload{*p}}

	statusCode, _, _ := mycenaeTools.HTTP.POST("api/put", ps.Marshal())
	assert.Equal(t, 204, statusCode)
	time.Sleep(tools.Sleep3)

	assertMycenae(t, ksMycenae, *p.Timestamp, *p.Timestamp, *p.Value, hashID)

	assertElasticEmpty(t, ksMycenae, p.Metric, p.Tags, hashID)
}

func TestRESTv2PayloadWithOnlyNumbersOrLetters(t *testing.T) {
	t.Parallel()

	numbers := "01234567890123456789"
	letters := "abcdefghijklmnopqrstuvwxyzabcd"

	numbersOrLetters := []string{numbers, letters}

	for _, numOrLetters := range numbersOrLetters {

		var wg sync.WaitGroup
		wg.Add(3)

		go testMetric(t, numOrLetters, &wg, false)
		go testTagKey(t, numOrLetters, &wg, false)
		go testTagValue(t, numOrLetters, &wg, false)

		wg.Wait()
	}
}

func TestRESTv2PayloadWithSpecialChars(t *testing.T) {
	t.Parallel()

	tests := make([]byte, 3)

	wg := sync.WaitGroup{}
	wg.Add(len(tests))

	for test := range tests {

		go func(test int) {

			p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

			specialChars := fmt.Sprint("9Aa35ffg", p.Random, "...-___-.%&#;./.a1")

			switch test {
			case 0:
				p.Metric = specialChars
				*p.Value = float32(test)
			case 1:
				p.Tags = map[string]string{"ksid": ksMycenae, specialChars: p.TagValue}
				*p.Value = float32(test)
			case 2:
				p.Tags[p.TagKey] = specialChars
				*p.Value = float32(test)
			}

			ps := tools.PayloadSlice{PS: []tools.Payload{*p}}

			statusCode, _, _ := mycenaeTools.HTTP.POST("api/put", ps.Marshal())
			assert.Equal(t, 204, statusCode)

			// special chars take longer to be saved
			time.Sleep(tools.Sleep3 * 2)

			hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(p.Metric, p.Tags)

			assertMycenae(t, ksMycenae, *p.Timestamp, *p.Timestamp, *p.Value, hashID)

			assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)

			wg.Done()
		}(test)
	}
	wg.Wait()
}

func TestRESTv2PayloadWithLastCharUnderscore(t *testing.T) {
	t.Parallel()

	lastCharUnderscore := fmt.Sprint("9Aa35ffg...-___-..._")

	var wg sync.WaitGroup
	wg.Add(3)

	go testMetric(t, lastCharUnderscore, &wg, false)
	go testTagKey(t, lastCharUnderscore, &wg, false)
	go testTagValue(t, lastCharUnderscore, &wg, false)

	wg.Wait()
}

func TestRESTv2PayloadWithNegativeValue(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	*p.Value = -5.0

	sendRESTPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2PayloadWithZeroValue(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	*p.Value = 0.0

	sendRESTPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2PayloadWithMaxFloat32Value(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	*p.Value = math.MaxFloat32

	sendRESTPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2PayloadWithBiggerThanFloat32Value(t *testing.T) {
	t.Parallel()

	value := math.MaxFloat32 * 2
	metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

	payload := fmt.Sprintf(
		`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}]`,
		value,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	tags := map[string]string{tagKey: tagValue}

	sendRESTPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)
}

func TestRESTv2PayloadsWithSameMetricTagsTimestamp(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	ps := tools.PayloadSlice{PS: []tools.Payload{*p}}

	statusCode, _, _ := mycenaeTools.HTTP.POST("api/put", ps.Marshal())
	assert.Equal(t, 204, statusCode)
	time.Sleep(tools.Sleep3)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(p.Metric, p.Tags)

	assertMycenae(t, ksMycenae, *p.Timestamp, *p.Timestamp, *p.Value, hashID)

	assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)

	*p.Value = 6.1

	sendRESTPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2PayloadsWithSameMetricTagsTimestampTwoEqualTags(t *testing.T) {
	t.Parallel()

	value1 := 5.0
	value2 := 6.0
	metric, tagKey1, tagValue1, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()
	tagKey2 := tagKey1
	tagValue2 := tagValue1

	payload1 := fmt.Sprintf(
		`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "%v": "%v"}, "timestamp": %v}]`,
		value1,
		metric,
		ksMycenae,
		tagKey1,
		tagValue1,
		tagKey2,
		tagValue2,
		timestamp,
	)

	payload2 := fmt.Sprintf(
		`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v", "%v": "%v"}, "timestamp": %v}]`,
		value2,
		metric,
		ksMycenae,
		tagKey1,
		tagValue1,
		tagKey2,
		tagValue2,
		timestamp,
	)

	tags := map[string]string{tagKey1: tagValue1}

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, tags)

	statusCode, _ := mycenaeTools.HTTP.POSTstring("api/put", payload1)
	assert.Equal(t, 204, statusCode)

	statusCode, _ = mycenaeTools.HTTP.POSTstring("api/put", payload2)
	assert.Equal(t, 204, statusCode)
	time.Sleep(tools.Sleep3)

	assertMycenae(t, ksMycenae, timestamp, timestamp, float32(value2), hashID)

	assertElastic(t, ksMycenae, metric, tags, hashID)
}

func TestRESTv2PayloadWithStringValue(t *testing.T) {
	t.Parallel()

	value := "testValue"
	metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

	payload := fmt.Sprintf(
		`[{"value": "%v", "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}]`,
		value,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	tags := map[string]string{tagKey: tagValue}

	sendRESTPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)
}

func TestRESTv2PayloadWithValueNotSent(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	p.Value = nil

	errMessage := "Wrong Format: Field \"value\" is required. NO information will be saved"

	sendRESTPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), ksMycenae, p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2PayloadWithEmptyValues(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	wg.Add(6)

	go func() {
		// empty value
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"value":, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}]`,
			metric,
			ksMycenae,
			tagKey,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, tagKey: tagValue}

		sendRESTPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)

		wg.Done()
	}()

	go func() {
		// empty metric
		_, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"value": %.1f, "metric":, "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}]`,
			1.0,
			ksMycenae,
			tagKey,
			tagValue,
			timestamp,
		)
		metric := ""
		tags := map[string]string{"ksid": ksMycenae, tagKey: tagValue}

		sendRESTPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)

		wg.Done()
	}()

	go func() {
		// empty ksid
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"value": %.1f, "metric": "%v", "tags": {"ksid":, "%v": "%v"}, "timestamp": %v}]`,
			2.0,
			metric,
			tagKey,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": "", tagKey: tagValue}

		sendRESTPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)

		wg.Done()
	}()

	go func() {
		// empty tag key
		metric, _, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", : "%v"}, "timestamp": %v}]`,
			3.0,
			metric,
			ksMycenae,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, "": tagValue}

		sendRESTPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)

		wg.Done()
	}()

	go func() {
		// empty tag value
		metric, tagKey, _, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v":}, "timestamp": %v}]`,
			4.0,
			metric,
			ksMycenae,
			tagKey,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, tagKey: ""}

		sendRESTPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)

		wg.Done()
	}()

	go func() {
		// empty timestamp
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp":}]`,
			5.0,
			metric,
			ksMycenae,
			tagKey,
			tagValue,
		)
		tags := map[string]string{"ksid": ksMycenae, tagKey: tagValue}

		sendRESTPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, time.Now().Add(tools.Sleep3).Unix())

		wg.Done()
	}()
	wg.Wait()
}

func TestRESTv2PayloadWithInvalidChars(t *testing.T) {
	t.Parallel()

	invalidChars := []string{" ", "space between", "\\", "?", "!", "@", "$", "*", "(", ")", "{", "}", "[", "]", "|", "+", "=", "`", "^", "~", ",", ":", "<", ">", "ü"}

	var wgOut sync.WaitGroup
	wgOut.Add(len(invalidChars))

	for _, invalidChar := range invalidChars {

		go func(char string) {

			var wgIn sync.WaitGroup
			wgIn.Add(3)

			go testInvalidMetric(t, char, &wgIn, false)
			go testInvalidTagKey(t, char, &wgIn, false)
			go testInvalidTagValue(t, char, &wgIn, false)

			wgIn.Wait()
			wgOut.Done()

		}(invalidChar)
	}
	wgOut.Wait()
}

func TestRESTv2PayloadWithInvalidCharsAtOnce(t *testing.T) {
	t.Parallel()

	tests := make([]byte, 3)

	invalidChars := []string{" ", "space between", "\\", "?", "!", "@", "$", "*", "(", ")", "{", "}", "[", "]", "|", "+", "=", "`", "^", "~", ",", ":", "<", ">", "ü"}

	payload := []tools.Payload{}

	timestamp := time.Now().Unix()

	for _, invalidChar := range invalidChars {

		for test := range tests {

			p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
			*p.Timestamp = timestamp

			switch test {
			case 0:
				p.Metric = invalidChar
			case 1:
				p.Tags = map[string]string{"ksid": ksMycenae, invalidChar: p.TagValue}
			case 2:
				p.Tags[p.TagKey] = invalidChar
			}

			payload = append(payload, *p)
		}
	}

	ps := tools.PayloadSlice{PS: payload}

	statusCode, resp, err := mycenaeTools.HTTP.POST("api/put", ps.Marshal())
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, 400, statusCode)
	time.Sleep(tools.Sleep3)

	var restError tools.RestErrors

	err = json.Unmarshal(resp, &restError)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Equal(t, len(payload), len(restError.Errors))
	assert.Equal(t, len(payload), restError.Failed)
	assert.Equal(t, 0, restError.Success)

	for i, err := range restError.Errors {

		assert.Equal(t, *payload[0].Value, restError.Errors[i].Datapoint.Value)
		assert.Equal(t, len(payload[0].Tags), len(restError.Errors[i].Datapoint.Tags))
		assert.Equal(t, payload[0].Tags["ksid"], restError.Errors[i].Datapoint.Tags["ksid"])
		assert.Contains(t, err.Error, "Wrong Format: ")
	}

	for _, p := range payload {

		hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(p.Metric, p.Tags)

		assertMycenaeEmpty(t, p.Tags["ksid"], timestamp, timestamp, hashID)
		assertElasticEmpty(t, p.Tags["ksid"], p.Metric, p.Tags, hashID)
	}
}

func TestRESTv2PayloadValuesWithOnlySpace(t *testing.T) {
	t.Parallel()

	space := " "

	var wg sync.WaitGroup
	wg.Add(6)

	go func() {
		// value case
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"value": %v, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}]`,
			space,
			metric,
			ksMycenae,
			tagKey,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, tagKey: tagValue}

		sendRESTPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// metric case
		_, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}]`,
			1.0,
			space,
			ksMycenae,
			tagKey,
			tagValue,
			timestamp,
		)
		metric := space
		tags := map[string]string{"ksid": ksMycenae, tagKey: tagValue}

		errMessage := "Wrong Format: Field \"metric\" ( ) is not well formed. NO information will be saved"

		sendRESTPayloadStringAndAssertErrorAndEmpty(t, errMessage, payload, ksMycenae, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// keyspace case
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}]`,
			2.0,
			metric,
			space,
			tagKey,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": space, tagKey: tagValue}

		errMessage := "Wrong Format: Tag ksid ( ) is not well formed. NO information will be saved"

		sendRESTPayloadStringAndAssertErrorAndEmpty(t, errMessage, payload, space, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// tag key case
		metric, _, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}]`,
			3.0,
			metric,
			ksMycenae,
			space,
			tagValue,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, space: tagValue}

		errMessage := "Wrong Format: Tag key ( ) is not well formed. NO information will be saved"

		sendRESTPayloadStringAndAssertErrorAndEmpty(t, errMessage, payload, ksMycenae, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// tag value case
		metric, tagKey, _, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}]`,
			4.0,
			metric,
			ksMycenae,
			tagKey,
			space,
			timestamp,
		)
		tags := map[string]string{"ksid": ksMycenae, tagKey: space}

		errMessage := "Wrong Format: Tag value ( ) is not well formed. NO information will be saved"

		sendRESTPayloadStringAndAssertErrorAndEmpty(t, errMessage, payload, ksMycenae, metric, tags, timestamp, timestamp)
		wg.Done()
	}()

	go func() {
		// timestamp case
		metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

		payload := fmt.Sprintf(
			`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}]`,
			5.0,
			metric,
			ksMycenae,
			tagKey,
			tagValue,
			space,
		)
		tags := map[string]string{"ksid": ksMycenae, tagKey: tagValue}

		sendRESTPayloadStringAndAssertEmpty(t, payload, metric, tags, timestamp, time.Now().Add(tools.Sleep3).Unix())
		wg.Done()
	}()
	wg.Wait()
}

func TestRESTv2PayloadWithAKsidTag(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	delete(p.Tags, p.TagKey)

	errMessage := `Wrong Format: At least two tags are required. One of each must be "ksid". NO information will be saved`
	sendRESTPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), p.Tags["ksid"], p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2PayloadWithoutKsid(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	delete(p.Tags, "ksid")

	errMessage := `Wrong Format: At least two tags are required. One of each must be "ksid". NO information will be saved`
	sendRESTPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), "", p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2PayloadWithInvalidKsid(t *testing.T) {
	t.Parallel()

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Tags["ksid"] = "ksMycenae"

	errMessage := "Wrong Format: Tag ksid (ksMycenae) is not well formed. NO information will be saved"

	sendRESTPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), p.Tags["ksid"], p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
}

func TestRESTv2PayloadWithInvalidTimestamp(t *testing.T) {
	t.Parallel()

	dateBefore := time.Now().Unix()

	value := 5.0
	metric, tagKey, tagValue, _ := mycenaeTools.Mycenae.GetRandomMetricTags()
	timestamp := 9999999.9

	payload := fmt.Sprintf(
		`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v}]`,
		value,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	sendRESTPayloadStringAndAssertEmpty(t, payload, metric, map[string]string{tagKey: tagValue}, dateBefore, time.Now().Add(tools.Sleep3).Unix())
}

func TestRESTv2PayloadWithStringTimestamp(t *testing.T) {
	t.Parallel()

	value := 5.0
	metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

	payload := fmt.Sprintf(
		`[{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": "%v"}]`,
		value,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	sendRESTPayloadStringAndAssertEmpty(t, payload, metric, map[string]string{tagKey: tagValue}, timestamp, time.Now().Add(tools.Sleep3).Unix())
}

func TestRESTv2PayloadWithBadFormatedJson(t *testing.T) {
	t.Parallel()

	value := 5.0
	metric, tagKey, tagValue, timestamp := mycenaeTools.Mycenae.GetRandomMetricTags()

	payload := fmt.Sprintf(
		`{"value": %.1f, "metric": "%v", "tags": {"ksid": "%v", "%v": "%v"}, "timestamp": %v`,
		value,
		metric,
		ksMycenae,
		tagKey,
		tagValue,
		timestamp,
	)

	sendRESTPayloadStringAndAssertEmpty(t, payload, metric, map[string]string{tagKey: tagValue}, timestamp, timestamp)
}

func TestRESTv2PayloadWithTwoPoints(t *testing.T) {
	t.Parallel()

	p1 := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	p2 := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	ps := tools.PayloadSlice{PS: []tools.Payload{*p1, *p2}}

	sendRESTPayloadWithMoreThanAPointAndAssertPoints(t, ps, false)
}

func TestRESTv2PayloadWithTwoPointsWithHeaderGZIP(t *testing.T) {
	t.Parallel()

	p1 := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	p1.TagKey2 = fmt.Sprint("testTagKey2-", p1.Random)
	p1.TagValue2 = fmt.Sprint("testTagValue2-", p1.Random)
	p1.Tags[p1.TagKey2] = p1.TagValue2

	p2 := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	p2.TagKey2 = fmt.Sprint("testTagKey2-", p2.Random)
	p2.TagValue2 = fmt.Sprint("testTagValue2-", p2.Random)
	p2.Tags[p2.TagKey2] = p2.TagValue2

	ps := tools.PayloadSlice{PS: []tools.Payload{*p1, *p2}}

	sendRESTPayloadWithMoreThanAPointAndAssertPoints(t, ps, true)
}

func TestRESTv2PayloadWithTwoPointsWithTwoTagsEach(t *testing.T) {
	t.Parallel()

	p1 := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	p1.TagKey2 = fmt.Sprint("testTagKey2-", p1.Random)
	p1.TagValue2 = fmt.Sprint("testTagValue2-", p1.Random)
	p1.Tags[p1.TagKey2] = p1.TagValue2

	p2 := mycenaeTools.Mycenae.GetPayload(ksMycenae)
	p2.TagKey2 = fmt.Sprint("testTagKey2-", p2.Random)
	p2.TagValue2 = fmt.Sprint("testTagValue2-", p2.Random)
	p2.Tags[p2.TagKey2] = p2.TagValue2

	ps := tools.PayloadSlice{PS: []tools.Payload{*p1, *p2}}

	sendRESTPayloadWithMoreThanAPointAndAssertPoints(t, ps, false)
}

func TestRESTv2PayloadWithTwoPointsAndAWrongFormatEmptyString(t *testing.T) {
	t.Parallel()

	cases := make([]byte, 3)

	wg := sync.WaitGroup{}
	wg.Add(len(cases))

	for test := range cases {

		go func(test int) {

			pInvalid := mycenaeTools.Mycenae.GetPayload(ksMycenae)
			var errMessage string

			switch test {
			case 0:
				pInvalid.Metric = ""
				pInvalid.TagKey2 = fmt.Sprint("testTagKey2-", pInvalid.Random)
				pInvalid.TagValue2 = fmt.Sprint("testTagValue2-", pInvalid.Random)
				pInvalid.Tags[pInvalid.TagKey2] = pInvalid.TagValue2
				errMessage = "Wrong Format: Field \"metric\" () is not well formed. NO information will be saved"
			case 1:
				pInvalid.TagKey2 = ""
				pInvalid.TagValue2 = fmt.Sprint("testTagValue2-", pInvalid.Random)
				pInvalid.Tags[pInvalid.TagKey2] = pInvalid.TagValue2
				errMessage = "Wrong Format: Tag key () is not well formed. NO information will be saved"
			case 2:
				pInvalid.TagValue2 = ""
				pInvalid.TagKey2 = fmt.Sprint("testTagKey2-", pInvalid.Random)
				pInvalid.Tags[pInvalid.TagKey2] = pInvalid.TagValue2
				errMessage = "Wrong Format: Tag value () is not well formed. NO information will be saved"
			}

			p2 := mycenaeTools.Mycenae.GetPayload(ksMycenae)
			p2.TagKey2 = fmt.Sprint("testTagKey2-", p2.Random)
			p2.TagValue2 = fmt.Sprint("testTagValue2-", p2.Random)
			p2.Tags[p2.TagKey2] = p2.TagValue2

			ps := tools.PayloadSlice{PS: []tools.Payload{*pInvalid, *p2}}

			sendRESTPayloadWithMoreThanAPointAndAssertError(t, errMessage, ps, 0)

			wg.Done()
		}(test)
	}
	wg.Wait()
}

func TestRESTv2EmptyPayload(t *testing.T) {
	t.Parallel()

	payload := fmt.Sprintf(`[]`)

	statusCode, resp := mycenaeTools.HTTP.POSTstring("api/put", payload)
	assert.Equal(t, 400, statusCode)

	expectedErr := tools.Error{
		Error:   "no points",
		Message: "no points",
	}

	receivedErr := tools.Error{}

	err := json.Unmarshal(resp, &receivedErr)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	assert.Equal(t, expectedErr, receivedErr)
}

func TestRESTv2BucketLimits(t *testing.T) {
	t.Parallel()

	timestamps := [6]int64{
		time.Date(2016, time.March, 6, 23, 59, 59, 0, time.UTC).Unix(),
		time.Date(2016, time.March, 7, 00, 00, 01, 0, time.UTC).Unix(),
		time.Date(2016, time.March, 6, 23, 59, 59, 0, time.UTC).AddDate(0, 0, 7).Unix(),
		time.Date(2016, time.March, 7, 00, 00, 01, 0, time.UTC).AddDate(0, 0, 7).Unix(),
		time.Date(2016, time.March, 6, 23, 59, 59, 0, time.UTC).AddDate(0, 0, 14).Unix(),
		time.Date(2016, time.March, 7, 00, 00, 01, 0, time.UTC).AddDate(0, 0, 14).Unix(),
	}

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(p.Metric, p.Tags)

	for i := 0; i < len(timestamps); i++ {

		*p.Value = float32(i)
		*p.Timestamp = timestamps[i]

		statusCode, _ := mycenaeTools.HTTP.POSTstring("api/put", p.StringArray())
		assert.Equal(t, 204, statusCode)
	}

	time.Sleep(tools.Sleep3)

	for i := 0; i < len(timestamps); i++ {

		assertMycenae(t, ksMycenae, timestamps[i], timestamps[i], float32(i), hashID)
	}

	countValue := mycenaeTools.Cassandra.Timeseries.CountValueFromIDSTAMP(ksMycenae, fmt.Sprintf("%v%v%v", 2016, 9, hashID))
	assert.Equal(t, 1, countValue)

	countValue = mycenaeTools.Cassandra.Timeseries.CountValueFromIDSTAMP(ksMycenae, fmt.Sprintf("%v%v%v", 2016, 10, hashID))
	assert.Equal(t, 2, countValue)

	countValue = mycenaeTools.Cassandra.Timeseries.CountValueFromIDSTAMP(ksMycenae, fmt.Sprintf("%v%v%v", 2016, 11, hashID))
	assert.Equal(t, 2, countValue)

	countValue = mycenaeTools.Cassandra.Timeseries.CountValueFromIDSTAMP(ksMycenae, fmt.Sprintf("%v%v%v", 2016, 12, hashID))
	assert.Equal(t, 1, countValue)

	assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)
}

func TestRESTv2Bucket53WeeksYear(t *testing.T) {
	t.Parallel()

	timestamps := [6]int64{
		time.Date(2015, time.December, 31, 23, 59, 59, 0, time.UTC).Unix(),
		time.Date(2016, time.January, 1, 00, 00, 01, 0, time.UTC).Unix(),
		time.Date(2016, time.January, 1, 23, 59, 59, 0, time.UTC).Unix(),
		time.Date(2016, time.January, 3, 00, 00, 01, 0, time.UTC).Unix(),
		time.Date(2016, time.January, 3, 23, 59, 59, 0, time.UTC).Unix(),
		time.Date(2016, time.January, 4, 00, 00, 01, 0, time.UTC).Unix(),
	}

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(p.Metric, p.Tags)

	for i := 0; i < len(timestamps); i++ {

		*p.Value = float32(i)
		*p.Timestamp = timestamps[i]

		statusCode, _ := mycenaeTools.HTTP.POSTstring("api/put", p.StringArray())
		assert.Equal(t, 204, statusCode)
	}

	time.Sleep(tools.Sleep3)

	for i := 0; i < len(timestamps); i++ {

		assertMycenae(t, ksMycenae, timestamps[i], timestamps[i], float32(i), hashID)
	}

	countValue := mycenaeTools.Cassandra.Timeseries.CountValueFromIDSTAMP(ksMycenae, fmt.Sprintf("%v%v%v", 2015, 53, hashID))
	assert.Equal(t, 5, countValue)

	countValue = mycenaeTools.Cassandra.Timeseries.CountValueFromIDSTAMP(ksMycenae, fmt.Sprintf("%v%v%v", 2016, 1, hashID))
	assert.Equal(t, 1, countValue)

	assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)
}

func TestRESTv2Bucket52WeeksYear(t *testing.T) {
	t.Parallel()

	timestamps := [6]int64{
		time.Date(2014, time.December, 28, 00, 00, 01, 0, time.UTC).Unix(),
		time.Date(2014, time.December, 28, 23, 59, 59, 0, time.UTC).Unix(),
		time.Date(2014, time.December, 29, 00, 00, 01, 0, time.UTC).Unix(),
		time.Date(2014, time.December, 29, 23, 59, 59, 0, time.UTC).Unix(),
		time.Date(2015, time.January, 1, 00, 00, 01, 0, time.UTC).Unix(),
		time.Date(2015, time.January, 1, 23, 59, 59, 0, time.UTC).Unix(),
	}

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(p.Metric, p.Tags)

	for i := 0; i < len(timestamps); i++ {

		*p.Value = float32(i)
		*p.Timestamp = timestamps[i]

		statusCode, _ := mycenaeTools.HTTP.POSTstring("api/put", p.StringArray())
		assert.Equal(t, 204, statusCode)
	}

	time.Sleep(tools.Sleep3)

	for i := 0; i < len(timestamps); i++ {

		assertMycenae(t, ksMycenae, timestamps[i], timestamps[i], float32(i), hashID)
	}

	countValue := mycenaeTools.Cassandra.Timeseries.CountValueFromIDSTAMP(ksMycenae, fmt.Sprintf("%v%v%v", 2014, 52, hashID))
	assert.Equal(t, 2, countValue)

	countValue = mycenaeTools.Cassandra.Timeseries.CountValueFromIDSTAMP(ksMycenae, fmt.Sprintf("%v%v%v", 2014, 53, hashID))
	assert.Equal(t, 0, countValue)

	countValue = mycenaeTools.Cassandra.Timeseries.CountValueFromIDSTAMP(ksMycenae, fmt.Sprintf("%v%v%v", 2015, 1, hashID))
	assert.Equal(t, 4, countValue)

	assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)
}

func TestRESTv2BucketFullYear(t *testing.T) {
	t.Parallel()

	timestamps := [52]int64{}
	day := time.Date(2014, time.January, 1, 12, 00, 01, 0, time.UTC)

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(p.Metric, p.Tags)

	for i := 0; i < len(timestamps); i++ {

		*p.Value = float32(i)
		*p.Timestamp = day.Unix()
		timestamps[i] = *p.Timestamp

		statusCode, _ := mycenaeTools.HTTP.POSTstring("api/put", p.StringArray())
		assert.Equal(t, 204, statusCode)

		day = day.AddDate(0, 0, 7)
	}

	time.Sleep(tools.Sleep3)

	for i := 0; i < len(timestamps); i++ {

		assertMycenae(t, ksMycenae, timestamps[i], timestamps[i], float32(i), hashID)

		countValue := mycenaeTools.Cassandra.Timeseries.CountValueFromIDSTAMP(ksMycenae, fmt.Sprintf("%v%v%v", 2014, i+1, hashID))
		assert.Equal(t, 1, countValue)
	}

	assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)
}

func TestRESTv2BucketFullPastYearAtOnce(t *testing.T) {
	t.Parallel()

	payload := make([]tools.Payload, 52)

	firstWeek := time.Date(2014, time.January, 1, 12, 00, 01, 0, time.UTC)

	metric, tagk, tagv, _ := mycenaeTools.Mycenae.GetRandomMetricTags()

	for i := 0; i < len(payload); i++ {

		value := float32(i)
		timestamp := time.Unix(firstWeek.Unix(), 0).AddDate(0, 0, i*7).Unix()

		p := tools.Payload{
			Value:     &value,
			Metric:    metric,
			Tags:      map[string]string{"ksid": ksMycenae, tagk: tagv},
			Timestamp: &timestamp,
		}

		payload[i] = p
	}

	ps := tools.PayloadSlice{PS: payload}

	statusCode, _, _ := mycenaeTools.HTTP.POST("api/put", ps.Marshal())
	assert.Equal(t, 204, statusCode)
	time.Sleep(tools.Sleep3)

	for _, p := range payload {

		hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(p.Metric, p.Tags)

		assertMycenae(t, ksMycenae, *p.Timestamp, *p.Timestamp, *p.Value, hashID)

		year, week := time.Unix(*p.Timestamp, 0).ISOWeek()
		bucket := fmt.Sprintf("%v%v%v", year, week, hashID)

		countValue := mycenaeTools.Cassandra.Timeseries.CountValueFromIDSTAMP(ksMycenae, bucket)
		assert.Equal(t, 1, countValue, bucket)

		assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)
	}
}

func TestRESTv2BucketFuturePoints(t *testing.T) {
	t.Parallel()

	var currentBucket, lastBucket string

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(p.Metric, p.Tags)

	for i := 0; i < 3; i++ {

		*p.Value = float32(i)

		statusCode, _ := mycenaeTools.HTTP.POSTstring("api/put", p.StringArray())
		assert.Equal(t, 204, statusCode)
		time.Sleep(tools.Sleep3)

		assertMycenae(t, ksMycenae, *p.Timestamp, *p.Timestamp, *p.Value, hashID)

		year, week := time.Unix(*p.Timestamp, 0).ISOWeek()
		currentBucket = fmt.Sprintf("%v%v%v", year, week, hashID)

		if lastBucket != "" {

			countValue := mycenaeTools.Cassandra.Timeseries.CountValueFromIDSTAMP(ksMycenae, lastBucket)
			assert.Equal(t, 1, countValue)
		}

		countValue := mycenaeTools.Cassandra.Timeseries.CountValueFromIDSTAMP(ksMycenae, currentBucket)
		assert.Equal(t, 0, countValue)

		lastBucket = currentBucket
		*p.Timestamp = time.Unix(*p.Timestamp, 0).AddDate(0, 0, 7).Unix()
	}

	assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)
}

func TestRESTv2BucketFuturePointsAtOnceAndThenPast(t *testing.T) {
	t.Parallel()

	payload := make([]tools.Payload, 3)

	metric, tagk, tagv, now := mycenaeTools.Mycenae.GetRandomMetricTags()
	tags := map[string]string{"ksid": ksMycenae, tagk: tagv}

	for i := 0; i < len(payload); i++ {

		value := float32(i)
		timestamp := time.Unix(now, 0).AddDate(0, 0, i*7).Unix()

		p := tools.Payload{
			Value:     &value,
			Metric:    metric,
			Tags:      tags,
			Timestamp: &timestamp,
		}

		payload[i] = p
	}

	ps := tools.PayloadSlice{PS: payload}

	statusCode, _, _ := mycenaeTools.HTTP.POST("api/put", ps.Marshal())
	assert.Equal(t, 204, statusCode)
	time.Sleep(tools.Sleep3)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, tags)

	for i, p := range payload {

		assertMycenae(t, ksMycenae, *p.Timestamp, *p.Timestamp, *p.Value, hashID)

		year, week := time.Unix(*p.Timestamp, 0).ISOWeek()
		bucket := fmt.Sprintf("%v%v%v", year, week, hashID)

		countValue := mycenaeTools.Cassandra.Timeseries.CountValueFromIDSTAMP(ksMycenae, bucket)

		if i < len(payload)-1 {

			assert.Equal(t, 1, countValue, bucket)

		} else {

			assert.Equal(t, 0, countValue, bucket)
		}
	}

	assertElastic(t, ksMycenae, metric, tags, hashID)

	value := float32(len(payload))
	timestamp := time.Unix(now, 0).AddDate(0, 0, -7).Unix()

	p := tools.Payload{
		Value:     &value,
		Metric:    metric,
		Tags:      tags,
		Timestamp: &timestamp,
	}

	year, week := time.Unix(*p.Timestamp, 0).ISOWeek()
	bucket := fmt.Sprintf("%v%v%v", year, week, hashID)

	statusCode, _ = mycenaeTools.HTTP.POSTstring("api/put", p.StringArray())
	assert.Equal(t, 204, statusCode)
	time.Sleep(tools.Sleep3)

	assertMycenae(t, ksMycenae, *p.Timestamp, *p.Timestamp, *p.Value, hashID)

	countValue := mycenaeTools.Cassandra.Timeseries.CountValueFromIDSTAMP(ksMycenae, bucket)
	assert.Equal(t, 1, countValue)
}

func TestRESTv2BucketFuturePointsDifferentSeriesAtOnce(t *testing.T) {
	t.Parallel()

	payload := make([]tools.Payload, 3)

	for i := 0; i < len(payload); i++ {

		p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

		*p.Value = float32(i)
		*p.Timestamp = time.Unix(*p.Timestamp, 0).AddDate(0, 0, i*7).Unix()

		payload[i] = *p
	}

	ps := tools.PayloadSlice{PS: payload}

	statusCode, _, _ := mycenaeTools.HTTP.POST("api/put", ps.Marshal())
	assert.Equal(t, 204, statusCode)
	time.Sleep(tools.Sleep3)

	for _, p := range payload {

		hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(p.Metric, p.Tags)

		assertMycenae(t, ksMycenae, *p.Timestamp, *p.Timestamp, *p.Value, hashID)

		assertElastic(t, ksMycenae, p.Metric, p.Tags, hashID)

		year, week := time.Unix(*p.Timestamp, 0).ISOWeek()
		bucket := fmt.Sprintf("%v%v%v", year, week, hashID)

		countValue := mycenaeTools.Cassandra.Timeseries.CountValueFromIDSTAMP(ksMycenae, bucket)

		assert.Equal(t, 0, countValue, bucket)
	}
}

func testMetric(t *testing.T, value string, wg *sync.WaitGroup, udp bool) {

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Metric = value

	if udp {
		sendUDPPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
	} else {
		sendRESTPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
	}
	wg.Done()
}

func testTagKey(t *testing.T, value string, wg *sync.WaitGroup, udp bool) {

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Tags = map[string]string{"ksid": ksMycenae, value: p.TagValue}

	if udp {
		sendUDPPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
	} else {
		sendRESTPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
	}
	wg.Done()
}

func testTagValue(t *testing.T, value string, wg *sync.WaitGroup, udp bool) {

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Tags[p.TagKey] = value

	if udp {
		sendUDPPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
	} else {
		sendRESTPayloadAndAssertPoint(t, p, *p.Timestamp, *p.Timestamp)
	}
	wg.Done()
}

func testInvalidMetric(t *testing.T, value string, wg *sync.WaitGroup, udp bool) {

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Metric = value

	if udp {
		sendUDPPayloadStringAndAssertEmpty(t, string(p.Marshal()), p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
	} else {
		errMessage := fmt.Sprintf("Wrong Format: Field \"metric\" (%s) is not well formed. NO information will be saved", value)
		sendRESTPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), ksMycenae, p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
	}
	wg.Done()
}

func testInvalidTagKey(t *testing.T, value string, wg *sync.WaitGroup, udp bool) {

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Tags = map[string]string{"ksid": ksMycenae, value: p.TagValue}

	if udp {
		sendUDPPayloadStringAndAssertEmpty(t, string(p.Marshal()), p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
	} else {
		errMessage := fmt.Sprintf("Wrong Format: Tag key (%s) is not well formed. NO information will be saved", value)
		sendRESTPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), ksMycenae, p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
	}
	wg.Done()
}

func testInvalidTagValue(t *testing.T, value string, wg *sync.WaitGroup, udp bool) {

	p := mycenaeTools.Mycenae.GetPayload(ksMycenae)

	p.Tags[p.TagKey] = value

	if udp {
		sendUDPPayloadStringAndAssertEmpty(t, string(p.Marshal()), p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
	} else {
		errMessage := fmt.Sprintf("Wrong Format: Tag value (%s) is not well formed. NO information will be saved", value)
		sendRESTPayloadStringAndAssertErrorAndEmpty(t, errMessage, p.StringArray(), ksMycenae, p.Metric, p.Tags, *p.Timestamp, *p.Timestamp)
	}
	wg.Done()
}

func sendRESTPayloadAndAssertPoint(t *testing.T, payload *tools.Payload, start, end int64) {

	ps := tools.PayloadSlice{PS: []tools.Payload{*payload}}

	statusCode, _, _ := mycenaeTools.HTTP.POST("api/put", ps.Marshal())
	assert.Equal(t, 204, statusCode)

	time.Sleep(tools.Sleep3)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(payload.Metric, payload.Tags)

	assertMycenae(t, ksMycenae, start, end, *payload.Value, hashID)

	assertElastic(t, ksMycenae, payload.Metric, payload.Tags, hashID)
}

// payload must be composed by point(s) with Timestamp(s) != nil
func sendRESTPayloadWithMoreThanAPointAndAssertPoints(t *testing.T, payload tools.PayloadSlice, gzipit bool) {

	if gzipit {
		statusCode, _, _ := mycenaeTools.HTTP.POSTgziped("api/put", payload.Marshal())
		assert.Equal(t, 204, statusCode)
	} else {
		statusCode, _, _ := mycenaeTools.HTTP.POST("api/put", payload.Marshal())
		assert.Equal(t, 204, statusCode)
	}

	time.Sleep(tools.Sleep3)

	for _, point := range payload.PS {

		hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(point.Metric, point.Tags)

		assertMycenae(t, ksMycenae, *point.Timestamp, *point.Timestamp, *point.Value, hashID)

		assertElastic(t, ksMycenae, point.Metric, point.Tags, hashID)
	}
}

func sendRESTPayloadStringAndAssertEmpty(t *testing.T, payload, metric string, tags map[string]string, start, end int64) {

	statusCode, _ := mycenaeTools.HTTP.POSTstring("api/put", payload)
	assert.Equal(t, 400, statusCode)

	time.Sleep(tools.Sleep3)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, tags)

	assertMycenaeEmpty(t, ksMycenae, start, end, hashID)

	assertElasticEmpty(t, ksMycenae, metric, tags, hashID)
}

// Payload must represent an array of length = 1
func sendRESTPayloadStringAndAssertErrorAndEmpty(t *testing.T, errMessage, payload, keyspace, metric string, tags map[string]string, start, end int64) {

	statusCode, resp := mycenaeTools.HTTP.POSTstring("api/put", payload)
	assert.Equal(t, 400, statusCode)

	var restError tools.RestErrors

	err := json.Unmarshal(resp, &restError)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	payStruct := tools.PayloadSlice{}.PS

	err = json.Unmarshal([]byte(payload), &payStruct)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	assert.Len(t, payStruct, 1)

	assertRESTError(t, restError, &payStruct[0], keyspace, errMessage, 1, 0)

	time.Sleep(tools.Sleep3)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(metric, tags)

	assertMycenaeEmpty(t, ksMycenae, start, end, hashID)

	assertElasticEmpty(t, ksMycenae, metric, tags, hashID)
}

func sendRESTPayloadWithMoreThanAPointAndAssertError(t *testing.T, errMessage string, payload tools.PayloadSlice, invalidPointPosition int) {

	statusCode, resp, _ := mycenaeTools.HTTP.POST("api/put", payload.Marshal())
	assert.Equal(t, 400, statusCode)

	var restError tools.RestErrors

	err := json.Unmarshal(resp, &restError)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	invalidPoint := &payload.PS[invalidPointPosition]

	assertRESTError(t, restError, invalidPoint, ksMycenae, errMessage, 1, len(payload.PS)-1)

	hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(invalidPoint.Metric, invalidPoint.Tags)

	assertMycenaeEmpty(t, ksMycenae, *invalidPoint.Timestamp, *invalidPoint.Timestamp, hashID)

	assertElasticEmpty(t, ksMycenae, invalidPoint.Metric, invalidPoint.Tags, hashID)

	time.Sleep(tools.Sleep3)

	for index, point := range payload.PS {

		if index != invalidPointPosition {

			hashID := mycenaeTools.Cassandra.Timeseries.GetHashFromMetricAndTags(point.Metric, point.Tags)

			assertMycenae(t, ksMycenae, *point.Timestamp, *point.Timestamp, *point.Value, hashID)

			assertElastic(t, ksMycenae, point.Metric, point.Tags, hashID)
		}
	}
}
