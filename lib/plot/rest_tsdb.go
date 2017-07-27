package plot

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rip"

	"github.com/uol/mycenae/lib/parser"
	"github.com/uol/mycenae/lib/structs"
	"github.com/uol/mycenae/lib/utils"
)

func (plot *Plot) Lookup(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	keyspace := ps.ByName("keyspace")
	if keyspace == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/api/search/lookup", "keyspace": "empty"})
		rip.Fail(w, errNotFound("Lookup"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/api/search/lookup", "keyspace": keyspace})

	m := r.URL.Query().Get("m")

	if m == "" {
		gerr := errValidationS("Lookup", `missing query parameter "m"`)
		rip.Fail(w, gerr)
		return
	}

	metric, tags, gerr := parseQuery(m)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	tagMap := map[string][]string{}

	for _, tag := range tags {
		if tag.Key == "" {
			tag.Key = "*"
		}
		if tag.Value == "" {
			tag.Key = "*"
		}
		if _, ok := tagMap[tag.Key]; !ok {
			tagMap[tag.Key] = []string{
				tag.Value,
			}
		} else {
			if tag.Value == "*" {
				tagMap[tag.Key] = []string{
					tag.Value,
				}
			} else {
				if tagMap[tag.Key][0] != "*" {
					tagMap[tag.Key] = append(tagMap[tag.Key], tag.Value)
				}
			}
		}
	}

	tsds, total, gerr := plot.MetaOpenTSDB(keyspace, "", metric, tagMap, int64(10000), int64(0))
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	look := TSDBlookup{
		Type:         "LOOKUP",
		Metric:       metric,
		Tags:         tags,
		Results:      tsds,
		TotalResults: total,
	}

	rip.SuccessJSON(w, http.StatusOK, look)
	return
}

func (plot *Plot) Suggest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	keyspace := ps.ByName("keyspace")
	if keyspace == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/api/suggest", "keyspace": "empty"})
		rip.Fail(w, errNotFound("Suggest"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/api/suggest", "keyspace": keyspace})

	q := r.URL.Query()

	maxStr := q.Get("max")
	max := 25
	resp := []string{}

	var err error

	if maxStr != "" {

		max, err = strconv.Atoi(maxStr)
		if err != nil {
			gerr := errValidationE("Suggest", err)
			rip.Fail(w, gerr)
			return
		}
	}

	var gerr gobol.Error

	switch q.Get("type") {
	case "":
		gerr = errValidationS("Suggest", "type required")
		rip.Fail(w, gerr)
		return
	case "metrics":
		q := fmt.Sprintf("%v.*", q.Get("q"))
		resp, _, gerr = plot.ListMetrics(keyspace, "metric", q, int64(max), int64(0))
	case "tagk":
		q := fmt.Sprintf("%v.*", q.Get("q"))
		resp, _, gerr = plot.ListTagKey(keyspace, q, int64(max), int64(0))
	case "tagv":
		q := fmt.Sprintf("%v.*", q.Get("q"))
		resp, _, gerr = plot.ListTagValue(keyspace, q, int64(max), int64(0))
	default:
		gerr = errValidationS("Suggest", "unsopported type")
		rip.Fail(w, gerr)
		return
	}
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	sort.Strings(resp)

	rip.SuccessJSON(w, http.StatusOK, resp)
	return
}

func (plot *Plot) Query(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	keyspace := ps.ByName("keyspace")
	if keyspace == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/api/query", "keyspace": "empty"})
		rip.Fail(w, errNotFound("Query"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/api/query", "keyspace": keyspace})

	_, found, gerr := plot.boltc.GetKeyspace(keyspace)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}
	if !found {
		gerr := errNotFound("Query")
		rip.Fail(w, gerr)
		return
	}

	query := structs.TSDBqueryPayload{}

	gerr = rip.FromJSON(r, &query)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	resps, gerr := plot.getTimeseries(keyspace, query)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	if len(resps) == 0 {
		rip.SuccessJSON(w, http.StatusOK, []string{})
		return
	}

	rip.SuccessJSON(w, http.StatusOK, resps)
	return
}

func (plot *Plot) getTimeseries(
	keyspace string,
	query structs.TSDBqueryPayload,
) (resps TSDBresponses, gerr gobol.Error) {

	if query.Relative != "" {
		now := time.Now()
		start, gerr := parser.GetRelativeStart(now, query.Relative)
		if gerr != nil {
			return resps, gerr
		}
		query.Start = start.Unix()
		query.End = now.Unix()
	} else {

		if query.Start == 0 {
			return resps, errValidationS("getTimeseries", "start cannot be zero")
		}

		t0, err := utils.MilliToSeconds(query.Start)
		if err != nil {
			return resps, errValidationS("getTimeseries", err.Error())
		}
		query.Start = t0

		if query.End == 0 {
			query.End = time.Now().Unix()
		} else {
			tn, err := utils.MilliToSeconds(query.End)
			if err != nil {
				return resps, errValidationS("getTimeseries", err.Error())
			}
			query.End = tn
		}

		if query.End < query.Start {
			return resps, errValidationS(
				"getTimeseries",
				fmt.Sprintf("end date %v should be equal or bigger than start date %v", query.End, query.Start),
			)
		}
	}

	oldDs := structs.Downsample{}

	for _, q := range query.Queries {

		if q.Downsample != "" {

			ds := strings.Split(q.Downsample, "-")
			var unit string
			var val int

			if string(ds[0][len(ds[0])-2:]) == "ms" {
				unit = ds[0][len(ds[0])-2:]
				val, _ = strconv.Atoi(ds[0][:len(ds[0])-2])
			} else {
				unit = ds[0][len(ds[0])-1:]
				val, _ = strconv.Atoi(ds[0][:len(ds[0])-1])
			}

			apporx := ds[1]

			if apporx == "count" {
				apporx = "pnt"
			}

			switch unit {
			case "ms":
				oldDs.Options.Unit = "sec"
			case "s":
				oldDs.Options.Unit = "sec"
			case "m":
				oldDs.Options.Unit = "min"
			case "h":
				oldDs.Options.Unit = "hour"
			case "d":
				oldDs.Options.Unit = "day"
			case "w":
				oldDs.Options.Unit = "week"
			case "n":
				oldDs.Options.Unit = "month"
			case "y":
				oldDs.Options.Unit = "year"
			}

			if len(ds) == 3 {
				oldDs.Options.Fill = ds[2]
			} else {
				oldDs.Options.Fill = "none"
			}

			oldDs.Options.Downsample = apporx
			oldDs.Options.Value = val
			oldDs.Enabled = true

		}

		for k, v := range q.Tags {

			members := strings.Split(v, "|")
			filter := structs.TSDBfilter{}

			if members[0] == "*" || len(members) > 1 {
				filter = structs.TSDBfilter{
					Ftype:   "wildcard",
					Tagk:    k,
					Filter:  v,
					GroupBy: true,
				}
			} else {
				filter = structs.TSDBfilter{
					Ftype:   "wildcard",
					Tagk:    k,
					Filter:  v,
					GroupBy: false,
				}
			}

			q.Filters = append(q.Filters, filter)
		}

		//tagMap := map[string][]string{}
		//
		//for _, filter := range q.Filters {
		//	if _, ok := tagMap[filter.Tagk]; ok {
		//		tagMap[filter.Tagk] = append(tagMap[filter.Tagk], filter.Filter)
		//	} else {
		//		tagMap[filter.Tagk] = []string{filter.Filter}
		//	}
		//}

		tsobs, total, gerr := plot.MetaFilterOpenTSDB(keyspace, "", q.Metric, q.Filters, int64(plot.MaxTimeseries))
		if gerr != nil {
			return resps, gerr
		}

		if total > plot.LogQueryThreshold {
			statsQueryThreshold(keyspace)
			gblog.Sugar().Warnf("TS THRESHOLD EXEECED: %+v", query)
		}

		if total > plot.MaxTimeseries {
			statsQueryLimit(keyspace)
			return TSDBresponses{}, errValidationS(
				"getTimeseries",
				fmt.Sprintf(
					"query exedded the maximum allowed number of timeseries. max is %d and the query returned %d",
					plot.MaxTimeseries,
					total,
				),
			)
		}

		if len(tsobs) == 0 {
			continue
		}

		groups := plot.GetGroups(q.Filters, tsobs)

		for _, group := range groups {
			ids := []string{}
			tagK := make(map[string]map[string]string)

			for _, tsd := range group {

				for k, v := range tsd.Tags {
					if _, ok := tagK[k]; ok {
						tagK[k][v] = ""
					} else {
						tagK[k] = map[string]string{
							v: "",
						}
					}
				}

				ids = append(ids, tsd.Tsuid)

			}

			aggTags := []string{}

			if q.RateOptions.CounterMax == nil {
				var maxInt int64
				maxInt = 1<<63 - 1
				q.RateOptions.CounterMax = &maxInt
			}

			filterV := structs.FilterValueOperation{}

			if q.FilterValue != "" {
				filterV.Enabled = true
				if q.FilterValue[:2] == ">=" || q.FilterValue[:2] == "<=" || q.FilterValue[:2] == "==" {
					val, err := strconv.ParseFloat(q.FilterValue[2:], 32)
					if err != nil {
						return resps, errValidationE("getTimeseries", err)
					}
					filterV.BoolOper = q.FilterValue[:2]
					filterV.Value = float32(val)
				} else if q.FilterValue[:1] == ">" || q.FilterValue[:1] == "<" {
					val, err := strconv.ParseFloat(q.FilterValue[1:], 64)
					if err != nil {
						return resps, errValidationE("getTimeseries", err)
					}
					filterV.BoolOper = q.FilterValue[:1]
					filterV.Value = float32(val)
				}
			}

			merge := q.Aggregator

			if q.Aggregator == "count" {
				merge = "pnt"
			}

			opers := structs.DataOperations{
				Downsample: oldDs,
				Merge:      merge,
				Rate: structs.RateOperation{
					Enabled: q.Rate,
					Options: q.RateOptions,
				},
				FilterValue: filterV,
				Order:       q.Order,
			}

			keepEmpty := false

			if oldDs.Options.Fill != "none" {
				keepEmpty = true
			}

			serie, gerr := plot.GetTimeSeries(
				keyspace,
				ids,
				query.Start,
				query.End,
				opers,
				query.MsResolution,
				keepEmpty,
			)
			if gerr != nil {
				return resps, gerr
			}

			for k, kv := range tagK {
				if len(kv) > 1 {
					aggTags = append(aggTags, k)
				}
			}

			sort.Strings(aggTags)
			if serie.Data.Len() > 0 {
				tagsU := make(map[string]string)
				for k, kv := range tagK {
					if len(kv) == 1 {
						for v := range kv {
							tagsU[k] = v
						}
					}
				}

				resp := TSDBresponse{
					Metric:         q.Metric,
					Tags:           tagsU,
					AggregatedTags: aggTags,
					Dps: &TSMarshaler{
						fill:  oldDs.Options.Fill,
						milli: query.MsResolution,
						data:  serie.Data,
					},
				}

				if query.ShowTSUIDs {
					resp.Tsuids = ids
				}
				resps = append(resps, resp)
			}
		}
	}

	sort.Sort(resps)
	return resps, gerr
}

func parseQuery(query string) (string, []Tag, gobol.Error) {

	metric, sub := getMetric(query)

	tags, gerr := getTags(sub)

	return metric, tags, gerr
}

func getMetric(query string) (string, string) {

	mr := []rune{}

	for i, r := range query {
		if string(r) == "{" {
			return strings.TrimSpace(string(mr)), query[i:]
		}
		mr = append(mr, r)
	}

	return strings.TrimSpace(string(mr)), ""
}

func getTags(query string) ([]Tag, gobol.Error) {

	if len(query) == 0 {
		return []Tag{}, nil
	}

	if string(query[0]) != "{" || string(query[len(query)-1]) != "}" {
		return []Tag{}, errValidationS("getTags", `Missing '}' at the end of query`)
	}

	query = query[1:]

	mr := []rune{}

	var key string

	tags := []Tag{}

	for _, r := range query {

		if string(r) == "=" {
			key = strings.TrimSpace(string(mr))
			mr = []rune{}
			continue
		}

		if string(r) == "," || string(r) == "}" {
			if key == "" {
				return []Tag{}, errValidationS("getTags", `invalid tag format`)
			}

			tag := Tag{
				Key:   key,
				Value: strings.TrimSpace(string(mr)),
			}

			tags = append(tags, tag)
			mr = []rune{}
			continue
		}

		mr = append(mr, r)
	}

	return tags, nil
}
