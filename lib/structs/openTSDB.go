package structs

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/config"
)

var (
	validFields = regexp.MustCompile(`^[0-9A-Za-z-._%&#;\\/]+$`)
	validFwild  = regexp.MustCompile(`^[0-9A-Za-z-._%&#;\\/*]+$`)
	validFor    = regexp.MustCompile(`^[0-9A-Za-z-._%&#;\\/|]+$`)
)

type TSDBqueryPayload struct {
	Start        int64       `json:"start,omitempty"`
	End          int64       `json:"end,omitempty"`
	Relative     string      `json:"relative,omitempty"`
	Queries      []TSDBquery `json:"queries"`
	ShowTSUIDs   bool        `json:"showTSUIDs"`
	MsResolution bool        `json:"msResolution"`
	ShowQuery    bool        `json:"showQuery"`
}

type TSDBquery struct {
	Aggregator   string            `json:"aggregator"`
	Metric       string            `json:"metric"`
	TSUIDs       []string          `json:"tsuids"`
	Downsample   *string           `json:"downsample"`
	Rate         bool              `json:"rate"`
	Filters      []TSDBfilter      `json:"filters"`
	Index        *int              `json:"index,omitempty"`
	Tags         map[string]string `json:"tags"`
	FilterTagKs  []string          `json:"filterTagKs"`
	RateOptions  *TSDBrateOptions  `json:"rateOptions"`
	Order        []string          `json:"order,omitempty"`
	FilterValue  string            `json:"filterValue,omitempty"`
	ExplicitTags bool              `json:"explicitTags"`
}

type TSDBfilter struct {
	Tagk        string `json:"tagk"`
	Filter      string `json:"filter"`
	GroupBy     bool   `json:"groupBy,omitempty"`
	GroupByResp bool   `json:"group_by"`
	Ftype       string `json:"type"`
}

type TSDBrateOptions struct {
	Counter    bool   `json:"counter"`
	CounterMax *int64 `json:"counterMax,omitempty"`
	ResetValue int64  `json:"resetValue,omitempty"`
}

func (query TSDBqueryPayload) Validate() gobol.Error {

	if query.Relative != "" {
		if err := query.checkDuration(query.Relative); err != nil {
			return err
		}
	}

	if len(query.Queries) == 0 {
		return errValidation(errors.New("At least one quey should be present"))
	}

	for i, q := range query.Queries {

		if err := query.checkField("metric", q.Metric); err != nil {
			return err
		}

		if err := query.checkAggregator(q.Aggregator); err != nil {
			return err
		}

		if q.Downsample != nil && *q.Downsample != "" {

			ds := strings.Split(*q.Downsample, "-")

			if len(ds) < 2 {
				return errValidation(errors.New("invalid downsample format"))
			}

			if err := query.checkDuration(ds[0]); err != nil {
				return err
			}

			if err := query.checkDownsampler(ds[1]); err != nil {
				return err
			}

			if len(ds) > 2 {
				if err := query.checkFiller(ds[2]); err != nil {
					return err
				}
			}

		}

		if q.Rate && q.RateOptions != nil {
			if err := query.checkRate(*q.RateOptions); err != nil {
				return err
			}
		}

		if q.FilterValue != "" {
			q.FilterValue = strings.Replace(q.FilterValue, " ", "", -1)
			query.Queries[i].FilterValue = q.FilterValue

			if len(q.FilterValue) < 2 {
				return errValidation(fmt.Errorf("invalid filter value %s", q.FilterValue))
			}

			if q.FilterValue[:2] == ">=" || q.FilterValue[:2] == "<=" || q.FilterValue[:2] == "==" {
				_, err := strconv.ParseFloat(q.FilterValue[2:], 64)
				if err != nil {
					return errValidation(err)
				}
			} else if q.FilterValue[:1] == ">" || q.FilterValue[:1] == "<" {
				_, err := strconv.ParseFloat(q.FilterValue[1:], 64)
				if err != nil {
					return errValidation(err)
				}
			} else {
				return errValidation(fmt.Errorf("invalid filter value %s", q.FilterValue))
			}
		}

		if len(q.Order) == 0 {

			if q.FilterValue != "" {
				query.Queries[i].Order = append(query.Queries[i].Order, "filterValue")
			}

			if q.Downsample != nil && *q.Downsample != "" {
				query.Queries[i].Order = append(query.Queries[i].Order, "downsample")
			}

			query.Queries[i].Order = append(query.Queries[i].Order, "aggregation")

			if q.Rate {
				query.Queries[i].Order = append(query.Queries[i].Order, "rate")
			}

		} else {

			orderCheck := make([]string, len(q.Order))

			copy(orderCheck, q.Order)

			k := 0
			occur := 0
			for j, order := range orderCheck {

				if order == "aggregation" {
					k = j
					occur++
				}

			}

			if occur == 0 {
				return errValidation(
					errors.New("aggregation configured but no aggregation found in order array"),
				)
			}

			if occur > 1 {
				return errValidation(errors.New("more than one aggregation found in order array"))
			}

			if occur == 1 {
				orderCheck = append(orderCheck[:k], orderCheck[k+1:]...)
			}

			k = 0
			occur = 0
			for j, order := range orderCheck {

				if order == "filterValue" {
					k = j
					occur++
				}

			}

			if q.FilterValue != "" && occur == 0 {
				return errValidation(
					errors.New("filterValue configured but no filterValue found in order array"),
				)
			}

			if occur > 1 {
				return errValidation(errors.New("more than one filterValue found in order array"))
			}

			if occur == 1 {
				orderCheck = append(orderCheck[:k], orderCheck[k+1:]...)
			}

			k = 0
			occur = 0
			for j, order := range orderCheck {

				if order == "downsample" {
					k = j
					occur++
				}

			}

			if q.Downsample != nil && *q.Downsample != "" && occur == 0 {
				return errValidation(
					errors.New(
						"downsample configured but no downsample found in order array",
					),
				)
			}

			if occur > 1 {
				return errValidation(errors.New("more than one downsample found in order array"))
			}

			if occur == 1 {
				orderCheck = append(orderCheck[:k], orderCheck[k+1:]...)
			}

			k = 0
			occur = 0
			for j, order := range orderCheck {

				if order == "rate" {
					k = j
					occur++
				}

			}

			if q.Rate && occur == 0 {
				return errValidation(errors.New("rate configured but no rate found in order array"))
			}

			if occur > 1 {
				return errValidation(errors.New("more than one rate found in order array"))
			}

			if occur == 1 {
				orderCheck = append(orderCheck[:k], orderCheck[k+1:]...)
			}

			if len(orderCheck) != 0 {
				return errValidation(fmt.Errorf("invalid operations in order array %v", orderCheck))
			}

		}

		if err := query.checkFilter(q.Filters); err != nil {
			return err
		}

	}

	i := 0
	msTime := query.Start

	for {
		msTime = msTime / 10
		if msTime == 0 {
			break
		}
		i++
	}

	if i > 13 {
		return errValidation(errors.New("the maximum resolution suported for timestamp is milliseconds"))

	}

	if i > 10 {
		query.Start = query.Start / 1000
	}

	return nil
}

func (query TSDBqueryPayload) checkRate(opts TSDBrateOptions) gobol.Error {

	if opts.CounterMax != nil && *opts.CounterMax < 0 {
		return errRate("counter max needs to be a positive integer")
	}

	return nil
}

func (query TSDBqueryPayload) checkAggregator(aggr string) gobol.Error {

	ok := false

	for _, vAggr := range config.GetAggregators() {
		if vAggr == aggr {
			ok = true
			break
		}
	}

	if !ok {
		return errAggregator("unkown aggregation value")
	}

	return nil
}

func (query TSDBqueryPayload) checkDownsampler(DSr string) gobol.Error {

	ok := false

	for _, vDSr := range config.GetDownsamplers() {
		if vDSr == DSr {
			ok = true
			break
		}
	}

	if !ok {
		return errDownsampler("Invalid downsample")
	}

	return nil
}

func (query TSDBqueryPayload) checkFiller(DSf string) gobol.Error {

	ok := false

	for _, vDSf := range config.GetFillers() {
		if vDSf == DSf {
			ok = true
			break
		}
	}

	if !ok {
		return errFiller("Invalid fill value")
	}

	return nil
}

func (query TSDBqueryPayload) checkFilter(filters []TSDBfilter) gobol.Error {

	vFilters := config.GetFilters()

	for _, filter := range filters {

		ok := false

		ft := filter.Ftype

		if ft == "iliteral_or" {
			ft = "literal_or"
		} else if ft == "not_iliteral_or" {
			ft = "not_literal_or"
		} else if ft == "iwildcard" {
			ft = "wildcard"
		}

		for _, vFilter := range vFilters {
			if ft == vFilter {
				ok = true
				break
			}
		}
		if !ok {
			return errFilter(fmt.Sprintf("Invalid filter type %s", filter.Ftype))
		}

		if err := query.checkField("tagk", filter.Tagk); err != nil {
			return err
		}

		if err := query.checkFilterField("filter", ft, filter.Filter); err != nil {
			return err
		}
	}

	return nil
}

func (query TSDBqueryPayload) checkDuration(s string) gobol.Error {

	if len(s) < 2 {
		return errCheckDuration(errors.New("Invalid time interval"))
	}

	var n int
	var err error

	if string(s[len(s)-2:]) == "ms" {
		n, err = strconv.Atoi(string(s[:len(s)-2]))
		if err != nil {
			return errCheckDuration(err)
		}
		return nil
	}

	switch s[len(s)-1:] {
	case "s", "m", "h", "d", "w", "n", "y":
		n, err = strconv.Atoi(string(s[:len(s)-1]))
		if err != nil {
			return errCheckDuration(err)
		}
	default:
		return errCheckDuration(errors.New("Invalid unit"))
	}

	if n < 1 {
		return errCheckDuration(errors.New("interval needs to be bigger than 0"))
	}

	return nil
}

func (query TSDBqueryPayload) checkField(n, f string) gobol.Error {

	if !validFields.MatchString(f) {
		return errField(fmt.Sprintf("Invalid characters in field %s: %s", n, f))
	}
	return nil
}

func (query TSDBqueryPayload) checkFilterField(n, tf, f string) gobol.Error {

	match := false

	switch tf {
	case "wildcard":
		match = validFwild.MatchString(f)
	case "literal_or", "not_literal_or":
		match = validFor.MatchString(f)
	case "regexp":
		match = true
	}

	if !match {
		return errFilterField(fmt.Sprintf("Invalid characters in field %s: %s", n, f))
	}

	return nil
}
