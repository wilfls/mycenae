package parser

import (
	"fmt"
	"strconv"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/structs"
)

func parseRate(exp string, tsdb *structs.TSDBquery) (string, gobol.Error) {

	params := parseParams(string(exp[4:]))

	if len(params) != 4 {
		return "", errParams(
			"parseRate",
			"rate needs 4 parameters: counter, counterMax, resetValue and a function",
			fmt.Errorf("rate expects 4 parameters but found %d: %v", len(params), params),
		)
	}

	b, err := strconv.ParseBool(params[0])
	if err != nil {
		return "", errRateCounter(err)
	}

	tsdb.RateOptions = &structs.TSDBrateOptions{Counter: b}

	if params[1] != "null" {
		counterMax, err := strconv.ParseInt(params[1], 10, 64)
		if err != nil {
			return "", errRateCounterMax(err)
		}
		tsdb.RateOptions.CounterMax = &counterMax
	}

	tsdb.RateOptions.ResetValue, err = strconv.ParseInt(params[2], 10, 64)
	if err != nil {
		return "", errRateResetValue(err)
	}

	tsdb.Rate = true

	for _, oper := range tsdb.Order {
		if oper == "rate" {
			return "", errDoubleFunc("parseRate", "rate")
		}
	}

	tsdb.Order = append([]string{"rate"}, tsdb.Order...)

	return params[3], nil
}

func writeRate(exp string, rate bool, rateOptions *structs.TSDBrateOptions) string {

	if rate {
		cm := "null"

		if rateOptions != nil && rateOptions.CounterMax != nil {
			cm = fmt.Sprintf("%d", *rateOptions.CounterMax)
		}

		if rateOptions == nil {
			rateOptions = &structs.TSDBrateOptions{}
		}

		exp = fmt.Sprintf("rate(%t,%s,%d,%s)", rateOptions.Counter, cm, rateOptions.ResetValue, exp)
	}

	return exp
}
