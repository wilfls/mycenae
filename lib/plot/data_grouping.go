package plot

import (
	"math"
	"time"

	"github.com/uol/mycenae/lib/storage"
	"github.com/uol/mycenae/lib/structs"
)

const (
	secMin  = 60
	secHour = secMin * 60
	secDay  = secHour * 24
	secWeek = secDay * 7
)

func basic(totalPoints int, serie storage.Pnts) (groupSerie storage.Pnts) {

	total := len(serie)

	group := float64(total) / float64(totalPoints)

	group = round(group, .5, 0)

	var counter float64

	var avgCounter float64

	var nilCounter float64

	var groupDate int64

	var groupValue float64

	for i, point := range serie {

		groupDate += point.Date

		if point.Empty {

			nilCounter++

		} else {

			groupValue += point.Value

			avgCounter++

		}

		counter++

		if counter == group || i == total-1 {

			groupDate = groupDate / int64(counter)

			groupValue = groupValue / avgCounter

			var groupPoint storage.Pnt

			groupPoint.Date = groupDate

			groupPoint.Value = groupValue

			if nilCounter == counter {
				groupPoint.Empty = true
			}

			groupSerie = append(groupSerie, groupPoint)

			counter = 0

			avgCounter = 0

			nilCounter = 0

			groupDate = 0

			groupValue = 0

		}

	}

	return

}

func rate(options structs.TSDBrateOptions, serie storage.Pnts) storage.Pnts {

	if len(serie) == 1 {
		return serie
	}

	rateSerie := storage.Pnts{}

	for i := 1; i < len(serie); i++ {

		if serie[i].Empty || serie[i-1].Empty {
			p := storage.Pnt{
				Date:  serie[i].Date,
				Empty: true,
			}
			rateSerie = append(rateSerie, p)
			continue
		}

		var value float64

		if options.Counter && serie[i].Value < serie[i-1].Value {
			value = (float64(*options.CounterMax) + serie[i].Value - serie[i-1].Value) / float64((serie[i].Date/int64(1000))-(serie[i-1].Date/int64(1000)))
			if options.ResetValue != 0 && float64(options.ResetValue) <= value {
				value = 0
			}
		} else {
			value = (serie[i].Value - serie[i-1].Value) / float64((serie[i].Date/int64(1000))-(serie[i-1].Date/int64(1000)))
		}

		p := storage.Pnt{
			Value: value,
			Date:  serie[i].Date,
			Empty: false,
		}

		rateSerie = append(rateSerie, p)
	}

	return rateSerie
}

func downsample(options structs.DSoptions, keepEmpties bool, start, end int64, serie storage.Pnts) storage.Pnts {

	startDate := time.Unix(start, 0)

	switch options.Unit {
	case "sec":
		base := time.Date(
			startDate.Year(),
			startDate.Month(),
			startDate.Day(),
			startDate.Hour(),
			startDate.Minute(),
			startDate.Second(),
			0,
			time.Local,
		)
		start = base.Unix()
	case "min":
		base := time.Date(
			startDate.Year(),
			startDate.Month(),
			startDate.Day(),
			startDate.Hour(),
			startDate.Minute(),
			0,
			0,
			time.Local,
		)
		start = base.Unix()
	case "hour":
		base := time.Date(
			startDate.Year(),
			startDate.Month(),
			startDate.Day(),
			startDate.Hour(),
			0,
			0,
			0,
			time.Local,
		)
		start = base.Unix()
	case "day":
		base := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, time.Local)
		start = base.Unix()
	case "week":
		base := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, time.Local)
		for base.Weekday() != time.Monday {
			base = base.AddDate(0, 0, -1)
		}
		start = base.Unix()
	case "month":
		base := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, time.Local)
		for base.Month() == startDate.Month() {
			base = base.AddDate(0, 0, -1)
		}
		base = base.AddDate(0, 0, 1)
		start = base.Unix()
	case "year":
		base := time.Date(startDate.Year(), time.January, 1, 0, 0, 0, 0, time.Local)
		start = base.Unix()
	}

	groupDate := start

	endInterval := getEndInterval(start, options.Unit, options.Value)

	var groupedCount float64

	groupedPoint := storage.Pnt{}

	groupedSerie := storage.Pnts{}

	for i := 0; i < len(serie); i++ {

		point := serie[i]

		//Ajusting for missing points
		for point.Date >= endInterval {
			if keepEmpties {
				groupedPoint.Date = groupDate

				if options.Fill == "zero" {
					groupedPoint.Value = 0
				} else {
					groupedPoint.Empty = true
				}

				groupedSerie = append(groupedSerie, groupedPoint)

				groupedPoint = storage.Pnt{}
			}

			groupDate = endInterval

			endInterval = getEndInterval(endInterval, options.Unit, options.Value)
		}

		groupedCount++

		switch options.Downsample {
		case "avg":
			groupedPoint.Value += point.Value
		case "sum":
			groupedPoint.Value += point.Value
		case "max":
			if groupedCount == 1 {
				groupedPoint.Value = point.Value
			}
			if point.Value > groupedPoint.Value {
				groupedPoint.Value = point.Value
			}
		case "min":
			if groupedCount == 1 {
				groupedPoint.Value = point.Value
			}
			if point.Value < groupedPoint.Value {
				groupedPoint.Value = point.Value
			}
		case "pnt":
			groupedPoint.Value = groupedCount
		}

		if i+1 == len(serie) || serie[i+1].Date >= endInterval {

			groupedPoint.Date = groupDate

			groupDate = endInterval

			if options.Downsample == "avg" {
				groupedPoint.Value = groupedPoint.Value / groupedCount
			}

			groupedSerie = append(groupedSerie, groupedPoint)

			groupedCount = 0

			groupedPoint = storage.Pnt{}

			if i+1 != len(serie) {
				endInterval = getEndInterval(endInterval, options.Unit, options.Value)
			}
		}

	}

	if keepEmpties {
		for i := endInterval; i < end; i = endInterval {

			groupedPoint := storage.Pnt{
				Date: endInterval,
			}

			if options.Fill == "zero" {
				groupedPoint.Value = 0
			} else {
				groupedPoint.Empty = true
			}

			groupedSerie = append(groupedSerie, groupedPoint)

			endInterval = getEndInterval(i, options.Unit, options.Value)
		}
	}

	return groupedSerie
}

func getEndInterval(start int64, unit string, value int) int64 {

	var end int64

	switch unit {
	case "sec":
		end = start + int64(value)
	case "min":
		end = start + secMin*int64(value)
	case "hour":
		end = start + secHour*int64(value)
	case "day":
		end = start + secDay*int64(value)
	case "week":
		end = start + secWeek*int64(value)
	case "month":
		startDate := time.Unix(start, 0)

		base := time.Date(startDate.Year(), startDate.Month(), 1, 0, 0, 0, 0, time.Local)

		base = base.AddDate(0, value, 0)

		end = base.Unix()
	case "year":
		startDate := time.Unix(start, 0)

		base := time.Date(startDate.Year(), time.January, 1, 0, 0, 0, 0, time.Local)

		base = base.AddDate(value, 0, 0)

		end = base.Unix()
	default:
		return end
	}

	return end
}

func merge(mergeType string, keepEmpties bool, serie storage.Pnts) storage.Pnts {

	mergedSerie := storage.Pnts{}

	for i := 0; i < len(serie); i++ {

		point := serie[i]

		var mergedPoint storage.Pnt

		if i < len(serie)-1 {

			j := i + 1

			nextPoint := serie[j]

			var mergedCount, nullCount float64

			mergedPoint = point

			mergedCount++

			if point.Empty {
				nullCount++
			}

			for point.Date == nextPoint.Date {

				i++

				mergedCount++

				if !nextPoint.Empty {

					mergedPoint.Empty = false

					switch mergeType {
					case "avg":
						mergedPoint.Value = mergedPoint.Value + nextPoint.Value
					case "sum":
						mergedPoint.Value = mergedPoint.Value + nextPoint.Value
					case "max":
						if nextPoint.Value > mergedPoint.Value {
							mergedPoint = nextPoint
						}
					case "min":
						if nextPoint.Value < mergedPoint.Value {
							mergedPoint = nextPoint
						}
					case "pnt":
						mergedPoint.Value = mergedCount
					}
				} else {
					nullCount++
				}

				if j == len(serie)-1 {
					break
				}

				j++

				nextPoint = serie[j]
			}

			if mergedCount-nullCount > 1 {

				if mergeType == "avg" {
					mergedPoint.Value = mergedPoint.Value / mergedCount
				}

			} else if nullCount == mergedCount && keepEmpties {
				mergedPoint.Empty = true
			}

		} else {
			mergedPoint = point
		}

		mergedSerie = append(mergedSerie, mergedPoint)

	}

	return mergedSerie
}

func filterValues(oper structs.FilterValueOperation, serie storage.Pnts) storage.Pnts {

	filteredSerie := storage.Pnts{}

	switch oper.BoolOper {
	case "<":
		for _, pnt := range serie {
			if pnt.Value < oper.Value {
				filteredSerie = append(filteredSerie, pnt)
			}
		}
	case ">":
		for _, pnt := range serie {
			if pnt.Value > oper.Value {
				filteredSerie = append(filteredSerie, pnt)
			}
		}
	case "==":
		for _, pnt := range serie {
			if pnt.Value == oper.Value {
				filteredSerie = append(filteredSerie, pnt)
			}
		}
	case ">=":
		for _, pnt := range serie {
			if pnt.Value >= oper.Value {
				filteredSerie = append(filteredSerie, pnt)
			}
		}
	case "<=":
		for _, pnt := range serie {
			if pnt.Value <= oper.Value {
				filteredSerie = append(filteredSerie, pnt)
			}
		}
	}

	return filteredSerie
}

func round(val float64, roundOn float64, places int) (newVal float64) {

	var round float64

	pow := math.Pow(10, float64(places))

	digit := pow * val

	_, div := math.Modf(digit)

	if div >= roundOn {
		round = math.Ceil(digit)
	} else {
		round = math.Floor(digit)
	}

	newVal = round / pow

	return

}
