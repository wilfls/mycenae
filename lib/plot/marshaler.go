package plot

import (
	"bytes"
	"encoding/json"
	"strconv"

	"github.com/uol/mycenae/lib/gorilla"
)

// TSMarshaler defines how points are returned by the rest functions
type TSMarshaler struct {
	milli bool
	fill  string
	data  gorilla.Pnts
}

var (
	nilJSON = []byte("null")
	nanJSON = []byte("\"NaN\"")
)

// MarshalJSON implements the Marshaler interface
func (m *TSMarshaler) MarshalJSON() ([]byte, error) {
	output := bytes.NewBuffer(nil)
	output.WriteString("{")
	for i, point := range m.data {
		if i != 0 {
			output.WriteString(",")
		}
		date := point.Date
		if m.milli {
			date = date * 1000
		}

		var (
			value []byte
			err   error
		)
		if point.Empty {
			switch m.fill {
			case "null":
				value = nilJSON
			case "nan":
				value = nanJSON
			default:
				if value, err = json.Marshal(point.Value); err != nil {
					return nil, err
				}
			}
		} else if value, err = json.Marshal(point.Value); err != nil {
			return nil, err
		}

		// fmt.Fprintf(output, "\"%d\":%s", date, string(value))
		output.WriteString("\"")
		output.WriteString(strconv.Itoa(int(date)))
		output.WriteString("\":")
		output.Write(value)

	}
	output.WriteString("}")
	return output.Bytes(), nil
}
