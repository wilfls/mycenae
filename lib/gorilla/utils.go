package gorilla

import "time"

const (
	hour       = 3600
	day        = 24 * hour
	maxBlocks  = 12
	headerSize = 12
)

// BlockID returns an UTC timestamp at
// the head of the even hour based on the
// given timestamp
func BlockID(timestamp int64) int64 {
	now := time.Unix(timestamp, 0).UTC()

	_, m, s := now.Clock()
	now = now.Add(-(time.Duration(m) * time.Minute) - (time.Duration(s) * time.Second))

	if now.Hour()%2 == 0 {
		return now.Unix()
	}

	return now.Unix() - hour
}

func getIndex(timestamp int64) int {

	return time.Unix(timestamp, 0).UTC().Hour() / 2

}
