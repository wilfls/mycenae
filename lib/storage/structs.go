package storage

type Pnt struct {
	Date  int64
	Value float64
	Empty bool
}

type TextPnt struct {
	Date  int64  `json:"x"`
	Value string `json:"title"`
}

type Pnts []Pnt

func (s Pnts) Len() int {
	return len(s)
}

func (s Pnts) Less(i, j int) bool {
	return s[i].Date < s[j].Date
}

func (s Pnts) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type TextPnts []TextPnt

func (s TextPnts) Len() int {
	return len(s)
}

func (s TextPnts) Less(i, j int) bool {
	return s[i].Date < s[j].Date
}

func (s TextPnts) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
