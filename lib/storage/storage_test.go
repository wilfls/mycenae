package storage

import (
	"math/rand"
	"testing"
	"time"
)

const (
	keyspace = "test"
	key      = "test"
)

type TH struct {
	Timestamp int64
}

func (th *TH) Now() int64 {

	return th.Timestamp
}

func Test2hPointsPerMinute(t *testing.T) {

	now := time.Now()
	start := now
	end := now.Add(+2 * time.Hour)

	th := &TH{}

	th.Timestamp = start.Unix()

	strg := New(nil, nil, nil, th)

	currentTime := start
	for end.After(currentTime) {
		th.Timestamp = currentTime.Unix()
		strg.Add(keyspace, key, currentTime.Unix(), rand.Float64())
		currentTime = currentTime.Add(time.Minute)
	}

	//nBuckets := len(strg.getSerie(keyspace, key).buckets)

	pts := strg.getSerie(keyspace, key).read(start.Unix(), end.Unix())

	count := len(pts)
	if count > 120 {
		t.Fatalf("Number of points bigger than expected: %d", count)
	}
	if count < 120 {
		t.Fatalf("Number of points lower than expected: %d", count)
	}

	t.Logf("start: %v\tend: %v\tpts: %v\n", start.Unix(), end.Unix(), count)

}

func Test1hPointsPerSecond(t *testing.T) {

	now := time.Now()
	start := now
	end := now.Add(+1 * time.Hour)

	th := &TH{start.Unix()}
	strg := New(nil, nil, nil, th)

	currentTime := start
	inserted := 0
	for end.After(currentTime) {
		strg.Add(keyspace, key, currentTime.Unix(), rand.Float64())
		currentTime = currentTime.Add(time.Second)
		inserted++
	}

	_, count, err := strg.Read(keyspace, key, start.Unix(), currentTime.Unix(), false)
	if err != nil {
		t.Fatal(err)
	}

	if count != 3600 {
		t.Fatalf("number of inserted poits %v is differente from readed count %v\n", inserted, count)
	}
	t.Logf("inserted: %v\tcount: %v\n", inserted, count)

}

func Test4hPointsPerMinute(t *testing.T) {

	now := time.Now()
	start := now.Add(-4 * time.Hour)
	end := now
	th := &TH{start.Unix()}

	strg := New(nil, nil, nil, th)

	currentTime := start
	c := 0

	for end.After(currentTime) {
		v := rand.Float64()
		//fmt.Println("Inserted:", th.Now(), v)
		strg.Add(keyspace, key, currentTime.Unix(), v)
		currentTime = currentTime.Add(time.Minute)
		th.Timestamp = currentTime.Unix()

		c++
	}

	//fmt.Println("TESTE RANGE", start.Unix(), currentTime.Unix()-7260)
	_, count, err := strg.Read(keyspace, key, start.Unix(), currentTime.Unix()-7260, false)
	if err != nil {
		t.Fatal(err)
	}
	if count > 120 {
		t.Fatalf("Number of points bigger than expected: %d", count)
	}
	if count < 120 {
		t.Fatalf("Number of points lower than expected: %d", count)
	}
	t.Logf("%v\tstart: %v\tend: %v\tpoints: %v\n", c, start.Unix(), currentTime.Unix()-7260, count)

	//fmt.Println("TESTE RANGE", start.Unix(), currentTime.Unix())
	_, count, err = strg.Read(keyspace, key, start.Unix(), currentTime.Unix(), false)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%v\tstart: %v\tend: %v\tpoints: %v\n", c, start.Unix(), currentTime.Unix(), count)
	if count > 240 {
		t.Fatalf("Number of points bigger than expected: %d", count)
	}
	if count < 240 {
		t.Fatalf("Number of points lower than expected: %d", count)
	}

	//t.Logf("start: %v\tend: %v\tpoints: %v\n", start.Unix(), currentTime.Unix(), count)
}

func BenchmarkInsertPoints1Serie(b *testing.B) {
	th := &TH{time.Now().Unix()}
	strg := New(nil, nil, nil, th)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strg.Add(keyspace, key, th.Now(), rand.Float64())
		th.Timestamp++
	}
	b.StopTimer()
}

func BenchmarkReadPoints1Serie(b *testing.B) {
	ptsCount := 1000000

	now := time.Now()

	th := &TH{now.Unix() - int64(ptsCount)}

	strg := New(nil, nil, nil, th)

	start := th.Now()
	end := now.Unix()

	for i := 0; i < ptsCount; i++ {
		strg.Add(keyspace, key, th.Now(), rand.Float64())
		th.Timestamp++
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strg.Read(keyspace, key, start, end, false)
	}
	b.StopTimer()
}

func BenchmarkReadPoints1SerieMinute(b *testing.B) {
	ptsCount := 1560

	now := time.Now()
	th := &TH{now.Unix() - int64(ptsCount)*60}

	strg := New(nil, nil, nil, th)

	start := th.Now()
	end := now.Unix()

	for i := 0; i < ptsCount; i++ {
		strg.Add(keyspace, key, th.Now(), rand.Float64())
		th.Timestamp += 60
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strg.Read(keyspace, key, start, end, false)
	}
	b.StopTimer()
}

func BenchmarkReadPoints1SerieBucket(b *testing.B) {
	ptsCount := 7200

	now := time.Now()

	th := &TH{now.Unix() - int64(ptsCount)}

	strg := New(nil, nil, nil, th)

	start := th.Now()
	end := now.Unix()

	for i := 0; i < ptsCount; i++ {
		strg.Add(keyspace, key, th.Now(), rand.Float64())
		th.Timestamp++
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strg.Read(keyspace, key, start, end, false)
	}
	b.StopTimer()
}

func BenchmarkReadPoints1SerieBlock(b *testing.B) {
	ptsCount := 14400

	now := time.Now()

	th := &TH{now.Unix() - int64(ptsCount)}

	strg := New(nil, nil, nil, th)

	start := th.Now()
	end := now.Unix() - 7201

	for i := 0; i < ptsCount; i++ {
		strg.Add(keyspace, key, th.Now(), rand.Float64())
		th.Timestamp++
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strg.Read(keyspace, key, start, end, false)
	}
	b.StopTimer()
}

func BenchmarkInsertPointsMultiSeries(b *testing.B) {
	th := &TH{time.Now().Unix()}
	strg := New(nil, nil, nil, th)

	ks := []string{"a", "b", "c", "d"}
	k := []string{"x", "p", "t", "o"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		th.Timestamp++
		strg.Add(ks[rand.Intn(3)], k[rand.Intn(3)], th.Now(), rand.Float64())
	}
	b.StopTimer()

}

func BenchmarkReadPointsMultiSeries(b *testing.B) {

	now := time.Now()
	th := &TH{now.Add(-1000000 * time.Second).Unix()}
	start := th.Now()
	strg := New(nil, nil, nil, th)

	ks := []string{"a", "b", "c", "d"}
	k := []string{"x", "p", "t", "o"}

	ptsCount := 1000000
	for i := 0; i < ptsCount; i++ {
		th.Timestamp += int64(i)
		strg.Add(ks[rand.Intn(3)], k[rand.Intn(3)], th.Now(), rand.Float64())
	}

	end := start + int64(ptsCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strg.Read(ks[rand.Intn(3)], k[rand.Intn(3)], start, end, false)
	}
	b.StopTimer()
}
