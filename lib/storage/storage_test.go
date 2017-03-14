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

func Test2hPointsPerMinute(t *testing.T) {

	strg := New(nil, nil, nil)

	now := time.Now()
	start := now.Add(-2 * time.Hour)
	end := now

	currentTime := start
	for end.After(currentTime) {
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
	strg := New(nil, nil, nil)

	now := time.Now()
	start := now.Add(-1 * time.Hour)
	end := now

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

	if count != 1536 {
		t.Fatalf("number of inserted poits %v is differente from readed count %v\n", inserted, count)
	}

	t.Logf("inserted: %v\tcount: %v\n", inserted, count)
}

func Test4hPointsPerMinute(t *testing.T) {
	strg := New(nil, nil, nil)

	now := time.Now()
	start := now.Add(-4 * time.Hour)
	end := now

	currentTime := start
	for end.After(currentTime) {
		strg.Add(keyspace, key, currentTime.Unix(), rand.Float64())
		currentTime = currentTime.Add(time.Minute)
	}

	bucketIndex := strg.getSerie(keyspace, key).index

	if bucketIndex > 1 {
		t.Fatalf("bucket index bigger than expected: %d", bucketIndex)
	}
	if bucketIndex < 1 {
		t.Fatalf("bucket lower than expected: %d", bucketIndex)
	}

	_, count, err := strg.Read(keyspace, key, start.Unix(), start.Add(119*time.Minute).Unix(), false)
	if err != nil {
		t.Fatal(err)
	}
	if count > 120 {
		t.Fatalf("Number of points bigger than expected: %d", count)
	}
	if count < 120 {
		t.Fatalf("Number of points lower than expected: %d", count)
	}
	t.Logf("start: %v\tend: %v\tpoints: %v\n", start.Unix(), start.Add(119*time.Minute).Unix(), count)

	_, count, err = strg.Read(keyspace, key, start.Unix(), end.Unix(), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("start: %v\tend: %v\tpoints: %v\n", start.Unix(), end.Unix(), count)
	if count > 240 {
		t.Fatalf("Number of points bigger than expected: %d", count)
	}
	if count < 240 {
		t.Fatalf("Number of points lower than expected: %d", count)
	}
	t.Logf("start: %v\tend: %v\tpoints: %v\n", start.Unix(), start.Add(119*time.Minute).Unix(), count)
}

func BenchmarkInsertPoints1Serie(b *testing.B) {
	strg := New(nil, nil, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strg.Add(keyspace, key, time.Now().Unix(), rand.Float64())
	}
	b.StopTimer()
}

func BenchmarkReadPoints1Serie(b *testing.B) {
	strg := New(nil, nil, nil)

	now := time.Now().Unix()
	ptsCount := 1000000

	for i := 0; i < ptsCount; i++ {
		strg.Add(keyspace, key, time.Now().Unix(), rand.Float64())
	}

	end := now + int64(ptsCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strg.Read(keyspace, key, now, end, false)
	}
	b.StopTimer()
}

func BenchmarkInsertPointsMultiSeries(b *testing.B) {
	strg := New(nil, nil, nil)

	ks := []string{"a", "b", "c", "d"}
	k := []string{"x", "p", "t", "o"}

	now := time.Now().Unix()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strg.Add(ks[rand.Intn(3)], k[rand.Intn(3)], now+int64(i), rand.Float64())
	}
	b.StopTimer()

}

func BenchmarkReadPointsMultiSeries(b *testing.B) {
	strg := New(nil, nil, nil)

	ks := []string{"a", "b", "c", "d"}
	k := []string{"x", "p", "t", "o"}

	now := time.Now().Unix()
	ptsCount := 1000000
	for i := 0; i < ptsCount; i++ {
		strg.Add(ks[rand.Intn(3)], k[rand.Intn(3)], now+int64(i), rand.Float64())
	}

	end := now + int64(ptsCount)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strg.Read(ks[rand.Intn(3)], k[rand.Intn(3)], now, end, false)
	}
	b.StopTimer()

}
