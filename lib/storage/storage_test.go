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

func Test1hPointsPerMinute1Bucket(t *testing.T) {

	strg := New(nil, nil, nil)

	now := time.Now()
	start := now.Add(-1 * time.Hour)
	end := now

	currentTime := start
	for end.After(currentTime) {
		//t.Logf("current: %v\n", currentTime.UnixNano()/1e6)
		strg.Add(keyspace, key, currentTime.Unix()*1e3, rand.Float64())
		currentTime = currentTime.Add(time.Minute)
	}

	id := strg.id(keyspace, key)
	nBuckets := len(strg.getSerie(id).buckets)
	if nBuckets > 1 {
		t.Fatalf("Number of buckets bigger than expected: %d", nBuckets)
	}
	if nBuckets < 1 {
		t.Fatalf("Number of buckets lower than expected: %d", nBuckets)
	}

	t.Logf("start: %v\tend: %v\tbuckets: %v\n", start.Unix()*1e3, end.Unix()*1e3, nBuckets)

}

func Test1hPointsPerSecondNumberBuckets(t *testing.T) {
	strg := New(nil, nil, nil)

	now := time.Now()
	start := now.Add(-1 * time.Hour)
	end := now

	currentTime := start
	for end.After(currentTime) {
		strg.Add(keyspace, key, currentTime.Unix()*1e3, rand.Float64())
		currentTime = currentTime.Add(time.Second)
	}

	id := strg.id(keyspace, key)
	nBuckets := len(strg.getSerie(id).buckets)
	if nBuckets > 57 {
		t.Fatalf("Number of buckets bigger than expected: %d", nBuckets)
	}
	if nBuckets < 57 {
		t.Fatalf("Number of buckets lower than expected: %d", nBuckets)
	}
	t.Logf("start: %v\tend: %v\tbuckets: %v\n", start.Unix()*1e3, end.Unix()*1e3, nBuckets)

}

func Test4hPointsPerMinuteNumberBktsBktSize(t *testing.T) {
	strg := New(nil, nil, nil)

	now := time.Now()
	start := now.Add(-4 * time.Hour)
	end := now

	currentTime := start
	for i := 0; i < 240; i++ {
		strg.Add(keyspace, key, currentTime.Unix()*1e3, rand.Float64())
		currentTime = currentTime.Add(time.Minute)
	}

	id := strg.id(keyspace, key)
	nBuckets := len(strg.getSerie(id).buckets)
	if nBuckets > 4 {
		t.Fatalf("Number of buckets bigger than expected: %d", nBuckets)
	}
	if nBuckets < 4 {
		t.Fatalf("Number of buckets lower than expected: %d", nBuckets)
	}

	for _, bkt := range strg.getSerie(id).buckets {
		//delta := bkt.points[bkt.index-1].Date - bkt.points[0].Date
		//t.Logf("bucket %v: index: %v start: %v end: %v delta: %v\n", i, bkt.index, bkt.points[0].Date, bkt.points[bkt.index-1].Date, delta)
		if bkt.index > 60 {
			t.Fatalf("Bucket size bigger than expected: %d", bkt.index)
		}
		if bkt.index < 60 {
			t.Fatalf("Bucket size lower than expected: %d", bkt.index)
		}
	}

	t.Logf("start: %v\tend: %v\tbuckets: %v\n", start.Unix()*1e3, end.Unix()*1e3, nBuckets)

}

func BenchmarkInsertPoints1Serie(b *testing.B) {
	strg := New(nil, nil, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strg.Add(keyspace, key, time.Now().Unix()*1e3, rand.Float64())
	}
	b.StopTimer()
}

func BenchmarkReadPoints1Serie(b *testing.B) {
	strg := New(nil, nil, nil)

	now := time.Now().Unix() * 1e3
	ptsCount := 1000000

	for i := 0; i < ptsCount; i++ {
		strg.Add(keyspace, key, time.Now().Unix()*1e3, rand.Float64())
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

	now := time.Now().Unix() * 1e3
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

	now := time.Now().Unix() * 1e3
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
