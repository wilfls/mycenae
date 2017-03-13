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
		strg.Add(keyspace, key, currentTime.Unix(), rand.Float64())
		currentTime = currentTime.Add(time.Minute)
	}

	//nBuckets := len(strg.getSerie(keyspace, key).buckets)

	pts := strg.getSerie(keyspace, key).read(start.Unix(), end.Unix())

	/*
		if nBuckets > 1 {
			t.Fatalf("Number of buckets bigger than expected: %d", nBuckets)
		}
		if nBuckets < 1 {
			t.Fatalf("Number of buckets lower than expected: %d", nBuckets)
		}
	*/

	t.Logf("start: %v\tend: %v\tpts: %v\n", start.Unix(), end.Unix(), len(pts))

}

/*
func Test1hPointsPerSecondNumberBuckets(t *testing.T) {
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

	nBuckets := len(strg.getSerie(keyspace, key).buckets)
	if nBuckets > 29 {
		t.Fatalf("Number of buckets bigger than expected: %d", nBuckets)
	}
	if nBuckets < 29 {
		t.Fatalf("Number of buckets lower than expected: %d", nBuckets)
	}
	t.Logf("start: %v\tend: %v\tbuckets: %v\n", start.Unix(), end.Unix(), nBuckets)

	_, count, err := strg.Read(keyspace, key, start.Unix(), currentTime.Unix(), false)
	if err != nil {
		t.Fatal(err)
	}

	if count != inserted {
		t.Fatalf("number of inserted poits %v is differente from readed count %v\n", inserted, count)
	}

	t.Logf("inserted: %v\tcount: %v\n", inserted, count)

}



func Test4hPointsPerMinuteNumberBktsBktSize(t *testing.T) {
	strg := New(nil, nil, nil)

	now := time.Now()
	start := now.Add(-4 * time.Hour)
	end := now

	currentTime := start
	for i := 0; i < 240; i++ {
		strg.Add(keyspace, key, currentTime.Unix(), rand.Float64())
		currentTime = currentTime.Add(time.Minute)
	}

	nBuckets := len(strg.getSerie(keyspace, key).buckets)

		if nBuckets > 2 {
			t.Fatalf("Number of buckets bigger than expected: %d", nBuckets)
		}
		if nBuckets < 2 {
			t.Fatalf("Number of buckets lower than expected: %d", nBuckets)
		}


	for i, bkt := range strg.getSerie(keyspace, key).buckets {
		delta := bkt.Points[bkt.Index-1].Date - bkt.Points[0].Date
		t.Logf("bucket %v: index: %v start: %v end: %v delta: %v\n", i, bkt.Index, bkt.Points[0].Date, bkt.Points[bkt.Index-1].Date, delta)
		if bkt.Index > 120 {
			t.Fatalf("Bucket size bigger than expected: %d", bkt.Index)
		}
		if bkt.Index < 120 {
			t.Fatalf("Bucket size lower than expected: %d", bkt.Index)
		}
	}

	t.Logf("start: %v\tend: %v\tbuckets: %v\n", start.Unix(), end.Unix(), nBuckets)

}
*/

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
