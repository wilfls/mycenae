package tsz

import (
	"testing"
	"time"

	"github.com/uol/go-tsz/testdata"
)

func TestExampleEncoding(t *testing.T) {

	// Example from the paper
	t0, _ := time.ParseInLocation("Jan _2 2006 15:04:05", "Mar 24 2015 02:00:00", time.Local)
	tunix := t0.Unix()

	enc := NewEncoder(tunix)

	tunix += 62
	enc.Encode(tunix, 12)

	tunix += 60
	enc.Encode(tunix, 12)

	tunix += 60
	enc.Encode(tunix, 24)

	// extra tests

	// floating point masking/shifting bug
	tunix += 60
	enc.Encode(tunix, 13)

	tunix += 60
	enc.Encode(tunix, 24)

	// delta-of-delta sizes
	tunix += 300 // == delta-of-delta of 240
	enc.Encode(tunix, 24)

	tunix += 900 // == delta-of-delta of 600
	enc.Encode(tunix, 24)

	tunix += 900 + 2050 // == delta-of-delta of 600
	enc.Encode(tunix, 24)

	b, err := enc.Close()
	if err != nil {
		t.Errorf(err.Error())
	}

	dec := NewDecoder(b, t0.Unix())

	tunix = t0.Unix()
	want := []struct {
		t int64
		v float32
	}{
		{tunix + 62, 12},
		{tunix + 122, 12},
		{tunix + 182, 24},

		{tunix + 242, 13},
		{tunix + 302, 24},

		{tunix + 602, 24},
		{tunix + 1502, 24},
		{tunix + 4452, 24},
	}

	var ts int64
	var val float32

	for _, w := range want {

		next := dec.Scan(&ts, &val)

		if !next {
			t.Fatalf("Next()=false, want true")
		} else {
			if w.t != ts || w.v != val {
				t.Errorf("Values()=(%v,%v), want (%v,%v)\n", ts, val, w.t, w.v)
			}
		}

		if !next {
			t.Fatalf("next=false, want true")
		}

	}

	if dec.Scan(&ts, &val) {
		t.Fatalf("dec.Scan()=true, want false")
	}

	if err := dec.Close(); err != nil {
		t.Errorf("dec.Close()=%v, want nil", err)
	}
}

func TestRoundtrip(t *testing.T) {

	enc := NewEncoder(testdata.TwoHoursData[0].T)
	for _, p := range testdata.TwoHoursData {
		enc.Encode(p.T, p.V)
	}

	b, err := enc.Close()
	if err != nil {
		t.Errorf("enc.Close()=%v, want nil", err)
	}

	dec := NewDecoder(b, testdata.TwoHoursData[0].T)

	var ts int64
	var val float32

	for _, w := range testdata.TwoHoursData {

		next := dec.Scan(&ts, &val)

		if !next {
			t.Fatalf("next=false, want true")
		}

		if w.T != ts || w.V != val {
			t.Errorf("Values()=(%v,%v), want (%v,%v)\n", ts, val, w.T, w.V)
		}
	}

	if dec.Scan(&ts, &val) {
		t.Fatalf("dec.Scan()=true, want false")
	}

	if err := dec.Close(); err != nil {
		t.Errorf("dec.Close()=%v, want nil", err)
	}
}

func BenchmarkEncode(b *testing.B) {
	b.SetBytes(int64(len(testdata.TwoHoursData) * 12))
	for i := 0; i < b.N; i++ {
		enc := NewEncoder(testdata.TwoHoursData[0].T)
		for _, tt := range testdata.TwoHoursData {
			enc.Encode(tt.T, tt.V)
		}
	}
}

func BenchmarkDecodeSeries(b *testing.B) {
	b.SetBytes(int64(len(testdata.TwoHoursData) * 12))
	enc := NewEncoder(testdata.TwoHoursData[0].T)
	for _, tt := range testdata.TwoHoursData {
		enc.Encode(tt.T, tt.V)
	}

	sb, err := enc.Close()
	if err != nil {
		b.Errorf("enc.Close()=%v, want nil", err)
	}

	b.ResetTimer()

	dec := NewDecoder(sb, testdata.TwoHoursData[0].T)

	var ts int64
	var f float32

	for i := 0; i < b.N; i++ {
		var j int
		for dec.Scan(&ts, &f) {
			j++
		}
	}

	if err := dec.Close(); err != nil {
		b.Errorf("dec.Close()=%v, want nil", err)
	}
}

func TestEncodeSimilarFloats(t *testing.T) {
	tunix := time.Unix(0, 0).Unix()
	enc := NewEncoder(tunix)
	want := []struct {
		t int64
		v float32
	}{
		{tunix, 6.00065e+06},
		{tunix + 1, 6.000656e+06},
		{tunix + 2, 6.000657e+06},
		{tunix + 3, 6.000659e+06},
		{tunix + 4, 6.000661e+06},
	}

	for _, v := range want {
		enc.Encode(v.t, float32(v.v))
	}

	be, err := enc.Close()
	if err != nil {
		t.Errorf("enc.Close()=%v, want nil", err)
	}

	dec := NewDecoder(be, tunix)

	var ts int64
	var f float32
	var next bool

	for _, w := range want {

		next = dec.Scan(&ts, &f)

		if !next {
			t.Fatalf("Next()=false, want true")
		}
		if w.t != ts || w.v != f {
			t.Errorf("Values()=(%v,%v), want (%v,%v)\n", ts, f, w.t, w.v)
		}
	}

	next = dec.Scan(&ts, &f)

	if next {
		t.Fatalf("Next()=true, want false")
	}

	if err := dec.Close(); err != nil {
		t.Errorf("it.Err()=%v, want nil", err)
	}
}

func TestDoubleDecode(t *testing.T) {

	enc := NewEncoder(testdata.TwoHoursData[0].T)
	for _, p := range testdata.TwoHoursData {
		enc.Encode(p.T, p.V)
	}

	b, err := enc.Close()
	if err != nil {
		t.Errorf("enc.Close()=%v, want nil", err)
	}

	dec := NewDecoder(b, testdata.TwoHoursData[0].T)

	var ts int64
	var val float32

	for _, w := range testdata.TwoHoursData {

		next := dec.Scan(&ts, &val)

		if !next {
			t.Fatalf("next=false, want true")
		}

		if w.T != ts || w.V != val {
			t.Errorf("Values()=(%v,%v), want (%v,%v)\n", ts, val, w.T, w.V)
		}
	}

	if dec.Scan(&ts, &val) {
		t.Fatalf("dec.Scan()=true, want false")
	}

	if err := dec.Close(); err != nil {
		t.Errorf("dec.Close()=%v, want nil", err)
	}

	dec = NewDecoder(b, testdata.TwoHoursData[0].T)

	for _, w := range testdata.TwoHoursData {

		next := dec.Scan(&ts, &val)

		if !next {
			t.Fatalf("next=false, want true")
		}

		if w.T != ts || w.V != val {
			t.Errorf("Values()=(%v,%v), want (%v,%v)\n", ts, val, w.T, w.V)
		}
	}

	if dec.Scan(&ts, &val) {
		t.Fatalf("dec.Scan()=true, want false")
	}

	if err := dec.Close(); err != nil {
		t.Errorf("dec.Close()=%v, want nil", err)
	}

}
