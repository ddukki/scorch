package column

import (
	"bytes"
	"math"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

func roundTrip[T comparable](t *testing.T, col ColumnOf[T], vals []T) {
	t.Helper()
	for _, v := range vals {
		col.Append(v)
	}

	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}

	r := proto.NewReader(bytes.NewReader(buf.Buf))
	dst := NewBase[T]("test")
	if err := dst.DecodeColumn(r, len(vals)); err != nil {
		t.Fatal(err)
	}
	if dst.Len() != len(vals) {
		t.Fatalf("len: got %d, want %d", dst.Len(), len(vals))
	}
	for i, expected := range vals {
		if dst.Row(i) != expected {
			t.Fatalf("row %d: got %v, want %v", i, dst.Row(i), expected)
		}
	}
}

func TestBaseRoundTrip(t *testing.T) {
	t.Run("uint8", func(t *testing.T) { roundTrip(t, NewBase[uint8]("v"), []uint8{1, 2, 255}) })
	t.Run("uint16", func(t *testing.T) { roundTrip(t, NewBase[uint16]("v"), []uint16{1, 256, 65535}) })
	t.Run("uint32", func(t *testing.T) { roundTrip(t, NewBase[uint32]("v"), []uint32{1, 70000, math.MaxUint32}) })
	t.Run("uint64", func(t *testing.T) { roundTrip(t, NewBase[uint64]("v"), []uint64{1, math.MaxUint64}) })
	t.Run("int8", func(t *testing.T) { roundTrip(t, NewBase[int8]("v"), []int8{-128, 0, 127}) })
	t.Run("int16", func(t *testing.T) { roundTrip(t, NewBase[int16]("v"), []int16{-32768, 0, 32767}) })
	t.Run("int32", func(t *testing.T) { roundTrip(t, NewBase[int32]("v"), []int32{math.MinInt32, 0, math.MaxInt32}) })
	t.Run("int64", func(t *testing.T) { roundTrip(t, NewBase[int64]("v"), []int64{math.MinInt64, 0, math.MaxInt64}) })
	t.Run("float32", func(t *testing.T) { roundTrip(t, NewBase[float32]("v"), []float32{0, 3.14, -2.5, math.MaxFloat32}) })
	t.Run("float64", func(t *testing.T) { roundTrip(t, NewBase[float64]("v"), []float64{0, 3.14159265359, -2.5, math.MaxFloat64}) })
}

func TestBaseDataUnsafe(t *testing.T) {
	col := NewBase[uint64]("id")
	col.Append(10)
	col.Append(20)
	col.Append(30)

	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}

	r := proto.NewReader(bytes.NewReader(buf.Buf))
	got := NewBase[uint64]("id")
	if err := got.DecodeColumn(r, 3); err != nil {
		t.Fatal(err)
	}
	if got.Len() != 3 {
		t.Fatalf("len: got %d, want 3", got.Len())
	}

	du := got.DataUnsafe()
	if len(du) != 3 {
		t.Fatalf("DataUnsafe len: got %d, want 3", len(du))
	}
	if du[0] != got.Row(0) || du[1] != got.Row(1) || du[2] != got.Row(2) {
		t.Fatal("DataUnsafe values differ from Row")
	}
}

func TestStrRoundTrip(t *testing.T) {
	col := NewStr("s")
	col.Append("hello")
	col.Append("")
	col.Append("world")

	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}

	r := proto.NewReader(bytes.NewReader(buf.Buf))
	got := NewStr("s")
	if err := got.DecodeColumn(r, 3); err != nil {
		t.Fatal(err)
	}
	if got.Len() != 3 {
		t.Fatalf("len: got %d, want 3", got.Len())
	}

	cases := []struct{ i int; want string }{
		{0, "hello"},
		{1, ""},
		{2, "world"},
	}
	for _, c := range cases {
		if got.Row(c.i) != c.want {
			t.Fatalf("row %d: got %q, want %q", c.i, got.Row(c.i), c.want)
		}
	}
}

func TestNullableRoundTrip(t *testing.T) {
	col := NewNullable(NewBase[uint64]("v"))
	col.Append(1, false)
	col.Append(0, true)
	col.Append(3, false)

	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}

	r := proto.NewReader(bytes.NewReader(buf.Buf))
	got := NewNullable(NewBase[uint64]("v"))
	if err := got.DecodeColumn(r, 3); err != nil {
		t.Fatal(err)
	}
	if got.Len() != 3 {
		t.Fatalf("len: got %d, want 3", got.Len())
	}

	v0, n0 := got.Row(0)
	if v0 != 1 || n0 {
		t.Fatalf("row 0: got (%d, %v), want (1, false)", v0, n0)
	}
	v1, n1 := got.Row(1)
	if v1 != 0 || !n1 {
		t.Fatalf("row 1: got (%d, %v), want (0, true)", v1, n1)
	}
	v2, n2 := got.Row(2)
	if v2 != 3 || n2 {
		t.Fatalf("row 2: got (%d, %v), want (3, false)", v2, n2)
	}
}

func TestLowCardinalityRoundTrip(t *testing.T) {
	t.Run("uint8", func(t *testing.T) {
		base := NewBase[uint8]("v")
		col := NewLowCardinality(base)
		base.AppendArr([]uint8{1, 2, 3, 1, 2, 3})

		var buf proto.Buffer
		if err := col.EncodeColumn(&buf); err != nil {
			t.Fatal(err)
		}

		r := proto.NewReader(bytes.NewReader(buf.Buf))
		got := NewLowCardinality(NewBase[uint8]("v"))
		if err := got.DecodeColumn(r, 6); err != nil {
			t.Fatal(err)
		}
		if got.Len() != 6 {
			t.Fatalf("len: got %d, want 6", got.Len())
		}
		for i, want := range []uint8{1, 2, 3, 1, 2, 3} {
			if got.Values.Row(i) != want {
				t.Fatalf("row %d: got %d, want %d", i, got.Values.Row(i), want)
			}
		}
	})

	t.Run("string", func(t *testing.T) {
		s := NewStr("v")
		col := NewLowCardinality(s)
		s.AppendArr([]string{"a", "b", "a", "c"})

		var buf proto.Buffer
		if err := col.EncodeColumn(&buf); err != nil {
			t.Fatal(err)
		}

		r := proto.NewReader(bytes.NewReader(buf.Buf))
		got := NewLowCardinality(NewStr("v"))
		if err := got.DecodeColumn(r, 4); err != nil {
			t.Fatal(err)
		}
		if got.Len() != 4 {
			t.Fatalf("len: got %d, want 4", got.Len())
		}
		for i, want := range []string{"a", "b", "a", "c"} {
			if got.Values.Row(i) != want {
				t.Fatalf("row %d: got %q, want %q", i, got.Values.Row(i), want)
			}
		}
	})
}

func TestTupleRoundTrip(t *testing.T) {
	col := NewTuple2(NewBase[uint64]("a"), NewStr("b"))
	col.Append(Tuple2Value[uint64, string]{1, "one"})
	col.Append(Tuple2Value[uint64, string]{2, "two"})

	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}

	got := NewTuple2(NewBase[uint64]("a"), NewStr("b"))
	r := proto.NewReader(bytes.NewReader(buf.Buf))
	if err := got.DecodeColumn(r, 2); err != nil {
		t.Fatal(err)
	}
	if got.Len() != 2 {
		t.Fatalf("len: got %d, want 2", got.Len())
	}

	r0 := got.Row(0)
	if r0.T1 != 1 || r0.T2 != "one" {
		t.Fatalf("row 0: got (%d, %q), want (1, one)", r0.T1, r0.T2)
	}
	r1 := got.Row(1)
	if r1.T1 != 2 || r1.T2 != "two" {
		t.Fatalf("row 1: got (%d, %q), want (2, two)", r1.T1, r1.T2)
	}
}

func TestEmptyColumn(t *testing.T) {
	t.Run("Base[uint64]", func(t *testing.T) {
		col := NewBase[uint64]("v")
		var buf proto.Buffer
		if err := col.EncodeColumn(&buf); err != nil {
			t.Fatal(err)
		}
		r := proto.NewReader(bytes.NewReader(buf.Buf))
		got := NewBase[uint64]("v")
		if err := got.DecodeColumn(r, 0); err != nil {
			t.Fatal(err)
		}
		if got.Len() != 0 {
			t.Fatalf("len: got %d, want 0", got.Len())
		}
	})

	t.Run("Str", func(t *testing.T) {
		col := NewStr("v")
		var buf proto.Buffer
		if err := col.EncodeColumn(&buf); err != nil {
			t.Fatal(err)
		}
		r := proto.NewReader(bytes.NewReader(buf.Buf))
		got := NewStr("v")
		if err := got.DecodeColumn(r, 0); err != nil {
			t.Fatal(err)
		}
		if got.Len() != 0 {
			t.Fatalf("len: got %d, want 0", got.Len())
		}
	})

	t.Run("Nullable[uint64]", func(t *testing.T) {
		col := NewNullable(NewBase[uint64]("v"))
		var buf proto.Buffer
		if err := col.EncodeColumn(&buf); err != nil {
			t.Fatal(err)
		}
		r := proto.NewReader(bytes.NewReader(buf.Buf))
		got := NewNullable(NewBase[uint64]("v"))
		if err := got.DecodeColumn(r, 0); err != nil {
			t.Fatal(err)
		}
		if got.Len() != 0 {
			t.Fatalf("len: got %d, want 0", got.Len())
		}
	})
}

func TestBaseType(t *testing.T) {
	tests := []struct {
		col  Column
		want proto.ColumnType
	}{
		{NewBase[uint8]("v"), proto.ColumnTypeUInt8},
		{NewBase[uint16]("v"), proto.ColumnTypeUInt16},
		{NewBase[uint32]("v"), proto.ColumnTypeUInt32},
		{NewBase[uint64]("v"), proto.ColumnTypeUInt64},
		{NewBase[int8]("v"), proto.ColumnTypeInt8},
		{NewBase[int16]("v"), proto.ColumnTypeInt16},
		{NewBase[int32]("v"), proto.ColumnTypeInt32},
		{NewBase[int64]("v"), proto.ColumnTypeInt64},
		{NewBase[float32]("v"), proto.ColumnTypeFloat32},
		{NewBase[float64]("v"), proto.ColumnTypeFloat64},
		{NewBase[string]("v"), proto.ColumnType("")},
	}
	for _, tt := range tests {
		if got := tt.col.Type(); got != tt.want {
			t.Fatalf("Type() = %q, want %q", got, tt.want)
		}
	}
}

func TestBaseUnsupportedType(t *testing.T) {
	col := NewBase[string]("v")
	if got := col.Type(); got != "" {
		t.Fatalf("unexpected type for string: %q", got)
	}
}

func TestName(t *testing.T) {
	if got := NewBase[uint64]("id").Name(); got != "id" {
		t.Fatalf("Name: got %q, want id", got)
	}
	if got := NewStr("s").Name(); got != "s" {
		t.Fatalf("Name: got %q, want s", got)
	}
	if got := NewNullable(NewBase[uint64]("v")).Name(); got != "v" {
		t.Fatalf("Name: got %q, want v", got)
	}
	if got := NewLowCardinality(NewBase[uint64]("v")).Name(); got != "v" {
		t.Fatalf("Name: got %q, want v", got)
	}
	tup := NewTuple2(NewBase[uint64]("a"), NewStr("b"))
	if got := tup.Name(); got != "" {
		t.Fatalf("Tuple Name: got %q, want empty", got)
	}
}

func TestAppendArr(t *testing.T) {
	col := NewBase[uint64]("v")
	col.AppendArr([]uint64{1, 2, 3})
	if col.Len() != 3 {
		t.Fatalf("len: got %d, want 3", col.Len())
	}
	if col.Row(0) != 1 || col.Row(2) != 3 {
		t.Fatal("values mismatch after AppendArr")
	}
}

func TestSingleton(t *testing.T) {
	col := NewBase[uint64]("v")
	col.Append(42)

	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}

	r := proto.NewReader(bytes.NewReader(buf.Buf))
	got := NewBase[uint64]("v")
	if err := got.DecodeColumn(r, 1); err != nil {
		t.Fatal(err)
	}
	if got.Len() != 1 {
		t.Fatalf("len: got %d, want 1", got.Len())
	}
	if got.Row(0) != 42 {
		t.Fatalf("row 0: got %d, want 42", got.Row(0))
	}
}
