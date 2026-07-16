package column

import (
	"bytes"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

func TestMapStringUInt64RoundTrip(t *testing.T) {
	col := NewMapColumn[string, uint64]("")
	if err := col.Infer(proto.ColumnTypeMap.With("String", "UInt64")); err != nil {
		t.Fatal(err)
	}
	col.Append([]MapEntry[string, uint64]{{Key: "a", Value: 1}, {Key: "b", Value: 2}})
	col.Append([]MapEntry[string, uint64]{{Key: "c", Value: 3}})
	col.Append(nil)
	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}
	got := NewMapColumn[string, uint64]("")
	if err := got.Infer(proto.ColumnTypeMap.With("String", "UInt64")); err != nil {
		t.Fatal(err)
	}
	if err := got.DecodeColumn(proto.NewReader(bytes.NewReader(buf.Buf)), 3); err != nil {
		t.Fatal(err)
	}
	if got.Len() != 3 {
		t.Fatalf("Len: got %d, want 3", got.Len())
	}
	r0 := got.Row(0)
	if len(r0) != 2 || r0[0].Key != "a" || r0[0].Value != 1 || r0[1].Key != "b" || r0[1].Value != 2 {
		t.Fatalf("Row(0): got %+v, want [{a 1} {b 2}]", r0)
	}
	r1 := got.Row(1)
	if len(r1) != 1 || r1[0].Key != "c" || r1[0].Value != 3 {
		t.Fatalf("Row(1): got %+v, want [{c 3}]", r1)
	}
	r2 := got.Row(2)
	if len(r2) != 0 {
		t.Fatalf("Row(2): got %+v, want empty", r2)
	}
}

func TestMapUInt64StringRoundTrip(t *testing.T) {
	col := NewMapColumn[uint64, string]("")
	if err := col.Infer(proto.ColumnTypeMap.With("UInt64", "String")); err != nil {
		t.Fatal(err)
	}
	col.Append([]MapEntry[uint64, string]{{Key: 10, Value: "foo"}, {Key: 20, Value: "bar"}})
	col.Append([]MapEntry[uint64, string]{{Key: 30, Value: "baz"}})
	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}
	got := NewMapColumn[uint64, string]("")
	if err := got.Infer(proto.ColumnTypeMap.With("UInt64", "String")); err != nil {
		t.Fatal(err)
	}
	if err := got.DecodeColumn(proto.NewReader(bytes.NewReader(buf.Buf)), 2); err != nil {
		t.Fatal(err)
	}
	if got.Len() != 2 {
		t.Fatalf("Len: got %d, want 2", got.Len())
	}
	r0 := got.Row(0)
	if len(r0) != 2 || r0[0].Key != 10 || r0[0].Value != "foo" || r0[1].Key != 20 || r0[1].Value != "bar" {
		t.Fatalf("Row(0): got %+v, want [{10 foo} {20 bar}]", r0)
	}
	r1 := got.Row(1)
	if len(r1) != 1 || r1[0].Key != 30 || r1[0].Value != "baz" {
		t.Fatalf("Row(1): got %+v, want [{30 baz}]", r1)
	}
}

func TestMapZeroRows(t *testing.T) {
	col := NewMapColumn[string, uint64]("")
	if err := col.Infer(proto.ColumnTypeMap.With("String", "UInt64")); err != nil {
		t.Fatal(err)
	}
	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}
	got := NewMapColumn[string, uint64]("")
	if err := got.Infer(proto.ColumnTypeMap.With("String", "UInt64")); err != nil {
		t.Fatal(err)
	}
	if err := got.DecodeColumn(proto.NewReader(bytes.NewReader(buf.Buf)), 0); err != nil {
		t.Fatal(err)
	}
	if got.Len() != 0 {
		t.Fatalf("Len: got %d, want 0", got.Len())
	}
}

func TestMapType(t *testing.T) {
	col := NewMapColumn[string, uint64]("")
	if err := col.Infer(proto.ColumnTypeMap.With("String", "UInt64")); err != nil {
		t.Fatal(err)
	}
	got := col.Type()
	want := proto.ColumnType("Map(String, UInt64)")
	if got != want {
		t.Fatalf("Type(): got %q, want %q", got, want)
	}
}

func TestMapOffsets(t *testing.T) {
	col := NewMapColumn[string, uint64]("")
	if err := col.Infer(proto.ColumnTypeMap.With("String", "UInt64")); err != nil {
		t.Fatal(err)
	}
	col.Append([]MapEntry[string, uint64]{{Key: "a", Value: 1}})
	col.Append([]MapEntry[string, uint64]{{Key: "b", Value: 2}, {Key: "c", Value: 3}})
	if len(col.Offsets) != 2 {
		t.Fatalf("len(Offsets): got %d, want 2", len(col.Offsets))
	}
	if col.Offsets[0] != 1 {
		t.Fatalf("Offsets[0]: got %d, want 1", col.Offsets[0])
	}
	if col.Offsets[1] != 3 {
		t.Fatalf("Offsets[1]: got %d, want 3", col.Offsets[1])
	}
}

func TestMapForEachEntry(t *testing.T) {
	col := NewMapColumn[string, uint64]("")
	if err := col.Infer(proto.ColumnTypeMap.With("String", "UInt64")); err != nil {
		t.Fatal(err)
	}
	col.Append([]MapEntry[string, uint64]{{Key: "a", Value: 1}, {Key: "b", Value: 2}})
	var keys []string
	var vals []uint64
	if err := col.ForEachEntry(0, func(e MapEntry[string, uint64]) error {
		keys = append(keys, e.Key)
		vals = append(vals, e.Value)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Fatalf("keys: got %v, want [a b]", keys)
	}
	if len(vals) != 2 || vals[0] != 1 || vals[1] != 2 {
		t.Fatalf("vals: got %v, want [1 2]", vals)
	}
}

func TestMapNullable(t *testing.T) {
	inner := NewMapColumn[string, uint64]("")
	if err := inner.Infer(proto.ColumnTypeMap.With("String", "UInt64")); err != nil {
		t.Fatal(err)
	}
	col := NewNullable[[]MapEntry[string, uint64]](inner)
	col.Append([]MapEntry[string, uint64]{{Key: "a", Value: 1}}, false)
	var zero []MapEntry[string, uint64]
	col.Append(zero, true)
	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}
	gotInner := NewMapColumn[string, uint64]("")
	if err := gotInner.Infer(proto.ColumnTypeMap.With("String", "UInt64")); err != nil {
		t.Fatal(err)
	}
	got := NewNullable[[]MapEntry[string, uint64]](gotInner)
	if err := got.DecodeColumn(proto.NewReader(bytes.NewReader(buf.Buf)), 2); err != nil {
		t.Fatal(err)
	}
	r0, null0 := got.Row(0)
	if null0 {
		t.Fatal("Row(0): expected non-null")
	}
	if len(r0) != 1 || r0[0].Key != "a" || r0[0].Value != 1 {
		t.Fatalf("Row(0): got %+v, want [{a 1}]", r0)
	}
	_, null1 := got.Row(1)
	if !null1 {
		t.Fatal("Row(1): expected null")
	}
}

func TestMapInferError(t *testing.T) {
	col := NewMapColumn[string, uint64]("")
	err := col.Infer(proto.ColumnTypeInt64)
	if err == nil {
		t.Fatal("expected error for non-Map type")
	}
}

func TestMapAppendArr(t *testing.T) {
	col := NewMapColumn[string, uint64]("")
	if err := col.Infer(proto.ColumnTypeMap.With("String", "UInt64")); err != nil {
		t.Fatal(err)
	}
	col.AppendArr([][]MapEntry[string, uint64]{
		{{Key: "a", Value: 1}},
		{{Key: "b", Value: 2}, {Key: "c", Value: 3}},
	})
	if col.Len() != 2 {
		t.Fatalf("Len: got %d, want 2", col.Len())
	}
	if col.Offsets[0] != 1 || col.Offsets[1] != 3 {
		t.Fatalf("Offsets: got %v, want [1 3]", col.Offsets)
	}
}

func TestMapReset(t *testing.T) {
	col := NewMapColumn[string, uint64]("")
	if err := col.Infer(proto.ColumnTypeMap.With("String", "UInt64")); err != nil {
		t.Fatal(err)
	}
	col.Append([]MapEntry[string, uint64]{{Key: "a", Value: 1}})
	col.Reset()
	if col.Len() != 0 {
		t.Fatalf("Len after reset: got %d, want 0", col.Len())
	}
	if len(col.Offsets) != 0 {
		t.Fatalf("Offsets after reset: got %d, want 0", len(col.Offsets))
	}
	if col.Keys.Len() != 0 {
		t.Fatalf("Keys.Len after reset: got %d, want 0", col.Keys.Len())
	}
	if col.Values.Len() != 0 {
		t.Fatalf("Values.Len after reset: got %d, want 0", col.Values.Len())
	}
}

func TestMapEntriesToMap(t *testing.T) {
	entries := []MapEntry[string, uint64]{
		{Key: "a", Value: 1},
		{Key: "b", Value: 2},
	}
	m := EntriesToMap(entries)
	if len(m) != 2 || m["a"] != 1 || m["b"] != 2 {
		t.Fatalf("EntriesToMap: got %v, want map[a:1 b:2]", m)
	}
}

func TestMapEntriesToMapDuplicateKeys(t *testing.T) {
	entries := []MapEntry[string, uint64]{
		{Key: "a", Value: 1},
		{Key: "a", Value: 2},
	}
	m := EntriesToMap(entries)
	if len(m) != 1 || m["a"] != 2 {
		t.Fatalf("EntriesToMap: got %v, want map[a:2] (later overwrites)", m)
	}
}

func TestMapName(t *testing.T) {
	col := NewMapColumn[string, uint64]("my_map")
	if col.Name() != "my_map" {
		t.Fatalf("Name(): got %q, want %q", col.Name(), "my_map")
	}
}
