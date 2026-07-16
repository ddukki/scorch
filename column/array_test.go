package column

import (
	"bytes"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

func TestArrayUInt64RoundTrip(t *testing.T) {
	elem := NewBaseColumn[uint64]("")
	col := NewArrayColumn("", elem)
	if err := col.Infer(proto.ColumnTypeArray.With(proto.ColumnTypeUInt64.String())); err != nil {
		t.Fatal(err)
	}
	col.Append([]uint64{1, 2, 3})
	col.Append([]uint64{4, 5})
	col.Append(nil)
	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}
	elem2 := NewBaseColumn[uint64]("")
	got := NewArrayColumn("", elem2)
	if err := got.Infer(proto.ColumnTypeArray.With(proto.ColumnTypeUInt64.String())); err != nil {
		t.Fatal(err)
	}
	if err := got.DecodeColumn(proto.NewReader(bytes.NewReader(buf.Buf)), col.Len()); err != nil {
		t.Fatal(err)
	}
	if got.Len() != 3 {
		t.Fatalf("Len: got %d, want 3", got.Len())
	}
	r0 := got.Row(0)
	if len(r0) != 3 || r0[0] != 1 || r0[1] != 2 || r0[2] != 3 {
		t.Fatalf("Row(0): got %v, want [1 2 3]", r0)
	}
	r1 := got.Row(1)
	if len(r1) != 2 || r1[0] != 4 || r1[1] != 5 {
		t.Fatalf("Row(1): got %v, want [4 5]", r1)
	}
	r2 := got.Row(2)
	if len(r2) != 0 {
		t.Fatalf("Row(2): got %v, want []", r2)
	}
}

func TestArrayStringRoundTrip(t *testing.T) {
	elem := new(StrColumn)
	col := NewArrayColumn("", elem)
	if err := col.Infer(proto.ColumnTypeArray.With(proto.ColumnTypeString.String())); err != nil {
		t.Fatal(err)
	}
	col.Append([]string{"hello", "world"})
	col.Append([]string{"foo"})
	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}
	elem2 := new(StrColumn)
	got := NewArrayColumn("", elem2)
	if err := got.Infer(proto.ColumnTypeArray.With(proto.ColumnTypeString.String())); err != nil {
		t.Fatal(err)
	}
	if err := got.DecodeColumn(proto.NewReader(bytes.NewReader(buf.Buf)), col.Len()); err != nil {
		t.Fatal(err)
	}
	r0 := got.Row(0)
	if len(r0) != 2 || r0[0] != "hello" || r0[1] != "world" {
		t.Fatalf("Row(0): got %v, want [hello world]", r0)
	}
	r1 := got.Row(1)
	if len(r1) != 1 || r1[0] != "foo" {
		t.Fatalf("Row(1): got %v, want [foo]", r1)
	}
}

func TestArrayZeroRows(t *testing.T) {
	elem := NewBaseColumn[uint64]("")
	col := NewArrayColumn("", elem)
	if err := col.Infer(proto.ColumnTypeArray.With(proto.ColumnTypeUInt64.String())); err != nil {
		t.Fatal(err)
	}
	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}
	elem2 := NewBaseColumn[uint64]("")
	got := NewArrayColumn("", elem2)
	if err := got.Infer(proto.ColumnTypeArray.With(proto.ColumnTypeUInt64.String())); err != nil {
		t.Fatal(err)
	}
	if err := got.DecodeColumn(proto.NewReader(bytes.NewReader(buf.Buf)), 0); err != nil {
		t.Fatal(err)
	}
	if got.Len() != 0 {
		t.Fatalf("Len: got %d, want 0", got.Len())
	}
}

func TestArrayType(t *testing.T) {
	elem := NewBaseColumn[uint64]("")
	col := NewArrayColumn("", elem)
	if err := col.Infer(proto.ColumnTypeArray.With(proto.ColumnTypeUInt64.String())); err != nil {
		t.Fatal(err)
	}
	got := col.Type()
	want := proto.ColumnType("Array(UInt64)")
	if got != want {
		t.Fatalf("Type(): got %q, want %q", got, want)
	}
}

func TestArrayOffsets(t *testing.T) {
	elem := NewBaseColumn[uint64]("")
	col := NewArrayColumn("", elem)
	if err := col.Infer(proto.ColumnTypeArray.With(proto.ColumnTypeUInt64.String())); err != nil {
		t.Fatal(err)
	}
	col.Append([]uint64{10, 20})
	col.Append([]uint64{30})
	if len(col.Offsets) != 2 {
		t.Fatalf("len(Offsets): got %d, want 2", len(col.Offsets))
	}
	if col.Offsets[0] != 2 {
		t.Fatalf("Offsets[0]: got %d, want 2", col.Offsets[0])
	}
	if col.Offsets[1] != 3 {
		t.Fatalf("Offsets[1]: got %d, want 3", col.Offsets[1])
	}
}

func TestArrayUInt64ZeroCopy(t *testing.T) {
	elem := NewBaseColumn[uint64]("")
	col := NewArrayColumn("", elem)
	if err := col.Infer(proto.ColumnTypeArray.With(proto.ColumnTypeUInt64.String())); err != nil {
		t.Fatal(err)
	}
	col.Append([]uint64{1, 2, 3})
	r0 := col.Row(0)
	// r0 should reference the same backing as elem.Data
	if &r0[0] != &elem.Data[0] {
		t.Fatal("Row(0) does not reference Elem.Data backing")
	}
}

func TestArrayStringZeroCopy(t *testing.T) {
	elem := new(StrColumn)
	col := NewArrayColumn("", elem)
	if err := col.Infer(proto.ColumnTypeArray.With(proto.ColumnTypeString.String())); err != nil {
		t.Fatal(err)
	}
	col.Append([]string{"a", "b"})
	r0 := col.Row(0)
	if &r0[0] != &elem.Data[0] {
		t.Fatal("Row(0) does not reference Elem.Data backing")
	}
}

func TestArrayAppendArr(t *testing.T) {
	elem := NewBaseColumn[uint64]("")
	col := NewArrayColumn("", elem)
	if err := col.Infer(proto.ColumnTypeArray.With(proto.ColumnTypeUInt64.String())); err != nil {
		t.Fatal(err)
	}
	col.AppendArr([][]uint64{{1, 2}, {3}})
	if col.Len() != 2 {
		t.Fatalf("Len: got %d, want 2", col.Len())
	}
	if col.Offsets[0] != 2 || col.Offsets[1] != 3 {
		t.Fatalf("Offsets: got %v, want [2 3]", col.Offsets)
	}
}

func TestArrayInferError(t *testing.T) {
	elem := NewBaseColumn[uint64]("")
	col := NewArrayColumn("", elem)
	err := col.Infer(proto.ColumnTypeInt64)
	if err == nil {
		t.Fatal("expected error for non-Array type")
	}
}

func TestArrayNullable(t *testing.T) {
	elem := NewBaseColumn[uint64]("")
	inner := NewArrayColumn("", elem)
	if err := inner.Infer(proto.ColumnTypeArray.With(proto.ColumnTypeUInt64.String())); err != nil {
		t.Fatal(err)
	}
	col := NewNullable[[]uint64](inner)
	col.Append([]uint64{1, 2}, false)
	var zero []uint64
	col.Append(zero, true)
	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}
	elem2 := NewBaseColumn[uint64]("")
	gotInner := NewArrayColumn("", elem2)
	if err := gotInner.Infer(proto.ColumnTypeArray.With(proto.ColumnTypeUInt64.String())); err != nil {
		t.Fatal(err)
	}
	got := NewNullable[[]uint64](gotInner)
	if err := got.DecodeColumn(proto.NewReader(bytes.NewReader(buf.Buf)), 2); err != nil {
		t.Fatal(err)
	}
	r0, null0 := got.Row(0)
	if null0 {
		t.Fatal("Row(0): expected non-null")
	}
	if len(r0) != 2 || r0[0] != 1 || r0[1] != 2 {
		t.Fatalf("Row(0): got %v, want [1 2]", r0)
	}
	_, null1 := got.Row(1)
	if !null1 {
		t.Fatal("Row(1): expected null")
	}
}

func TestArrayReset(t *testing.T) {
	elem := NewBaseColumn[uint64]("")
	col := NewArrayColumn("", elem)
	if err := col.Infer(proto.ColumnTypeArray.With(proto.ColumnTypeUInt64.String())); err != nil {
		t.Fatal(err)
	}
	col.Append([]uint64{1, 2, 3})
	col.Reset()
	if col.Len() != 0 {
		t.Fatalf("Len after reset: got %d, want 0", col.Len())
	}
	if len(col.Offsets) != 0 {
		t.Fatalf("Offsets after reset: got %d, want 0", len(col.Offsets))
	}
	if elem.Len() != 0 {
		t.Fatalf("Elem.Len after reset: got %d, want 0", elem.Len())
	}
}

func TestArrayEdgeCaseSingleElements(t *testing.T) {
	elem := NewBaseColumn[uint64]("")
	col := NewArrayColumn("", elem)
	if err := col.Infer(proto.ColumnTypeArray.With(proto.ColumnTypeUInt64.String())); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 100; i++ {
		col.Append([]uint64{i})
	}
	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}
	elem2 := NewBaseColumn[uint64]("")
	got := NewArrayColumn("", elem2)
	if err := got.Infer(proto.ColumnTypeArray.With(proto.ColumnTypeUInt64.String())); err != nil {
		t.Fatal(err)
	}
	if err := got.DecodeColumn(proto.NewReader(bytes.NewReader(buf.Buf)), col.Len()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		r := got.Row(i)
		if len(r) != 1 || r[0] != uint64(i+1) {
			t.Fatalf("Row(%d): got %v, want [%d]", i, r, i+1)
		}
	}
}
