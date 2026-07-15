package column

import (
	"bytes"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

func TestFixedStringRoundTripExact(t *testing.T) {
	col := NewFixedStringColumn("test", 8)
	col.Append([]byte("hello\x00\x00\x00"))
	col.Append([]byte("world123"))
	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}
	got := NewFixedStringColumn("test", 8)
	if err := got.DecodeColumn(proto.NewReader(bytes.NewReader(buf.Buf)), 2); err != nil {
		t.Fatal(err)
	}
	if got.Len() != 2 {
		t.Fatalf("Len: got %d, want 2", got.Len())
	}
	if string(got.Row(0)) != "hello\x00\x00\x00" {
		t.Fatalf("Row(0): got %q, want %q", got.Row(0), "hello\x00\x00\x00")
	}
	if string(got.Row(1)) != "world123" {
		t.Fatalf("Row(1): got %q, want %q", got.Row(1), "world123")
	}
}

func TestFixedStringAppendTruncate(t *testing.T) {
	col := NewFixedStringColumn("test", 4)
	col.Append([]byte("abcdefgh"))
	if string(col.Row(0)) != "abcd" {
		t.Fatalf("Row(0): got %q, want %q", col.Row(0), "abcd")
	}
}

func TestFixedStringAppendPad(t *testing.T) {
	col := NewFixedStringColumn("test", 8)
	col.Append([]byte("hi"))
	if string(col.Row(0)) != "hi\x00\x00\x00\x00\x00\x00" {
		t.Fatalf("Row(0): got %q, want %q", col.Row(0), "hi\x00\x00\x00\x00\x00\x00")
	}
}

func TestFixedStringAppendNil(t *testing.T) {
	col := NewFixedStringColumn("test", 4)
	col.Append(nil)
	if string(col.Row(0)) != "\x00\x00\x00\x00" {
		t.Fatalf("Row(0): got %q, want %q", col.Row(0), "\x00\x00\x00\x00")
	}
}

func TestFixedStringConstructorPanicZero(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for n=0")
		}
	}()
	NewFixedStringColumn("test", 0)
}

func TestFixedStringConstructorPanicNegative(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for n=-1")
		}
	}()
	NewFixedStringColumn("test", -1)
}

func TestFixedStringDecodeDistinctBuffers(t *testing.T) {
	col := NewFixedStringColumn("test", 4)
	buf := make([]byte, 12)
	copy(buf[0:4], []byte("aaaa"))
	copy(buf[4:8], []byte("bbbb"))
	copy(buf[8:12], []byte("cccc"))
	if err := col.DecodeColumn(proto.NewReader(bytes.NewReader(buf)), 3); err != nil {
		t.Fatal(err)
	}
	if len(col.Data) != 3 {
		t.Fatalf("Data len: got %d, want 3", len(col.Data))
	}
	for i, d := range col.Data {
		if len(d) != 4 {
			t.Fatalf("Data[%d] len: got %d, want 4", i, len(d))
		}
	}
	// Verify distinct backing arrays: modifying one should not affect others.
	col.Data[0][0] = 'x'
	if col.Data[1][0] != 'b' {
		t.Fatal("Data[1] shares backing with Data[0] after decode")
	}
}

func TestFixedStringType(t *testing.T) {
	col := NewFixedStringColumn("test", 16)
	if col.Type() != proto.ColumnTypeFixedString.With("16") {
		t.Fatalf("Type: got %q, want %q", col.Type(), proto.ColumnTypeFixedString.With("16"))
	}
}

func TestFixedStringInfer(t *testing.T) {
	col := NewFixedStringColumn("test", 1)
	if err := col.Infer(proto.ColumnTypeFixedString.With("8")); err != nil {
		t.Fatal(err)
	}
	if col.n != 8 {
		t.Fatalf("n: got %d, want 8", col.n)
	}
	if col.Type() != proto.ColumnTypeFixedString.With("8") {
		t.Fatalf("Type after Infer: got %q, want %q", col.Type(), proto.ColumnTypeFixedString.With("8"))
	}
}

func TestFixedStringEncodeBlocks(t *testing.T) {
	col := NewFixedStringColumn("test", 3)
	col.Append([]byte("ab"))
	col.Append([]byte("cdef"))
	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}
	want := []byte("ab\x00cde")
	if !bytes.Equal(buf.Buf, want) {
		t.Fatalf("encoded: got %v, want %v", buf.Buf, want)
	}
}

func TestFixedStringZeroRows(t *testing.T) {
	col := NewFixedStringColumn("test", 4)
	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}
	got := NewFixedStringColumn("test", 4)
	if err := got.DecodeColumn(proto.NewReader(bytes.NewReader(buf.Buf)), 0); err != nil {
		t.Fatal(err)
	}
	if got.Len() != 0 {
		t.Fatalf("Len: got %d, want 0", got.Len())
	}
}

func TestFixedStringNullable(t *testing.T) {
	inner := NewFixedStringColumn("v", 4)
	col := NewNullable(inner)
	col.Append([]byte("ab"), false)
	col.Append(nil, true)
	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}
	got := NewNullable(NewFixedStringColumn("v", 4))
	if err := got.DecodeColumn(proto.NewReader(bytes.NewReader(buf.Buf)), 2); err != nil {
		t.Fatal(err)
	}
	r0, null0 := got.Row(0)
	if null0 {
		t.Fatal("Row(0): expected non-null")
	}
	if string(r0) != "ab\x00\x00" {
		t.Fatalf("Row(0): got %q, want %q", r0, "ab\x00\x00")
	}
	_, null1 := got.Row(1)
	if !null1 {
		t.Fatal("Row(1): expected null")
	}
}

func TestFixedStringRoundTripShortLong(t *testing.T) {
	col := NewFixedStringColumn("test", 5)
	vals := [][]byte{
		[]byte("hello"),
		[]byte("hi"),
		[]byte(""),
		nil,
		[]byte("toolongggg"),
	}
	for _, v := range vals {
		col.Append(v)
	}
	var buf proto.Buffer
	if err := col.EncodeColumn(&buf); err != nil {
		t.Fatal(err)
	}
	got := NewFixedStringColumn("test", 5)
	if err := got.DecodeColumn(proto.NewReader(bytes.NewReader(buf.Buf)), len(vals)); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		idx  int
		want string
	}{
		{0, "hello"},
		{1, "hi\x00\x00\x00"},
		{2, "\x00\x00\x00\x00\x00"},
		{3, "\x00\x00\x00\x00\x00"},
		{4, "toolo"},
	}
	for _, tt := range tests {
		if string(got.Row(tt.idx)) != tt.want {
			t.Errorf("Row(%d): got %q, want %q", tt.idx, got.Row(tt.idx), tt.want)
		}
	}
}
