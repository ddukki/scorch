package column

import (
	"fmt"
	"math/big"

	"github.com/ClickHouse/ch-go/proto"
)

// UInt256 is a 256-bit unsigned integer.
type UInt256 struct {
	Lo UInt128
	Hi UInt128
}

func (v UInt256) String() string {
	return "0x" + fmt.Sprintf("%016x%016x%016x%016x", v.Hi.Hi, v.Hi.Lo, v.Lo.Hi, v.Lo.Lo)
}

// Cmp compares v and x, returning -1/0/1 (unsigned comparison).
func (v UInt256) Cmp(x UInt256) int {
	if v.Hi.Cmp(x.Hi) != 0 {
		return v.Hi.Cmp(x.Hi)
	}
	return v.Lo.Cmp(x.Lo)
}

// ToBigInt converts to a big.Int.
func (v UInt256) ToBigInt() *big.Int {
	var buf [32]byte
	putLE128(buf[0:16], Int128{Lo: v.Lo.Lo, Hi: v.Lo.Hi})
	putLE128(buf[16:32], Int128{Lo: v.Hi.Lo, Hi: v.Hi.Hi})
	for i, j := 0, 31; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}
	return new(big.Int).SetBytes(buf[:])
}

// UInt256FromBigInt creates a UInt256 from a big.Int.
func UInt256FromBigInt(bi *big.Int) (UInt256, error) {
	if bi == nil {
		return UInt256{}, fmt.Errorf("nil *big.Int")
	}
	if bi.Sign() < 0 {
		return UInt256{}, fmt.Errorf("UInt256 cannot be negative")
	}
	var be [32]byte
	b := bi.Bytes()
	if len(b) > 32 {
		return UInt256{}, fmt.Errorf("UInt256 value out of range")
	}
	copy(be[32-len(b):], b)
	for i, j := 0, 31; i < j; i, j = i+1, j-1 {
		be[i], be[j] = be[j], be[i]
	}
	return UInt256{
		Lo: UInt128{Lo: readLE64(be[0:8]), Hi: readLE64(be[8:16])},
		Hi: UInt128{Lo: readLE64(be[16:24]), Hi: readLE64(be[24:32])},
	}, nil
}

// UInt256Column is a UInt256 column.
type UInt256Column struct {
	name string
	Data []UInt256
}

// NewUInt256Column creates a UInt256Column.
func NewUInt256Column(name string) *UInt256Column {
	return &UInt256Column{name: name}
}

func (c *UInt256Column) Name() string                        { return c.name }
func (c *UInt256Column) Type() proto.ColumnType               { return proto.ColumnTypeUInt256 }
func (c *UInt256Column) Infer(t proto.ColumnType) error {
	if t.Base() != proto.ColumnTypeUInt256 {
		return fmt.Errorf("UInt256Column: expected UInt256, got %q", t.Base())
	}
	return nil
}
func (c *UInt256Column) Len() int                             { return len(c.Data) }
func (c *UInt256Column) Append(v UInt256)                      { c.Data = append(c.Data, v) }
func (c *UInt256Column) AppendArr(v []UInt256)                  { c.Data = append(c.Data, v...) }
func (c *UInt256Column) Row(i int) UInt256                      { return c.Data[i] }
func (c *UInt256Column) DataSlice(start, end int) []UInt256     { return c.Data[start:end] }
func (c *UInt256Column) Reset()                                 { c.Data = c.Data[:0] }

func (c *UInt256Column) DecodeColumn(r *proto.Reader, rows int) error {
	return decodeFixed(r, rows, 32, &c.Data)
}

func (c *UInt256Column) EncodeColumn(b *proto.Buffer) error {
	return encodeFixed(c.Data, 32, b)
}

func (c *UInt256Column) WriteColumn(w *proto.Writer) {
	writeFixed(c.Data, 32, w)
}
