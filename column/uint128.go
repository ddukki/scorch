package column

import (
	"fmt"
	"math/big"

	"github.com/ClickHouse/ch-go/proto"
)

// UInt128 is a 128-bit unsigned integer.
type UInt128 struct {
	Lo uint64
	Hi uint64
}

func (v UInt128) String() string {
	return "0x" + fmt.Sprintf("%016x%016x", v.Hi, v.Lo)
}

// Cmp compares v and x, returning -1/0/1 (unsigned comparison).
func (v UInt128) Cmp(x UInt128) int {
	if v.Hi != x.Hi {
		if v.Hi < x.Hi {
			return -1
		}
		return 1
	}
	if v.Lo != x.Lo {
		if v.Lo < x.Lo {
			return -1
		}
		return 1
	}
	return 0
}

// ToBigInt converts to a big.Int.
func (v UInt128) ToBigInt() *big.Int {
	var buf [16]byte
	buf[0] = byte(v.Lo)
	buf[1] = byte(v.Lo >> 8)
	buf[2] = byte(v.Lo >> 16)
	buf[3] = byte(v.Lo >> 24)
	buf[4] = byte(v.Lo >> 32)
	buf[5] = byte(v.Lo >> 40)
	buf[6] = byte(v.Lo >> 48)
	buf[7] = byte(v.Lo >> 56)
	buf[8] = byte(v.Hi)
	buf[9] = byte(v.Hi >> 8)
	buf[10] = byte(v.Hi >> 16)
	buf[11] = byte(v.Hi >> 24)
	buf[12] = byte(v.Hi >> 32)
	buf[13] = byte(v.Hi >> 40)
	buf[14] = byte(v.Hi >> 48)
	buf[15] = byte(v.Hi >> 56)
	for i, j := 0, 15; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}
	return new(big.Int).SetBytes(buf[:])
}

// UInt128FromBigInt creates a UInt128 from a big.Int. Returns error if out of range.
func UInt128FromBigInt(bi *big.Int) (UInt128, error) {
	if bi == nil {
		return UInt128{}, fmt.Errorf("nil *big.Int")
	}
	if bi.Sign() < 0 {
		return UInt128{}, fmt.Errorf("UInt128 cannot be negative")
	}
	var be [16]byte
	b := bi.Bytes()
	if len(b) > 16 {
		return UInt128{}, fmt.Errorf("UInt128 value out of range")
	}
	copy(be[16-len(b):], b)
	for i, j := 0, 15; i < j; i, j = i+1, j-1 {
		be[i], be[j] = be[j], be[i]
	}
	return UInt128{
		Lo: uint64(be[0]) | uint64(be[1])<<8 | uint64(be[2])<<16 | uint64(be[3])<<24 |
			uint64(be[4])<<32 | uint64(be[5])<<40 | uint64(be[6])<<48 | uint64(be[7])<<56,
		Hi: uint64(be[8]) | uint64(be[9])<<8 | uint64(be[10])<<16 | uint64(be[11])<<24 |
			uint64(be[12])<<32 | uint64(be[13])<<40 | uint64(be[14])<<48 | uint64(be[15])<<56,
	}, nil
}

// UInt128Column is a UInt128 column.
type UInt128Column struct {
	name string
	Data []UInt128
}

// NewUInt128Column creates a UInt128Column.
func NewUInt128Column(name string) *UInt128Column {
	return &UInt128Column{name: name}
}

func (c *UInt128Column) Name() string                        { return c.name }
func (c *UInt128Column) Type() proto.ColumnType               { return proto.ColumnTypeUInt128 }
func (c *UInt128Column) Infer(t proto.ColumnType) error {
	if t.Base() != proto.ColumnTypeUInt128 {
		return fmt.Errorf("UInt128Column: expected UInt128, got %q", t.Base())
	}
	return nil
}
func (c *UInt128Column) Len() int                             { return len(c.Data) }
func (c *UInt128Column) Append(v UInt128)                      { c.Data = append(c.Data, v) }
func (c *UInt128Column) AppendArr(v []UInt128)                  { c.Data = append(c.Data, v...) }
func (c *UInt128Column) Row(i int) UInt128                      { return c.Data[i] }
func (c *UInt128Column) DataSlice(start, end int) []UInt128     { return c.Data[start:end] }
func (c *UInt128Column) Reset()                                 { c.Data = c.Data[:0] }

func (c *UInt128Column) DecodeColumn(r *proto.Reader, rows int) error {
	return decodeFixed(r, rows, 16, &c.Data)
}

func (c *UInt128Column) EncodeColumn(b *proto.Buffer) error {
	return encodeFixed(c.Data, 16, b)
}

func (c *UInt128Column) WriteColumn(w *proto.Writer) {
	writeFixed(c.Data, 16, w)
}
