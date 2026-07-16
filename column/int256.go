package column

import (
	"fmt"
	"math/big"

	"github.com/ClickHouse/ch-go/proto"
)

// Int256 is a 256-bit signed integer.
type Int256 struct {
	Lo Int128
	Hi Int128
}

func neg256(v Int256) Int256 {
	// ~v + 1 across 256 bits
	lo := Int128{Lo: ^v.Lo.Lo, Hi: ^v.Lo.Hi}
	hi := Int128{Lo: ^v.Hi.Lo, Hi: ^v.Hi.Hi}
	lo.Lo++
	if lo.Lo == 0 {
		lo.Hi++
		if lo.Hi == 0 {
			hi.Lo++
			if hi.Lo == 0 {
				hi.Hi++
			}
		}
	}
	return Int256{Lo: lo, Hi: hi}
}

func (v Int256) String() string {
	if v.Hi.Hi&0x8000000000000000 != 0 {
		n := neg256(v)
		return "-0x" + hex256(n.Hi, n.Lo)
	}
	return "0x" + hex256(v.Hi, v.Lo)
}

func hex256(hi, lo Int128) string {
	return fmt.Sprintf("%016x%016x%016x%016x", hi.Hi, hi.Lo, lo.Hi, lo.Lo)
}

// Cmp compares v and x, returning -1/0/1.
func (v Int256) Cmp(x Int256) int {
	vNeg := v.Hi.Hi&0x8000000000000000 != 0
	xNeg := x.Hi.Hi&0x8000000000000000 != 0
	if vNeg != xNeg {
		if vNeg {
			return -1
		}
		return 1
	}
	// Same sign — compare magnitude
	if v.Hi.Cmp(x.Hi) != 0 {
		return v.Hi.Cmp(x.Hi)
	}
	return v.Lo.Cmp(x.Lo)
}

// ToBigInt converts to a big.Int.
func (v Int256) ToBigInt() *big.Int {
	var buf [32]byte
	// Lo (first 16 bytes LE)
	putLE128(buf[0:16], v.Lo)
	putLE128(buf[16:32], v.Hi)
	// Reverse to BE
	for i, j := 0, 31; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}
	bi := new(big.Int).SetBytes(buf[:])
	if v.Hi.Hi&0x8000000000000000 != 0 {
		max := new(big.Int).Lsh(big.NewInt(1), 256)
		bi.Sub(bi, max)
	}
	return bi
}

// Int256FromBigInt creates an Int256 from a big.Int.
func Int256FromBigInt(bi *big.Int) (Int256, error) {
	if bi == nil {
		return Int256{}, fmt.Errorf("nil *big.Int")
	}
	neg := bi.Sign() < 0
	mag := new(big.Int).Abs(bi)
	var be [32]byte
	b := mag.Bytes()
	if len(b) > 32 {
		return Int256{}, fmt.Errorf("Int256 value out of range")
	}
	copy(be[32-len(b):], b)
	for i, j := 0, 31; i < j; i, j = i+1, j-1 {
		be[i], be[j] = be[j], be[i]
	}
	v := Int256{
		Lo: Int128{Lo: readLE64(be[0:8]), Hi: readLE64(be[8:16])},
		Hi: Int128{Lo: readLE64(be[16:24]), Hi: readLE64(be[24:32])},
	}
	if neg {
		v = neg256(v)
		if v.Hi.Hi&0x8000000000000000 == 0 {
			return Int256{}, fmt.Errorf("Int256 value out of range")
		}
	} else if v.Hi.Hi&0x8000000000000000 != 0 {
		return Int256{}, fmt.Errorf("Int256 value out of range")
	}
	return v, nil
}

func putLE128(buf []byte, v Int128) {
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
}

func readLE64(buf []byte) uint64 {
	return uint64(buf[0]) | uint64(buf[1])<<8 | uint64(buf[2])<<16 | uint64(buf[3])<<24 |
		uint64(buf[4])<<32 | uint64(buf[5])<<40 | uint64(buf[6])<<48 | uint64(buf[7])<<56
}

// Int256Column is an Int256 column.
type Int256Column struct {
	name string
	Data []Int256
}

// NewInt256Column creates an Int256Column.
func NewInt256Column(name string) *Int256Column {
	return &Int256Column{name: name}
}

func (c *Int256Column) Name() string                        { return c.name }
func (c *Int256Column) Type() proto.ColumnType               { return proto.ColumnTypeInt256 }
func (c *Int256Column) Infer(t proto.ColumnType) error {
	if t.Base() != proto.ColumnTypeInt256 {
		return fmt.Errorf("Int256Column: expected Int256, got %q", t.Base())
	}
	return nil
}
func (c *Int256Column) Len() int                             { return len(c.Data) }
func (c *Int256Column) Append(v Int256)                      { c.Data = append(c.Data, v) }
func (c *Int256Column) AppendArr(v []Int256)                  { c.Data = append(c.Data, v...) }
func (c *Int256Column) Row(i int) Int256                      { return c.Data[i] }
func (c *Int256Column) DataSlice(start, end int) []Int256     { return c.Data[start:end] }
func (c *Int256Column) Reset()                                { c.Data = c.Data[:0] }

func (c *Int256Column) DecodeColumn(r *proto.Reader, rows int) error {
	return decodeFixed(r, rows, 32, &c.Data)
}

func (c *Int256Column) EncodeColumn(b *proto.Buffer) error {
	return encodeFixed(c.Data, 32, b)
}

func (c *Int256Column) WriteColumn(w *proto.Writer) {
	writeFixed(c.Data, 32, w)
}
