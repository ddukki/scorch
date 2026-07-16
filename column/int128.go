package column

import (
	"fmt"
	"math/big"
	"unsafe"

	"github.com/ClickHouse/ch-go/proto"
)

// Int128 is a 128-bit signed integer.
type Int128 struct {
	Lo uint64
	Hi uint64
}

func neg128(v Int128) Int128 {
	lo := ^v.Lo + 1
	hi := ^v.Hi
	if lo == 0 {
		hi++
	}
	return Int128{Lo: lo, Hi: hi}
}

func (v Int128) String() string {
	if v.Hi&0x8000000000000000 != 0 {
		n := neg128(v)
		return "-0x" + hex128(n.Hi, n.Lo)
	}
	return "0x" + hex128(v.Hi, v.Lo)
}

func hex128(hi, lo uint64) string {
	// 16 hex chars per uint64, zero-padded
	return fmt.Sprintf("%016x%016x", hi, lo)
}

// Cmp compares v and x, returning -1/0/1.
func (v Int128) Cmp(x Int128) int {
	vNeg := v.Hi&0x8000000000000000 != 0
	xNeg := x.Hi&0x8000000000000000 != 0
	if vNeg != xNeg {
		if vNeg {
			return -1
		}
		return 1
	}
	// Same sign — compare magnitude
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
func (v Int128) ToBigInt() *big.Int {
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
	// buf is LE, need BE for big.Int — reverse in place
	for i, j := 0, 15; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}
	bi := new(big.Int).SetBytes(buf[:])
	if v.Hi&0x8000000000000000 != 0 {
		max := new(big.Int).Lsh(big.NewInt(1), 128)
		bi.Sub(bi, max)
	}
	return bi
}

// Int128FromBigInt creates an Int128 from a big.Int. Returns error if out of range.
func Int128FromBigInt(bi *big.Int) (Int128, error) {
	if bi == nil {
		return Int128{}, fmt.Errorf("nil *big.Int")
	}
	neg := bi.Sign() < 0
	mag := new(big.Int).Abs(bi)
	var be [16]byte
	b := mag.Bytes()
	if len(b) > 16 {
		return Int128{}, fmt.Errorf("Int128 value out of range")
	}
	copy(be[16-len(b):], b)
	// Reverse BE → LE
	for i, j := 0, 15; i < j; i, j = i+1, j-1 {
		be[i], be[j] = be[j], be[i]
	}
	v := Int128{
		Lo: uint64(be[0]) | uint64(be[1])<<8 | uint64(be[2])<<16 | uint64(be[3])<<24 |
			uint64(be[4])<<32 | uint64(be[5])<<40 | uint64(be[6])<<48 | uint64(be[7])<<56,
		Hi: uint64(be[8]) | uint64(be[9])<<8 | uint64(be[10])<<16 | uint64(be[11])<<24 |
			uint64(be[12])<<32 | uint64(be[13])<<40 | uint64(be[14])<<48 | uint64(be[15])<<56,
	}
	if neg {
		v = neg128(v)
		if v.Hi&0x8000000000000000 == 0 {
			return Int128{}, fmt.Errorf("Int128 value out of range")
		}
	} else if v.Hi&0x8000000000000000 != 0 {
		return Int128{}, fmt.Errorf("Int128 value out of range")
	}
	return v, nil
}

// Int128Column is an Int128 column.
type Int128Column struct {
	name string
	Data []Int128
}

// NewInt128Column creates an Int128Column.
func NewInt128Column(name string) *Int128Column {
	return &Int128Column{name: name}
}

func (c *Int128Column) Name() string                        { return c.name }
func (c *Int128Column) Type() proto.ColumnType               { return proto.ColumnTypeInt128 }
func (c *Int128Column) Infer(t proto.ColumnType) error {
	if t.Base() != proto.ColumnTypeInt128 {
		return fmt.Errorf("Int128Column: expected Int128, got %q", t.Base())
	}
	return nil
}
func (c *Int128Column) Len() int                             { return len(c.Data) }
func (c *Int128Column) Append(v Int128)                      { c.Data = append(c.Data, v) }
func (c *Int128Column) AppendArr(v []Int128)                  { c.Data = append(c.Data, v...) }
func (c *Int128Column) Row(i int) Int128                      { return c.Data[i] }
func (c *Int128Column) DataSlice(start, end int) []Int128     { return c.Data[start:end] }
func (c *Int128Column) Reset()                                { c.Data = c.Data[:0] }

func (c *Int128Column) DecodeColumn(r *proto.Reader, rows int) error {
	return decodeFixed(r, rows, 16, &c.Data)
}

func (c *Int128Column) EncodeColumn(b *proto.Buffer) error {
	return encodeFixed(c.Data, 16, b)
}

func (c *Int128Column) WriteColumn(w *proto.Writer) {
	writeFixed(c.Data, 16, w)
}

// Shared helpers for fixed-width types (16 or 32 byte elements).
func decodeFixed[T any](r *proto.Reader, rows int, elemSize int, data *[]T) error {
	if rows == 0 {
		*data = (*data)[:0]
		return nil
	}
	n, err := safeMul(rows, elemSize)
	if err != nil {
		return fmt.Errorf("decode column: %w", err)
	}
	if cap(*data) >= rows {
		*data = (*data)[:rows]
	} else {
		*data = make([]T, rows, rows*2)
	}
	dst := unsafe.Slice((*byte)(unsafe.Pointer(&(*data)[0])), n)
	return r.ReadFull(dst)
}

func encodeFixed[T any](data []T, elemSize int, b *proto.Buffer) error {
	if len(data) == 0 {
		return nil
	}
	n, err := safeMul(len(data), elemSize)
	if err != nil {
		return err
	}
	off := len(b.Buf)
	b.Buf = append(b.Buf, make([]byte, n)...)
	src := unsafe.Slice((*byte)(unsafe.Pointer(&data[0])), n)
	copy(b.Buf[off:], src)
	return nil
}

func writeFixed[T any](data []T, elemSize int, w *proto.Writer) {
	if len(data) == 0 {
		return
	}
	n, err := safeMul(len(data), elemSize)
	if err != nil {
		return
	}
	src := unsafe.Slice((*byte)(unsafe.Pointer(&data[0])), n)
	w.ChainWrite(src)
}
