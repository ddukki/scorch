package column

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"

	"github.com/ClickHouse/ch-go/proto"
)

func safeMul(rows, elemSize int) (int, error) {
	if rows <= 0 || elemSize <= 0 {
		return 0, fmt.Errorf("non-positive multiplier")
	}
	if math.MaxInt/rows < elemSize {
		return 0, fmt.Errorf("integer overflow: %d * %d", rows, elemSize)
	}
	return rows * elemSize, nil
}

type Column interface {
	Name() string
	Type() proto.ColumnType
	Len() int
	DecodeColumn(r *proto.Reader, rows int) error
	EncodeColumn(b *proto.Buffer) error
	WriteColumn(w *proto.Writer)
}

type ColumnOf[T any] interface {
	Column
	Append(v T)
	AppendArr(v []T)
	Row(i int) T
}

type Base[T any] struct {
	name string
	Data []T
}

func NewBase[T any](name string) *Base[T] {
	return &Base[T]{name: name}
}

func (c *Base[T]) Name() string { return c.name }

func (c *Base[T]) Len() int { return len(c.Data) }

func (c *Base[T]) Append(v T) { c.Data = append(c.Data, v) }

func (c *Base[T]) AppendArr(v []T) { c.Data = append(c.Data, v...) }

func (c *Base[T]) Row(i int) T { return c.Data[i] }

func (c *Base[T]) Type() proto.ColumnType {
	var zero T
	switch any(zero).(type) {
	case uint8:
		return proto.ColumnTypeUInt8
	case uint16:
		return proto.ColumnTypeUInt16
	case uint32:
		return proto.ColumnTypeUInt32
	case uint64:
		return proto.ColumnTypeUInt64
	case int8:
		return proto.ColumnTypeInt8
	case int16:
		return proto.ColumnTypeInt16
	case int32:
		return proto.ColumnTypeInt32
	case int64:
		return proto.ColumnTypeInt64
	case float32:
		return proto.ColumnTypeFloat32
	case float64:
		return proto.ColumnTypeFloat64
	default:
		return ""
	}
}

func (c *Base[T]) DecodeColumn(r *proto.Reader, rows int) error {
	if rows == 0 {
		c.Data = c.Data[:0]
		return nil
	}
	var zero T
	elemSize := int(unsafe.Sizeof(zero))
	n, err := safeMul(rows, elemSize)
	if err != nil {
		return fmt.Errorf("decode column %s: %w", c.name, err)
	}
	raw := make([]byte, n)
	if err := r.ReadFull(raw); err != nil {
		return err
	}
	c.Data = make([]T, rows)
	for i := 0; i < rows; i++ {
		switch elemSize {
		case 1:
			*(*uint8)(unsafe.Pointer(&c.Data[i])) = raw[i]
		case 2:
			*(*uint16)(unsafe.Pointer(&c.Data[i])) = binary.LittleEndian.Uint16(raw[i*2:])
		case 4:
			*(*uint32)(unsafe.Pointer(&c.Data[i])) = binary.LittleEndian.Uint32(raw[i*4:])
		case 8:
			*(*uint64)(unsafe.Pointer(&c.Data[i])) = binary.LittleEndian.Uint64(raw[i*8:])
		}
	}
	return nil
}

func (c *Base[T]) EncodeColumn(b *proto.Buffer) error {
	if len(c.Data) == 0 {
		return nil
	}
	var zero T
	elemSize := int(unsafe.Sizeof(zero))
	n, err := safeMul(len(c.Data), elemSize)
	if err != nil {
		return fmt.Errorf("encode column %s: %w", c.name, err)
	}
	off := len(b.Buf)
	b.Buf = append(b.Buf, make([]byte, n)...)
	for i, v := range c.Data {
		switch elemSize {
		case 1:
			b.Buf[off+i] = *(*byte)(unsafe.Pointer(&v))
		case 2:
			binary.LittleEndian.PutUint16(b.Buf[off+i*2:], *(*uint16)(unsafe.Pointer(&v)))
		case 4:
			binary.LittleEndian.PutUint32(b.Buf[off+i*4:], *(*uint32)(unsafe.Pointer(&v)))
		case 8:
			binary.LittleEndian.PutUint64(b.Buf[off+i*8:], *(*uint64)(unsafe.Pointer(&v)))
		}
	}
	return nil
}

func (c *Base[T]) WriteColumn(w *proto.Writer) {
	if len(c.Data) == 0 {
		return
	}
	var zero T
	elemSize := int(unsafe.Sizeof(zero))
	n, err := safeMul(len(c.Data), elemSize)
	if err != nil {
		panic(fmt.Errorf("write column %s: %w", c.name, err))
	}
	b := make([]byte, n)
	for i, v := range c.Data {
		switch elemSize {
		case 1:
			b[i] = *(*byte)(unsafe.Pointer(&v))
		case 2:
			binary.LittleEndian.PutUint16(b[i*2:], *(*uint16)(unsafe.Pointer(&v)))
		case 4:
			binary.LittleEndian.PutUint32(b[i*4:], *(*uint32)(unsafe.Pointer(&v)))
		case 8:
			binary.LittleEndian.PutUint64(b[i*8:], *(*uint64)(unsafe.Pointer(&v)))
		}
	}
	w.ChainWrite(b)
}
