package column

import (
	"fmt"
	"log"
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

// Column is the interface that all columns implement.
type Column interface {
	Name() string
	Type() proto.ColumnType
	Len() int
	DecodeColumn(r *proto.Reader, rows int) error
	EncodeColumn(b *proto.Buffer) error
	WriteColumn(w *proto.Writer)
}

// ColumnOf extends Column with element access for typed columns.
type ColumnOf[T any] interface {
	Column
	Append(v T)
	AppendArr(v []T)
	Row(i int) T
}

// Base is a generic fixed-width column (UInt64, Float64, etc.).
type Base[T any] struct {
	name string
	Data []T
}

// NewBase creates a Base column with the given column name.
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
	if cap(c.Data) >= rows {
		c.Data = c.Data[:rows]
	} else {
		c.Data = make([]T, rows, rows*2)
	}
	switch elemSize {
	case 1, 2, 4, 8:
		buf := unsafe.Slice((*byte)(unsafe.Pointer(&c.Data[0])), n)
		if err := r.ReadFull(buf); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported element size %d for column %s", elemSize, c.name)
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
		return err
	}
	off := len(b.Buf)
	b.Buf = append(b.Buf, make([]byte, n)...)
	switch elemSize {
	case 1, 2, 4, 8:
		src := unsafe.Slice((*byte)(unsafe.Pointer(&c.Data[0])), n)
		copy(b.Buf[off:], src)
	default:
		return fmt.Errorf("unsupported element size %d for column %s", elemSize, c.name)
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
		log.Printf("chu-go/column: encode %s safeMul %d*%d: %v", c.name, len(c.Data), elemSize, err)
		return
	}
	switch elemSize {
	case 1, 2, 4, 8:
		src := unsafe.Slice((*byte)(unsafe.Pointer(&c.Data[0])), n)
		w.ChainWrite(src)
	default:
		log.Printf("chu-go/column: unsupported element size %d for column %s", elemSize, c.name)
	}
}
