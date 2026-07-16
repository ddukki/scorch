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

// Of is a typed column interface for element access.
type Of[T any] interface {
	Column
	Append(v T)
	AppendArr(v []T)
	Row(i int) T
	Reset()
}

// SliceAccessor provides zero-copy sub-slice access to backing Data.
// Implemented by types where Go T matches the backing slice element type.
// Not implemented by Enum, Decimal (backing type differs from T).
type SliceAccessor[T any] interface {
	DataSlice(start, end int) []T
}

// BaseColumn is a generic fixed-width column (UInt64, Float64, etc.).
type BaseColumn[T any] struct {
	name string
	Data []T
}

// NewBaseColumn creates a BaseColumn with the given column name.
func NewBaseColumn[T any](name string) *BaseColumn[T] {
	return &BaseColumn[T]{name: name}
}

// Name returns the column name.
func (c *BaseColumn[T]) Name() string { return c.name }

// Len returns the number of elements in the column.
func (c *BaseColumn[T]) Len() int { return len(c.Data) }

// Append adds a single value to the column.
func (c *BaseColumn[T]) Append(v T) { c.Data = append(c.Data, v) }

// AppendArr adds multiple values to the column.
func (c *BaseColumn[T]) AppendArr(v []T) { c.Data = append(c.Data, v...) }

// Row returns the value at index i.
func (c *BaseColumn[T]) Row(i int) T { return c.Data[i] }
func (c *BaseColumn[T]) DataSlice(start, end int) []T { return c.Data[start:end] }

// Infer validates that t matches this column's fixed type.
func (c *BaseColumn[T]) Infer(t proto.ColumnType) error {
	if t != c.Type() {
		return fmt.Errorf("BaseColumn: expected type %s, got %s", c.Type(), t)
	}
	return nil
}

// Reset clears the column data without releasing the backing array.
func (c *BaseColumn[T]) Reset() { c.Data = c.Data[:0] }

// Type returns the column's ClickHouse wire type.
func (c *BaseColumn[T]) Type() proto.ColumnType {
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

// DecodeColumn decodes rows from the wire protocol into the backing array.
func (c *BaseColumn[T]) DecodeColumn(r *proto.Reader, rows int) error {
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

// EncodeColumn encodes the column data to the wire buffer.
func (c *BaseColumn[T]) EncodeColumn(b *proto.Buffer) error {
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

// WriteColumn writes the column data to the wire writer.
func (c *BaseColumn[T]) WriteColumn(w *proto.Writer) {
	if len(c.Data) == 0 {
		return
	}
	var zero T
	elemSize := int(unsafe.Sizeof(zero))
	n, err := safeMul(len(c.Data), elemSize)
	if err != nil {
		log.Printf("scorch/column: encode %s safeMul %d*%d: %v", c.name, len(c.Data), elemSize, err)
		return
	}
	switch elemSize {
	case 1, 2, 4, 8:
		src := unsafe.Slice((*byte)(unsafe.Pointer(&c.Data[0])), n)
		w.ChainWrite(src)
	default:
		log.Printf("scorch/column: unsupported element size %d for column %s", elemSize, c.name)
	}
}
