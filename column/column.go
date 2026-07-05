package column

import (
	"unsafe"

	"github.com/ClickHouse/ch-go/proto"
)

type Column interface {
	Name() string
	Type() proto.ColumnType
	Len() int
	DecodeColumn(r *proto.Reader, rows int) error
	EncodeColumn(b *proto.Buffer) error
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

func (c *Base[T]) DataUnsafe() []T { return c.Data }

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
	n := rows * int(unsafe.Sizeof(zero))
	data, err := r.ReadRaw(n)
	if err != nil {
		return err
	}
	src := unsafe.Slice((*T)(unsafe.Pointer(&data[0])), rows)
	c.Data = make([]T, rows)
	copy(c.Data, src)
	return nil
}

func (c *Base[T]) EncodeColumn(b *proto.Buffer) error {
	if len(c.Data) == 0 {
		return nil
	}
	var zero T
	n := len(c.Data) * int(unsafe.Sizeof(zero))
	b.Buf = append(b.Buf, unsafe.Slice((*byte)(unsafe.Pointer(&c.Data[0])), n)...)
	return nil
}
