package column

import (
	"unsafe"

	"github.com/ClickHouse/ch-go/proto"
)

// Nullable wraps an Of column with a null bitmap.
type Nullable[T any] struct {
	Values Of[T]
	Nulls  []bool
}

// NewNullable wraps a column into a Nullable column.
func NewNullable[T any](col Of[T]) *Nullable[T] {
	return &Nullable[T]{Values: col}
}

// Name returns the inner column name.
func (c *Nullable[T]) Name() string { return c.Values.Name() }

// Type returns proto.ColumnTypeNullable(inner).
func (c *Nullable[T]) Type() proto.ColumnType {
	return proto.ColumnTypeNullable.Sub(c.Values.Type())
}

// Len returns the number of elements in the column.
func (c *Nullable[T]) Len() int { return len(c.Nulls) }

// Append adds a value with its null flag.
func (c *Nullable[T]) Append(v T, isNull bool) {
	if isNull {
		var zero T
		c.Values.Append(zero)
	} else {
		c.Values.Append(v)
	}
	c.Nulls = append(c.Nulls, isNull)
}

// AppendArr adds multiple non-null values.
func (c *Nullable[T]) AppendArr(v []T) {
	for _, x := range v {
		c.Append(x, false)
	}
}

// Row returns the value at index i and whether it is null.
func (c *Nullable[T]) Row(i int) (T, bool) {
	return c.Values.Row(i), c.Nulls[i]
}

// DecodeColumn decodes nullable rows from the wire protocol.
func (c *Nullable[T]) DecodeColumn(r *proto.Reader, rows int) error {
	if rows == 0 {
		c.Nulls = c.Nulls[:0]
		return c.Values.DecodeColumn(r, 0)
	}
	if cap(c.Nulls) >= rows {
		c.Nulls = c.Nulls[:rows]
	} else {
		c.Nulls = make([]bool, rows, rows*2)
	}
	buf := unsafe.Slice((*byte)(unsafe.Pointer(&c.Nulls[0])), rows)
	if err := r.ReadFull(buf); err != nil {
		return err
	}
	return c.Values.DecodeColumn(r, rows)
}

// EncodeColumn encodes the null bitmap and inner data.
func (c *Nullable[T]) EncodeColumn(b *proto.Buffer) error {
	for _, isNull := range c.Nulls {
		if isNull {
			b.Buf = append(b.Buf, 1)
		} else {
			b.Buf = append(b.Buf, 0)
		}
	}
	return c.Values.EncodeColumn(b)
}

// WriteColumn writes the nullable column to the wire writer.
func (c *Nullable[T]) WriteColumn(w *proto.Writer) {
	w.ChainBuffer(func(b *proto.Buffer) {
		for _, isNull := range c.Nulls {
			if isNull {
				b.Buf = append(b.Buf, 1)
			} else {
				b.Buf = append(b.Buf, 0)
			}
		}
	})
	c.Values.WriteColumn(w)
}
