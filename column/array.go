package column

import (
	"encoding/binary"
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
)

type arrayElem[T any] interface {
	Of[T]
	Infer(proto.ColumnType) error
}

type ArrayColumn[T any] struct {
	name    string
	Offsets []uint64
	Elem    Of[T]
	infer   arrayElem[T]
}

func NewArrayColumn[T any](name string, elem Of[T]) *ArrayColumn[T] {
	c := &ArrayColumn[T]{name: name, Elem: elem}
	if ie, ok := elem.(arrayElem[T]); ok {
		c.infer = ie
	}
	return c
}

func (c *ArrayColumn[T]) Name() string { return c.name }

func (c *ArrayColumn[T]) Type() proto.ColumnType {
	return proto.ColumnTypeArray.With(c.Elem.Type().String())
}

func (c *ArrayColumn[T]) Len() int { return len(c.Offsets) }

func (c *ArrayColumn[T]) Reset() {
	c.Offsets = c.Offsets[:0]
	c.Elem.Reset()
}

func (c *ArrayColumn[T]) Append(v []T) {
	prev := uint64(c.Elem.Len())
	c.Elem.AppendArr(v)
	c.Offsets = append(c.Offsets, prev+uint64(len(v)))
}

func (c *ArrayColumn[T]) AppendArr(v [][]T) {
	for _, a := range v {
		c.Append(a)
	}
}

func (c *ArrayColumn[T]) Row(i int) []T {
	lo := uint64(0)
	if i > 0 {
		lo = c.Offsets[i-1]
	}
	hi := c.Offsets[i]
	if sa, ok := c.Elem.(SliceAccessor[T]); ok {
		return sa.DataSlice(int(lo), int(hi))
	}
	out := make([]T, hi-lo)
	for j := range out {
		out[j] = c.Elem.Row(int(lo) + j)
	}
	return out
}

func (c *ArrayColumn[T]) Infer(t proto.ColumnType) error {
	base := t.Base()
	if base != proto.ColumnTypeArray {
		return fmt.Errorf("array: expected Array, got %q", base)
	}
	elemType := t.Elem()
	if c.infer == nil {
		return fmt.Errorf("array: element column does not support Infer")
	}
	if err := c.infer.Infer(elemType); err != nil {
		return fmt.Errorf("array: infer element: %w", err)
	}
	return nil
}

func (c *ArrayColumn[T]) DecodeColumn(r *proto.Reader, rows int) error {
	c.Offsets = c.Offsets[:0]
	for i := 0; i < rows; i++ {
		v, err := r.UVarInt()
		if err != nil {
			return fmt.Errorf("array: read offset %d: %w", i, err)
		}
		c.Offsets = append(c.Offsets, v)
	}
	totalElems := 0
	if rows > 0 {
		totalElems = int(c.Offsets[rows-1])
	}
	return c.Elem.DecodeColumn(r, totalElems)
}

func (c *ArrayColumn[T]) EncodeColumn(b *proto.Buffer) error {
	if err := encodeUVarInts(c.Offsets, b); err != nil {
		return err
	}
	return c.Elem.EncodeColumn(b)
}

func (c *ArrayColumn[T]) WriteColumn(w *proto.Writer) {
	writeUVarInts(c.Offsets, w)
	c.Elem.WriteColumn(w)
}

func encodeUVarInts(offsets []uint64, b *proto.Buffer) error {
	var buf [binary.MaxVarintLen64]byte
	for _, v := range offsets {
		n := binary.PutUvarint(buf[:], v)
		b.Buf = append(b.Buf, buf[:n]...)
	}
	return nil
}

func writeUVarInts(offsets []uint64, w *proto.Writer) {
	var buf [binary.MaxVarintLen64]byte
	for _, v := range offsets {
		n := binary.PutUvarint(buf[:], v)
		w.ChainWrite(buf[:n])
	}
}
