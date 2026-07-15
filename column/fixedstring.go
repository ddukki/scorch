package column

import (
	"fmt"
	"strconv"

	"github.com/ClickHouse/ch-go/proto"
)

// FixedStringColumn is a FixedString(N) column storing N-byte fixed-size blocks.
type FixedStringColumn struct {
	name string
	n    int
	Data [][]byte
}

// NewFixedStringColumn creates a FixedStringColumn with the given column name and size n.
// Panics if n <= 0.
func NewFixedStringColumn(name string, n int) *FixedStringColumn {
	if n <= 0 {
		panic(fmt.Sprintf("column: NewFixedStringColumn: size %d <= 0", n))
	}
	return &FixedStringColumn{name: name, n: n}
}

func (c *FixedStringColumn) Name() string { return c.name }

func (c *FixedStringColumn) Type() proto.ColumnType {
	return proto.ColumnTypeFixedString.With(strconv.Itoa(c.n))
}

func (c *FixedStringColumn) Len() int { return len(c.Data) }

// Append appends v to the column. If len(v) >= n, v is truncated to n bytes
// and stored directly (reuses caller's backing array). If len(v) < n (including
// nil), a new n-byte buffer is allocated, v is copied in, and remaining bytes
// are zero-filled.
func (c *FixedStringColumn) Append(v []byte) {
	if len(v) >= c.n {
		v = v[:c.n]
		c.Data = append(c.Data, v)
		return
	}
	buf := make([]byte, c.n)
	copy(buf, v)
	c.Data = append(c.Data, buf)
}

func (c *FixedStringColumn) AppendArr(v [][]byte) {
	for _, vv := range v {
		c.Append(vv)
	}
}

func (c *FixedStringColumn) Row(i int) []byte { return c.Data[i] }

func (c *FixedStringColumn) Reset() { c.Data = c.Data[:0] }

func (c *FixedStringColumn) DecodeColumn(r *proto.Reader, rows int) error {
	for i := 0; i < rows; i++ {
		buf := make([]byte, c.n)
		if err := r.ReadFull(buf); err != nil {
			return err
		}
		c.Data = append(c.Data, buf)
	}
	return nil
}

func (c *FixedStringColumn) EncodeColumn(b *proto.Buffer) error {
	for _, v := range c.Data {
		b.Buf = append(b.Buf, v...)
	}
	return nil
}

func (c *FixedStringColumn) WriteColumn(w *proto.Writer) {
	w.ChainBuffer(func(b *proto.Buffer) {
		for _, v := range c.Data {
			b.Buf = append(b.Buf, v...)
		}
	})
}

// Infer parses "FixedString(N)" from t.Elem() and sets the internal size n.
func (c *FixedStringColumn) Infer(t proto.ColumnType) error {
	elem := string(t.Elem())
	if elem == "" {
		return fmt.Errorf("fixedstring: no elements in %q", t)
	}
	n, err := strconv.Atoi(elem)
	if err != nil {
		return fmt.Errorf("fixedstring: parse size: %w", err)
	}
	if n <= 0 {
		return fmt.Errorf("fixedstring: size %d <= 0", n)
	}
	c.n = n
	return nil
}
