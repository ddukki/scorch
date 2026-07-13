package column

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// DateTime is a DateTime column (stored as UInt32).
type DateTime struct {
	name string
	Data []uint32
}

// NewDateTime creates a DateTime column with the given column name.
func NewDateTime(name string) *DateTime {
	return &DateTime{name: name}
}

func (c *DateTime) Name() string { return c.name }

func (c *DateTime) Type() proto.ColumnType { return proto.ColumnTypeDateTime }

func (c *DateTime) Len() int { return len(c.Data) }

func (c *DateTime) Append(v time.Time) {
	c.Data = append(c.Data, uint32(v.Unix()))
}

func (c *DateTime) AppendArr(vs []time.Time) {
	for _, v := range vs {
		c.Data = append(c.Data, uint32(v.Unix()))
	}
}

func (c *DateTime) Row(i int) time.Time {
	return time.Unix(int64(c.Data[i]), 0)
}

func (c *DateTime) DecodeColumn(r *proto.Reader, rows int) error {
	if rows == 0 {
		c.Data = c.Data[:0]
		return nil
	}
	c.Data = make([]uint32, rows)
	for i := range c.Data {
		v, err := r.UInt32()
		if err != nil {
			return err
		}
		c.Data[i] = v
	}
	return nil
}

func (c *DateTime) EncodeColumn(b *proto.Buffer) error {
	for _, v := range c.Data {
		b.PutUInt32(v)
	}
	return nil
}

func (c *DateTime) WriteColumn(w *proto.Writer) {
	w.ChainBuffer(func(b *proto.Buffer) {
		for _, v := range c.Data {
			b.PutUInt32(v)
		}
	})
}
