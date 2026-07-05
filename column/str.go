package column

import (
	"github.com/ClickHouse/ch-go/proto"
)

type Str struct {
	name string
	Data []string
}

func NewStr(name string) *Str {
	return &Str{name: name}
}

func (c *Str) Name() string { return c.name }

func (c *Str) Type() proto.ColumnType { return proto.ColumnTypeString }

func (c *Str) Len() int { return len(c.Data) }

func (c *Str) Append(v string) { c.Data = append(c.Data, v) }

func (c *Str) AppendArr(v []string) { c.Data = append(c.Data, v...) }

func (c *Str) Row(i int) string { return c.Data[i] }

func (c *Str) DecodeColumn(r *proto.Reader, rows int) error {
	if rows == 0 {
		c.Data = c.Data[:0]
		return nil
	}
	c.Data = make([]string, rows)
	for i := 0; i < rows; i++ {
		v, err := r.Str()
		if err != nil {
			return err
		}
		c.Data[i] = v
	}
	return nil
}

func (c *Str) EncodeColumn(b *proto.Buffer) error {
	for _, v := range c.Data {
		b.PutString(v)
	}
	return nil
}
