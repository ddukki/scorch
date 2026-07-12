package column

import (
	"unsafe"

	"github.com/ClickHouse/ch-go/proto"
)

type Str struct {
	name string
	Data []string
	buf  []byte  // contiguous string data
	pos  []int   // start offsets into buf; sentinel at len(pos)-1
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
		c.buf = c.buf[:0]
		c.pos = c.pos[:0]
		return nil
	}

	c.buf = c.buf[:0]
	c.pos = c.pos[:0]

	want := rows + 1
	if cap(c.pos) < want {
		c.pos = make([]int, want)
	}
	c.pos = c.pos[:want]

	var end int
	for i := 0; i < rows; i++ {
		n, err := r.StrLen()
		if err != nil {
			return err
		}
		c.pos[i] = end
		end += n
		if cap(c.buf) < end {
			newCap := end + n*(rows-i)
			if n >= 128 {
				newCap = end
			}
			b := make([]byte, end, newCap)
			copy(b, c.buf)
			c.buf = b
		} else {
			c.buf = c.buf[:end]
		}
		if err := r.ReadFull(c.buf[c.pos[i]:end]); err != nil {
			return err
		}
	}
	c.pos[rows] = end

	if cap(c.Data) < rows {
		c.Data = make([]string, rows)
	} else {
		c.Data = c.Data[:rows]
	}

	base := unsafe.Pointer(unsafe.SliceData(c.buf))
	for i := 0; i < rows; i++ {
		c.Data[i] = unsafe.String((*byte)(unsafe.Add(base, c.pos[i])), c.pos[i+1]-c.pos[i])
	}

	return nil
}

func (c *Str) EncodeColumn(b *proto.Buffer) error {
	for _, v := range c.Data {
		b.PutString(v)
	}
	return nil
}

func (c *Str) WriteColumn(w *proto.Writer) {
	w.ChainBuffer(func(b *proto.Buffer) {
		for _, v := range c.Data {
			b.PutString(v)
		}
	})
}
