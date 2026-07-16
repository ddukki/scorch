package column

import (
	"errors"
	"fmt"
	"io"
	"unsafe"

	"github.com/ClickHouse/ch-go/proto"
)

// StrColumn is a String column with contiguous buffer storage.
type StrColumn struct {
	name string
	Data []string
	buf  []byte // contiguous string data
	pos  []int  // start offsets into buf; sentinel at len(pos)-1
	vib  []byte // 1-byte read buffer for inline UVarint (heap-resident, no escape)
}

// NewStrColumn creates a StrColumn with the given column name.
func NewStrColumn(name string) *StrColumn {
	return &StrColumn{name: name, vib: make([]byte, 1)}
}

// readUVarint reads a UVarint from r using c.vib (heap-resident buffer).
// Uses r.Read directly (bufio.Reader.Read) instead of r.ReadByte (7 call frames),
// cutting the call chain to 2 frames.
func (c *StrColumn) readUVarint(r *proto.Reader) (int, error) {
	var x uint64
	var s uint
	for i := 0; i < 10; i++ {
		n, err := r.Read(c.vib)
		if n == 0 {
			return 0, io.ErrUnexpectedEOF
		}
		if err != nil {
			return 0, err
		}
		b := c.vib[0]
		if b < 0x80 {
			if i == 9 && b > 1 {
				return 0, errors.New("uvarint overflow")
			}
			return int(x | uint64(b)<<s), nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, errors.New("uvarint overflow")
}

// Name returns the column name.
func (c *StrColumn) Name() string { return c.name }

// Type returns proto.ColumnTypeString.
func (c *StrColumn) Type() proto.ColumnType { return proto.ColumnTypeString }

func (c *StrColumn) Infer(t proto.ColumnType) error {
	if t.Base() != proto.ColumnTypeString {
		return fmt.Errorf("StrColumn: expected String, got %q", t.Base())
	}
	return nil
}

// Len returns the number of elements in the column.
func (c *StrColumn) Len() int { return len(c.Data) }

// Append adds a single string value to the column.
func (c *StrColumn) Append(v string) { c.Data = append(c.Data, v) }

// AppendArr adds multiple string values to the column.
func (c *StrColumn) AppendArr(v []string) { c.Data = append(c.Data, v...) }

// Row returns the value at index i.
func (c *StrColumn) Row(i int) string { return c.Data[i] }
func (c *StrColumn) DataSlice(start, end int) []string { return c.Data[start:end] }

// Reset clears the column data without releasing the backing array.
func (c *StrColumn) Reset() { c.Data = c.Data[:0] }

// DecodeColumn decodes string rows from the wire protocol.
func (c *StrColumn) DecodeColumn(r *proto.Reader, rows int) error {
	if rows == 0 {
		c.Data = c.Data[:0]
		c.buf = c.buf[:0]
		c.pos = c.pos[:0]
		return nil
	}
	if c.vib == nil {
		c.vib = make([]byte, 1)
	}

	c.buf = c.buf[:0]
	c.pos = c.pos[:0]

	want := rows + 1
	if cap(c.pos) < want {
		c.pos = make([]int, want, want*2)
	}
	c.pos = c.pos[:want]

	var end int
	for i := 0; i < rows; i++ {
		n, err := c.readUVarint(r)
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
		c.Data = make([]string, rows, rows*2)
	} else {
		c.Data = c.Data[:rows]
	}

	base := unsafe.Pointer(unsafe.SliceData(c.buf))
	for i := 0; i < rows; i++ {
		c.Data[i] = unsafe.String((*byte)(unsafe.Add(base, c.pos[i])), c.pos[i+1]-c.pos[i])
	}

	return nil
}

// EncodeColumn encodes string data to the wire buffer.
func (c *StrColumn) EncodeColumn(b *proto.Buffer) error {
	for _, v := range c.Data {
		b.PutString(v)
	}
	return nil
}

// WriteColumn writes the string column to the wire writer.
func (c *StrColumn) WriteColumn(w *proto.Writer) {
	w.ChainBuffer(func(b *proto.Buffer) {
		for _, v := range c.Data {
			b.PutString(v)
		}
	})
}
