package column

import (
	"fmt"
	"unsafe"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/google/uuid"
)

// UUID is a 16-byte UUID value.
type UUID [16]byte

// UUIDColumn is a UUID column.
type UUIDColumn struct {
	name string
	Data []UUID
}

// NewUUIDColumn creates a UUIDColumn with the given column name.
func NewUUIDColumn(name string) *UUIDColumn {
	return &UUIDColumn{name: name}
}

// Name returns the column name.
func (c *UUIDColumn) Name() string { return c.name }

// Type returns proto.ColumnTypeUUID.
func (c *UUIDColumn) Type() proto.ColumnType { return proto.ColumnTypeUUID }
func (c *UUIDColumn) Infer(t proto.ColumnType) error {
	if t.Base() != proto.ColumnTypeUUID {
		return fmt.Errorf("UUIDColumn: expected UUID, got %q", t.Base())
	}
	return nil
}

// Len returns the number of elements in the column.
func (c *UUIDColumn) Len() int { return len(c.Data) }

// Reset clears the column data without releasing the backing array.
func (c *UUIDColumn) Reset() { c.Data = c.Data[:0] }

// Append adds a single UUID value.
func (c *UUIDColumn) Append(v UUID) { c.Data = append(c.Data, v) }

// AppendArr adds multiple UUID values.
func (c *UUIDColumn) AppendArr(vs []UUID) { c.Data = append(c.Data, vs...) }

// Row returns the UUID at index i.
func (c *UUIDColumn) Row(i int) UUID { return c.Data[i] }
func (c *UUIDColumn) DataSlice(start, end int) []UUID { return c.Data[start:end] }

// DecodeColumn decodes UUID rows from the wire protocol.
func (c *UUIDColumn) DecodeColumn(r *proto.Reader, rows int) error {
	if rows == 0 {
		c.Data = c.Data[:0]
		return nil
	}
	c.Data = make([]UUID, rows)
	dest := unsafe.Slice((*byte)(unsafe.Pointer(&c.Data[0])), rows*16)
	return r.ReadFull(dest)
}

// EncodeColumn encodes UUID data to the wire buffer.
func (c *UUIDColumn) EncodeColumn(b *proto.Buffer) error {
	if len(c.Data) == 0 {
		return nil
	}
	off := len(b.Buf)
	b.Buf = append(b.Buf, make([]byte, len(c.Data)*16)...)
	src := unsafe.Slice((*byte)(unsafe.Pointer(&c.Data[0])), len(c.Data)*16)
	copy(b.Buf[off:], src)
	return nil
}

// WriteColumn writes the UUID column to the wire writer.
func (c *UUIDColumn) WriteColumn(w *proto.Writer) {
	w.ChainBuffer(func(b *proto.Buffer) {
		if len(c.Data) == 0 {
			return
		}
		off := len(b.Buf)
		b.Buf = append(b.Buf, make([]byte, len(c.Data)*16)...)
		src := unsafe.Slice((*byte)(unsafe.Pointer(&c.Data[0])), len(c.Data)*16)
		copy(b.Buf[off:], src)
	})
}

// UUIDToGo converts a scorch UUID to a google/uuid UUID.
func UUIDToGo(u UUID) uuid.UUID { return uuid.UUID(u) }

// GoToUUID converts a google/uuid UUID to a scorch UUID.
func GoToUUID(u uuid.UUID) UUID { return UUID(u) }

// String returns the hyphenated hex format.
func (v UUID) String() string { return uuid.UUID(v).String() }
