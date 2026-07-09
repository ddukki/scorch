package column

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"

	"github.com/ClickHouse/ch-go/proto"
)

type LowCardinality[T comparable] struct {
	Values ColumnOf[T]
	keys   []T
	idxBuf []byte
}

func NewLowCardinality[T comparable](col ColumnOf[T]) *LowCardinality[T] {
	return &LowCardinality[T]{Values: col}
}

func (c *LowCardinality[T]) Name() string { return c.Values.Name() }

func (c *LowCardinality[T]) Type() proto.ColumnType {
	return proto.ColumnTypeLowCardinality.Sub(c.Values.Type())
}

func (c *LowCardinality[T]) Len() int { return c.Values.Len() }

func (c *LowCardinality[T]) DecodeColumn(r *proto.Reader, rows int) error {
	if rows == 0 {
		return nil
	}
	if rows > 100_000_000 || rows < 0 {
		return fmt.Errorf("rows %d out of range", rows)
	}
	// serialization version byte
	if _, err := r.ReadByte(); err != nil {
		return err
	}

	keyCount, err := r.Int()
	if err != nil {
		return err
	}
	if keyCount > 100_000_000 || keyCount < 0 {
		return fmt.Errorf("key count %d out of range", keyCount)
	}

	// Decode dictionary keys
	c.keys = append(c.keys[:0], make([]T, keyCount)...)
	if err := c.decodeKeys(r, keyCount); err != nil {
		return err
	}

	// Index encoding: 1/2/4 bytes per index based on key count
	var indexSize int
	switch {
	case keyCount <= 256:
		indexSize = 1
	case keyCount <= 65536:
		indexSize = 2
	default:
		indexSize = 4
	}

	n, err := safeMul(rows, indexSize)
	if err != nil {
		return fmt.Errorf("low cardinality index buffer: %w", err)
	}
	c.idxBuf = append(c.idxBuf[:0], make([]byte, n)...)
	if err := r.ReadFull(c.idxBuf); err != nil {
		return err
	}

	keyLen := len(c.keys)
	for i := 0; i < rows; i++ {
		var idx int
		switch indexSize {
		case 1:
			idx = int(c.idxBuf[i])
		case 2:
			idx = int(binary.LittleEndian.Uint16(c.idxBuf[i*2:]))
		case 4:
			idx = int(binary.LittleEndian.Uint32(c.idxBuf[i*4:]))
		}
		if idx >= keyLen || idx < 0 {
			return fmt.Errorf("key index %d out of range [0, %d)", idx, keyLen)
		}
		c.Values.Append(c.keys[idx])
	}
	return nil
}

func (c *LowCardinality[T]) WriteColumn(w *proto.Writer) {
	w.ChainBuffer(func(b *proto.Buffer) {
		if err := c.EncodeColumn(b); err != nil {
			panic(err)
		}
	})
}

func (c *LowCardinality[T]) EncodeColumn(b *proto.Buffer) error {
	// Build dictionary: collect unique keys
	dict := make(map[T]int)
	for i := 0; i < c.Values.Len(); i++ {
		v := c.Values.Row(i)
		if _, ok := dict[v]; !ok {
			dict[v] = len(dict)
		}
	}

	// Write serialization version
	b.Buf = append(b.Buf, 1)

	// Write dictionary
	b.PutUVarInt(uint64(len(dict)))
	if err := c.encodeKeys(b, dict); err != nil {
		return err
	}

	// Write index type
	var indexSize int
	switch {
	case len(dict) <= 256:
		indexSize = 1
	case len(dict) <= 65536:
		indexSize = 2
	default:
		indexSize = 4
	}

	// Write indices
	for i := 0; i < c.Values.Len(); i++ {
		v := c.Values.Row(i)
		idx := dict[v]
		switch indexSize {
		case 1:
			b.Buf = append(b.Buf, byte(idx))
		case 2:
			var buf [2]byte
			binary.LittleEndian.PutUint16(buf[:], uint16(idx))
			b.Buf = append(b.Buf, buf[:]...)
		case 4:
			var buf [4]byte
			binary.LittleEndian.PutUint32(buf[:], uint32(idx))
			b.Buf = append(b.Buf, buf[:]...)
		}
	}
	return nil
}

func (c *LowCardinality[T]) decodeKeys(r *proto.Reader, n int) error {
	var zero T
	switch any(zero).(type) {
	case uint8:
		data, err := r.ReadRaw(n)
		if err != nil {
			return err
		}
		src := unsafe.Slice((*uint8)(unsafe.Pointer(&data[0])), n)
		for i, v := range src {
			c.keys[i] = any(v).(T)
		}
	case uint16:
		data, err := r.ReadRaw(n * 2)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			c.keys[i] = any(binary.LittleEndian.Uint16(data[i*2:])).(T)
		}
	case uint32:
		data, err := r.ReadRaw(n * 4)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			c.keys[i] = any(binary.LittleEndian.Uint32(data[i*4:])).(T)
		}
	case uint64:
		data, err := r.ReadRaw(n * 8)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			c.keys[i] = any(binary.LittleEndian.Uint64(data[i*8:])).(T)
		}
	case int8:
		data, err := r.ReadRaw(n)
		if err != nil {
			return err
		}
		for i, v := range data {
			c.keys[i] = any(int8(v)).(T)
		}
	case int16:
		data, err := r.ReadRaw(n * 2)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			c.keys[i] = any(int16(binary.LittleEndian.Uint16(data[i*2:]))).(T)
		}
	case int32:
		data, err := r.ReadRaw(n * 4)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			c.keys[i] = any(int32(binary.LittleEndian.Uint32(data[i*4:]))).(T)
		}
	case int64:
		data, err := r.ReadRaw(n * 8)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			c.keys[i] = any(int64(binary.LittleEndian.Uint64(data[i*8:]))).(T)
		}
	case float32:
		data, err := r.ReadRaw(n * 4)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			c.keys[i] = any(math.Float32frombits(binary.LittleEndian.Uint32(data[i*4:]))).(T)
		}
	case float64:
		data, err := r.ReadRaw(n * 8)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			c.keys[i] = any(math.Float64frombits(binary.LittleEndian.Uint64(data[i*8:]))).(T)
		}
	case string:
		for i := 0; i < n; i++ {
			v, err := r.Str()
			if err != nil {
				return err
			}
			c.keys[i] = any(v).(T)
		}
	}
	return nil
}

func (c *LowCardinality[T]) encodeKeys(b *proto.Buffer, dict map[T]int) error {
	// Build a reverse index: position → key
	ordered := make([]T, len(dict))
	for k, idx := range dict {
		ordered[idx] = k
	}

	var zero T
	switch any(zero).(type) {
	case uint8:
		for _, k := range ordered {
			b.Buf = append(b.Buf, byte(any(k).(uint8)))
		}
	case uint16:
		for _, k := range ordered {
			var buf [2]byte
			binary.LittleEndian.PutUint16(buf[:], any(k).(uint16))
			b.Buf = append(b.Buf, buf[:]...)
		}
	case uint32:
		for _, k := range ordered {
			var buf [4]byte
			binary.LittleEndian.PutUint32(buf[:], any(k).(uint32))
			b.Buf = append(b.Buf, buf[:]...)
		}
	case uint64:
		for _, k := range ordered {
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], any(k).(uint64))
			b.Buf = append(b.Buf, buf[:]...)
		}
	case int8:
		for _, k := range ordered {
			b.Buf = append(b.Buf, byte(any(k).(int8)))
		}
	case int16:
		for _, k := range ordered {
			var buf [2]byte
			binary.LittleEndian.PutUint16(buf[:], uint16(any(k).(int16)))
			b.Buf = append(b.Buf, buf[:]...)
		}
	case int32:
		for _, k := range ordered {
			var buf [4]byte
			binary.LittleEndian.PutUint32(buf[:], uint32(any(k).(int32)))
			b.Buf = append(b.Buf, buf[:]...)
		}
	case int64:
		for _, k := range ordered {
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], uint64(any(k).(int64)))
			b.Buf = append(b.Buf, buf[:]...)
		}
	case float32:
		for _, k := range ordered {
			var buf [4]byte
			binary.LittleEndian.PutUint32(buf[:], math.Float32bits(any(k).(float32)))
			b.Buf = append(b.Buf, buf[:]...)
		}
	case float64:
		for _, k := range ordered {
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], math.Float64bits(any(k).(float64)))
			b.Buf = append(b.Buf, buf[:]...)
		}
	case string:
		for _, k := range ordered {
			b.PutString(any(k).(string))
		}
	}
	return nil
}
