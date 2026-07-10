package column

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"

	"github.com/ClickHouse/ch-go/proto"
)

const (
	keySerializationVersion         = 1
	cardinalityKeyMask       int64  = 0b0000_1111_1111
	cardinalityNeedGlobalDict int64 = 1 << 8
	cardinalityHasAdditionalKeys    = 1 << 9
	cardinalityNeedUpdateDict       = 1 << 10
	cardinalityUpdateAll            = cardinalityHasAdditionalKeys | cardinalityNeedUpdateDict
)

type keyType byte

const (
	keyUInt8  keyType = 0
	keyUInt16 keyType = 1
	keyUInt32 keyType = 2
	keyUInt64 keyType = 3
)

type LowCardinality[T comparable] struct {
	Values ColumnOf[T]
	dict   []T   // unique keys
	keys   []int // key indices
}

func NewLowCardinality[T comparable](col ColumnOf[T]) *LowCardinality[T] {
	return &LowCardinality[T]{Values: col}
}

func (c *LowCardinality[T]) Name() string { return c.Values.Name() }

func (c *LowCardinality[T]) Type() proto.ColumnType {
	return proto.ColumnTypeLowCardinality.Sub(c.Values.Type())
}

func (c *LowCardinality[T]) Len() int { return c.Values.Len() }

func (c *LowCardinality[T]) EncodeState(b *proto.Buffer) {
	b.PutInt64(int64(keySerializationVersion))
}

func (c *LowCardinality[T]) DecodeState(r *proto.Reader) error {
	v, err := r.Int64()
	if err != nil {
		return fmt.Errorf("low cardinality version: %w", err)
	}
	if v != int64(keySerializationVersion) {
		return fmt.Errorf("low cardinality version: got %d, want %d", v, keySerializationVersion)
	}
	return nil
}

func (c *LowCardinality[T]) DecodeColumn(r *proto.Reader, rows int) error {
	if rows == 0 {
		return nil
	}

	meta, err := r.Int64()
	if err != nil {
		return fmt.Errorf("low cardinality meta: %w", err)
	}
	if (meta & cardinalityNeedGlobalDict) != 0 {
		return fmt.Errorf("low cardinality global dictionary not supported")
	}
	if (meta & cardinalityHasAdditionalKeys) == 0 {
		return fmt.Errorf("low cardinality missing additional keys bit")
	}

	kt := keyType(meta & cardinalityKeyMask)

	indexRows, err := r.Int64()
	if err != nil {
		return fmt.Errorf("low cardinality index rows: %w", err)
	}
	if indexRows > 100_000_000 || indexRows < 0 {
		return fmt.Errorf("low cardinality index rows %d out of range", indexRows)
	}

	// Decode dictionary keys
	c.dict = append(c.dict[:0], make([]T, indexRows)...)
	if err := c.decodeDictKeys(r, int(indexRows)); err != nil {
		return fmt.Errorf("low cardinality dict: %w", err)
	}

	keyRows, err := r.Int64()
	if err != nil {
		return fmt.Errorf("low cardinality key rows: %w", err)
	}
	if keyRows != int64(rows) {
		return fmt.Errorf("low cardinality key rows mismatch: got %d, want %d", keyRows, rows)
	}

	// Decode key indices based on key type
	c.keys = append(c.keys[:0], make([]int, rows)...)
	if err := c.decodeKeyIndices(r, rows, kt); err != nil {
		return fmt.Errorf("low cardinality keys: %w", err)
	}

	// Resolve values from dict + key indices
	for _, idx := range c.keys {
		if idx >= len(c.dict) || idx < 0 {
			return fmt.Errorf("key index %d out of range [0, %d)", idx, len(c.dict))
		}
		c.Values.Append(c.dict[idx])
	}
	return nil
}

func (c *LowCardinality[T]) WriteColumn(w *proto.Writer) {
	w.ChainBuffer(func(b *proto.Buffer) {
		_ = c.EncodeColumn(b)
	})
}

func (c *LowCardinality[T]) EncodeColumn(b *proto.Buffer) error {
	n := c.Values.Len()
	if n == 0 {
		return nil
	}

	// Build dictionary
	dictMap := make(map[T]int)
	for i := 0; i < n; i++ {
		v := c.Values.Row(i)
		if _, ok := dictMap[v]; !ok {
			dictMap[v] = len(dictMap)
		}
	}
	dictSize := len(dictMap)

	// Determine key type
	var kt keyType
	switch {
	case dictSize <= 256:
		kt = keyUInt8
	case dictSize <= 65536:
		kt = keyUInt16
	case int64(dictSize) <= math.MaxUint32:
		kt = keyUInt32
	default:
		kt = keyUInt64
	}

	// Meta
	meta := cardinalityUpdateAll | int64(kt)
	b.PutInt64(meta)

	// Index (dictionary) size
	b.PutInt64(int64(dictSize))

	// Write ordered dict keys
	ordered := make([]T, dictSize)
	for k, idx := range dictMap {
		ordered[idx] = k
	}
	c.encodeDictKeys(b, ordered)

	// Key rows
	b.PutInt64(int64(n))

	// Write key indices
	var indexBuf []byte
	for i := 0; i < n; i++ {
		idx := dictMap[c.Values.Row(i)]
		switch kt {
		case keyUInt8:
			indexBuf = append(indexBuf, byte(idx))
		case keyUInt16:
			var buf [2]byte
			binary.LittleEndian.PutUint16(buf[:], uint16(idx))
			indexBuf = append(indexBuf, buf[:]...)
		case keyUInt32:
			var buf [4]byte
			binary.LittleEndian.PutUint32(buf[:], uint32(idx))
			indexBuf = append(indexBuf, buf[:]...)
		case keyUInt64:
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], uint64(idx))
			indexBuf = append(indexBuf, buf[:]...)
		}
	}
	b.Buf = append(b.Buf, indexBuf...)

	return nil
}

func (c *LowCardinality[T]) encodeDictKeys(b *proto.Buffer, ordered []T) {
	var zero T
	switch any(zero).(type) {
	case string:
		for _, k := range ordered {
			b.PutString(any(k).(string))
		}
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
	}
}

func (c *LowCardinality[T]) decodeDictKeys(r *proto.Reader, n int) error {
	c.dict = c.dict[:n]
	var zero T
	switch any(zero).(type) {
	case string:
		for i := 0; i < n; i++ {
			v, err := r.Str()
			if err != nil {
				return err
			}
			c.dict[i] = any(v).(T)
		}
	case uint8:
		data, err := r.ReadRaw(n)
		if err != nil {
			return err
		}
		src := unsafe.Slice((*uint8)(unsafe.Pointer(&data[0])), n)
		for i, v := range src {
			c.dict[i] = any(v).(T)
		}
	case uint16:
		data, err := r.ReadRaw(n * 2)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			c.dict[i] = any(binary.LittleEndian.Uint16(data[i*2:])).(T)
		}
	case uint32:
		data, err := r.ReadRaw(n * 4)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			c.dict[i] = any(binary.LittleEndian.Uint32(data[i*4:])).(T)
		}
	case uint64:
		data, err := r.ReadRaw(n * 8)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			c.dict[i] = any(binary.LittleEndian.Uint64(data[i*8:])).(T)
		}
	case int8:
		data, err := r.ReadRaw(n)
		if err != nil {
			return err
		}
		for i, v := range data {
			c.dict[i] = any(int8(v)).(T)
		}
	case int16:
		data, err := r.ReadRaw(n * 2)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			c.dict[i] = any(int16(binary.LittleEndian.Uint16(data[i*2:]))).(T)
		}
	case int32:
		data, err := r.ReadRaw(n * 4)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			c.dict[i] = any(int32(binary.LittleEndian.Uint32(data[i*4:]))).(T)
		}
	case int64:
		data, err := r.ReadRaw(n * 8)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			c.dict[i] = any(int64(binary.LittleEndian.Uint64(data[i*8:]))).(T)
		}
	case float32:
		data, err := r.ReadRaw(n * 4)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			c.dict[i] = any(math.Float32frombits(binary.LittleEndian.Uint32(data[i*4:]))).(T)
		}
	case float64:
		data, err := r.ReadRaw(n * 8)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			c.dict[i] = any(math.Float64frombits(binary.LittleEndian.Uint64(data[i*8:]))).(T)
		}
	}
	return nil
}

func (c *LowCardinality[T]) decodeKeyIndices(r *proto.Reader, rows int, kt keyType) error {
	c.keys = c.keys[:rows]
	switch kt {
	case keyUInt8:
		data, err := r.ReadRaw(rows)
		if err != nil {
			return err
		}
		for i, v := range data {
			c.keys[i] = int(v)
		}
	case keyUInt16:
		data, err := r.ReadRaw(rows * 2)
		if err != nil {
			return err
		}
		for i := 0; i < rows; i++ {
			c.keys[i] = int(binary.LittleEndian.Uint16(data[i*2:]))
		}
	case keyUInt32:
		data, err := r.ReadRaw(rows * 4)
		if err != nil {
			return err
		}
		for i := 0; i < rows; i++ {
			c.keys[i] = int(binary.LittleEndian.Uint32(data[i*4:]))
		}
	case keyUInt64:
		data, err := r.ReadRaw(rows * 8)
		if err != nil {
			return err
		}
		for i := 0; i < rows; i++ {
			c.keys[i] = int(binary.LittleEndian.Uint64(data[i*8:]))
		}
	}
	return nil
}
