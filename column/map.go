package column

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/shopspring/decimal"
)

// MapEntry is a single key-value pair in a Map column row.
type MapEntry[K, V any] struct {
	Key   K
	Value V
}

type mapElem[T any] interface {
	Of[T]
	Infer(proto.ColumnType) error
}

// MapColumn stores ordered key-value pairs.
type MapColumn[K, V any] struct {
	name    string
	Offsets []uint64
	Keys    Of[K]
	Values  Of[V]
	keyIn   mapElem[K]
	valIn   mapElem[V]
}

// NewMapColumn creates a MapColumn. Keys/Values are nil until Infer is called.
func NewMapColumn[K, V any](name string) *MapColumn[K, V] {
	return &MapColumn[K, V]{name: name}
}

func (c *MapColumn[K, V]) Name() string { return c.name }

func (c *MapColumn[K, V]) Len() int { return len(c.Offsets) }

func (c *MapColumn[K, V]) Reset() {
	c.Offsets = c.Offsets[:0]
	c.Keys.Reset()
	c.Values.Reset()
}

func (c *MapColumn[K, V]) Type() proto.ColumnType {
	return proto.ColumnTypeMap.With(c.Keys.Type().String(), c.Values.Type().String())
}

// Infer sets up Keys and Values columns from a Map(KeyType,ValType) ColumnType.
func (c *MapColumn[K, V]) Infer(t proto.ColumnType) error {
	base := t.Base()
	if base != proto.ColumnTypeMap {
		return fmt.Errorf("map: expected Map, got %q", base)
	}
	keyType, valType, err := parseMapTypes(t)
	if err != nil {
		return fmt.Errorf("map: parse types: %w", err)
	}
	// Create Keys column matching K.
	c.Keys, err = newColumnOf[K](keyType)
	if err != nil {
		return fmt.Errorf("map: keys: %w", err)
	}
	if ie, ok := c.Keys.(mapElem[K]); ok {
		c.keyIn = ie
	} else {
		return fmt.Errorf("map: keys column does not support Infer")
	}
	if err := c.keyIn.Infer(keyType); err != nil {
		return fmt.Errorf("map: infer keys: %w", err)
	}
	// Create Values column matching V.
	c.Values, err = newColumnOf[V](valType)
	if err != nil {
		return fmt.Errorf("map: values: %w", err)
	}
	if ie, ok := c.Values.(mapElem[V]); ok {
		c.valIn = ie
	} else {
		return fmt.Errorf("map: values column does not support Infer")
	}
	if err := c.valIn.Infer(valType); err != nil {
		return fmt.Errorf("map: infer values: %w", err)
	}
	return nil
}

func (c *MapColumn[K, V]) Append(v []MapEntry[K, V]) {
	for _, entry := range v {
		c.Keys.Append(entry.Key)
		c.Values.Append(entry.Value)
	}
	c.Offsets = append(c.Offsets, uint64(c.Keys.Len()))
}

func (c *MapColumn[K, V]) AppendArr(v [][]MapEntry[K, V]) {
	for _, a := range v {
		c.Append(a)
	}
}

func (c *MapColumn[K, V]) Row(i int) []MapEntry[K, V] {
	lo := uint64(0)
	if i > 0 {
		lo = c.Offsets[i-1]
	}
	hi := c.Offsets[i]
	n := int(hi - lo)
	out := make([]MapEntry[K, V], n)
	for j := 0; j < n; j++ {
		out[j] = MapEntry[K, V]{
			Key:   c.Keys.Row(int(lo) + j),
			Value: c.Values.Row(int(lo) + j),
		}
	}
	return out
}

func (c *MapColumn[K, V]) ForEachEntry(i int, fn func(MapEntry[K, V]) error) error {
	lo := uint64(0)
	if i > 0 {
		lo = c.Offsets[i-1]
	}
	hi := c.Offsets[i]
	for j := lo; j < hi; j++ {
		if err := fn(MapEntry[K, V]{
			Key:   c.Keys.Row(int(j)),
			Value: c.Values.Row(int(j)),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (c *MapColumn[K, V]) DecodeColumn(r *proto.Reader, rows int) error {
	c.Offsets = c.Offsets[:0]
	for i := 0; i < rows; i++ {
		v, err := r.UVarInt()
		if err != nil {
			return fmt.Errorf("map: read offset %d: %w", i, err)
		}
		c.Offsets = append(c.Offsets, v)
	}
	total := 0
	if rows > 0 {
		total = int(c.Offsets[rows-1])
	}
	if err := c.Keys.DecodeColumn(r, total); err != nil {
		return fmt.Errorf("map: decode keys: %w", err)
	}
	if err := c.Values.DecodeColumn(r, total); err != nil {
		return fmt.Errorf("map: decode values: %w", err)
	}
	return nil
}

func (c *MapColumn[K, V]) EncodeColumn(b *proto.Buffer) error {
	if err := encodeUVarInts(c.Offsets, b); err != nil {
		return err
	}
	if err := c.Keys.EncodeColumn(b); err != nil {
		return err
	}
	return c.Values.EncodeColumn(b)
}

func (c *MapColumn[K, V]) WriteColumn(w *proto.Writer) {
	writeUVarInts(c.Offsets, w)
	c.Keys.WriteColumn(w)
	c.Values.WriteColumn(w)
}

// parseMapTypes extracts key and value ColumnTypes from a Map ColumnType.
func parseMapTypes(t proto.ColumnType) (key, val proto.ColumnType, err error) {
	// t.Elem() for Map returns the inner parameter string, e.g. "String, UInt64".
	inner := string(t.Elem())
	if inner == "" {
		return "", "", fmt.Errorf("empty Map type parameters")
	}
	keyStr, valStr := splitAtDepth0(inner, ',')
	key = proto.ColumnType(strings.TrimSpace(keyStr))
	val = proto.ColumnType(strings.TrimSpace(valStr))
	if key == "" || val == "" {
		return "", "", fmt.Errorf("invalid Map type: %q", t.String())
	}
	return key, val, nil
}

// splitAtDepth0 splits s on sep at depth 0 (outside parentheses).
func splitAtDepth0(s string, sep rune) (string, string) {
	depth := 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case sep:
			if depth == 0 {
				return s[:i], s[i+1:]
			}
		}
	}
	return s, ""
}

// EntriesToMap converts a slice of MapEntry to a Go map.
// Later entries overwrite earlier ones on duplicate keys.
func EntriesToMap[K comparable, V any](entries []MapEntry[K, V]) map[K]V {
	m := make(map[K]V, len(entries))
	for _, e := range entries {
		m[e.Key] = e.Value
	}
	return m
}

// newColumnOf creates a column of the appropriate type for T, validated against t.
func newColumnOf[T any](t proto.ColumnType) (Of[T], error) {
	baseType := func(expected proto.ColumnType) error {
		if t.Base() != expected {
			return fmt.Errorf("type mismatch: expected %s, got %s", expected, t.Base())
		}
		return nil
	}
	var zero T
	switch any(zero).(type) {
	case uint8:
		if err := baseType(proto.ColumnTypeUInt8); err != nil {
			return nil, err
		}
		return any(NewBaseColumn[uint8]("")).(Of[T]), nil
	case uint16:
		if err := baseType(proto.ColumnTypeUInt16); err != nil {
			return nil, err
		}
		return any(NewBaseColumn[uint16]("")).(Of[T]), nil
	case uint32:
		if err := baseType(proto.ColumnTypeUInt32); err != nil {
			return nil, err
		}
		return any(NewBaseColumn[uint32]("")).(Of[T]), nil
	case uint64:
		if err := baseType(proto.ColumnTypeUInt64); err != nil {
			return nil, err
		}
		return any(NewBaseColumn[uint64]("")).(Of[T]), nil
	case int8:
		if err := baseType(proto.ColumnTypeInt8); err != nil {
			return nil, err
		}
		return any(NewBaseColumn[int8]("")).(Of[T]), nil
	case int16:
		if err := baseType(proto.ColumnTypeInt16); err != nil {
			return nil, err
		}
		return any(NewBaseColumn[int16]("")).(Of[T]), nil
	case int32:
		if err := baseType(proto.ColumnTypeInt32); err != nil {
			return nil, err
		}
		return any(NewBaseColumn[int32]("")).(Of[T]), nil
	case int64:
		if err := baseType(proto.ColumnTypeInt64); err != nil {
			return nil, err
		}
		return any(NewBaseColumn[int64]("")).(Of[T]), nil
	case float32:
		if err := baseType(proto.ColumnTypeFloat32); err != nil {
			return nil, err
		}
		return any(NewBaseColumn[float32]("")).(Of[T]), nil
	case float64:
		if err := baseType(proto.ColumnTypeFloat64); err != nil {
			return nil, err
		}
		return any(NewBaseColumn[float64]("")).(Of[T]), nil
	case string:
		switch t.Base() {
		case proto.ColumnTypeString:
			return any(NewStrColumn("")).(Of[T]), nil
		case proto.ColumnTypeFixedString:
			n := extractFixedStringN(t)
			return any(NewFixedStringColumn("", n)).(Of[T]), nil
		case proto.ColumnTypeEnum8:
			return any(NewEnum8Column("")).(Of[T]), nil
		case proto.ColumnTypeEnum16:
			return any(NewEnum16Column("")).(Of[T]), nil
		}
	case decimal.Decimal:
		switch t.Base() {
		case proto.ColumnTypeDecimal:
			return any(NewDecimal128Column("")).(Of[T]), nil
		case proto.ColumnTypeDecimal32:
			return any(NewDecimal32Column("")).(Of[T]), nil
		case proto.ColumnTypeDecimal64:
			return any(NewDecimal64Column("")).(Of[T]), nil
		case proto.ColumnTypeDecimal128:
			return any(NewDecimal128Column("")).(Of[T]), nil
		case proto.ColumnTypeDecimal256:
			return any(NewDecimal256Column("")).(Of[T]), nil
		}
	case Int128:
		if err := baseType(proto.ColumnTypeInt128); err != nil {
			return nil, err
		}
		return any(NewInt128Column("")).(Of[T]), nil
	case UInt128:
		if err := baseType(proto.ColumnTypeUInt128); err != nil {
			return nil, err
		}
		return any(NewUInt128Column("")).(Of[T]), nil
	case Int256:
		if err := baseType(proto.ColumnTypeInt256); err != nil {
			return nil, err
		}
		return any(NewInt256Column("")).(Of[T]), nil
	case UInt256:
		if err := baseType(proto.ColumnTypeUInt256); err != nil {
			return nil, err
		}
		return any(NewUInt256Column("")).(Of[T]), nil
	case UUID:
		if err := baseType(proto.ColumnTypeUUID); err != nil {
			return nil, err
		}
		return any(NewUUIDColumn("")).(Of[T]), nil
	case time.Time:
		switch t.Base() {
		case proto.ColumnTypeDate:
			return any(NewDateColumn("")).(Of[T]), nil
		case proto.ColumnTypeDate32:
			return any(NewDate32Column("")).(Of[T]), nil
		case proto.ColumnTypeDateTime:
			return any(NewDateTimeColumn("")).(Of[T]), nil
		case proto.ColumnTypeDateTime64:
			return any(NewDateTime64Column("", 0)).(Of[T]), nil
		}
	case net.IP:
		switch t.Base() {
		case proto.ColumnTypeIPv4:
			return any(NewIPv4Column("")).(Of[T]), nil
		case proto.ColumnTypeIPv6:
			return any(NewIPv6Column("")).(Of[T]), nil
		}
	}
	return nil, fmt.Errorf("map: unsupported type %s for Go %T", t.Base(), zero)
}

// checkBase panics if t.Base() does not match expected.
func checkBase(t proto.ColumnType, expected proto.ColumnType) {
	if t.Base() != expected {
		panic(fmt.Sprintf("type mismatch: expected %s, got %s", expected, t.Base()))
	}
}

// extractFixedStringN returns the N from FixedString(N).
func extractFixedStringN(t proto.ColumnType) int {
	s := t.String()
	// Format: "FixedString(<n>)"
	start := strings.IndexByte(s, '(')
	end := strings.IndexByte(s, ')')
	if start < 0 || end < 0 || start >= end {
		return 0
	}
	var n int
	fmt.Sscanf(s[start+1:end], "%d", &n)
	return n
}
