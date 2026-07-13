package conn

import "github.com/ClickHouse/ch-go/proto"

// Column is a bound target for a SELECT result column.
type Column interface {
	Name() string
	Type() proto.ColumnType
	Len() int
	DecodeColumn(r *proto.Reader, rows int) error
	EncodeColumn(b *proto.Buffer) error
	WriteColumn(w *proto.Writer)
}

// StateEncoder is implemented by columns that write per-block state before column data.
type StateEncoder interface {
	EncodeState(b *proto.Buffer)
}

// StateDecoder is implemented by columns that read per-block state before column data.
type StateDecoder interface {
	DecodeState(r *proto.Reader) error
}
