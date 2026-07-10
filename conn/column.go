package conn

import "github.com/ClickHouse/ch-go/proto"

type Column interface {
	Name() string
	Type() proto.ColumnType
	Len() int
	DecodeColumn(r *proto.Reader, rows int) error
	EncodeColumn(b *proto.Buffer) error
	WriteColumn(w *proto.Writer)
}

type StateEncoder interface {
	EncodeState(b *proto.Buffer)
}

type StateDecoder interface {
	DecodeState(r *proto.Reader) error
}
