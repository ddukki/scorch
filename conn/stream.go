package conn

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
)

type SelectStream struct {
	c       *Conn
	ctx     context.Context
	query   string
	cols    []Column
	bound   bool
	done    bool
	closed  bool
	err     error
	release func()

	blockCols int
	blockRows int
}

type InsertStream struct {
	c       *Conn
	ctx     context.Context
	query   string
	cols    []Column
	bound   bool
	closed  bool
	release func()
}

func (c *Conn) SelectStream(ctx context.Context, query string) (*SelectStream, error) {
	if err := c.lock(); err != nil {
		return nil, err
	}

	q := proto.Query{
		Body:        query,
		Stage:       proto.StageComplete,
		Compression: c.cfg.Compression,
		Info:        makeClientInfo(c.server, c.localAddr),
		Settings:    c.cfg.Settings,
	}
	c.writer.ChainBuffer(func(b *proto.Buffer) {
		q.EncodeAware(b, c.server.Revision)
	})
	c.writer.ChainBuffer(func(b *proto.Buffer) {
		proto.ClientCodeData.Encode(b)
		proto.ClientData{}.EncodeAware(b, c.server.Revision)
		block := proto.Block{Info: proto.BlockInfo{BucketNum: 0}}
		block.EncodeAware(b, c.server.Revision)
	})
	if _, err := c.writer.Flush(); err != nil {
		c.unlock()
		return nil, &Error{Kind: KindNetwork, Message: "flush query+blank", Err: err}
	}

	return &SelectStream{
		c:       c,
		ctx:     ctx,
		query:   query,
		release: c.unlock,
	}, nil
}

func (s *SelectStream) Bind(cols ...Column) {
	s.cols = cols
	s.bound = true
}

func (s *SelectStream) Next() bool {
	if s.closed || s.done {
		return false
	}
	if !s.bound {
		s.err = &Error{Kind: KindProtocol, Message: "Bind must be called before Next"}
		s.done = true
		return false
	}

	c := s.c

	for {
		select {
		case <-s.ctx.Done():
			s.cancel()
			s.err = &Error{Kind: KindInternal, Message: "context canceled", Err: s.ctx.Err()}
			s.done = true
			return false
		default:
		}

		code, err := c.reader.UVarInt()
		if err != nil {
			s.err = &Error{Kind: KindNetwork, Message: "read server code", Err: err}
			s.done = true
			return false
		}

		switch proto.ServerCode(code) {
		case proto.ServerCodeData:
			if proto.FeatureTempTables.In(c.server.Revision) {
				if _, err := c.reader.Str(); err != nil {
					s.err = &Error{Kind: KindProtocol, Message: "read temp table name", Err: err}
					s.done = true
					return false
				}
			}
			if proto.FeatureBlockInfo.In(c.server.Revision) {
				var info proto.BlockInfo
				if err := info.Decode(c.reader); err != nil {
					s.err = &Error{Kind: KindProtocol, Message: "decode block info", Err: err}
					s.done = true
					return false
				}
			}

			blockCols, err := c.reader.Int()
			if err != nil {
				s.err = &Error{Kind: KindProtocol, Message: "decode block columns", Err: err}
				s.done = true
				return false
			}
			blockRows, err := c.reader.Int()
			if err != nil {
				s.err = &Error{Kind: KindProtocol, Message: "decode block rows", Err: err}
				s.done = true
				return false
			}

			if blockCols > 1_000_000 || blockCols < 0 {
				s.err = &Error{Kind: KindProtocol, Message: fmt.Sprintf("block columns %d out of range", blockCols)}
				s.done = true
				return false
			}
			if blockRows > 100_000_000 || blockRows < 0 {
				s.err = &Error{Kind: KindProtocol, Message: fmt.Sprintf("block rows %d out of range", blockRows)}
				s.done = true
				return false
			}

			if blockRows == 0 {
				for j := 0; j < blockCols; j++ {
					if _, err := c.reader.Str(); err != nil {
						s.err = &Error{Kind: KindProtocol, Message: fmt.Sprintf("read skip column %d name", j), Err: err}
						s.done = true
						return false
					}
					if _, err := c.reader.Str(); err != nil {
						s.err = &Error{Kind: KindProtocol, Message: fmt.Sprintf("read skip column %d type", j), Err: err}
						s.done = true
						return false
					}
					if proto.FeatureCustomSerialization.In(c.server.Revision) {
						if _, err := c.reader.Bool(); err != nil {
							s.err = &Error{Kind: KindProtocol, Message: fmt.Sprintf("read skip column %d custom serialization", j), Err: err}
							s.done = true
							return false
						}
					}
				}
				continue
			}

			if len(s.cols) != blockCols {
				s.err = &Error{
					Kind:    KindProtocol,
					Message: fmt.Sprintf("column count mismatch: server sent %d, got %d", blockCols, len(s.cols)),
				}
				s.done = true
				return false
			}

			for i, col := range s.cols {
				if _, err := c.reader.Str(); err != nil {
					s.err = &Error{Kind: KindProtocol, Message: fmt.Sprintf("read column %d name", i), Err: err}
					s.done = true
					return false
				}
				if _, err := c.reader.Str(); err != nil {
					s.err = &Error{Kind: KindProtocol, Message: fmt.Sprintf("read column %d type", i), Err: err}
					s.done = true
					return false
				}
				if proto.FeatureCustomSerialization.In(c.server.Revision) {
					if _, err := c.reader.Bool(); err != nil {
						s.err = &Error{Kind: KindProtocol, Message: fmt.Sprintf("read column %d custom serialization", i), Err: err}
						s.done = true
						return false
					}
				}
				if err := col.DecodeColumn(c.reader, blockRows); err != nil {
					s.err = &Error{Kind: KindProtocol, Message: fmt.Sprintf("decode column %d [%s]", i, col.Name()), Err: err}
					s.done = true
					return false
				}
			}

			s.blockCols = blockCols
			s.blockRows += blockRows
			return true

		case proto.ServerCodeEndOfStream:
			s.done = true
			return false

		case proto.ServerCodeException:
			var ex proto.Exception
			if err := ex.DecodeAware(c.reader, proto.Version); err != nil {
				s.err = &Error{Kind: KindProtocol, Message: "decode exception", Err: err}
				s.done = true
				return false
			}
			s.err = &Error{Kind: KindServer, Message: ex.Message, ServerCode: int(ex.Code)}
			s.done = true
			return false

		case proto.ServerCodeProgress:
			var p proto.Progress
			if err := p.DecodeAware(c.reader, c.server.Revision); err != nil {
				s.err = &Error{Kind: KindProtocol, Message: "decode progress", Err: err}
				s.done = true
				return false
			}
			if c.OnProgress != nil {
				c.OnProgress(p)
			}

		case proto.ServerCodeProfile:
			var p proto.Profile
			if err := p.DecodeAware(c.reader, c.server.Revision); err != nil {
				s.err = &Error{Kind: KindProtocol, Message: "decode profile", Err: err}
				s.done = true
				return false
			}
			if c.OnProfile != nil {
				c.OnProfile(p)
			}

		case proto.ServerProfileEvents:
			if err := c.skipBlock(); err != nil {
				s.err = err
				s.done = true
				return false
			}

		case proto.ServerCodeLog:
			if err := c.skipBlock(); err != nil {
				s.err = err
				s.done = true
				return false
			}

		case proto.ServerCodeTotals, proto.ServerCodeExtremes:
			if err := c.skipBlock(); err != nil {
				s.err = err
				s.done = true
				return false
			}

		case proto.ServerCodeTableColumns:
			var tc proto.TableColumns
			if err := tc.DecodeAware(c.reader, int(c.server.Revision)); err != nil {
				s.err = &Error{Kind: KindProtocol, Message: "decode table columns", Err: err}
				s.done = true
				return false
			}
		}
	}
}

func (s *SelectStream) Cancel() {
	if s.closed || s.done {
		return
	}
	s.cancel()
	s.done = true
}

func (s *SelectStream) cancel() {
	c := s.c
	c.sendCancel()
	for {
		code, err := c.reader.UVarInt()
		if err != nil {
			return
		}
		if proto.ServerCode(code) == proto.ServerCodeEndOfStream {
			return
		}
		switch proto.ServerCode(code) {
		case proto.ServerCodeData:
			c.skipBlock()
		case proto.ServerCodeProgress:
			var p proto.Progress
			p.DecodeAware(c.reader, c.server.Revision)
		case proto.ServerCodeProfile:
			var p proto.Profile
			p.DecodeAware(c.reader, c.server.Revision)
		case proto.ServerCodeException:
			var ex proto.Exception
			ex.DecodeAware(c.reader, c.server.Revision)
		case proto.ServerProfileEvents, proto.ServerCodeLog:
			c.skipBlock()
		case proto.ServerCodeTotals, proto.ServerCodeExtremes:
			c.skipBlock()
		case proto.ServerCodeTableColumns:
			var tc proto.TableColumns
			tc.DecodeAware(c.reader, int(c.server.Revision))
		}
	}
}

func (s *SelectStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if !s.done {
		s.cancel()
	}
	if s.release != nil {
		s.release()
	}
	return s.err
}

func (s *SelectStream) SetRelease(fn func()) {
	s.release = fn
}

func (s *SelectStream) Release() {
	if s.release != nil {
		s.release()
	}
}

func (s *SelectStream) Err() error {
	return s.err
}

func (s *SelectStream) BlockRows() int {
	return s.blockRows
}

func (c *Conn) InsertStream(ctx context.Context, query string) (*InsertStream, error) {
	if err := c.lock(); err != nil {
		return nil, err
	}

	q := proto.Query{
		Body:        query,
		Stage:       proto.StageComplete,
		Compression: c.cfg.Compression,
		Info:        makeClientInfo(c.server, c.localAddr),
		Settings:    c.cfg.Settings,
	}

	c.writer.ChainBuffer(func(b *proto.Buffer) {
		q.EncodeAware(b, c.server.Revision)
	})
	c.writer.ChainBuffer(func(b *proto.Buffer) {
		proto.ClientCodeData.Encode(b)
		proto.ClientData{}.EncodeAware(b, c.server.Revision)
		block := proto.Block{
			Info: proto.BlockInfo{BucketNum: 0},
		}
		block.EncodeAware(b, c.server.Revision)
	})
	if _, err := c.writer.Flush(); err != nil {
		c.unlock()
		return nil, &Error{Kind: KindNetwork, Message: "flush query+blank", Err: err}
	}

	if err := c.readColumnInfo(); err != nil {
		c.unlock()
		return nil, err
	}

	return &InsertStream{
		c:       c,
		ctx:     ctx,
		query:   query,
		release: c.unlock,
	}, nil
}

func (s *InsertStream) Bind(cols ...Column) {
	s.cols = cols
	s.bound = true
}

func (s *InsertStream) Append() error {
	if s.closed {
		return &Error{Kind: KindInternal, Message: "stream is closed"}
	}
	if !s.bound {
		return &Error{Kind: KindProtocol, Message: "Bind must be called before Append"}
	}
	if len(s.cols) == 0 || s.cols[0].Len() == 0 {
		return &Error{Kind: KindProtocol, Message: "no data to append"}
	}

	rows := s.cols[0].Len()
	for i := 1; i < len(s.cols); i++ {
		if s.cols[i].Len() != rows {
			return &Error{
				Kind:    KindProtocol,
				Message: fmt.Sprintf("column %d has %d rows, expected %d", i, s.cols[i].Len(), rows),
			}
		}
	}

	w := s.c.writer
	w.ChainBuffer(func(b *proto.Buffer) {
		proto.ClientCodeData.Encode(b)
		proto.ClientData{}.EncodeAware(b, s.c.server.Revision)
	})
	w.ChainBuffer(func(b *proto.Buffer) {
		if proto.FeatureBlockInfo.In(s.c.server.Revision) {
			proto.BlockInfo{BucketNum: -1}.Encode(b)
		}
		b.PutInt(len(s.cols))
		b.PutInt(rows)
	})
	for _, col := range s.cols {
		w.ChainBuffer(func(b *proto.Buffer) {
			b.PutString(col.Name())
			b.PutString(string(col.Type()))
			if proto.FeatureCustomSerialization.In(s.c.server.Revision) {
				b.PutBool(false)
			}
		})
		col.WriteColumn(w)
	}
	if _, err := w.Flush(); err != nil {
		return &Error{Kind: KindNetwork, Message: "flush append block", Err: err}
	}

	return nil
}

func (s *InsertStream) SetRelease(fn func()) {
	s.release = fn
}

func (s *InsertStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true

	w := s.c.writer
	w.ChainBuffer(func(b *proto.Buffer) {
		proto.ClientCodeData.Encode(b)
		proto.ClientData{}.EncodeAware(b, s.c.server.Revision)
		proto.Block{}.EncodeAware(b, s.c.server.Revision)
	})
	if _, err := w.Flush(); err != nil {
		s.releaseFn()
		return &Error{Kind: KindNetwork, Message: "flush end-of-data", Err: err}
	}

	for {
		select {
		case <-s.ctx.Done():
			s.c.sendCancel()
			s.releaseFn()
			return &Error{Kind: KindInternal, Message: "context canceled", Err: s.ctx.Err()}
		default:
		}

		code, err := s.c.reader.UVarInt()
		if err != nil {
			s.releaseFn()
			return &Error{Kind: KindNetwork, Message: "read insert response", Err: err}
		}

		switch proto.ServerCode(code) {
		case proto.ServerCodeEndOfStream:
			s.releaseFn()
			return nil

		case proto.ServerCodeException:
			var ex proto.Exception
			if err := ex.DecodeAware(s.c.reader, s.c.server.Revision); err != nil {
				s.releaseFn()
				return &Error{Kind: KindProtocol, Message: "decode exception", Err: err}
			}
			s.releaseFn()
			return &Error{Kind: KindServer, Message: fmt.Sprintf("%s (code %d)", ex.Message, ex.Code), ServerCode: int(ex.Code)}

		case proto.ServerCodeProgress:
			var p proto.Progress
			if err := p.DecodeAware(s.c.reader, s.c.server.Revision); err != nil {
				s.releaseFn()
				return &Error{Kind: KindProtocol, Message: "decode progress", Err: err}
			}
			if s.c.OnProgress != nil {
				s.c.OnProgress(p)
			}

		case proto.ServerCodeData:
			if err := s.c.skipBlock(); err != nil {
				s.releaseFn()
				return err
			}

		case proto.ServerCodeTableColumns:
			var tc proto.TableColumns
			if err := tc.DecodeAware(s.c.reader, int(s.c.server.Revision)); err != nil {
				s.releaseFn()
				return &Error{Kind: KindProtocol, Message: "decode table columns", Err: err}
			}

		case proto.ServerCodeProfile:
			var p proto.Profile
			if err := p.DecodeAware(s.c.reader, s.c.server.Revision); err != nil {
				s.releaseFn()
				return &Error{Kind: KindProtocol, Message: "decode profile", Err: err}
			}

		case proto.ServerProfileEvents:
			if err := s.c.skipBlock(); err != nil {
				s.releaseFn()
				return err
			}

		case proto.ServerCodeLog:
			if err := s.c.skipBlock(); err != nil {
				s.releaseFn()
				return err
			}

		default:
			s.releaseFn()
			return &Error{Kind: KindProtocol, Message: fmt.Sprintf("unexpected server code %d in insert close", code)}
		}
	}
}

func (s *InsertStream) releaseFn() {
	if s.release != nil {
		s.release()
	}
}
