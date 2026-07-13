package conn

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
)

// Exec executes a DDL/DML query that returns no result rows.
func (c *Conn) Exec(ctx context.Context, query string) error {
	if err := c.lock(); err != nil {
		return err
	}
	defer c.unlock()

	q := proto.Query{
		Body:        query,
		Stage:       proto.StageComplete,
		Compression: c.cfg.Compression,
		Info:        makeClientInfo(c.server, c.localAddr),
		Settings:    c.cfg.Settings,
	}
	if err := c.writeQuery(q); err != nil {
		return &Error{Kind: KindNetwork, Message: "flush query+blank", Err: err}
	}

	for {
		select {
		case <-ctx.Done():
			c.sendCancel()
			return &Error{Kind: KindInternal, Message: "context canceled", Err: ctx.Err()}
		default:
		}

		code, err := c.reader.UVarInt()
		if err != nil {
			return &Error{Kind: KindNetwork, Message: "read server code", Err: err}
		}

		switch proto.ServerCode(code) {
		case proto.ServerCodeData, proto.ServerCodeTotals, proto.ServerCodeExtremes:
			if err := c.skipBlock(proto.ServerCodeData); err != nil {
				return err
			}
		case proto.ServerCodeTableColumns:
			var tc proto.TableColumns
			if err := tc.DecodeAware(c.reader, int(c.server.Revision)); err != nil {
				return &Error{Kind: KindProtocol, Message: "decode table columns", Err: err}
			}
		case proto.ServerCodeEndOfStream:
			return nil
		case proto.ServerCodeException:
			var ex proto.Exception
			if err := ex.DecodeAware(c.reader, proto.Version); err != nil {
				return &Error{Kind: KindProtocol, Message: "decode exception", Err: err}
			}
			return &Error{Kind: KindServer, Message: ex.Message, ServerCode: int(ex.Code)}
		case proto.ServerCodeProgress:
			var p proto.Progress
			if err := p.DecodeAware(c.reader, c.server.Revision); err != nil {
				return &Error{Kind: KindProtocol, Message: "decode progress", Err: err}
			}
			if c.OnProgress != nil {
				c.OnProgress(p)
			}
		case proto.ServerCodeProfile:
			var p proto.Profile
			if err := p.DecodeAware(c.reader, c.server.Revision); err != nil {
				return &Error{Kind: KindProtocol, Message: "decode profile", Err: err}
			}
			if c.OnProfile != nil {
				c.OnProfile(p)
			}
		case proto.ServerProfileEvents:
			if err := c.skipBlock(proto.ServerProfileEvents); err != nil {
				return err
			}
		case proto.ServerCodeLog:
			if err := c.skipBlock(proto.ServerCodeLog); err != nil {
				return err
			}
		default:
		}
	}
}

// Ping sends a ping to verify the connection is alive.
func (c *Conn) Ping(ctx context.Context) error {
	if err := c.lock(); err != nil {
		return err
	}
	defer c.unlock()

	c.writer.ChainBuffer(func(b *proto.Buffer) {
		b.PutUVarInt(uint64(proto.ClientCodePing))
	})
	if _, err := c.writer.Flush(); err != nil {
		return &Error{Kind: KindNetwork, Message: "flush ping", Err: err}
	}

	code, err := c.reader.UVarInt()
	if err != nil {
		return &Error{Kind: KindNetwork, Message: "read pong", Err: err}
	}
	if proto.ServerCode(code) != proto.ServerCodePong {
		return &Error{Kind: KindProtocol, Message: "unexpected ping response"}
	}
	return nil
}

// readColumnInfo reads the column info sent by the server in response to
// an INSERT query (with blank block sent after the query). It skips all
// intermediate packets (Progress, Profile, etc.) until column info or
// end-of-stream is received.
func (c *Conn) readColumnInfo() error {
	for {
		code, err := c.reader.UVarInt()
		if err != nil {
			return &Error{Kind: KindNetwork, Message: "read server code", Err: err}
		}

		switch proto.ServerCode(code) {
		case proto.ServerCodeData:
			// Read the header block (column metadata, rows=0).
			// Must read manually — skipBlock uses Results.Auto() which also
			// reads column name/type/CS, causing double-read with DecodeBlock.
			if err := c.readColumnInfoBlock(); err != nil {
				return err
			}
			return nil

		case proto.ServerCodeException:
			var ex proto.Exception
			if err := ex.DecodeAware(c.reader, c.server.Revision); err != nil {
				return &Error{Kind: KindProtocol, Message: "decode exception", Err: err}
			}
			return &Error{Kind: KindServer, Message: ex.Message, ServerCode: int(ex.Code)}

		case proto.ServerCodeEndOfStream:
			return &Error{Kind: KindProtocol, Message: "unexpected end of stream waiting for column info"}

		case proto.ServerCodeProgress:
			var p proto.Progress
			if err := p.DecodeAware(c.reader, c.server.Revision); err != nil {
				return &Error{Kind: KindProtocol, Message: "decode progress", Err: err}
			}
			if c.OnProgress != nil {
				c.OnProgress(p)
			}

		case proto.ServerCodeTableColumns:
			var tc proto.TableColumns
			if err := tc.DecodeAware(c.reader, int(c.server.Revision)); err != nil {
				return &Error{Kind: KindProtocol, Message: "decode table columns", Err: err}
			}

		case proto.ServerCodeProfile:
			var p proto.Profile
			if err := p.DecodeAware(c.reader, c.server.Revision); err != nil {
				return &Error{Kind: KindProtocol, Message: "decode profile", Err: err}
			}
			if c.OnProfile != nil {
				c.OnProfile(p)
			}

		case proto.ServerProfileEvents:
			if err := c.skipBlock(proto.ServerProfileEvents); err != nil {
				return &Error{Kind: KindProtocol, Message: "skip profile events", Err: err}
			}

		case proto.ServerCodeLog:
			if err := c.skipBlock(proto.ServerCodeLog); err != nil {
				return &Error{Kind: KindProtocol, Message: "skip log", Err: err}
			}

		default:
		}
	}
}

// readColumnInfoBlock reads the column-info block sent as the initial
// response to an INSERT query. This is a Data block with rows=0, just
// column metadata (name + type + custom serialization). Must not use
// skipBlock here because Block.DecodeBlock + Results.Auto() double-reads
// the column name/type/CS from the wire.
func (c *Conn) readColumnInfoBlock() error {
	// Server always writes writeStringBinary("") before every data block.
	if _, err := c.reader.StrRaw(); err != nil {
		return &Error{Kind: KindProtocol, Message: "read table name", Err: err}
	}
	if c.cfg.Compression == CompressionEnabled {
		c.reader.EnableCompression()
		defer c.reader.DisableCompression()
	}
	var bi proto.BlockInfo
	if proto.FeatureBlockInfo.In(c.server.Revision) {
		if err := bi.Decode(c.reader); err != nil {
			return &Error{Kind: KindProtocol, Message: "read column info block info", Err: err}
		}
	}
	cols, err := c.reader.Int()
	if err != nil {
		return &Error{Kind: KindNetwork, Message: "read column count", Err: err}
	}
	if cols > 1_000_000 || cols < 0 {
		return &Error{Kind: KindProtocol, Message: fmt.Sprintf("column count %d out of range", cols)}
	}
	rows, err := c.reader.Int()
	if err != nil {
		return &Error{Kind: KindNetwork, Message: "read row count", Err: err}
	}
	if rows != 0 {
		return &Error{Kind: KindProtocol, Message: "expected rows=0 for column info block"}
	}
	for i := 0; i < int(cols); i++ {
		if _, err := c.reader.StrRaw(); err != nil {
			return &Error{Kind: KindProtocol, Message: "read column name", Err: err}
		}
		if _, err := c.reader.StrRaw(); err != nil {
			return &Error{Kind: KindProtocol, Message: "read column type", Err: err}
		}
		if proto.FeatureCustomSerialization.In(c.server.Revision) {
			if _, err := c.reader.Bool(); err != nil {
				return &Error{Kind: KindProtocol, Message: "read custom serialization", Err: err}
			}
		}
	}
	return nil
}

func (c *Conn) sendCancel() {
	c.writer.ChainBuffer(func(b *proto.Buffer) {
		b.PutUVarInt(uint64(proto.ClientCodeCancel))
	})
	// Best-effort; caller is already on ctx.Done() path.
	_, _ = c.writer.Flush()
}

// skipBlock reads and discards a data block sent by the server.  Server
// always writes writeStringBinary("") before every block (unconditional, not
// gated by FeatureTempTables).  Reads table name first to align the stream,
// then delegates to Block.DecodeBlock with cached proto.Results for reuse.
func (c *Conn) skipBlock(code proto.ServerCode) error {
	if _, err := c.reader.StrRaw(); err != nil {
		return &Error{Kind: KindProtocol, Message: "skip table name", Err: err}
	}
	if c.cfg.Compression == CompressionEnabled {
		c.reader.EnableCompression()
		defer c.reader.DisableCompression()
	}
	if c.skipResultsCode != code {
		c.skipResults = nil
		c.skipResultsCode = code
	}
	var block proto.Block
	if err := block.DecodeBlock(c.reader, c.server.Revision, c.skipResults.Auto()); err != nil {
		return &Error{Kind: KindProtocol, Message: "skip block", Err: err}
	}
	return nil
}
