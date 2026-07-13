package conn

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
)

// Select executes a SELECT query and reads results into the given columns.
func (c *Conn) Select(ctx context.Context, query string, cols ...Column) (int, error) {
	if err := c.lock(); err != nil {
		return 0, err
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
		return 0, &Error{Kind: KindNetwork, Message: "flush query+blank", Err: err}
	}

	var rows int
	for {
		select {
		case <-ctx.Done():
			c.sendCancel()
			return rows, &Error{Kind: KindInternal, Message: "context canceled", Err: ctx.Err()}
		default:
		}

		code, err := c.reader.UVarInt()
		if err != nil {
			return rows, &Error{Kind: KindNetwork, Message: "read server code", Err: err}
		}

		switch proto.ServerCode(code) {
		case proto.ServerCodeData:
			added, err := c.handleDataBlock(cols)
			if err != nil {
				return rows, err
			}
			rows += added

		case proto.ServerCodeEndOfStream:
			return rows, nil

		case proto.ServerCodeException:
			var ex proto.Exception
			if err := ex.DecodeAware(c.reader, proto.Version); err != nil {
				return rows, &Error{Kind: KindProtocol, Message: "decode exception", Err: err}
			}
			return rows, &Error{Kind: KindServer, Message: ex.Message, ServerCode: int(ex.Code)}

		case proto.ServerCodeProgress:
			var p proto.Progress
			if err := p.DecodeAware(c.reader, c.server.Revision); err != nil {
				return rows, &Error{Kind: KindProtocol, Message: "decode progress", Err: err}
			}
			if c.OnProgress != nil {
				c.OnProgress(p)
			}

		case proto.ServerCodeProfile:
			var p proto.Profile
			if err := p.DecodeAware(c.reader, c.server.Revision); err != nil {
				return rows, &Error{Kind: KindProtocol, Message: "decode profile", Err: err}
			}
			if c.OnProfile != nil {
				c.OnProfile(p)
			}

		case proto.ServerProfileEvents:
			if err := c.skipBlock(proto.ServerProfileEvents); err != nil {
				return rows, err
			}

		case proto.ServerCodeLog:
			if err := c.skipBlock(proto.ServerCodeLog); err != nil {
				return rows, err
			}

		case proto.ServerCodeTotals, proto.ServerCodeExtremes:
			if err := c.skipBlock(proto.ServerCodeTotals); err != nil {
				return rows, err
			}
		default:
		}
	}
}

// handleDataBlock reads one data block from the server response.
// Temp table name is uncompressed; block body is compressed if enabled.
func (c *Conn) handleDataBlock(cols []Column) (int, error) {
	if proto.FeatureTempTables.In(c.server.Revision) {
		if _, err := c.reader.StrRaw(); err != nil {
			return 0, &Error{Kind: KindProtocol, Message: "read temp table name", Err: err}
		}
	}
	if c.cfg.Compression == CompressionEnabled {
		c.reader.EnableCompression()
		defer c.reader.DisableCompression()
	}

	if proto.FeatureBlockInfo.In(c.server.Revision) {
		var info proto.BlockInfo
		if err := info.Decode(c.reader); err != nil {
			return 0, &Error{Kind: KindProtocol, Message: "decode block info", Err: err}
		}
	}

	blockCols, err := c.reader.Int()
	if err != nil {
		return 0, &Error{Kind: KindProtocol, Message: "decode block columns", Err: err}
	}
	blockRows, err := c.reader.Int()
	if err != nil {
		return 0, &Error{Kind: KindProtocol, Message: "decode block rows", Err: err}
	}

	if blockCols > 1_000_000 || blockCols < 0 {
		return 0, &Error{Kind: KindProtocol, Message: fmt.Sprintf("block columns %d out of range", blockCols)}
	}
	if blockRows > 100_000_000 || blockRows < 0 {
		return 0, &Error{Kind: KindProtocol, Message: fmt.Sprintf("block rows %d out of range", blockRows)}
	}

	if blockCols == 0 && blockRows == 0 {
		return 0, nil
	}

	if len(cols) != blockCols {
		return 0, &Error{
			Kind:    KindProtocol,
			Message: fmt.Sprintf("column count mismatch: server sent %d, got %d", blockCols, len(cols)),
		}
	}

	for i, col := range cols {
		if _, err := c.reader.StrRaw(); err != nil {
			return 0, &Error{Kind: KindProtocol, Message: fmt.Sprintf("read column %d name", i), Err: err}
		}
		if _, err := c.reader.StrRaw(); err != nil {
			return 0, &Error{Kind: KindProtocol, Message: fmt.Sprintf("read column %d type", i), Err: err}
		}
		if proto.FeatureCustomSerialization.In(c.server.Revision) {
			if _, err := c.reader.Bool(); err != nil {
				return 0, &Error{Kind: KindProtocol, Message: fmt.Sprintf("read column %d custom serialization", i), Err: err}
			}
		}
		if blockRows > 0 {
			if s, ok := col.(StateDecoder); ok {
				if err := s.DecodeState(c.reader); err != nil {
					return 0, &Error{Kind: KindProtocol, Message: fmt.Sprintf("decode state column %d [%s]", i, col.Name()), Err: err}
				}
			}
			if err := col.DecodeColumn(c.reader, blockRows); err != nil {
				return 0, &Error{Kind: KindProtocol, Message: fmt.Sprintf("decode column %d [%s]", i, col.Name()), Err: err}
			}
		}
	}

	return blockRows, nil
}
