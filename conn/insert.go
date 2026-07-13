package conn

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
)

// Insert executes an INSERT query with the given columns.
func (c *Conn) Insert(ctx context.Context, query string, cols ...Column) error {
	if err := c.lock(); err != nil {
		return err
	}
	defer c.unlock()

	if len(cols) == 0 || cols[0].Len() == 0 {
		return &Error{Kind: KindProtocol, Message: "no data to insert"}
	}

	rows := cols[0].Len()
	for i := 1; i < len(cols); i++ {
		if cols[i].Len() != rows {
			return &Error{
				Kind:    KindProtocol,
				Message: fmt.Sprintf("column %d has %d rows, expected %d", i, cols[i].Len(), rows),
			}
		}
	}

	q := proto.Query{
		Body:        query,
		Stage:       proto.StageComplete,
		Compression: c.cfg.Compression,
		Info:        makeClientInfo(c.server, c.localAddr),
		Settings:    c.cfg.Settings,
	}

	// Step 1: Send query + blank block together (matching ch-go's sendQuery pattern).
	// The blank block signals end-of-external-data to the server, which then
	// responds with column info for INSERT queries.
	if err := c.writeQuery(q); err != nil {
		return &Error{Kind: KindNetwork, Message: "flush query+blank", Err: err}
	}

	// Step 2: Read column info from server before sending data.
	// The server sends ServerCodeData with column definitions for INSERT.
	if err := c.readColumnInfo(); err != nil {
		return err
	}

	// Step 3-4: Send data block + blank block — replicate ch-go's WriteBlock
	// segmentation pattern using ChainBuffer for metadata and ChainWrite per
	// column to force buffer cuts (producing multi-segment TCP write).
	w := c.writer
	// Match ch-go WriteBlock pattern exactly:
	// 1. ClientCodeData + ClientData header
	w.ChainBuffer(func(b *proto.Buffer) {
		proto.ClientCodeData.Encode(b)
		proto.ClientData{}.EncodeAware(b, c.server.Revision)
	})
	// 2. BlockInfo + columns + rows (WriteBlock's vec[1])
	w.ChainBuffer(func(b *proto.Buffer) {
		if proto.FeatureBlockInfo.In(c.server.Revision) {
			proto.BlockInfo{BucketNum: -1}.Encode(b)
		}
		b.PutInt(len(cols))
		b.PutInt(rows)
	})
	// 3. Per-column: EncodeStart (name + type + CS flag) + WriteColumn
	for _, col := range cols {
		w.ChainBuffer(func(b *proto.Buffer) {
			b.PutString(col.Name())
			b.PutString(string(col.Type()))
			if proto.FeatureCustomSerialization.In(c.server.Revision) {
				b.PutBool(false)
			}
		})
		if s, ok := col.(StateEncoder); ok {
			w.ChainBuffer(s.EncodeState)
		}
		col.WriteColumn(w)
	}
	// 4. Blank block
	w.ChainBuffer(func(b *proto.Buffer) {
		proto.ClientCodeData.Encode(b)
		proto.ClientData{}.EncodeAware(b, c.server.Revision)
		proto.Block{}.EncodeAware(b, c.server.Revision)
	})
	if _, err := w.Flush(); err != nil {
		return &Error{Kind: KindNetwork, Message: "flush data+blank", Err: err}
	}

	// Step 5: Read response (EndOfStream or Exception).
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
		case proto.ServerCodeEndOfStream:
			return nil

		case proto.ServerCodeException:
			var ex proto.Exception
			if err := ex.DecodeAware(c.reader, c.server.Revision); err != nil {
				return &Error{Kind: KindProtocol, Message: "decode exception", Err: err}
			}
			return &Error{Kind: KindServer, Message: fmt.Sprintf("%s (server code %d, exception code %d)", ex.Message, code, ex.Code)}

		case proto.ServerCodeProgress:
			var p proto.Progress
			if err := p.DecodeAware(c.reader, c.server.Revision); err != nil {
				return &Error{Kind: KindProtocol, Message: "decode progress", Err: err}
			}
			if c.OnProgress != nil {
				c.OnProgress(p)
			}

		case proto.ServerCodeData:
			if err := c.skipBlock(proto.ServerCodeData); err != nil {
				return err
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

		case proto.ServerProfileEvents:
			if err := c.skipBlock(proto.ServerProfileEvents); err != nil {
				return err
			}

		case proto.ServerCodeLog:
			if err := c.skipBlock(proto.ServerCodeLog); err != nil {
				return err
			}

		default:
			return &Error{Kind: KindProtocol, Message: fmt.Sprintf("unexpected server code %d in insert response", code)}
		}
	}
}
