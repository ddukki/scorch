package conn

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/ddukki/scorch/column"
)

type dumpConn struct {
	net.Conn
	mu sync.Mutex
}

func (d *dumpConn) Write(b []byte) (int, error) {
	d.mu.Lock()
	fmt.Fprintf(os.Stderr, "  >>> WRITE %d bytes:\n%s", len(b), hex.Dump(b))
	d.mu.Unlock()
	return d.Conn.Write(b)
}

func (d *dumpConn) Read(b []byte) (int, error) {
	n, err := d.Conn.Read(b)
	d.mu.Lock()
	if n > 0 {
		fmt.Fprintf(os.Stderr, "  <<< READ %d bytes:\n%s", n, hex.Dump(b[:n]))
	}
	d.mu.Unlock()
	return n, err
}

func TestConnectInsertDebug(t *testing.T) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("CLICKHOUSE_HOST not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	raw, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	dump := &dumpConn{Conn: raw}
	defer func() { _ = dump.Close() }()

	buf := new(proto.Buffer)
	w := proto.NewWriter(dump, buf)
	r := proto.NewReader(dump)

	hello := proto.ClientHello{
		Name:            "scorch",
		Major:           24,
		Minor:           3,
		ProtocolVersion: proto.Version,
		Database:        "default",
		User:            "default",
		Password:        "test",
	}
	w.ChainBuffer(func(b *proto.Buffer) { hello.Encode(b) })
	if _, err := w.Flush(); err != nil {
		t.Fatalf("flush hello: %v", err)
	}

	code, err := r.UVarInt()
	if err != nil {
		t.Fatalf("read hello: %v", err)
	}
	if proto.ServerCode(code) != proto.ServerCodeHello {
		var ex proto.Exception
		if err := ex.DecodeAware(r, proto.Version); err != nil {
			t.Fatalf("decode exception: %v", err)
		}
		t.Fatalf("expected hello: %+v", ex)
	}
	var sh proto.ServerHello
	if err := sh.DecodeAware(r, proto.Version); err != nil {
		t.Fatalf("decode server hello: %v", err)
	}
	t.Logf("Server rev=%d, name=%s", sh.Revision, sh.Name)

	if proto.FeatureAddendum.In(proto.Version) {
		w.ChainBuffer(func(b *proto.Buffer) { b.PutString("") })
		if _, err := w.Flush(); err != nil {
			t.Fatalf("flush addendum: %v", err)
		}
	}

	localAddr := raw.LocalAddr().String()

	// DDL
	for _, q := range []string{
		"DROP TABLE IF EXISTS test_connect_insert_debug",
		"CREATE TABLE IF NOT EXISTS test_connect_insert_debug (id UInt64, name String) ENGINE = Memory",
	} {
		execQueryOnConn(w, r, sh.Revision, localAddr, q, t)
	}

	// Build a *Conn manually so we can use Insert() while still dumping hex
	c := &Conn{
		state:     StateReady,
		conn:      dump,
		cfg:       Config{Password: "test"},
		reader:    r,
		writer:    w,
		server:    sh,
		localAddr: raw.LocalAddr(),
	}
	defer func() { _ = c.Close() }()

	// Now use the scorch Insert
	idCol := column.NewBase[uint64]("id")
	idCol.Append(42)
	nameCol := column.NewStr("name")
	nameCol.Append("hello")

	if err := c.Insert(ctx, "INSERT INTO test_connect_insert_debug (id, name) VALUES", idCol, nameCol); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	t.Log("INSERT SUCCESS!")
}

func TestMinimalInsertDebug(t *testing.T) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("CLICKHOUSE_HOST not set")
	}

	raw, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	dump := &dumpConn{Conn: raw}
	defer func() { _ = dump.Close() }()

	buf := new(proto.Buffer)
	w := proto.NewWriter(dump, buf)
	r := proto.NewReader(dump)

	hello := proto.ClientHello{
		Name:            "scorch",
		Major:           24,
		Minor:           3,
		ProtocolVersion: proto.Version,
		Database:        "default",
		User:            "default",
		Password:        "test",
	}
	w.ChainBuffer(func(b *proto.Buffer) { hello.Encode(b) })
	if _, err := w.Flush(); err != nil {
		t.Fatalf("flush hello: %v", err)
	}

	code, err := r.UVarInt()
	if err != nil {
		t.Fatalf("read hello: %v", err)
	}
	if proto.ServerCode(code) != proto.ServerCodeHello {
		var ex proto.Exception
		if err := ex.DecodeAware(r, proto.Version); err != nil {
			t.Fatalf("decode exception: %v", err)
		}
		t.Fatalf("expected hello: %+v", ex)
	}
	var sh proto.ServerHello
	if err := sh.DecodeAware(r, proto.Version); err != nil {
		t.Fatalf("decode server hello: %v", err)
	}
	t.Logf("Server rev=%d, name=%s", sh.Revision, sh.Name)

	if proto.FeatureAddendum.In(proto.Version) {
		w.ChainBuffer(func(b *proto.Buffer) { b.PutString("") })
		if _, err := w.Flush(); err != nil {
			t.Fatalf("flush addendum: %v", err)
		}
	}

	localAddr := raw.LocalAddr().String()

	// DDL
	for _, q := range []string{
		"DROP TABLE IF EXISTS test_min_debug",
		"CREATE TABLE IF NOT EXISTS test_min_debug (id UInt64, name String) ENGINE = Memory",
	} {
		execQueryOnConn(w, r, sh.Revision, localAddr, q, t)
	}

	ci := proto.ClientInfo{
		ProtocolVersion: sh.Revision,
		Major:           24,
		Minor:           3,
		Interface:       proto.InterfaceTCP,
		Query:           proto.ClientQueryInitial,
		ClientName:      "scorch",
		InitialAddress:  localAddr,
	}

	fmt.Fprintf(os.Stderr, "=== INSERT QUERY + BLANK (query+blank in one flush) ===\n")
	qp := proto.Query{
		Body:  "INSERT INTO test_min_debug (id, name) VALUES",
		Stage: proto.StageComplete,
		Info:  ci,
	}
	w.ChainBuffer(func(b *proto.Buffer) {
		qp.EncodeAware(b, sh.Revision)
	})
	w.ChainBuffer(func(b *proto.Buffer) {
		proto.ClientCodeData.Encode(b)
		proto.ClientData{}.EncodeAware(b, sh.Revision)
		block := proto.Block{Info: proto.BlockInfo{BucketNum: 0}}
		block.EncodeAware(b, sh.Revision)
	})
	if _, err := w.Flush(); err != nil {
		t.Fatalf("flush insert+blank: %v", err)
	}

	// Read ALL server responses until we get column info Data block.
	// At version 54485, the server sends:
	//   ServerCodeTableColumns → ServerCodeData (col info) → maybe more
	gotColInfo := false
readLoop:
	for {
		code, err := r.UVarInt()
		if err != nil {
			t.Fatalf("read code: %v", err)
		}
		switch proto.ServerCode(code) {
		case proto.ServerCodeData:
			if proto.FeatureTempTables.In(sh.Revision) {
				if _, err := r.Str(); err != nil {
					t.Fatalf("read temp table: %v", err)
				}
			}
			if proto.FeatureBlockInfo.In(sh.Revision) {
				var bi proto.BlockInfo
				if err := bi.Decode(r); err != nil {
					t.Fatalf("decode block info: %v", err)
				}
			}
			cols, _ := r.Int()
			rows, _ := r.Int()
			t.Logf("Data block: cols=%d rows=%d", cols, rows)
			for i := 0; i < int(cols); i++ {
				n, _ := r.Str()
				ty, _ := r.Str()
				if proto.FeatureCustomSerialization.In(sh.Revision) {
					if _, err := r.Bool(); err != nil {
						t.Fatalf("read custom serialization: %v", err)
					}
				}
				t.Logf("  col %d: %q %q", i, n, ty)
				_ = n
				_ = ty
			}
			colInfo := cols > 0
			if colInfo {
				gotColInfo = true
			}
			// If rows > 0 this is actual result data, otherwise column info
			if rows == 0 && cols > 0 {
				t.Log("Received column info")
			}

		case proto.ServerCodeEndOfStream:
			t.Fatal("Unexpected EndOfStream")
		case proto.ServerCodeException:
			var ex proto.Exception
			if err := ex.DecodeAware(r, sh.Revision); err != nil {
				t.Fatalf("decode exception: %v", err)
			}
			t.Fatalf("Exception: %s (code %d)", ex.Message, ex.Code)
		case proto.ServerCodeProgress:
			var p proto.Progress
			if err := p.DecodeAware(r, sh.Revision); err != nil {
				t.Fatalf("decode progress: %v", err)
			}
			t.Logf("Progress: %+v", p)
		case proto.ServerCodeTableColumns:
			var tc proto.TableColumns
			if err := tc.DecodeAware(r, sh.Revision); err != nil {
				t.Fatalf("decode table columns: %v", err)
			}
			t.Logf("TableColumns: %q", tc)
		default:
			t.Logf("Server code: %d", code)
		}

		if gotColInfo {
			break readLoop
		}
	}

	// Now send data block + end block
	fmt.Fprintf(os.Stderr, "=== INSERT DATA+BEGIN+END WITH ch-go STYLE ENCODING ===\n")

	// ch-go uses Block.WriteBlock which calls:
	// Block.EncodeBlock(buf, version, input):
	//   1. FeatureBlockInfo → b.Info.Encode(buf)
	//   2. EncodeRawBlock(buf, version, input):
	//      - PutInt(b.Columns)
	//      - PutInt(b.Rows)
	//      - for each col: EncodeStart, Prepare, EncodeState, EncodeColumn

	// Column order: metadata + data per column, interleaved — matching ch-go's
	// WriteBlock/EncodeRawBlock iteration order. The server expects per-column
	// interleaving: col0 meta → col0 data → col1 meta → col1 data.
	w.ChainBuffer(func(b *proto.Buffer) {
		proto.ClientCodeData.Encode(b)
		proto.ClientData{}.EncodeAware(b, sh.Revision)
		bi := proto.BlockInfo{BucketNum: -1}
		bi.Encode(b)
		b.PutInt(2) // 2 cols
		b.PutInt(1) // 1 row
		// col0: id UInt64
		b.PutString("id")
		b.PutString("UInt64")
		if proto.FeatureCustomSerialization.In(sh.Revision) {
			b.PutBool(false)
		}
	})
	// col0 data: uint64(42)
	w.ChainBuffer(func(b *proto.Buffer) {
		b.PutUInt64(42)
	})
	// col1: name String + data interleaved
	w.ChainBuffer(func(b *proto.Buffer) {
		b.PutString("name")
		b.PutString("String")
		if proto.FeatureCustomSerialization.In(sh.Revision) {
			b.PutBool(false)
		}
		b.PutString("hello")
	})

	// End block (blank block - signals end of INSERT data)
	w.ChainBuffer(func(b *proto.Buffer) {
		proto.ClientCodeData.Encode(b)
		proto.ClientData{}.EncodeAware(b, sh.Revision)
		block := proto.Block{Info: proto.BlockInfo{BucketNum: 0}}
		block.EncodeAware(b, sh.Revision)
	})

	if _, err := w.Flush(); err != nil {
		t.Fatalf("flush data+end: %v", err)
	}

	fmt.Fprintf(os.Stderr, "=== READ FINAL RESPONSE ===\n")
	// Read response with goroutine + timeout
	done := make(chan error, 1)
	go func() {
		for {
			code, err := r.UVarInt()
			if err != nil {
				done <- fmt.Errorf("read: %w", err)
				return
			}
			t.Logf("Got server code: %d", code)
			switch proto.ServerCode(code) {
			case proto.ServerCodeEndOfStream:
				done <- nil
				return
			case proto.ServerCodeException:
				var ex proto.Exception
				if err := ex.DecodeAware(r, sh.Revision); err != nil {
					done <- fmt.Errorf("decode exception: %w", err)
					return
				}
				done <- fmt.Errorf("%s (code %d)", ex.Message, ex.Code)
				return
			case proto.ServerCodeProgress:
				var p proto.Progress
				if err := p.DecodeAware(r, sh.Revision); err != nil {
					done <- fmt.Errorf("decode progress: %w", err)
					return
				}
				t.Logf("Progress: %+v", p)
			case proto.ServerCodeData:
				t.Log("Got unexpected Data")
				if proto.FeatureTempTables.In(sh.Revision) {
					if _, err := r.Str(); err != nil {
						done <- fmt.Errorf("read table name: %w", err)
						return
					}
				}
				if proto.FeatureBlockInfo.In(sh.Revision) {
					var bi proto.BlockInfo
					if err := bi.Decode(r); err != nil {
						done <- fmt.Errorf("decode block info: %w", err)
						return
					}
				}
				cols, err := r.Int()
				if err != nil {
					done <- fmt.Errorf("read cols: %w", err)
					return
				}
				rows, err := r.Int()
				if err != nil {
					done <- fmt.Errorf("read rows: %w", err)
					return
				}
				t.Logf("Data: cols=%d rows=%d", cols, rows)
				_ = cols
				_ = rows
			case proto.ServerCodeProfile:
				var p proto.Profile
				if err := p.DecodeAware(r, sh.Revision); err != nil {
					done <- fmt.Errorf("decode profile: %w", err)
					return
				}
				t.Logf("Profile: rows=%d blocks=%d bytes=%d", p.Rows, p.Blocks, p.Bytes)
			case proto.ServerProfileEvents:
				t.Log("Skipping ProfileEvents block")
				if err := skipRawBlock(r, sh.Revision); err != nil {
					done <- fmt.Errorf("skip ProfileEvents: %w", err)
					return
				}
			case proto.ServerCodeLog:
				t.Log("Skipping Log block")
				if err := skipRawBlock(r, sh.Revision); err != nil {
					done <- fmt.Errorf("skip Log: %w", err)
					return
				}
			default:
				t.Logf("Unexpected code: %d", code)
			}
		}
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Response error: %v", err)
		}
		t.Log("INSERT SUCCESS!")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for final response")
	}
}

// TestQueryHexDumpE2E sends a bare SELECT query and dumps the wire bytes,
// to verify Query + ClientInfo encoding matches protocol spec.
func TestQueryHexDumpE2E(t *testing.T) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("CLICKHOUSE_HOST not set")
	}

	raw, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	dump := &dumpConn{Conn: raw}
	defer func() { _ = dump.Close() }()

	buf := new(proto.Buffer)
	w := proto.NewWriter(dump, buf)
	r := proto.NewReader(dump)

	hello := proto.ClientHello{
		Name:            "scorch",
		Major:           24,
		Minor:           3,
		ProtocolVersion: proto.Version,
		Database:        "default",
		User:            "default",
		Password:        "test",
	}
	fmt.Fprintf(os.Stderr, "=== ClientHello ===\n")
	w.ChainBuffer(func(b *proto.Buffer) { hello.Encode(b) })
	if _, err := w.Flush(); err != nil {
		t.Fatalf("flush hello: %v", err)
	}

	code, err := r.UVarInt()
	if err != nil {
		t.Fatalf("read hello: %v", err)
	}
	if proto.ServerCode(code) != proto.ServerCodeHello {
		var ex proto.Exception
		if err := ex.DecodeAware(r, proto.Version); err != nil {
			t.Fatalf("decode exception: %v", err)
		}
		t.Fatalf("expected hello: %+v", ex)
	}
	var sh proto.ServerHello
	if err := sh.DecodeAware(r, proto.Version); err != nil {
		t.Fatalf("decode server hello: %v", err)
	}
	t.Logf("Server rev=%d", sh.Revision)

	if proto.FeatureAddendum.In(proto.Version) {
		w.ChainBuffer(func(b *proto.Buffer) { b.PutString("") })
		if _, err := w.Flush(); err != nil {
			t.Fatalf("flush addendum: %v", err)
		}
	}

	// === Send SELECT 1 query + blank block, dump all bytes ===
	fmt.Fprintf(os.Stderr, "=== SELECT 1 QUERY + BLANK (scorch style) ===\n")
	q := proto.Query{
		Body:        "SELECT 1 AS one",
		Stage:       proto.StageComplete,
		Compression: proto.CompressionDisabled,
		Info:        makeClientInfo(sh, raw.LocalAddr()),
		Settings:    nil,
	}
	w.ChainBuffer(func(b *proto.Buffer) {
		q.EncodeAware(b, sh.Revision)
	})
	w.ChainBuffer(func(b *proto.Buffer) {
		proto.ClientCodeData.Encode(b)
		proto.ClientData{}.EncodeAware(b, sh.Revision)
		block := proto.Block{Info: proto.BlockInfo{BucketNum: 0}}
		block.EncodeAware(b, sh.Revision)
	})
	if _, err := w.Flush(); err != nil {
		t.Fatalf("flush query+blank: %v", err)
	}

	// Read response — expect Progress + Data + EndOfStream
	fmt.Fprintf(os.Stderr, "=== READ RESPONSE ===\n")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout reading response")
		default:
		}
		code, err := r.UVarInt()
		if err != nil {
			t.Fatalf("read server code: %v", err)
		}
		switch proto.ServerCode(code) {
		case proto.ServerCodeData:
			if proto.FeatureTempTables.In(sh.Revision) {
				if _, err := r.Str(); err != nil {
					t.Fatalf("read temp table: %v", err)
				}
			}
			if proto.FeatureBlockInfo.In(sh.Revision) {
				var bi proto.BlockInfo
				if err := bi.Decode(r); err != nil {
					t.Fatalf("decode block info: %v", err)
				}
			}
			cols, _ := r.Int()
			rows, _ := r.Int()
			t.Logf("Data block: cols=%d rows=%d", cols, rows)
			if cols == 0 && rows == 0 {
				continue
			}
			for i := 0; i < int(cols); i++ {
				if _, err := r.Str(); err != nil {
					t.Fatalf("read col name: %v", err)
				}
				if _, err := r.Str(); err != nil {
					t.Fatalf("read col type: %v", err)
				}
				if proto.FeatureCustomSerialization.In(sh.Revision) {
					if _, err := r.Bool(); err != nil {
						t.Fatalf("read custom serialization: %v", err)
					}
				}
			}
		case proto.ServerCodeProgress:
			var p proto.Progress
			if err := p.DecodeAware(r, sh.Revision); err != nil {
				t.Fatalf("decode progress: %v", err)
			}
			t.Logf("Progress: rows=%d bytes=%d", p.Rows, p.Bytes)
		case proto.ServerCodeProfile:
			var p proto.Profile
			if err := p.DecodeAware(r, sh.Revision); err != nil {
				t.Fatalf("decode profile: %v", err)
			}
			t.Logf("Profile: rows=%d blocks=%d bytes=%d", p.Rows, p.Blocks, p.Bytes)
		case proto.ServerProfileEvents:
			t.Log("Skipping ProfileEvents block")
			if err := skipRawBlock(r, sh.Revision); err != nil {
				t.Fatalf("skip raw block: %v", err)
			}
		case proto.ServerCodeException:
			var ex proto.Exception
			if err := ex.DecodeAware(r, sh.Revision); err != nil {
				t.Fatalf("decode exception: %v", err)
			}
			t.Fatalf("Exception: %s (code %d)", ex.Message, ex.Code)
		case proto.ServerCodeEndOfStream:
			t.Log("EndOfStream")
			return
		default:
			t.Logf("Server code: %d", code)
		}
	}
}

func skipRawBlock(r *proto.Reader, rev int) error {
	if proto.FeatureTempTables.In(rev) {
		if _, err := r.Str(); err != nil {
			return err
		}
	}
	var block proto.Block
	return block.DecodeBlock(r, rev, new(proto.Results).Auto())
}

func execQueryOnConn(w *proto.Writer, r *proto.Reader, rev int, addr string, query string, t *testing.T) {
	t.Helper()
	q := proto.Query{
		Body:  query,
		Stage: proto.StageComplete,
		Info: proto.ClientInfo{
			ProtocolVersion: rev,
			Major:           24,
			Minor:           3,
			Interface:       proto.InterfaceTCP,
			Query:           proto.ClientQueryInitial,
			ClientName:      "scorch",
			InitialAddress:  addr,
		},
	}
	w.ChainBuffer(func(b *proto.Buffer) {
		q.EncodeAware(b, rev)
	})
	w.ChainBuffer(func(b *proto.Buffer) {
		proto.ClientCodeData.Encode(b)
		proto.ClientData{}.EncodeAware(b, rev)
		block := proto.Block{Info: proto.BlockInfo{BucketNum: 0}}
		block.EncodeAware(b, rev)
	})
	if _, err := w.Flush(); err != nil {
		t.Fatalf("flush DDL: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for DDL response")
		default:
		}
		code, err := r.UVarInt()
		if err != nil {
			t.Fatalf("read DDL: %v", err)
		}
		switch proto.ServerCode(code) {
		case proto.ServerCodeEndOfStream:
			return
		case proto.ServerCodeException:
			var ex proto.Exception
			if err := ex.DecodeAware(r, rev); err != nil {
				t.Fatalf("decode exception: %v", err)
			}
			t.Fatalf("DDL %q: %s (code %d)", query, ex.Message, ex.Code)
		case proto.ServerCodeProgress:
			var p proto.Progress
			if err := p.DecodeAware(r, rev); err != nil {
				t.Fatalf("decode progress: %v", err)
			}
		case proto.ServerCodeData:
			if proto.FeatureTempTables.In(rev) {
				if _, err := r.Str(); err != nil {
					t.Fatalf("read temp table: %v", err)
				}
			}
			if proto.FeatureBlockInfo.In(rev) {
				var bi proto.BlockInfo
				if err := bi.Decode(r); err != nil {
					t.Fatalf("decode block info: %v", err)
				}
			}
			cols, _ := r.Int()
			rows, _ := r.Int()
			for i := 0; i < int(cols); i++ {
				if _, err := r.Str(); err != nil {
					t.Fatalf("read col name: %v", err)
				}
				if _, err := r.Str(); err != nil {
					t.Fatalf("read col type: %v", err)
				}
				if proto.FeatureCustomSerialization.In(rev) {
					if _, err := r.Bool(); err != nil {
						t.Fatalf("read custom serialization: %v", err)
					}
				}
				if rows > 0 {
					if _, err := r.Read(make([]byte, 1024)); err != nil {
						t.Fatalf("read col data: %v", err)
					}
				}
			}
		case proto.ServerCodeTableColumns:
			var tc proto.TableColumns
			if err := tc.DecodeAware(r, rev); err != nil {
				t.Fatalf("decode table columns: %v", err)
			}
		}
	}
}
