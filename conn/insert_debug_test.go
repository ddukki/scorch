package conn

import (
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

type dumpConn2 struct {
	net.Conn
	mu sync.Mutex
}

func (d *dumpConn2) Write(b []byte) (int, error) {
	d.mu.Lock()
	fmt.Fprintf(os.Stderr, "  >>> WRITE %d bytes:\n%s", len(b), hex.Dump(b))
	d.mu.Unlock()
	return d.Conn.Write(b)
}

func (d *dumpConn2) Read(b []byte) (int, error) {
	n, err := d.Conn.Read(b)
	d.mu.Lock()
	if n > 0 {
		fmt.Fprintf(os.Stderr, "  <<< READ %d bytes:\n%s", n, hex.Dump(b[:n]))
	}
	d.mu.Unlock()
	return n, err
}

func TestInsertChGoStyleE2E(t *testing.T) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("CLICKHOUSE_HOST not set")
	}

	raw, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	dump := &dumpConn2{Conn: raw}
	defer func() {
		if err := dump.Close(); err != nil {
			t.Logf("dump conn close: %v", err)
		}
	}()

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
		_ = ex.DecodeAware(r, proto.Version)
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
	for _, ddl := range []string{
		"DROP TABLE IF EXISTS test_chgo_style",
		"CREATE TABLE IF NOT EXISTS test_chgo_style (id UInt64, name String) ENGINE = Memory",
	} {
		q := proto.Query{
			Body:  ddl,
			Stage: proto.StageComplete,
			Info: proto.ClientInfo{
				ProtocolVersion: proto.Version,
				Major:           24,
				Minor:           3,
				Interface:       proto.InterfaceTCP,
				Query:           proto.ClientQueryInitial,
				ClientName:      "scorch",
				InitialAddress:  localAddr,
			},
		}
		w.ChainBuffer(func(b *proto.Buffer) { q.EncodeAware(b, sh.Revision) })
		w.ChainBuffer(func(b *proto.Buffer) {
			proto.ClientCodeData.Encode(b)
			proto.ClientData{}.EncodeAware(b, sh.Revision)
			proto.Block{}.EncodeAware(b, sh.Revision)
		})
		if _, err := w.Flush(); err != nil {
			t.Fatalf("flush DDL: %v", err)
		}
		for {
			c, err := r.UVarInt()
			if err != nil {
				t.Fatalf("read DDL response: %v", err)
			}
			switch proto.ServerCode(c) {
			case proto.ServerCodeEndOfStream:
				goto nextDDL
			case proto.ServerCodeException:
				var ex proto.Exception
				_ = ex.DecodeAware(r, sh.Revision)
				t.Fatalf("DDL %q: %s (code %d)", ddl, ex.Message, ex.Code)
			case proto.ServerCodeTableColumns:
				var tc proto.TableColumns
				_ = tc.DecodeAware(r, sh.Revision)
			case proto.ServerCodeProgress:
				var p proto.Progress
				_ = p.DecodeAware(r, sh.Revision)
			case proto.ServerCodeData:
				var bi proto.BlockInfo
				_ = bi.Decode(r)
				cols, _ := r.Int()
				_, _ = r.Int()
				for i := 0; i < cols; i++ {
					_, _ = r.Str()
					_, _ = r.Str()
					if proto.FeatureCustomSerialization.In(sh.Revision) {
						_, _ = r.Bool()
					}
				}
			default:
				t.Logf("DDL: server code %d", c)
			}
		}
	nextDDL:
	}

	// INSERT using ch-go's Block.WriteBlock
	fmt.Fprintf(os.Stderr, "=== INSERT (ch-go style) ===\n")
	q := proto.Query{
		Body:  "INSERT INTO test_chgo_style (id, name) VALUES",
		Stage: proto.StageComplete,
		Info: proto.ClientInfo{
			ProtocolVersion: proto.Version,
			Major:           24,
			Minor:           3,
			Interface:       proto.InterfaceTCP,
			Query:           proto.ClientQueryInitial,
			ClientName:      "scorch",
			InitialAddress:  localAddr,
		},
	}

	// Query + blank for external data terminator
	w.ChainBuffer(func(b *proto.Buffer) { q.EncodeAware(b, sh.Revision) })
	w.ChainBuffer(func(b *proto.Buffer) {
		proto.ClientCodeData.Encode(b)
		proto.ClientData{}.EncodeAware(b, sh.Revision)
		proto.Block{}.EncodeAware(b, sh.Revision)
	})
	if _, err := w.Flush(); err != nil {
		t.Fatalf("flush INSERT query: %v", err)
	}

	// Read column info
	fmt.Fprintf(os.Stderr, "=== READ COLUMN INFO ===\n")
readColInfo:
	for {
		c, err := r.UVarInt()
		if err != nil {
			t.Fatalf("read col info: %v", err)
		}
		switch proto.ServerCode(c) {
		case proto.ServerCodeData:
			if proto.FeatureTempTables.In(sh.Revision) {
				_, _ = r.Str()
			}
			var bi proto.BlockInfo
			_ = bi.Decode(r)
			cols, _ := r.Int()
			rows, _ := r.Int()
			t.Logf("Column info: cols=%d rows=%d", cols, rows)
			if cols > 0 && rows == 0 {
				for i := 0; i < int(cols); i++ {
					n, _ := r.Str()
					ty, _ := r.Str()
					if proto.FeatureCustomSerialization.In(sh.Revision) {
						_, _ = r.Bool()
					}
					_ = proto.ColInfo{Name: n, Type: proto.ColumnType(ty)}
				}
				break readColInfo
			}
		case proto.ServerCodeException:
			var ex proto.Exception
			_ = ex.DecodeAware(r, sh.Revision)
			t.Fatalf("Exception: %s (code %d)", ex.Message, ex.Code)
		case proto.ServerCodeEndOfStream:
			t.Fatal("Unexpected EndOfStream")
		case proto.ServerCodeProgress:
			var p proto.Progress
			_ = p.DecodeAware(r, sh.Revision)
		case proto.ServerCodeTableColumns:
			var tc proto.TableColumns
			_ = tc.DecodeAware(r, sh.Revision)
		default:
			t.Logf("Server code: %d", c)
		}
	}

	// Send data block using ch-go's Block.WriteBlock
	fmt.Fprintf(os.Stderr, "=== SEND DATA (ch-go Block.WriteBlock) ===\n")
	var (
		idCol   proto.ColUInt64
		nameCol proto.ColStr
	)
	idCol.Append(42)
	nameCol.Append("hello")

	input := []proto.InputColumn{
		{Name: "id", Data: &idCol},
		{Name: "name", Data: &nameCol},
	}

	// Use ch-go's Block.WriteBlock directly
	w.ChainBuffer(func(b *proto.Buffer) {
		proto.ClientCodeData.Encode(b)
		proto.ClientData{}.EncodeAware(b, sh.Revision)
	})
	block := proto.Block{
		Columns: len(input),
		Rows:    input[0].Data.Rows(),
		Info:    proto.BlockInfo{BucketNum: -1},
	}
	if err := block.WriteBlock(w, sh.Revision, input); err != nil {
		t.Fatalf("write data block: %v", err)
	}

	// Blank block (end of INSERT data) using ch-go style
	w.ChainBuffer(func(b *proto.Buffer) {
		proto.ClientCodeData.Encode(b)
		proto.ClientData{}.EncodeAware(b, sh.Revision)
		proto.Block{}.EncodeAware(b, sh.Revision)
	})

	if _, err := w.Flush(); err != nil {
		t.Fatalf("flush data+blank: %v", err)
	}

	// Read response
	fmt.Fprintf(os.Stderr, "=== READ FINAL RESPONSE ===\n")
	done := make(chan error, 1)
	go func() {
		for {
			c, err := r.UVarInt()
			if err != nil {
				done <- fmt.Errorf("read: %w", err)
				return
			}
			switch proto.ServerCode(c) {
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
			case proto.ServerCodeData:
				t.Log("Got unexpected Data")
				if proto.FeatureTempTables.In(sh.Revision) {
					_, _ = r.Str()
				}
				if proto.FeatureBlockInfo.In(sh.Revision) {
					var bi proto.BlockInfo
					if err := bi.Decode(r); err != nil {
						done <- fmt.Errorf("decode block info: %w", err)
						return
					}
				}
				cols, _ := r.Int()
				rows, _ := r.Int()
				t.Logf("Data: cols=%d rows=%d", cols, rows)
			default:
				t.Logf("Unexpected code: %d", c)
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
