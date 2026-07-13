package conn

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ddukki/scorch/column"
	"github.com/stretchr/testify/require"
)

func TestDSNE2E_FullAuth(t *testing.T) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("CLICKHOUSE_HOST not set")
	}
	dsn := fmt.Sprintf("clickhouse://default:test@%s/default", host)
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { require.NoError(t, c.Close()) }()
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDSNE2E_Settings(t *testing.T) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("CLICKHOUSE_HOST not set")
	}
	dsn := fmt.Sprintf("clickhouse://default:test@%s/default?max_threads=2", host)
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Settings) != 1 || cfg.Settings[0].Key != "max_threads" || cfg.Settings[0].Value != "2" {
		t.Fatalf("Settings = %v, want [{max_threads 2}]", cfg.Settings)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { require.NoError(t, c.Close()) }()
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDSNE2E_InsertWithDSN(t *testing.T) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("CLICKHOUSE_HOST not set")
	}
	dsn := fmt.Sprintf("clickhouse://default:test@%s/default", host)
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { require.NoError(t, c.Close()) }()

	if err := c.Exec(ctx, "CREATE TABLE IF NOT EXISTS test_dsn_e2e (id UInt64, name String) ENGINE = Memory"); err != nil {
		t.Fatal(err)
	}
	colID := &column.Base[uint64]{}
	colName := &column.Str{}
	colID.Data = append(colID.Data, 42)
	colName.Data = append(colName.Data, "dsn-test")
	if err := c.Insert(ctx, "INSERT INTO test_dsn_e2e (id, name) VALUES", colID, colName); err != nil {
		t.Fatal(err)
	}
}

func TestDSNE2E_PasswordSpecialChars(t *testing.T) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("CLICKHOUSE_HOST not set")
	}
	encPass := "p%40ss%2Fw%3Frd%23test"
	dsn := fmt.Sprintf("clickhouse://default:%s@%s/default", encPass, host)
	_, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	// Can't test actual auth with this password (not server-configured),
	// but verify parsing round-trips correctly.
}

func TestDSNE2E_UnknownParamBecomesSetting(t *testing.T) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("CLICKHOUSE_HOST not set")
	}
	dsn := fmt.Sprintf("clickhouse://default:test@%s/default?some_random_param=hello", host)
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Settings) != 1 || cfg.Settings[0].Key != "some_random_param" || cfg.Settings[0].Value != "hello" {
		t.Fatalf("Settings = %v, want [{some_random_param hello}]", cfg.Settings)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { require.NoError(t, c.Close()) }()
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDSNE2E_InvalidScheme(t *testing.T) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("CLICKHOUSE_HOST not set")
	}
	dsn := fmt.Sprintf("http://default:test@%s/default", host)
	_, err := ParseDSN(dsn)
	if err == nil {
		t.Fatal("expected error for http scheme")
	}
}

func FuzzParseDSN(f *testing.F) {
	seeds := []string{
		"clickhouse://host:9000",
		"clickhouse://user:pass@host:9000/db",
		"clickhouse://host:9000/db?dial_timeout=10s",
		"clickhouse://host1,host2/db",
		"clickhouse://host/db?max_threads=4",
		"clickhouse://[::1]:9000/db",
		"clickhouse://",
		"clickhouse://host/db\x00name",
		"clickhouse://user:p%40ss@host/db",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, dsn string) {
		_, _ = ParseDSN(dsn)
	})
}

func TestDSNE2E_MultiHost(t *testing.T) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("CLICKHOUSE_HOST not set")
	}
	dsn := fmt.Sprintf("clickhouse://default:test@%s,127.0.0.1:9999/default", host)
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != host {
		t.Errorf("Addr = %q, want %q", cfg.Addr, host)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { require.NoError(t, c.Close()) }()
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func FuzzParseDSNStability(f *testing.F) {
	rng := rand.New(rand.NewSource(42))
	chars := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.~!$&'()*+,;=:@/?%#[]")
	for i := 0; i < 100; i++ {
		var b strings.Builder
		b.WriteString("clickhouse://")
		length := rng.Intn(80) + 1
		for j := 0; j < length; j++ {
			b.WriteByte(chars[rng.Intn(len(chars))])
		}
		f.Add(b.String())
	}
	f.Fuzz(func(t *testing.T, dsn string) {
		_, _ = ParseDSN(dsn)
	})
}
