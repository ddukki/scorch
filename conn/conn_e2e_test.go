package conn

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"io"
	"log"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:26.6",
		ExposedPorts: []string{"9000/tcp"},
		WaitingFor:   wait.ForLog("Ready for connections"),
	}
	ch, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "skip e2e tests: %v\n", err)
		os.Exit(m.Run())
	}
	defer ch.Terminate(ctx)

	// Log ClickHouse version for test output traceability.
	if code, r, err := ch.Exec(ctx, []string{"clickhouse-server", "--version"}); err == nil && code == 0 {
		b, _ := io.ReadAll(r)
		log.Printf("ClickHouse: %s", string(b))
	}

	addr, err := ch.Endpoint(ctx, "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "skip e2e tests: %v\n", err)
		os.Exit(m.Run())
	}
	os.Setenv("CLICKHOUSE_HOST", addr)
	os.Exit(m.Run())
}

func connectE2E(t *testing.T) *Conn {
	t.Helper()
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("CLICKHOUSE_HOST not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Connect(ctx, Config{Addr: host})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return c
}

func TestConnectE2E(t *testing.T) {
	c := connectE2E(t)
	defer c.Close()

	if c.State() != StateReady {
		t.Fatalf("state: got %d, want %d", c.State(), StateReady)
	}
}

func TestPingE2E(t *testing.T) {
	c := connectE2E(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestExecDDLE2E(t *testing.T) {
	c := connectE2E(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.Exec(ctx, "CREATE TABLE IF NOT EXISTS test_exec_e2e (id UInt64, name String) ENGINE = Memory"); err != nil {
		t.Fatalf("Exec CREATE: %v", err)
	}

	if err := c.Exec(ctx, "DROP TABLE IF EXISTS test_exec_e2e"); err != nil {
		t.Fatalf("Exec DROP: %v", err)
	}
}

func TestCloseIdempotentE2E(t *testing.T) {
	c := connectE2E(t)
	defer c.Close()

	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestStateTransitionsE2E(t *testing.T) {
	if os.Getenv("CLICKHOUSE_HOST") == "" {
		t.Skip("CLICKHOUSE_HOST not set")
	}

	c, err := Connect(context.Background(), Config{Addr: os.Getenv("CLICKHOUSE_HOST")})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if c.State() != StateReady {
		t.Fatalf("after Connect, state %d, want %d", c.State(), StateReady)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if c.State() != StateClosed {
		t.Fatalf("after Close, state %d, want %d", c.State(), StateClosed)
	}
}

func TestExecContextCancelE2E(t *testing.T) {
	c := connectE2E(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)

	err := c.Exec(ctx, "SELECT sleep(3)")
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}
