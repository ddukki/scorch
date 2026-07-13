package pool

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"io"
	"log"

	"github.com/ddukki/scorch/column"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ddukki/scorch/conn"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}

	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:26.6",
		ExposedPorts: []string{"9000/tcp"},
		WaitingFor:   wait.ForListeningPort("9000/tcp"),
		Env: map[string]string{
			"CLICKHOUSE_PASSWORD": "test",
		},
	}
	ch, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "skip e2e tests: %v\n", err)
		os.Exit(m.Run())
	}
	defer func() {
		if err := ch.Terminate(ctx); err != nil {
			log.Printf("terminate container: %v", err)
		}
	}()

	if code, r, err := ch.Exec(ctx, []string{"clickhouse-server", "--version"}); err == nil && code == 0 {
		b, _ := io.ReadAll(r)
		log.Printf("ClickHouse: %s", string(b))
	}

	addr, err := ch.Endpoint(ctx, "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "skip e2e tests: %v\n", err)
		os.Exit(m.Run())
	}
	if err := os.Setenv("CLICKHOUSE_HOST", addr); err != nil {
		fmt.Fprintf(os.Stderr, "setenv: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func connectPoolE2E(t *testing.T) *Pool {
	t.Helper()
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("CLICKHOUSE_HOST not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := New(ctx, Config{
		Config:   conn.Config{Addr: host, Password: "test"},
		MaxConns: 5,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

func TestNewPoolE2E(t *testing.T) {
	p := connectPoolE2E(t)
	defer p.Close()

	st := p.State()
	if st.Total == 0 && st.Idle == 0 {
		t.Log("pool created, no active connections yet")
	}
}

func TestAcquireReleaseE2E(t *testing.T) {
	p := connectPoolE2E(t)
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pc, err := p.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if pc.State() != conn.StateReady {
		t.Fatalf("state after acquire: got %d, want %d", pc.State(), conn.StateReady)
	}
	pc.Release()

	st := p.State()
	if st.Active != 0 {
		t.Fatalf("expected 0 active after release, got %d", st.Active)
	}
}

func TestAcquireCloseE2E(t *testing.T) {
	p := connectPoolE2E(t)
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pc, err := p.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	pc.Close() // destroy, not release

	// puddle Destroy() is async (go func), so we must wait for it.
	require.Eventually(t, func() bool {
		return p.State().Active == 0
	}, 3*time.Second, 10*time.Millisecond)
}

func TestPoolExecE2E(t *testing.T) {
	p := connectPoolE2E(t)
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := p.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatalf("Exec: %v", err)
	}
}

func TestPoolSelectInsertE2E(t *testing.T) {
	p := connectPoolE2E(t)
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := p.Exec(ctx, "DROP TABLE IF EXISTS pool_test_select_insert"); err != nil {
		t.Fatalf("Exec DROP: %v", err)
	}
	if err := p.Exec(ctx, "CREATE TABLE pool_test_select_insert (id UInt64, name String) ENGINE = Memory"); err != nil {
		t.Fatalf("Exec CREATE: %v", err)
	}
	defer func() {
		if err := p.Exec(ctx, "DROP TABLE IF EXISTS pool_test_select_insert"); err != nil {
			t.Logf("drop table: %v", err)
		}
	}()

	idCol := column.NewBase[uint64]("id")
	idCol.Append(1)
	idCol.Append(2)

	nameCol := column.NewStr("name")
	nameCol.Append("a")
	nameCol.Append("b")

	if err := p.Insert(ctx, "INSERT INTO pool_test_select_insert (id, name) VALUES", idCol, nameCol); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	outID := column.NewBase[uint64]("id")
	outName := column.NewStr("name")

	n, err := p.Select(ctx, "SELECT id, name FROM pool_test_select_insert ORDER BY id", outID, outName)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if n != 2 {
		t.Fatalf("row count: got %d, want 2", n)
	}
	if outID.Row(0) != 1 || outName.Row(0) != "a" {
		t.Fatalf("row 0: got id=%d name=%s", outID.Row(0), outName.Row(0))
	}
}

func TestPoolConcurrentE2E(t *testing.T) {
	p := connectPoolE2E(t)
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	errCh := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func() {
			pc, err := p.Acquire(ctx)
			if err != nil {
				errCh <- err
				return
			}
			defer pc.Release()
			time.Sleep(10 * time.Millisecond)
			errCh <- nil
		}()
	}

	for i := 0; i < 10; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent acquire: %v", err)
		}
	}
}

func TestPoolCloseIdempotentE2E(t *testing.T) {
	p := connectPoolE2E(t)

	p.Close()
	p.Close() // second close should not panic
}

func TestPoolClosedAcquireE2E(t *testing.T) {
	p := connectPoolE2E(t)
	p.Close()

	ctx := context.Background()
	_, err := p.Acquire(ctx)
	if err == nil {
		t.Fatal("expected error acquiring from closed pool")
	}
}

func TestPoolAddrStateE2E(t *testing.T) {
	p := connectPoolE2E(t)
	defer p.Close()

	states := p.AddrStates()
	if len(states) == 0 {
		t.Fatal("expected at least one address state")
	}
	if states[0].Addr == "" {
		t.Fatal("expected non-empty address")
	}
}
