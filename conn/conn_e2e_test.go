package conn

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"io"
	"log"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/ddukki/scorch/column"
)

func TestMain(m *testing.M) {
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
			log.Printf("terminate: %v", err)
		}
	}()

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
	if err := os.Setenv("CLICKHOUSE_HOST", addr); err != nil {
		fmt.Fprintf(os.Stderr, "setenv: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestChGoClientE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("CLICKHOUSE_HOST not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c, err := ch.Dial(ctx, ch.Options{
		Address:  host,
		Password: "test",
	})
	if err != nil {
		t.Fatalf("ch.Dial: %v", err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	}()

	// SELECT 1
	var one proto.ColUInt8
	if err := c.Do(ctx, ch.Query{
		Body: "SELECT 1 AS one",
		Result: proto.Results{
			proto.ResultColumn{Name: "one", Data: &one},
		},
	}); err != nil {
		t.Fatalf("ch.Do SELECT 1: %v", err)
	}
	t.Logf("SELECT 1: %+v", one)

	// DDL
	if err := c.Do(ctx, ch.Query{
		Body: "DROP TABLE IF EXISTS test_ch_go_e2e",
	}); err != nil {
		t.Fatalf("ch.Do DROP: %v", err)
	}
	if err := c.Do(ctx, ch.Query{
		Body: "CREATE TABLE test_ch_go_e2e (id UInt64, name String) ENGINE = Memory",
	}); err != nil {
		t.Fatalf("ch.Do CREATE: %v", err)
	}
	defer func() { _ = c.Do(ctx, ch.Query{Body: "DROP TABLE IF EXISTS test_ch_go_e2e"}) }()

	// INSERT
	var (
		idCol   proto.ColUInt64
		nameCol proto.ColStr
	)
	idCol.Append(1)
	idCol.Append(2)
	nameCol.Append("foo")
	nameCol.Append("bar")

	if err := c.Do(ctx, ch.Query{
		Body: "INSERT INTO test_ch_go_e2e (id, name) VALUES",
		Input: []proto.InputColumn{
			{Name: "id", Data: &idCol},
			{Name: "name", Data: &nameCol},
		},
	}); err != nil {
		t.Fatalf("ch.Do INSERT: %v", err)
	}
	t.Log("INSERT ok")

	// SELECT back
	var (
		outID   proto.ColUInt64
		outName proto.ColStr
	)
	if err := c.Do(ctx, ch.Query{
		Body: "SELECT id, name FROM test_ch_go_e2e ORDER BY id",
		Result: proto.Results{
			proto.ResultColumn{Name: "id", Data: &outID},
			proto.ResultColumn{Name: "name", Data: &outName},
		},
	}); err != nil {
		t.Fatalf("ch.Do SELECT back: %v", err)
	}
	t.Logf("OUT: id=%v name=%v", outID, outName)
}

func connectE2E(t *testing.T) *Conn {
	t.Helper()
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("CLICKHOUSE_HOST not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Connect(ctx, Config{Addr: host, Password: "test"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return c
}

func TestConnectE2E(t *testing.T) {
	c := connectE2E(t)
	defer func() {
		if err := c.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	}()

	if c.State() != StateReady {
		t.Fatalf("state: got %d, want %d", c.State(), StateReady)
	}
}

func TestPingE2E(t *testing.T) {
	c := connectE2E(t)
	defer func() {
		if err := c.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestExecDDLE2E(t *testing.T) {
	c := connectE2E(t)
	defer func() {
		if err := c.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.Exec(ctx, "CREATE TABLE IF NOT EXISTS test_exec_e2e (id UInt64, name String) ENGINE = Memory"); err != nil {
		t.Fatalf("Exec CREATE: %v", err)
	}

	if err := c.Exec(ctx, "DROP TABLE IF EXISTS test_exec_e2e"); err != nil {
		t.Fatalf("Exec DROP: %v", err)
	}
}

func TestSelectOnlyE2E(t *testing.T) {
	c := connectE2E(t)
	defer func() {
		if err := c.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// SELECT 1 — simplest query, one UInt8 column
	one := column.NewBase[uint8]("1")
	n, err := c.Select(ctx, "SELECT 1 AS `1`", one)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if n != 1 {
		t.Fatalf("rows: got %d, want 1", n)
	}
	if one.Row(0) != 1 {
		t.Fatalf("value: got %d, want 1", one.Row(0))
	}
	t.Logf("SELECT 1: %+v", one.Data)
}

func TestCloseIdempotentE2E(t *testing.T) {
	c := connectE2E(t)
	defer func() {
		if err := c.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	}()

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

	c, err := Connect(context.Background(), Config{Addr: os.Getenv("CLICKHOUSE_HOST"), Password: "test"})
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
	defer func() {
		if err := c.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)

	err := c.Exec(ctx, "SELECT sleep(3)")
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}

func TestSelectInsertE2E(t *testing.T) {
	c := connectE2E(t)
	defer func() {
		if err := c.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.Exec(ctx, "DROP TABLE IF EXISTS test_select_insert_e2e"); err != nil {
		t.Fatalf("Exec DROP: %v", err)
	}
	if err := c.Exec(ctx, "CREATE TABLE test_select_insert_e2e (id UInt64, name String) ENGINE = Memory"); err != nil {
		t.Fatalf("Exec CREATE: %v", err)
	}
	defer func() { _ = c.Exec(ctx, "DROP TABLE IF EXISTS test_select_insert_e2e") }()

	idCol := column.NewBase[uint64]("id")
	idCol.Append(1)
	idCol.Append(2)
	idCol.Append(3)

	nameCol := column.NewStr("name")
	nameCol.Append("foo")
	nameCol.Append("bar")
	nameCol.Append("baz")

	if err := c.Insert(ctx, "INSERT INTO test_select_insert_e2e (id, name) VALUES", idCol, nameCol); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	outID := column.NewBase[uint64]("id")
	outName := column.NewStr("name")

	n, err := c.Select(ctx, "SELECT id, name FROM test_select_insert_e2e ORDER BY id", outID, outName)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if n != 3 {
		t.Fatalf("Select row count: got %d, want 3", n)
	}
	if outID.Len() != 3 {
		t.Fatalf("id column len: got %d, want 3", outID.Len())
	}
	if outID.Row(0) != 1 || outID.Row(1) != 2 || outID.Row(2) != 3 {
		t.Fatalf("id values: got %v", outID.Data)
	}
	if outName.Row(0) != "foo" || outName.Row(1) != "bar" || outName.Row(2) != "baz" {
		t.Fatalf("name values: got %v", outName.Data)
	}
}

func TestSelectColumnMismatchE2E(t *testing.T) {
	c := connectE2E(t)
	defer func() {
		if err := c.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := c.Select(ctx, "SELECT 1 AS a, 2 AS b", column.NewBase[uint64]("a"))
	if err == nil {
		t.Fatal("expected column count mismatch error")
	}
	var ce *Error
	if !errors.As(err, &ce) {
		t.Fatalf("expected *Error, got %T", err)
	}
	if ce.Kind != KindProtocol {
		t.Fatalf("error kind: got %d, want %d", ce.Kind, KindProtocol)
	}
}

func TestInsertZeroRowsE2E(t *testing.T) {
	c := connectE2E(t)
	defer func() {
		if err := c.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	idCol := column.NewBase[uint64]("id")
	err := c.Insert(ctx, "INSERT INTO test (id) VALUES", idCol)
	if err == nil {
		t.Fatal("expected error on zero-row insert")
	}
	var ce *Error
	if !errors.As(err, &ce) {
		t.Fatalf("expected *Error, got %T", err)
	}
	if ce.Kind != KindProtocol {
		t.Fatalf("error kind: got %d, want %d", ce.Kind, KindProtocol)
	}
}

func TestSelectInsertLowCardinalityE2E(t *testing.T) {
	c := connectE2E(t)
	defer func() {
		if err := c.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.Exec(ctx, "DROP TABLE IF EXISTS test_lc_e2e"); err != nil {
		t.Fatalf("Exec DROP: %v", err)
	}
	if err := c.Exec(ctx, "CREATE TABLE test_lc_e2e (id UInt64, city LowCardinality(String)) ENGINE = Memory"); err != nil {
		t.Fatalf("Exec CREATE: %v", err)
	}
	defer func() { _ = c.Exec(ctx, "DROP TABLE IF EXISTS test_lc_e2e") }()

	idCol := column.NewBase[uint64]("id")
	idCol.Append(10)
	idCol.Append(20)
	idCol.Append(30)

	cityCol := column.NewLowCardinality(column.NewStr("city"))
	cityCol.Values.Append("NYC")
	cityCol.Values.Append("LA")
	cityCol.Values.Append("NYC")

	if err := c.Insert(ctx, "INSERT INTO test_lc_e2e (id, city) VALUES", idCol, cityCol); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	outID := column.NewBase[uint64]("id")
	outCity := column.NewLowCardinality(column.NewStr("city"))

	n, err := c.Select(ctx, "SELECT id, city FROM test_lc_e2e ORDER BY id", outID, outCity)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if n != 3 {
		t.Fatalf("row count: got %d, want 3", n)
	}
	if outID.Row(0) != 10 || outCity.Row(0) != "NYC" {
		t.Fatalf("row 0: got id=%d city=%s", outID.Row(0), outCity.Row(0))
	}
	if outCity.Row(2) != "NYC" {
		t.Fatalf("row 2 city: got %s, want NYC", outCity.Row(2))
	}
}

func TestSelectCallbackE2E(t *testing.T) {
	c := connectE2E(t)
	defer func() {
		if err := c.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	}()

	var progressCalled bool
	c.OnProgress = func(p proto.Progress) {
		progressCalled = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.Exec(ctx, "DROP TABLE IF EXISTS test_cb_e2e"); err != nil {
		t.Fatalf("Exec DROP: %v", err)
	}
	if err := c.Exec(ctx, "CREATE TABLE test_cb_e2e (id UInt64) ENGINE = Memory"); err != nil {
		t.Fatalf("Exec CREATE: %v", err)
	}
	defer func() { _ = c.Exec(ctx, "DROP TABLE IF EXISTS test_cb_e2e") }()

	idCol := column.NewBase[uint64]("id")
	for i := uint64(0); i < 1000; i++ {
		idCol.Append(i)
	}
	if err := c.Insert(ctx, "INSERT INTO test_cb_e2e (id) VALUES", idCol); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	outID := column.NewBase[uint64]("id")
	n, err := c.Select(ctx, "SELECT id FROM test_cb_e2e ORDER BY id", outID)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if n != 1000 {
		t.Fatalf("row count: got %d, want 1000", n)
	}
	if !progressCalled {
		t.Fatal("expected OnProgress to be called")
	}
}
