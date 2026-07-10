package conn

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/ddukki/chu-go/column"
)

func TestSelectStreamSingleBlockE2E(t *testing.T) {
	c := connectE2E(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c.Exec(ctx, "DROP TABLE IF EXISTS test_stream_select")
	c.Exec(ctx, "CREATE TABLE test_stream_select (id UInt64, name String) ENGINE = Memory")

	idCol := column.NewBase[uint64]("id")
	idCol.AppendArr([]uint64{1, 2, 3})
	nameCol := column.NewStr("name")
	nameCol.AppendArr([]string{"a", "b", "c"})
	c.Insert(ctx, "INSERT INTO test_stream_select (id, name) VALUES", idCol, nameCol)

	outID := column.NewBase[uint64]("id")
	outName := column.NewStr("name")

	s, err := c.SelectStream(ctx, "SELECT id, name FROM test_stream_select ORDER BY id")
	if err != nil {
		t.Fatalf("SelectStream: %v", err)
	}
	defer s.Close()

	s.Bind(outID, outName)

	var blocks int
	for s.Next() {
		blocks++
	}
	if s.Err() != nil {
		t.Fatalf("Err: %v", s.Err())
	}
	if blocks != 1 {
		t.Fatalf("blocks: got %d, want 1", blocks)
	}
	if outID.Len() != 3 {
		t.Fatalf("id rows: got %d, want 3", outID.Len())
	}
	if outName.Len() != 3 {
		t.Fatalf("name rows: got %d, want 3", outName.Len())
	}
	if outID.Row(0) != 1 || outID.Row(1) != 2 || outID.Row(2) != 3 {
		t.Fatalf("id data: got %v", outID.Data)
	}
	if outName.Row(0) != "a" || outName.Row(1) != "b" || outName.Row(2) != "c" {
		t.Fatalf("name data: got %v", outName.Data)
	}
}

func TestSelectStreamBindBeforeNextE2E(t *testing.T) {
	c := connectE2E(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	s, err := c.SelectStream(ctx, "SELECT 1 AS v")
	if err != nil {
		t.Fatalf("SelectStream: %v", err)
	}
	defer s.Close()

	if s.Next() {
		t.Fatal("expected Next() == false without Bind")
	}
	if s.Err() == nil {
		t.Fatal("expected error without Bind")
	}
}

func TestSelectStreamCancelMidStreamE2E(t *testing.T) {
	c := connectE2E(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c.Exec(ctx, "DROP TABLE IF EXISTS test_stream_cancel")
	c.Exec(ctx, "CREATE TABLE test_stream_cancel (id UInt64) ENGINE = Memory")

	col := column.NewBase[uint64]("id")
	for i := uint64(0); i < 500; i++ {
		col.Append(i)
	}
	c.Insert(ctx, "INSERT INTO test_stream_cancel (id) VALUES", col)

	out := column.NewBase[uint64]("id")
	s, err := c.SelectStream(ctx, "SELECT id FROM test_stream_cancel ORDER BY id")
	if err != nil {
		t.Fatalf("SelectStream: %v", err)
	}
	defer s.Close()

	s.Bind(out)
	if !s.Next() {
		t.Fatal("expected Next() == true")
	}

	s.Cancel()
	if s.Next() {
		t.Fatal("expected Next() == false after Cancel")
	}
}

func TestSelectStreamCloseBeforeNextE2E(t *testing.T) {
	c := connectE2E(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	s, err := c.SelectStream(ctx, "SELECT 1 AS v")
	if err != nil {
		t.Fatalf("SelectStream: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close without Next: %v", err)
	}
}

func TestSelectStreamMultipleCloseE2E(t *testing.T) {
	c := connectE2E(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	s, err := c.SelectStream(ctx, "SELECT 1 AS v")
	if err != nil {
		t.Fatalf("SelectStream: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestSelectStreamCallbacksE2E(t *testing.T) {
	c := connectE2E(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	progressFired := make(chan struct{}, 1)
	c.OnProgress = func(p proto.Progress) {
		select {
		case progressFired <- struct{}{}:
		default:
		}
	}

	c.Exec(ctx, "DROP TABLE IF EXISTS test_stream_cb")
	c.Exec(ctx, "CREATE TABLE test_stream_cb (id UInt64) ENGINE = Memory")
	col := column.NewBase[uint64]("id")
	col.AppendArr([]uint64{1, 2, 3})
	c.Insert(ctx, "INSERT INTO test_stream_cb (id) VALUES", col)

	out := column.NewBase[uint64]("id")
	s, err := c.SelectStream(ctx, "SELECT id FROM test_stream_cb")
	if err != nil {
		t.Fatalf("SelectStream: %v", err)
	}
	defer s.Close()

	s.Bind(out)
	for s.Next() {
	}
	if s.Err() != nil {
		t.Fatalf("Err: %v", s.Err())
	}

	select {
	case <-progressFired:
	case <-time.After(5 * time.Second):
		t.Fatal("OnProgress was not fired")
	}
}

func TestInsertStreamMultiBlockE2E(t *testing.T) {
	c := connectE2E(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c.Exec(ctx, "DROP TABLE IF EXISTS test_insert_stream")
	c.Exec(ctx, "CREATE TABLE test_insert_stream (id UInt64, name String) ENGINE = Memory")

	idCol := column.NewBase[uint64]("id")
	nameCol := column.NewStr("name")

	s, err := c.InsertStream(ctx, "INSERT INTO test_insert_stream (id, name) VALUES")
	if err != nil {
		t.Fatalf("InsertStream: %v", err)
	}

	s.Bind(idCol, nameCol)

	idCol.AppendArr([]uint64{1, 2, 3})
	nameCol.AppendArr([]string{"a", "b", "c"})
	if err := s.Append(); err != nil {
		t.Fatalf("first Append: %v", err)
	}

	idCol.Data = idCol.Data[:0]
	nameCol.Data = nameCol.Data[:0]
	idCol.AppendArr([]uint64{4, 5})
	nameCol.AppendArr([]string{"d", "e"})
	if err := s.Append(); err != nil {
		t.Fatalf("second Append: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	outID := column.NewBase[uint64]("id")
	outName := column.NewStr("name")
	n, err := c.Select(ctx, "SELECT id, name FROM test_insert_stream ORDER BY id", outID, outName)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if n != 5 {
		t.Fatalf("rows: got %d, want 5", n)
	}
	if outID.Row(0) != 1 || outID.Row(4) != 5 {
		t.Fatalf("id data: got %v", outID.Data)
	}
}

func TestInsertStreamBindBeforeAppendE2E(t *testing.T) {
	c := connectE2E(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	s, err := c.InsertStream(ctx, "INSERT INTO test_insert_stream (id) VALUES")
	if err != nil {
		t.Fatalf("InsertStream: %v", err)
	}
	defer s.Close()

	if err := s.Append(); err == nil {
		t.Fatal("expected error without Bind")
	}
}

func TestInsertStreamNoDataE2E(t *testing.T) {
	c := connectE2E(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	s, err := c.InsertStream(ctx, "INSERT INTO test_insert_stream (id) VALUES")
	if err != nil {
		t.Fatalf("InsertStream: %v", err)
	}
	defer s.Close()

	s.Bind(column.NewBase[uint64]("id"))
	if err := s.Append(); err == nil {
		t.Fatal("expected error with empty data")
	}
}

func TestInsertStreamMultipleCloseE2E(t *testing.T) {
	c := connectE2E(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	s, err := c.InsertStream(ctx, "INSERT INTO test_insert_stream (id) VALUES")
	if err != nil {
		t.Fatalf("InsertStream: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
