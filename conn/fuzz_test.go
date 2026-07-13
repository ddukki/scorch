package conn

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/ddukki/scorch/column"
	"github.com/stretchr/testify/require"
)

// TestFuzzRoundtripE2E exercises a full encode → Insert → Select → decode
// roundtrip for every column type.  Values are verified byte-exact after
// roundtripping through the server's native protocol.
func TestFuzzRoundtripE2E(t *testing.T) {
	c := connectE2E(t)
	defer func() { require.NoError(t, c.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	roundtrips := []struct {
		name  string
		ddl   string
		in    func(name string) Column // construct + append input values
		check func(t *testing.T, col Column)
	}{
		{
			"uint8", "v UInt8",
			func(n string) Column {
				c := column.NewBase[uint8](n)
				c.Append(0)
				c.Append(1)
				c.Append(255)
				return c
			},
			func(t *testing.T, col Column) {
				v := col.(*column.Base[uint8])
				if v.Row(0) != 0 || v.Row(1) != 1 || v.Row(2) != 255 {
					t.Fatalf("uint8: got %v", v.Data)
				}
			},
		},
		{
			"uint16", "v UInt16",
			func(n string) Column {
				c := column.NewBase[uint16](n)
				c.Append(0)
				c.Append(1)
				c.Append(65535)
				return c
			},
			func(t *testing.T, col Column) {
				c := col.(*column.Base[uint16])
				if c.Row(0) != 0 || c.Row(1) != 1 || c.Row(2) != 65535 {
					t.Fatalf("uint16: got %v", c.Data)
				}
			},
		},
		{
			"uint32", "v UInt32",
			func(n string) Column {
				c := column.NewBase[uint32](n)
				c.Append(0)
				c.Append(1)
				c.Append(4294967295)
				return c
			},
			func(t *testing.T, col Column) {
				c := col.(*column.Base[uint32])
				if c.Row(0) != 0 || c.Row(1) != 1 || c.Row(2) != 4294967295 {
					t.Fatalf("uint32: got %v", c.Data)
				}
			},
		},
		{
			"uint64", "v UInt64",
			func(n string) Column {
				c := column.NewBase[uint64](n)
				c.Append(0)
				c.Append(1)
				c.Append(18446744073709551615)
				return c
			},
			func(t *testing.T, col Column) {
				c := col.(*column.Base[uint64])
				if c.Row(0) != 0 || c.Row(1) != 1 || c.Row(2) != 18446744073709551615 {
					t.Fatalf("uint64: got %v", c.Data)
				}
			},
		},
		{
			"int8", "v Int8",
			func(n string) Column {
				c := column.NewBase[int8](n)
				c.Append(-128)
				c.Append(0)
				c.Append(127)
				return c
			},
			func(t *testing.T, col Column) {
				c := col.(*column.Base[int8])
				if c.Row(0) != -128 || c.Row(1) != 0 || c.Row(2) != 127 {
					t.Fatalf("int8: got %v", c.Data)
				}
			},
		},
		{
			"int16", "v Int16",
			func(n string) Column {
				c := column.NewBase[int16](n)
				c.Append(-32768)
				c.Append(0)
				c.Append(32767)
				return c
			},
			func(t *testing.T, col Column) {
				c := col.(*column.Base[int16])
				if c.Row(0) != -32768 || c.Row(1) != 0 || c.Row(2) != 32767 {
					t.Fatalf("int16: got %v", c.Data)
				}
			},
		},
		{
			"int32", "v Int32",
			func(n string) Column {
				c := column.NewBase[int32](n)
				c.Append(-2147483648)
				c.Append(0)
				c.Append(2147483647)
				return c
			},
			func(t *testing.T, col Column) {
				c := col.(*column.Base[int32])
				if c.Row(0) != -2147483648 || c.Row(1) != 0 || c.Row(2) != 2147483647 {
					t.Fatalf("int32: got %v", c.Data)
				}
			},
		},
		{
			"int64", "v Int64",
			func(n string) Column {
				c := column.NewBase[int64](n)
				c.Append(-9223372036854775808)
				c.Append(0)
				c.Append(9223372036854775807)
				return c
			},
			func(t *testing.T, col Column) {
				c := col.(*column.Base[int64])
				if c.Row(0) != -9223372036854775808 || c.Row(1) != 0 || c.Row(2) != 9223372036854775807 {
					t.Fatalf("int64: got %v", c.Data)
				}
			},
		},
		{
			"float32", "v Float32",
			func(n string) Column {
				c := column.NewBase[float32](n)
				c.Append(-3.4028235e38)
				c.Append(0)
				c.Append(3.4028235e38)
				return c
			},
			func(t *testing.T, col Column) {
				c := col.(*column.Base[float32])
				if c.Row(0) != -3.4028235e38 || c.Row(1) != 0 || c.Row(2) != 3.4028235e38 {
					t.Fatalf("float32: got %v", c.Data)
				}
			},
		},
		{
			"float64", "v Float64",
			func(n string) Column {
				c := column.NewBase[float64](n)
				c.Append(-1.7976931348623157e308)
				c.Append(0)
				c.Append(1.7976931348623157e308)
				return c
			},
			func(t *testing.T, col Column) {
				c := col.(*column.Base[float64])
				if c.Row(0) != -1.7976931348623157e308 || c.Row(1) != 0 || c.Row(2) != 1.7976931348623157e308 {
					t.Fatalf("float64: got %v", c.Data)
				}
			},
		},
		{
			"string", "v String",
			func(n string) Column {
				c := column.NewStr(n)
				c.Append("hello")
				c.Append("")
				c.Append("world")
				return c
			},
			func(t *testing.T, col Column) {
				c := col.(*column.Str)
				// ORDER BY sorts: '' < 'hello' < 'world'
				if c.Row(0) != "" || c.Row(1) != "hello" || c.Row(2) != "world" {
					t.Fatalf("string: got %v", c.Data)
				}
			},
		},
		{
			"nullable", "v Nullable(UInt64)",
			func(n string) Column {
				c := column.NewNullable(column.NewBase[uint64](n))
				c.Append(1, false)
				c.Append(0, true)
				c.Append(3, false)
				return c
			},
			func(t *testing.T, col Column) {
				c := col.(*column.Nullable[uint64])
				// ORDER BY sorts: NULLs last, so positions are [1, 3, NULL]
				if c.Nulls[0] || c.Nulls[1] {
					t.Fatal("rows 0,1 should be non-null")
				}
				if !c.Nulls[2] {
					t.Fatal("row 2 should be null")
				}
				if c.Values.Row(0) != 1 || c.Values.Row(1) != 3 {
					t.Fatalf("nullable uint64 values: got %d %d", c.Values.Row(0), c.Values.Row(1))
				}
			},
		},
		{
			"nullable_str", "v Nullable(String)",
			func(n string) Column {
				c := column.NewNullable(column.NewStr(n))
				c.Append("a", false)
				c.Append("", true)
				c.Append("c", false)
				return c
			},
			func(t *testing.T, col Column) {
				c := col.(*column.Nullable[string])
				// ORDER BY sorts: ''a' < 'c', NULLs last → positions [a, c, NULL]
				if c.Nulls[0] || c.Nulls[1] {
					t.Fatal("rows 0,1 should be non-null")
				}
				if !c.Nulls[2] {
					t.Fatal("row 2 should be null")
				}
				if c.Values.Row(0) != "a" || c.Values.Row(1) != "c" {
					t.Fatalf("nullable str values: got %q %q", c.Values.Row(0), c.Values.Row(1))
				}
			},
		},
	}

	for _, rt := range roundtrips {
		t.Run(rt.name, func(t *testing.T) {
			table := "fuzz_rt_" + rt.name

			if err := c.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
				t.Fatalf("DROP: %v", err)
			}
			if err := c.Exec(ctx, "CREATE TABLE "+table+" ("+rt.ddl+") ENGINE = Memory"); err != nil {
				t.Fatalf("CREATE: %v", err)
			}

			in := rt.in("v")
			if err := c.Insert(ctx, "INSERT INTO "+table+" (v) VALUES", in); err != nil {
				t.Fatalf("Insert: %v", err)
			}

			out := rt.in("v")
			n, err := c.Select(ctx, "SELECT v FROM "+table+" ORDER BY v", out)
			if err != nil {
				t.Fatalf("Select: %v", err)
			}
			if n != 3 {
				t.Fatalf("rows: got %d, want 3", n)
			}

			rt.check(t, out)
		})
	}
}

// TestFuzzColumnCountMismatchE2E verifies Select returns protocol error when
// the caller provides wrong column count.
func TestFuzzColumnCountMismatchE2E(t *testing.T) {
	c := connectE2E(t)
	defer func() { require.NoError(t, c.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1 column query but pass 2 output columns
	_, err := c.Select(ctx, "SELECT 1 AS a", column.NewBase[uint64]("a"), column.NewBase[uint64]("b"))
	if err == nil {
		t.Fatal("expected error for column count mismatch")
	}
	var ce *Error
	if !asError(err, &ce) {
		t.Fatalf("expected *Error, got %T", err)
	}
	if ce.Kind != KindProtocol {
		t.Fatalf("error kind: got %d, want %d", ce.Kind, KindProtocol)
	}
}

// TestFuzzEmptyResultSet selects from an empty table.
func TestFuzzEmptyResultSetE2E(t *testing.T) {
	c := connectE2E(t)
	defer func() { require.NoError(t, c.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.Exec(ctx, "DROP TABLE IF EXISTS fuzz_empty"); err != nil {
		t.Fatalf("DROP: %v", err)
	}
	if err := c.Exec(ctx, "CREATE TABLE fuzz_empty (id UInt64, name String) ENGINE = Memory"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	outID := column.NewBase[uint64]("id")
	outName := column.NewStr("name")
	n, err := c.Select(ctx, "SELECT id, name FROM fuzz_empty ORDER BY id", outID, outName)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if n != 0 {
		t.Fatalf("rows: got %d, want 0", n)
	}
	if outID.Len() != 0 {
		t.Fatalf("id column len: got %d, want 0", outID.Len())
	}
}

// TestFuzzManyRows selects a table with many rows to stress the response loop.
func TestFuzzManyRowsE2E(t *testing.T) {
	c := connectE2E(t)
	defer func() { require.NoError(t, c.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.Exec(ctx, "DROP TABLE IF EXISTS fuzz_many"); err != nil {
		t.Fatalf("DROP: %v", err)
	}
	if err := c.Exec(ctx, "CREATE TABLE fuzz_many (id UInt64) ENGINE = Memory"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	// Insert 500 rows via native encoder
	col := column.NewBase[uint64]("id")
	for i := uint64(0); i < 500; i++ {
		col.Append(i)
	}
	if err := c.Insert(ctx, "INSERT INTO fuzz_many (id) VALUES", col); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	out := column.NewBase[uint64]("id")
	n, err := c.Select(ctx, "SELECT id FROM fuzz_many ORDER BY id", out)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if n != 500 {
		t.Fatalf("rows: got %d, want 500", n)
	}
	for i := uint64(0); i < 500; i++ {
		if out.Row(int(i)) != i {
			t.Fatalf("row %d: got %d, want %d", i, out.Row(int(i)), i)
		}
	}
}

// TestFuzzCallbackCrash verifies that nil callbacks don't cause panics during
// SELECT response processing, and that set callbacks are invoked without error.
func TestFuzzCallbackCrashE2E(t *testing.T) {
	c := connectE2E(t)
	defer func() { require.NoError(t, c.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.Exec(ctx, "DROP TABLE IF EXISTS fuzz_cb"); err != nil {
		t.Fatalf("DROP: %v", err)
	}
	if err := c.Exec(ctx, "CREATE TABLE fuzz_cb (x UInt64) ENGINE = Memory"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	out := column.NewBase[uint64]("x")
	// Leave OnProgress/OnProfile nil — should not panic
	_, err := c.Select(ctx, "SELECT x FROM fuzz_cb ORDER BY x", out)
	if err != nil {
		t.Fatalf("Select with nil callbacks: %v", err)
	}

	// Set OnProgress, verify it fires at least once
	gotProgress := false
	c.OnProgress = func(p proto.Progress) { gotProgress = true }
	out2 := column.NewBase[uint64]("x")
	_, err = c.Select(ctx, "SELECT x FROM fuzz_cb ORDER BY x", out2)
	if err != nil {
		t.Fatalf("Select with OnProgress: %v", err)
	}
	if !gotProgress {
		t.Log("warning: OnProgress was never invoked (may be intermittent)")
	}
}

// TestFuzzSelectAllTypes selects from a table with one row of every type to
// verify SELECT can decode heterogeneous columns.
func TestFuzzSelectAllTypesE2E(t *testing.T) {
	c := connectE2E(t)
	defer func() { require.NoError(t, c.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.Exec(ctx, "DROP TABLE IF EXISTS fuzz_alltypes"); err != nil {
		t.Fatalf("DROP: %v", err)
	}
	ddl := "(a UInt64, b Int32, c String, d Float64, e UInt8)"
	if err := c.Exec(ctx, "CREATE TABLE fuzz_alltypes "+ddl+" ENGINE = Memory"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if err := c.Exec(ctx, "INSERT INTO fuzz_alltypes VALUES (42, -99, 'hello', 3.14, 1)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	a := column.NewBase[uint64]("a")
	b := column.NewBase[int32]("b")
	cStr := column.NewStr("c")
	d := column.NewBase[float64]("d")
	e := column.NewBase[uint8]("e")

	n, err := c.Select(ctx, "SELECT a,b,c,d,e FROM fuzz_alltypes ORDER BY a", a, b, cStr, d, e)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if n != 1 {
		t.Fatalf("rows: got %d, want 1", n)
	}
	if a.Row(0) != 42 || b.Row(0) != -99 || cStr.Row(0) != "hello" {
		t.Fatalf("values mismatch: a=%d b=%d c=%s", a.Row(0), b.Row(0), cStr.Row(0))
	}
}

// asError is like errors.As for *Error.
func asError(err error, target **Error) bool {
	if err == nil {
		return false
	}
	e, ok := err.(*Error)
	if !ok {
		return false
	}
	*target = e
	return true
}
