# chu-go

A Go client for the ClickHouse native (TCP) protocol. Type-safe generic columns, separate `Exec`/`Insert`/`Select` methods, built-in connection pooling.

```
go get github.com/ddukki/chu-go
```

## Why

chu-go is inspired by **[chconn](https://github.com/vahid-sohrabloo/chconn)** — the first Go client to prove generic columns over ClickHouse native protocol. chconn showed that Go 1.18 generics could eliminate per-type column structs and that single-allocation column decode (one `make([]T, rows)` per column) is dramatically faster than per-element append (35 vs 6683 allocs on 100M UInt64 reads).

We wanted chconn's generic column API, but we also wanted the protocol reliability, fuzz testing, and active maintenance of **[ch-go](https://github.com/ClickHouse/ch-go)**. Rather than compromise on either, chu-go combines both:

- **Generic columns like chconn** — `Base[T]`, `Str`, `Nullable[T]`, `LowCardinality[T]`, Tuple2–Tuple12.
- **Protocol from ch-go** — ch-go's wire layer is battle-tested with fuzz, golden, and e2e protocol tests. We don't reimplement the protocol.
- **Safe decode** — one `make([]T, rows)` allocation per column, direct `ReadFull` into the backing array. `Data` is always valid — no reader-buffer expiry, no corruption.
- **Error returns, not panics** — overflow, bounds, and protocol violations are safe by construction.
- **Fuzz + e2e tests from day one** — protocol-level fuzz tests, ch-go cross-verification, testcontainers-based e2e.
- **Built-in pool** — puddle-based connection pool with health checks, dead replica detection, configurable concurrency.

Other clients for context:

- **[ch-go](https://github.com/ClickHouse/ch-go)** — wire-level primitives, one `Do(ctx, Query{})` method, concrete column types per wire format. Excellent protocol tests but verbose column API.
- **[clickhouse-go](https://github.com/ClickHouse/clickhouse-go)** — struct-tag mapping, query builder, ORM-like API. Convenient for row-oriented code, heavy when you need column-level control.
- **[chconn](https://github.com/vahid-sohrabloo/chconn)** — Generic native-protocol columns. Pioneered the column-oriented generics approach chu-go builds on.

If you want raw protocol access, use ch-go. If you want ORM-style struct mapping, use clickhouse-go. If you want generic columns over native protocol with active maintenance, use chu-go.

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/ddukki/chu-go"
    "github.com/ddukki/chu-go/column"
)

func main() {
    ctx := context.Background()

    c, err := chu.Connect(ctx, chu.Config{
        Addr:     "localhost:9000",
        Password: "",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    c.Exec(ctx, "CREATE TABLE test (id UInt64, name String) ENGINE = Memory")

    idCol := column.NewBase[uint64]("id")
    idCol.AppendArr([]uint64{1, 2, 3})
    nameCol := column.NewStr("name")
    nameCol.Append("foo"); nameCol.Append("bar"); nameCol.Append("baz")
    c.Insert(ctx, "INSERT INTO test (id, name) VALUES", idCol, nameCol)

    outID := column.NewBase[uint64]("id")
    outName := column.NewStr("name")
    n, _ := c.Select(ctx, "SELECT id, name FROM test ORDER BY id", outID, outName)
    fmt.Printf("%d rows\n%v %v\n", n, outID.Data, outName.Data)
    // 3 rows
    // [1 2 3] [foo bar baz]
}
```

## API

### Connect

```go
c, err := chu.Connect(ctx, chu.Config{Addr: "localhost:9000"})
```

`Config` fields with zero-value defaults:

| Field | Default |
|-------|---------|
| `Addr` | `127.0.0.1:9000` |
| `User` | `default` |
| `Password` | `""` |
| `Database` | `default` |
| `Compression` | disabled |
| `DialTimeout` | no timeout |
| `ReadTimeout` | no timeout |
| `WriteTimeout` | no timeout |
| `TLSConfig` | nil (plain TCP) |

### Exec

Execute DDL/DML queries that return no result rows.

```go
err := c.Exec(ctx, "CREATE TABLE t (x UInt64) ENGINE = Memory")
```

### Insert

Insert rows via native protocol. Pass one `Column` per table column, **including the column names**.

```go
col := column.NewBase[uint64]("id")
col.Append(1); col.Append(2)
c.Insert(ctx, "INSERT INTO t (id) VALUES", col)
```

### Select

Read results into pre-allocated columns. Returns row count.

```go
col := column.NewBase[uint64]("id")
n, err := c.Select(ctx, "SELECT id FROM t", col)
```

### Callbacks

Observe server-side telemetry during any operation:

```go
c.OnProgress = func(p proto.Progress) {
    log.Printf("rows=%d bytes=%d", p.Rows, p.Bytes)
}
c.OnProfile = func(p proto.Profile) { /* ... */ }
c.OnProfileEvent = func(p proto.ProfileEvent) { /* ... */ }
c.OnLog = func(l proto.Log) { /* ... */ }
```

### SelectStream

Stream large result sets block by block.

```go
s, _ := c.SelectStream(ctx, "SELECT * FROM large_table")
s.Bind(idCol, nameCol)
for s.Next() {
    // Each Next() appends one block to bound columns
    // Access col.Data to get all rows accumulated so far
}
if err := s.Err(); err != nil {
    log.Fatal(err)
}
s.Close()
```

Cancel mid-stream:

```go
s, _ := c.SelectStream(ctx, "SELECT * FROM huge_table")
s.Bind(col)
for s.Next() {
    if someCondition {
        s.Cancel()  // sends cancel, drains remaining blocks
        break
    }
}
s.Close()
```

### InsertStream

Insert data in multiple blocks.

```go
s, _ := c.InsertStream(ctx, "INSERT INTO t (id, name) VALUES")
s.Bind(idCol, nameCol)

idCol.AppendArr([]uint64{1, 2, 3})
nameCol.AppendArr([]string{"a", "b", "c"})
s.Append()  // sends block

idCol.Data = idCol.Data[:0]
nameCol.Data = nameCol.Data[:0]
idCol.AppendArr([]uint64{4, 5})
nameCol.AppendArr([]string{"d", "e"})
s.Append()  // sends second block

s.Close()  // sends end-of-data, reads server response
```

### Connection pool

```go
import "github.com/ddukki/chu-go/pool"

p, _ := pool.New(ctx, pool.PoolConfig{
    Config:              chu.Config{Addr: "localhost:9000"},
    MaxConns:            10,
    HealthCheckInterval: 30 * time.Second,
})
defer p.Close()

p.Exec(ctx, "SELECT 1")
p.Select(ctx, "SELECT id FROM t", col)
p.Insert(ctx, "INSERT INTO t VALUES", col)

ss, _ := p.SelectStream(ctx, "SELECT * FROM large")
ss.Bind(col); for ss.Next() { /* ... */ }; ss.Close()
is, _ := p.InsertStream(ctx, "INSERT INTO t VALUES")
is.Bind(col); is.Append(); is.Close()
```

## Column types

| Type | Go type | Constructor |
|------|---------|-------------|
| UInt8, UInt16, UInt32, UInt64 | `uint8, uint16, uint32, uint64` | `NewBase[T]("name")` |
| Int8, Int16, Int32, Int64 | `int8, int16, int32, int64` | `NewBase[T]("name")` |
| Float32, Float64 | `float32, float64` | `NewBase[T]("name")` |
| String | `string` | `NewStr("name")` |
| Nullable(T) | `(T, bool)` | `NewNullable[T](inner)` |
| LowCardinality(T) | `T` (deduplicated) | `NewLowCardinality[T](inner)` |
| Tuple(T1..T12) | `Tuple2Value[T1,T2]` etc. | `NewTuple2(col1, col2)` |

Missing types (open an issue or PR): Decimal, Date, DateTime, Array, Map, IPv4, IPv6, UUID, Enum, Geo types.

## Compared to ch-go

| | ch-go | chu-go |
|---|---|---|
| **Column API** | Per-type structs (`ColUInt64`, `ColStr`, ...) | Generics (`Base[T]`, `Str`, ...) |
| **Operation dispatch** | Single `Do(ctx, Query{})` | `Exec` / `Insert` / `Select` |
| **Connection pool** | Not included | `pool/` package (puddle-based) |
| **Tuple support** | Manual | `Tuple2`–`Tuple12` codegen |
| **Nullable** | `ColNullable` wrapper | `Nullable[T]` generic |
| **LowCardinality** | `ColLowCardinality` wrapper | `LowCardinality[T]` generic |
| **Error handling** | Panics on overflow | Returned errors, no panics |

## Compared to clickhouse-go

| | clickhouse-go | chu-go |
|---|---|---|
| **Protocol** | Native + HTTP | Native only |
| **Query style** | `conn.QueryRowContext("SELECT ?", args)` | `c.Exec("SELECT 1")` (raw SQL) |
| **Result mapping** | `Scan(&a, &b)` struct tags | Manual column extraction |
| **Type system** | Reflection + `Scan` | Generic types + unsafe decode |
| **Connection pool** | Built-in | Separate `pool/` package |
| **API surface** | Large (~30 packages) | Small (~4 packages) |

## Design

- **Wraps ch-go wire primitives**, not a reimplementation. Uses `proto.Reader`, `proto.Writer`, `proto.Buffer` from ch-go for all wire encoding.
- **Column-oriented.** You build columns, not rows. Insert passes columns; Select fills columns.
- **State machine.** `Initial → Ready → Busy → Ready → Closed`. No concurrent queries per connection.
- **Streaming.** SelectStream pulls blocks via Next(); InsertStream pushes blocks via Append(). Both use Bind() for pre-bound columns.
- **Revision-gated.** Checks server revision for features (`FeatureCustomSerialization`, `FeatureBlockInfo`, etc.) at runtime.
- **No panics.** All errors returned. Overflow, bounds, and protocol violations are safe by construction.
- **No panics in library code.** Overflow and bounds violations return errors. Safe by construction.
