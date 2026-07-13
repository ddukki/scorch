package pool

import (
	"github.com/jackc/puddle/v2"

	"github.com/ddukki/chu-go/conn"
)

// PoolConn wraps a *conn.Conn with pool management.
type PoolConn struct {
	*conn.Conn
	addr string
	res  *puddle.Resource[*conn.Conn]
}

// Release returns the connection to the pool for reuse.
func (pc *PoolConn) Release() {
	pc.res.Release()
}

// Close destroys the connection instead of returning it.
func (pc *PoolConn) Close() {
	pc.res.Destroy()
}

// Addr returns the server address for this connection.
func (pc *PoolConn) Addr() string {
	return pc.addr
}
