package pool

import (
	"github.com/jackc/puddle/v2"

	"github.com/ddukki/scorch/conn"
)

// Conn wraps a *conn.Conn with pool management.
type Conn struct {
	*conn.Conn
	addr string
	res  *puddle.Resource[*conn.Conn]
}

// Release returns the connection to the pool for reuse.
func (pc *Conn) Release() {
	pc.res.Release()
}

// Close destroys the connection instead of returning it.
func (pc *Conn) Close() {
	pc.res.Destroy()
}

// Addr returns the server address for this connection.
func (pc *Conn) Addr() string {
	return pc.addr
}
