package pool

import (
	"time"

	"github.com/ddukki/chu-go/conn"
)

// PoolConfig configures a connection pool.
type PoolConfig struct {
	Addrs []string
	conn.Config

	MaxConns    int
	MinConns    int
	MaxIdle     int
	MaxLifetime time.Duration

	HealthCheckInterval time.Duration
	HealthCheckTimeout  time.Duration
}
