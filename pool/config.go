package pool

import (
	"time"

	"github.com/ddukki/scorch/conn"
)

// Config configures a connection pool.
type Config struct {
	Addrs []string
	conn.Config

	MaxConns    int
	MinConns    int
	MaxIdle     int
	MaxLifetime time.Duration

	HealthCheckInterval time.Duration
	HealthCheckTimeout  time.Duration
}
