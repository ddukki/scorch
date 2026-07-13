package pool

import (
	"strings"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/ddukki/chu-go/conn"
	"github.com/ddukki/chu-go/dsn"
)

// ParsePoolDSN parses a clickhouse:// DSN into PoolConfig.
func ParsePoolDSN(s string) (PoolConfig, error) {
	r, err := dsn.Parse(s)
	if err != nil {
		return PoolConfig{}, err
	}
	compression, ok := parseCompression(r.Compression)
	if !ok {
		return PoolConfig{}, &conn.Error{Kind: conn.KindConfig, Message: "invalid compress value"}
	}
	return PoolConfig{
		Addrs: r.Addrs,
		Config: conn.Config{
			Addr:         r.Addrs[0],
			User:         r.User,
			Password:     r.Password,
			Database:     r.Database,
			Compression:  compression,
			DialTimeout:  r.DialTimeout,
			ReadTimeout:  r.ReadTimeout,
			WriteTimeout: r.WriteTimeout,
			Settings:     r.Settings,
		},
		MaxConns:            r.MaxConns,
		MinConns:            r.MinConns,
		MaxIdle:             r.MaxIdle,
		MaxLifetime:         r.MaxLifetime,
		HealthCheckInterval: r.HealthCheckInterval,
	}, nil
}

func parseCompression(s string) (proto.Compression, bool) {
	switch strings.ToLower(s) {
	case "":
		return proto.CompressionDisabled, true
	case "none":
		return proto.CompressionDisabled, true
	case "lz4", "true", "enabled":
		return proto.CompressionEnabled, true
	}
	return 0, false
}
