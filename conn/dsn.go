package conn

import (
	"strings"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/ddukki/chu-go/dsn"
)

// ParseDSN parses a ClickHouse DSN into Config.
func ParseDSN(s string) (Config, error) {
	r, err := dsn.Parse(s)
	if err != nil {
		return Config{}, err
	}
	compression, ok := parseCompression(r.Compression)
	if !ok {
		return Config{}, &Error{Kind: KindConfig, Message: "invalid compress value"}
	}
	return Config{
		Addr:         r.Addrs[0],
		User:         r.User,
		Password:     r.Password,
		Database:     r.Database,
		Compression:  compression,
		DialTimeout:  r.DialTimeout,
		ReadTimeout:  r.ReadTimeout,
		WriteTimeout: r.WriteTimeout,
		Settings:     r.Settings,
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
