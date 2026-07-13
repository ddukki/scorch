package conn

import (
	"crypto/tls"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// Compression is a ClickHouse wire compression mode.
type Compression = proto.Compression

const (
	// CompressionDisabled disables wire compression.
	CompressionDisabled = proto.CompressionDisabled
	// CompressionEnabled enables LZ4 wire compression.
	CompressionEnabled  = proto.CompressionEnabled
)

// Setting is a ClickHouse query setting.
type Setting = proto.Setting

// Config configures a ClickHouse native-protocol connection.
type Config struct {
	Addr         string
	User         string
	Password     string
	Database     string
	Compression  Compression
	Settings     []Setting
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	TLSConfig    *tls.Config
}

func (c Config) addr() string {
	if c.Addr == "" { return "127.0.0.1:9000" }
	return c.Addr
}

func (c Config) user() string {
	if c.User == "" { return "default" }
	return c.User
}

func (c Config) database() string {
	if c.Database == "" { return "default" }
	return c.Database
}
