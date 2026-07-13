package dsn

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/ClickHouse/ch-go/proto"
)

// Config stores the result of parsing a ClickHouse DSN.
type Config struct {
	Addrs               []string
	User                string
	Password            string
	Database            string
	Compression         string
	DialTimeout         time.Duration
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	Secure              bool
	Settings            []proto.Setting
	MaxConns            int
	MinConns            int
	MaxIdle             int
	MaxLifetime         time.Duration
	HealthCheckInterval time.Duration
}

// ErrorKind classifies a DSN parse error.
type ErrorKind int

const (
	// KindParse is a DSN parse error.
	KindParse    ErrorKind = iota
	// KindSecurity is a security validation error.
	KindSecurity
	// KindScheme is an unsupported URL scheme error.
	KindScheme
)

// Error is a DSN parse error with a Kind and optional wrapped error.
type Error struct {
	Kind    ErrorKind
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("dsn: %s: %v", e.Message, e.Err)
	}
	return fmt.Sprintf("dsn: %s", e.Message)
}

func (e *Error) Unwrap() error { return e.Err }

// Parse parses a clickhouse:// DSN string into Config.
func Parse(dsn string) (Config, error) {
	const maxFieldBytes = 1024
	const maxHosts = 100

	u, err := url.Parse(dsn)
	if err != nil {
		return Config{}, &Error{Kind: KindParse, Message: "invalid URL", Err: err}
	}

	if u.Scheme != "clickhouse" {
		return Config{}, &Error{Kind: KindScheme, Message: fmt.Sprintf("unsupported scheme %q (only clickhouse://)", u.Scheme)}
	}

	rawHosts := strings.Split(u.Host, ",")
	if len(rawHosts) > maxHosts {
		return Config{}, &Error{Kind: KindSecurity, Message: fmt.Sprintf("too many hosts: %d (max %d)", len(rawHosts), maxHosts)}
	}

	var addrs []string
	for i, h := range rawHosts {
		h = strings.TrimSpace(h)
		if h == "" {
			return Config{}, &Error{Kind: KindParse, Message: fmt.Sprintf("empty host segment at position %d", i)}
		}
		var host, port string
	if strings.Contains(h, ":") {
		var err error
		host, port, err = net.SplitHostPort(h)
		if err != nil {
			return Config{}, &Error{Kind: KindParse, Message: fmt.Sprintf("invalid host %q", h), Err: err}
		}
	} else {
		host = h
	}
	if host == "" {
		return Config{}, &Error{Kind: KindParse, Message: fmt.Sprintf("empty host at position %d", i)}
	}
	if port == "" {
		h = net.JoinHostPort(host, "9000")
	}
		addrs = append(addrs, h)
	}
	if len(addrs) == 0 {
		return Config{}, &Error{Kind: KindParse, Message: "no hosts in DSN"}
	}

	cfg := Config{Addrs: addrs}

	if u.User != nil {
		cfg.User = u.User.Username()
		cfg.Password, _ = u.User.Password()
	}

	if len(cfg.User) > maxFieldBytes {
		return Config{}, &Error{Kind: KindSecurity, Message: "username exceeds 1024 bytes"}
	}
	if len(cfg.Password) > maxFieldBytes {
		return Config{}, &Error{Kind: KindSecurity, Message: "password exceeds 1024 bytes"}
	}

	db := strings.TrimPrefix(u.Path, "/")
	db = strings.TrimRight(db, "/")
	if db != "" {
		if containsPathTraversal(db) {
			return Config{}, &Error{Kind: KindSecurity, Message: "database name contains path traversal characters"}
		}
		if containsControlChars(db) {
			return Config{}, &Error{Kind: KindSecurity, Message: "database name contains control characters"}
		}
		if len(db) > maxFieldBytes {
			return Config{}, &Error{Kind: KindSecurity, Message: "database name exceeds 1024 bytes"}
		}
		cfg.Database = db
	}

	for key, vals := range u.Query() {
		val := vals[0]
		switch key {
		case "dial_timeout":
			d, err := time.ParseDuration(val)
			if err != nil {
				return Config{}, &Error{Kind: KindParse, Message: fmt.Sprintf("invalid dial_timeout %q", val), Err: err}
			}
			cfg.DialTimeout = d
		case "compress":
			cfg.Compression = val
		case "secure":
			b, err := strconv.ParseBool(val)
			if err != nil {
				return Config{}, &Error{Kind: KindParse, Message: fmt.Sprintf("invalid secure %q", val), Err: err}
			}
			cfg.Secure = b
		case "read_timeout":
			d, err := time.ParseDuration(val)
			if err != nil {
				return Config{}, &Error{Kind: KindParse, Message: fmt.Sprintf("invalid read_timeout %q", val), Err: err}
			}
			cfg.ReadTimeout = d
		case "write_timeout":
			d, err := time.ParseDuration(val)
			if err != nil {
				return Config{}, &Error{Kind: KindParse, Message: fmt.Sprintf("invalid write_timeout %q", val), Err: err}
			}
			cfg.WriteTimeout = d
		case "pool_max_conns":
			n, err := strconv.Atoi(val)
			if err != nil {
				return Config{}, &Error{Kind: KindParse, Message: fmt.Sprintf("invalid pool_max_conns %q", val), Err: err}
			}
			cfg.MaxConns = n
		case "pool_min_conns":
			n, err := strconv.Atoi(val)
			if err != nil {
				return Config{}, &Error{Kind: KindParse, Message: fmt.Sprintf("invalid pool_min_conns %q", val), Err: err}
			}
			cfg.MinConns = n
		case "health_check_interval":
			d, err := time.ParseDuration(val)
			if err != nil {
				return Config{}, &Error{Kind: KindParse, Message: fmt.Sprintf("invalid health_check_interval %q", val), Err: err}
			}
			cfg.HealthCheckInterval = d
		default:
			cfg.Settings = append(cfg.Settings, proto.Setting{Key: key, Value: val})
		}
	}

	return cfg, nil
}

func containsPathTraversal(s string) bool {
	if strings.Contains(s, "..") {
		return true
	}
	if strings.ContainsAny(s, "/\\") {
		return true
	}
	return false
}

func containsControlChars(s string) bool {
	for _, r := range s {
		if r != '\n' && r != '\r' && unicode.IsControl(r) {
			return true
		}
	}
	return false
}

// SanitizeForError removes control characters from a string for safe display.
func SanitizeForError(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r == '\n' || r == '\r' || r == '\x00' {
			b.WriteRune('.')
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}
