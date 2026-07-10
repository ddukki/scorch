package conn

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

type State int

const (
	StateInitial State = iota
	StateReady
	StateBusy
	StateClosed
)

type Conn struct {
	mu    sync.Mutex
	state State
	conn  net.Conn
	cfg   Config
	reader   *proto.Reader
	writer   *proto.Writer
	server   proto.ServerHello
	localAddr net.Addr

	OnProgress     func(proto.Progress)
	OnProfile      func(proto.Profile)
	OnProfileEvent func(proto.ProfileEvent)
	OnLog          func(proto.Log)
}

func Connect(ctx context.Context, cfg Config) (*Conn, error) {
	c := &Conn{
		state: StateBusy,
		cfg:   cfg,
	}

	dialer := net.Dialer{Timeout: cfg.DialTimeout}
	raw, err := dialer.DialContext(ctx, "tcp", cfg.addr())
	if err != nil {
		return nil, &Error{Kind: KindNetwork, Message: "dial", Err: err}
	}
	c.conn = raw

	c.conn = raw
	if cfg.TLSConfig != nil {
		c.conn = tls.Client(raw, cfg.TLSConfig)
	}

	buf := new(proto.Buffer)
	c.writer = proto.NewWriter(c.conn, buf)
	c.reader = proto.NewReader(c.conn)

	hello := proto.ClientHello{
		Name:            proto.Name,
		Major:           24,
		Minor:           3,
		ProtocolVersion: proto.Version,
		Database:        cfg.database(),
		User:            cfg.user(),
		Password:        cfg.Password,
	}
	c.writer.ChainBuffer(func(b *proto.Buffer) {
		hello.Encode(b)
	})
	if _, err := c.writer.Flush(); err != nil {
		c.conn.Close()
		return nil, &Error{Kind: KindNetwork, Message: "flush hello", Err: err}
	}

	code, err := c.reader.UVarInt()
	if err != nil {
		c.conn.Close()
		return nil, &Error{Kind: KindNetwork, Message: "read hello response", Err: err}
	}
	switch proto.ServerCode(code) {
	case proto.ServerCodeHello:
		var sh proto.ServerHello
		if err := sh.DecodeAware(c.reader, proto.Version); err != nil {
			c.conn.Close()
			return nil, &Error{Kind: KindProtocol, Message: "decode server hello", Err: err}
		}
		c.server = sh
	c.localAddr = raw.LocalAddr()
	case proto.ServerCodeException:
		var ex proto.Exception
		if err := ex.DecodeAware(c.reader, proto.Version); err != nil {
			c.conn.Close()
			return nil, &Error{Kind: KindProtocol, Message: "decode exception", Err: err}
		}
		c.conn.Close()
		return nil, &Error{Kind: KindServer, Message: ex.Message, ServerCode: int(ex.Code)}
	default:
		c.conn.Close()
		return nil, &Error{Kind: KindProtocol, Message: "unexpected server code"}
	}

	if proto.FeatureAddendum.In(proto.Version) {
		c.writer.ChainBuffer(func(b *proto.Buffer) {
			b.PutString("")
		})
		if _, err := c.writer.Flush(); err != nil {
			c.conn.Close()
			return nil, &Error{Kind: KindNetwork, Message: "flush addendum", Err: err}
		}
	}

	c.state = StateReady
	return c, nil
}

func (c *Conn) Close() error {
	c.mu.Lock()
	if c.state == StateClosed {
		c.mu.Unlock()
		return nil
	}
	c.state = StateClosed
	c.mu.Unlock()
	return c.conn.Close()
}

func (c *Conn) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *Conn) lock() error {
	c.mu.Lock()
	if c.state != StateReady {
		c.mu.Unlock()
		return &Error{Kind: KindInternal, Message: "connection is not ready"}
	}
	c.state = StateBusy
	c.mu.Unlock()
	return nil
}

func (c *Conn) unlock() {
	c.mu.Lock()
	c.state = StateReady
	c.mu.Unlock()
}

func (c *Conn) Unlock() {
	c.unlock()
}

func (c *Conn) setReadTimeout(d time.Duration) {
	if d > 0 {
		c.conn.SetReadDeadline(time.Now().Add(d))
	} else {
		c.conn.SetReadDeadline(time.Time{})
	}
}

func makeClientInfo(server proto.ServerHello, localAddr net.Addr) proto.ClientInfo {
	addr := localAddr.String()
	ci := proto.ClientInfo{
		ProtocolVersion: proto.Version,
		Major:           24,
		Minor:           3,
		Patch:           0,
		Interface:       proto.InterfaceTCP,
		Query:           proto.ClientQueryInitial,
		OSUser:          "",
		ClientHostname:  "",
		ClientName:      "chu-go",
	}
	if addr != "" {
		ci.InitialAddress = addr
	}
	return ci
}
