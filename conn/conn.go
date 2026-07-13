package conn

import (
	"context"
	"crypto/tls"
	"log"
	"net"
	"sync"

	"github.com/ClickHouse/ch-go/proto"
)

// State represents the connection lifecycle state.
type State int

const (
	// StateInitial is the zero state before connecting.
	StateInitial State = iota
	// StateReady means an idle, connected connection.
	StateReady
	// StateBusy means a connection is executing a query.
	StateBusy
	// StateClosed means the connection has been closed.
	StateClosed
)

// Conn is a single ClickHouse native-protocol connection.
// Not safe for concurrent use — at most one query at a time.
type Conn struct {
	mu              sync.Mutex
	state           State
	conn            net.Conn
	cfg             Config
	reader          *proto.Reader
	writer          *proto.Writer
	server          proto.ServerHello
	localAddr       net.Addr
	prefixBytes     []byte
	queryBuf        *proto.Buffer
	skipResults     proto.Results
	skipResultsCode proto.ServerCode

	OnProgress     func(proto.Progress)
	OnProfile      func(proto.Profile)
	OnProfileEvent func(proto.ProfileEvent)
	OnLog          func(proto.Log)
}

// Connect opens a new ClickHouse native-protocol connection.
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
		_ = c.conn.Close()
		return nil, &Error{Kind: KindNetwork, Message: "flush hello", Err: err}
	}

	code, err := c.reader.UVarInt()
	if err != nil {
		_ = c.conn.Close()
		return nil, &Error{Kind: KindNetwork, Message: "read hello response", Err: err}
	}
	switch proto.ServerCode(code) {
	case proto.ServerCodeHello:
		var sh proto.ServerHello
		if err := sh.DecodeAware(c.reader, proto.Version); err != nil {
			if err := c.conn.Close(); err != nil {
				log.Printf("scorch: close conn after decode error: %v", err)
			}
			return nil, &Error{Kind: KindProtocol, Message: "decode server hello", Err: err}
		}
		c.server = sh
		c.localAddr = raw.LocalAddr()
	case proto.ServerCodeException:
		var ex proto.Exception
		if err := ex.DecodeAware(c.reader, proto.Version); err != nil {
			if err := c.conn.Close(); err != nil {
				log.Printf("scorch: close conn after decode exception: %v", err)
			}
			return nil, &Error{Kind: KindProtocol, Message: "decode exception", Err: err}
		}
		if err := c.conn.Close(); err != nil {
			log.Printf("scorch: close conn after server exception: %v", err)
		}
		return nil, &Error{Kind: KindServer, Message: ex.Message, ServerCode: int(ex.Code)}
	default:
		if err := c.conn.Close(); err != nil {
			log.Printf("scorch: close conn after unexpected code: %v", err)
		}
		return nil, &Error{Kind: KindProtocol, Message: "unexpected server code"}
	}

	if proto.FeatureAddendum.In(proto.Version) {
		c.writer.ChainBuffer(func(b *proto.Buffer) {
			b.PutString("")
		})
		if _, err := c.writer.Flush(); err != nil {
			if err := c.conn.Close(); err != nil {
				log.Printf("scorch: close conn after flush error: %v", err)
			}
			return nil, &Error{Kind: KindNetwork, Message: "flush addendum", Err: err}
		}
	}

	c.state = StateReady
	return c, nil
}

// Close closes the connection.
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

// State returns the connection's lifecycle state.
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

// Unlock manually returns a busy connection to ready state.
func (c *Conn) Unlock() {
	c.unlock()
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
		ClientName:      "scorch",
	}
	if addr != "" {
		ci.InitialAddress = addr
	}
	return ci
}

func (c *Conn) encodePrefix() {
	if c.prefixBytes != nil {
		return
	}
	var buf proto.Buffer
	proto.ClientCodeData.Encode(&buf)
	proto.ClientData{}.EncodeAware(&buf, c.server.Revision)
	block := proto.Block{Info: proto.BlockInfo{BucketNum: 0}}
	block.EncodeAware(&buf, c.server.Revision)
	c.prefixBytes = buf.Buf
	c.queryBuf = new(proto.Buffer)
}

// writeQuery encodes and flushes a query with its trailing prefix
// (ClientCodeData + blank block). No closures, no heap allocs.
func (c *Conn) writeQuery(q proto.Query) error {
	c.encodePrefix()
	c.queryBuf.Reset()
	q.EncodeAware(c.queryBuf, c.server.Revision)
	c.writer.ChainWrite(c.queryBuf.Buf)
	c.writer.ChainWrite(c.prefixBytes)
	_, err := c.writer.Flush()
	return err
}
