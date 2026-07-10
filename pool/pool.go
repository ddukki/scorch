package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/puddle/v2"

	"github.com/ddukki/chu-go/conn"
)

type addrPool struct {
	addr string
	dead bool
	p    *puddle.Pool[*conn.Conn]
}

type ConnState struct {
	Total   int
	Idle    int
	Active  int
	Waiting int
}

type AddrState struct {
	Addr string
	ConnState
	Dead bool
}

type Pool struct {
	mu     sync.Mutex
	cfg    PoolConfig
	subs   []*addrPool
	rrIdx  int
	closed bool
	stopCh chan struct{}
}

func New(ctx context.Context, cfg PoolConfig) (*Pool, error) {
	addrs := cfg.Addrs
	if len(addrs) == 0 {
		a := cfg.Addr
		if a == "" {
			a = "127.0.0.1:9000"
		}
		addrs = []string{a}
	}

	maxSize := int32(cfg.MaxConns)
	if maxSize == 0 {
		maxSize = 1000
	}

	p := &Pool{
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}

	for _, addr := range addrs {
		subCfg := cfg.Config
		subCfg.Addr = addr

		sub := &addrPool{addr: addr}
		pp, err := puddle.NewPool(&puddle.Config[*conn.Conn]{
			Constructor: func(ctx context.Context) (*conn.Conn, error) {
				return conn.Connect(ctx, subCfg)
			},
			Destructor: func(c *conn.Conn) {
				c.Close()
			},
			MaxSize: maxSize,
		})
		if err != nil {
			for _, s := range p.subs {
				s.p.Close()
			}
			return nil, fmt.Errorf("pool: %w", err)
		}
		sub.p = pp
		p.subs = append(p.subs, sub)
	}

	if cfg.HealthCheckInterval > 0 {
		go p.healthLoop(ctx)
	}

	return p, nil
}

func (p *Pool) Acquire(ctx context.Context) (*PoolConn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, &conn.Error{Kind: conn.KindInternal, Message: "pool is closed"}
	}

	var live []*addrPool
	for _, s := range p.subs {
		if !s.dead {
			live = append(live, s)
		}
	}
	if len(live) == 0 {
		p.mu.Unlock()
		return nil, &conn.Error{Kind: conn.KindInternal, Message: "all replicas unreachable"}
	}

	idx := p.rrIdx % len(live)
	p.rrIdx = (idx + 1) % len(live)
	p.mu.Unlock()

	for i := 0; i < len(live); i++ {
		sp := live[idx]
		idx = (idx + 1) % len(live)

		res, err := sp.p.Acquire(ctx)
		if err != nil {
			if errors.Is(err, puddle.ErrClosedPool) {
				continue
			}
			return nil, &conn.Error{Kind: conn.KindInternal, Message: "acquire", Err: err}
		}

		return &PoolConn{
			Conn: res.Value(),
			addr: sp.addr,
			res:  res,
		}, nil
	}

	return nil, &conn.Error{Kind: conn.KindInternal, Message: "no available connections"}
}

func (p *Pool) Exec(ctx context.Context, query string) error {
	pc, err := p.Acquire(ctx)
	if err != nil {
		return err
	}

	err = pc.Conn.Exec(ctx, query)
	if err != nil {
		var ce *conn.Error
		if errors.As(err, &ce) && ce.Kind == conn.KindNetwork {
			pc.Close()
			pc2, err2 := p.Acquire(ctx)
			if err2 != nil {
				return err
			}
			defer pc2.Release()
			return pc2.Conn.Exec(ctx, query)
		}
		pc.Close()
		return err
	}

	pc.Release()
	return nil
}

func (p *Pool) Select(ctx context.Context, query string, cols ...conn.Column) (int, error) {
	pc, err := p.Acquire(ctx)
	if err != nil {
		return 0, err
	}

	n, err := pc.Conn.Select(ctx, query, cols...)
	if err != nil {
		var ce *conn.Error
		if errors.As(err, &ce) && ce.Kind == conn.KindNetwork {
			pc.Close()
			pc2, err2 := p.Acquire(ctx)
			if err2 != nil {
				return 0, err
			}
			defer pc2.Release()
			return pc2.Conn.Select(ctx, query, cols...)
		}
		pc.Close()
		return n, err
	}

	pc.Release()
	return n, nil
}

func (p *Pool) Insert(ctx context.Context, query string, cols ...conn.Column) error {
	pc, err := p.Acquire(ctx)
	if err != nil {
		return err
	}

	err = pc.Conn.Insert(ctx, query, cols...)
	if err != nil {
		var ce *conn.Error
		if errors.As(err, &ce) && ce.Kind == conn.KindNetwork {
			pc.Close()
			pc2, err2 := p.Acquire(ctx)
			if err2 != nil {
				return err
			}
			defer pc2.Release()
			return pc2.Conn.Insert(ctx, query, cols...)
		}
		pc.Close()
		return err
	}

	pc.Release()
	return nil
}

func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()

	close(p.stopCh)

	for _, s := range p.subs {
		s.p.Close()
	}
}

func (p *Pool) State() ConnState {
	var st ConnState
	for _, s := range p.subs {
		ps := s.p.Stat()
		st.Total += int(ps.TotalResources())
		st.Idle += int(ps.IdleResources())
		st.Active += int(ps.AcquiredResources())
	}
	return st
}

func (p *Pool) AddrStates() []AddrState {
	p.mu.Lock()
	defer p.mu.Unlock()

	states := make([]AddrState, len(p.subs))
	for i, s := range p.subs {
		ps := s.p.Stat()
		states[i] = AddrState{
			Addr: s.addr,
			ConnState: ConnState{
				Total:  int(ps.TotalResources()),
				Idle:   int(ps.IdleResources()),
				Active: int(ps.AcquiredResources()),
			},
			Dead: s.dead,
		}
	}
	return states
}

func (p *Pool) SelectStream(ctx context.Context, query string) (*conn.SelectStream, error) {
	pc, err := p.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	s, err := pc.Conn.SelectStream(ctx, query)
	if err != nil {
		pc.Release()
		return nil, err
	}

	s.SetRelease(func() {
		pc.Conn.Unlock()
		pc.Release()
	})

	return s, nil
}

func (p *Pool) InsertStream(ctx context.Context, query string) (*conn.InsertStream, error) {
	pc, err := p.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	s, err := pc.Conn.InsertStream(ctx, query)
	if err != nil {
		pc.Release()
		return nil, err
	}

	s.SetRelease(func() {
		pc.Conn.Unlock()
		pc.Release()
	})

	return s, nil
}

func (p *Pool) healthLoop(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.checkHealth(ctx)
		}
	}
}

func (p *Pool) checkHealth(ctx context.Context) {
	p.mu.Lock()
	deadList := make([]*addrPool, 0)
	for _, s := range p.subs {
		if s.dead {
			deadList = append(deadList, s)
		}
	}
	p.mu.Unlock()

	if len(deadList) == 0 {
		return
	}

	dialTimeout := p.cfg.HealthCheckTimeout
	if dialTimeout == 0 {
		dialTimeout = 5 * time.Second
	}

	for _, s := range deadList {
		subCfg := p.cfg.Config
		subCfg.Addr = s.addr

		checkCtx, cancel := context.WithTimeout(ctx, dialTimeout)
		c, err := conn.Connect(checkCtx, subCfg)
		cancel()
		if err != nil {
			continue
		}
		c.Close()

		p.mu.Lock()
		s.dead = false
		p.mu.Unlock()
	}
}
