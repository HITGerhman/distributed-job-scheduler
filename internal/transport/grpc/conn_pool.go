package grpc

import (
	"context"
	"sync"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ConnPool struct {
	mu    sync.Mutex
	conns map[string]*grpcgo.ClientConn
}

func NewConnPool() *ConnPool {
	RegisterJSONCodec()
	return &ConnPool{conns: make(map[string]*grpcgo.ClientConn)}
}

func (p *ConnPool) Get(ctx context.Context, addr string) (*grpcgo.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, ok := p.conns[addr]; ok {
		return conn, nil
	}

	conn, err := grpcgo.DialContext(
		ctx,
		addr,
		grpcgo.WithTransportCredentials(insecure.NewCredentials()),
		grpcgo.WithDefaultCallOptions(grpcgo.CallContentSubtype(JSONCodecName)),
		grpcgo.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, err
	}
	p.conns[addr] = conn
	return conn, nil
}

func (p *ConnPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var closeErr error
	for addr, conn := range p.conns {
		if err := conn.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		delete(p.conns, addr)
	}
	return closeErr
}
