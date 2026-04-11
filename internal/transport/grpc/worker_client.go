package grpc

import (
	"context"

	"djs/proto/workerpb"
)

type WorkerClient struct {
	pool *ConnPool
}

func NewWorkerClient(pool *ConnPool) *WorkerClient {
	if pool == nil {
		pool = NewConnPool()
	}
	return &WorkerClient{pool: pool}
}

func (c *WorkerClient) DispatchTask(ctx context.Context, addr string, req *workerpb.DispatchTaskRequest) (*workerpb.DispatchTaskResponse, error) {
	conn, err := c.pool.Get(ctx, addr)
	if err != nil {
		return nil, err
	}
	return workerpb.NewWorkerServiceClient(conn).DispatchTask(ctx, req)
}

func (c *WorkerClient) KillTask(ctx context.Context, addr string, req *workerpb.KillTaskRequest) (*workerpb.KillTaskResponse, error) {
	conn, err := c.pool.Get(ctx, addr)
	if err != nil {
		return nil, err
	}
	return workerpb.NewWorkerServiceClient(conn).KillTask(ctx, req)
}
