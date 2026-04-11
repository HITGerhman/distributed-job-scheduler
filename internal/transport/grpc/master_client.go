package grpc

import (
	"context"

	"djs/proto/workerpb"
)

type MasterClient struct {
	pool *ConnPool
}

func NewMasterClient(pool *ConnPool) *MasterClient {
	if pool == nil {
		pool = NewConnPool()
	}
	return &MasterClient{pool: pool}
}

func (c *MasterClient) ReportStarted(ctx context.Context, addr string, req *workerpb.ReportStartedRequest) (*workerpb.ReportStartedResponse, error) {
	conn, err := c.pool.Get(ctx, addr)
	if err != nil {
		return nil, err
	}
	return workerpb.NewMasterServiceClient(conn).ReportStarted(ctx, req)
}

func (c *MasterClient) ReportFinished(ctx context.Context, addr string, req *workerpb.ReportFinishedRequest) (*workerpb.ReportFinishedResponse, error) {
	conn, err := c.pool.Get(ctx, addr)
	if err != nil {
		return nil, err
	}
	return workerpb.NewMasterServiceClient(conn).ReportFinished(ctx, req)
}

func (c *MasterClient) ReportHeartbeat(ctx context.Context, addr string, req *workerpb.ReportHeartbeatRequest) (*workerpb.ReportHeartbeatResponse, error) {
	conn, err := c.pool.Get(ctx, addr)
	if err != nil {
		return nil, err
	}
	return workerpb.NewMasterServiceClient(conn).ReportHeartbeat(ctx, req)
}

func (c *MasterClient) CreateJob(ctx context.Context, addr string, req *workerpb.CreateJobRequest) (*workerpb.CreateJobResponse, error) {
	conn, err := c.pool.Get(ctx, addr)
	if err != nil {
		return nil, err
	}
	return workerpb.NewMasterServiceClient(conn).CreateJob(ctx, req)
}

func (c *MasterClient) KillInstance(ctx context.Context, addr string, req *workerpb.KillInstanceRequest) (*workerpb.KillInstanceResponse, error) {
	conn, err := c.pool.Get(ctx, addr)
	if err != nil {
		return nil, err
	}
	return workerpb.NewMasterServiceClient(conn).KillInstance(ctx, req)
}
