package grpc

import (
	"net"

	"djs/proto/workerpb"
)

type WorkerServer struct {
	server interface {
		Serve(net.Listener) error
		GracefulStop()
	}
}

func NewWorkerServer(handler workerpb.WorkerServiceServer) *WorkerServer {
	srv := NewServer()
	workerpb.RegisterWorkerServiceServer(srv, handler)
	return &WorkerServer{server: srv}
}

func (s *WorkerServer) Serve(listener net.Listener) error {
	return s.server.Serve(listener)
}

func (s *WorkerServer) GracefulStop() {
	s.server.GracefulStop()
}
