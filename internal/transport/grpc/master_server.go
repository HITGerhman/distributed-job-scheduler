package grpc

import (
	"net"

	"djs/proto/workerpb"
)

type MasterServer struct {
	server interface {
		Serve(net.Listener) error
		GracefulStop()
	}
}

func NewMasterServer(handler workerpb.MasterServiceServer) *MasterServer {
	srv := NewServer()
	workerpb.RegisterMasterServiceServer(srv, handler)
	return &MasterServer{server: srv}
}

func (s *MasterServer) Serve(listener net.Listener) error {
	return s.server.Serve(listener)
}

func (s *MasterServer) GracefulStop() {
	s.server.GracefulStop()
}
