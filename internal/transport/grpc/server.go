package grpc

import (
	"net"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	grpcgo "google.golang.org/grpc"
)

func NewServer() *grpcgo.Server {
	RegisterJSONCodec()
	return grpcgo.NewServer(
		grpcgo.StatsHandler(otelgrpc.NewServerHandler()),
	)
}

func Listen(addr string) (net.Listener, error) {
	return net.Listen("tcp", addr)
}
