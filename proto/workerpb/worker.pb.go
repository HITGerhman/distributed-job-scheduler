package workerpb

import (
	"context"

	"google.golang.org/grpc"
)

type DispatchTaskRequest struct {
	InstanceId     uint64 `json:"instance_id"`
	AttemptNo      uint32 `json:"attempt_no"`
	WorkerId       string `json:"worker_id"`
	JobId          uint64 `json:"job_id"`
	JobName        string `json:"job_name"`
	Payload        []byte `json:"payload"`
	TimeoutSeconds uint32 `json:"timeout_seconds"`
}

type DispatchTaskResponse struct {
	Accepted bool   `json:"accepted"`
	Message  string `json:"message,omitempty"`
}

type KillTaskRequest struct {
	InstanceId uint64 `json:"instance_id"`
	AttemptNo  uint32 `json:"attempt_no"`
	Reason     string `json:"reason,omitempty"`
}

type KillTaskResponse struct {
	Accepted        bool   `json:"accepted"`
	AlreadyFinished bool   `json:"already_finished"`
	Message         string `json:"message,omitempty"`
}

type ReportStartedRequest struct {
	WorkerId        string `json:"worker_id"`
	InstanceId      uint64 `json:"instance_id"`
	AttemptNo       uint32 `json:"attempt_no"`
	StartedAtUnixMs int64  `json:"started_at_unix_ms"`
}

type ReportStartedResponse struct {
	Accepted bool   `json:"accepted"`
	Message  string `json:"message,omitempty"`
}

type ReportFinishedRequest struct {
	WorkerId         string `json:"worker_id"`
	InstanceId       uint64 `json:"instance_id"`
	AttemptNo        uint32 `json:"attempt_no"`
	Status           string `json:"status"`
	FinishedAtUnixMs int64  `json:"finished_at_unix_ms"`
	ExitCode         int32  `json:"exit_code"`
	ErrorMessage     string `json:"error_message,omitempty"`
	ResultSummary    []byte `json:"result_summary,omitempty"`
}

type ReportFinishedResponse struct {
	Accepted bool   `json:"accepted"`
	Message  string `json:"message,omitempty"`
}

type ReportHeartbeatRequest struct {
	WorkerId          string `json:"worker_id"`
	InstanceId        uint64 `json:"instance_id"`
	AttemptNo         uint32 `json:"attempt_no"`
	HeartbeatAtUnixMs int64  `json:"heartbeat_at_unix_ms"`
}

type ReportHeartbeatResponse struct {
	Accepted bool   `json:"accepted"`
	Message  string `json:"message,omitempty"`
}

type CreateJobRequest struct {
	Name                string `json:"name"`
	CronExpr            string `json:"cron_expr"`
	Timezone            string `json:"timezone"`
	Payload             []byte `json:"payload"`
	TimeoutSeconds      uint32 `json:"timeout_seconds"`
	MaxRetries          uint32 `json:"max_retries"`
	RetryBackoffSeconds uint32 `json:"retry_backoff_seconds"`
	AllowConcurrent     bool   `json:"allow_concurrent"`
	Status              string `json:"status"`
}

type CreateJobResponse struct {
	JobId uint64 `json:"job_id"`
}

type KillInstanceRequest struct {
	InstanceId uint64 `json:"instance_id"`
	Reason     string `json:"reason,omitempty"`
}

type KillInstanceResponse struct {
	Accepted bool   `json:"accepted"`
	Message  string `json:"message,omitempty"`
}

type WorkerServiceClient interface {
	DispatchTask(ctx context.Context, in *DispatchTaskRequest, opts ...grpc.CallOption) (*DispatchTaskResponse, error)
	KillTask(ctx context.Context, in *KillTaskRequest, opts ...grpc.CallOption) (*KillTaskResponse, error)
}

type workerServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewWorkerServiceClient(cc grpc.ClientConnInterface) WorkerServiceClient {
	return &workerServiceClient{cc: cc}
}

func (c *workerServiceClient) DispatchTask(ctx context.Context, in *DispatchTaskRequest, opts ...grpc.CallOption) (*DispatchTaskResponse, error) {
	out := new(DispatchTaskResponse)
	if err := c.cc.Invoke(ctx, "/djs.worker.v1.WorkerService/DispatchTask", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *workerServiceClient) KillTask(ctx context.Context, in *KillTaskRequest, opts ...grpc.CallOption) (*KillTaskResponse, error) {
	out := new(KillTaskResponse)
	if err := c.cc.Invoke(ctx, "/djs.worker.v1.WorkerService/KillTask", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

type WorkerServiceServer interface {
	DispatchTask(context.Context, *DispatchTaskRequest) (*DispatchTaskResponse, error)
	KillTask(context.Context, *KillTaskRequest) (*KillTaskResponse, error)
}

func RegisterWorkerServiceServer(s grpc.ServiceRegistrar, srv WorkerServiceServer) {
	s.RegisterService(&WorkerService_ServiceDesc, srv)
}

var WorkerService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "djs.worker.v1.WorkerService",
	HandlerType: (*WorkerServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "DispatchTask", Handler: _WorkerService_DispatchTask_Handler},
		{MethodName: "KillTask", Handler: _WorkerService_KillTask_Handler},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/worker.proto",
}

func _WorkerService_DispatchTask_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DispatchTaskRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerServiceServer).DispatchTask(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/djs.worker.v1.WorkerService/DispatchTask"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerServiceServer).DispatchTask(ctx, req.(*DispatchTaskRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WorkerService_KillTask_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(KillTaskRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerServiceServer).KillTask(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/djs.worker.v1.WorkerService/KillTask"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerServiceServer).KillTask(ctx, req.(*KillTaskRequest))
	}
	return interceptor(ctx, in, info, handler)
}

type MasterServiceClient interface {
	ReportStarted(ctx context.Context, in *ReportStartedRequest, opts ...grpc.CallOption) (*ReportStartedResponse, error)
	ReportFinished(ctx context.Context, in *ReportFinishedRequest, opts ...grpc.CallOption) (*ReportFinishedResponse, error)
	ReportHeartbeat(ctx context.Context, in *ReportHeartbeatRequest, opts ...grpc.CallOption) (*ReportHeartbeatResponse, error)
	CreateJob(ctx context.Context, in *CreateJobRequest, opts ...grpc.CallOption) (*CreateJobResponse, error)
	KillInstance(ctx context.Context, in *KillInstanceRequest, opts ...grpc.CallOption) (*KillInstanceResponse, error)
}

type masterServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewMasterServiceClient(cc grpc.ClientConnInterface) MasterServiceClient {
	return &masterServiceClient{cc: cc}
}

func (c *masterServiceClient) ReportStarted(ctx context.Context, in *ReportStartedRequest, opts ...grpc.CallOption) (*ReportStartedResponse, error) {
	out := new(ReportStartedResponse)
	if err := c.cc.Invoke(ctx, "/djs.worker.v1.MasterService/ReportStarted", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *masterServiceClient) ReportFinished(ctx context.Context, in *ReportFinishedRequest, opts ...grpc.CallOption) (*ReportFinishedResponse, error) {
	out := new(ReportFinishedResponse)
	if err := c.cc.Invoke(ctx, "/djs.worker.v1.MasterService/ReportFinished", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *masterServiceClient) ReportHeartbeat(ctx context.Context, in *ReportHeartbeatRequest, opts ...grpc.CallOption) (*ReportHeartbeatResponse, error) {
	out := new(ReportHeartbeatResponse)
	if err := c.cc.Invoke(ctx, "/djs.worker.v1.MasterService/ReportHeartbeat", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *masterServiceClient) CreateJob(ctx context.Context, in *CreateJobRequest, opts ...grpc.CallOption) (*CreateJobResponse, error) {
	out := new(CreateJobResponse)
	if err := c.cc.Invoke(ctx, "/djs.worker.v1.MasterService/CreateJob", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *masterServiceClient) KillInstance(ctx context.Context, in *KillInstanceRequest, opts ...grpc.CallOption) (*KillInstanceResponse, error) {
	out := new(KillInstanceResponse)
	if err := c.cc.Invoke(ctx, "/djs.worker.v1.MasterService/KillInstance", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

type MasterServiceServer interface {
	ReportStarted(context.Context, *ReportStartedRequest) (*ReportStartedResponse, error)
	ReportFinished(context.Context, *ReportFinishedRequest) (*ReportFinishedResponse, error)
	ReportHeartbeat(context.Context, *ReportHeartbeatRequest) (*ReportHeartbeatResponse, error)
	CreateJob(context.Context, *CreateJobRequest) (*CreateJobResponse, error)
	KillInstance(context.Context, *KillInstanceRequest) (*KillInstanceResponse, error)
}

func RegisterMasterServiceServer(s grpc.ServiceRegistrar, srv MasterServiceServer) {
	s.RegisterService(&MasterService_ServiceDesc, srv)
}

var MasterService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "djs.worker.v1.MasterService",
	HandlerType: (*MasterServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "ReportStarted", Handler: _MasterService_ReportStarted_Handler},
		{MethodName: "ReportFinished", Handler: _MasterService_ReportFinished_Handler},
		{MethodName: "ReportHeartbeat", Handler: _MasterService_ReportHeartbeat_Handler},
		{MethodName: "CreateJob", Handler: _MasterService_CreateJob_Handler},
		{MethodName: "KillInstance", Handler: _MasterService_KillInstance_Handler},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/worker.proto",
}

func _MasterService_ReportStarted_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReportStartedRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MasterServiceServer).ReportStarted(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/djs.worker.v1.MasterService/ReportStarted"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MasterServiceServer).ReportStarted(ctx, req.(*ReportStartedRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MasterService_ReportFinished_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReportFinishedRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MasterServiceServer).ReportFinished(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/djs.worker.v1.MasterService/ReportFinished"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MasterServiceServer).ReportFinished(ctx, req.(*ReportFinishedRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MasterService_ReportHeartbeat_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReportHeartbeatRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MasterServiceServer).ReportHeartbeat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/djs.worker.v1.MasterService/ReportHeartbeat"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MasterServiceServer).ReportHeartbeat(ctx, req.(*ReportHeartbeatRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MasterService_CreateJob_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateJobRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MasterServiceServer).CreateJob(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/djs.worker.v1.MasterService/CreateJob"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MasterServiceServer).CreateJob(ctx, req.(*CreateJobRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MasterService_KillInstance_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(KillInstanceRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MasterServiceServer).KillInstance(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/djs.worker.v1.MasterService/KillInstance"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MasterServiceServer).KillInstance(ctx, req.(*KillInstanceRequest))
	}
	return interceptor(ctx, in, info, handler)
}
