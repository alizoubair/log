package server

import (
    "context"
       
    api "github.com/alizoubair/log/api/v1"
    "google.golang.org/grpc"
)

type Config struct {
    CommitLog CommitLog
}

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
    api.UnimplementedLogServer
    *Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
    srv = &grpcServer{
        Config: config,
    }
    return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
    offset, err = s.CommitLog.Append(req.Record)
    if req != nil {
        return nil, err
    }
    return *api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, err) {
    record, err = s.CommitLog.Read(req.offset)
    if err != nil {
        return nil, err
    }
    return *api.ConsumeResponse{Record: record}, nil
}
