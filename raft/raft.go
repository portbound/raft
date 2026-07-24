package raft

import (
	"context"
	"net"
	"portbound/raft/internal/node"
	"portbound/raft/internal/transport/rpc"
	"portbound/raft/proto"

	"google.golang.org/grpc"
)

type Raft struct {
	node    *node.Node
	grpcSrv *grpc.Server
	lis     net.Listener
}

func New(id, addr string, peers map[string]string) (*Raft, error) {
	n, err := node.New(id, addr, peers)
	if err != nil {
		return nil, err
	}

	grpcSrv := grpc.NewServer()
	raftSrv := rpc.New(n)
	proto.RegisterRaftServer(grpcSrv, raftSrv)

	return &Raft{node: n, grpcSrv: grpcSrv}, nil
}

func (r *Raft) Run() error {
	lis, err := net.Listen("tcp", r.node.Addr)
	if err != nil {
		return err
	}

	go r.node.Run()

	return r.grpcSrv.Serve(lis)
}

func (r *Raft) Submit(ctx context.Context, b []byte) error {
	return r.node.Submit(ctx, b)
}
