package raft

import (
	"context"
	"net"
	"portbound/raft/internal/node"
	"portbound/raft/internal/transport"
	"portbound/raft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Raft struct {
	addr    string
	node    *node.Node
	grpcSrv *grpc.Server
}

func New(nodeId, listenAddr string, peers map[string]string) (*Raft, error) {
	cluster := map[string]node.Peer{}

	for id, addr := range peers {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		cluster[id] = transport.NewPeer(proto.NewRaftClient(conn))
	}

	n := node.New(nodeId, cluster)
	raftSrv := transport.NewServer(n)

	grpcSrv := grpc.NewServer()
	proto.RegisterRaftServer(grpcSrv, raftSrv)

	return &Raft{
		addr:    listenAddr,
		node:    n,
		grpcSrv: grpcSrv}, nil
}

func (r *Raft) Serve() error {
	lis, err := net.Listen("tcp", r.addr)
	if err != nil {
		return err
	}

	go r.node.Run()

	return r.grpcSrv.Serve(lis)
}

func (r *Raft) Submit(ctx context.Context, b []byte) error {
	return r.node.Submit(ctx, b)
}
