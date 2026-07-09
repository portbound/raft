package rpc

import (
	"context"
	"portbound/raft/internal/node"
	"portbound/raft/proto"
)

type Server struct {
	proto.UnimplementedRaftServer
	node *node.Node
}

func New(n *node.Node) *Server {
	return &Server{node: n}
}

func (s *Server) AppendEntries(ctx context.Context, request *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	req := node.AppendEntriesRequest{}

	resp := s.node.AppendEntries(ctx, &req)
	return &proto.AppendEntriesResponse{
		Term:    resp.Term,
		Success: resp.Success,
	}, nil
}

func (s *Server) RequestVote(ctx context.Context, request *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	req := node.RequestVoteRequest{
		Term:         request.Term,
		LastLogIndex: request.LastLogIndex,
		LastLogTerm:  request.LastLogTerm,
		CandidateId:  request.CandidateId,
	}

	resp := s.node.RequestVote(ctx, &req)

	return &proto.RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}
