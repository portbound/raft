package transport

import (
	"context"
	"portbound/raft/internal/node"
	"portbound/raft/proto"
)

type RaftServer struct {
	proto.UnimplementedRaftServer
	node *node.Node
}

func NewServer(n *node.Node) *RaftServer {
	return &RaftServer{node: n}
}

func (s *RaftServer) AppendEntries(ctx context.Context, request *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	entries := make([]*node.LogEntry, len(request.Entries))
	for i, e := range request.Entries {
		entries[i] = &node.LogEntry{
			Index: e.Index,
			Term:  e.Term,
			Data:  e.Data,
		}
	}

	resp := s.node.HandleAppendEntries(ctx, &node.AppendEntriesReq{
		Term:         request.Term,
		LeaderId:     request.LeaderId,
		PrevLogIndex: request.PrevLogIndex,
		PrevLogTerm:  request.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: request.LeaderCommit,
	})

	return &proto.AppendEntriesResponse{
		Term:    resp.Term,
		Success: resp.Success,
	}, nil
}

func (s *RaftServer) RequestVote(ctx context.Context, request *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	resp := s.node.HandleRequestVote(ctx, &node.RequestVoteReq{
		Term:         request.Term,
		LastLogIndex: request.LastLogIndex,
		LastLogTerm:  request.LastLogTerm,
		CandidateId:  request.CandidateId,
	})

	return &proto.RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}
