package transport

import (
	"context"
	"portbound/raft/internal/node"
	"portbound/raft/proto"
)

type peer struct {
	client proto.RaftClient
}

func NewPeer(client proto.RaftClient) node.Peer {
	return &peer{client: client}
}

func (p *peer) AppendEntries(ctx context.Context, req *node.AppendEntriesReq) (*node.AppendEntriesResp, error) {
	entries := make([]*proto.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = &proto.LogEntry{
			Index: e.Index,
			Term:  e.Term,
			Cmd:   e.Cmd,
		}
	}

	resp, err := p.client.AppendEntries(ctx, &proto.AppendEntriesRequest{
		Term:         req.Term,
		LeaderId:     req.LeaderId,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	})
	if err != nil {
		return nil, err
	}

	return &node.AppendEntriesResp{
		Term:    resp.Term,
		Success: resp.Success,
	}, nil
}

func (p *peer) RequestVote(ctx context.Context, req *node.RequestVoteReq) (*node.RequestVoteResp, error) {
	resp, err := p.client.RequestVote(ctx, &proto.RequestVoteRequest{
		Term:         req.Term,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
		CandidateId:  req.CandidateId,
	})
	if err != nil {
		return nil, err
	}

	return &node.RequestVoteResp{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}
