package main

import (
	"context"
	"portbound/raft/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Role int

const (
	Leader Role = iota
	Candidate
	Follower
)

type Node struct {
	proto.UnimplementedRaftServer
	id          string
	addr        string
	server      *grpc.Server
	peers       map[string]proto.RaftClient
	role        Role
	currentTerm uint64
	votedFor    string
	log         []*proto.LogEntry
	commitIndex uint64
	lastApplied uint64
}

func NewNode(id, addr string, peers map[string]string) (*Node, error) {
	n := &Node{
		id:    id,
		addr:  addr,
		peers: map[string]proto.RaftClient{},
	}

	for id, addr := range peers {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		n.peers[id] = proto.NewRaftClient(conn)
	}

	n.server = grpc.NewServer()
	proto.RegisterRaftServer(n.server, n)

	return n, nil
}

func (n *Node) AppendEntries(ctx context.Context, request *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	response := proto.AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: false,
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if request.Term < n.currentTerm {
		return &response, nil
	}

	// 2. Reply false if n.log doesn’t contain an entry at request.PrevLogIndex whose term matches request.PrevLogTerm (§5.3)
	if request.PrevLogIndex > 0 {
		if int(request.PrevLogIndex) > len(n.log) {
			return &response, nil
		}

		prevEntry := n.log[request.PrevLogIndex-1]
		if prevEntry.Term != request.PrevLogTerm {
			return &response, nil
		}
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	var newEntries []*proto.LogEntry
	for i, e := range request.Entries {
		if int(e.Index) > len(n.log) { // we know that this must be +=1 from the last entry in n.log because of the 2nd clause
			newEntries = request.Entries[i:]
			break
		}

		if e.Term != n.log[e.Index-1].Term {
			n.log = n.log[:e.Index-1]
			newEntries = request.Entries[i:]
			break
		}
	}

	// 4. Append any new entries not already in the log
	n.log = append(n.log, newEntries...)

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if request.LeaderCommit > n.commitIndex {
		lastIndex := uint64(len(n.log))
		if len(request.Entries) > 0 {
			lastIndex = request.Entries[len(request.Entries)-1].Index
		}
		n.commitIndex = min(request.LeaderCommit, lastIndex)
	}

	response.Success = true
	return &response, nil
}

func (n *Node) RequestVote(ctx context.Context, request *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	return &proto.RequestVoteResponse{}, nil
}
