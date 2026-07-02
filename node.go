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
	if n.currentTerm < request.Term {
		return &response, nil
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if request.PrevLogIndex > 0 { // check to ensure that this is not the first entry
		if int(request.PrevLogIndex) > len(n.log) { // bounds checking
			return &response, nil
		}

		entry := n.log[request.PrevLogIndex-1]
		if entry.Term != request.PrevLogTerm {
			return &response, nil
		}

		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		for _, entry := range request.Entries {
			if n.log[entry.Index-1].Term != entry.Term {
				n.log = n.log[:entry.Index-1]
				break
			}
		}
	}

	// 4. Append any new entries not already in the log
	n.log = append(n.log, request.Entries...)

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	response.Success = true
	return &response, nil
}
func (n *Node) RequestVote(ctx context.Context, request *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error)
