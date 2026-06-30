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
	log         []proto.LogEntry
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

func (n *Node) AppendEntries(ctx context.Context, request *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error)
func (n *Node) RequestVote(ctx context.Context, request *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error)
