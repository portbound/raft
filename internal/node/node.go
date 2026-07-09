package node

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

type AppendEntriesRequest struct {
	Term         uint64
	LeaderId     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []*LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

type RequestVoteRequest struct {
	Term         uint64
	LastLogIndex uint64
	LastLogTerm  uint64
	CandidateId  string
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type LogEntry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type Node struct {
	id          string
	addr        string
	peers       map[string]proto.RaftClient
	role        Role
	currentTerm uint64
	votedFor    string
	log         []*LogEntry
	commitIndex uint64
	lastApplied uint64
}

func New(id, addr string, peers map[string]string) (*Node, error) {
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

	return n, nil
}

func (n *Node) AppendEntries(ctx context.Context, request *AppendEntriesRequest) *AppendEntriesResponse {
	response := AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: false,
	}
	// 1. Reply false if term < currentTerm (§5.1)
	if request.Term < n.currentTerm {
		return &response
	}

	// 2. Reply false if n.log doesn’t contain an entry at request.PrevLogIndex whose term matches request.PrevLogTerm (§5.3)
	if request.PrevLogIndex > 0 {
		if int(request.PrevLogIndex) > len(n.log) {
			return &response
		}

		prevEntry := n.log[request.PrevLogIndex-1]
		if prevEntry.Term != request.PrevLogTerm {
			return &response
		}
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	var newEntries []*LogEntry
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

	return &response
}

func (n *Node) RequestVote(ctx context.Context, request *RequestVoteRequest) *RequestVoteResponse {

	// probably want to extract to a function to "clear state"
	if request.Term > n.currentTerm {
		// need to aquire a mu or send these on a channel like nodeState since AppendEntries will also need to update these values
		n.currentTerm = request.Term
		n.role = Follower
		n.votedFor = ""
	}

	response := RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if request.Term < n.currentTerm {
		return &response
	}

	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if n.votedFor != "" && n.votedFor != request.CandidateId {
		return &response
	}

	if len(n.log) > 0 {
		if request.LastLogTerm < n.log[len(n.log)-1].Term {
			return &response
		}

		if request.LastLogTerm == n.log[len(n.log)-1].Term {
			if request.LastLogIndex < n.log[len(n.log)-1].Index {
				return &response
			}

		}
	}

	n.votedFor = request.CandidateId
	response.VoteGranted = true

	return &response
}
