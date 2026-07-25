package node

import (
	"context"
)

type role int

const (
	Leader role = iota
	Candidate
	Follower
)

type Peer interface {
	AppendEntries(ctx context.Context, req *AppendEntriesReq) (*AppendEntriesResp, error)
	RequestVote(ctx context.Context, req *RequestVoteReq) (*RequestVoteResp, error)
}

type AppendEntriesReq struct {
	Term         uint64
	LeaderId     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []*LogEntry
	LeaderCommit uint64
}

type AppendEntriesResp struct {
	Term    uint64
	Success bool
}

type appendEntriesCall struct {
	req     *AppendEntriesReq
	results chan *AppendEntriesResp
}

type RequestVoteReq struct {
	Term         uint64
	LastLogIndex uint64
	LastLogTerm  uint64
	CandidateId  string
}

type RequestVoteResp struct {
	Term        uint64
	VoteGranted bool
}

type requestVoteCall struct {
	req     *RequestVoteReq
	results chan *RequestVoteResp
}

type LogEntry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type Node struct {
	id                 string
	peers              map[string]Peer
	role               role
	currentTerm        uint64
	votedFor           string
	log                []*LogEntry
	commitIndex        uint64
	lastApplied        uint64
	requestVoteCalls   chan *requestVoteCall
	appendEntriesCalls chan *appendEntriesCall
}

func New(id string, peers map[string]Peer) *Node {
	return &Node{
		id:    id,
		peers: peers,
	}
}

func (n *Node) Submit(ctx context.Context, b []byte) error {
	return nil
}

func (n *Node) Run() {
	for {
		select {
		case call := <-n.appendEntriesCalls:
			call.results <- n.appendEntries(call.req)
		case call := <-n.requestVoteCalls:
			call.results <- n.requestVote(call.req)
		}
	}
}

func (n *Node) HandleAppendEntries(ctx context.Context, req *AppendEntriesReq) *AppendEntriesResp {
	results := make(chan *AppendEntriesResp, 1)
	n.appendEntriesCalls <- &appendEntriesCall{
		req:     req,
		results: results,
	}

	select {
	case resp := <-results:
		return resp
	case <-ctx.Done():
		return nil
	}
}

func (n *Node) HandleRequestVote(ctx context.Context, req *RequestVoteReq) *RequestVoteResp {
	results := make(chan *RequestVoteResp, 1)
	n.requestVoteCalls <- &requestVoteCall{
		req:     req,
		results: results,
	}

	select {
	case resp := <-results:
		return resp
	case <-ctx.Done():
		return nil
	}
}

func (n *Node) appendEntries(request *AppendEntriesReq) *AppendEntriesResp {
	response := AppendEntriesResp{
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

func (n *Node) requestVote(req *RequestVoteReq) *RequestVoteResp {
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.role = Follower
		n.votedFor = ""
	}

	response := RequestVoteResp{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if req.Term < n.currentTerm {
		return &response
	}

	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if n.votedFor != "" && n.votedFor != req.CandidateId {
		return &response
	}

	if len(n.log) > 0 {
		if req.LastLogTerm < n.log[len(n.log)-1].Term {
			return &response
		}

		if req.LastLogTerm == n.log[len(n.log)-1].Term {
			if req.LastLogIndex < n.log[len(n.log)-1].Index {
				return &response
			}

		}
	}

	n.votedFor = req.CandidateId
	response.VoteGranted = true

	return &response
}
