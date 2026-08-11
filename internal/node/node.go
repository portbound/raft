package node

import (
	"context"
	"math/rand/v2"
	"time"
)

type role int

const (
	Follower role = iota + 1
	Candidate
	Leader
)

const MaxRPCTimeout = 300 * time.Millisecond

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

type submission struct {
	payload []byte
	results chan error
}

type Node struct {
	id                 string
	cluster            map[string]Peer
	role               role
	currentTerm        uint64
	votedFor           string
	electionTimer      *time.Timer
	log                []*LogEntry
	commitIndex        uint64
	lastApplied        uint64
	requestVoteCalls   chan *requestVoteCall
	appendEntriesCalls chan *appendEntriesCall
	electionResults    chan uint64
}

func New(id string, cluster map[string]Peer) *Node {
	return &Node{
		id:          id,
		cluster:     cluster,
		role:        Follower,
		currentTerm: 0,
		log:         []*LogEntry{},
	}
}

func (n *Node) Submit(ctx context.Context, b []byte) error {
	return nil
}

func (n *Node) Run() {
	n.electionTimer = newElectionTimer()
	for {
		select {
		case call := <-n.appendEntriesCalls:
			call.results <- n.appendEntries(call.req)
		case call := <-n.requestVoteCalls:
			call.results <- n.requestVote(call.req)
		case <-n.electionTimer.C:
			n.beginElection()
		case term := <-n.electionResults:
			if n.role == Candidate && n.currentTerm == term {
				// TODO go lead
			}
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
	case res := <-results:
		return res
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

func (n *Node) appendEntries(req *AppendEntriesReq) *AppendEntriesResp {
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.role = Follower
		n.votedFor = ""
	}

	response := AppendEntriesResp{
		Term:    n.currentTerm,
		Success: false,
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if req.Term < n.currentTerm {
		return &response
	}

	// 2. Reply false if n.log doesn’t contain an entry at request.PrevLogIndex whose term matches request.PrevLogTerm (§5.3)
	if req.PrevLogIndex > 0 {
		if int(req.PrevLogIndex) > len(n.log) {
			return &response
		}

		prevEntry := n.log[req.PrevLogIndex-1]
		if prevEntry.Term != req.PrevLogTerm {
			return &response
		}
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	var newEntries []*LogEntry
	for i, e := range req.Entries {
		if int(e.Index) > len(n.log) { // we know that this must be +=1 from the last entry in n.log because of the 2nd clause
			newEntries = req.Entries[i:]
			break
		}

		if e.Term != n.log[e.Index-1].Term {
			n.log = n.log[:e.Index-1]
			newEntries = req.Entries[i:]
			break
		}
	}

	// 4. Append any new entries not already in the log
	n.log = append(n.log, newEntries...)

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if req.LeaderCommit > n.commitIndex {
		lastIndex := uint64(len(n.log))
		if len(req.Entries) > 0 {
			lastIndex = req.Entries[len(req.Entries)-1].Index
		}
		n.commitIndex = min(req.LeaderCommit, lastIndex)
	}

	response.Success = true
	n.electionTimer = newElectionTimer() // TODO, I'm not sure if appendEntries() should be responsible for resetting the timer... but if we wait to reset outside of the function, we will need to interrogate the response to determine whether or not it was from a leader or not

	return &response
}

func (n *Node) requestVote(req *RequestVoteReq) *RequestVoteResp {
	response := RequestVoteResp{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if req.Term < n.currentTerm {
		return &response
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.role = Follower
		n.votedFor = ""
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

// A Node begins an election by incrementing it's current term, updating the it's role to Candidate, and voting for itself.
// Next, the Node sends RequestVote RPCs to each of it's peers in the cluster before creating a separate Go routine processes the incoming RequestVoteResp's.
// The Go routine notifies the orchestrator if an election has been won.
func (n *Node) beginElection() {
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id

	peers := map[string]Peer{}
	for id, peer := range n.cluster {
		if id != n.id {
			peers[id] = peer
		}
	}

	req := &RequestVoteReq{
		Term:        n.currentTerm,
		CandidateId: n.id,
	}

	if len(n.log) > 0 {
		req.LastLogIndex = n.log[len(n.log)-1].Index
		req.Term = n.log[len(n.log)-1].Term
	}

	responses := make(chan *RequestVoteResp, len(peers))

	for id, peer := range peers {
		go func(id string, peer Peer) {
			ctx, cancel := context.WithTimeout(context.Background(), MaxRPCTimeout) // TODO revisit the ctx timeout here
			defer cancel()

			resp, err := peer.RequestVote(ctx, req)
			if err != nil {
				// TODO not sure how to handle this error - not going to return, but maybe logging is worth?
				resp = &RequestVoteResp{VoteGranted: false}
			}
			responses <- resp
		}(id, peer)
	}

	go func(term uint64) {
		var votesGranted int

		for range len(peers) {
			select {
			case resp := <-responses:
				if resp.VoteGranted && resp.Term == term {
					votesGranted++
					if votesGranted >= (len(n.cluster)/2)+1 {
						n.electionResults <- term
						return
					}
				}
			case <-time.After(MaxRPCTimeout):
				return
			}
		}
	}(n.currentTerm)
}

func newElectionTimer() *time.Timer {
	return time.NewTimer(time.Duration(rand.IntN(300-150+1)+150) * time.Millisecond)
}
