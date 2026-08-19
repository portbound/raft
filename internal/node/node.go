package node

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"
)

type role int

const (
	Follower role = iota + 1
	Candidate
	Leader
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
	recv  *AppendEntriesReq
	reply chan *AppendEntriesResp
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
	recv  *RequestVoteReq
	reply chan *RequestVoteResp
}

type LogEntry struct {
	Index uint64
	Term  uint64
	Cmd   []byte
}

type submission struct {
	cmd     []byte
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
	mu                 sync.Mutex
	heartBeatCtx       context.Context
	heartBeatCancel    context.CancelFunc
	heartbeats         chan struct{}
	electionCtx        context.Context
	electionCancel     context.CancelFunc
	electionWon        chan uint64
	requestVoteCalls   chan *requestVoteCall
	appendEntriesCalls chan *appendEntriesCall
}

// New initializes a new Node instance.
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

// Run contains the main Raft event loop. It blocks, listening on various channels to drive role transitions and maintain consensus.
func (n *Node) Run() {
	n.roleTransition(Follower)
	for {
		select {
		case call := <-n.appendEntriesCalls:
			n.checkStaleTerm(call.recv.Term)
			resp := n.appendEntries(call.recv)
			if resp.Success {
				n.roleTransition(Follower)
			}
			call.reply <- resp
		case call := <-n.requestVoteCalls:
			n.checkStaleTerm(call.recv.Term)
			resp := n.requestVote(call.recv)
			if resp.VoteGranted {
				n.roleTransition(Follower)
			}
			call.reply <- resp
		case <-n.electionTimer.C:
			n.roleTransition(Candidate)
		case term := <-n.electionWon:
			if n.role == Candidate && n.currentTerm == term {
				n.roleTransition(Leader)
			}
		case <-n.heartbeats:
			n.sendHeartbeat()
		}
	}
}

func (n *Node) checkStaleTerm(term uint64) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		n.roleTransition(Follower)
	}
}

// HandleRequestVote processes incoming AppendEntries RPCs. Waits for a valid reply, or cancels on ctx.Done().
func (n *Node) HandleAppendEntries(ctx context.Context, req *AppendEntriesReq) *AppendEntriesResp {
	resp := make(chan *AppendEntriesResp, 1)
	n.appendEntriesCalls <- &appendEntriesCall{
		recv:  req,
		reply: resp,
	}

	select {
	case reply := <-resp:
		return reply
	case <-ctx.Done():
		return nil
	}
}

// HandleRequestVote processes incoming RequestVote RPCs. Waits for a valid reply, or cancels on ctx.Done().
func (n *Node) HandleRequestVote(ctx context.Context, req *RequestVoteReq) *RequestVoteResp {
	resp := make(chan *RequestVoteResp, 1)
	n.requestVoteCalls <- &requestVoteCall{
		recv:  req,
		reply: resp,
	}

	select {
	case reply := <-resp:
		return reply
	case <-ctx.Done():
		return nil
	}
}

// appendEntries implements Raft's core append-entries logic.
func (n *Node) appendEntries(req *AppendEntriesReq) *AppendEntriesResp {
	response := &AppendEntriesResp{
		Term:    n.currentTerm,
		Success: false,
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if req.Term < n.currentTerm {
		return response
	}

	// 2. Reply false if n.log doesn’t contain an entry at request.PrevLogIndex whose term matches request.PrevLogTerm (§5.3)
	if req.PrevLogIndex > 0 {
		if int(req.PrevLogIndex) > len(n.log) {
			return response
		}

		if n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			return response
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

	return response
}

// requestVote implements Raft's core vote-granting logic.
func (n *Node) requestVote(req *RequestVoteReq) *RequestVoteResp {
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

// startElection initiates a new election cycle.
func (n *Node) startElection() {
	// TODO not sure if we want to handle this inside the roleTransition()
	n.currentTerm++
	n.votedFor = n.id

	req := &RequestVoteReq{
		Term:        n.currentTerm,
		CandidateId: n.id,
	}

	if len(n.log) > 0 {
		req.LastLogIndex = n.log[len(n.log)-1].Index
		req.Term = n.log[len(n.log)-1].Term
	}

	ctx, cancel := context.WithCancel(context.Background())
	n.electionCtx = ctx
	n.electionCancel = cancel

	peers := map[string]Peer{}
	for id, peer := range n.cluster {
		if id != n.id {
			peers[id] = peer
		}
	}

	responses := make(chan *RequestVoteResp, len(peers))

	for id, peer := range peers {
		go func(id string, peer Peer) {
			resp, err := peer.RequestVote(n.electionCtx, req)
			if err != nil {
				// TODO not sure how to handle this error - not going to return, but maybe logging is worth?
				resp = &RequestVoteResp{VoteGranted: false}
			}
			responses <- resp
		}(id, peer)
	}

	// need to reset the election timer before we begin processing results
	n.resetElectionTimer()
	go func(term uint64) {
		votesGranted := 1
		for range len(peers) {
			select {
			case resp := <-responses:
				if resp.VoteGranted && resp.Term == term {
					votesGranted++
					if votesGranted >= (len(n.cluster)/2)+1 {
						n.electionWon <- term
						return
					}
				}
			case <-n.electionCtx.Done():
				return
			}
		}
	}(n.currentTerm)
}

// resetElectionTimer sets or resets the randomized timer used for measuring Follower timeout. It is also used at the start of a Candidate's election.
func (n *Node) resetElectionTimer() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}

	n.electionTimer = time.NewTimer(time.Duration(rand.IntN(300-150+1)+150) * time.Millisecond)
}

// stopElection cancels any ongoing election and cleans up related state.
func (n *Node) stopElection() {
	if n.electionCtx != nil {
		n.electionCancel()
		n.electionCancel = nil
		n.electionCtx = nil
	}
}

// startHeartbeats begins the periodic sending of heartbeats when a node is elected as Leader.
func (n *Node) startHeartbeats() {
	ctx, cancel := context.WithCancel(context.Background())
	n.heartBeatCtx = ctx
	n.heartBeatCancel = cancel

	ticker := time.NewTicker(50 * time.Millisecond) // TODO not sure 50 ms is right
	go func() {
		for {
			select {
			case <-ticker.C:
				<-n.heartbeats
			case <-n.heartBeatCtx.Done():
				return
			}
		}
	}()
}

// stopHeartbeats stops any ongoing heartBeats and cleans up related state.
func (n *Node) stopHeartbeats() {
	if n.heartBeatCtx != nil {
		n.heartBeatCancel()
		n.heartBeatCancel = nil
		n.heartBeatCtx = nil
	}
}

// sendHeartbeat sends empty AppendEntries requests to all peers. This is the primary mechanism for maintaining leadership and replicating state across the cluster.
func (n *Node) sendHeartbeat() {
	req := &AppendEntriesReq{
		Term:         n.currentTerm,
		LeaderId:     n.id,
		Entries:      []*LogEntry{},
		LeaderCommit: n.commitIndex,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	}

	if len(n.log) > 0 {
		req.PrevLogIndex = n.log[len(n.log)-1].Index
		req.PrevLogTerm = n.log[len(n.log)-1].Term
	}

	for id, node := range n.cluster {
		if id != n.id {
			node.AppendEntries(n.heartBeatCtx, req) // TODO not sure if this needs to carry a separate ctx
		}
	}
}

// roleTransition handles deterministic state change, and cleanup when Node role is updated.
func (n *Node) roleTransition(newRole role) {
	currentRole := n.role

	switch currentRole {
	case Follower: // TODO nodes start in follower, not sure we need to do anything here
	case Candidate:
		n.stopElection()
	case Leader:
		n.stopHeartbeats()
	}

	n.role = newRole

	switch newRole {
	case Follower:
		n.resetElectionTimer()
	case Candidate:
		n.startElection()
	case Leader:
		n.startHeartbeats()
	}
}
