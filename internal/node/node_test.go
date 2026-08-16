package node

import (
	"context"
	"reflect"
	"testing"
)

type mockPeer struct {
	id     string
	rvResp *RequestVoteResp
	aeResp *AppendEntriesResp
}

func (p *mockPeer) AppendEntries(ctx context.Context, req *AppendEntriesReq) (*AppendEntriesResp, error) {
	return p.aeResp, nil
}

func (p *mockPeer) RequestVote(ctx context.Context, req *RequestVoteReq) (*RequestVoteResp, error) {
	return p.rvResp, nil
}

func TestNode_appendEntries(t *testing.T) {
	tests := []struct {
		name          string
		initialLog    []*LogEntry
		initialTerm   uint64
		initialCommit uint64
		request       *AppendEntriesReq
		wantLog       []*LogEntry
		wantCommit    uint64
		wantSuccess   bool
		wantErr       bool
	}{
		{
			name: "heartbeat with no new entries advances commitIndex",
			initialLog: []*LogEntry{
				{
					Index: 1,
					Term:  1,
					Data:  []byte{},
				},
			},
			initialTerm:   0,
			initialCommit: 0,
			request: &AppendEntriesReq{
				Term:         1,
				LeaderId:     "test-id",
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []*LogEntry{},
				LeaderCommit: 1,
			},
			wantLog: []*LogEntry{
				{
					Index: 1,
					Term:  1,
					Data:  []byte{},
				},
			},
			wantCommit:  1,
			wantSuccess: true,
			wantErr:     false,
		},
		{
			name:          "stale term rejected",
			initialLog:    []*LogEntry{},
			initialTerm:   2,
			initialCommit: 0,
			request: &AppendEntriesReq{
				Term:         1,
				LeaderId:     "test-id",
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []*LogEntry{},
				LeaderCommit: 1,
			},
			wantLog:     []*LogEntry{},
			wantCommit:  0,
			wantSuccess: false,
			wantErr:     false,
		},
		{
			name: "conflicting entry truncates log",
			initialLog: []*LogEntry{
				{
					Index: 1,
					Term:  1,
					Data:  []byte{},
				},
				{
					Index: 2,
					Term:  2,
					Data:  []byte{},
				},
				{
					Index: 3,
					Term:  3,
					Data:  []byte{},
				},
				{
					Index: 4,
					Term:  3,
					Data:  []byte{},
				},
				{
					Index: 5,
					Term:  3,
					Data:  []byte{},
				},
			},
			initialTerm:   3,
			initialCommit: 0,
			request: &AppendEntriesReq{
				Term:         4,
				LeaderId:     "test-id",
				PrevLogIndex: 3,
				PrevLogTerm:  3,
				Entries: []*LogEntry{
					{
						Index: 4,
						Term:  4,
						Data:  []byte{},
					},
					{
						Index: 5,
						Term:  4,
						Data:  []byte{},
					},
				},
				LeaderCommit: 3,
			},
			wantLog: []*LogEntry{
				{
					Index: 1,
					Term:  1,
					Data:  []byte{},
				},
				{
					Index: 2,
					Term:  2,
					Data:  []byte{},
				},
				{
					Index: 3,
					Term:  3,
					Data:  []byte{},
				},
				{
					Index: 4,
					Term:  4,
					Data:  []byte{},
				},
				{
					Index: 5,
					Term:  4,
					Data:  []byte{},
				},
			},
			wantCommit:  3,
			wantSuccess: true,
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := New("test-node", map[string]Peer{})
			n.log = tt.initialLog
			n.currentTerm = tt.initialTerm
			n.commitIndex = tt.initialCommit

			got := n.appendEntries(tt.request)

			if !got.Success {
				if tt.wantSuccess {
					t.Fatal("RPC failed")
				}
				return
			}
			if !tt.wantSuccess {
				t.Fatal("RPC suceeded unexpectedly")
			}

			if n.commitIndex != tt.wantCommit {
				t.Fatalf("commitIndex does not match: got %d, want %d", n.commitIndex, tt.wantCommit)
			}

			if !reflect.DeepEqual(n.log, tt.wantLog) {
				t.Fatalf("logs do not match: got %v, want %v", n.log, tt.wantLog)
			}

		})
	}
}

func TestNode_requestVote(t *testing.T) {
	tests := []struct {
		name     string
		log      []*LogEntry
		term     uint64
		votedFor string
		request  *RequestVoteReq
		want     *RequestVoteResp
	}{
		{
			name: "voteGranted to candidate with matching log",
			log: []*LogEntry{
				{
					Index: 1,
					Term:  1,
					Data:  []byte{},
				},
				{
					Index: 2,
					Term:  1,
					Data:  []byte{},
				},
				{
					Index: 3,
					Term:  1,
					Data:  []byte{},
				},
				{
					Index: 4,
					Term:  2,
					Data:  []byte{},
				},
				{
					Index: 5,
					Term:  2,
					Data:  []byte{},
				},
				{
					Index: 6,
					Term:  3,
					Data:  []byte{},
				},
			},
			term:     4,
			votedFor: "",
			request: &RequestVoteReq{
				Term:         4,
				CandidateId:  "test-candidate",
				LastLogIndex: 6,
				LastLogTerm:  3,
			},
			want: &RequestVoteResp{
				Term:        4,
				VoteGranted: true,
			},
		},
		{
			name:     "vote denied for candidate with stale term",
			log:      []*LogEntry{},
			term:     5,
			votedFor: "",
			request: &RequestVoteReq{
				Term:         4,
				CandidateId:  "test-candidate",
				LastLogIndex: 6,
				LastLogTerm:  3,
			},
			want: &RequestVoteResp{
				Term:        5,
				VoteGranted: false,
			},
		},
		{
			name:     "vote denied by node with conflicting votedFor",
			log:      []*LogEntry{},
			term:     4,
			votedFor: "another-candidate",
			request: &RequestVoteReq{
				Term:         4,
				CandidateId:  "test-candidate",
				LastLogIndex: 6,
				LastLogTerm:  3,
			},
			want: &RequestVoteResp{
				Term:        4,
				VoteGranted: false,
			},
		},
		{
			name:     "vote granted to node with empty log",
			log:      []*LogEntry{},
			term:     4,
			votedFor: "",
			request: &RequestVoteReq{
				Term:         4,
				CandidateId:  "test-candidate",
				LastLogIndex: 6,
				LastLogTerm:  3,
			},
			want: &RequestVoteResp{
				Term:        4,
				VoteGranted: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := New("test_node", map[string]Peer{})
			n.log = tt.log
			n.currentTerm = tt.term
			n.votedFor = tt.votedFor

			got := n.requestVote(tt.request)

			if got.VoteGranted != tt.want.VoteGranted {
				t.Fatalf("VoteGranted: got %v, want %v", got.VoteGranted, tt.want.VoteGranted)
			}

			if got.Term != tt.want.Term {
				t.Fatalf("Term: got %v, want %v", got.Term, tt.want.Term)
			}
		})
	}
}

func TestNode_beginElection(t *testing.T) {
	tests := []struct {
		name            string
		candidateId     string
		peerResults     map[string]*RequestVoteResp
		wantTerm        uint64
		wantWinElection bool
	}{
		{
			name:        "Election W with majority",
			candidateId: "node-0",
			peerResults: map[string]*RequestVoteResp{
				"node-1": {Term: 1, VoteGranted: true},
				"node-2": {Term: 1, VoteGranted: true},
				"node-3": {Term: 1, VoteGranted: false},
				"node-4": {Term: 1, VoteGranted: false},
			},
			wantTerm:        1,
			wantWinElection: true,
		},
		{
			name:        "Election L",
			candidateId: "node-0",
			peerResults: map[string]*RequestVoteResp{
				"node-1": {Term: 1, VoteGranted: true},
				"node-2": {Term: 2, VoteGranted: false},
				"node-3": {Term: 2, VoteGranted: false},
				"node-4": {Term: 2, VoteGranted: false},
			},
			wantTerm:        1,
			wantWinElection: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := map[string]Peer{}
			for id, resp := range tt.peerResults {
				cluster[id] = &mockPeer{rvResp: resp}
			}

			candidate := New(tt.candidateId, cluster)
			cluster[tt.candidateId] = &mockPeer{}
			candidate.cluster = cluster
			candidate.electionWon = make(chan uint64, 1)
			candidate.startElection()

			select {
			case term := <-candidate.electionWon:
				if !tt.wantWinElection {
					t.Fatal("unexpectedly won election")
				}
				if term != tt.wantTerm {
					t.Fatalf("stale term: got: %d, want: %d", term, tt.wantTerm)
				}
			case <-candidate.electionTimer.C:
				if tt.wantWinElection {
					t.Fatal("timed out waiting for election result")
				}
			}
		})
	}
}
