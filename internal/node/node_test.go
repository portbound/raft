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
		name            string
		initialTerm     uint64
		initialLog      []*LogEntry
		req             *AppendEntriesReq
		wantSuccess     bool
		wantCommitIndex uint64
		wantLog         []*LogEntry
	}{
		{
			name:            "valid_heartbeat_received",
			initialTerm:     0,
			req:             &AppendEntriesReq{Term: 1, Entries: []*LogEntry{}},
			wantSuccess:     true,
			wantCommitIndex: 0,
			wantLog:         []*LogEntry{},
		},
		{
			name:            "heartbeat_received_from_stale_leader",
			initialTerm:     2,
			req:             &AppendEntriesReq{Term: 1, Entries: []*LogEntry{}},
			wantSuccess:     false,
			wantCommitIndex: 0,
			wantLog:         []*LogEntry{},
		},
		{
			name:        "append_single_entry",
			initialTerm: 0,
			req: &AppendEntriesReq{
				Term:         1,
				LeaderId:     "node-0",
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries: []*LogEntry{
					{
						Index: 1,
						Term:  1,
						Cmd:   []byte{},
					},
				},
				LeaderCommit: 0,
			},
			wantSuccess:     true,
			wantCommitIndex: 0,
			wantLog: []*LogEntry{
				{
					Index: 1,
					Term:  1,
					Cmd:   []byte{},
				},
			},
		},
		{
			name:        "append_multiple_entries",
			initialTerm: 0,
			req: &AppendEntriesReq{
				Term:         1,
				LeaderId:     "node-0",
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries: []*LogEntry{
					{
						Index: 1,
						Term:  1,
						Cmd:   []byte{},
					},
					{
						Index: 2,
						Term:  1,
						Cmd:   []byte{},
					},
				},
				LeaderCommit: 0,
			},
			wantSuccess:     true,
			wantCommitIndex: 0,
			wantLog: []*LogEntry{
				{
					Index: 1,
					Term:  1,
					Cmd:   []byte{},
				},
				{
					Index: 2,
					Term:  1,
					Cmd:   []byte{},
				},
			},
		},
		{
			name:        "log_with_mismatched_index_and_term",
			initialTerm: 1,
			initialLog: []*LogEntry{
				{
					Index: 1,
					Term:  1,
					Cmd:   []byte{},
				},
				{
					Index: 2,
					Term:  1,
					Cmd:   []byte{},
				},
				{
					Index: 3,
					Term:  1,
					Cmd:   []byte{},
				},
			},
			req: &AppendEntriesReq{
				Term:         3,
				LeaderId:     "",
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries: []*LogEntry{
					{
						Index: 3,
						Term:  2,
						Cmd:   []byte{},
					},
					{
						Index: 4,
						Term:  2,
						Cmd:   []byte{},
					},
					{
						Index: 5,
						Term:  3,
						Cmd:   []byte{},
					},
				},
				LeaderCommit: 4,
			},
			wantSuccess:     true,
			wantCommitIndex: 4,
			wantLog: []*LogEntry{
				{
					Index: 1,
					Term:  1,
					Cmd:   []byte{},
				},
				{
					Index: 2,
					Term:  1,
					Cmd:   []byte{},
				},
				{
					Index: 3,
					Term:  2,
					Cmd:   []byte{},
				},
				{
					Index: 4,
					Term:  2,
					Cmd:   []byte{},
				},
				{
					Index: 5,
					Term:  3,
					Cmd:   []byte{},
				},
			},
		},
		{
			name:        "node_with_stale_term",
			initialTerm: 5,
			req:         &AppendEntriesReq{Term: 1},
			wantSuccess: false,
			wantLog:     []*LogEntry{},
		},
		{
			name:        "node_is_behind_on_log",
			initialTerm: 1,
			initialLog: []*LogEntry{
				{
					Index: 1,
					Term:  1,
					Cmd:   []byte{},
				},
				{
					Index: 2,
					Term:  1,
					Cmd:   []byte{},
				},
				{
					Index: 3,
					Term:  1,
					Cmd:   []byte{},
				},
			},
			req: &AppendEntriesReq{
				Term:         2,
				LeaderId:     "node-0",
				PrevLogIndex: 6,
				PrevLogTerm:  2,
				Entries: []*LogEntry{
					{
						Index: 7,
						Term:  2,
						Cmd:   []byte{},
					},
				},
			},
			wantSuccess: false,
			wantLog: []*LogEntry{
				{
					Index: 1,
					Term:  1,
					Cmd:   []byte{},
				},
				{
					Index: 2,
					Term:  1,
					Cmd:   []byte{},
				},
				{
					Index: 3,
					Term:  1,
					Cmd:   []byte{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := New("node-1", map[string]Peer{})
			n.currentTerm = tt.initialTerm
			n.log = tt.initialLog

			got := n.appendEntries(tt.req)

			if !got.Success {
				if tt.wantSuccess {
					t.Fatal("RPC failed unexpectedly")
				}
				return
			}

			if !tt.wantSuccess {
				t.Fatal("RPC suceeded unexpectedly")
			}

			if n.commitIndex != tt.wantCommitIndex {
				t.Fatalf("commitIndexes do not match: got %d, want %d", n.commitIndex, tt.wantCommitIndex)
			}

			if len(n.log) != len(tt.wantLog) {
				t.Fatalf("log length mismatch: got %d, want %d", len(n.log), len(tt.wantLog))
			}

			for i := range n.log {
				if !reflect.DeepEqual(*n.log[i], *tt.wantLog[i]) {
					t.Fatalf("logs do not match at index %d: got %+v, want %+v", i, *n.log[i], *tt.wantLog[i])
				}
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
					Cmd:   []byte{},
				},
				{
					Index: 2,
					Term:  1,
					Cmd:   []byte{},
				},
				{
					Index: 3,
					Term:  1,
					Cmd:   []byte{},
				},
				{
					Index: 4,
					Term:  2,
					Cmd:   []byte{},
				},
				{
					Index: 5,
					Term:  2,
					Cmd:   []byte{},
				},
				{
					Index: 6,
					Term:  3,
					Cmd:   []byte{},
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
