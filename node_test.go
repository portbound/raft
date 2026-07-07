package main

import (
	"portbound/raft/proto"
	"reflect"
	"testing"
)

func TestNode_AppendEntries(t *testing.T) {
	tests := []struct {
		name          string
		initialLog    []*proto.LogEntry
		initialTerm   uint64
		initialCommit uint64
		request       *proto.AppendEntriesRequest
		wantLog       []*proto.LogEntry
		wantCommit    uint64
		wantSuccess   bool
		wantErr       bool
	}{
		{
			name: "heartbeat with no new entries advances commitIndex",
			initialLog: []*proto.LogEntry{
				{
					Index: 1,
					Term:  1,
					Data:  []byte{},
				},
			},
			initialTerm:   0,
			initialCommit: 0,
			request: &proto.AppendEntriesRequest{
				Term:         1,
				LeaderId:     "test-id",
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []*proto.LogEntry{},
				LeaderCommit: 1,
			},
			wantLog: []*proto.LogEntry{
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
			initialLog:    []*proto.LogEntry{},
			initialTerm:   2,
			initialCommit: 0,
			request: &proto.AppendEntriesRequest{
				Term:         1,
				LeaderId:     "test-id",
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []*proto.LogEntry{},
				LeaderCommit: 1,
			},
			wantLog:     []*proto.LogEntry{},
			wantCommit:  0,
			wantSuccess: false,
			wantErr:     false,
		},
		{
			name: "conflicting entry truncates log",
			initialLog: []*proto.LogEntry{
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
			request: &proto.AppendEntriesRequest{
				Term:         4,
				LeaderId:     "test-id",
				PrevLogIndex: 3,
				PrevLogTerm:  3,
				Entries: []*proto.LogEntry{
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
			wantLog: []*proto.LogEntry{
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
			n, err := NewNode("test-node", "test-addr", map[string]string{})
			if err != nil {
				t.Fatalf("could not construct receiver type: %v", err)
			}
			n.log = tt.initialLog
			n.currentTerm = tt.initialTerm
			n.commitIndex = tt.initialCommit

			got, gotErr := n.AppendEntries(t.Context(), tt.request)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("AppendEntries() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("AppendEntries() succeeded unexpectedly")
			}

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

func TestNode_RequestVote(t *testing.T) {
	tests := []struct {
		name            string
		log             []*proto.LogEntry
		term            uint64
		votedFor        string
		request         *proto.RequestVoteRequest
		wantVoteGranted bool
		wantErr         bool
	}{
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := NewNode("test-node", "test-addr", map[string]string{})
			if err != nil {
				t.Fatalf("could not construct receiver type: %v", err)
			}
			n.log = tt.log
			n.currentTerm = tt.term

			got, gotErr := n.RequestVote(t.Context(), tt.request)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("RequestVote() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("RequestVote() succeeded unexpectedly")
			}

			if got.VoteGranted != tt.wantVoteGranted {
				t.Fatalf("RPC failed: got %v, want %v", got.VoteGranted, tt.wantVoteGranted)
			}
		})
	}
}
