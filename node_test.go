package main

import (
	"context"
	"portbound/raft/proto"
	"reflect"
	"testing"
)

func TestNode_AppendEntries(t *testing.T) {
	tests := []struct {
		name          string
		initialLog    []*proto.LogEntry
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
					Data:  []byte("test"),
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
					Data:  []byte("test"),
				},
			},
			wantCommit:  1,
			wantSuccess: true,
			wantErr:     false,
		},
		{
			name: "stale term rejected",
			// ...
		},
		{
			name: "conflicting entry truncates log",
			// ...
		},
		{
			name: "follower has stale trailing entries beyond request.Entries",
			// the case I flagged above
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := NewNode("test-node", "test-addr", map[string]string{})
			if err != nil {
				t.Fatalf("could not construct receiver type: %v", err)
			}

			got, gotErr := n.AppendEntries(context.Background(), tt.request)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("AppendEntries() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("AppendEntries() succeeded unexpectedly")
			}

			if got.Success {
				if !tt.wantSuccess {
					t.Fatal("RPC suceeded unexpectedly")
				}
				return
			}

			if n.commitIndex != tt.wantCommit {
				t.Fatalf("commitIndex does not match: got %d, want %d", n.commitIndex, tt.wantCommit)
			}

			if !slices.Equal(n.log, tt.wantLog) {
				t.Fatalf("logs do not match: got %v+, want %v+", n.log, tt.wantLog)
			}

		})
	}
}
