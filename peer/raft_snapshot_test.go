package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReceiveSnapshotRejectsStaleTerm(t *testing.T) {
	stateDir := setupSnapshotTestState(t, "leader")

	raftState.mu.Lock()
	raftState.persistentState.CurrentTerm = 5
	persistLocked()
	raftState.mu.Unlock()

	resp, err := receiveSnapshot(InstallSnapshotRequest{
		Term:              4,
		LeaderId:          "node2",
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
		Offset:            0,
		Data:              []byte("ignored"),
		Done:              true,
	})
	if err != nil {
		t.Fatalf("receiveSnapshot returned error: %v", err)
	}
	if resp.Term != 5 {
		t.Fatalf("expected stale snapshot response term 5, got %d", resp.Term)
	}

	raftState.mu.Lock()
	defer raftState.mu.Unlock()
	if raftState.role != "leader" {
		t.Fatalf("expected stale snapshot to leave role unchanged, got %q", raftState.role)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "node1.snapshot.json")); !os.IsNotExist(err) {
		t.Fatalf("expected no snapshot file to be created, stat err=%v", err)
	}
}

func TestReceiveSnapshotPublishesOnlyAfterDone(t *testing.T) {
	stateDir := setupSnapshotTestState(t, "follower")

	raftState.mu.Lock()
	raftState.persistentState.CurrentTerm = 2
	raftState.persistentState.Log = []LogEntry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2},
		{Term: 2, Index: 3},
	}
	raftState.indexState.CommitIndex = 0
	raftState.indexState.LastApplied = 0
	persistLocked()
	raftState.mu.Unlock()

	Store.mu.Lock()
	Store.data = map[string]string{"before": "value"}
	Store.appliedReqIDs = map[string]AppliedResult{"before": {Value: "value"}}
	Store.mu.Unlock()

	payload := mustSnapshotPayload(t, SnapshotFile{
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
		Data:              map[string]string{"after": "value"},
		AppliedReqIDs:     map[string]AppliedResult{"after": {Value: "value"}},
	})
	middle := len(payload) / 2

	resp, err := receiveSnapshot(InstallSnapshotRequest{
		Term:              2,
		LeaderId:          "node2",
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
		Offset:            0,
		Data:              payload[:middle],
		Done:              false,
	})
	if err != nil {
		t.Fatalf("receiveSnapshot first chunk returned error: %v", err)
	}
	if resp.Term != 2 {
		t.Fatalf("expected first chunk response term 2, got %d", resp.Term)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "node1.snapshot.tmp")); err != nil {
		t.Fatalf("expected temp snapshot file after partial chunk: %v", err)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "node1.snapshot.json")); !os.IsNotExist(err) {
		t.Fatalf("expected no final snapshot file before done chunk, stat err=%v", err)
	}

	Store.mu.RLock()
	if got := Store.data["before"]; got != "value" {
		Store.mu.RUnlock()
		t.Fatalf("expected store to remain unchanged before done chunk, got %q", got)
	}
	if _, ok := Store.data["after"]; ok {
		Store.mu.RUnlock()
		t.Fatal("expected store snapshot data to remain unapplied before done chunk")
	}
	Store.mu.RUnlock()

	raftState.mu.Lock()
	if raftState.snapshotState.LastIncludedIndex != 0 {
		raftState.mu.Unlock()
		t.Fatalf("expected snapshot state to remain unchanged before done chunk, got %d", raftState.snapshotState.LastIncludedIndex)
	}
	raftState.mu.Unlock()

	resp, err = receiveSnapshot(InstallSnapshotRequest{
		Term:              2,
		LeaderId:          "node2",
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
		Offset:            middle,
		Data:              payload[middle:],
		Done:              true,
	})
	if err != nil {
		t.Fatalf("receiveSnapshot final chunk returned error: %v", err)
	}
	if resp.Term != 2 {
		t.Fatalf("expected final chunk response term 2, got %d", resp.Term)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "node1.snapshot.tmp")); !os.IsNotExist(err) {
		t.Fatalf("expected temp snapshot file to be removed after final chunk, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "node1.snapshot.json")); err != nil {
		t.Fatalf("expected final snapshot file after done chunk: %v", err)
	}

	Store.mu.RLock()
	if got := Store.data["after"]; got != "value" {
		Store.mu.RUnlock()
		t.Fatalf("expected store to update from installed snapshot, got %q", got)
	}
	if _, ok := Store.data["before"]; ok {
		Store.mu.RUnlock()
		t.Fatal("expected installed snapshot to replace old store contents")
	}
	Store.mu.RUnlock()

	raftState.mu.Lock()
	defer raftState.mu.Unlock()
	if raftState.snapshotState.LastIncludedIndex != 2 {
		t.Fatalf("expected snapshot index 2 after install, got %d", raftState.snapshotState.LastIncludedIndex)
	}
	if raftState.indexState.CommitIndex != 2 {
		t.Fatalf("expected commit index to advance to 2, got %d", raftState.indexState.CommitIndex)
	}
	if raftState.indexState.LastApplied != 2 {
		t.Fatalf("expected last applied to advance to 2, got %d", raftState.indexState.LastApplied)
	}
	if len(raftState.persistentState.Log) != 1 || raftState.persistentState.Log[0].Index != 3 {
		t.Fatalf("expected matching log prefix to be compacted, got %+v", raftState.persistentState.Log)
	}
}

func TestMaySnapshotLockedRewritesSnapshotWithoutStaleKeys(t *testing.T) {
	stateDir := setupSnapshotTestState(t, "leader")

	oldLogSizeThreshold := logSizeThreshold
	oldUnappliedLogEntries := unappliedLogEntries
	logSizeThreshold = 0
	unappliedLogEntries = 0
	t.Cleanup(func() {
		logSizeThreshold = oldLogSizeThreshold
		unappliedLogEntries = oldUnappliedLogEntries
	})

	err := saveSnapshotFile("node1", stateDir, SnapshotFile{
		LastIncludedIndex: 0,
		LastIncludedTerm:  0,
		LastSnapshotAt:    time.Time{},
		Data:              map[string]string{"stale": "old"},
		AppliedReqIDs:     map[string]AppliedResult{"stale": {Value: "old"}},
	})
	if err != nil {
		t.Fatalf("save initial snapshot: %v", err)
	}

	Store.mu.Lock()
	Store.data = map[string]string{"fresh": "value"}
	Store.appliedReqIDs = map[string]AppliedResult{"fresh": {Value: "value"}}
	Store.mu.Unlock()

	raftState.mu.Lock()
	raftState.persistentState.Log = []LogEntry{{
		Term:  3,
		Index: 1,
		Command: Command{
			ReqId:     "fresh",
			Operation: "PUT",
			Key:       "fresh",
			Value:     "value",
		},
	}}
	raftState.indexState.CommitIndex = 1
	raftState.indexState.LastApplied = 1
	persistLocked()
	maySnapshotLocked()
	raftState.mu.Unlock()

	snapshot := loadSnapshotFile("node1", stateDir)
	if got := snapshot.Data["fresh"]; got != "value" {
		t.Fatalf("expected rewritten snapshot to contain fresh key, got %q", got)
	}
	if _, ok := snapshot.Data["stale"]; ok {
		t.Fatal("expected rewritten snapshot to drop stale keys")
	}
	if _, ok := snapshot.AppliedReqIDs["stale"]; ok {
		t.Fatal("expected rewritten snapshot to drop stale request ids")
	}

	raftState.mu.Lock()
	defer raftState.mu.Unlock()
	if raftState.snapshotState.LastIncludedIndex != 1 {
		t.Fatalf("expected snapshot metadata to advance to index 1, got %d", raftState.snapshotState.LastIncludedIndex)
	}
	if len(raftState.persistentState.Log) != 0 {
		t.Fatalf("expected log to be truncated after local snapshot, got %+v", raftState.persistentState.Log)
	}
}

func setupSnapshotTestState(t *testing.T, role string) string {
	t.Helper()

	stateDir := t.TempDir()
	initRaftState("node1", role, nil, stateDir)
	if raftState.timer != nil {
		raftState.timer.Stop()
	}

	Store.mu.Lock()
	Store.data = make(map[string]string)
	Store.appliedReqIDs = make(map[string]AppliedResult)
	Store.mu.Unlock()

	t.Cleanup(func() {
		if raftState.timer != nil {
			raftState.timer.Stop()
		}
	})

	return stateDir
}

func mustSnapshotPayload(t *testing.T, snapshot SnapshotFile) []byte {
	t.Helper()

	bytes, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot payload: %v", err)
	}

	return bytes
}